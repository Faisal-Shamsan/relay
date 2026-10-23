import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { defineWorkflow, runActivation } from "@relay/sdk";
import { createTestDatabase, startServer } from "relay-server";
import { startWorker, waitForFile } from "./spawn.js";

test("F08 parallel recovery memoizes A, restarts B, and keeps C exhausted", { timeout: 30_000 }, async () => {
  const harness = await openHarness();
  const dir = mkdtempSync(join(tmpdir(), "relay-f08-"));
  const barrierDir = join(dir, "barrier");
  const failpointFile = join(dir, "failpoints.json");
  const invocationLog = join(dir, "invocations.log");
  const resultFile = join(dir, "result.json");
  try {
    const runId = await createRun(harness.url);
    writeFileSync(failpointFile, JSON.stringify({
      points: [
        {
          point: "step.after-start-commit-before-callback",
          match: { runId, stepKey: "b", attempt: 1 },
          occurrence: 1,
          action: "pause-on-barrier",
        },
        {
          point: "step.after-user-function-before-completion",
          match: { runId, stepKey: "b", attempt: 1 },
          occurrence: 1,
          action: "crash-process",
        },
      ],
    }));
    const crashed = startWorker({
      RELAY_SERVER_URL: harness.url,
      RELAY_INVOCATION_LOG: invocationLog,
      RELAY_RESULT_FILE: resultFile,
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: barrierDir,
    }, "packages/sdk/dist/src/parallel-worker.js");
    await waitForFile(join(barrierDir, "hit.json"));
    await waitForStates(harness.admin, runId, { a: "completed", b: "running", c: "exhausted" });
    writeFileSync(join(barrierDir, "release"), "1");
    const first = await crashed.done;
    assert.equal(first.code, 78, first.stderr);
    await releaseLease(harness.admin, runId);

    const recovered = startWorker({
      RELAY_SERVER_URL: harness.url,
      RELAY_INVOCATION_LOG: invocationLog,
      RELAY_RESULT_FILE: resultFile,
    }, "packages/sdk/dist/src/parallel-worker.js");
    const second = await recovered.done;
    assert.equal(second.code, 0, second.stderr);

    const lines = readFileSync(invocationLog, "utf8").trim().split("\n").map((line) => JSON.parse(line) as { step: string; attempt: number });
    assert.deepEqual(lines.filter((line) => line.step === "a").map((line) => line.attempt), [1]);
    assert.deepEqual(lines.filter((line) => line.step === "c").map((line) => line.attempt), [1]);
    assert.deepEqual(lines.filter((line) => line.step === "b").map((line) => line.attempt), [1, 2]);

    const stored = JSON.parse(readFileSync(resultFile, "utf8")) as {
      failures: Array<{ index: number; stepKey: string; message: string }>;
      results: Array<{ item?: string } | null>;
    };
    assert.deepEqual(stored.failures, [{ index: 2, stepKey: "c", message: "c failed" }]);
    assert.deepEqual(stored.results, [{ item: "a" }, { item: "b" }, null]);
    const output = await harness.admin.query<{ output: unknown }>(`SELECT output FROM runs WHERE id = $1`, [runId]);
    assert.deepEqual(output.rows[0]?.output, stored);

    const steps = await harness.admin.query<{ step_key: string; state: string; attempt: string; ordinal: string; child_index: number }>(
      `SELECT step_key, state, attempt::text AS attempt, first_seen_ordinal::text AS ordinal, child_index
       FROM steps WHERE run_id = $1 ORDER BY child_index`,
      [runId],
    );
    assert.deepEqual(steps.rows, [
      { step_key: "a", state: "completed", attempt: "1", ordinal: "0", child_index: 0 },
      { step_key: "b", state: "completed", attempt: "2", ordinal: "1", child_index: 1 },
      { step_key: "c", state: "exhausted", attempt: "1", ordinal: "2", child_index: 2 },
    ]);
    const attempts = await harness.admin.query<{ step_key: string; attempt: string; state: string }>(
      `SELECT step_key, attempt::text AS attempt, state
       FROM step_attempts WHERE run_id = $1 ORDER BY step_key, attempt`,
      [runId],
    );
    assert.deepEqual(attempts.rows, [
      { step_key: "a", attempt: "1", state: "completed" },
      { step_key: "b", attempt: "1", state: "abandoned" },
      { step_key: "b", attempt: "2", state: "completed" },
      { step_key: "c", attempt: "1", state: "failed" },
    ]);
    const command = await harness.admin.query<{ descriptor: { children?: string[]; concurrency?: number } }>(
      `SELECT descriptor FROM commands WHERE run_id = $1 AND kind = 'parallel'`,
      [runId],
    );
    assert.equal(command.rows.length, 1);
    assert.deepEqual(command.rows[0]?.descriptor.children, ["a", "b", "c"]);
    assert.equal(command.rows[0]?.descriptor.concurrency, 3);
    await assertContiguousSequences(harness.admin, runId);
  } finally {
    await harness.close();
  }
});

test("F09 reversed parallel completion keeps declaration order", { timeout: 20_000 }, async () => {
  const harness = await openHarness();
  const dir = mkdtempSync(join(tmpdir(), "relay-f09-"));
  const barrierDir = join(dir, "barrier");
  const previousFailpoints = process.env.RELAY_ALLOW_FAILPOINTS;
  process.env.RELAY_ALLOW_FAILPOINTS = "1";
  try {
    const runId = await createRun(harness.url);
    const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step.parallel("group", [
        { name: "a", run: async () => ({ item: "a" }) },
        { name: "b", run: async () => ({ item: "b" }) },
      ], { concurrency: 2 });
    });
    const activation = runActivation({
      serverUrl: harness.url,
      workflow,
      failpoints: {
        points: [{
          point: "step.after-user-function-before-completion",
          match: { runId, stepKey: "a", attempt: 1 },
          occurrence: 1,
          action: "pause-on-barrier",
        }],
      },
      barrierDir,
    });
    await waitForFile(join(barrierDir, "hit.json"));
    await waitForStates(harness.admin, runId, { a: "running", b: "completed" });
    writeFileSync(join(barrierDir, "release"), "1");
    const output = await activation;
    assert.deepEqual(output, [{ item: "a" }, { item: "b" }]);

    const ordinals = await harness.admin.query<{ step_key: string; ordinal: string; child_index: number }>(
      `SELECT step_key, first_seen_ordinal::text AS ordinal, child_index
       FROM steps WHERE run_id = $1 ORDER BY child_index`,
      [runId],
    );
    assert.deepEqual(ordinals.rows, [
      { step_key: "a", ordinal: "0", child_index: 0 },
      { step_key: "b", ordinal: "1", child_index: 1 },
    ]);
    const completions = await harness.admin.query<{ step_key: string; seq: string }>(
      `SELECT step_key, seq::text AS seq FROM run_events
       WHERE run_id = $1 AND type = 'StepCompleted' ORDER BY run_events.seq`,
      [runId],
    );
    assert.deepEqual(completions.rows.map((row) => row.step_key), ["b", "a"]);
    const seqB = Number(completions.rows[0]?.seq);
    const seqA = Number(completions.rows[1]?.seq);
    assert.ok(seqB < seqA);
    await assertContiguousSequences(harness.admin, runId);
  } finally {
    if (previousFailpoints === undefined) delete process.env.RELAY_ALLOW_FAILPOINTS;
    else process.env.RELAY_ALLOW_FAILPOINTS = previousFailpoints;
    await harness.close();
  }
});

test("duplicate parallel children are rejected before a callback or command", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step.parallel("group", [
      { name: "a", run: async () => { calls += 1; return { ok: true }; } },
      { name: "a", run: async () => { calls += 1; return { ok: false }; } },
    ]);
    return { ok: true };
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "DuplicateStepKey",
    );
    assert.equal(calls, 0);
    const commands = await harness.admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM commands WHERE run_id = $1`,
      [runId],
    );
    assert.equal(commands.rows[0]?.count, "0");
  } finally {
    await harness.close();
  }
});

test("an empty parallel group records one command and returns no children", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const results = await ctx.step.parallel("none", []);
    const after = await ctx.step("after", async () => ({ ok: true }));
    return { results, after };
  });
  try {
    const runId = await createRun(harness.url);
    const output = await runActivation({ serverUrl: harness.url, workflow });
    assert.deepEqual(output, { results: [], after: { ok: true } });
    const commands = await harness.admin.query<{ kind: string; command_key: string; descriptor: { children?: unknown[] } }>(
      `SELECT kind, command_key, descriptor FROM commands WHERE run_id = $1 ORDER BY command_pos`,
      [runId],
    );
    assert.equal(commands.rows[0]?.kind, "parallel");
    assert.equal(commands.rows[0]?.command_key, "none");
    assert.deepEqual(commands.rows[0]?.descriptor.children, []);
    assert.equal(commands.rows[1]?.kind, "step");
    assert.equal(commands.rows[1]?.command_key, "after");
    const ordinal = await harness.admin.query<{ ordinal: string }>(
      `SELECT first_seen_ordinal::text AS ordinal FROM steps WHERE run_id = $1 AND step_key = 'after'`,
      [runId],
    );
    assert.equal(ordinal.rows[0]?.ordinal, "0");
  } finally {
    await harness.close();
  }
});

async function waitForStates(admin: pg.Client, runId: string, expected: Record<string, string>): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const rows = await admin.query<{ step_key: string; state: string }>(
      `SELECT step_key, state FROM steps WHERE run_id = $1`,
      [runId],
    );
    const actual = Object.fromEntries(rows.rows.map((row) => [row.step_key, row.state]));
    if (Object.entries(expected).every(([key, state]) => actual[key] === state)) return;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  throw new Error(`steps did not reach ${JSON.stringify(expected)}`);
}

async function assertContiguousSequences(admin: pg.Client, runId: string): Promise<void> {
  const rows = await admin.query<{ seq: string }>(
    `SELECT seq::text AS seq FROM run_events WHERE run_id = $1 ORDER BY run_events.seq`,
    [runId],
  );
  assert.ok(rows.rows.length > 0);
  rows.rows.forEach((row, index) => {
    assert.equal(row.seq, String(index + 1));
  });
}

async function openHarness(): Promise<{ url: string; admin: pg.Client; close: () => Promise<void> }> {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  await fetch(`${server.url}/worker/v1/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: crypto.randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    }),
  });
  return {
    url: server.url,
    admin,
    close: async () => {
      await admin.end();
      await server.close();
      await db.drop();
    },
  };
}

async function createRun(url: string): Promise<string> {
  const created = await fetch(`${url}/v1/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: { n: 1 } }),
  });
  return String((await created.json() as { runId: string }).runId);
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}
