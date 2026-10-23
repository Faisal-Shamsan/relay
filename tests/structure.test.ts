import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation } from "@relay/sdk";

test("F10 a duplicate step key fails before the second callback", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("only", async () => {
      calls += 1;
      return { ok: true };
    });
    await ctx.step("only", async () => {
      calls += 1;
      return { ok: false };
    });
    return { ok: true };
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "DuplicateStepKey",
    );
    assert.equal(calls, 1);
    const commands = await harness.admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'CommandRecorded'`,
      [runId],
    );
    assert.equal(commands.rows[0]?.count, "1");
  } finally {
    await harness.close();
  }
});

test("F11 reordered, removed, and inserted commands diverge before the new callback", async () => {
  const harness = await openHarness();
  const calls: string[] = [];
  const original = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const first = await ctx.step("a", async () => {
      calls.push("a");
      return { step: "a" };
    });
    const second = await ctx.step("b", async () => {
      calls.push("b");
      return { step: "b" };
    });
    return { first, second };
  });
  try {
    const runId = await createRun(harness.url);
    await runActivation({ serverUrl: harness.url, workflow: original, finishRun: false });
    assert.deepEqual(calls, ["a", "b"]);
    await releaseLease(harness.admin, runId);

    const reordered = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.step("b", async () => {
        calls.push("b-again");
        return { step: "b" };
      });
      return { ok: true };
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: reordered, finishRun: false }),
      (error: { name?: string; details?: { position?: string; expected?: { key?: string }; actual?: { key?: string } } }) =>
        error.name === "Nondeterminism" && error.details?.position === "0" && error.details.expected?.key === "a" && error.details.actual?.key === "b",
    );
    assert.deepEqual(calls, ["a", "b"]);
    await releaseLease(harness.admin, runId);

    const removed = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("a", async () => {
        calls.push("a-again");
        return { step: "a" };
      });
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: removed, finishRun: false }),
      (error: { name?: string; details?: { position?: string; reason?: string; expected?: { key?: string } } }) =>
        error.name === "Nondeterminism" && error.details?.reason === "unconsumed history" && error.details.position === "1" && error.details.expected?.key === "b",
    );
    assert.deepEqual(calls, ["a", "b"]);
    await releaseLease(harness.admin, runId);

    const inserted = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.step("a", async () => {
        calls.push("a-again");
        return { step: "a" };
      });
      await ctx.step("c", async () => {
        calls.push("c");
        return { step: "c" };
      });
      return { ok: true };
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: inserted, finishRun: false }),
      (error: { name?: string; details?: { position?: string; expected?: { key?: string }; actual?: { key?: string } } }) =>
        error.name === "Nondeterminism" && error.details?.position === "1" && error.details.expected?.key === "b" && error.details.actual?.key === "c",
    );
    assert.deepEqual(calls, ["a", "b"]);
  } finally {
    await harness.close();
  }
});

test("F12 a captured value change with the same command shape is not detected", async () => {
  const harness = await openHarness();
  let region = "us";
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("prices", async () => {
      calls += 1;
      return { region };
    });
  });
  try {
    const runId = await createRun(harness.url);
    const first = await runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    assert.deepEqual(first, { region: "us" });
    assert.equal(calls, 1);
    region = "eu";
    await releaseLease(harness.admin, runId);
    const second = await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.deepEqual(second, { region: "us" });
  } finally {
    await harness.close();
  }
});

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
