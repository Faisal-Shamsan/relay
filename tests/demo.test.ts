import assert from "node:assert/strict";
import { execSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { defineWorkflow, runActivation } from "@relay/sdk";
import { createTestDatabase, startServer } from "relay-server";
import { repoRoot, startWorker } from "./spawn.js";

const v1 = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
  return ctx.step("only", async () => ({ ok: true }));
});
const v2 = defineWorkflow({ name: "demo", version: "demo-v2" }, async (ctx) => {
  return ctx.step("only", async () => ({ ok: true }));
});

test("phase 1 demo: about 100 runs recover without a stale completion", { timeout: 180_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-demo-"));
  const runIds: string[] = [];
  try {
    const retiredV1 = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: retiredV1,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v2" }],
    });
    await admin.query(
      `UPDATE worker_sessions SET expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`,
      [retiredV1],
    );
    const waiting = await createRun(server.url, "demo-v1", { n: "wait" });
    const routed = await createRun(server.url, "demo-v2", { n: "v2" });
    runIds.push(waiting, routed);
    const listed = await fetch(`${server.url}/v1/workflows`);
    const workflows = await listed.json() as {
      workflows: Array<{ version: string; compatibleWorkers: number; pendingRuns: number }>;
    };
    const listedV1 = workflows.workflows.find((row) => row.version === "demo-v1");
    const listedV2 = workflows.workflows.find((row) => row.version === "demo-v2");
    assert.equal(listedV1?.compatibleWorkers, 0);
    assert.equal(listedV1?.pendingRuns, 1);
    assert.equal(listedV2?.compatibleWorkers, 1);
    assert.equal(listedV2?.pendingRuns, 1);
    assert.deepEqual(await runActivation({ serverUrl: server.url, workflow: v2 }), { ok: true });
    const held = await admin.query<{ state: string; owner: string | null }>(
      `SELECT r.state, q.lease_owner::text AS owner
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [waiting],
    );
    assert.equal(held.rows[0]?.state, "pending");
    assert.equal(held.rows[0]?.owner, null);
    assert.deepEqual(await runActivation({ serverUrl: server.url, workflow: v1 }), { ok: true });

    const durations: number[] = [];
    for (let i = 0; i < 94; i += 1) {
      const runId = await createRun(server.url, "demo-v1", { n: i });
      runIds.push(runId);
      const started = performance.now();
      assert.deepEqual(await runActivation({ serverUrl: server.url, workflow: v1 }), { ok: true });
      durations.push(performance.now() - started);
    }

    const leaseCrash = await crashAndRecover(server, admin, dir, "lease", "lease.after-commit-before-response", false);
    const effectCrash = await crashAndRecover(server, admin, dir, "effect", "step.after-user-function-before-completion", true);
    const memoCrash = await crashAndRecover(server, admin, dir, "memo", "step.after-completion-commit-before-response", false);
    runIds.push(leaseCrash.runId, effectCrash.runId, memoCrash.runId);
    assert.equal(leaseCrash.invocations, 1);
    assert.equal(effectCrash.invocations, 2);
    assert.equal(effectCrash.effects, 1);
    assert.equal(memoCrash.invocations, 1);

    const lost = await lostAcknowledgement(server.url);
    runIds.push(lost);

    const finished = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM runs WHERE id = ANY($1::uuid[]) AND state = 'completed'`,
      [runIds],
    );
    assert.equal(finished.rows[0]?.count, 100);
    assert.equal(runIds.length, 100);
    const stale = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count
       FROM step_attempts a
       JOIN run_events done ON done.run_id = a.run_id AND done.step_key = a.step_key AND done.type = 'StepCompleted'
       JOIN run_events started ON started.run_id = a.run_id AND started.step_key = a.step_key
         AND started.attempt = a.attempt AND started.type = 'StepStarted'
       WHERE a.run_id = ANY($1::uuid[]) AND started.seq > done.seq`,
      [runIds],
    );
    assert.equal(stale.rows[0]?.count, 0);
    const effectAttempts = await admin.query<{ attempt: number; state: string }>(
      `SELECT attempt, state FROM step_attempts WHERE run_id = $1 ORDER BY attempt`,
      [effectCrash.runId],
    );
    assert.deepEqual(effectAttempts.rows, [
      { attempt: 1, state: "abandoned" },
      { attempt: 2, state: "completed" },
    ]);
    const memoAttempts = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM step_attempts WHERE run_id = $1`,
      [memoCrash.runId],
    );
    assert.equal(memoAttempts.rows[0]?.count, 1);

    const history = await admin.query<{ bytes: string }>(
      `SELECT coalesce(sum(octet_length(payload::text)), 0)::text AS bytes
       FROM run_events
       WHERE run_id = ANY($1::uuid[])`,
      [runIds],
    );
    const postgres = await admin.query<{ version: string }>(`SELECT version()`);
    let commit = "unknown";
    try {
      commit = execSync("git rev-parse HEAD", { cwd: repoRoot, encoding: "utf8" }).trim();
    } catch {
      commit = "unknown";
    }
    const p50 = percentile(durations, 0.5);
    const p95 = percentile(durations, 0.95);
    const p99 = percentile(durations, 0.99);
    assert.equal(durations.length, 94);
    assert.equal(p50 <= p95 && p95 <= p99, true);
    console.log([
      "phase-1 demo ledger",
      `commit ${commit}`,
      `node ${process.version}`,
      `postgres ${postgres.rows[0]?.version ?? "unknown"}`,
      "runs 100",
      "normal in-process activations 94",
      `activation ms p50 ${p50.toFixed(1)} p95 ${p95.toFixed(1)} p99 ${p99.toFixed(1)}`,
      `lease crash callbacks ${leaseCrash.invocations}`,
      `effect crash callbacks ${effectCrash.invocations} provider effects ${effectCrash.effects}`,
      `memoized recovery callbacks ${memoCrash.invocations}`,
      `history bytes ${history.rows[0]?.bytes ?? "0"}`,
      "not measured: stream catch-up, 1000 runs, 10000 runs, postgres CPU, replay duration",
      "effect dedupe is the fake provider, not an exactly-once claim",
    ].join("\n"));
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function createRun(url: string, version: string, input: { n: number | string }): Promise<string> {
  const response = await fetch(`${url}/v1/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflowName: "demo", workflowVersion: version, input }),
  });
  if (response.status !== 201) throw new Error(await response.text());
  const body = await response.json() as { runId: string };
  return body.runId;
}

async function crashAndRecover(
  server: { url: string },
  admin: pg.Client,
  dir: string,
  name: string,
  point: string,
  provider: boolean,
): Promise<{ runId: string; invocations: number; effects: number }> {
  const runId = await createRun(server.url, "demo-v1", { n: name });
  const invocationLog = join(dir, `${name}-invocations.log`);
  const providerLog = join(dir, `${name}-provider.log`);
  const failpointFile = join(dir, `${name}-failpoints.json`);
  writeFileSync(failpointFile, JSON.stringify({
    points: [{ point, match: { runId, stepKey: point.startsWith("step.") ? "only" : undefined, attempt: point.startsWith("step.") ? 1 : undefined }, occurrence: 1, action: "crash-process" }],
  }));
  const crashed = startWorker({
    RELAY_SERVER_URL: server.url,
    RELAY_INVOCATION_LOG: invocationLog,
    RELAY_PROVIDER_LOG: provider ? providerLog : "",
    RELAY_PROVIDER_DEDUPE: provider ? "1" : "",
    RELAY_FAILPOINT_FILE: failpointFile,
    RELAY_BARRIER_DIR: join(dir, `${name}-barrier`),
  });
  const first = await crashed.done;
  assert.equal(first.code, 78, first.stderr);
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
  const recovered = startWorker({
    RELAY_SERVER_URL: server.url,
    RELAY_INVOCATION_LOG: invocationLog,
    RELAY_PROVIDER_LOG: provider ? providerLog : "",
    RELAY_PROVIDER_DEDUPE: provider ? "1" : "",
    RELAY_BARRIER_DIR: join(dir, `${name}-recover`),
  });
  const second = await recovered.done;
  assert.equal(second.code, 0, second.stderr);
  return { runId, invocations: countLines(invocationLog), effects: provider ? countKind(providerLog, "effect") : 0 };
}

async function lostAcknowledgement(url: string): Promise<string> {
  const runId = await createRun(url, "demo-v1", { n: "lost" });
  const sessionId = randomUUID();
  await post(url, "/worker/v1/register", {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    tenantId: SEEDED_TENANT_ID,
    workflows: [{ name: "demo", version: "demo-v1" }],
  });
  const lease = await post(url, "/worker/v1/lease", {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId: sessionId,
  }) as { fence: string };
  const started = await post(url, "/worker/v1/events", {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    runId,
    fence: lease.fence,
    intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
  }) as { attemptToken: string };
  const completion = {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    runId,
    fence: lease.fence,
    intent: { kind: "complete_step", stepKey: "only", attempt: 1, attemptToken: started.attemptToken, result: { ok: true } },
  };
  const first = await post(url, "/worker/v1/events", completion);
  const second = await post(url, "/worker/v1/events", completion);
  assert.deepEqual(second, first);
  await post(url, "/worker/v1/events", {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    runId,
    fence: lease.fence,
    intent: { kind: "finish_run", output: { ok: true } },
  });
  return runId;
}

async function post(url: string, path: string, body: unknown): Promise<unknown> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text}`);
  return JSON.parse(text) as unknown;
}

function countLines(path: string): number {
  try {
    return readFileSync(path, "utf8").split("\n").filter((line) => line.length > 0).length;
  } catch {
    return 0;
  }
}

function countKind(path: string, kind: string): number {
  return countLines(path) === 0 ? 0 : readFileSync(path, "utf8").split("\n").filter((line) => {
    if (line.length === 0) return false;
    return (JSON.parse(line) as { kind?: string }).kind === kind;
  }).length;
}

function percentile(samples: number[], p: number): number {
  const sorted = [...samples].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[index] ?? 0;
}
