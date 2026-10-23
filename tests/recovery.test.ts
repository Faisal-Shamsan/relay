import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { defineWorkflow, runActivation } from "@relay/sdk";
import { createTestDatabase, startServer } from "relay-server";
import { startWorker } from "./spawn.js";

test("recovery budget exhaustion fails the run instead of starting another attempt", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    recoveryBudget: 1,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-budget-"));
  try {
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
    const created = await fetch(`${server.url}/v1/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: { n: 1 } }),
    });
    const runId = String((await created.json() as { runId: string }).runId);
    const barrierDir = join(dir, "barrier");
    const failpointFile = join(dir, "failpoints.json");
    writeFileSync(failpointFile, JSON.stringify({
      points: [{
        point: "step.after-start-commit-before-callback",
        match: { runId, stepKey: "only", attempt: 1 },
        occurrence: 1,
        action: "crash-process",
      }],
    }));
    const crashed = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: barrierDir,
    });
    const first = await crashed.done;
    assert.equal(first.code, 78, first.stderr);
    await admin.query(
      `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
      [runId],
    );
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);

    let calls = 0;
    const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("only", async () => {
        calls += 1;
        return { ok: true };
      });
    });
    await assert.rejects(
      () => runActivation({ serverUrl: server.url, workflow, finishRun: false }),
      (error: Error) => error.message.includes("409") && error.message.includes("RecoveryBudgetExceeded"),
    );
    assert.equal(calls, 0);

    const run = await admin.query<{ state: string; code: string }>(
      `SELECT state, error->>'code' AS code FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(run.rows[0]?.state, "failed");
    assert.equal(run.rows[0]?.code, "RecoveryBudgetExceeded");
    const attempts = await admin.query<{ attempt: string; state: string }>(
      `SELECT attempt::text AS attempt, state FROM step_attempts WHERE run_id = $1 ORDER BY attempt`,
      [runId],
    );
    assert.deepEqual(attempts.rows, [{ attempt: "1", state: "abandoned" }]);
    const step = await admin.query<{ state: string; budget: number }>(
      `SELECT state, recovery_budget AS budget FROM steps WHERE run_id = $1`,
      [runId],
    );
    assert.equal(step.rows[0]?.state, "interrupted");
    assert.equal(step.rows[0]?.budget, 1);
    const queue = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    assert.equal(queue.rows[0]?.count, "0");
    const started = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepStarted'`,
      [runId],
    );
    assert.equal(started.rows[0]?.count, "1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});
