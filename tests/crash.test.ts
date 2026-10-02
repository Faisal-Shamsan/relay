import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { startWorker, waitForFile } from "./spawn.js";

test("crash-process kills the worker after StepStarted and leaves the runner alive", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-crash-"));
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
    const invocationLog = join(dir, "invocations.log");
    writeFileSync(failpointFile, JSON.stringify({
      points: [
        {
          point: "step.after-start-commit-before-callback",
          match: { runId, stepKey: "only", attempt: 1 },
          occurrence: 1,
          action: "pause-on-barrier",
        },
        {
          point: "step.after-user-function-before-completion",
          match: { runId, stepKey: "only", attempt: 1 },
          occurrence: 1,
          action: "crash-process",
        },
      ],
    }));
    const worker = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: invocationLog,
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: barrierDir,
    });
    await waitForFile(join(barrierDir, "hit.json"));
    const started = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepStarted' AND attempt = 1`,
      [runId],
    );
    assert.equal(started.rows[0]?.count, "1");
    writeFileSync(join(barrierDir, "release"), "1");
    const result = await worker.done;
    assert.equal(result.code, 78, result.stderr);
    const completed = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completed.rows[0]?.count, "0");
    assert.equal(process.pid > 0, true);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});
