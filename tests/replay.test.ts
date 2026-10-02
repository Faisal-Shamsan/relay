import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { logicalStepIdempotencyKey, SEEDED_TENANT_ID, validateRelayJson } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation } from "@relay/sdk";

test("a completed step replays from history without calling the user function again", async () => {
  assert.throws(() => validateRelayJson(new Date()), /Date/);
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  let calls = 0;
  let recordedKey = "";
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("only", async (stepCtx) => {
      calls += 1;
      recordedKey = stepCtx.idempotencyKey;
      return { ok: true };
    });
  });
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
    await runActivation({ serverUrl: server.url, workflow, finishRun: false });
    assert.equal(calls, 1);
    await admin.query(
      `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
      [runId],
    );
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const output = await runActivation({ serverUrl: server.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.deepEqual(output, { ok: true });
    const again = logicalStepIdempotencyKey(SEEDED_TENANT_ID, runId, "only");
    assert.equal(recordedKey, again);
    assert.equal(again.startsWith("relay-step-v1:"), true);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});
