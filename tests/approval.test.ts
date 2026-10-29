import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation, signal } from "@relay/sdk";

test("a run waits for approval and then finishes", async () => {
  const harness = await openHarness();
  let applied = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const decision = await ctx.waitForEvent("approval", { timeoutMs: 60_000 });
    return ctx.step("apply", async () => {
      applied += 1;
      return decision;
    });
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    assert.equal(applied, 0);
    await signal({
      serverUrl: harness.url,
      runId,
      name: "approval",
      payload: { approved: true },
      idempotencyKey: "approval-1",
    });
    const output = await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(applied, 1);
    assert.equal(typeof output === "object" && output !== null && !Array.isArray(output) && output.outcome, "signal");
    const payload = typeof output === "object" && output !== null && !Array.isArray(output) ? output.payload : null;
    assert.deepEqual(payload, { approved: true });
    const run = await harness.admin.query<{ state: string }>(`SELECT state FROM runs WHERE id = $1`, [runId]);
    assert.equal(run.rows[0]?.state, "completed");
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
  } finally {
    await harness.close();
  }
});

async function openHarness(): Promise<{ url: string; admin: pg.Client; close: () => Promise<void> }> {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    timerIntervalMs: 0,
  });
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

async function eventCount(admin: pg.Client, runId: string, type: string): Promise<string> {
  const rows = await admin.query<{ count: string }>(
    `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = $2`,
    [runId, type],
  );
  return rows.rows[0]?.count ?? "0";
}
