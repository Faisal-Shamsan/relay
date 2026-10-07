import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { canonicalRelayJson, SEEDED_TENANT_ID, type Json } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

test("http completion receipt survives a later fence and a foreign version cannot lease", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 400, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const v1 = randomUUID();
    const v2 = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v2,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v2" }],
    });
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    assert.equal(created.status, 201);
    const runId = String(created.body.runId);

    const wrong = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v2,
    });
    assert.equal(wrong.status, 404);
    const still = await admin.query<{ state: string; fence: string }>(
      `SELECT state, current_fence::text AS fence FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(still.rows[0]?.state, "pending");
    assert.equal(still.rows[0]?.fence, "0");

    const lease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
    });
    assert.equal(lease.status, 200);
    assert.equal(lease.body.fence, "1");
    const history = await fetch(
      `${server.url}/worker/v1/runs/${runId}/history?workerSessionId=${v1}&after=0&through=${lease.body.historyWatermark}`,
    );
    assert.equal(history.status, 200);

    const started = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
      runId,
      fence: "1",
      intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
    });
    assert.equal(started.status, 200);
    const completion = {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
      runId,
      fence: "1",
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: 1,
        attemptToken: started.body.attemptToken,
        result: { ok: true },
      },
    };
    const completed = await post(server.url, "/worker/v1/events", completion);
    assert.equal(completed.status, 200);

    await waitUntilExpired(admin, runId);
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const v1Again = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1Again,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    const secondLease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1Again,
    });
    assert.equal(secondLease.body.fence, "2");

    const duplicate = await post(server.url, "/worker/v1/events", completion);
    assert.equal(duplicate.status, 200);
    assert.equal(canonicalRelayJson(duplicate.body as Json), canonicalRelayJson(completed.body as Json));
    const completions = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completions.rows[0]?.count, "1");

    const missingEvents = await fetch(`${server.url}/v1/runs/${runId}/events`);
    assert.equal(missingEvents.status, 404);
    const missingFailpoint = await fetch(`${server.url}/failpoint`);
    assert.equal(missingFailpoint.status, 404);

    const bad = await fetch(`${server.url}/v1/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"workflowName":"demo","workflowVersion":"demo-v1","input":{"a":1,"a":2}}',
    });
    assert.equal(bad.status, 422);
    const runs = await admin.query<{ count: string }>(`SELECT count(*)::text AS count FROM runs`);
    assert.equal(runs.rows[0]?.count, "1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

async function waitUntilExpired(admin: pg.Client, runId: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const row = await admin.query<{ due: boolean }>(
      `SELECT clock_timestamp() >= lease_expires_at AS due FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    if (row.rows[0]?.due) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("lease did not expire");
}
