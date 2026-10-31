import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";
import pg from "pg";
import { sha256Hex, SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, DEV_WORKER_KEY, startServer } from "relay-server";
import { cancel, defineWorkflow, runActivation } from "@relay/sdk";

const B_CLIENT = "relay_other_client_key";

test("redrive starts a new run and leaves the terminal run unchanged", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("save", async () => ({ ok: true }));
    return { saved: true };
  });
  try {
    const sourceId = await createRun(harness.url, { n: 1 });
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    const before = await snapshot(harness.admin, sourceId);

    const created = await redrive(harness.url, sourceId, { idempotencyKey: "again" });
    assert.equal(created.status, 201);
    const childId = created.body.runId;
    assert.equal(typeof childId, "string");
    if (childId === undefined) throw new Error("redrive did not return a run id");
    assert.notEqual(childId, sourceId);
    await assertChild(harness.admin, childId, sourceId, { n: 1 });
    assert.deepEqual(await snapshot(harness.admin, sourceId), before);
    assert.equal(await eventCount(harness.admin, sourceId), before.events);

    const replay = await redrive(harness.url, sourceId, { idempotencyKey: "again" });
    assert.equal(replay.status, 200);
    assert.equal(replay.body.runId, childId);
    assert.equal(await redriveCount(harness.admin, sourceId), "1");

    const conflict = await redrive(harness.url, sourceId, { idempotencyKey: "again", input: { n: 1 } });
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.code, "IdempotencyConflict");
    assert.equal(conflict.body.message, "redrive idempotency key was reused with a different body");
    assert.equal(await redriveCount(harness.admin, sourceId), "1");
    assert.deepEqual(await snapshot(harness.admin, sourceId), before);

    const rebuilt = await harness.admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_rebuild_projections($1) AS result`,
      [sourceId],
    );
    assert.equal(rebuilt.rows[0]?.result.ok, true);
    assert.equal(await redriveCount(harness.admin, sourceId), "1");
    assert.deepEqual(await snapshot(harness.admin, sourceId), before);

    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    const child = await fetch(`${harness.url}/v1/runs/${childId}`);
    assert.equal(child.status, 200);
    assert.equal((await child.json() as { state: string }).state, "completed");
    assert.deepEqual(await snapshot(harness.admin, sourceId), before);

    const overridden = await redrive(harness.url, sourceId, { idempotencyKey: "other", input: { n: 2 } });
    assert.equal(overridden.status, 201);
    const otherId = overridden.body.runId;
    if (otherId === undefined) throw new Error("redrive did not return a run id");
    assert.notEqual(otherId, childId);
    await assertChild(harness.admin, otherId, sourceId, { n: 2 });
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });

    const namespaced = await fetch(`${harness.url}/v1/runs`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": "again" },
      body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: { n: 1 } }),
    });
    assert.equal(namespaced.status, 201);
    const triggerId = String((await namespaced.json() as { runId: string }).runId);
    assert.notEqual(triggerId, childId);

    const pendingId = await createRun(harness.url, { n: 3 });
    const pending = await redrive(harness.url, pendingId, { idempotencyKey: "early" });
    assert.equal(pending.status, 404);
    assert.equal(pending.body.code, "InvalidRunState");
    assert.equal(pending.body.message, "run is not terminal");
    assert.equal(await redriveCount(harness.admin, pendingId), "0");

    const cancelledId = await createRun(harness.url, { n: 4 });
    const cancelledBefore = await snapshot(harness.admin, cancelledId);
    await cancel({ serverUrl: harness.url, runId: cancelledId });
    const cancelledSnap = await snapshot(harness.admin, cancelledId);
    assert.equal(cancelledSnap.state, "cancelled");
    const cancelledChild = await redrive(harness.url, cancelledId, { idempotencyKey: "once" });
    assert.equal(cancelledChild.status, 201);
    assert.notEqual(cancelledChild.body.runId, cancelledId);
    assert.deepEqual(await snapshot(harness.admin, cancelledId), cancelledSnap);
    assert.notDeepEqual(cancelledSnap, cancelledBefore);
    const queued = await harness.admin.query<{ state: string }>(
      `SELECT state FROM run_queue WHERE run_id = $1`,
      [cancelledChild.body.runId],
    );
    assert.equal(queued.rows[0]?.state, "ready");
  } finally {
    await harness.close();
  }
});

test("a missing run and another tenant's run are the same 404", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("save", async () => ({ ok: true }));
    return { saved: true };
  });
  try {
    const sourceId = await createRun(harness.url, { n: 1 });
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    const missing = await redrive(harness.url, crypto.randomUUID(), { idempotencyKey: "again" });
    const tenantB = crypto.randomUUID();
    await harness.admin.query(`INSERT INTO tenants (id, name) VALUES ($1, 'other')`, [tenantB]);
    await harness.admin.query(
      `INSERT INTO api_credentials (id, tenant_id, key_hash, kind, label)
       VALUES ($1, $2, $3, 'client', 'b-client')`,
      [crypto.randomUUID(), tenantB, sha256Hex(B_CLIENT)],
    );
    const foreign = await redrive(
      harness.url,
      sourceId,
      { idempotencyKey: "again" },
      { authorization: `Bearer ${B_CLIENT}` },
    );
    assert.equal(missing.status, 404);
    assert.equal(foreign.status, missing.status);
    assert.equal(foreign.body.code, missing.body.code);
    assert.equal(foreign.body.code, "InvalidRunState");
    assert.equal(foreign.body.message, missing.body.message);
    assert.equal(missing.body.message, "run not found");
    assert.equal(await redriveCount(harness.admin, sourceId), "0");

    const worker = await redrive(
      harness.url,
      sourceId,
      { idempotencyKey: "again" },
      { authorization: `Bearer ${DEV_WORKER_KEY}` },
    );
    assert.equal(worker.status, 401);
    assert.equal(await redriveCount(harness.admin, sourceId), "0");
  } finally {
    await harness.close();
  }
});

async function openHarness(): Promise<{
  url: string;
  admin: pg.Client;
  close: () => Promise<void>;
}> {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    timerIntervalMs: 0,
    heartbeatIntervalMs: 10_000,
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

async function createRun(url: string, input: { n: number }): Promise<string> {
  const created = await fetch(`${url}/v1/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input }),
  });
  assert.equal(created.status, 201);
  return String((await created.json() as { runId: string }).runId);
}

async function redrive(
  url: string,
  runId: string,
  body: { idempotencyKey: string; input?: { n: number } },
  headers: Record<string, string> = {},
): Promise<{ status: number; body: { runId?: string; code?: string; message?: string } }> {
  const response = await fetch(`${url}/v1/runs/${runId}/redrive`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  return {
    status: response.status,
    body: await response.json() as { runId?: string; code?: string; message?: string },
  };
}

async function snapshot(admin: pg.Client, runId: string): Promise<{
  state: string;
  output: unknown;
  error: unknown;
  nextEventSeq: string;
  events: string;
}> {
  const rows = await admin.query<{
    state: string;
    output: unknown;
    error: unknown;
    next_event_seq: string;
    events: string;
  }>(
    `SELECT state, output, error, next_event_seq::text AS next_event_seq,
            (SELECT count(*)::text FROM run_events e WHERE e.run_id = runs.id) AS events
     FROM runs WHERE id = $1`,
    [runId],
  );
  const row = rows.rows[0];
  if (!row) throw new Error("run missing");
  return {
    state: row.state,
    output: row.output,
    error: row.error,
    nextEventSeq: row.next_event_seq,
    events: row.events,
  };
}

async function eventCount(admin: pg.Client, runId: string): Promise<string> {
  const rows = await admin.query<{ count: string }>(
    `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1`,
    [runId],
  );
  return rows.rows[0]?.count ?? "0";
}

async function redriveCount(admin: pg.Client, runId: string): Promise<string> {
  const rows = await admin.query<{ count: string }>(
    `SELECT count(*)::text AS count FROM run_redrives WHERE source_run_id = $1`,
    [runId],
  );
  return rows.rows[0]?.count ?? "0";
}

async function assertChild(admin: pg.Client, childId: string, sourceId: string, input: { n: number }): Promise<void> {
  const rows = await admin.query<{
    state: string;
    input: { n: number };
    idempotency_key: string | null;
    same_version: boolean;
    same_tenant: boolean;
    queue_state: string | null;
    events: string;
    event_type: string | null;
  }>(
    `SELECT r.state, r.input, r.idempotency_key,
            r.workflow_version_id = s.workflow_version_id AS same_version,
            r.tenant_id = s.tenant_id AS same_tenant,
            q.state AS queue_state,
            (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id) AS events,
            (SELECT type FROM run_events e WHERE e.run_id = r.id ORDER BY e.seq LIMIT 1) AS event_type
     FROM runs r
     JOIN runs s ON s.id = $2
     LEFT JOIN run_queue q ON q.run_id = r.id
     WHERE r.id = $1`,
    [childId, sourceId],
  );
  const row = rows.rows[0];
  assert.equal(row?.state, "pending");
  assert.deepEqual(row?.input, input);
  assert.equal(row?.idempotency_key, null);
  assert.equal(row?.same_version, true);
  assert.equal(row?.same_tenant, true);
  assert.equal(row?.queue_state, "ready");
  assert.equal(row?.events, "1");
  assert.equal(row?.event_type, "RunCreated");
}
