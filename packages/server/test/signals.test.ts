import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { sha256Hex, SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, DEV_CLIENT_KEY, DEV_WORKER_KEY, startServer } from "../src/index.js";

test("an early signal is stored once, replayed, and rebuilt from history", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    timerIntervalMs: 0,
    streamRetentionIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const tenantB = randomUUID();
  try {
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    }, { authorization: `Bearer ${DEV_WORKER_KEY}` });
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    const runId = String(created.body.runId);
    const first = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { ok: true },
      idempotencyKey: "approve-1",
    });
    assert.equal(first.status, 201);
    const replay = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { ok: true },
      idempotencyKey: "approve-1",
    });
    assert.equal(replay.status, 200);
    assert.equal(replay.body.ingressSeq, first.body.ingressSeq);
    const conflict = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { ok: false },
      idempotencyKey: "approve-1",
    });
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.code, "IdempotencyConflict");
    const second = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { n: 2 },
      idempotencyKey: "approve-2",
    });
    assert.equal(second.status, 201);

    const stored = await admin.query<{ n: string; consumed: string | null }>(
      `SELECT count(*)::text AS n, bool_or(consumed_by_command_pos IS NOT NULL)::text AS consumed
       FROM signal_inbox WHERE run_id = $1`,
      [runId],
    );
    assert.equal(stored.rows[0]?.n, "2");
    assert.equal(stored.rows[0]?.consumed, "false");

    await admin.query(`INSERT INTO tenants (id, name) VALUES ($1, 'other')`, [tenantB]);
    await admin.query(
      `INSERT INTO api_credentials (id, tenant_id, key_hash, kind, label)
       VALUES ($1, $2, $3, 'client', 'b')`,
      [randomUUID(), tenantB, sha256Hex("relay_other_client_key")],
    );
    const foreign = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { n: 3 },
      idempotencyKey: "approve-3",
    }, { authorization: "Bearer relay_other_client_key" });
    assert.equal(foreign.status, 404);
    assert.equal(foreign.body.message, "run not found");

    const before = await coordination(admin, runId);
    await admin.query(`UPDATE signal_inbox SET payload = '{"tampered":true}'::jsonb WHERE run_id = $1`, [runId]);
    const rebuilt = await admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_rebuild_projections($1) AS result`,
      [runId],
    );
    assert.equal(rebuilt.rows[0]?.result.ok, true);
    const restored = await admin.query<{ payload: { ok?: boolean } }>(
      `SELECT payload FROM signal_inbox WHERE run_id = $1 AND idempotency_key = 'approve-1'`,
      [runId],
    );
    assert.equal(restored.rows[0]?.payload.ok, true);
    const after = await coordination(admin, runId);
    assert.deepEqual(after, before);

    await admin.query(
      `UPDATE runs SET state = 'completed', output = '{"ok":true}'::jsonb, completed_at = clock_timestamp() WHERE id = $1`,
      [runId],
    );
    const events = await admin.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM run_events WHERE run_id = $1 AND type = 'SignalReceived'`,
      [runId],
    );
    const late = await post(server.url, `/v1/runs/${runId}/signal`, {
      name: "approval",
      payload: { n: 4 },
      idempotencyKey: "approve-4",
    });
    assert.equal(late.status, 404);
    assert.equal(late.body.message, "run is terminal");
    const eventsAfter = await admin.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM run_events WHERE run_id = $1 AND type = 'SignalReceived'`,
      [runId],
    );
    assert.equal(eventsAfter.rows[0]?.n, events.rows[0]?.n);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function coordination(admin: pg.Client, runId: string): Promise<{ fence: string; queue: string; receipts: string }> {
  const row = await admin.query<{ fence: string; queue: string; receipts: string }>(
    `SELECT r.current_fence::text AS fence,
            (SELECT state FROM run_queue q WHERE q.run_id = r.id) AS queue,
            (SELECT count(*)::text FROM operation_receipts o WHERE o.run_id = r.id) AS receipts
     FROM runs r WHERE r.id = $1`,
    [runId],
  );
  return row.rows[0]!;
}

async function post(
  url: string,
  path: string,
  body: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${DEV_CLIENT_KEY}`,
      ...headers,
    },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}
