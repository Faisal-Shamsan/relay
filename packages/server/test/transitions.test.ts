import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase } from "../src/testing.js";

const HASH = "ab".repeat(32);

test("grant deadline uses post-lock clock_timestamp and expire leaves the fence", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const sessionId = randomUUID();
    const runId = randomUUID();
    await app.query(`SELECT relay_register_worker($1,$2,$3,$4,1,$5::jsonb)`, [
      SEEDED_TENANT_ID,
      sessionId,
      randomUUID(),
      HASH,
      JSON.stringify([{ name: "demo", version: "demo-v1" }]),
    ]);
    await app.query(`SELECT relay_create_run($1,'demo','demo-v1',$2::jsonb,$3,$4)`, [
      SEEDED_TENANT_ID,
      JSON.stringify({ n: 1 }),
      HASH,
      runId,
    ]);

    await admin.query("BEGIN");
    await admin.query("SELECT run_id FROM run_queue WHERE run_id = $1 FOR UPDATE", [runId]);
    const grant = app.query(`SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`, [
      sessionId,
      randomUUID(),
      HASH,
    ]);
    const waitDeadline = Date.now() + 5_000;
    let waiting = false;
    while (Date.now() < waitDeadline) {
      const locks = await admin.query(
        `SELECT 1 FROM pg_stat_activity WHERE query LIKE '%relay_grant_lease%' AND wait_event_type = 'Lock'`,
      );
      if ((locks.rowCount ?? 0) > 0) {
        waiting = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 15));
    }
    assert.equal(waiting, true);
    const before = await admin.query<{ t: Date }>("SELECT clock_timestamp() AS t");
    await admin.query("SELECT pg_sleep(0.4)");
    const after = await admin.query<{ t: Date }>("SELECT clock_timestamp() AS t");
    await admin.query("COMMIT");
    await grant;

    const row = await admin.query<{ db_now: Date; lease_expires_at: Date; fence: string }>(
      `SELECT (e.payload->>'databaseNow')::timestamptz AS db_now,
              q.lease_expires_at,
              r.current_fence::text AS fence
       FROM run_events e
       JOIN run_queue q ON q.run_id = e.run_id
       JOIN runs r ON r.id = e.run_id
       WHERE e.run_id = $1 AND e.type = 'LeaseGranted'`,
      [runId],
    );
    const dbNow = row.rows[0]!.db_now.getTime();
    assert.equal(row.rows[0]!.lease_expires_at.getTime() - dbNow, 30_000);
    assert.ok(dbNow > before.rows[0]!.t.getTime() + 200);
    assert.ok(dbNow >= after.rows[0]!.t.getTime() - 50);
    assert.equal(row.rows[0]!.fence, "1");

    await assert.rejects(
      () => admin.query(`SELECT relay_expire_lease($1)`, [runId]),
      (error: { message?: string }) => error.message === "NotExpired",
    );
    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`, [runId]);
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const expired = await admin.query<{ state: string; queue: string; fence: string; owner: string | null }>(
      `SELECT r.state, q.state AS queue, r.current_fence::text AS fence, q.lease_owner::text AS owner
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(expired.rows[0]?.state, "pending");
    assert.equal(expired.rows[0]?.queue, "ready");
    assert.equal(expired.rows[0]?.fence, "1");
    assert.equal(expired.rows[0]?.owner, null);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

test("completion and finish roll back together, and a duplicate receipt does not write", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const sessionId = randomUUID();
    const runId = randomUUID();
    await app.query(`SELECT relay_register_worker($1,$2,$3,$4,1,$5::jsonb)`, [
      SEEDED_TENANT_ID,
      sessionId,
      randomUUID(),
      HASH,
      JSON.stringify([{ name: "demo", version: "demo-v1" }]),
    ]);
    await app.query(`SELECT relay_create_run($1,'demo','demo-v1',$2::jsonb,$3,$4)`, [
      SEEDED_TENANT_ID,
      JSON.stringify({ n: 1 }),
      HASH,
      runId,
    ]);
    const lease = await app.query<{ result: { fence: string } }>(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
      [sessionId, randomUUID(), HASH],
    );
    const fence = lease.rows[0]!.result.fence;
    const started = await app.query<{ result: { attemptToken: string } }>(
      `SELECT relay_declare_and_start($1,$2,$3,$4::bigint,'only',0,$5) AS result`,
      [randomUUID(), sessionId, runId, fence, HASH],
    );
    const token = started.rows[0]!.result.attemptToken;
    const before = await counts(admin, runId);

    const operationId = randomUUID();
    await app.query("BEGIN");
    await app.query(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
      [operationId, sessionId, runId, fence, token, JSON.stringify({ ok: true }), HASH],
    );
    await app.query("ROLLBACK");
    assert.deepEqual(await counts(admin, runId), before);
    const step = await admin.query<{ state: string }>(`SELECT state FROM steps WHERE run_id = $1`, [runId]);
    assert.equal(step.rows[0]?.state, "running");

    await app.query(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
      [operationId, sessionId, runId, fence, token, JSON.stringify({ ok: true }), HASH],
    );
    const afterComplete = await counts(admin, runId);
    await assert.rejects(
      () => app.query(
        `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
        [operationId, sessionId, runId, fence, token, JSON.stringify({ ok: true }), "cd".repeat(32)],
      ),
      (error: { message?: string }) => error.message === "IdempotencyConflict",
    );
    await app.query(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
      [operationId, sessionId, runId, fence, token, JSON.stringify({ ok: true }), HASH],
    );
    assert.deepEqual(await counts(admin, runId), afterComplete);

    const finishId = randomUUID();
    await app.query("BEGIN");
    await app.query(`SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6)`, [
      finishId,
      sessionId,
      runId,
      fence,
      JSON.stringify({ ok: true }),
      HASH,
    ]);
    await app.query("ROLLBACK");
    const queue = await admin.query(`SELECT state FROM run_queue WHERE run_id = $1`, [runId]);
    assert.equal(queue.rows[0]?.state, "leased");
    const run = await admin.query<{ state: string }>(`SELECT state FROM runs WHERE id = $1`, [runId]);
    assert.equal(run.rows[0]?.state, "running");

    await assert.rejects(
      () => app.query(`INSERT INTO run_events (run_id, seq, fence, actor, type, payload) VALUES ($1, 99, 0, 'system', 'RunCreated', '{}'::jsonb)`, [runId]),
      (error: { code?: string }) => error.code === "42501",
    );
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

async function counts(admin: pg.Client, runId: string): Promise<{ events: string; seq: string; receipts: string }> {
  const result = await admin.query<{ events: string; seq: string; receipts: string }>(
    `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1) AS events,
            (SELECT next_event_seq::text FROM runs WHERE id = $1) AS seq,
            (SELECT count(*)::text FROM operation_receipts WHERE run_id = $1) AS receipts`,
    [runId],
  );
  return result.rows[0]!;
}
