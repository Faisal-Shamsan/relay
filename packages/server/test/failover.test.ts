import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase } from "../src/testing.js";

const HASH = "a1".repeat(32);

test("F22 an unknown commit is resolved from the receipt and an open transaction does not survive disconnect", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  const open = new pg.Client({ connectionString: db.adminUrl });
  try {
    const sessionId = randomUUID();
    const runId = randomUUID();
    const operationId = randomUUID();
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
    const first = await app.query<{ result: { result: { ok: boolean } } }>(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7) AS result`,
      [operationId, sessionId, runId, fence, started.rows[0]!.result.attemptToken, JSON.stringify({ ok: true }), HASH],
    );
    const retry = await app.query<{ result: { result: { ok: boolean } } }>(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7) AS result`,
      [operationId, sessionId, runId, fence, started.rows[0]!.result.attemptToken, JSON.stringify({ ok: true }), HASH],
    );
    assert.deepEqual(retry.rows[0]?.result, first.rows[0]?.result);
    const completions = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completions.rows[0]?.count, 1);

    await open.connect();
    let markDead = () => {};
    const died = new Promise<void>((resolve) => {
      markDead = resolve;
    });
    open.on("error", () => markDead());
    await open.query("BEGIN");
    await open.query(
      `INSERT INTO run_events (run_id, seq, fence, actor, type, payload)
       VALUES ($1, 1000, 0, 'system', 'LeaseReleased', '{"schemaVersion":1}'::jsonb)`,
      [runId],
    );
    const pid = await open.query<{ pid: number }>(`SELECT pg_backend_pid() AS pid`);
    await admin.query(`SELECT pg_terminate_backend($1)`, [pid.rows[0]!.pid]);
    await died;
    const ghost = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM run_events WHERE run_id = $1 AND seq = 1000`,
      [runId],
    );
    assert.equal(ghost.rows[0]?.count, 0);
    const still = await admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(still.rows[0]?.count, 1);
  } finally {
    await open.end().catch(() => undefined);
    await app.end();
    await admin.end();
    await db.drop();
  }
});
