import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase } from "../src/testing.js";

const HASH = "ab".repeat(32);
const AFTER_INSERT = "transition.after-event-insert-before-projections";
const AFTER_PROJECTIONS = "transition.after-projections-before-queue";
const BEFORE_COMMIT = "transition.before-commit";

test("F16 finish-run failpoints roll back the event, projection, queue, and receipt", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    await assertFinishFunctionOrder(admin);

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
    await app.query(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
      [randomUUID(), sessionId, runId, fence, token, JSON.stringify({ ok: true }), HASH],
    );
    const before = await snapshot(admin, runId);
    assert.equal(before.state, "running");
    assert.equal(before.output, null);
    assert.equal(before.queueOwner, sessionId);
    assert.equal(before.stepCompleted.length, 1);
    assert.equal(before.stepCompleted[0]?.type, "StepCompleted");

    for (const hook of [AFTER_INSERT, AFTER_PROJECTIONS, BEFORE_COMMIT]) {
      await finishWithFailpoint(app, { sessionId, runId, fence }, hook, true);
      assert.deepEqual(await snapshot(admin, runId), before);
    }
    await finishWithFailpoint(app, { sessionId, runId, fence }, BEFORE_COMMIT, false);
    assert.deepEqual(await snapshot(admin, runId), before);

    const finishId = randomUUID();
    await app.query(`SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6)`, [
      finishId,
      sessionId,
      runId,
      fence,
      JSON.stringify({ ok: true }),
      HASH,
    ]);
    const committed = await admin.query<{ completed: string; seq: string; queued: string; receipts: string }>(
      `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'RunCompleted') AS completed,
              (SELECT next_event_seq::text FROM runs WHERE id = $1) AS seq,
              (SELECT count(*)::text FROM run_queue WHERE run_id = $1) AS queued,
              (SELECT count(*)::text FROM operation_receipts WHERE run_id = $1 AND operation_id = $2) AS receipts`,
      [runId, finishId],
    );
    assert.equal(committed.rows[0]?.completed, "1");
    assert.equal(committed.rows[0]?.seq, (BigInt(before.next_event_seq) + 1n).toString());
    assert.equal(committed.rows[0]?.queued, "0");
    assert.equal(committed.rows[0]?.receipts, "1");
    const after = await snapshot(admin, runId);
    assert.equal(after.receipts.length, before.receipts.length + 1);
    assert.deepEqual(after.stepCompleted, before.stepCompleted);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

async function finishWithFailpoint(
  app: pg.Client,
  run: { sessionId: string; runId: string; fence: string },
  hook: string,
  allow: boolean,
): Promise<void> {
  const operationId = randomUUID();
  await app.query("BEGIN");
  try {
    if (allow) {
      await app.query(`SELECT set_config('relay.allow_failpoints', '1', true)`);
    }
    await app.query(`SELECT set_config('relay.failpoint', $1, true)`, [hook]);
    await assert.rejects(
      () => app.query(`SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6)`, [
        operationId,
        run.sessionId,
        run.runId,
        run.fence,
        JSON.stringify({ ok: true }),
        HASH,
      ]),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        if (allow) {
          assert.equal(error.message, "FailpointAbort");
          assert.equal((error as { detail?: string }).detail, hook);
        } else {
          assert.equal(error.message, "failpoint activation is rejected");
        }
        return true;
      },
    );
  } finally {
    await app.query("ROLLBACK");
  }
}

async function assertFinishFunctionOrder(admin: pg.Client): Promise<void> {
  const result = await admin.query<{ src: string }>(
    `SELECT pg_get_functiondef('relay_finish_run(uuid,uuid,uuid,bigint,jsonb,text)'::regprocedure) AS src`,
  );
  const src = result.rows[0]!.src;
  precedes(src, "relay_alloc_events", "RunCompleted");
  precedes(src, "relay_alloc_events", AFTER_INSERT);
  precedes(src, "RunCompleted", AFTER_INSERT);
  precedes(src, AFTER_INSERT, "UPDATE runs");
  precedes(src, "UPDATE runs", AFTER_PROJECTIONS);
  precedes(src, AFTER_PROJECTIONS, "DELETE FROM run_queue");
  precedes(src, "INSERT INTO operation_receipts", BEFORE_COMMIT);
}

function precedes(source: string, earlier: string, later: string): void {
  const left = source.indexOf(earlier);
  const right = source.indexOf(later);
  assert.notEqual(left, -1, earlier);
  assert.notEqual(right, -1, later);
  assert.ok(left < right, `${earlier} before ${later}`);
}

interface FinishSnapshot {
  events: { seq: string; type: string; payload: unknown }[];
  next_event_seq: string;
  state: string;
  output: unknown;
  queueOwner: string | null;
  queueState: string | null;
  receipts: { operation_id: string; request_hash: string; response: unknown }[];
  stepCompleted: { seq: string; type: string; step_key: string; attempt: number; payload: unknown }[];
}

async function snapshot(admin: pg.Client, runId: string): Promise<FinishSnapshot> {
  const events = await admin.query<{ seq: string; type: string; payload: unknown }>(
    `SELECT seq::text, type, payload FROM run_events WHERE run_id = $1 ORDER BY seq`,
    [runId],
  );
  const run = await admin.query<{ state: string; output: unknown; seq: string; owner: string | null; queue_state: string | null }>(
    `SELECT r.state, r.output, r.next_event_seq::text AS seq, q.lease_owner::text AS owner, q.state AS queue_state
     FROM runs r LEFT JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
    [runId],
  );
  const receipts = await admin.query<{ operation_id: string; request_hash: string; response: unknown }>(
    `SELECT operation_id::text, request_hash, response
     FROM operation_receipts WHERE run_id = $1 ORDER BY operation_id`,
    [runId],
  );
  const stepCompleted = await admin.query<{ seq: string; type: string; step_key: string; attempt: number; payload: unknown }>(
    `SELECT seq::text, type, step_key, attempt, payload
     FROM run_events WHERE run_id = $1 AND type = 'StepCompleted' ORDER BY seq`,
    [runId],
  );
  const row = run.rows[0]!;
  return {
    events: events.rows,
    next_event_seq: row.seq,
    state: row.state,
    output: row.output,
    queueOwner: row.owner,
    queueState: row.queue_state,
    receipts: receipts.rows,
    stepCompleted: stepCompleted.rows,
  };
}
