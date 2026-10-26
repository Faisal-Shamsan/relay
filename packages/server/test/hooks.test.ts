import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase } from "../src/testing.js";

const HASH = "cd".repeat(32);

test("phase 1 failpoint hooks roll back the open transaction", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const sources = await admin.query<{ name: string; src: string }>(
      `SELECT p.proname AS name, pg_get_functiondef(p.oid) AS src
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'public'
         AND p.proname IN ('relay_grant_lease', 'relay_heartbeat', 'relay_declare_and_start', 'relay_declare_parallel', 'relay_complete_step', 'relay_record_helper')`,
    );
    const byName = new Map(sources.rows.map((row) => [row.name, row.src]));
    assert.equal(byName.get("relay_grant_lease")?.includes("lease.after-lock-before-validation"), true);
    assert.equal(byName.get("relay_grant_lease")?.includes("lease.after-update-before-commit"), true);
    assert.equal(byName.get("relay_heartbeat")?.includes("heartbeat.after-update-before-commit"), true);
    assert.equal(byName.get("relay_declare_and_start")?.includes("command.after-declaration-before-commit"), true);
    assert.equal(byName.get("relay_declare_parallel")?.includes("command.after-declaration-before-commit"), true);
    assert.equal(byName.get("relay_complete_step")?.includes("step.before-completion-commit"), true);
    assert.equal(byName.get("relay_record_helper")?.includes("command.after-declaration-before-commit"), true);

    const sessionId = randomUUID();
    await app.query(`SELECT relay_register_worker($1,$2,$3,$4,1,$5::jsonb)`, [
      SEEDED_TENANT_ID,
      sessionId,
      randomUUID(),
      HASH,
      JSON.stringify([{ name: "demo", version: "demo-v1" }]),
    ]);
    const runId = await createRun(app);
    const lease = await app.query<{ result: { fence: string } }>(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
      [sessionId, randomUUID(), HASH],
    );
    const fence = lease.rows[0]!.result.fence;
    const before = await admin.query<{ deadline: string; commands: string; completed: string }>(
      `SELECT q.lease_expires_at::text AS deadline,
              (SELECT count(*)::text FROM commands WHERE run_id = $1) AS commands,
              (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'StepCompleted') AS completed
       FROM run_queue q WHERE q.run_id = $1`,
      [runId],
    );

    await expectAbort(app, "heartbeat.after-update-before-commit", true, () => app.query(
      `SELECT relay_heartbeat($1,$2,$3::bigint,$4,$5, interval '30 seconds')`,
      [sessionId, runId, fence, randomUUID(), HASH],
    ));
    await expectAbort(app, "heartbeat.after-update-before-commit", false, () => app.query(
      `SELECT relay_heartbeat($1,$2,$3::bigint,$4,$5, interval '30 seconds')`,
      [sessionId, runId, fence, randomUUID(), HASH],
    ));
    await expectAbort(app, "command.after-declaration-before-commit", true, () => app.query(
      `SELECT relay_declare_and_start($1,$2,$3,$4::bigint,'only',0,$5)`,
      [randomUUID(), sessionId, runId, fence, HASH],
    ));
    const afterDeclareAbort = await admin.query<{ commands: string; deadline: string }>(
      `SELECT (SELECT count(*)::text FROM commands WHERE run_id = $1) AS commands,
              q.lease_expires_at::text AS deadline
       FROM run_queue q WHERE q.run_id = $1`,
      [runId],
    );
    assert.equal(afterDeclareAbort.rows[0]?.commands, "0");
    assert.equal(afterDeclareAbort.rows[0]?.deadline, before.rows[0]?.deadline);

    const started = await app.query<{ result: { attemptToken: string } }>(
      `SELECT relay_declare_and_start($1,$2,$3,$4::bigint,'only',0,$5) AS result`,
      [randomUUID(), sessionId, runId, fence, HASH],
    );
    await expectAbort(app, "command.after-declaration-before-commit", true, () => app.query(
      `SELECT relay_record_helper($1,$2,$3,$4::bigint,'now','report-time',1,$5)`,
      [randomUUID(), sessionId, runId, fence, HASH],
    ));
    const afterHelperAbort = await admin.query<{ commands: string }>(
      `SELECT count(*)::text AS commands FROM commands WHERE run_id = $1`,
      [runId],
    );
    assert.equal(afterHelperAbort.rows[0]?.commands, "1");
    await expectAbort(app, "step.before-completion-commit", true, () => app.query(
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`,
      [randomUUID(), sessionId, runId, fence, started.rows[0]!.result.attemptToken, JSON.stringify({ ok: true }), HASH],
    ));
    const completed = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completed.rows[0]?.count, before.rows[0]?.completed);

    const locked = await createRun(app);
    await expectAbort(app, "lease.after-lock-before-validation", true, () => app.query(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds')`,
      [sessionId, randomUUID(), HASH],
    ));
    await expectAbort(app, "lease.after-update-before-commit", true, () => app.query(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds')`,
      [sessionId, randomUUID(), HASH],
    ));
    const waiting = await admin.query<{ state: string; fence: string; queue: string; grants: string }>(
      `SELECT r.state, r.current_fence::text AS fence, q.state AS queue,
              (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id AND e.type = 'LeaseGranted') AS grants
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [locked],
    );
    assert.equal(waiting.rows[0]?.state, "pending");
    assert.equal(waiting.rows[0]?.fence, "0");
    assert.equal(waiting.rows[0]?.queue, "ready");
    assert.equal(waiting.rows[0]?.grants, "0");
    const consumed = await app.query<{ result: { runId: string } }>(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
      [sessionId, randomUUID(), HASH],
    );
    assert.equal(consumed.rows[0]?.result.runId, locked);

    const parallel = await createRun(app);
    const parallelLease = await app.query<{ result: { runId: string; fence: string } }>(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
      [sessionId, randomUUID(), HASH],
    );
    assert.equal(parallelLease.rows[0]?.result.runId, parallel);
    await expectAbort(app, "command.after-declaration-before-commit", true, () => app.query(
      `SELECT relay_declare_parallel($1,$2,$3,$4::bigint,'group',0,$5::jsonb,2,$6)`,
      [randomUUID(), sessionId, parallel, parallelLease.rows[0]!.result.fence, JSON.stringify(["a", "b"]), HASH],
    ));
    const groups = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM commands WHERE run_id = $1`,
      [parallel],
    );
    assert.equal(groups.rows[0]?.count, "0");
    await app.query(
      `SELECT relay_declare_parallel($1,$2,$3,$4::bigint,'group',0,$5::jsonb,2,$6)`,
      [randomUUID(), sessionId, parallel, parallelLease.rows[0]!.result.fence, JSON.stringify(["a", "b"]), HASH],
    );
    const verified = await app.query<{ result: { ok: boolean; mismatches: unknown[] } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [parallel],
    );
    assert.equal(verified.rows[0]?.result.ok, true, JSON.stringify(verified.rows[0]?.result.mismatches));
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

async function createRun(app: pg.Client): Promise<string> {
  const runId = randomUUID();
  await app.query(`SELECT relay_create_run($1,'demo','demo-v1',$2::jsonb,$3,$4)`, [
    SEEDED_TENANT_ID,
    JSON.stringify({ n: 1 }),
    HASH,
    runId,
  ]);
  return runId;
}

async function expectAbort(app: pg.Client, hook: string, allow: boolean, call: () => Promise<unknown>): Promise<void> {
  await app.query("BEGIN");
  try {
    if (allow) await app.query(`SELECT set_config('relay.allow_failpoints', '1', true)`);
    await app.query(`SELECT set_config('relay.failpoint', $1, true)`, [hook]);
    await assert.rejects(call, (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, allow ? "FailpointAbort" : "failpoint activation is rejected");
      if (allow) assert.equal((error as { detail?: string }).detail, hook);
      return true;
    });
  } finally {
    await app.query("ROLLBACK");
  }
}
