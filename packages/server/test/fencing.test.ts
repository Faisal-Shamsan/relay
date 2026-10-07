import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { canonicalRelayJson, SEEDED_TENANT_ID, type Json } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

const HASH = "ab".repeat(32);

test("F03 expired lease cannot be renewed or written before the reaper", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const owner = randomUUID();
    const other = randomUUID();
    await post(server.url, "/worker/v1/register", sessionBody(owner));
    await post(server.url, "/worker/v1/register", sessionBody(other));
    const runId = await createRun(server.url);
    const lease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
    });
    assert.equal(lease.status, 200);
    const fence = String(lease.body.fence);
    const started = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      runId,
      fence,
      intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
    });
    assert.equal(started.status, 200);
    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1`, [runId]);

    const heartbeat = await post(server.url, "/worker/v1/heartbeat", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      runId,
      fence,
      clientNow: "2000-01-01T00:00:00.000Z",
    });
    assert.equal(heartbeat.status, 409);
    assert.equal(heartbeat.body.code, "LeaseLost");
    const completion = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      runId,
      fence,
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: 1,
        attemptToken: started.body.attemptToken,
        result: { ok: true },
      },
    });
    assert.equal(completion.status, 409);
    assert.equal(completion.body.code, "LeaseLost");

    const row = await admin.query<{ fence: string; owner: string; state: string; completed: string }>(
      `SELECT r.current_fence::text AS fence, q.lease_owner::text AS owner, q.state,
              (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id AND e.type = 'StepCompleted') AS completed
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(row.rows[0]!.fence, "1");
    assert.equal(row.rows[0]!.owner, owner);
    assert.equal(row.rows[0]!.state, "leased");
    assert.equal(row.rows[0]!.completed, "0");

    const second = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: other,
    });
    assert.equal(second.status, 404);
    assert.equal(second.body.code, "InvalidRunState");
    const still = await admin.query<{ owner: string; state: string }>(
      `SELECT lease_owner::text AS owner, state FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    assert.equal(still.rows[0]!.owner, owner);
    assert.equal(still.rows[0]!.state, "leased");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F04 a request blocked on the run lock rejects once the deadline has passed", async () => {
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
    const grant = await app.query<{ result: { fence: string } }>(
      `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
      [sessionId, randomUUID(), HASH],
    );
    const started = await app.query<{ result: { attemptToken: string } }>(
      `SELECT relay_declare_and_start($1,$2,$3,1,'only',0,$4) AS result`,
      [randomUUID(), sessionId, runId, HASH],
    );

    await admin.query("BEGIN");
    await admin.query(`SELECT id FROM runs WHERE id = $1 FOR UPDATE`, [runId]);
    const heartbeat = app.query(`SELECT relay_heartbeat($1,$2,1,$3,$4, interval '30 seconds') AS result`, [
      sessionId,
      runId,
      randomUUID(),
      HASH,
    ]);
    await waitForLock(admin, "relay_heartbeat");
    const stamped = await admin.query<{ lease_expires_at: Date }>(
      `UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1 RETURNING lease_expires_at`,
      [runId],
    );
    await admin.query("COMMIT");
    await assert.rejects(heartbeat, (error: { message?: string }) => error.message === "LeaseLost");
    const afterHeartbeat = await admin.query<{ lease_expires_at: Date }>(
      `SELECT lease_expires_at FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    assert.equal(afterHeartbeat.rows[0]!.lease_expires_at.getTime(), stamped.rows[0]!.lease_expires_at.getTime());

    await admin.query(
      `UPDATE run_queue SET lease_expires_at = clock_timestamp() + interval '30 seconds' WHERE run_id = $1`,
      [runId],
    );
    await admin.query("BEGIN");
    await admin.query(`SELECT id FROM runs WHERE id = $1 FOR UPDATE`, [runId]);
    const completion = app.query(
      `SELECT relay_complete_step($1,$2,$3,1,'only',1,$4,'{"ok":true}'::jsonb,$5) AS result`,
      [randomUUID(), sessionId, runId, started.rows[0]!.result.attemptToken, HASH],
    );
    await waitForLock(admin, "relay_complete_step");
    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1`, [runId]);
    await admin.query("COMMIT");
    await assert.rejects(completion, (error: { message?: string }) => error.message === "LeaseLost");
    const completed = await admin.query<{ count: string; state: string }>(
      `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'StepCompleted') AS count,
              (SELECT state FROM step_attempts WHERE run_id = $1 AND attempt = 1) AS state`,
      [runId],
    );
    assert.equal(completed.rows[0]!.count, "0");
    assert.equal(completed.rows[0]!.state, "running");
    assert.equal(grant.rows[0]!.result.fence, "1");
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

test("F07 concurrent grants produce one owner and increasing fences", async () => {
  const db = await createTestDatabase();
  const admin = new pg.Client({ connectionString: db.adminUrl });
  const first = new pg.Client({ connectionString: db.appUrl });
  const second = new pg.Client({ connectionString: db.appUrl });
  await admin.connect();
  await first.connect();
  await second.connect();
  try {
    const sessionA = randomUUID();
    const sessionB = randomUUID();
    const runId = randomUUID();
    for (const sessionId of [sessionA, sessionB]) {
      await first.query(`SELECT relay_register_worker($1,$2,$3,$4,1,$5::jsonb)`, [
        SEEDED_TENANT_ID,
        sessionId,
        randomUUID(),
        HASH,
        JSON.stringify([{ name: "demo", version: "demo-v1" }]),
      ]);
    }
    await first.query(`SELECT relay_create_run($1,'demo','demo-v1',$2::jsonb,$3,$4)`, [
      SEEDED_TENANT_ID,
      JSON.stringify({ n: 1 }),
      HASH,
      runId,
    ]);

    await assertOneGrant(first, second, sessionA, sessionB);
    let fence = await admin.query<{ fence: string; owners: string }>(
      `SELECT current_fence::text AS fence,
              (SELECT count(*)::text FROM run_queue WHERE run_id = $1 AND state = 'leased') AS owners
       FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(fence.rows[0]!.fence, "1");
    assert.equal(fence.rows[0]!.owners, "1");

    await admin.query(
      `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
      [runId],
    );
    await reapAll(first);
    const expired = await admin.query<{ state: string; fence: string; expired: string }>(
      `SELECT q.state, r.current_fence::text AS fence,
              (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id AND e.type = 'LeaseExpired') AS expired
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(expired.rows[0]!.state, "ready");
    assert.equal(expired.rows[0]!.fence, "1");
    assert.equal(expired.rows[0]!.expired, "1");

    await assertOneGrant(first, second, sessionA, sessionB);
    fence = await admin.query<{ fence: string; owners: string }>(
      `SELECT current_fence::text AS fence,
              (SELECT count(*)::text FROM run_queue WHERE run_id = $1 AND state = 'leased') AS owners
       FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(fence.rows[0]!.fence, "2");
    assert.equal(fence.rows[0]!.owners, "1");
    const grants = await admin.query<{ fences: string; rows: string }>(
      `SELECT count(DISTINCT fence)::text AS fences, count(*)::text AS rows
       FROM run_events WHERE run_id = $1 AND type = 'LeaseGranted'`,
      [runId],
    );
    assert.equal(grants.rows[0]!.fences, "2");
    assert.equal(grants.rows[0]!.rows, "2");
  } finally {
    await first.end();
    await second.end();
    await admin.end();
    await db.drop();
  }
});

test("F20 client clocks do not choose the lease deadline", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 400,
    heartbeatIntervalMs: 10_000,
    reaperIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const sessionId = randomUUID();
    await post(server.url, "/worker/v1/register", sessionBody(sessionId));
    const runId = await createRun(server.url);
    const lease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      leaseDurationMs: 999_999_999,
      clientNow: "2099-01-01T00:00:00.000Z",
    });
    assert.equal(lease.status, 200);
    assert.equal(lease.body.leaseDurationMs, 400);
    assert.equal(lease.body.heartbeatIntervalMs, 10_000);
    const granted = await admin.query<{ ms: number }>(
      `SELECT round((EXTRACT(EPOCH FROM (q.lease_expires_at - (e.payload->>'databaseNow')::timestamptz)) * 1000))::int AS ms
       FROM run_events e JOIN run_queue q ON q.run_id = e.run_id
       WHERE e.run_id = $1 AND e.type = 'LeaseGranted'`,
      [runId],
    );
    assert.equal(granted.rows[0]!.ms, 400);

    const operationId = randomUUID();
    const heartbeatBody = {
      protocolVersion: 1,
      operationId,
      workerSessionId: sessionId,
      runId,
      fence: lease.body.fence,
      leaseExpiresAt: "2099-01-01T00:00:00.000Z",
      clientNow: "2000-01-01T00:00:00.000Z",
    };
    const heartbeat = await post(server.url, "/worker/v1/heartbeat", heartbeatBody);
    assert.equal(heartbeat.status, 200);
    const renewed = await admin.query<{ ms: number; expires: Date }>(
      `SELECT round((EXTRACT(EPOCH FROM (lease_expires_at - $2::timestamptz)) * 1000))::int AS ms,
              lease_expires_at AS expires
       FROM run_queue WHERE run_id = $1`,
      [runId, heartbeat.body.databaseNow],
    );
    assert.equal(renewed.rows[0]!.ms, 400);
    assert.ok(renewed.rows[0]!.expires.getTime() < Date.parse("2090-01-01T00:00:00.000Z"));

    const duplicate = await post(server.url, "/worker/v1/heartbeat", heartbeatBody);
    assert.equal(duplicate.status, 200);
    assert.equal(canonicalRelayJson(duplicate.body as Json), canonicalRelayJson(heartbeat.body as Json));
    const unchanged = await admin.query<{ expires: Date }>(
      `SELECT lease_expires_at AS expires FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    assert.equal(unchanged.rows[0]!.expires.getTime(), renewed.rows[0]!.expires.getTime());

    const conflict = await post(server.url, "/worker/v1/heartbeat", { ...heartbeatBody, clientNow: "1999-01-01T00:00:00.000Z" });
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.code, "IdempotencyConflict");

    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1`, [runId]);
    const late = await post(server.url, "/worker/v1/heartbeat", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: lease.body.fence,
      leaseExpiresAt: "2099-01-01T00:00:00.000Z",
    });
    assert.equal(late.status, 409);
    assert.equal(late.body.code, "LeaseLost");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("a repeated release returns the stored receipt", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const sessionId = randomUUID();
    await post(server.url, "/worker/v1/register", sessionBody(sessionId));
    const runId = await createRun(server.url);
    const lease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
    });
    const body = {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: lease.body.fence,
    };
    const released = await post(server.url, "/worker/v1/release", body);
    assert.equal(released.status, 200);
    const duplicate = await post(server.url, "/worker/v1/release", body);
    assert.equal(duplicate.status, 200);
    assert.equal(canonicalRelayJson(duplicate.body as Json), canonicalRelayJson(released.body as Json));
    const row = await admin.query<{ released: string; state: string; fence: string }>(
      `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'LeaseReleased') AS released,
              q.state, r.current_fence::text AS fence
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(row.rows[0]!.released, "1");
    assert.equal(row.rows[0]!.state, "ready");
    assert.equal(row.rows[0]!.fence, "1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("the reaper expires only due leases and leaves a live owner", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const dueSession = randomUUID();
    const liveSession = randomUUID();
    const dueRun = randomUUID();
    const liveRun = randomUUID();
    for (const [sessionId, runId] of [[dueSession, dueRun], [liveSession, liveRun]] as const) {
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
      await app.query(`SELECT relay_grant_lease($1,$2,$3, interval '30 seconds')`, [
        sessionId,
        randomUUID(),
        HASH,
      ]);
    }
    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1`, [dueRun]);
    await reapAll(app);
    const rows = await admin.query<{ run_id: string; state: string; fence: string; expired: string }>(
      `SELECT r.id::text AS run_id, q.state, r.current_fence::text AS fence,
              (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id AND e.type = 'LeaseExpired') AS expired
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id IN ($1, $2)
       ORDER BY r.id = $1 DESC`,
      [dueRun, liveRun],
    );
    assert.equal(rows.rows[0]!.state, "ready");
    assert.equal(rows.rows[0]!.fence, "1");
    assert.equal(rows.rows[0]!.expired, "1");
    assert.equal(rows.rows[1]!.state, "leased");
    assert.equal(rows.rows[1]!.fence, "1");
    assert.equal(rows.rows[1]!.expired, "0");
    await assert.rejects(
      () => app.query(`SELECT relay_expire_lease($1)`, [liveRun]),
      (error: { code?: string }) => error.code === "42501",
    );
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

test("F02 a stale owner cannot complete, heartbeat, or release after a new grant", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 400,
    reaperIntervalMs: 50,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const ownerA = randomUUID();
    const ownerB = randomUUID();
    await post(server.url, "/worker/v1/register", sessionBody(ownerA));
    await post(server.url, "/worker/v1/register", sessionBody(ownerB));
    const runId = await createRun(server.url);
    const leaseA = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerA,
    });
    assert.equal(leaseA.body.fence, "1");
    const started = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerA,
      runId,
      fence: "1",
      intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
    });
    assert.equal(started.status, 200);
    await waitUntilExpired(admin, runId);
    await waitForExpiredEvent(admin, runId);

    const leaseB = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerB,
    });
    assert.equal(leaseB.status, 200);
    assert.equal(leaseB.body.fence, "2");

    const completion = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerA,
      runId,
      fence: "1",
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: started.body.attempt,
        attemptToken: started.body.attemptToken,
        result: { ok: true },
      },
    });
    const heartbeat = await post(server.url, "/worker/v1/heartbeat", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerA,
      runId,
      fence: "1",
    });
    const release = await post(server.url, "/worker/v1/release", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerA,
      runId,
      fence: "1",
    });
    assert.equal(completion.status, 409);
    assert.equal(completion.body.code, "LeaseLost");
    assert.equal(heartbeat.status, 409);
    assert.equal(heartbeat.body.code, "LeaseLost");
    assert.equal(release.status, 409);
    assert.equal(release.body.code, "LeaseLost");

    const owned = await admin.query<{ owner: string; fence: string }>(
      `SELECT q.lease_owner::text AS owner, r.current_fence::text AS fence
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(owned.rows[0]!.owner, ownerB);
    assert.equal(owned.rows[0]!.fence, "2");

    const restarted = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerB,
      runId,
      fence: "2",
      intent: { kind: "restart_attempt", stepKey: "only" },
    });
    assert.equal(restarted.status, 200);
    const completed = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: ownerB,
      runId,
      fence: "2",
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: restarted.body.attempt,
        attemptToken: restarted.body.attemptToken,
        result: { ok: true },
      },
    });
    assert.equal(completed.status, 200);
    const after = await admin.query<{ owner: string; fence: string; completed: string }>(
      `SELECT q.lease_owner::text AS owner, r.current_fence::text AS fence,
              (SELECT count(*)::text FROM run_events e WHERE e.run_id = r.id AND e.type = 'StepCompleted') AS completed
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(after.rows[0]!.owner, ownerB);
    assert.equal(after.rows[0]!.fence, "2");
    assert.equal(after.rows[0]!.completed, "1");

    const stream = await fetch(`${server.url}/worker/v1/stream`, { method: "POST" });
    assert.equal(stream.status, 404);
    const events = await fetch(`${server.url}/v1/runs/${runId}/events`);
    assert.equal(events.status, 404);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function reapAll(client: pg.Client): Promise<void> {
  for (let attempt = 0; attempt < 132; attempt += 1) {
    const row = await client.query<{ result: string }>(`SELECT relay_reap_expired_leases(1) AS result`);
    if (row.rows[0]!.result === "none") return;
  }
  throw new Error("reaper did not finish");
}

async function assertOneGrant(first: pg.Client, second: pg.Client, sessionA: string, sessionB: string): Promise<void> {
  const settled = await Promise.allSettled([
    first.query(`SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`, [sessionA, randomUUID(), HASH]),
    second.query(`SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`, [sessionB, randomUUID(), HASH]),
  ]);
  const rejected = settled.filter((item) => item.status === "rejected");
  const fulfilled = settled.filter((item) => item.status === "fulfilled");
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);
  assert.equal((rejected[0] as PromiseRejectedResult).reason.message, "InvalidRunState");
}

async function waitForLock(admin: pg.Client, name: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const locks = await admin.query(
      `SELECT 1 FROM pg_stat_activity WHERE query LIKE $1 AND wait_event_type = 'Lock'`,
      [`%${name}%`],
    );
    if ((locks.rowCount ?? 0) > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  throw new Error(`${name} did not wait on the lock`);
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

async function waitForExpiredEvent(admin: pg.Client, runId: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const row = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'LeaseExpired'`,
      [runId],
    );
    if (row.rows[0]?.count === "1") return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("reaper did not expire the lease");
}

function sessionBody(workerSessionId: string): Json {
  return {
    protocolVersion: 1,
    operationId: randomUUID(),
    workerSessionId,
    tenantId: SEEDED_TENANT_ID,
    workflows: [{ name: "demo", version: "demo-v1" }],
  };
}

async function createRun(url: string): Promise<string> {
  const created = await post(url, "/v1/runs", {
    workflowName: "demo",
    workflowVersion: "demo-v1",
    input: { n: 1 },
  });
  assert.equal(created.status, 201);
  return String(created.body.runId);
}

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}
