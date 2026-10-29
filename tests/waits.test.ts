import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation, signal } from "@relay/sdk";

test("an early signal is consumed in order and replay does not park again", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const result = await ctx.waitForEvent("approval", { timeoutMs: 60_000 });
    return ctx.step("after", async () => {
      calls += 1;
      return result;
    });
  });
  try {
    const runId = await createRun(harness.url);
    const first = await signal({
      serverUrl: harness.url,
      runId,
      name: "approval",
      payload: { n: 1 },
      idempotencyKey: "approve-1",
    });
    await signal({
      serverUrl: harness.url,
      runId,
      name: "approval",
      payload: { n: 2 },
      idempotencyKey: "approve-2",
    });
    await runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
    assert.equal(await eventCount(harness.admin, runId, "RunSuspended"), "0");
    const resolved = await harness.admin.query<{ outcome: string; n: string; ingress: string }>(
      `SELECT payload->'result'->>'outcome' AS outcome,
              payload->'result'->'payload'->>'n' AS n,
              payload->'result'->>'ingressSeq' AS ingress
       FROM run_events WHERE run_id = $1 AND type = 'WaitResolved'`,
      [runId],
    );
    assert.equal(resolved.rows[0]?.outcome, "signal");
    assert.equal(resolved.rows[0]?.n, "1");
    assert.equal(resolved.rows[0]?.ingress, first.ingressSeq);
    const inbox = await harness.admin.query<{ seq: string; consumed: string | null }>(
      `SELECT ingress_seq::text AS seq, consumed_by_command_pos::text AS consumed
       FROM signal_inbox WHERE run_id = $1 ORDER BY ingress_seq`,
      [runId],
    );
    assert.equal(inbox.rows.length, 2);
    assert.equal(inbox.rows[0]?.consumed, "0");
    assert.equal(inbox.rows[1]?.consumed, null);

    await releaseLease(harness.admin, runId);
    const rebuilt = await harness.admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_rebuild_projections($1) AS result`,
      [runId],
    );
    assert.equal(rebuilt.rows[0]?.result.ok, true);
    const restored = await harness.admin.query<{ consumed: string | null }>(
      `SELECT consumed_by_command_pos::text AS consumed
       FROM signal_inbox WHERE run_id = $1 ORDER BY ingress_seq`,
      [runId],
    );
    assert.equal(restored.rows[0]?.consumed, "0");
    assert.equal(restored.rows[1]?.consumed, null);

    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
    assert.equal(await eventCount(harness.admin, runId, "RunSuspended"), "0");
  } finally {
    await harness.close();
  }
});

test("a timeout is recorded once and replay does not park", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const result = await ctx.waitForEvent("approval", { timeoutMs: 0 });
    return ctx.step("after", async () => {
      calls += 1;
      return result;
    });
  });
  try {
    const runId = await createRun(harness.url);
    await runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
    assert.equal(await eventCount(harness.admin, runId, "RunSuspended"), "0");
    const resolved = await harness.admin.query<{ outcome: string }>(
      `SELECT payload->'result'->>'outcome' AS outcome
       FROM run_events WHERE run_id = $1 AND type = 'WaitResolved'`,
      [runId],
    );
    assert.equal(resolved.rows[0]?.outcome, "timeout");
    const parked = await harness.admin.query<{ state: string; queue: string }>(
      `SELECT r.state, q.state AS queue FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.notEqual(parked.rows[0]?.state, "waiting");
    assert.notEqual(parked.rows[0]?.queue, "parked");

    await releaseLease(harness.admin, runId);
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
  } finally {
    await harness.close();
  }
});

test("a wait inside a callback is rejected", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("plan", async () => {
      await ctx.waitForEvent("inside", { timeoutMs: 0 });
      return { ok: true };
    });
    return { ok: true };
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "ConcurrentCommand",
    );
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "0");
    const waits = await harness.admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM commands WHERE run_id = $1 AND kind = 'wait'`,
      [runId],
    );
    assert.equal(waits.rows[0]?.count, "0");
  } finally {
    await harness.close();
  }
});

test("a signal and a timeout produce one wait result", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const result = await ctx.waitForEvent("approval", { timeoutMs: 60_000 });
    return ctx.step("after", async () => {
      calls += 1;
      return result;
    });
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    assert.equal(calls, 0);
    const parked = await harness.admin.query<{ state: string; queue: string }>(
      `SELECT r.state, q.state AS queue FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(parked.rows[0]?.state, "waiting");
    assert.equal(parked.rows[0]?.queue, "parked");
    await harness.admin.query(
      `UPDATE wait_deadlines SET wake_at = clock_timestamp() - interval '1 second' WHERE run_id = $1`,
      [runId],
    );

    const other = new pg.Client({ connectionString: harness.adminUrl });
    await other.connect();
    try {
      await Promise.all([
        harness.admin.query(`SELECT relay_fire_one_wait()`),
        other.query(`SELECT relay_fire_one_wait()`),
        signal({
          serverUrl: harness.url,
          runId,
          name: "approval",
          payload: { n: 1 },
          idempotencyKey: "late",
        }),
      ]);
    } finally {
      await other.end();
    }

    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
    const again = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_wait() AS result`);
    assert.equal(again.rows[0]?.result, "none");
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
    const consumed = await harness.admin.query<{ consumed: string }>(
      `SELECT count(*) FILTER (WHERE consumed_by_command_pos IS NOT NULL)::text AS consumed
       FROM signal_inbox WHERE run_id = $1`,
      [runId],
    );
    const outcome = await harness.admin.query<{ outcome: string }>(
      `SELECT payload->'result'->>'outcome' AS outcome
       FROM run_events WHERE run_id = $1 AND type = 'WaitResolved'`,
      [runId],
    );
    assert.equal(consumed.rows[0]?.consumed, outcome.rows[0]?.outcome === "signal" ? "1" : "0");

    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "WaitResolved"), "1");
  } finally {
    await harness.close();
  }
});

async function openHarness(): Promise<{
  url: string;
  adminUrl: string;
  admin: pg.Client;
  close: () => Promise<void>;
}> {
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
    adminUrl: db.adminUrl,
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

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}
