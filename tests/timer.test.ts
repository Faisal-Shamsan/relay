import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation } from "@relay/sdk";

test("a zero-duration timer is recorded once and replay does not schedule it again", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.sleep("cooldown", 0);
    return ctx.step("after", async () => {
      calls += 1;
      return { ok: true };
    });
  });
  try {
    const runId = await createRun(harness.url);
    await runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "TimerScheduled"), "1");
    assert.equal(await eventCount(harness.admin, runId, "TimerFired"), "1");
    const wake = await wakeAt(harness.admin, runId);
    await releaseLease(harness.admin, runId);

    const changed = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.sleep("cooldown", 5);
      return { ok: true };
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: changed, finishRun: false }),
      (error: { name?: string; details?: { position?: string } }) =>
        error.name === "Nondeterminism" && error.details?.position === "0",
    );
    assert.equal(await wakeAt(harness.admin, runId), wake);
    await releaseLease(harness.admin, runId);

    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "TimerFired"), "1");
    const verified = await harness.admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [runId],
    );
    assert.equal(verified.rows[0]?.result.ok, true);
  } finally {
    await harness.close();
  }
});

test("F35 a timer fires once from the recorded deadline", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.sleep("cooldown", 80);
    return ctx.step("after", async () => {
      calls += 1;
      return { ok: true };
    });
  });
  const early = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.sleep("later", 60_000);
    return { ok: true };
  });
  try {
    const parked = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: early, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    const parkedWake = await wakeAt(harness.admin, parked);
    const skipped = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_timer() AS result`);
    assert.equal(skipped.rows[0]?.result, "none");
    assert.equal(await wakeAt(harness.admin, parked), parkedWake);
    assert.equal(await eventCount(harness.admin, parked, "TimerFired"), "0");
    const parkedState = await harness.admin.query<{ state: string }>(
      `SELECT state FROM runs WHERE id = $1`,
      [parked],
    );
    assert.equal(parkedState.rows[0]?.state, "sleeping");

    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    assert.equal(calls, 0);
    const scheduled = await wakeAt(harness.admin, runId);
    await new Promise((resolve) => setTimeout(resolve, 150));

    const other = new pg.Client({ connectionString: harness.adminUrl });
    await other.connect();
    try {
      await expectFireAbort(harness.admin);
      assert.equal(await eventCount(harness.admin, runId, "TimerFired"), "0");
      assert.equal(await wakeAt(harness.admin, runId), scheduled);
      const raced = await Promise.all([
        harness.admin.query<{ result: string }>(`SELECT relay_fire_one_timer() AS result`),
        other.query<{ result: string }>(`SELECT relay_fire_one_timer() AS result`),
      ]);
      const results = raced.map((row) => row.rows[0]?.result).sort();
      assert.deepEqual(results, ["fired", "none"]);
    } finally {
      await other.end();
    }

    assert.equal(await eventCount(harness.admin, runId, "TimerScheduled"), "1");
    assert.equal(await eventCount(harness.admin, runId, "TimerFired"), "1");
    const fired = await harness.admin.query<{ wake: string }>(
      `SELECT payload->>'wakeAt' AS wake FROM run_events WHERE run_id = $1 AND type = 'TimerFired'`,
      [runId],
    );
    assert.equal(fired.rows[0]?.wake, scheduled);
    const ready = await harness.admin.query<{ state: string; queue: string }>(
      `SELECT r.state, q.state AS queue FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(ready.rows[0]?.state, "pending");
    assert.equal(ready.rows[0]?.queue, "ready");

    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 1);
    assert.equal(await eventCount(harness.admin, runId, "TimerFired"), "1");
    const verified = await harness.admin.query<{ result: { ok: boolean; mismatches: unknown } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [runId],
    );
    assert.equal(verified.rows[0]?.result.ok, true, JSON.stringify(verified.rows[0]?.result.mismatches));
  } finally {
    await harness.close();
  }
});

test("a timer inside a callback is rejected", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("plan", async () => {
      await ctx.sleep("inside", 0);
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
    assert.equal(await eventCount(harness.admin, runId, "TimerScheduled"), "0");
  } finally {
    await harness.close();
  }
});

async function openHarness(): Promise<{ url: string; adminUrl: string; admin: pg.Client; close: () => Promise<void> }> {
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

async function wakeAt(admin: pg.Client, runId: string): Promise<string> {
  const rows = await admin.query<{ wake: string }>(
    `SELECT payload->>'wakeAt' AS wake FROM run_events WHERE run_id = $1 AND type = 'TimerScheduled'`,
    [runId],
  );
  return rows.rows[0]?.wake ?? "";
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}

async function expectFireAbort(admin: pg.Client): Promise<void> {
  await admin.query("BEGIN");
  try {
    await admin.query(`SELECT set_config('relay.allow_failpoints', '1', true)`);
    await admin.query(`SELECT set_config('relay.failpoint', 'timer.after-fire-before-commit', true)`);
    await assert.rejects(admin.query(`SELECT relay_fire_one_timer()`), (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, "FailpointAbort");
      return true;
    });
  } finally {
    await admin.query("ROLLBACK");
  }
}
