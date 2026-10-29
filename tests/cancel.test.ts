import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { cancel, defineWorkflow, runActivation } from "@relay/sdk";

test("completion that commits first is left unchanged", async () => {
  const harness = await openHarness();
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("save", async () => ({ ok: true }));
    return { saved: true };
  });
  try {
    const runId = await createRun(harness.url);
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    const first = await cancel({ serverUrl: harness.url, runId });
    const second = await cancel({ serverUrl: harness.url, runId });
    assert.equal(first.state, "completed");
    assert.deepEqual(first.output, { saved: true });
    assert.equal(first.error, null);
    assert.deepEqual(second, first);
    assert.equal(await eventCount(harness.admin, runId, "RunCompleted"), "1");
    assert.equal(await eventCount(harness.admin, runId, "RunCancelled"), "0");
  } finally {
    await harness.close();
  }
});

test("cancel that commits first rejects the later completion", async () => {
  const harness = await openHarness(50);
  let effect = false;
  let sawAbort = false;
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("charge", async (step) => {
      effect = true;
      await gate;
      await new Promise((resolve) => setTimeout(resolve, 400));
      sawAbort = step.signal.aborted;
      return { charged: true };
    });
    return { charged: true };
  });
  try {
    const runId = await createRun(harness.url);
    const activation = runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    await waitFor(() => effect);
    const cancelled = await cancel({ serverUrl: harness.url, runId });
    release();
    await assert.rejects(activation, (error: Error) => error.message.includes("run is terminal"));
    assert.equal(effect, true);
    assert.equal(sawAbort, true);
    assert.equal(cancelled.state, "cancelled");
    assert.equal(cancelled.output, null);
    assert.equal(cancelled.error, null);
    assert.equal(await eventCount(harness.admin, runId, "RunCancelled"), "1");
    assert.equal(await eventCount(harness.admin, runId, "RunCompleted"), "0");
    const again = await cancel({ serverUrl: harness.url, runId });
    assert.equal(again.state, "cancelled");
    assert.equal(await eventCount(harness.admin, runId, "RunCancelled"), "1");

    const run = await harness.admin.query<{ state: string; output: unknown; error: unknown }>(
      `SELECT state, output, error FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(run.rows[0]?.state, "cancelled");
    assert.equal(run.rows[0]?.output, null);
    assert.equal(run.rows[0]?.error, null);
    const attempt = await harness.admin.query<{ state: string }>(
      `SELECT state FROM step_attempts WHERE run_id = $1`,
      [runId],
    );
    assert.equal(attempt.rows[0]?.state, "abandoned");
    const terminal = await harness.admin.query<{ count: string; state: string }>(
      `SELECT count(*)::text AS count, min(payload->>'state') AS state
       FROM run_stream_events WHERE run_id = $1 AND kind = 'run_terminal'`,
      [runId],
    );
    assert.equal(terminal.rows[0]?.count, "1");
    assert.equal(terminal.rows[0]?.state, "cancelled");
    const history = await fetch(`${harness.url}/v1/runs/${runId}/events`);
    const body = await history.json() as { events: Array<{ type: string }> };
    assert.equal(history.status, 200);
    assert.equal(body.events.some((event) => event.type === "RunCancelled"), true);

    const rebuilt = await harness.admin.query<{ result: { ok: boolean; mismatches: unknown } }>(
      `SELECT relay_rebuild_projections($1) AS result`,
      [runId],
    );
    assert.equal(rebuilt.rows[0]?.result.ok, true, JSON.stringify(rebuilt.rows[0]?.result.mismatches));
    const step = await harness.admin.query<{ state: string }>(
      `SELECT state FROM steps WHERE run_id = $1 AND step_key = 'charge'`,
      [runId],
    );
    assert.equal(step.rows[0]?.state, "interrupted");
  } finally {
    await harness.close();
  }
});

test("a cancelled run is not woken by a timer, retry, or wait", async () => {
  const harness = await openHarness();
  const sleeper = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.sleep("cooldown", 60_000);
    return { ok: true };
  });
  const waiter = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.waitForEvent("approval", { timeoutMs: 60_000 });
    return { ok: true };
  });
  try {
    const sleeping = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: sleeper, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    await cancel({ serverUrl: harness.url, runId: sleeping });
    assert.equal((await harness.admin.query(`SELECT relay_fire_one_timer() AS result`)).rows[0]?.result, "none");
    assert.equal(await eventCount(harness.admin, sleeping, "TimerFired"), "0");

    const waiting = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: waiter, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    await cancel({ serverUrl: harness.url, runId: waiting });
    assert.equal((await harness.admin.query(`SELECT relay_fire_one_wait() AS result`)).rows[0]?.result, "none");
    assert.equal((await harness.admin.query(`SELECT relay_fire_one_retry() AS result`)).rows[0]?.result, "none");
    assert.equal(await eventCount(harness.admin, waiting, "WaitResolved"), "0");
  } finally {
    await harness.close();
  }
});

async function openHarness(heartbeatIntervalMs = 10_000): Promise<{
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
    heartbeatIntervalMs,
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

async function waitFor(ready: () => boolean): Promise<void> {
  const started = Date.now();
  while (!ready()) {
    if (Date.now() - started > 5_000) throw new Error("timed out waiting for the callback");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
