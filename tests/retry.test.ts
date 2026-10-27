import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID, type RetryPolicy } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation, StepExecutionError } from "@relay/sdk";

const jittered: RetryPolicy = {
  maxAttempts: 3,
  initialDelayMs: 1000,
  maxDelayMs: 1000,
  multiplier: 1,
  jitter: "full",
  retryableCodes: ["429"],
};

test("F33 a retry stores one delay and reuses it", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("charge", { retry: jittered }, async () => {
      calls += 1;
      if (calls === 1) throw coded("429", "rate limited");
      return { ok: true };
    });
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    assert.equal(calls, 1);
    const delay = await failureDelay(harness.admin, runId);
    assert.equal(await eventCount(harness.admin, runId, "StepFailed"), "1");
    const parsed = Number(delay);
    assert.equal(Number.isInteger(parsed) && parsed >= 0 && parsed <= 1000, true);
    const again = await harness.admin.query<{ result: { delayMs?: string } }>(
      `SELECT relay_fail_step(r.operation_id, r.operation_id, r.run_id, 1, 'charge', 1, r.operation_id, '{"code":"429","message":"rate limited"}'::jsonb, r.request_hash) AS result
       FROM operation_receipts r
       WHERE r.run_id = $1 AND r.response->>'delayMs' IS NOT NULL`,
      [runId],
    );
    assert.equal(again.rows[0]?.result.delayMs, delay);
    assert.equal(await eventCount(harness.admin, runId, "StepFailed"), "1");
    await assert.rejects(
      harness.admin.query(
        `SELECT relay_fail_step(gen_random_uuid(), gen_random_uuid(), $1, 1, 'charge', 1, gen_random_uuid(), '{"code":"429","message":"rate limited"}'::jsonb, repeat('ab', 32))`,
        [runId],
      ),
    );
    assert.equal(await failureDelay(harness.admin, runId), delay);
    const early = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_retry() AS result`);
    assert.equal(early.rows[0]?.result, "none");
    assert.equal(await failureDelay(harness.admin, runId), delay);
    const waiting = await harness.admin.query<{ state: string; queue: string }>(
      `SELECT r.state, q.state AS queue
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [runId],
    );
    assert.equal(waiting.rows[0]?.state, "retrying");
    assert.equal(waiting.rows[0]?.queue, "scheduled");
    const listed = await failedRuns(harness.admin);
    assert.equal(listed.includes(runId), false);
  } finally {
    await harness.close();
  }
});

test("a zero delay retries once and replay returns the stored success", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("charge", {
      retry: { ...jittered, jitter: "none", initialDelayMs: 0, maxDelayMs: 0 },
    }, async () => {
      calls += 1;
      if (calls === 1) throw coded("429", "once");
      return { ok: true };
    });
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    const woke = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_retry() AS result`);
    assert.equal(woke.rows[0]?.result, "fired");
    await runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    assert.equal(calls, 2);
    await releaseLease(harness.admin, runId);
    await runActivation({ serverUrl: harness.url, workflow, finishRun: true });
    assert.equal(calls, 2);
    assert.equal(await eventCount(harness.admin, runId, "StepFailed"), "1");
    assert.equal(await failureDelay(harness.admin, runId), "0");
    const verified = await harness.admin.query<{ result: { ok: boolean; mismatches: unknown } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [runId],
    );
    assert.equal(verified.rows[0]?.result.ok, true, JSON.stringify(verified.rows[0]?.result.mismatches));
  } finally {
    await harness.close();
  }
});

test("F33 exhaustion is a failed run only when the workflow does not handle it", async () => {
  const harness = await openHarness();
  let poisonCalls = 0;
  let handledCalls = 0;
  const poison = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("charge", { retry: { ...jittered, maxAttempts: 2, jitter: "none", initialDelayMs: 80, maxDelayMs: 80 } }, async () => {
      poisonCalls += 1;
      throw coded("429", "still limited");
    });
  });
  const handled = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    try {
      return await ctx.step("charge", { retry: { ...jittered, maxAttempts: 1, jitter: "none", initialDelayMs: 0, maxDelayMs: 0 } }, async () => {
        handledCalls += 1;
        throw coded("429", "once");
      });
    } catch (error) {
      if (error instanceof StepExecutionError) return { handled: true };
      throw error;
    }
  });
  try {
    const failedId = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: poison, finishRun: false }),
      (error: { name?: string }) => error.name === "Suspended",
    );
    assert.equal(await failureDelay(harness.admin, failedId), "80");
    const tooSoon = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_retry() AS result`);
    assert.equal(tooSoon.rows[0]?.result, "none");
    await new Promise((resolve) => setTimeout(resolve, 150));
    await expectWakeAbort(harness.admin);
    assert.equal(await eventCount(harness.admin, failedId, "StepFailed"), "1");
    const fired = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_retry() AS result`);
    assert.equal(fired.rows[0]?.result, "fired");
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: poison, finishRun: true }),
      (error: StepExecutionError) => error instanceof StepExecutionError && error.message === "still limited",
    );
    assert.equal(poisonCalls, 2);
    const failed = await harness.admin.query<{ state: string; code: string }>(
      `SELECT state, error->>'code' AS code FROM runs WHERE id = $1`,
      [failedId],
    );
    assert.equal(failed.rows[0]?.state, "failed");
    assert.equal(failed.rows[0]?.code, "StepExecutionError");
    assert.equal((await failedRuns(harness.admin)).includes(failedId), true);

    const handledId = await createRun(harness.url);
    await runActivation({ serverUrl: harness.url, workflow: handled, finishRun: false });
    assert.equal(handledCalls, 1);
    await releaseLease(harness.admin, handledId);
    const output = await runActivation({ serverUrl: harness.url, workflow: handled, finishRun: true });
    assert.deepEqual(output, { handled: true });
    assert.equal(handledCalls, 1);
    const completed = await harness.admin.query<{ state: string }>(`SELECT state FROM runs WHERE id = $1`, [handledId]);
    assert.equal(completed.rows[0]?.state, "completed");
    assert.equal((await failedRuns(harness.admin)).includes(handledId), false);
    const verified = await harness.admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [handledId],
    );
    assert.equal(verified.rows[0]?.result.ok, true);
  } finally {
    await harness.close();
  }
});

test("F34 a parallel sibling finishes before the retry releases the run", async () => {
  const harness = await openHarness();
  let releaseSibling: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    releaseSibling = resolve;
  });
  const calls = { a: 0, b: 0 };
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step.parallel("group", [
      {
        name: "a",
        run: async () => {
          calls.a += 1;
          await gate;
          return { item: "a" };
        },
      },
      {
        name: "b",
        retry: { ...jittered, jitter: "none", initialDelayMs: 60_000, maxDelayMs: 60_000 },
        run: async () => {
          calls.b += 1;
          throw coded("429", "later");
        },
      },
    ], { concurrency: 2 });
  });
  try {
    const runId = await createRun(harness.url);
    const activation = runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    const caught = activation.then(() => {
      throw new Error("activation finished");
    }, (error: unknown) => error);
    await waitForState(harness.admin, runId, "b", "retry_wait");
    const mid = await harness.admin.query<{ state: string; queue: string; a: string }>(
      `SELECT r.state, q.state AS queue,
              (SELECT s.state FROM steps s WHERE s.run_id = r.id AND s.step_key = 'a') AS a
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [runId],
    );
    assert.equal(mid.rows[0]?.state, "running");
    assert.equal(mid.rows[0]?.queue, "leased");
    assert.equal(mid.rows[0]?.a, "running");
    assert.equal(calls.a, 1);
    assert.equal(calls.b, 1);
    releaseSibling();
    const error = await caught;
    assert.equal((error as { name?: string }).name, "Suspended");
    assert.equal(calls.a, 1);
    assert.equal(calls.b, 1);
    const after = await harness.admin.query<{ state: string; queue: string; a: string; b: string }>(
      `SELECT r.state, q.state AS queue,
              (SELECT state FROM steps WHERE run_id = r.id AND step_key = 'a') AS a,
              (SELECT state FROM steps WHERE run_id = r.id AND step_key = 'b') AS b
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [runId],
    );
    assert.equal(after.rows[0]?.state, "retrying");
    assert.equal(after.rows[0]?.queue, "scheduled");
    assert.equal(after.rows[0]?.a, "completed");
    assert.equal(after.rows[0]?.b, "retry_wait");
    const early = await harness.admin.query<{ result: string }>(`SELECT relay_fire_one_retry() AS result`);
    assert.equal(early.rows[0]?.result, "none");
    const verified = await harness.admin.query<{ result: { ok: boolean; mismatches: unknown } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [runId],
    );
    assert.equal(verified.rows[0]?.result.ok, true, JSON.stringify(verified.rows[0]?.result.mismatches));
  } finally {
    await harness.close();
  }
});

test("an explicit retry budget bounds an abandoned attempt", async () => {
  const harness = await openHarness();
  let release: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("charge", {
      retry: { ...jittered, maxAttempts: 1, jitter: "none", initialDelayMs: 0, maxDelayMs: 0 },
    }, async () => {
      calls += 1;
      await gate;
      return { ok: true };
    });
  });
  try {
    const runId = await createRun(harness.url);
    const first = runActivation({ serverUrl: harness.url, workflow, finishRun: false });
    const firstError = first.then(() => {
      throw new Error("activation finished");
    }, (error: unknown) => error);
    await waitForState(harness.admin, runId, "charge", "running");
    await releaseLease(harness.admin, runId);
    release();
    await firstError;
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow, finishRun: false }),
      (error: Error) => error.message.includes("RecoveryBudgetExceeded"),
    );
    assert.equal(calls, 1);
    const failed = await harness.admin.query<{ state: string; code: string }>(
      `SELECT state, error->>'code' AS code FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(failed.rows[0]?.state, "failed");
    assert.equal(failed.rows[0]?.code, "RecoveryBudgetExceeded");
    assert.equal((await failedRuns(harness.admin)).includes(runId), true);
  } finally {
    await harness.close();
  }
});

function coded(code: string, message: string): Error {
  const error = new Error(message) as Error & { code: string };
  error.code = code;
  return error;
}

async function openHarness(): Promise<{ url: string; admin: pg.Client; close: () => Promise<void> }> {
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

async function failureDelay(admin: pg.Client, runId: string): Promise<string> {
  const rows = await admin.query<{ delay: string }>(
    `SELECT payload->>'delayMs' AS delay
     FROM run_events
     WHERE run_id = $1 AND type = 'StepFailed'
     ORDER BY seq
     LIMIT 1`,
    [runId],
  );
  return rows.rows[0]?.delay ?? "";
}

async function failedRuns(admin: pg.Client): Promise<string[]> {
  const rows = await admin.query<{ result: string[] }>(
    `SELECT relay_failed_runs($1) AS result`,
    [SEEDED_TENANT_ID],
  );
  return rows.rows[0]?.result ?? [];
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}

async function waitForState(admin: pg.Client, runId: string, stepKey: string, state: string): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const rows = await admin.query<{ state: string }>(
      `SELECT state FROM steps WHERE run_id = $1 AND step_key = $2`,
      [runId, stepKey],
    );
    if (rows.rows[0]?.state === state) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`step ${stepKey} did not become ${state}`);
}

async function expectWakeAbort(admin: pg.Client): Promise<void> {
  await admin.query("BEGIN");
  try {
    await admin.query(`SELECT set_config('relay.allow_failpoints', '1', true)`);
    await admin.query(`SELECT set_config('relay.failpoint', 'retry.after-wake-before-commit', true)`);
    await assert.rejects(admin.query(`SELECT relay_fire_one_retry()`), (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, "FailpointAbort");
      return true;
    });
  } finally {
    await admin.query("ROLLBACK");
  }
}
