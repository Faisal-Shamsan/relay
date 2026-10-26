import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { canonicalHash } from "@relay/protocol";
import { defineWorkflow, runActivation } from "@relay/sdk";
import { createTestDatabase, startServer } from "relay-server";

test("F13 an explicit input mismatch is reported before the memoized result or a new attempt", async () => {
  const harness = await openHarness();
  const calls: string[] = [];
  const source = { region: "us", nested: { n: 1 } };
  const original = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("prices", { input: source }, async (_stepCtx, input) => {
      calls.push("prices");
      source.region = "mutated";
      const supplied = input as { region: string; nested: { n: number } };
      assert.equal(Object.isFrozen(supplied), true);
      assert.equal(Object.isFrozen(supplied.nested), true);
      assert.equal(supplied.region, "us");
      assert.throws(() => {
        supplied.region = "eu";
      });
      source.region = "us";
      return { region: supplied.region };
    });
  });
  try {
    const runId = await createRun(harness.url);
    const first = await runActivation({ serverUrl: harness.url, workflow: original, finishRun: false });
    assert.deepEqual(first, { region: "us" });
    assert.deepEqual(calls, ["prices"]);
    const stored = await harness.admin.query<{ input: unknown; hash: string; attempts: string; ok: boolean }>(
      `SELECT s.input, s.input_hash AS hash,
              (SELECT count(*)::text FROM step_attempts a WHERE a.run_id = s.run_id) AS attempts,
              (relay_verify_projections(s.run_id)->>'ok')::boolean AS ok
       FROM steps s WHERE s.run_id = $1 AND s.step_key = 'prices'`,
      [runId],
    );
    assert.deepEqual(stored.rows[0]?.input, { region: "us", nested: { n: 1 } });
    assert.equal(stored.rows[0]?.hash, canonicalHash({ region: "us", nested: { n: 1 } }));
    assert.equal(stored.rows[0]?.attempts, "1");
    assert.equal(stored.rows[0]?.ok, true);
    await releaseLease(harness.admin, runId);

    const replay = await runActivation({ serverUrl: harness.url, workflow: original, finishRun: false });
    assert.deepEqual(replay, { region: "us" });
    assert.deepEqual(calls, ["prices"]);
    await releaseLease(harness.admin, runId);

    const changed = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("prices", { input: { region: "eu", nested: { n: 1 } } }, async () => {
        calls.push("prices-again");
        return { region: "eu" };
      });
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: changed, finishRun: false }),
      (error: { name?: string; details?: { position?: string; expected?: { inputHash?: string }; actual?: { inputHash?: string } } }) =>
        error.name === "Nondeterminism"
        && error.details?.position === "0"
        && error.details.expected?.inputHash !== error.details.actual?.inputHash,
    );
    assert.deepEqual(calls, ["prices"]);
    const afterMismatch = await harness.admin.query<{ attempts: string; output: unknown }>(
      `SELECT (SELECT count(*)::text FROM step_attempts a WHERE a.run_id = $1) AS attempts,
              (SELECT output FROM steps s WHERE s.run_id = $1 AND s.step_key = 'prices') AS output`,
      [runId],
    );
    assert.equal(afterMismatch.rows[0]?.attempts, "1");
    assert.deepEqual(afterMismatch.rows[0]?.output, { region: "us" });
    await releaseLease(harness.admin, runId);

    const bare = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("prices", async () => {
        calls.push("bare");
        return { region: "none" };
      });
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: bare, finishRun: false }),
      (error: { name?: string; details?: { expected?: { explicitInput?: boolean }; actual?: { explicitInput?: boolean } } }) =>
        error.name === "Nondeterminism"
        && error.details?.expected?.explicitInput === true
        && error.details?.actual?.explicitInput === false,
    );
    assert.deepEqual(calls, ["prices"]);
  } finally {
    await harness.close();
  }
});

test("F13 explicit null differs from an omitted input", async () => {
  const harness = await openHarness();
  const calls: string[] = [];
  try {
    const nullRun = await createRun(harness.url);
    const withNull = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      const value = await ctx.step("only", { input: null }, async (_stepCtx, input) => {
        calls.push("null");
        assert.equal(input, null);
        return { saw: input };
      });
      return value;
    });
    assert.deepEqual(await runActivation({ serverUrl: harness.url, workflow: withNull, finishRun: false }), { saw: null });
    await releaseLease(harness.admin, nullRun);
    const omitted = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("only", async () => {
        calls.push("omitted");
        return { saw: "none" };
      });
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: omitted, finishRun: false }),
      (error: { name?: string }) => error.name === "Nondeterminism",
    );
    assert.deepEqual(calls, ["null"]);
  } finally {
    await harness.close();
  }
});

test("F13 a parallel child input change is rejected before either callback", async () => {
  const harness = await openHarness();
  const calls: string[] = [];
  try {
    const runId = await createRun(harness.url);
    const parallel = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step.parallel("group", [
        {
          name: "a",
          input: { k: 1 },
          run: async (_stepCtx, input) => {
            calls.push("a");
            return { k: (input as { k: number }).k };
          },
        },
        {
          name: "b",
          run: async () => {
            calls.push("b");
            return { b: true };
          },
        },
      ]);
    });
    assert.deepEqual(await runActivation({ serverUrl: harness.url, workflow: parallel, finishRun: false }), [{ k: 1 }, { b: true }]);
    assert.deepEqual([...calls].sort(), ["a", "b"]);
    await releaseLease(harness.admin, runId);
    const changedChild = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step.parallel("group", [
        {
          name: "a",
          input: { k: 2 },
          run: async () => {
            calls.push("a-again");
            return { k: 2 };
          },
        },
        {
          name: "b",
          run: async () => {
            calls.push("b-again");
            return { b: true };
          },
        },
      ]);
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: changedChild, finishRun: false }),
      (error: { name?: string }) => error.name === "Nondeterminism",
    );
    assert.deepEqual([...calls].sort(), ["a", "b"]);
    const verified = await harness.admin.query<{ ok: boolean }>(
      `SELECT (relay_verify_projections($1)->>'ok')::boolean AS ok`,
      [runId],
    );
    assert.equal(verified.rows[0]?.ok, true);
  } finally {
    await harness.close();
  }
});

test("F13 a changed explicit input does not start another attempt after an interrupted step", async () => {
  const harness = await openHarness();
  let calls = 0;
  const started = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("only", { input: { n: 1 } }, async () => {
      calls += 1;
      throw new Error("stop before completion");
    });
    return { ok: true };
  });
  try {
    const runId = await createRun(harness.url);
    await assert.rejects(() => runActivation({ serverUrl: harness.url, workflow: started, finishRun: false }));
    assert.equal(calls, 1);
    await releaseLease(harness.admin, runId);
    const changed = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.step("only", { input: { n: 2 } }, async () => {
        calls += 1;
        return { ok: true };
      });
      return { ok: true };
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: changed, finishRun: false }),
      (error: { name?: string }) => error.name === "Nondeterminism",
    );
    assert.equal(calls, 1);
    const attempts = await harness.admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM step_attempts WHERE run_id = $1`,
      [runId],
    );
    assert.equal(attempts.rows[0]?.count, "1");
  } finally {
    await harness.close();
  }
});

async function openHarness(): Promise<{ url: string; admin: pg.Client; close: () => Promise<void> }> {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0, streamRetentionIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  await fetch(`${server.url}/worker/v1/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: crypto.randomUUID(),
      tenantId: "11111111-1111-4111-8111-111111111111",
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
  assert.equal(created.status, 201);
  return String((await created.json() as { runId: string }).runId);
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}
