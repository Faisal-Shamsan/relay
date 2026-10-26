import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID, type Json } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation } from "@relay/sdk";

test("F14 a reordered helper is rejected and replay returns the committed values", async () => {
  const harness = await openHarness();
  let calls = 0;
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    const at = await ctx.now("report-time");
    const sample = await ctx.random("sample");
    const correlationId = await ctx.uuid("correlation-id");
    const step = await ctx.step("plan", async () => {
      calls += 1;
      return { ok: true };
    });
    return { at, sample, correlationId, step };
  });
  try {
    const runId = await createRun(harness.url);
    const first = asObject(await runActivation({ serverUrl: harness.url, workflow, finishRun: false }));
    assert.equal(calls, 1);
    assert.match(String(first.at), /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    assert.equal(typeof first.sample, "number");
    assert.ok(Number(first.sample) >= 0 && Number(first.sample) < 1);
    assert.match(String(first.correlationId), /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    await releaseLease(harness.admin, runId);

    const reordered = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.random("sample");
      return { ok: true };
    });
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: reordered, finishRun: false }),
      (error: { name?: string; details?: { position?: string; expected?: { key?: string }; actual?: { key?: string } } }) =>
        error.name === "Nondeterminism"
        && error.details?.position === "0"
        && error.details.expected?.key === "report-time"
        && error.details.actual?.key === "sample",
    );
    assert.equal(calls, 1);
    const afterReject = await commandCount(harness.admin, runId);
    assert.equal(afterReject, "4");
    await releaseLease(harness.admin, runId);

    const second = asObject(await runActivation({ serverUrl: harness.url, workflow, finishRun: true }));
    assert.equal(calls, 1);
    assert.deepEqual(second, first);
    assert.equal(await commandCount(harness.admin, runId), "4");
    const verified = await harness.admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_verify_projections($1) AS result`,
      [runId],
    );
    assert.equal(verified.rows[0]?.result.ok, true);
  } finally {
    await harness.close();
  }
});

test("F14 an identical helper retry returns the committed value", async () => {
  const harness = await openHarness();
  const sessionId = crypto.randomUUID();
  try {
    const runId = await createRun(harness.url);
    await fetch(`${harness.url}/worker/v1/register`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        protocolVersion: 1,
        operationId: crypto.randomUUID(),
        workerSessionId: sessionId,
        tenantId: SEEDED_TENANT_ID,
        workflows: [{ name: "demo", version: "demo-v1" }],
      }),
    });
    const leased = await postJson(`${harness.url}/worker/v1/lease`, {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: sessionId,
    });
    assert.equal(leased.runId, runId);
    const body = {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: leased.fence,
      intent: { kind: "record_helper", helperKind: "random", key: "sample", expectedCommandPos: "0" },
    };
    const first = await postJson(`${harness.url}/worker/v1/events`, body);
    const retry = await postJson(`${harness.url}/worker/v1/events`, body);
    assert.deepEqual(retry, first);
    const again = await postJson(`${harness.url}/worker/v1/events`, {
      ...body,
      operationId: crypto.randomUUID(),
    });
    assert.equal(again.outcome, first.outcome);
    assert.equal(await commandCount(harness.admin, runId), "1");
  } finally {
    await harness.close();
  }
});

test("a helper is rejected inside a callback and a repeated helper key is rejected", async () => {
  const harness = await openHarness();
  let calls = 0;
  const nested = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    await ctx.step("plan", async () => {
      calls += 1;
      await ctx.now("inside");
      return { ok: true };
    });
    return { ok: true };
  });
  try {
    const nestedRun = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: nested, finishRun: false }),
      (error: { name?: string }) => error.name === "ConcurrentCommand",
    );
    assert.equal(calls, 1);
    const helpers = await harness.admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM commands WHERE run_id = $1 AND kind = 'now'`,
      [nestedRun],
    );
    assert.equal(helpers.rows[0]?.count, "0");

    const duplicated = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      await ctx.now("report-time");
      await ctx.now("report-time");
      return { ok: true };
    });
    const duplicateRun = await createRun(harness.url);
    await assert.rejects(
      () => runActivation({ serverUrl: harness.url, workflow: duplicated, finishRun: false }),
      (error: { name?: string }) => error.name === "DuplicateStepKey",
    );
    assert.equal(await commandCount(harness.admin, duplicateRun), "1");
  } finally {
    await harness.close();
  }
});

function asObject(value: Json): { [key: string]: Json } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("expected object");
  }
  return value;
}

async function openHarness(): Promise<{ url: string; admin: pg.Client; close: () => Promise<void> }> {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
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

async function postJson(url: string, body: unknown): Promise<{ [key: string]: unknown }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  assert.equal(response.status, 200, text);
  return JSON.parse(text) as { [key: string]: unknown };
}

async function commandCount(admin: pg.Client, runId: string): Promise<string> {
  const commands = await admin.query<{ count: string }>(
    `SELECT count(*)::text AS count FROM commands WHERE run_id = $1`,
    [runId],
  );
  return commands.rows[0]?.count ?? "0";
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}
