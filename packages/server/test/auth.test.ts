import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { sha256Hex, SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, DEV_CLIENT_KEY, DEV_WORKER_KEY, startServer } from "../src/index.js";

const B_CLIENT = "relay_other_client_key";
const B_WORKER = "relay_other_worker_key";

test("F38 another tenant cannot read, stream, or mutate a run", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    timerIntervalMs: 0,
    streamRetentionIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  const app = new pg.Client({ connectionString: db.appUrl });
  await admin.connect();
  await app.connect();
  const tenantB = randomUUID();
  try {
    await admin.query(`INSERT INTO tenants (id, name) VALUES ($1, 'other')`, [tenantB]);
    await admin.query(
      `INSERT INTO api_credentials (id, tenant_id, key_hash, kind, label)
       VALUES ($1, $3, $4, 'client', 'b-client'), ($2, $3, $5, 'worker', 'b-worker')`,
      [randomUUID(), randomUUID(), tenantB, sha256Hex(B_CLIENT), sha256Hex(B_WORKER)],
    );

    const anonymous = await fetch(`${server.url}/v1/runs`, { headers: { "x-relay-test-anonymous": "1" } });
    assert.equal(anonymous.status, 401);
    assert.equal((await anonymous.json()).code, "Unauthorized");

    const wrongKind = await fetch(`${server.url}/v1/runs`, {
      headers: { authorization: `Bearer ${DEV_WORKER_KEY}` },
    });
    assert.equal(wrongKind.status, 401);

    const registered = await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    }, { authorization: `Bearer ${DEV_WORKER_KEY}` });
    assert.equal(registered.status, 200);

    const mismatched = await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: tenantB,
      workflows: [{ name: "demo", version: "demo-v1" }],
    }, { authorization: `Bearer ${DEV_WORKER_KEY}` });
    assert.equal(mismatched.status, 401);

    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    }, { authorization: `Bearer ${DEV_CLIENT_KEY}` });
    assert.equal(created.status, 201);
    const runId = String(created.body.runId);
    const missingId = randomUUID();

    const foreign = await get(server.url, `/v1/runs/${runId}`, B_CLIENT);
    const missing = await get(server.url, `/v1/runs/${missingId}`, B_CLIENT);
    assert.equal(foreign.status, 404);
    assert.deepEqual(foreign.body.code, missing.body.code);
    assert.equal(foreign.body.code, "InvalidRunState");
    assert.equal(foreign.body.message, missing.body.message);

    const foreignStream = await get(server.url, `/v1/runs/${runId}/stream`, B_CLIENT);
    const missingStream = await get(server.url, `/v1/runs/${missingId}/stream`, B_CLIENT);
    assert.equal(foreignStream.status, 404);
    assert.equal(foreignStream.body.message, missingStream.body.message);

    const metrics = await get(server.url, "/v1/metrics", B_CLIENT);
    assert.equal(metrics.status, 200);
    assert.equal(String(metrics.body).includes('relay_runs{state="pending"} 0'), true);

    const bSession = randomUUID();
    const bRegister = await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: bSession,
      tenantId: tenantB,
      workflows: [{ name: "demo", version: "demo-v1" }],
    }, { authorization: `Bearer ${B_WORKER}` });
    assert.equal(bRegister.status, 200);
    const leased = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: bSession,
    }, { authorization: `Bearer ${B_WORKER}` });
    assert.equal(leased.status, 404);
    assert.notEqual(leased.body.runId, runId);
    const still = await admin.query<{ state: string }>(`SELECT state FROM runs WHERE id = $1`, [runId]);
    assert.equal(still.rows[0]?.state, "pending");

    const completion = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: bSession,
      runId,
      fence: "1",
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: 1,
        attemptToken: randomUUID(),
        result: { ok: true },
      },
    }, { authorization: `Bearer ${B_WORKER}` });
    const missingCompletion = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: bSession,
      runId: missingId,
      fence: "1",
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: 1,
        attemptToken: randomUUID(),
        result: { ok: true },
      },
    }, { authorization: `Bearer ${B_WORKER}` });
    assert.equal(completion.status, 404);
    assert.equal(completion.body.code, "InvalidRunState");
    assert.equal(completion.body.message, missingCompletion.body.message);

    const hidden = await app.query(`SELECT id FROM runs WHERE id = $1`, [runId]);
    assert.equal(hidden.rowCount, 0);
    await app.query(`SELECT set_config('app.tenant_id', $1, false)`, [tenantB]);
    const asOther = await app.query(`SELECT id FROM runs WHERE id = $1`, [runId]);
    assert.equal(asOther.rowCount, 0);
    await app.query(`SELECT set_config('app.tenant_id', $1, false)`, [SEEDED_TENANT_ID]);
    const asOwner = await app.query(`SELECT id FROM runs WHERE id = $1`, [runId]);
    assert.equal(asOwner.rowCount, 1);

    await assert.rejects(
      () => app.query(`INSERT INTO runs (id, tenant_id, workflow_version_id, state, input, trigger_hash) VALUES ($1, $2, $1, 'pending', '{}'::jsonb, $3)`, [
        randomUUID(),
        SEEDED_TENANT_ID,
        "ab".repeat(32),
      ]),
      (error: { code?: string }) => error.code === "42501",
    );
  } finally {
    await app.end();
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function post(
  url: string,
  path: string,
  body: unknown,
  headers: Record<string, string>,
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}

async function get(
  url: string,
  path: string,
  key: string,
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, { headers: { authorization: `Bearer ${key}` } });
  const text = await response.text();
  if (path.endsWith("/metrics")) return { status: response.status, body: text as unknown as Record<string, unknown> };
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}
