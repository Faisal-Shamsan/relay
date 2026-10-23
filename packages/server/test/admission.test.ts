import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

test("F17 identical triggers share one run and a different intent conflicts", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [
        { name: "demo", version: "demo-v1" },
        { name: "demo", version: "demo-v2" },
      ],
    });
    const first = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    }, { "idempotency-key": "order-1" });
    assert.equal(first.status, 201);
    const replay = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    }, { "idempotency-key": "order-1" });
    assert.equal(replay.status, 200);
    assert.equal(replay.body.runId, first.body.runId);

    const [left, right] = await Promise.all([
      post(server.url, "/v1/runs", {
        workflowName: "demo",
        workflowVersion: "demo-v1",
        input: { n: 2 },
      }, { "idempotency-key": "order-2" }),
      post(server.url, "/v1/runs", {
        workflowName: "demo",
        workflowVersion: "demo-v1",
        input: { n: 2 },
      }, { "idempotency-key": "order-2" }),
    ]);
    assert.equal(left.body.runId, right.body.runId);
    assert.deepEqual([left.status, right.status].sort(), [200, 201]);

    const conflict = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 3 },
    }, { "idempotency-key": "order-1" });
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.code, "IdempotencyConflict");
    const otherVersion = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v2",
      input: { n: 1 },
    }, { "idempotency-key": "order-1" });
    assert.equal(otherVersion.status, 409);

    const plainA = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    const plainB = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    assert.equal(plainA.status, 201);
    assert.equal(plainB.status, 201);
    assert.notEqual(plainA.body.runId, plainB.body.runId);

    const runs = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM runs WHERE idempotency_key = 'order-1'`,
    );
    assert.equal(runs.rows[0]?.count, "1");
    const created = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'RunCreated'`,
      [first.body.runId],
    );
    assert.equal(created.rows[0]?.count, "1");
    const stored = await admin.query<{ input: { n: number } }>(
      `SELECT input FROM runs WHERE id = $1`,
      [first.body.runId],
    );
    assert.deepEqual(stored.rows[0]?.input, { n: 1 });
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F19 a removed v1 worker leaves the pinned run waiting instead of running on v2", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const v1 = randomUUID();
    const v2 = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v2,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v2" }],
    });
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    const runId = String(created.body.runId);
    await admin.query(
      `UPDATE worker_sessions SET expires_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`,
      [v1],
    );

    const removed = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v1,
    });
    assert.equal(removed.status, 404);
    const wrong = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: v2,
    });
    assert.equal(wrong.status, 404);
    const waiting = await admin.query<{ state: string; owner: string | null }>(
      `SELECT r.state, q.lease_owner::text AS owner
       FROM runs r JOIN run_queue q ON q.run_id = r.id
       WHERE r.id = $1`,
      [runId],
    );
    assert.equal(waiting.rows[0]?.state, "pending");
    assert.equal(waiting.rows[0]?.owner, null);

    const listed = await fetch(`${server.url}/v1/workflows`);
    assert.equal(listed.status, 200);
    const body = await listed.json() as {
      workflows: Array<{ name: string; version: string; compatibleWorkers: number; pendingRuns: number }>;
    };
    const pinned = body.workflows.find((row) => row.name === "demo" && row.version === "demo-v1");
    const next = body.workflows.find((row) => row.name === "demo" && row.version === "demo-v2");
    assert.equal(pinned?.compatibleWorkers, 0);
    assert.equal(pinned?.pendingRuns, 1);
    assert.equal(next?.compatibleWorkers, 1);
    assert.equal(next?.pendingRuns, 0);

    const replacement = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: replacement,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    const leased = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: replacement,
    });
    assert.equal(leased.status, 200);
    assert.equal(leased.body.runId, runId);
    assert.equal(leased.body.workflowVersion, "demo-v1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F21 invalid state, a missing tenant, and direct writes do not persist", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  const app = new pg.Client({ connectionString: db.appUrl });
  await admin.connect();
  await app.connect();
  try {
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    const runId = String(created.body.runId);
    const version = await admin.query<{ id: string }>(
      `SELECT id::text AS id FROM workflow_versions WHERE name = 'demo' AND version = 'demo-v1'`,
    );
    const versionId = version.rows[0]?.id;
    const hash = "ab".repeat(32);

    await assert.rejects(
      () => app.query(`UPDATE runs SET state = 'completed', output = '{}'::jsonb, completed_at = clock_timestamp() WHERE id = $1`, [runId]),
      (error: { code?: string }) => error.code === "42501",
    );
    await assert.rejects(
      () => app.query(
        `INSERT INTO runs (id, tenant_id, workflow_version_id, state, input, trigger_hash)
         VALUES ($1, $2, $3, 'pending', '{}'::jsonb, $4)`,
        [randomUUID(), SEEDED_TENANT_ID, versionId, hash],
      ),
      (error: { code?: string }) => error.code === "42501",
    );
    const unchanged = await admin.query<{ state: string }>(`SELECT state FROM runs WHERE id = $1`, [runId]);
    assert.equal(unchanged.rows[0]?.state, "pending");

    const invalidId = randomUUID();
    await assert.rejects(
      () => admin.query(
        `INSERT INTO runs (id, tenant_id, workflow_version_id, state, input, trigger_hash)
         VALUES ($1, $2, $3, 'nope', '{}'::jsonb, $4)`,
        [invalidId, SEEDED_TENANT_ID, versionId, hash],
      ),
      (error: { code?: string }) => error.code === "23514",
    );
    const missingTenant = randomUUID();
    await assert.rejects(
      () => admin.query(
        `INSERT INTO runs (id, tenant_id, workflow_version_id, state, input, trigger_hash)
         VALUES ($1, $2, $3, 'pending', '{}'::jsonb, $4)`,
        [missingTenant, "22222222-2222-4222-8222-222222222222", versionId, hash],
      ),
      (error: { code?: string }) => error.code === "23503",
    );
    const leaked = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM runs WHERE id IN ($1, $2)`,
      [invalidId, missingTenant],
    );
    assert.equal(leaked.rows[0]?.count, "0");
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
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}
