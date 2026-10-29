import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase } from "../src/index.js";

const HASH = "ab".repeat(32);

test("same-priority grants rotate tenants and a higher priority still wins", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  const tenantB = randomUUID();
  const sessionId = randomUUID();
  try {
    await admin.query(`INSERT INTO tenants (id, name) VALUES ($1, 'other')`, [tenantB]);
    await app.query(`SELECT relay_register_worker($1,$2,$3,$4,1,$5::jsonb)`, [
      SEEDED_TENANT_ID,
      sessionId,
      randomUUID(),
      HASH,
      JSON.stringify([{ name: "demo", version: "demo-v1" }]),
    ]);
    const versionB = randomUUID();
    await admin.query(
      `INSERT INTO workflow_versions (id, tenant_id, name, version, protocol_version)
       VALUES ($1, $2, 'demo', 'demo-v1', 1)`,
      [versionB, tenantB],
    );
    await admin.query(
      `INSERT INTO worker_session_versions (worker_session_id, workflow_version_id, tenant_id)
       VALUES ($1, $2, $3)`,
      [sessionId, versionB, tenantB],
    );

    const seededRuns = [await createRun(app, SEEDED_TENANT_ID), await createRun(app, SEEDED_TENANT_ID), await createRun(app, SEEDED_TENANT_ID)];
    const otherRun = await createRun(app, tenantB);
    const first = await grant(app, sessionId);
    const second = await grant(app, sessionId);
    assert.equal(seededRuns.includes(first), true);
    assert.equal(second, otherRun);

    const high = await createRun(app, SEEDED_TENANT_ID);
    const low = await createRun(app, tenantB);
    await admin.query(`UPDATE run_queue SET priority = 5 WHERE run_id = $1`, [high]);
    await admin.query(
      `UPDATE tenants SET last_granted_at = clock_timestamp() - interval '1 hour' WHERE id = $1`,
      [tenantB],
    );
    await admin.query(`UPDATE tenants SET last_granted_at = clock_timestamp() WHERE id = $1`, [SEEDED_TENANT_ID]);
    assert.equal(await grant(app, sessionId), high);
    assert.equal(low === high, false);

    const quiet = await createRun(app, SEEDED_TENANT_ID);
    await admin.query(`UPDATE tenants SET last_granted_at = '2020-01-01T00:00:00Z' WHERE id = $1`, [SEEDED_TENANT_ID]);
    const rebuilt = await admin.query(`SELECT relay_rebuild_projections($1) AS result`, [quiet]);
    assert.equal(rebuilt.rows[0]?.result.ok, true);
    const stamp = await admin.query<{ at: string }>(
      `SELECT last_granted_at::text AS at FROM tenants WHERE id = $1`,
      [SEEDED_TENANT_ID],
    );
    assert.equal(stamp.rows[0]?.at.startsWith("2020-01-01"), true);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

async function createRun(app: pg.Client, tenantId: string): Promise<string> {
  const runId = randomUUID();
  await app.query(`SELECT relay_create_run($1,'demo','demo-v1',$2::jsonb,$3,$4)`, [
    tenantId,
    JSON.stringify({ n: 1 }),
    HASH,
    runId,
  ]);
  return runId;
}

async function grant(app: pg.Client, sessionId: string): Promise<string> {
  const result = await app.query<{ result: { runId: string } }>(
    `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
    [sessionId, randomUUID(), HASH],
  );
  return result.rows[0]!.result.runId;
}
