import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { verifyProjections } from "../src/projections.js";
import { createTestDatabase } from "../src/testing.js";

const HASH = "ef".repeat(32);
const main = join(dirname(fileURLToPath(import.meta.url)), "../src/main.js");

test("F36 rebuild restores a tampered result and leaves coordination state", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const runId = await finishRun(app);
    const before = await coordination(admin, runId);
    const events = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1`,
      [runId],
    );
    await admin.query(`UPDATE steps SET output = '{"tampered":true}'::jsonb WHERE run_id = $1`, [runId]);
    const cli = await runCli(["projections", "rebuild", runId], {
      ...process.env,
      RELAY_ADMIN_URL: db.adminUrl,
    });
    assert.equal(cli.code, 0, cli.stderr);
    assert.equal(JSON.parse(cli.stdout).ok, true);
    const restored = await admin.query<{ output: { ok?: boolean } }>(
      `SELECT output FROM steps WHERE run_id = $1`,
      [runId],
    );
    assert.equal(restored.rows[0]?.output.ok, true);
    const verified = await verifyProjections(db.adminUrl, runId);
    assert.equal(verified.ok, true, JSON.stringify(verified.mismatches));
    const after = await coordination(admin, runId);
    assert.deepEqual(after, before);
    const again = await admin.query<{ result: { ok: boolean } }>(
      `SELECT relay_rebuild_projections($1) AS result`,
      [runId],
    );
    assert.equal(again.rows[0]?.result.ok, true);
    assert.deepEqual(await coordination(admin, runId), before);
    const eventsAfter = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1`,
      [runId],
    );
    assert.equal(eventsAfter.rows[0]?.count, events.rows[0]?.count);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

test("F36 a live lease is not rebuilt", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
    const runId = await leaseRun(app);
    const before = await coordination(admin, runId);
    await admin.query(`UPDATE steps SET retry_policy = '{"maxAttempts":2}'::jsonb WHERE run_id = $1`, [runId]);
    await assert.rejects(
      admin.query(`SELECT relay_rebuild_projections($1)`, [runId]),
      (error: Error) => error.message === "InvalidRunState",
    );
    const policy = await admin.query<{ policy: { maxAttempts?: number } }>(
      `SELECT retry_policy AS policy FROM steps WHERE run_id = $1`,
      [runId],
    );
    assert.equal(policy.rows[0]?.policy.maxAttempts, 2);
    assert.deepEqual(await coordination(admin, runId), before);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

async function finishRun(app: pg.Client): Promise<string> {
  const started = await leaseRun(app);
  const runId = started;
  const fence = await app.query<{ fence: string }>(
    `SELECT current_fence::text AS fence FROM runs WHERE id = $1`,
    [runId],
  );
  const attempt = await app.query<{ token: string }>(
    `SELECT attempt_token::text AS token FROM step_attempts WHERE run_id = $1`,
    [runId],
  );
  const session = await app.query<{ id: string }>(
    `SELECT lease_owner::text AS id FROM run_queue WHERE run_id = $1`,
    [runId],
  );
  await app.query(`SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`, [
    randomUUID(),
    session.rows[0]!.id,
    runId,
    fence.rows[0]!.fence,
    attempt.rows[0]!.token,
    JSON.stringify({ ok: true }),
    HASH,
  ]);
  await app.query(`SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6)`, [
    randomUUID(),
    session.rows[0]!.id,
    runId,
    fence.rows[0]!.fence,
    JSON.stringify({ ok: true }),
    HASH,
  ]);
  return runId;
}

async function leaseRun(app: pg.Client): Promise<string> {
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
  const lease = await app.query<{ result: { fence: string } }>(
    `SELECT relay_grant_lease($1,$2,$3, interval '30 seconds') AS result`,
    [sessionId, randomUUID(), HASH],
  );
  await app.query(`SELECT relay_declare_and_start($1,$2,$3,$4::bigint,'only',0,$5)`, [
    randomUUID(),
    sessionId,
    runId,
    lease.rows[0]!.result.fence,
    HASH,
  ]);
  return runId;
}

async function coordination(admin: pg.Client, runId: string): Promise<{
  fence: string;
  eventSeq: string;
  commandPos: string;
  stepOrdinal: string;
  streamSeq: string;
  receipts: string;
  queue: string | null;
}> {
  const row = await admin.query<{
    fence: string;
    eventSeq: string;
    commandPos: string;
    stepOrdinal: string;
    streamSeq: string;
    receipts: string;
    queue: string | null;
  }>(
    `SELECT r.current_fence::text AS fence,
            r.next_event_seq::text AS "eventSeq",
            r.next_command_pos::text AS "commandPos",
            r.next_step_ordinal::text AS "stepOrdinal",
            r.next_stream_seq::text AS "streamSeq",
            (SELECT count(*)::text FROM operation_receipts WHERE run_id = r.id) AS receipts,
            (SELECT state FROM run_queue WHERE run_id = r.id) AS queue
     FROM runs r
     WHERE r.id = $1`,
    [runId],
  );
  return row.rows[0]!;
}

function runCli(args: string[], env: NodeJS.ProcessEnv): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = spawn(process.execPath, [main, ...args], { env });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  return new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}
