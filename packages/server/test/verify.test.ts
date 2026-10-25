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

test("projection verification reports drift and does not change history", async () => {
  const db = await createTestDatabase();
  const app = new pg.Client({ connectionString: db.appUrl });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await app.connect();
  await admin.connect();
  try {
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
    const fence = lease.rows[0]!.result.fence;
    const started = await app.query<{ result: { attemptToken: string } }>(
      `SELECT relay_declare_and_start($1,$2,$3,$4::bigint,'only',0,$5) AS result`,
      [randomUUID(), sessionId, runId, fence, HASH],
    );
    await app.query(`SELECT relay_complete_step($1,$2,$3,$4::bigint,'only',1,$5,$6::jsonb,$7)`, [
      randomUUID(),
      sessionId,
      runId,
      fence,
      started.rows[0]!.result.attemptToken,
      JSON.stringify({ ok: true }),
      HASH,
    ]);
    await app.query(`SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6)`, [
      randomUUID(),
      sessionId,
      runId,
      fence,
      JSON.stringify({ ok: true }),
      HASH,
    ]);

    const cli = await runCli(["projections", "verify", runId], {
      ...process.env,
      RELAY_DATABASE_URL: db.appUrl,
    });
    assert.equal(cli.code, 0, cli.stderr);
    assert.equal(JSON.parse(cli.stdout).ok, true);

    const before = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1`,
      [runId],
    );
    await admin.query(`UPDATE steps SET output = '{"tampered":true}'::jsonb WHERE run_id = $1`, [runId]);
    const drifted = await verifyProjections(db.appUrl, runId);
    assert.equal(drifted.ok, false);
    assert.equal(drifted.mismatches.some((row) => {
      const mismatch = row as { check?: string };
      return mismatch.check === "step-completed";
    }), true);
    const after = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1`,
      [runId],
    );
    assert.equal(after.rows[0]?.count, before.rows[0]?.count);
  } finally {
    await app.end();
    await admin.end();
    await db.drop();
  }
});

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
