import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { logicalStepIdempotencyKey, SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { startWorker } from "./spawn.js";

test("F05 effect without provider dedupe is applied again on the next attempt", { timeout: 120_000 }, async () => {
  await proveCrashBeforeCompletion(false);
});

test("F05 effect with provider dedupe is applied once across two callbacks", { timeout: 120_000 }, async () => {
  await proveCrashBeforeCompletion(true);
});

async function proveCrashBeforeCompletion(dedupe: boolean): Promise<void> {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-effects-"));
  const invocationLog = join(dir, "invocations.log");
  const providerLog = join(dir, "provider.log");
  try {
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
    const created = await fetch(`${server.url}/v1/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: { n: 1 } }),
    });
    assert.equal(created.status, 201);
    const runId = String((await created.json() as { runId: string }).runId);
    const key = logicalStepIdempotencyKey(SEEDED_TENANT_ID, runId, "only");
    const failA = join(dir, "fail-a.json");
    writeFileSync(failA, JSON.stringify({
      points: [
        {
          point: "step.after-user-function-before-completion",
          match: { runId, stepKey: "only", attempt: 1 },
          occurrence: 1,
          action: "crash-process",
        },
      ],
    }));
    const workerA = startWorker(workerEnv({
      serverUrl: server.url,
      invocationLog,
      providerLog,
      dedupe,
      failpointFile: failA,
    }));
    const crashedA = await workerA.done;
    assert.equal(crashedA.code, 78, crashedA.stderr);
    const completed = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completed.rows[0]?.count, "0");
    const afterCrash = readProvider(providerLog);
    assert.deepEqual(afterCrash.map((row) => row.kind), ["invocation", "effect"]);
    assert.equal(afterCrash[0]?.idempotencyKey, key);
    assert.equal(afterCrash[1]?.idempotencyKey, key);
    const callbacksAfterCrash = readCallbacks(invocationLog);
    assert.equal(callbacksAfterCrash.length, 1);
    assert.equal(callbacksAfterCrash[0]?.idempotencyKey, key);

    await waitUntilExpired(admin, runId);
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const workerB = startWorker(workerEnv({
      serverUrl: server.url,
      invocationLog,
      providerLog,
      dedupe,
    }));
    const finishedB = await workerB.done;
    assert.equal(finishedB.code, 0, finishedB.stderr);

    const rows = readProvider(providerLog);
    const invocations = rows.filter((row) => row.kind === "invocation");
    const effects = rows.filter((row) => row.kind === "effect");
    assert.equal(invocations.length + effects.length, rows.length);
    assert.equal(invocations.length, 2);
    assert.equal(invocations[0]?.attempt, 1);
    assert.equal(invocations[1]?.attempt, 2);
    assert.notEqual(invocations[0]?.attemptToken, invocations[1]?.attemptToken);
    for (const row of invocations) assert.equal(row.idempotencyKey, key);
    const callbacks = readCallbacks(invocationLog);
    assert.equal(callbacks.length, 2);
    assert.equal(callbacks[0]?.idempotencyKey, key);
    assert.equal(callbacks[1]?.idempotencyKey, key);
    if (dedupe) {
      assert.equal(effects.length, 1);
      assert.equal(effects[0]?.idempotencyKey, key);
    } else {
      assert.equal(effects.length, 2);
      assert.equal(effects[0]?.idempotencyKey, key);
      assert.equal(effects[1]?.idempotencyKey, key);
      assert.notEqual(effects[0]?.attemptToken, effects[1]?.attemptToken);
    }
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
}

function workerEnv(input: {
  serverUrl: string;
  invocationLog: string;
  providerLog: string;
  dedupe: boolean;
  failpointFile?: string;
}): NodeJS.ProcessEnv {
  return {
    RELAY_SERVER_URL: input.serverUrl,
    RELAY_INVOCATION_LOG: input.invocationLog,
    RELAY_PROVIDER_LOG: input.providerLog,
    RELAY_PROVIDER_DEDUPE: input.dedupe ? "1" : undefined,
    RELAY_FAILPOINT_FILE: input.failpointFile,
    RELAY_ALLOW_FAILPOINTS: undefined,
    RELAY_BARRIER_DIR: undefined,
  };
}

interface ProviderRecord {
  kind: string;
  attempt: number;
  attemptToken: string;
  idempotencyKey: string;
}

function readProvider(path: string): ProviderRecord[] {
  return readFileSync(path, "utf8")
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as ProviderRecord);
}

function readCallbacks(path: string): { attempt: number; idempotencyKey: string }[] {
  return readFileSync(path, "utf8")
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as { attempt: number; idempotencyKey: string });
}

async function waitUntilExpired(admin: pg.Client, runId: string): Promise<void> {
  const deadline = Date.now() + 45_000;
  while (Date.now() < deadline) {
    const row = await admin.query<{ due: boolean }>(
      `SELECT clock_timestamp() >= lease_expires_at AS due FROM run_queue WHERE run_id = $1`,
      [runId],
    );
    if (row.rows[0]?.due) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("lease did not expire");
}
