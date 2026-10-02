import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { logicalStepIdempotencyKey, SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { startWorker, waitForFile } from "./spawn.js";

test("M1 exit: crash, expire, second attempt, replay, and duplicate completion receipt", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 400 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-exit-"));
  const invocationLog = join(dir, "invocations.log");
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
    const createdRow = await admin.query<{ state: string; fence: string; queue: string; owner: string | null; event: string }>(
      `SELECT r.state, r.current_fence::text AS fence, q.state AS queue, q.lease_owner::text AS owner,
              (SELECT type FROM run_events WHERE run_id = r.id AND seq = 1) AS event
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(createdRow.rows[0]?.event, "RunCreated");
    assert.equal(createdRow.rows[0]?.state, "pending");
    assert.equal(createdRow.rows[0]?.queue, "ready");
    assert.equal(createdRow.rows[0]?.fence, "0");
    assert.equal(createdRow.rows[0]?.owner, null);

    const barrierA = join(dir, "barrier-a");
    const failA = join(dir, "fail-a.json");
    writeFileSync(failA, JSON.stringify({
      points: [
        { point: "step.after-start-commit-before-callback", match: { runId, stepKey: "only", attempt: 1 }, occurrence: 1, action: "pause-on-barrier" },
        { point: "step.after-user-function-before-completion", match: { runId, stepKey: "only", attempt: 1 }, occurrence: 1, action: "crash-process" },
      ],
    }));
    const workerA = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: invocationLog,
      RELAY_FAILPOINT_FILE: failA,
      RELAY_BARRIER_DIR: barrierA,
    });
    await waitForFile(join(barrierA, "hit.json"));
    const firstLease = await admin.query<{
      fence: string;
      queue: string;
      state: string;
      matched: boolean;
      started: string;
    }>(
      `SELECT r.current_fence::text AS fence, q.state AS queue, r.state,
              q.lease_expires_at = ((e.payload->>'databaseNow')::timestamptz + interval '400 milliseconds') AS matched,
              (SELECT count(*)::text FROM run_events WHERE run_id = r.id AND type = 'StepStarted' AND attempt = 1) AS started
       FROM runs r
       JOIN run_queue q ON q.run_id = r.id
       JOIN run_events e ON e.run_id = r.id AND e.type = 'LeaseGranted' AND e.fence = 1
       WHERE r.id = $1`,
      [runId],
    );
    assert.equal(firstLease.rows[0]?.fence, "1");
    assert.equal(firstLease.rows[0]?.queue, "leased");
    assert.equal(firstLease.rows[0]?.state, "running");
    assert.equal(firstLease.rows[0]?.matched, true);
    assert.equal(firstLease.rows[0]?.started, "1");
    assert.equal(existsSync(invocationLog), false);
    writeFileSync(join(barrierA, "release"), "1");
    const crashedA = await workerA.done;
    assert.equal(crashedA.code, 78, crashedA.stderr);
    const afterCrash = await admin.query<{ completed: string; attempt: string; fence: string; queue: string }>(
      `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'StepCompleted') AS completed,
              (SELECT state FROM step_attempts WHERE run_id = $1 AND attempt = 1) AS attempt,
              r.current_fence::text AS fence,
              q.state AS queue
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(afterCrash.rows[0]?.completed, "0");
    assert.equal(afterCrash.rows[0]?.attempt, "running");
    assert.equal(afterCrash.rows[0]?.fence, "1");
    assert.equal(afterCrash.rows[0]?.queue, "leased");

    await waitUntilExpired(admin, runId);
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const expired = await admin.query<{ state: string; queue: string; fence: string; owner: string | null; event: string }>(
      `SELECT r.state, q.state AS queue, r.current_fence::text AS fence, q.lease_owner::text AS owner,
              (SELECT count(*)::text FROM run_events WHERE run_id = r.id AND type = 'LeaseExpired') AS event
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(expired.rows[0]?.state, "pending");
    assert.equal(expired.rows[0]?.queue, "ready");
    assert.equal(expired.rows[0]?.fence, "1");
    assert.equal(expired.rows[0]?.owner, null);
    assert.equal(expired.rows[0]?.event, "1");

    const barrierB = join(dir, "barrier-b");
    const failB = join(dir, "fail-b.json");
    const completionPath = join(dir, "completion.json");
    writeFileSync(failB, JSON.stringify({
      points: [
        { point: "step.after-start-commit-before-callback", match: { runId, stepKey: "only", attempt: 2 }, occurrence: 1, action: "pause-on-barrier" },
        { point: "step.after-completion-commit-before-response", match: { runId, stepKey: "only", attempt: 2 }, occurrence: 1, action: "crash-process" },
      ],
    }));
    const workerB = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: invocationLog,
      RELAY_FAILPOINT_FILE: failB,
      RELAY_BARRIER_DIR: barrierB,
      RELAY_COMPLETION_REQUEST_PATH: completionPath,
    });
    await waitForFile(join(barrierB, "hit.json"));
    const secondStart = await admin.query<{ fence: string; abandoned: string; started: string; lines: string }>(
      `SELECT r.current_fence::text AS fence,
              (SELECT count(*)::text FROM run_events WHERE run_id = r.id AND type = 'StepAttemptAbandoned') AS abandoned,
              (SELECT count(*)::text FROM run_events WHERE run_id = r.id AND type = 'StepStarted' AND attempt = 2) AS started,
              '0' AS lines
       FROM runs r WHERE r.id = $1`,
      [runId],
    );
    assert.equal(secondStart.rows[0]?.fence, "2");
    assert.equal(secondStart.rows[0]?.abandoned, "1");
    assert.equal(secondStart.rows[0]?.started, "1");
    assert.equal(readFileSync(invocationLog, "utf8").trim().split("\n").length, 1);
    writeFileSync(join(barrierB, "release"), "1");
    const crashedB = await workerB.done;
    assert.equal(crashedB.code, 78, crashedB.stderr);
    const afterComplete = await admin.query<{ state: string; queue: string; completed: string }>(
      `SELECT r.state, q.state AS queue,
              (SELECT count(*)::text FROM run_events WHERE run_id = r.id AND type = 'StepCompleted') AS completed
       FROM runs r JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(afterComplete.rows[0]?.state, "running");
    assert.equal(afterComplete.rows[0]?.queue, "leased");
    assert.equal(afterComplete.rows[0]?.completed, "1");
    const linesAfterB = readFileSync(invocationLog, "utf8").trim().split("\n").map((line) => JSON.parse(line) as { attempt: number; idempotencyKey: string });
    assert.equal(linesAfterB.length, 2);
    assert.equal(linesAfterB[0]?.idempotencyKey, linesAfterB[1]?.idempotencyKey);
    assert.equal(linesAfterB[1]?.idempotencyKey, logicalStepIdempotencyKey(SEEDED_TENANT_ID, runId, "only"));

    await waitUntilExpired(admin, runId);
    await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
    const fenceBeforeReplay = await admin.query<{ fence: string }>(
      `SELECT current_fence::text AS fence FROM runs WHERE id = $1`,
      [runId],
    );
    assert.equal(fenceBeforeReplay.rows[0]?.fence, "2");
    const workerC = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: invocationLog,
    });
    const finished = await workerC.done;
    assert.equal(finished.code, 0, finished.stderr);
    const replay = await admin.query<{ fence: string; queue: string | null; state: string }>(
      `SELECT r.current_fence::text AS fence, r.state, q.run_id::text AS queue
       FROM runs r LEFT JOIN run_queue q ON q.run_id = r.id WHERE r.id = $1`,
      [runId],
    );
    assert.equal(replay.rows[0]?.fence, "3");
    assert.equal(replay.rows[0]?.state, "completed");
    assert.equal(replay.rows[0]?.queue, null);
    assert.equal(readFileSync(invocationLog, "utf8").trim().split("\n").length, 2);

    const completion = JSON.parse(readFileSync(completionPath, "utf8")) as unknown;
    const duplicate = await fetch(`${server.url}/worker/v1/events`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(completion),
    });
    assert.equal(duplicate.status, 200);
    assert.equal(readFileSync(invocationLog, "utf8").trim().split("\n").length, 2);
    const stillOne = await admin.query<{ count: string; seq: string }>(
      `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1 AND type = 'StepCompleted') AS count,
              (SELECT max(seq)::text FROM run_events WHERE run_id = $1) AS seq`,
      [runId],
    );
    const beforeDuplicateSeq = stillOne.rows[0]?.seq;
    assert.equal(stillOne.rows[0]?.count, "1");

    const totals = await admin.query<{ type: string; count: string }>(
      `SELECT type, count(*)::text AS count FROM run_events WHERE run_id = $1 GROUP BY type`,
      [runId],
    );
    const counts = Object.fromEntries(totals.rows.map((row) => [row.type, row.count]));
    assert.equal(counts.RunCreated, "1");
    assert.equal(counts.LeaseGranted, "3");
    assert.equal(counts.LeaseExpired, "2");
    assert.equal(counts.CommandRecorded, "1");
    assert.equal(counts.StepStarted, "2");
    assert.equal(counts.StepAttemptAbandoned, "1");
    assert.equal(counts.StepCompleted, "1");
    assert.equal(counts.RunCompleted, "1");
    const sequence = await admin.query<{ seq: string; next: string; fence: string; output: { ok: boolean } }>(
      `SELECT string_agg(seq::text, ',' ORDER BY seq) AS seq, r.next_event_seq::text AS next,
              r.current_fence::text AS fence, r.output
       FROM run_events e JOIN runs r ON r.id = e.run_id WHERE e.run_id = $1 GROUP BY r.next_event_seq, r.current_fence, r.output`,
      [runId],
    );
    assert.equal(sequence.rows[0]?.seq, "1,2,3,4,5,6,7,8,9,10,11,12");
    assert.equal(sequence.rows[0]?.next, "13");
    assert.equal(sequence.rows[0]?.fence, "3");
    assert.deepEqual(sequence.rows[0]?.output, { ok: true });
    assert.equal(beforeDuplicateSeq, "12");
    const attempts = await admin.query<{ attempt: number; state: string }>(
      `SELECT attempt, state FROM step_attempts WHERE run_id = $1 ORDER BY attempt`,
      [runId],
    );
    assert.deepEqual(attempts.rows, [
      { attempt: 1, state: "abandoned" },
      { attempt: 2, state: "completed" },
    ]);
    const receipt = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM operation_receipts WHERE run_id = $1 AND operation_id = $2`,
      [runId, (completion as { operationId: string }).operationId],
    );
    assert.equal(receipt.rows[0]?.count, "1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function waitUntilExpired(admin: pg.Client, runId: string): Promise<void> {
  const deadline = Date.now() + 5_000;
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
