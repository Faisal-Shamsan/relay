import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { defineWorkflow, runActivation } from "@relay/sdk";
import { createTestDatabase, startServer, type StartedServer } from "relay-server";
import { startWorker, waitForFile } from "./spawn.js";

const entry = "packages/sdk/dist/src/stream-worker.js";

test("F23 a retried stream batch keeps one chunk and the same sequence", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const open = await openAttempt(server);
    const operationId = crypto.randomUUID();
    const first = await post(server.url, "/worker/v1/stream", streamBody(open, operationId, "0", "hello"));
    const retry = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "0", "hello"));
    const sameOperation = await post(server.url, "/worker/v1/stream", streamBody(open, operationId, "0", "hello"));
    assert.equal(first.status, 200);
    assert.equal(retry.status, 200);
    assert.equal(sameOperation.status, 200);
    assert.equal(retry.body.streamSeq, first.body.streamSeq);
    assert.equal(sameOperation.body.streamSeq, first.body.streamSeq);
    assert.equal(retry.body.batchSeq, "0");
    const stored = await admin.query<{ chunks: string; last: string; next: string }>(
      `SELECT
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $1 AND e.kind = 'chunk') AS chunks,
         (SELECT last_batch_seq::text FROM step_attempts a WHERE a.run_id = $1 AND a.step_key = 'only' AND a.attempt = 1) AS last,
         (SELECT next_stream_seq::text FROM runs r WHERE r.id = $1) AS next
       `,
      [open.runId],
    );
    assert.equal(stored.rows[0]?.chunks, "1");
    assert.equal(stored.rows[0]?.last, "0");
    assert.equal(stored.rows[0]?.next, "2");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F24 a conflicting batch and a gap write nothing", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const open = await openAttempt(server);
    const gap = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "1", "skipped"));
    assert.equal(gap.status, 409);
    assert.equal(gap.body.code, "StreamGap");
    const first = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "0", "hello"));
    assert.equal(first.status, 200);
    const conflict = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "0", "other"));
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.code, "StreamBatchConflict");
    const laterGap = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "2", "later"));
    assert.equal(laterGap.status, 409);
    assert.equal(laterGap.body.code, "StreamGap");
    const stored = await admin.query<{ chunks: string; text: string; next: string }>(
      `SELECT
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $1) AS chunks,
         (SELECT payload->>'text' FROM run_stream_events e WHERE e.run_id = $1 AND e.kind = 'chunk') AS text,
         (SELECT next_stream_seq::text FROM runs r WHERE r.id = $1) AS next
       `,
      [open.runId],
    );
    assert.equal(stored.rows[0]?.chunks, "1");
    assert.equal(stored.rows[0]?.text, "hello");
    assert.equal(stored.rows[0]?.next, "2");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F26 completion rejects a final batch behind the committed buffer", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const open = await openAttempt(server);
    const batch = await post(server.url, "/worker/v1/stream", streamBody(open, crypto.randomUUID(), "0", "hello"));
    assert.equal(batch.status, 200);
    const behind = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: open.sessionId,
      runId: open.runId,
      fence: open.fence,
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: open.attempt,
        attemptToken: open.attemptToken,
        result: { ok: true },
        finalBatchSeq: "-1",
      },
    });
    assert.equal(behind.status, 409);
    assert.equal(behind.body.code, "StreamManifest");
    const stored = await admin.query<{ completed: string; state: string }>(
      `SELECT
         (SELECT count(*)::text FROM run_events e WHERE e.run_id = $1 AND e.type = 'StepCompleted') AS completed,
         (SELECT state FROM step_attempts a WHERE a.run_id = $1 AND a.step_key = 'only' AND a.attempt = 1) AS state
       `,
      [open.runId],
    );
    assert.equal(stored.rows[0]?.completed, "0");
    assert.equal(stored.rows[0]?.state, "running");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F26 the sdk flushes the buffer before it sends completion", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-stream-"));
  const completionRequestPath = join(dir, "completion.json");
  try {
    const runId = await prepareRun(server.url);
    const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("only", async (stepCtx) => {
        await stepCtx.emit("hello");
        return { ok: true };
      });
    });
    await runActivation({ serverUrl: server.url, workflow, completionRequestPath });
    const recorded = JSON.parse(readFileSync(completionRequestPath, "utf8")) as {
      intent: { finalBatchSeq?: string };
    };
    assert.equal(recorded.intent.finalBatchSeq, "0");
    const stored = await admin.query<{ chunks: string; controls: string; ok: boolean }>(
      `SELECT
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $1 AND e.kind = 'chunk') AS chunks,
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $1 AND e.kind = 'step_completed') AS controls,
         (relay_verify_projections($1)->>'ok')::boolean AS ok
       `,
      [runId],
    );
    assert.equal(stored.rows[0]?.chunks, "1");
    assert.equal(stored.rows[0]?.controls, "1");
    assert.equal(stored.rows[0]?.ok, true);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("an emit larger than 4 KiB splits on a UTF-8 boundary", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-stream-"));
  const completionRequestPath = join(dir, "completion.json");
  try {
    const runId = await prepareRun(server.url);
    const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("only", async (stepCtx) => {
        await stepCtx.emit("a".repeat(5000));
        return { ok: true };
      });
    });
    await runActivation({ serverUrl: server.url, workflow, completionRequestPath });
    const recorded = JSON.parse(readFileSync(completionRequestPath, "utf8")) as {
      intent: { finalBatchSeq?: string };
    };
    assert.equal(recorded.intent.finalBatchSeq, "1");
    const chunks = await admin.query<{ batch: string; bytes: number }>(
      `SELECT batch_seq::text AS batch, octet_length(payload->>'text')::int AS bytes
       FROM run_stream_events WHERE run_id = $1 AND kind = 'chunk' ORDER BY batch_seq`,
      [runId],
    );
    assert.deepEqual(chunks.rows, [
      { batch: "0", bytes: 4096 },
      { batch: "1", bytes: 904 },
    ]);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F27 a crash after completion leaves the committed chunks and does not restart", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-stream-"));
  try {
    const runId = await prepareRun(server.url);
    const failpointFile = join(dir, "failpoints.json");
    writeFileSync(failpointFile, JSON.stringify({
      points: [{
        point: "step.after-completion-commit-before-response",
        match: { runId, stepKey: "only", attempt: 1 },
        occurrence: 1,
        action: "crash-process",
      }],
    }));
    const crashed = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "hello",
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: join(dir, "barrier"),
    }, entry);
    const first = await crashed.done;
    assert.equal(first.code, 78, first.stderr);
    const committed = await streamRows(admin, runId);
    assert.equal(committed.filter((row) => row.kind === "chunk" && row.attempt === 1).length, 1);
    assert.equal(committed.filter((row) => row.kind === "step_completed" && row.attempt === 1).length, 1);
    await expire(admin, runId);
    const recovered = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "hello",
    }, entry);
    const second = await recovered.done;
    assert.equal(second.code, 0, second.stderr);
    const lines = readFileSync(join(dir, "invocations.log"), "utf8").trim().split("\n");
    assert.equal(lines.length, 1);
    const after = await streamRows(admin, runId);
    assert.equal(after.filter((row) => row.kind === "superseded").length, 0);
    assert.equal(after.filter((row) => row.kind === "chunk").length, 1);
    const attempts = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM step_attempts WHERE run_id = $1`,
      [runId],
    );
    assert.equal(attempts.rows[0]?.count, "1");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F25 a crash after the final flush supersedes only that attempt", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-stream-"));
  try {
    const runId = await prepareRun(server.url);
    const failpointFile = join(dir, "failpoints.json");
    writeFileSync(failpointFile, JSON.stringify({
      points: [{
        point: "stream.after-final-flush-before-completion",
        match: { runId, stepKey: "only", attempt: 1 },
        occurrence: 1,
        action: "crash-process",
      }],
    }));
    const crashed = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "hello",
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: join(dir, "barrier"),
    }, entry);
    const first = await crashed.done;
    assert.equal(first.code, 78, first.stderr);
    const committed = await streamRows(admin, runId);
    assert.equal(committed.filter((row) => row.kind === "chunk" && row.attempt === 1).length, 1);
    assert.equal(committed.some((row) => row.kind === "step_completed"), false);
    await expire(admin, runId);
    const recovered = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "again",
    }, entry);
    const second = await recovered.done;
    assert.equal(second.code, 0, second.stderr);
    const lines = readFileSync(join(dir, "invocations.log"), "utf8").trim().split("\n").map((line) => JSON.parse(line) as { attempt: number });
    assert.deepEqual(lines.map((line) => line.attempt), [1, 2]);
    const after = await streamRows(admin, runId);
    assert.deepEqual(
      after.filter((row) => row.kind === "superseded").map((row) => row.attempt),
      [1],
    );
    assert.equal(after.filter((row) => row.kind === "chunk" && row.attempt === 1).length, 1);
    assert.equal(after.filter((row) => row.kind === "chunk" && row.attempt === 2).length, 1);
    assert.deepEqual(
      after.filter((row) => row.kind === "step_completed").map((row) => row.attempt),
      [2],
    );
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F28 supersession keeps the sibling chunks and lets the new attempt append", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-stream-"));
  const barrierDir = join(dir, "barrier");
  let pausedChild: { kill: () => boolean } | undefined;
  try {
    const runId = await prepareRun(server.url);
    const failpointFile = join(dir, "failpoints.json");
    writeFileSync(failpointFile, JSON.stringify({
      points: [{
        point: "stream.after-final-flush-before-completion",
        match: { runId, stepKey: "a", attempt: 1 },
        occurrence: 1,
        action: "pause-on-barrier",
      }],
    }));
    const paused = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "from-a",
      RELAY_STREAM_MODE: "parallel",
      RELAY_FAILPOINT_FILE: failpointFile,
      RELAY_BARRIER_DIR: barrierDir,
    }, entry);
    pausedChild = paused.child;
    await waitForFile(join(barrierDir, "hit.json"));
    await waitForKind(admin, runId, "b", "step_completed");
    paused.child.kill();
    const stopped = await paused.done;
    assert.notEqual(stopped.code, 0);
    await expire(admin, runId);
    const recovered = startWorker({
      RELAY_SERVER_URL: server.url,
      RELAY_INVOCATION_LOG: join(dir, "invocations.log"),
      RELAY_STREAM_TEXT: "from-a-2",
      RELAY_STREAM_MODE: "parallel",
    }, entry);
    const second = await recovered.done;
    assert.equal(second.code, 0, second.stderr);
    const lines = readFileSync(join(dir, "invocations.log"), "utf8").trim().split("\n").map((line) => JSON.parse(line) as { step: string; attempt: number });
    assert.equal(lines.filter((line) => line.step === "a").length, 2);
    assert.equal(lines.filter((line) => line.step === "b").length, 1);
    const after = await streamRows(admin, runId);
    const superseded = after.filter((row) => row.kind === "superseded");
    assert.deepEqual(superseded.map((row) => `${row.stepKey}:${row.attempt}`), ["a:1"]);
    assert.equal(after.filter((row) => row.kind === "chunk" && row.stepKey === "a" && row.attempt === 1 && row.text === "from-a").length, 1);
    assert.equal(after.filter((row) => row.kind === "chunk" && row.stepKey === "b" && row.attempt === 1 && row.text === "from-a").length, 1);
    assert.equal(after.filter((row) => row.kind === "chunk" && row.stepKey === "a" && row.attempt === 2 && row.text === "from-a-2").length, 1);
    assert.equal(after.filter((row) => row.kind === "step_completed" && row.stepKey === "b" && row.attempt === 1).length, 1);
    assert.equal(after.filter((row) => row.kind === "step_completed" && row.stepKey === "a").map((row) => row.attempt).join(","), "2");
    const verified = await admin.query<{ ok: boolean }>(
      `SELECT (relay_verify_projections($1)->>'ok')::boolean AS ok`,
      [runId],
    );
    assert.equal(verified.rows[0]?.ok, true);
  } finally {
    pausedChild?.kill();
    await admin.end();
    await server.close();
    await db.drop();
  }
});

interface OpenAttempt {
  sessionId: string;
  runId: string;
  fence: string;
  attempt: number;
  attemptToken: string;
}

async function openAttempt(server: StartedServer): Promise<OpenAttempt> {
  const sessionId = crypto.randomUUID();
  await post(server.url, "/worker/v1/register", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: sessionId,
    tenantId: SEEDED_TENANT_ID,
    workflows: [{ name: "demo", version: "demo-v1" }],
  });
  const created = await post(server.url, "/v1/runs", {
    workflowName: "demo",
    workflowVersion: "demo-v1",
    input: { n: 1 },
  });
  assert.equal(created.status, 201);
  const lease = await post(server.url, "/worker/v1/lease", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: sessionId,
  });
  assert.equal(lease.status, 200);
  const started = await post(server.url, "/worker/v1/events", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: sessionId,
    runId: created.body.runId,
    fence: lease.body.fence,
    intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
  });
  assert.equal(started.status, 200);
  return {
    sessionId,
    runId: String(created.body.runId),
    fence: String(lease.body.fence),
    attempt: Number(started.body.attempt),
    attemptToken: String(started.body.attemptToken),
  };
}

function streamBody(open: OpenAttempt, operationId: string, batchSeq: string, text: string): unknown {
  return {
    protocolVersion: 1,
    operationId,
    workerSessionId: open.sessionId,
    runId: open.runId,
    fence: open.fence,
    stepKey: "only",
    attempt: open.attempt,
    attemptToken: open.attemptToken,
    batchSeq,
    text,
  };
}

async function prepareRun(url: string): Promise<string> {
  await post(url, "/worker/v1/register", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: crypto.randomUUID(),
    tenantId: SEEDED_TENANT_ID,
    workflows: [{ name: "demo", version: "demo-v1" }],
  });
  const created = await post(url, "/v1/runs", {
    workflowName: "demo",
    workflowVersion: "demo-v1",
    input: { n: 1 },
  });
  assert.equal(created.status, 201);
  return String(created.body.runId);
}

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}

async function expire(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}

interface StreamRow {
  stepKey: string | null;
  attempt: number | null;
  kind: string;
  text: string | null;
}

async function streamRows(admin: pg.Client, runId: string): Promise<StreamRow[]> {
  const result = await admin.query<StreamRow>(
    `SELECT step_key AS "stepKey", attempt, kind, payload->>'text' AS text
     FROM run_stream_events WHERE run_id = $1 ORDER BY seq`,
    [runId],
  );
  return result.rows;
}

async function waitForKind(admin: pg.Client, runId: string, stepKey: string, kind: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const row = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_stream_events
       WHERE run_id = $1 AND step_key = $2 AND kind = $3`,
      [runId, stepKey, kind],
    );
    if (row.rows[0]?.count === "1") return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${stepKey} ${kind}`);
}
