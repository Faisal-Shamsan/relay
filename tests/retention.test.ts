import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { StreamExpiredError, subscribe, type StreamEvent } from "@relay/sdk";
import { createTestDatabase, startServer, type StartedServer } from "relay-server";

test("F32 an expired terminal stream returns 410 and keeps the run result", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    streamRetentionIntervalMs: 0,
    streamPollMs: 40,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const abort = new AbortController();
  try {
    const finished = await openAttempt(server);
    const operationId = crypto.randomUUID();
    const batch = await post(server.url, "/worker/v1/stream", streamBody(finished, operationId, "0", "hello"));
    assert.equal(batch.status, 200);
    const streamSeq = String(batch.body.streamSeq);
    await completeAndFinish(server.url, finished);
    const active = await openAttempt(server);
    const activeBatch = await post(server.url, "/worker/v1/stream", streamBody(active, crypto.randomUUID(), "0", "still-here"));
    assert.equal(activeBatch.status, 200);

    const seen: StreamEvent[] = [];
    const reading = (async () => {
      for await (const event of subscribe({ serverUrl: server.url, runId: finished.runId, signal: abort.signal })) {
        seen.push(event);
      }
    })();
    await waitFor(() => seen.some((event) => event.kind === "chunk" && event.data.text === "hello"));
    const before = await admin.query<{ next: string }>(
      `SELECT next_stream_seq::text AS next FROM runs WHERE id = $1`,
      [finished.runId],
    );
    const nextBefore = before.rows[0]?.next;
    const expired = await admin.query<{ result: string }>(
      `SELECT relay_expire_streams(interval '0 milliseconds') AS result`,
    );
    assert.equal(expired.rows[0]?.result, "expired");

    await assert.rejects(reading, (error: unknown) => {
      assert.ok(error instanceof StreamExpiredError);
      assert.equal(error.state, "completed");
      assert.deepEqual(error.output, { ok: true });
      return true;
    });
    assert.ok(seen.some((event) => event.kind === "chunk" && event.data.text === "hello"));

    const again = await admin.query<{ result: string }>(
      `SELECT relay_expire_streams(interval '0 milliseconds') AS result`,
    );
    assert.equal(again.rows[0]?.result, "none");

    await assert.rejects(
      (async () => {
        for await (const _event of subscribe({ serverUrl: server.url, runId: finished.runId })) {
          assert.fail("expired stream yielded a retained event");
        }
      })(),
      (error: unknown) => error instanceof StreamExpiredError,
    );

    const late = await fetch(`${server.url}/v1/runs/${finished.runId}/stream?after=1`);
    const lateBody = JSON.parse(await late.text()) as { code: string; details: { output: unknown } };
    assert.equal(late.status, 410);
    assert.equal(lateBody.code, "StreamExpired");
    assert.deepEqual(lateBody.details.output, { ok: true });

    const run = await fetch(`${server.url}/v1/runs/${finished.runId}`);
    const runBody = JSON.parse(await run.text()) as { state: string; output: unknown };
    assert.equal(run.status, 200);
    assert.equal(runBody.state, "completed");
    assert.deepEqual(runBody.output, { ok: true });

    const rejected = await post(server.url, "/worker/v1/stream", streamBody(finished, crypto.randomUUID(), "1", "later"));
    assert.equal(rejected.status, 410);
    assert.equal(rejected.body.code, "StreamExpired");
    const replay = await post(server.url, "/worker/v1/stream", streamBody(finished, operationId, "0", "hello"));
    assert.equal(replay.status, 200);
    assert.equal(replay.body.streamSeq, streamSeq);

    const stored = await admin.query<{
      rows: string;
      next: string;
      events: string;
      stepOutput: unknown;
      activeRows: string;
      activeExpired: Date | null;
      ok: boolean;
    }>(
      `SELECT
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $1) AS rows,
         (SELECT next_stream_seq::text FROM runs r WHERE r.id = $1) AS next,
         (SELECT count(*)::text FROM run_events e WHERE e.run_id = $1 AND e.type = 'StepCompleted') AS events,
         (SELECT output FROM steps s WHERE s.run_id = $1 AND s.step_key = 'only') AS "stepOutput",
         (SELECT count(*)::text FROM run_stream_events e WHERE e.run_id = $2) AS "activeRows",
         (SELECT stream_expired_at FROM runs r WHERE r.id = $2) AS "activeExpired",
         (relay_verify_projections($1)->>'ok')::boolean AS ok
       `,
      [finished.runId, active.runId],
    );
    assert.equal(stored.rows[0]?.rows, "0");
    assert.equal(stored.rows[0]?.next, nextBefore);
    assert.equal(stored.rows[0]?.events, "1");
    assert.deepEqual(stored.rows[0]?.stepOutput, { ok: true });
    assert.equal(stored.rows[0]?.activeRows, "1");
    assert.equal(stored.rows[0]?.activeExpired, null);
    assert.equal(stored.rows[0]?.ok, true);

    const continued = await post(server.url, "/worker/v1/stream", streamBody(active, crypto.randomUUID(), "1", "more"));
    assert.equal(continued.status, 200);
  } finally {
    abort.abort();
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

async function completeAndFinish(url: string, open: OpenAttempt): Promise<void> {
  const completed = await post(url, "/worker/v1/events", {
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
      finalBatchSeq: "0",
    },
  });
  assert.equal(completed.status, 200);
  const finished = await post(url, "/worker/v1/events", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: open.sessionId,
    runId: open.runId,
    fence: open.fence,
    intent: { kind: "finish_run", output: { ok: true } },
  });
  assert.equal(finished.status, 200);
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

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text.length === 0 ? {} : JSON.parse(text) as Record<string, unknown> };
}

async function waitFor(ready: () => boolean): Promise<void> {
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) {
    if (ready()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("timed out waiting for a stream event");
}
