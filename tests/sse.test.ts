import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { reduceStream, subscribe, type StreamEvent } from "@relay/sdk";
import { createTestDatabase, DEV_CLIENT_KEY, startServer, type StartedServer } from "relay-server";

test("F29 a reconnect across supersession matches the client that stayed connected", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const continuousAbort = new AbortController();
  const partialAbort = new AbortController();
  const resumeAbort = new AbortController();
  try {
    const open = await openAttempt(server);
    const first = await post(server.url, "/worker/v1/stream", streamBody(open, "0", "old"));
    assert.equal(first.status, 200);

    const continuous: StreamEvent[] = [];
    const partial: StreamEvent[] = [];
    const resumed: StreamEvent[] = [];
    const continuousRead = read(server.url, open.runId, continuous, continuousAbort.signal);
    const partialRead = read(server.url, open.runId, partial, partialAbort.signal);
    await waitFor(() => continuous.some((event) => event.kind === "chunk") && partial.some((event) => event.kind === "chunk"));
    partialAbort.abort();
    await partialRead;

    await expire(admin, open.runId);
    const next = await leaseFresh(server.url, open.runId);
    const restarted = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: next.sessionId,
      runId: open.runId,
      fence: next.fence,
      intent: { kind: "restart_attempt", stepKey: "only" },
    });
    assert.equal(restarted.status, 200);
    const appended = await post(server.url, "/worker/v1/stream", {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: next.sessionId,
      runId: open.runId,
      fence: next.fence,
      stepKey: "only",
      attempt: restarted.body.attempt,
      attemptToken: restarted.body.attemptToken,
      batchSeq: "0",
      text: "new",
    });
    assert.equal(appended.status, 200);
    const completed = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: next.sessionId,
      runId: open.runId,
      fence: next.fence,
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: restarted.body.attempt,
        attemptToken: restarted.body.attemptToken,
        result: { ok: true },
        finalBatchSeq: "0",
      },
    });
    assert.equal(completed.status, 200);
    const finished = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: crypto.randomUUID(),
      workerSessionId: next.sessionId,
      runId: open.runId,
      fence: next.fence,
      intent: { kind: "finish_run", output: { ok: true } },
    });
    assert.equal(finished.status, 200);
    await waitFor(() => continuous.some((event) => event.kind === "run_terminal"));

    const cursor = partial[partial.length - 1]?.seq;
    assert.equal(typeof cursor, "string");
    const resumeRead = read(server.url, open.runId, resumed, resumeAbort.signal, cursor);
    await waitFor(() => resumed.some((event) => event.kind === "run_terminal"));
    resumeAbort.abort();
    await resumeRead;
    const stayed = reduceStream(continuous);
    const rejoined = reduceStream([...partial, ...resumed]);
    assert.deepEqual(rejoined, stayed);
    assert.equal(stayed.attempts.find((attempt) => attempt.attempt === 1)?.superseded, true);
    assert.equal(stayed.attempts.find((attempt) => attempt.attempt === 1)?.text, "old");
    assert.equal(stayed.attempts.find((attempt) => attempt.attempt === 2)?.text, "new");
    assert.equal(stayed.attempts.find((attempt) => attempt.attempt === 2)?.superseded, false);
    assert.equal(stayed.runState, "completed");
    assert.equal(resumed.some((event) => event.kind === "superseded"), true);
  } finally {
    continuousAbort.abort();
    partialAbort.abort();
    resumeAbort.abort();
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("F30 catch-up and polling deliver a row committed before the read and a missed notification", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  let releaseCatchup = (): void => undefined;
  const catchup = new Promise<void>((resolve) => {
    releaseCatchup = resolve;
  });
  let entered = false;
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    streamPollMs: 40,
    beforeStreamCatchup: async () => {
      if (entered) return;
      entered = true;
      await catchup;
    },
  });
  const abort = new AbortController();
  try {
    const open = await openAttempt(server);
    const ahead = await fetch(`${server.url}/v1/runs/${open.runId}/stream?after=1`);
    assert.equal(ahead.status, 409);
    const conflict = await fetch(`${server.url}/v1/runs/${open.runId}/stream?after=1`, {
      headers: { "last-event-id": "0" },
    });
    assert.equal(conflict.status, 422);

    const events: StreamEvent[] = [];
    const reading = read(server.url, open.runId, events, abort.signal);
    await waitFor(() => entered);
    const hidden = await post(server.url, "/worker/v1/stream", streamBody(open, "0", "hidden"));
    assert.equal(hidden.status, 200);
    server.suppressStreamNotifications();
    releaseCatchup();
    await waitFor(() => events.some((event) => event.data.text === "hidden"));
    const missed = await post(server.url, "/worker/v1/stream", streamBody(open, "1", "polled"));
    assert.equal(missed.status, 200);
    await waitFor(() => events.some((event) => event.data.text === "polled"));
    assert.deepEqual(events.map((event) => event.seq), ["1", "2"]);
    abort.abort();
    await reading;
  } finally {
    abort.abort();
    releaseCatchup();
    await server.close();
    await db.drop();
  }
});

test("F31 a slow client is disconnected without blocking a later batch", { timeout: 20_000 }, async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    streamQueueLimit: 1,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const open = await openAttempt(server);
    const first = await post(server.url, "/worker/v1/stream", streamBody(open, "0", "held"));
    assert.equal(first.status, 200);
    const second = await post(server.url, "/worker/v1/stream", streamBody(open, "1", "overflow"));
    assert.equal(second.status, 200);
    const closed = new Promise<void>((resolve, reject) => {
      const request = http.get(
        `${server.url}/v1/runs/${open.runId}/stream`,
        { headers: { authorization: `Bearer ${DEV_CLIENT_KEY}` } },
        (response) => {
        assert.equal(response.statusCode, 200);
        response.pause();
        response.on("close", () => resolve());
      });
      request.on("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "ECONNRESET") resolve();
        else reject(error);
      });
    });
    await Promise.race([
      closed,
      new Promise((_resolve, reject) => setTimeout(() => reject(new Error("slow client stayed connected")), 5_000)),
    ]);
    const third = await post(server.url, "/worker/v1/stream", streamBody(open, "2", "after"));
    assert.equal(third.status, 200);
    const stored = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_stream_events WHERE run_id = $1 AND kind = 'chunk'`,
      [open.runId],
    );
    assert.equal(stored.rows[0]?.count, "3");
  } finally {
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

async function leaseFresh(url: string, runId: string): Promise<{ sessionId: string; fence: string }> {
  const sessionId = crypto.randomUUID();
  await post(url, "/worker/v1/register", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: sessionId,
    tenantId: SEEDED_TENANT_ID,
    workflows: [{ name: "demo", version: "demo-v1" }],
  });
  const lease = await post(url, "/worker/v1/lease", {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
    workerSessionId: sessionId,
  });
  assert.equal(lease.status, 200);
  assert.equal(lease.body.runId, runId);
  return { sessionId, fence: String(lease.body.fence) };
}

function streamBody(open: OpenAttempt, batchSeq: string, text: string): unknown {
  return {
    protocolVersion: 1,
    operationId: crypto.randomUUID(),
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

async function read(url: string, runId: string, into: StreamEvent[], signal: AbortSignal, after?: string): Promise<void> {
  try {
    for await (const event of subscribe({ serverUrl: url, runId, after, signal })) into.push(event);
  } catch (error) {
    if (!signal.aborted) throw error;
  }
}

async function waitFor(ready: () => boolean): Promise<void> {
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) {
    if (ready()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("timed out waiting for a stream event");
}

async function expire(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
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
