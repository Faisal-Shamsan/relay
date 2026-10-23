import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { canonicalRelayJson, SEEDED_TENANT_ID, type Json } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

test("F18 same operation id conflicts on a different result and replays key order", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const sessionId = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
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
    const runId = String(created.body.runId);
    const lease = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
    });
    assert.equal(lease.status, 200);
    const fence = String(lease.body.fence);
    const started = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence,
      intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
    });
    assert.equal(started.status, 200);
    const attemptToken = String(started.body.attemptToken);
    const operationId = randomUUID();
    const original = rawCompleteBody({
      operationId,
      workerSessionId: sessionId,
      runId,
      fence,
      attemptToken,
      result: '{"ok":true}',
      order: "protocol-first",
    });
    const reordered = rawCompleteBody({
      operationId,
      workerSessionId: sessionId,
      runId,
      fence,
      attemptToken,
      result: '{"ok":true}',
      order: "intent-first",
    });
    assert.notEqual(original, reordered);
    assert.equal(original.startsWith('{"protocolVersion":'), true);
    assert.equal(reordered.startsWith('{"intent":'), true);
    assert.notEqual(reordered, JSON.stringify(JSON.parse(original)));
    assert.equal(
      canonicalRelayJson(JSON.parse(original) as Json),
      canonicalRelayJson(JSON.parse(reordered) as Json),
    );

    const completed = await postRaw(`${server.url}/worker/v1/events`, original);
    assert.equal(completed.status, 200);
    const before = await held(admin, runId, operationId);
    assert.deepEqual(before.output, { ok: true });

    const conflict = await postRaw(
      `${server.url}/worker/v1/events`,
      rawCompleteBody({
        operationId,
        workerSessionId: sessionId,
        runId,
        fence,
        attemptToken,
        result: '{"ok":false}',
        order: "protocol-first",
      }),
    );
    assert.equal(conflict.status, 409);
    assert.equal((conflict.body as { code?: string }).code, "IdempotencyConflict");
    assert.deepEqual(await held(admin, runId, operationId), before);

    const replay = await postRaw(`${server.url}/worker/v1/events`, original);
    assert.equal(replay.status, 200);
    assert.equal(canonicalRelayJson(replay.body as Json), canonicalRelayJson(completed.body as Json));
    assert.deepEqual(await held(admin, runId, operationId), before);

    const sameRequest = await postRaw(`${server.url}/worker/v1/events`, reordered);
    assert.equal(sameRequest.status, 200);
    assert.equal(canonicalRelayJson(sameRequest.body as Json), canonicalRelayJson(completed.body as Json));
    assert.deepEqual(await held(admin, runId, operationId), before);
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

function rawCompleteBody(input: {
  operationId: string;
  workerSessionId: string;
  runId: string;
  fence: string;
  attemptToken: string;
  result: string;
  order: "protocol-first" | "intent-first";
}): string {
  const intent = input.order === "protocol-first"
    ? `{"kind":"complete_step","stepKey":"only","attempt":1,"attemptToken":"${input.attemptToken}","result":${input.result}}`
    : `{"result":${input.result},"attemptToken":"${input.attemptToken}","attempt":1,"stepKey":"only","kind":"complete_step"}`;
  if (input.order === "protocol-first") {
    return `{"protocolVersion":1,"operationId":"${input.operationId}","workerSessionId":"${input.workerSessionId}","runId":"${input.runId}","fence":"${input.fence}","intent":${intent}}`;
  }
  return `{"intent":${intent},"fence":"${input.fence}","runId":"${input.runId}","workerSessionId":"${input.workerSessionId}","operationId":"${input.operationId}","protocolVersion":1}`;
}

interface HeldReceipt {
  events: string;
  next_event_seq: string;
  output: unknown;
  request_hash: string;
  response: unknown;
  stepCompleted: unknown;
}

async function held(admin: pg.Client, runId: string, operationId: string): Promise<HeldReceipt> {
  const result = await admin.query<HeldReceipt>(
    `SELECT (SELECT count(*)::text FROM run_events WHERE run_id = $1) AS events,
            (SELECT next_event_seq::text FROM runs WHERE id = $1) AS next_event_seq,
            (SELECT output FROM steps WHERE run_id = $1 AND step_key = 'only') AS output,
            r.request_hash,
            r.response,
            (SELECT payload FROM run_events WHERE run_id = $1 AND type = 'StepCompleted') AS "stepCompleted"
     FROM operation_receipts r
     WHERE r.run_id = $1 AND r.operation_id = $2`,
    [runId, operationId],
  );
  return result.rows[0]!;
}

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

async function postRaw(url: string, body: string): Promise<{ status: number; body: Json }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
  return { status: response.status, body: await response.json() as Json };
}
