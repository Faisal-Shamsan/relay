import assert from "node:assert/strict";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

test("run list and step inspection keep explicit inputs and failed runs on the run row", async () => {
  const db = await createTestDatabase();
  const server = await startServer({ appDatabaseUrl: db.appUrl, leaseDurationMs: 30_000, reaperIntervalMs: 0 });
  try {
    const sessionId = randomUUID();
    const registered = await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    assert.equal(registered.status, 200);
    const failed = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 2 },
    });
    assert.equal(failed.status, 201);
    const runId = String(failed.body.runId);
    const leased = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
    });
    assert.equal(leased.status, 200);
    assert.equal(leased.body.runId, runId);
    const plain = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: leased.body.fence,
      intent: { kind: "declare_and_start", stepKey: "plain", expectedCommandPos: "0" },
    });
    assert.equal(plain.status, 200);
    const priced = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: leased.body.fence,
      intent: {
        kind: "declare_and_start",
        stepKey: "priced",
        expectedCommandPos: "1",
        explicitInput: true,
        input: { n: 1 },
      },
    });
    assert.equal(priced.status, 200);
    const stopped = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
      runId,
      fence: leased.body.fence,
      intent: { kind: "fail_run", error: { code: "StepExecutionError", message: "stopped" } },
    });
    assert.equal(stopped.status, 200);
    const pending = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    assert.equal(pending.status, 201);

    const unknown = await fetch(`${server.url}/v1/runs?state=nope`);
    assert.equal(unknown.status, 422);
    const badCursor = await fetch(`${server.url}/v1/runs?cursor=next`);
    assert.equal(badCursor.status, 422);
    const listed = await fetch(`${server.url}/v1/runs?state=failed&workflow=demo&version=demo-v1`);
    assert.equal(listed.status, 200);
    const page = await listed.json() as { runs: Array<{ runId: string; state: string }>; next: string | null };
    assert.deepEqual(page.runs.map((run) => run.runId), [runId]);
    assert.equal(page.runs[0]?.state, "failed");
    assert.equal(page.next, null);
    const first = await fetch(`${server.url}/v1/runs?limit=1`);
    const firstPage = await first.json() as { runs: Array<{ runId: string }>; next: string | null };
    assert.equal(firstPage.runs.length, 1);
    assert.equal(firstPage.next, "1");
    const second = await fetch(`${server.url}/v1/runs?limit=1&cursor=${firstPage.next}`);
    const secondPage = await second.json() as { runs: Array<{ runId: string }>; next: string | null };
    assert.equal(secondPage.runs.length, 1);
    assert.notEqual(secondPage.runs[0]?.runId, firstPage.runs[0]?.runId);
    const otherWorkflow = await fetch(`${server.url}/v1/runs?workflow=missing`);
    const empty = await otherWorkflow.json() as { runs: unknown[] };
    assert.deepEqual(empty.runs, []);

    const missing = await fetch(`${server.url}/v1/runs/${randomUUID()}/steps`);
    assert.equal(missing.status, 404);
    const stepsResponse = await fetch(`${server.url}/v1/runs/${runId}/steps`);
    assert.equal(stepsResponse.status, 200);
    const body = await stepsResponse.json() as {
      steps: Array<{ stepKey: string; explicitInput: boolean; input?: unknown; attempts: Array<{ attempt: number; state: string; fence: string }> }>;
    };
    const omitted = body.steps.find((step) => step.stepKey === "plain");
    const recorded = body.steps.find((step) => step.stepKey === "priced");
    assert.equal(omitted?.explicitInput, false);
    assert.equal(omitted !== undefined && "input" in omitted, false);
    assert.equal(recorded?.explicitInput, true);
    assert.deepEqual(recorded?.input, { n: 1 });
    assert.equal(recorded?.attempts[0]?.state, "running");
    assert.equal(recorded?.attempts[0]?.fence, "1");
  } finally {
    await server.close();
    await db.drop();
  }
});

test("metrics are current gauges and a failed exporter does not change the commit", async () => {
  const db = await createTestDatabase();
  const received: string[] = [];
  const collector = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      received.push(Buffer.concat(chunks).toString("utf8"));
      res.writeHead(204);
      res.end();
    });
  });
  await new Promise<void>((resolve) => collector.listen(0, "127.0.0.1", () => resolve()));
  const address = collector.address();
  if (address === null || typeof address === "string") throw new Error("collector did not bind");
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    traceEndpoint: `http://127.0.0.1:${address.port}/v1/traces`,
  });
  const dead = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    reaperIntervalMs: 0,
    traceEndpoint: "http://127.0.0.1:1/v1/traces",
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  try {
    const retired = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: retired,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    }, { "x-relay-activity": "callback" });
    assert.equal(created.status, 201);
    const runId = String(created.body.runId);
    const replay = await fetch(`${server.url}/v1/runs/${runId}`, { headers: { "x-relay-activity": "replay" } });
    assert.equal(replay.status, 200);
    await admin.query(
      `UPDATE worker_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [retired],
    );
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v2" }],
    });
    const metrics = await fetch(`${server.url}/v1/metrics`);
    assert.equal(metrics.status, 200);
    assert.match(metrics.headers.get("content-type") ?? "", /text\/plain/);
    const text = await metrics.text();
    assert.match(text, /relay_runs\{state="pending"} 1/);
    assert.match(text, /relay_pending_runs\{workflow="demo",version="demo-v1"} 1/);
    assert.match(text, /relay_pending_runs\{workflow="demo",version="demo-v2"} 0/);
    assert.match(text, /relay_compatible_workers\{workflow="demo",version="demo-v1"} 0/);
    assert.match(text, /relay_compatible_workers\{workflow="demo",version="demo-v2"} 1/);
    assert.equal(/cost/i.test(text), false);

    const owner = randomUUID();
    await post(server.url, "/worker/v1/register", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "demo", version: "demo-v1" }],
    });
    const leased = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
    });
    assert.equal(leased.status, 200);
    assert.equal(leased.body.runId, runId);
    const started = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      runId,
      fence: leased.body.fence,
      intent: { kind: "declare_and_start", stepKey: "only", expectedCommandPos: "0" },
    }, { "x-relay-activity": "callback" });
    assert.equal(started.status, 200);
    const completed = await post(server.url, "/worker/v1/events", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: owner,
      runId,
      fence: leased.body.fence,
      intent: {
        kind: "complete_step",
        stepKey: "only",
        attempt: 1,
        attemptToken: started.body.attemptToken,
        result: { ok: true },
      },
    }, { "x-relay-activity": "callback" });
    assert.equal(completed.status, 200);

    await waitFor(() => {
      const text = received.join("\n");
      return text.includes("replay") && text.includes("relay.attempt") && text.includes("\"only\"");
    });
    const spans = received.map((body) => JSON.parse(body) as {
      resourceSpans: Array<{ scopeSpans: Array<{ spans: Array<{ attributes: Array<{ key: string; value: { stringValue: string } }> }> }> }>;
    });
    const attributes = spans.flatMap((span) => span.resourceSpans[0]?.scopeSpans[0]?.spans[0]?.attributes ?? []);
    const activities = attributes.filter((item) => item.key === "relay.activity").map((item) => item.value.stringValue);
    assert.equal(activities.includes("callback"), true);
    assert.equal(activities.includes("replay"), true);
    const completion = spans
      .map((span) => span.resourceSpans[0]?.scopeSpans[0]?.spans[0]?.attributes ?? [])
      .find((items) => items.some((item) => item.key === "relay.step_key" && item.value.stringValue === "only"
        && items.some((attempt) => attempt.key === "relay.attempt" && attempt.value.stringValue === "1")));
    assert.equal(completion !== undefined, true);
    const values = new Map((completion ?? []).map((item) => [item.key, item.value.stringValue]));
    assert.equal(values.get("relay.run_id"), runId);
    assert.equal(values.get("relay.fence"), "1");
    assert.equal(values.get("relay.activity"), "callback");
    assert.match(values.get("relay.request_id") ?? "", /^[0-9a-f-]{36}$/);

    const stillCommitted = await post(dead.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 3 },
    });
    assert.equal(stillCommitted.status, 201);
    const stored = await fetch(`${dead.url}/v1/runs/${String(stillCommitted.body.runId)}`);
    assert.equal(stored.status, 200);
  } finally {
    await server.close();
    await dead.close();
    await admin.end();
    await new Promise<void>((resolve, reject) => collector.close((error) => (error ? reject(error) : resolve())));
    await db.drop();
  }
});

async function post(
  url: string,
  path: string,
  body: unknown,
  headers: { [key: string]: string } = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

async function waitFor(ready: () => boolean): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!ready()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for a trace");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
