import assert from "node:assert/strict";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import test from "node:test";
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
    }, { "x-relay-activity": "callback" });
    assert.equal(created.status, 201);
    const runId = String(created.body.runId);
    const replay = await fetch(`${server.url}/v1/runs/${runId}`, { headers: { "x-relay-activity": "replay" } });
    assert.equal(replay.status, 200);
    const metrics = await fetch(`${server.url}/v1/metrics`);
    assert.equal(metrics.status, 200);
    assert.match(metrics.headers.get("content-type") ?? "", /text\/plain/);
    const text = await metrics.text();
    assert.match(text, /relay_runs\{state="pending"} 1/);
    assert.match(text, /relay_pending_runs\{workflow="demo",version="demo-v1"} 1/);
    assert.match(text, /relay_compatible_workers\{workflow="demo",version="demo-v1"} 1/);
    assert.equal(/cost/i.test(text), false);
    await waitFor(() => received.join("\n").includes("replay") && received.join("\n").includes("callback"));
    const spans = received.map((body) => JSON.parse(body) as {
      resourceSpans: Array<{ scopeSpans: Array<{ spans: Array<{ attributes: Array<{ key: string; value: { stringValue: string } }> }> }> }>;
    });
    const activities = spans.flatMap((span) => span.resourceSpans[0]?.scopeSpans[0]?.spans[0]?.attributes ?? [])
      .filter((item) => item.key === "relay.activity")
      .map((item) => item.value.stringValue);
    assert.deepEqual(activities.sort(), ["callback", "replay"]);
    const runIds = spans.flatMap((span) => span.resourceSpans[0]?.scopeSpans[0]?.spans[0]?.attributes ?? [])
      .filter((item) => item.key === "relay.run_id")
      .map((item) => item.value.stringValue);
    assert.equal(runIds.includes(runId), true);

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
