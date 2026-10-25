import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "../src/index.js";

test("phase 1 read APIs return the pinned run and a history page", async () => {
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
    const created = await post(server.url, "/v1/runs", {
      workflowName: "demo",
      workflowVersion: "demo-v1",
      input: { n: 1 },
    });
    assert.equal(created.status, 201);
    const runId = String(created.body.runId);
    const pending = await fetch(`${server.url}/v1/runs/${runId}`);
    assert.equal(pending.status, 200);
    const described = await pending.json() as {
      workflowVersion: string;
      state: string;
      ownership: { fence: string; queueState: string | null; leaseOwner: string | null };
    };
    assert.equal(described.workflowVersion, "demo-v1");
    assert.equal(described.state, "pending");
    assert.equal(described.ownership.fence, "0");
    assert.equal(described.ownership.queueState, "ready");
    assert.equal(described.ownership.leaseOwner, null);

    const leased = await post(server.url, "/worker/v1/lease", {
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: sessionId,
    });
    assert.equal(leased.status, 200);

    const running = await fetch(`${server.url}/v1/runs/${runId}`);
    const owned = await running.json() as { state: string; ownership: { fence: string; leaseOwner: string | null } };
    assert.equal(owned.state, "running");
    assert.equal(owned.ownership.fence, "1");
    assert.equal(owned.ownership.leaseOwner, sessionId);

    const first = await fetch(`${server.url}/v1/runs/${runId}/events?limit=1`);
    assert.equal(first.status, 200);
    const firstPage = await first.json() as { events: Array<{ seq: string; type: string }>; next: string | null };
    assert.equal(firstPage.events.length, 1);
    assert.equal(firstPage.events[0]?.type, "RunCreated");
    assert.equal(typeof firstPage.next, "string");
    const second = await fetch(`${server.url}/v1/runs/${runId}/events?after=${firstPage.next}&limit=1`);
    const secondPage = await second.json() as { events: Array<{ seq: string; type: string }>; next: string | null };
    assert.equal(secondPage.events[0]?.type, "LeaseGranted");
    assert.equal(BigInt(secondPage.events[0]!.seq) > BigInt(firstPage.events[0]!.seq), true);

    const missing = await fetch(`${server.url}/v1/runs/${randomUUID()}`);
    assert.equal(missing.status, 404);
    const badLimit = await fetch(`${server.url}/v1/runs/${runId}/events?limit=0`);
    assert.equal(badLimit.status, 422);
    const badCursor = await fetch(`${server.url}/v1/runs/${runId}/events?after=nope`);
    assert.equal(badCursor.status, 422);
  } finally {
    await server.close();
    await db.drop();
  }
});

async function post(url: string, path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}
