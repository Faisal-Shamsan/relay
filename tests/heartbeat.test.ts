import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { createTestDatabase, startServer } from "relay-server";
import { defineWorkflow, runActivation, type HeartbeatSchedule } from "@relay/sdk";

test("the heartbeat loop sends server-scheduled requests without a worker clock", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    heartbeatIntervalMs: 10_000,
    reaperIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  const dir = mkdtempSync(join(tmpdir(), "relay-hb-"));
  const barrier = join(dir, "barrier");
  const bodyPath = join(dir, "heartbeat.json");
  const previous = process.env.RELAY_ALLOW_FAILPOINTS;
  process.env.RELAY_ALLOW_FAILPOINTS = "1";
  let entered = false;
  let releaseStep: () => void = () => {};
  const stepGate = new Promise<void>((resolve) => {
    releaseStep = resolve;
  });
  const queued: Array<{ delay: number; run: () => void }> = [];
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("only", async () => {
      entered = true;
      await stepGate;
      return { ok: true };
    });
  });
  try {
    await registerAndCreate(server.url);
    const done = runActivation({
      serverUrl: server.url,
      workflow,
      barrierDir: barrier,
      heartbeatRequestPath: bodyPath,
      failpoints: {
        points: [{ point: "heartbeat.after-commit-before-response", occurrence: 1, action: "pause-on-barrier" }],
      },
      schedule: (delay, run): HeartbeatSchedule => {
        queued.push({ delay, run });
        return { cancel() {} };
      },
    });
    await waitFor(() => entered);
    assert.equal(queued[0]?.delay, 10_000);
    const before = await admin.query<{ lease_expires_at: Date }>(`SELECT lease_expires_at FROM run_queue`);
    queued[0]!.run();
    await waitForFile(join(barrier, "hit.json"));
    const sent = JSON.parse(readFileSync(bodyPath, "utf8")) as Record<string, unknown>;
    assert.deepEqual(Object.keys(sent).sort(), ["fence", "operationId", "protocolVersion", "runId", "workerSessionId"]);
    const after = await admin.query<{ lease_expires_at: Date }>(`SELECT lease_expires_at FROM run_queue`);
    assert.ok(after.rows[0]!.lease_expires_at.getTime() > before.rows[0]!.lease_expires_at.getTime());
    const { writeFileSync } = await import("node:fs");
    writeFileSync(join(barrier, "release"), "1");
    releaseStep();
    await done;
  } finally {
    if (previous === undefined) delete process.env.RELAY_ALLOW_FAILPOINTS;
    else process.env.RELAY_ALLOW_FAILPOINTS = previous;
    await admin.end();
    await server.close();
    await db.drop();
  }
});

test("a rejected heartbeat aborts the callback and does not complete the step", async () => {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 30_000,
    heartbeatIntervalMs: 10_000,
    reaperIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  let entered = false;
  const queued: Array<{ run: () => void }> = [];
  const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
    return ctx.step("only", async (stepCtx) => {
      entered = true;
      await new Promise<void>((resolve) => {
        if (stepCtx.signal.aborted) resolve();
        else stepCtx.signal.addEventListener("abort", () => resolve());
      });
      return { ok: true };
    });
  });
  try {
    const runId = await registerAndCreate(server.url);
    const done = runActivation({
      serverUrl: server.url,
      workflow,
      schedule: (_delay, run): HeartbeatSchedule => {
        queued.push({ run });
        return { cancel() {} };
      },
    });
    await waitFor(() => entered);
    await admin.query(`UPDATE run_queue SET lease_expires_at = clock_timestamp() WHERE run_id = $1`, [runId]);
    queued[0]!.run();
    await assert.rejects(done, /lease lost/);
    const completed = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM run_events WHERE run_id = $1 AND type = 'StepCompleted'`,
      [runId],
    );
    assert.equal(completed.rows[0]!.count, "0");
  } finally {
    await admin.end();
    await server.close();
    await db.drop();
  }
});

async function registerAndCreate(url: string): Promise<string> {
  await fetch(`${url}/worker/v1/register`, {
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
  const created = await fetch(`${url}/v1/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: { n: 1 } }),
  });
  return String((await created.json() as { runId: string }).runId);
}

async function waitFor(ready: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (ready()) return;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  throw new Error("timed out waiting for the callback");
}

async function waitForFile(path: string): Promise<void> {
  const { existsSync } = await import("node:fs");
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (existsSync(path)) return;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  throw new Error(`timed out waiting for ${path}`);
}
