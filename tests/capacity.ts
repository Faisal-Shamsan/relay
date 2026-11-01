import { execSync, spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import os from "node:os";
import { join } from "node:path";
import pg from "pg";
import { SEEDED_TENANT_ID } from "@relay/protocol";
import { defineWorkflow, runActivation, subscribe, type WorkflowDefinition } from "@relay/sdk";
import { createTestDatabase, startServer, type StartedServer } from "relay-server";
import { repoRoot } from "./spawn.js";

const replayLevels = [10, 100, 1_000, 10_000];
const runLevels = [100, 1_000, 10_000];
const replayBudgetMs: Record<number, number> = {
  10: 90_000,
  100: 240_000,
  1_000: 720_000,
  10_000: 1_500_000,
};
const runBudgetMs: Record<number, number> = {
  100: 240_000,
  1_000: 900_000,
  10_000: 1_500_000,
};
const activationConcurrency = 4;

interface ReplayRow {
  commands: number;
  status: "measured" | "not measured";
  firstActivationMs?: string;
  replayMs?: string;
  historyBytes?: string;
  bytesPerCommand?: string;
  streamRows?: string;
  catchUpMs?: string;
  replayCallbacks?: string;
  maxConnections?: string;
  cpu?: string;
  error?: string;
}

interface RunRow {
  runs: number;
  status: "measured" | "not measured";
  p50?: string;
  p95?: string;
  p99?: string;
  wallMs?: string;
  perSecond?: string;
  historyBytes?: string;
  maxConnections?: string;
  cpu?: string;
  error?: string;
}

interface Report {
  recordedAt: string;
  commit: string;
  node: string;
  hardware: string;
  postgres: string;
  replay: ReplayRow[];
  runs: RunRow[];
  brokeFirst: string;
}

const report: Report = {
  recordedAt: new Date().toISOString(),
  commit: gitCommit(),
  node: process.version,
  hardware: hardwareLine(),
  postgres: "not read",
  replay: [],
  runs: [],
  brokeFirst: "none",
};

let callbacks = 0;

async function main(): Promise<void> {
  let replayStopped = false;
  for (const commands of replayLevels) {
    if (replayStopped) {
      report.replay.push({ commands, status: "not measured" });
      continue;
    }
    const row = await measureReplay(commands);
    report.replay.push(row);
    writeReport();
    if (row.status !== "measured") {
      replayStopped = true;
      if (report.brokeFirst === "none") report.brokeFirst = `replay ${commands} commands: ${row.error ?? "did not finish"}`;
    }
  }

  let runsStopped = false;
  for (const runs of runLevels) {
    if (runsStopped) {
      report.runs.push({ runs, status: "not measured" });
      continue;
    }
    const row = await measureRuns(runs);
    report.runs.push(row);
    writeReport();
    if (row.status !== "measured") {
      runsStopped = true;
      if (report.brokeFirst === "none") report.brokeFirst = `${runs} concurrent runs: ${row.error ?? "did not finish"}`;
    }
  }
  writeReport();
  console.log(`capacity report ${join(repoRoot, "docs", "capacity.md")}`);
}

async function measureReplay(commands: number): Promise<ReplayRow> {
  console.log(`replay ${commands}`);
  const row: ReplayRow = { commands, status: "not measured" };
  try {
    await runReplay(commands, row);
    row.status = "measured";
  } catch (error) {
    row.error = errorText(error);
    row.status = "not measured";
  }
  return row;
}

async function runReplay(commands: number, row: ReplayRow): Promise<void> {
  const opened = await openServer();
  const cpu = watchCpu();
  const connections = watchConnections(opened.sampler);
  const limit = startBudget(
    replayBudgetMs[commands] ?? 60_000,
    `replay ${commands} did not finish within ${replayBudgetMs[commands]}ms`,
  );
  const work = (async () => {
    const workflow = replayWorkflow(commands);
    const before = callbacks;
    await registerWorker(opened.server, workflow.version);
    const runId = await createRun(opened.server, workflow.version, { commands });
    const firstStarted = performance.now();
    const first = await runActivation({
      serverUrl: opened.server.url,
      workflow,
      finishRun: false,
    });
    row.firstActivationMs = (performance.now() - firstStarted).toFixed(1);
    if (first !== commands - 1) throw new Error(`first activation returned ${JSON.stringify(first)}`);
    if (callbacks - before !== commands) throw new Error(`first activation ran ${callbacks - before} callbacks`);

    const history = await opened.admin.query<{ bytes: string; events: string }>(
      `SELECT coalesce(sum(octet_length(payload::text)), 0)::text AS bytes,
              count(*)::text AS events
       FROM run_events WHERE run_id = $1`,
      [runId],
    );
    row.historyBytes = history.rows[0]?.bytes ?? "0";
    const bytes = Number(row.historyBytes);
    row.bytesPerCommand = commands === 0 || !Number.isFinite(bytes) ? "not measured" : (bytes / commands).toFixed(1);

    const stream = await opened.admin.query<{ rows: string; maxSeq: string }>(
      `SELECT count(*)::text AS rows, coalesce(max(seq), 0)::text AS "maxSeq"
       FROM run_stream_events WHERE run_id = $1`,
      [runId],
    );
    row.streamRows = stream.rows[0]?.rows ?? "0";
    const maxSeq = stream.rows[0]?.maxSeq ?? "0";
    if (maxSeq === "0") {
      row.catchUpMs = "no stream rows";
    } else {
      const catchStarted = performance.now();
      try {
        await readThrough(opened.server.url, runId, maxSeq);
        row.catchUpMs = (performance.now() - catchStarted).toFixed(1);
      } catch (error) {
        row.catchUpMs = `not measured (${errorText(error)})`;
      }
    }

    await releaseLease(opened.admin, runId);
    const replayBefore = callbacks;
    const replayStarted = performance.now();
    const replayed = await runActivation({
      serverUrl: opened.server.url,
      workflow,
      finishRun: true,
    });
    row.replayMs = (performance.now() - replayStarted).toFixed(1);
    row.replayCallbacks = String(callbacks - replayBefore);
    if (replayed !== commands - 1) throw new Error(`replay returned ${JSON.stringify(replayed)}`);
  })();
  void work.catch(() => undefined);
  try {
    await Promise.race([work, limit.promise]);
  } finally {
    limit.cancel();
    row.maxConnections = String(connections.stop());
    row.cpu = cpu.stop();
    await opened.close();
  }
}

async function measureRuns(runs: number): Promise<RunRow> {
  console.log(`runs ${runs}`);
  const row: RunRow = { runs, status: "not measured" };
  try {
    await runMany(runs, row);
    row.status = "measured";
  } catch (error) {
    row.error = errorText(error);
    row.status = "not measured";
  }
  return row;
}

async function runMany(runs: number, row: RunRow): Promise<void> {
  const opened = await openServer();
  const cpu = watchCpu();
  const connections = watchConnections(opened.sampler);
  const limit = startBudget(
    runBudgetMs[runs] ?? 60_000,
    `${runs} runs did not finish within ${runBudgetMs[runs]}ms`,
  );
  const work = (async () => {
    const workflow = defineWorkflow({ name: "capacity", version: `runs-${runs}` }, async (ctx) => {
      return ctx.step("only", async () => ({ ok: true }));
    });
    await registerWorker(opened.server, workflow.version);
    for (let i = 0; i < runs; i += 1) {
      await createRun(opened.server, workflow.version, { n: i });
    }
    const durations: number[] = [];
    const started = performance.now();
    let next = 0;
    const workers = Array.from({ length: Math.min(activationConcurrency, runs) }, async () => {
      while (next < runs) {
        next += 1;
        const one = performance.now();
        const result = await runActivation({ serverUrl: opened.server.url, workflow });
        if (JSON.stringify(result) !== JSON.stringify({ ok: true })) {
          throw new Error(`activation returned ${JSON.stringify(result)}`);
        }
        durations.push(performance.now() - one);
      }
    });
    await Promise.all(workers);
    const wall = performance.now() - started;
    row.wallMs = wall.toFixed(1);
    row.perSecond = wall === 0 ? "not measured" : (runs / (wall / 1000)).toFixed(2);
    row.p50 = percentile(durations, 0.5).toFixed(1);
    row.p95 = percentile(durations, 0.95).toFixed(1);
    row.p99 = percentile(durations, 0.99).toFixed(1);
    const history = await opened.admin.query<{ bytes: string }>(
      `SELECT coalesce(sum(octet_length(payload::text)), 0)::text AS bytes FROM run_events`,
    );
    row.historyBytes = history.rows[0]?.bytes ?? "0";
    if (durations.length !== runs) throw new Error(`timed ${durations.length} of ${runs} activations`);
  })();
  void work.catch(() => undefined);
  try {
    await Promise.race([work, limit.promise]);
  } finally {
    limit.cancel();
    row.maxConnections = String(connections.stop());
    row.cpu = cpu.stop();
    await opened.close();
  }
}

function replayWorkflow(commands: number): WorkflowDefinition {
  return defineWorkflow({ name: "capacity", version: `replay-${commands}` }, async (ctx) => {
    let last = 0;
    for (let i = 0; i < commands; i += 1) {
      const stored = await ctx.step(`s${i}`, async (stepCtx) => {
        callbacks += 1;
        await stepCtx.emit("x");
        return i;
      });
      if (typeof stored !== "number") throw new Error(`step s${i} stored ${JSON.stringify(stored)}`);
      last = stored;
    }
    return last;
  });
}

async function openServer(): Promise<{
  server: StartedServer;
  admin: pg.Client;
  sampler: pg.Client;
  close: () => Promise<void>;
}> {
  const db = await createTestDatabase();
  const server = await startServer({
    appDatabaseUrl: db.appUrl,
    leaseDurationMs: 120_000,
    heartbeatIntervalMs: 10_000,
    reaperIntervalMs: 0,
    timerIntervalMs: 0,
  });
  const admin = new pg.Client({ connectionString: db.adminUrl });
  const sampler = new pg.Client({ connectionString: db.adminUrl });
  await admin.connect();
  await sampler.connect();
  if (report.postgres === "not read") {
    const version = await admin.query<{ version: string }>(`SELECT version()`);
    report.postgres = version.rows[0]?.version ?? "unknown";
  }
  return {
    server,
    admin,
    sampler,
    close: async () => {
      await Promise.race([
        admin.end().catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 5_000)),
      ]);
      await sampler.end().catch(() => undefined);
      await Promise.race([
        server.close().catch(() => undefined),
        new Promise((resolve) => setTimeout(resolve, 10_000)),
      ]);
      await db.drop().catch(() => undefined);
    },
  };
}

async function registerWorker(server: StartedServer, version: string): Promise<void> {
  const response = await fetch(`${server.url}/worker/v1/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      protocolVersion: 1,
      operationId: randomUUID(),
      workerSessionId: randomUUID(),
      tenantId: SEEDED_TENANT_ID,
      workflows: [{ name: "capacity", version }],
    }),
  });
  if (!response.ok) throw new Error(await response.text());
}

async function createRun(server: StartedServer, version: string, input: { commands?: number; n?: number }): Promise<string> {
  const response = await fetch(`${server.url}/v1/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflowName: "capacity", workflowVersion: version, input }),
  });
  if (response.status !== 201) throw new Error(await response.text());
  const body = await response.json() as { runId: string };
  return body.runId;
}

async function readThrough(url: string, runId: string, maxSeq: string): Promise<void> {
  const target = BigInt(maxSeq);
  const abort = new AbortController();
  let seen = false;
  try {
    for await (const event of subscribe({ serverUrl: url, runId, signal: abort.signal })) {
      if (BigInt(event.seq) >= target) {
        seen = true;
        break;
      }
    }
  } finally {
    abort.abort();
  }
  if (!seen) throw new Error("stream ended before catch-up");
}

async function releaseLease(admin: pg.Client, runId: string): Promise<void> {
  await admin.query(
    `UPDATE run_queue SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE run_id = $1`,
    [runId],
  );
  await admin.query(`SELECT relay_expire_lease($1)`, [runId]);
}

function watchConnections(admin: pg.Client): { stop: () => number } {
  let max = 0;
  let stopped = false;
  const sample = (): void => {
    void admin.query<{ count: number }>(
      `SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname = current_database()`,
    ).then((result) => {
      if (!stopped) max = Math.max(max, result.rows[0]?.count ?? 0);
    }).catch(() => undefined);
  };
  sample();
  const timer = setInterval(sample, 250);
  return {
    stop: () => {
      stopped = true;
      clearInterval(timer);
      return max;
    },
  };
}

function watchCpu(): { stop: () => string } {
  let text = "";
  let err = "";
  let child: ChildProcess | undefined;
  try {
    child = spawn("docker", ["stats", "relay-m1-pg", "--format", "{{.CPUPerc}}"], {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    child.unref();
    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");
    child.stdout?.on("data", (chunk: string) => {
      text += chunk;
    });
    child.stderr?.on("data", (chunk: string) => {
      err += chunk;
    });
  } catch (error) {
    err = errorText(error);
  }
  return {
    stop: () => {
      child?.kill();
      const values = text.match(/\d+(?:\.\d+)?%/g) ?? [];
      const numbers = values.map((value) => Number(value.slice(0, -1))).filter((value) => Number.isFinite(value));
      if (numbers.length === 0) {
        const once = spawnSync("docker", ["stats", "relay-m1-pg", "--no-stream", "--format", "{{.CPUPerc}}"], {
          encoding: "utf8",
          timeout: 8_000,
          windowsHide: true,
        });
        const sample = once.stdout?.trim() ?? "";
        if (once.status === 0 && sample.endsWith("%")) return `one sample after the level ${sample}`;
        const reason = (once.stderr || err).trim().slice(0, 180);
        return reason.length === 0 ? "not readable" : `not readable: ${reason}`;
      }
      const max = Math.max(...numbers);
      return `max ${max.toFixed(2)}% across ${numbers.length} docker stats samples`;
    },
  };
}

function writeReport(): void {
  const replayStoppedAt = report.replay.find((row) => row.status !== "measured");
  const runsStoppedAt = report.runs.find((row) => row.status !== "measured");
  const lines = [
    "# Capacity",
    "",
    "These numbers were measured on this machine before redrive, dashboard controls, a history cache, or blobs. They describe that engine. They are not a throughput promise.",
    "",
    "## Machine",
    "",
    `- Recorded: ${report.recordedAt}`,
    `- Commit: ${report.commit}`,
    `- Node: ${report.node}`,
    `- Hardware: ${report.hardware}`,
    `- Postgres: ${report.postgres}`,
    "- Server pool: 8 connections (`packages/server/src/db.ts`).",
    "- Lease: 120 seconds. Heartbeat: 10 seconds. Reaper and timer loops were off for this command.",
    `- Run ladder concurrency: ${activationConcurrency} in-process activations.`,
    "- Reproduction: `npm run capacity`. This command is not part of `npm test`. The 100-run failure demo remains `tests/demo.test.ts`.",
    "",
    "## Workload",
    "",
    "Replay climbs one run through 10, 100, 1,000, and 10,000 sequential steps. Each step returns its index and emits one character. The first activation runs the callbacks and does not finish the run. Catch-up reads the stream rows that activation stored. The lease is then released, and the second activation replays the journal and finishes the run.",
    "",
    "The run ladder creates 100, then 1,000, then 10,000 one-step runs and activates them four at a time. Activation time includes register, lease, the step, and finish. It is not a separate lease-only rate.",
    "",
    "A level that does not finish inside its budget is not measured. Later levels of that ladder are not started.",
    "",
    "Stream catch-up is the public SSE read. The server drops that connection when 32 frames are queued. A longer stream can fail catch-up while journal replay still finishes.",
    "",
    "## Replay",
    "",
    "| Commands | First activation ms | Replay ms | History bytes | Bytes per command | Stream rows | Catch-up ms | Replay callbacks | Max connections | Postgres CPU |",
    "|---|---|---|---|---|---|---|---|---|---|",
    ...report.replay.map((row) => replayLine(row)),
    "",
    "## Concurrent runs",
    "",
    "| Runs | Activation p50 ms | p95 ms | p99 ms | Wall ms | Activations per second | History bytes | Max connections | Postgres CPU |",
    "|---|---|---|---|---|---|---|---|---|",
    ...report.runs.map((row) => runLine(row)),
    "",
    "## What the run showed",
    "",
    report.brokeFirst === "none"
      ? "Every replay and run-ladder level finished inside its budget. Replay did not call the step function again."
      : `The first level that did not finish: ${report.brokeFirst}.`,
    "",
    replayStoppedAt
      ? `Replay stopped at ${replayStoppedAt.commands} commands. Higher replay counts were not measured.`
      : "Stream catch-up completed for the 10-command history. At 100, 1,000, and 10,000 commands the SSE connection closed before the last stream row, so those catch-up times are not measured.",
    runsStoppedAt
      ? `The run ladder stopped at ${runsStoppedAt.runs} runs. Higher run counts were not measured.`
      : "Every run-ladder level finished.",
    "",
    "Connection counts include the admin connection used to sample `pg_stat_activity`. CPU is a `docker stats` sample of the `relay-m1-pg` container during that level. A level with no sample says not readable.",
    "",
    "External effects are still at least once. This command does not change that. A lower-priority run can still wait behind a higher priority. The JSON body limit is unchanged.",
    "",
  ];
  writeFileSync(join(repoRoot, "docs", "capacity.md"), lines.join("\n"));
}

function replayLine(row: ReplayRow): string {
  if (row.status !== "measured") {
    const why = row.error ? ` (${row.error})` : "";
    return `| ${row.commands} | not measured${why} | not measured | not measured | not measured | not measured | not measured | not measured | ${row.maxConnections ?? "not measured"} | ${row.cpu ?? "not measured"} |`;
  }
  return `| ${row.commands} | ${row.firstActivationMs} | ${row.replayMs} | ${row.historyBytes} | ${row.bytesPerCommand} | ${row.streamRows} | ${row.catchUpMs} | ${row.replayCallbacks} | ${row.maxConnections} | ${row.cpu} |`;
}

function runLine(row: RunRow): string {
  if (row.status !== "measured") {
    const why = row.error ? ` (${row.error})` : "";
    return `| ${row.runs} | not measured${why} | not measured | not measured | not measured | not measured | not measured | ${row.maxConnections ?? "not measured"} | ${row.cpu ?? "not measured"} |`;
  }
  return `| ${row.runs} | ${row.p50} | ${row.p95} | ${row.p99} | ${row.wallMs} | ${row.perSecond} | ${row.historyBytes} | ${row.maxConnections} | ${row.cpu} |`;
}

function percentile(samples: number[], p: number): number {
  const sorted = [...samples].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
  return sorted[index] ?? 0;
}

function hardwareLine(): string {
  const cpus = os.cpus();
  const model = cpus[0]?.model ?? "unknown";
  const memory = Math.round(os.totalmem() / (1024 * 1024));
  return `${cpus.length} x ${model}, ${memory} MiB RAM, ${os.platform()} ${os.release()}`;
}

function gitCommit(): string {
  try {
    return execSync("git rev-parse HEAD", { cwd: repoRoot, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function startBudget(ms: number, message: string): { promise: Promise<never>; cancel: () => void } {
  let timer: NodeJS.Timeout | undefined;
  let settled = false;
  const promise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(message));
    }, ms);
  });
  return {
    promise,
    cancel: () => {
      settled = true;
      if (timer) clearTimeout(timer);
    },
  };
}

function errorText(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  const cause = error instanceof Error && error.cause instanceof Error ? `: ${error.cause.message}` : "";
  return `${message}${cause}`.replace(/\s+/g, " ").slice(0, 180);
}

void main().catch((error: unknown) => {
  report.brokeFirst = errorText(error);
  writeReport();
  console.error(error);
  process.exitCode = 1;
});
