import { writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import {
  logicalStepIdempotencyKey,
  PROTOCOL_VERSION,
  SEEDED_TENANT_ID,
  validateRelayJson,
  type Json,
} from "@relay/protocol";
import { hitFailpoint, loadFailpointPlan, type FailpointArm } from "./failpoints.js";
import type { StepContext, WorkflowDefinition } from "./workflow.js";

interface HistoryEvent {
  seq: string;
  type: string;
  stepKey: string | null;
  attempt: number | null;
  commandPos: string | null;
  payload: { descriptor?: Json; result?: Json; [key: string]: Json | undefined };
}

interface StepHistory {
  descriptor: { kind?: Json; key?: Json; explicitInput?: Json };
  completed: boolean;
  result?: Json;
}

export interface HeartbeatSchedule {
  cancel: () => void;
}

export async function runActivation(options: {
  serverUrl: string;
  workflow: WorkflowDefinition;
  finishRun?: boolean;
  failpoints?: { points: FailpointArm[] } | null;
  barrierDir?: string;
  completionRequestPath?: string;
  heartbeatRequestPath?: string;
  tenantId?: string;
  schedule?: (delayMs: number, run: () => void) => HeartbeatSchedule;
}): Promise<Json> {
  const tenantId = options.tenantId ?? SEEDED_TENANT_ID;
  const points = loadFailpointPlan(options.failpoints ?? null);
  const sessionId = randomUUID();
  await postJson(`${options.serverUrl}/worker/v1/register`, {
    protocolVersion: PROTOCOL_VERSION,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    tenantId,
    workflows: [{ name: options.workflow.name, version: options.workflow.version }],
  });
  const lease = await postJson(`${options.serverUrl}/worker/v1/lease`, {
    protocolVersion: PROTOCOL_VERSION,
    operationId: randomUUID(),
    workerSessionId: sessionId,
  });
  if (lease.workflowName !== options.workflow.name || lease.workflowVersion !== options.workflow.version) {
    throw new Error("leased workflow does not match the worker");
  }
  const runId = String(lease.runId);
  const fence = String(lease.fence);
  const watermark = String(lease.historyWatermark);
  const heartbeatIntervalMs = positiveInterval(lease.heartbeatIntervalMs);
  const history = await getHistory(options.serverUrl, runId, sessionId, watermark);
  const input = inputFromHistory(history);
  const commands = indexCommands(history);
  const seen = new Set<string>();
  let nextPos = 0;
  let inStep = false;
  const abort = new AbortController();
  const control = { lost: false };
  const beats = startHeartbeats({
    serverUrl: options.serverUrl,
    sessionId,
    runId,
    fence,
    intervalMs: heartbeatIntervalMs,
    schedule: options.schedule ?? defaultSchedule,
    points,
    barrierDir: options.barrierDir,
    heartbeatRequestPath: options.heartbeatRequestPath,
    abort,
    control,
  });

  const step = async (name: string, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json> => {
    if (inStep) {
      const error = new Error("nested step");
      error.name = "ConcurrentCommand";
      throw error;
    }
    if (seen.has(name)) {
      const error = new Error("duplicate step key");
      error.name = "DuplicateStepKey";
      throw error;
    }
    const position = nextPos;
    const expected = { kind: "step" as const, key: name, explicitInput: false };
    const historical = commands[position];
    if (historical) {
      if (
        historical.descriptor.kind !== expected.kind ||
        historical.descriptor.key !== expected.key ||
        historical.descriptor.explicitInput !== expected.explicitInput
      ) {
        throw nondeterminism(position, historical.descriptor, expected);
      }
      seen.add(name);
      nextPos += 1;
      if (historical.completed) {
        return validateRelayJson(historical.result ?? null);
      }
    } else {
      seen.add(name);
      nextPos += 1;
    }

    inStep = true;
    try {
      const started = historical
        ? await postIntent(options, sessionId, runId, fence, {
            kind: "restart_attempt",
            stepKey: name,
          }, options.completionRequestPath, control)
        : await postIntent(options, sessionId, runId, fence, {
            kind: "declare_and_start",
            stepKey: name,
            expectedCommandPos: String(position),
          }, options.completionRequestPath, control);
      const attempt = Number(started.attempt);
      const attemptToken = String(started.attemptToken);
      const idempotencyKey = logicalStepIdempotencyKey(tenantId, runId, name);
      await hitFailpoint(points, "step.after-start-commit-before-callback", { runId, stepKey: name, attempt }, options.barrierDir);
      const raw = await fn({
        idempotencyKey,
        attempt,
        attemptToken,
        signal: abort.signal,
      });
      const result = validateRelayJson(raw);
      await hitFailpoint(
        points,
        "step.after-user-function-before-completion",
        { runId, stepKey: name, attempt },
        options.barrierDir,
      );
      const completed = await postIntent(
        options,
        sessionId,
        runId,
        fence,
        { kind: "complete_step", stepKey: name, attempt, attemptToken, result },
        options.completionRequestPath,
        control,
      );
      await hitFailpoint(
        points,
        "step.after-completion-commit-before-response",
        { runId, stepKey: name, attempt },
        options.barrierDir,
      );
      return validateRelayJson(completed.result ?? result);
    } finally {
      inStep = false;
    }
  };

  try {
    const output = await options.workflow.fn({ step }, input);
    beats.stop();
    if (control.lost) throw new Error("lease lost");
    if (nextPos < commands.length) {
      const pending = commands[nextPos];
      throw nondeterminism(nextPos, pending?.descriptor ?? null, null, "unconsumed history");
    }
    const canonicalOutput = validateRelayJson(output);
    if (options.finishRun !== false) {
      await postIntent(
        options,
        sessionId,
        runId,
        fence,
        { kind: "finish_run", output: canonicalOutput },
        options.completionRequestPath,
        control,
      );
    }
    return canonicalOutput;
  } finally {
    beats.stop();
  }
}

function startHeartbeats(options: {
  serverUrl: string;
  sessionId: string;
  runId: string;
  fence: string;
  intervalMs: number;
  schedule: (delayMs: number, run: () => void) => HeartbeatSchedule;
  points: ReturnType<typeof loadFailpointPlan>;
  barrierDir: string | undefined;
  heartbeatRequestPath: string | undefined;
  abort: AbortController;
  control: { lost: boolean };
}): { stop: () => void } {
  let stopped = false;
  let current: HeartbeatSchedule | undefined;
  const tick = () => {
    if (stopped) return;
    void (async () => {
      try {
        const body = {
          protocolVersion: PROTOCOL_VERSION,
          operationId: randomUUID(),
          workerSessionId: options.sessionId,
          runId: options.runId,
          fence: options.fence,
        };
        if (options.heartbeatRequestPath) writeFileSync(options.heartbeatRequestPath, JSON.stringify(body));
        await postJson(`${options.serverUrl}/worker/v1/heartbeat`, body);
        if (stopped) return;
        await hitFailpoint(options.points, "heartbeat.after-commit-before-response", { runId: options.runId }, options.barrierDir);
        if (stopped) return;
        current = options.schedule(options.intervalMs, tick);
      } catch (error) {
        if (stopped) return;
        if (error instanceof WorkerHttpError && error.status === 409) {
          stopped = true;
          options.control.lost = true;
          options.abort.abort();
          return;
        }
        current = options.schedule(options.intervalMs, tick);
      }
    })();
  };
  current = options.schedule(options.intervalMs, tick);
  return {
    stop() {
      stopped = true;
      current?.cancel();
    },
  };
}

function defaultSchedule(delayMs: number, run: () => void): HeartbeatSchedule {
  const timer = setTimeout(run, delayMs);
  return { cancel: () => clearTimeout(timer) };
}

function positiveInterval(value: Json | undefined): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error("heartbeatIntervalMs must be a positive integer");
  }
  return value;
}

class WorkerHttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "WorkerHttpError";
  }
}

async function postIntent(
  options: { serverUrl: string },
  sessionId: string,
  runId: string,
  fence: string,
  intent: Json,
  completionRequestPath: string | undefined,
  control: { lost: boolean },
): Promise<{ [key: string]: Json }> {
  if (control.lost) throw new Error("lease lost");
  const body = {
    protocolVersion: PROTOCOL_VERSION,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    runId,
    fence,
    intent,
  };
  if (completionRequestPath && isComplete(intent)) {
    writeFileSync(completionRequestPath, JSON.stringify(body));
  }
  return postJson(`${options.serverUrl}/worker/v1/events`, body);
}

function isComplete(intent: Json): boolean {
  return typeof intent === "object" && intent !== null && !Array.isArray(intent) && intent.kind === "complete_step";
}

async function postJson(url: string, body: Json): Promise<{ [key: string]: Json }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  const parsed = text.length === 0 ? null : JSON.parse(text) as Json;
  if (!response.ok) {
    throw new WorkerHttpError(response.status, `POST ${url} failed ${response.status}: ${text}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("expected object response");
  }
  return parsed;
}

async function getHistory(serverUrl: string, runId: string, sessionId: string, watermark: string): Promise<HistoryEvent[]> {
  const url = new URL(`${serverUrl}/worker/v1/runs/${runId}/history`);
  url.searchParams.set("workerSessionId", sessionId);
  url.searchParams.set("after", "0");
  url.searchParams.set("through", watermark);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`history failed ${response.status}: ${await response.text()}`);
  const body = await response.json() as { events: HistoryEvent[] };
  return body.events;
}

function inputFromHistory(events: HistoryEvent[]): Json {
  const created = events.find((event) => event.type === "RunCreated");
  if (!created) throw new Error("RunCreated is missing from the lease snapshot");
  return validateRelayJson(created.payload.input ?? null);
}

function indexCommands(events: HistoryEvent[]): StepHistory[] {
  const commands: StepHistory[] = [];
  for (const event of events) {
    if (event.type === "CommandRecorded" && event.payload.descriptor && typeof event.payload.descriptor === "object") {
      const descriptor = event.payload.descriptor as StepHistory["descriptor"];
      const position = Number(event.commandPos ?? commands.length);
      commands[position] = { descriptor, completed: false };
    }
    if (event.type === "StepCompleted" && event.stepKey) {
      const command = commands.find((item) => item?.descriptor.key === event.stepKey);
      if (command) {
        command.completed = true;
        command.result = event.payload.result;
      }
    }
  }
  return commands;
}

function nondeterminism(position: number, expected: Json, actual: Json | null, reason = "command descriptor mismatch"): Error {
  const error = new Error(reason) as Error & { details: Json };
  error.name = "Nondeterminism";
  error.details = { position: String(position), expected, actual, reason };
  return error;
}
