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
  payload: { descriptor?: Json; result?: Json; [key: string]: Json | undefined };
}

interface StepHistory {
  descriptor: { kind?: Json; key?: Json; explicitInput?: Json };
  completed: boolean;
  result?: Json;
}

export async function runActivation(options: {
  serverUrl: string;
  workflow: WorkflowDefinition;
  finishRun?: boolean;
  failpoints?: { points: FailpointArm[] } | null;
  barrierDir?: string;
  completionRequestPath?: string;
  tenantId?: string;
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
  const history = await getHistory(options.serverUrl, runId, sessionId, watermark);
  const input = inputFromHistory(history);
  const steps = indexSteps(history);
  const seen = new Set<string>();
  let inStep = false;

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
    seen.add(name);
    const expected = { kind: "step", key: name, explicitInput: false };
    const existing = steps.get(name);
    if (existing) {
      if (
        existing.descriptor.kind !== expected.kind ||
        existing.descriptor.key !== expected.key ||
        existing.descriptor.explicitInput !== expected.explicitInput
      ) {
        const error = new Error("command descriptor mismatch");
        error.name = "Nondeterminism";
        throw error;
      }
      if (existing.completed) {
        return validateRelayJson(existing.result ?? null);
      }
    }

    inStep = true;
    try {
      const started = existing
        ? await postIntent(options, sessionId, runId, fence, {
            kind: "restart_attempt",
            stepKey: name,
          }, options.completionRequestPath)
        : await postIntent(options, sessionId, runId, fence, {
            kind: "declare_and_start",
            stepKey: name,
            expectedCommandPos: "0",
          }, options.completionRequestPath);
      const attempt = Number(started.attempt);
      const attemptToken = String(started.attemptToken);
      const idempotencyKey = logicalStepIdempotencyKey(tenantId, runId, name);
      await hitFailpoint(points, "step.after-start-commit-before-callback", { runId, stepKey: name, attempt }, options.barrierDir);
      const raw = await fn({
        idempotencyKey,
        attempt,
        attemptToken,
        signal: new AbortController().signal,
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

  const output = await options.workflow.fn({ step }, input);
  const canonicalOutput = validateRelayJson(output);
  if (options.finishRun !== false) {
    await postIntent(
      options,
      sessionId,
      runId,
      fence,
      { kind: "finish_run", output: canonicalOutput },
      options.completionRequestPath,
    );
  }
  return canonicalOutput;
}

async function postIntent(
  options: { serverUrl: string },
  sessionId: string,
  runId: string,
  fence: string,
  intent: Json,
  completionRequestPath: string | undefined,
): Promise<{ [key: string]: Json }> {
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
    throw new Error(`POST ${url} failed ${response.status}: ${text}`);
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

function indexSteps(events: HistoryEvent[]): Map<string, StepHistory> {
  const steps = new Map<string, StepHistory>();
  for (const event of events) {
    if (event.type === "CommandRecorded" && event.payload.descriptor && typeof event.payload.descriptor === "object") {
      const descriptor = event.payload.descriptor as StepHistory["descriptor"];
      if (typeof descriptor.key === "string") {
        steps.set(descriptor.key, { descriptor, completed: false });
      }
    }
    if (event.type === "StepCompleted" && event.stepKey) {
      const current = steps.get(event.stepKey) ?? { descriptor: {}, completed: false };
      current.completed = true;
      current.result = event.payload.result;
      steps.set(event.stepKey, current);
    }
  }
  return steps;
}
