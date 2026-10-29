import { writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import {
  canonicalHash,
  canonicalRetryPolicy,
  logicalStepIdempotencyKey,
  PROTOCOL_VERSION,
  RelayJsonError,
  SEEDED_TENANT_ID,
  validateRelayJson,
  type Json,
  type RetryPolicy,
} from "@relay/protocol";
import { createAttemptEmit, type AttemptEmit } from "./emit.js";
import { hitFailpoint, loadFailpointPlan, type FailpointArm } from "./failpoints.js";
import {
  ParallelStepError,
  StepExecutionError,
  type ParallelChild,
  type ParallelFailure,
  type StepContext,
  type StepOptions,
  type WorkflowDefinition,
} from "./workflow.js";

interface HistoryEvent {
  seq: string;
  type: string;
  stepKey: string | null;
  attempt: number | null;
  commandPos: string | null;
  payload: { descriptor?: Json; result?: Json; error?: Json; [key: string]: Json | undefined };
}

interface CommandHistory {
  descriptor: { [key: string]: Json };
  completed: boolean;
  result?: Json;
}

interface ChildHistory {
  terminal: "open" | "completed" | "failed" | "waiting";
  result?: Json;
  error?: Json;
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
  apiKey?: string;
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
  }, credentialHeader(options.apiKey));
  const lease = await postJson(`${options.serverUrl}/worker/v1/lease`, {
    protocolVersion: PROTOCOL_VERSION,
    operationId: randomUUID(),
    workerSessionId: sessionId,
  }, credentialHeader(options.apiKey));
  if (lease.workflowName !== options.workflow.name || lease.workflowVersion !== options.workflow.version) {
    throw new Error("leased workflow does not match the worker");
  }
  const runId = String(lease.runId);
  await hitFailpoint(points, "lease.after-commit-before-response", { runId }, options.barrierDir);
  const fence = String(lease.fence);
  const watermark = String(lease.historyWatermark);
  const heartbeatIntervalMs = positiveInterval(lease.heartbeatIntervalMs);
  const history = await getHistory(options.serverUrl, runId, sessionId, watermark, options.apiKey);
  const input = inputFromHistory(history);
  const commands = indexCommands(history);
  const childStates = indexChildren(history);
  const seen = new Set<string>();
  const seenGroups = new Set<string>();
  const seenHelpers = new Set<string>();
  let nextPos = 0;
  let inStep = false;
  let inCallback = 0;
  const abort = new AbortController();
  const control = { lost: false, suspended: false };
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
    apiKey: options.apiKey,
    abort,
    control,
  });

  async function step(name: string, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json>;
  async function step(name: string, options: { retry: RetryPolicy }, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json>;
  async function step(name: string, options: { input: Json; retry?: RetryPolicy }, fn: (stepCtx: StepContext, input: Json) => Promise<Json>): Promise<Json>;
  async function step(
    name: string,
    fnOrOptions: ((stepCtx: StepContext) => Promise<Json>) | StepOptions,
    maybeFn?: (stepCtx: StepContext, input: Json) => Promise<Json>,
  ): Promise<Json> {
    const resolved = resolveStepCall(fnOrOptions, maybeFn);
    if (control.suspended) throw namedError("Suspended", "timer suspended the run");
    if (inStep || inCallback > 0) {
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
    const expected = {
      kind: "step" as const,
      key: name,
      explicitInput: resolved.explicit !== null,
      ...(resolved.explicit === null ? {} : { inputHash: resolved.explicit.hash }),
      ...(resolved.retry === null ? {} : { retry: resolved.retry }),
    };
    const historical = commands[position];
    const prior = childStates.get(name);
    if (historical) {
      if (!sameStep(historical.descriptor, expected)) {
        throw nondeterminism(position, historical.descriptor, expected);
      }
      seen.add(name);
      nextPos += 1;
      if (historical.completed) {
        return validateRelayJson(historical.result ?? null);
      }
      if (prior?.terminal === "failed") {
        throw executionError(prior.error);
      }
    } else {
      seen.add(name);
      nextPos += 1;
    }

    inStep = true;
    let stream: AttemptEmit | undefined;
    try {
      const declareIntent: Json = {
        kind: "declare_and_start",
        stepKey: name,
        expectedCommandPos: String(position),
      };
      if (resolved.explicit !== null) {
        declareIntent.explicitInput = true;
        declareIntent.input = resolved.explicit.input;
      }
      if (resolved.retry !== null) declareIntent.retry = resolved.retry;
      const started = !historical
        ? await postIntent(options, sessionId, runId, fence, declareIntent, options.completionRequestPath, control)
        : prior?.terminal === "waiting"
          ? await postIntent(options, sessionId, runId, fence, {
              kind: "start_attempt",
              stepKey: name,
            }, options.completionRequestPath, control, "replay")
          : await postIntent(options, sessionId, runId, fence, {
              kind: "restart_attempt",
              stepKey: name,
            }, options.completionRequestPath, control, "replay");
      const attempt = Number(started.attempt);
      const attemptToken = String(started.attemptToken);
      if (historical) {
        await hitFailpoint(points, "stream.after-supersession-commit-before-new-chunk", { runId, stepKey: name, attempt }, options.barrierDir);
      }
      const idempotencyKey = logicalStepIdempotencyKey(tenantId, runId, name);
      stream = createAttemptEmit((text, batchSeq) => sendStreamBatch(
        options, control, points, sessionId, runId, fence, name, attempt, attemptToken, text, batchSeq,
      ));
      await hitFailpoint(points, "step.after-start-commit-before-callback", { runId, stepKey: name, attempt }, options.barrierDir);
      inCallback += 1;
      let raw: Json | undefined;
      let failure: unknown;
      try {
        const stepCtx = {
          idempotencyKey,
          attempt,
          attemptToken,
          signal: abort.signal,
          emit: (text: string) => stream!.emit(text),
        };
        raw = resolved.explicit === null
          ? await resolved.fn(stepCtx)
          : await resolved.fn(stepCtx, resolved.explicit.frozen);
      } catch (error) {
        failure = error;
      } finally {
        inCallback -= 1;
      }
      await stream.flush();
      if (failure) {
        if (engineFailure(failure) || resolved.retry === null) throw failure;
        const reported = callbackFailure(failure);
        const failed = await postIntent(options, sessionId, runId, fence, {
          kind: "fail_step",
          stepKey: name,
          attempt,
          attemptToken,
          error: reported,
        }, options.completionRequestPath, control, "callback");
        if (failed.suspended === true) {
          control.suspended = true;
          throw namedError("Suspended", "retry suspended the run");
        }
        if (failed.exhausted === true) throw new StepExecutionError(reported.message);
        throw namedError("Suspended", "retry was not scheduled");
      }
      const result = validateRelayJson(raw);
      await hitFailpoint(
        points,
        "step.after-user-function-before-completion",
        { runId, stepKey: name, attempt },
        options.barrierDir,
      );
      await hitFailpoint(
        points,
        "stream.after-final-flush-before-completion",
        { runId, stepKey: name, attempt },
        options.barrierDir,
      );
      const completed = await postIntent(
        options,
        sessionId,
        runId,
        fence,
        { kind: "complete_step", stepKey: name, attempt, attemptToken, result, finalBatchSeq: stream.finalBatchSeq() },
        options.completionRequestPath,
        control,
        "callback",
      );
      if (completed.suspended === true) {
        control.suspended = true;
        throw namedError("Suspended", "retry suspended the run");
      }
      await hitFailpoint(
        points,
        "step.after-completion-commit-before-response",
        { runId, stepKey: name, attempt },
        options.barrierDir,
      );
      return validateRelayJson(completed.result ?? result);
    } finally {
      stream?.stop();
      inStep = false;
    }
  }

  const parallel = async (
    group: string,
    children: readonly ParallelChild[],
    optionsParallel?: { concurrency?: number },
  ): Promise<Json[]> => {
    if (control.suspended) throw namedError("Suspended", "timer suspended the run");
    if (inStep || inCallback > 0) {
      throw namedError("ConcurrentCommand", "nested command");
    }
    const concurrency = optionsParallel?.concurrency ?? 4;
    if (!validKey(group)) throw namedError("Serialization", "group key is invalid");
    if (seenGroups.has(group)) throw namedError("DuplicateStepKey", "duplicate parallel group");
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64) {
      throw namedError("Serialization", "concurrency must be from 1 to 64");
    }
    if (children.length > 1000) throw namedError("Serialization", "parallel group exceeds 1000 children");
    const names: string[] = [];
    const localNames = new Set<string>();
    const childInputs: ChildFingerprint[] = [];
    const childRetries: ChildRetry[] = [];
    const explicitChildren = new Map<string, Json>();
    const retryChildren = new Map<string, RetryPolicy>();
    for (const child of children) {
      if (!validKey(child.name)) throw namedError("Serialization", "child step key is invalid");
      if (localNames.has(child.name) || seen.has(child.name)) throw namedError("DuplicateStepKey", "duplicate step key");
      for (const key of Object.keys(child)) {
        if (key !== "name" && key !== "input" && key !== "run" && key !== "retry") {
          throw namedError("Serialization", "parallel child options are invalid");
        }
      }
      localNames.add(child.name);
      names.push(child.name);
      if ("input" in child) {
        const input = validateRelayJson(child.input);
        const hash = canonicalHash(input);
        childInputs.push({ explicitInput: true, inputHash: hash });
        explicitChildren.set(child.name, freezeJson(input));
      } else {
        childInputs.push({ explicitInput: false });
      }
      if (child.retry !== undefined) {
        const policy = readRetry(child.retry);
        childRetries.push({ explicitRetry: true, retry: policy });
        retryChildren.set(child.name, policy);
      } else {
        childRetries.push({ explicitRetry: false });
      }
    }
    const position = nextPos;
    const expected = {
      kind: "parallel" as const,
      key: group,
      children: names,
      concurrency,
      explicitInput: false as const,
      childInputs,
      childRetries,
    };
    const historical = commands[position];
    if (historical && !sameParallel(historical.descriptor, expected)) {
      throw nondeterminism(position, historical.descriptor, expected);
    }
    inStep = true;
    try {
      for (const name of names) seen.add(name);
      seenGroups.add(group);
      nextPos += 1;
      if (!historical) {
        const anyInput = childInputs.some((child) => child.explicitInput);
        const declared: Json = {
          kind: "declare_parallel",
          groupKey: group,
          expectedCommandPos: String(position),
          children: names,
          concurrency,
        };
        if (anyInput) {
          const specs: Json[] = [];
          for (const child of children) {
            if ("input" in child) specs.push({ explicitInput: true, input: explicitChildren.get(child.name) ?? null });
            else specs.push({ explicitInput: false });
          }
          declared.childInputs = specs;
        }
        if (childRetries.some((child) => child.explicitRetry)) {
          const specs: Json[] = [];
          for (const child of childRetries) {
            if (child.explicitRetry) specs.push({ explicitRetry: true, retry: child.retry ?? null });
            else specs.push({ explicitRetry: false });
          }
          declared.childRetries = specs;
        }
        await postIntent(options, sessionId, runId, fence, declared, options.completionRequestPath, control);
      }
      const outcomes = await settleParallel({
        children,
        concurrency,
        historical: Boolean(historical),
        childStates,
        onFatal: () => abort.abort(),
        runChild: (child, restart) => runParallelChild(child, restart),
      });
      const results: Json[] = [];
      const failures: ParallelFailure[] = [];
      if (outcomes.some((outcome) => outcome.status === "waiting")) {
        control.suspended = true;
        throw namedError("Suspended", "retry suspended the run");
      }
      outcomes.forEach((outcome, index) => {
        if (outcome.status === "ok") {
          results.push(outcome.result);
          return;
        }
        if (outcome.status !== "failed") return;
        results.push(null);
        failures.push({ index, stepKey: names[index] ?? "", error: outcome.error });
      });
      if (failures.length > 0) {
        throw new ParallelStepError(failures, results);
      }
      return results;
    } finally {
      inStep = false;
    }

    async function runParallelChild(child: ParallelChild, restart: boolean): Promise<ChildOutcome> {
      const started = restart
        ? await postIntent(options, sessionId, runId, fence, {
            kind: "restart_attempt",
            stepKey: child.name,
          }, options.completionRequestPath, control, "replay")
        : await postIntent(options, sessionId, runId, fence, {
            kind: "start_attempt",
            stepKey: child.name,
          }, options.completionRequestPath, control);
      const attempt = Number(started.attempt);
      const attemptToken = String(started.attemptToken);
      if (restart) {
        await hitFailpoint(points, "stream.after-supersession-commit-before-new-chunk", { runId, stepKey: child.name, attempt }, options.barrierDir);
      }
      const stream = createAttemptEmit((text, batchSeq) => sendStreamBatch(
        options, control, points, sessionId, runId, fence, child.name, attempt, attemptToken, text, batchSeq,
      ));
      await hitFailpoint(points, "step.after-start-commit-before-callback", { runId, stepKey: child.name, attempt }, options.barrierDir);
      inCallback += 1;
      try {
        let raw: Json;
        try {
          const stepCtx = {
            idempotencyKey: logicalStepIdempotencyKey(tenantId, runId, child.name),
            attempt,
            attemptToken,
            signal: abort.signal,
            emit: (text: string) => stream.emit(text),
          };
          raw = explicitChildren.has(child.name)
            ? await child.run(stepCtx, explicitChildren.get(child.name) as Json)
            : await child.run(stepCtx);
        } catch (error) {
          if (error instanceof WorkerHttpError || control.lost || engineFailure(error)) throw error;
          const reported = callbackFailure(error);
          const policy = retryChildren.get(child.name) ?? null;
          await stream.flush();
          const failed = await postIntent(options, sessionId, runId, fence, {
            kind: "fail_step",
            stepKey: child.name,
            attempt,
            attemptToken,
            error: policy === null ? { code: "StepFailed", message: reported.message } : reported,
          }, options.completionRequestPath, control, "callback");
          if (failed.suspended === true) {
            control.suspended = true;
            throw namedError("Suspended", "retry suspended the run");
          }
          if (policy !== null && failed.retrying === true) return { status: "waiting" };
          if (policy !== null) return { status: "failed", error: { code: "StepExecutionError", message: reported.message } };
          return { status: "failed", error: { code: "StepFailed", message: reported.message } };
        }
        const result = validateRelayJson(raw);
        await hitFailpoint(points, "step.after-user-function-before-completion", { runId, stepKey: child.name, attempt }, options.barrierDir);
        await stream.flush();
        await hitFailpoint(points, "stream.after-final-flush-before-completion", { runId, stepKey: child.name, attempt }, options.barrierDir);
        if (control.lost) throw new Error("lease lost");
        const completed = await postIntent(options, sessionId, runId, fence, {
          kind: "complete_step",
          stepKey: child.name,
          attempt,
          attemptToken,
          result,
          finalBatchSeq: stream.finalBatchSeq(),
        }, options.completionRequestPath, control, "callback");
        if (completed.suspended === true) {
          control.suspended = true;
          throw namedError("Suspended", "retry suspended the run");
        }
        await hitFailpoint(points, "step.after-completion-commit-before-response", { runId, stepKey: child.name, attempt }, options.barrierDir);
        return { status: "ok", result: validateRelayJson(completed.result ?? result) };
      } finally {
        stream.stop();
        inCallback -= 1;
      }
    }
  };

  async function recordHelper(kind: "now" | "uuid", key: string): Promise<string>;
  async function recordHelper(kind: "random", key: string): Promise<number>;
  async function recordHelper(kind: "now" | "random" | "uuid", key: string): Promise<string | number> {
    if (control.suspended) throw namedError("Suspended", "timer suspended the run");
    if (inStep || inCallback > 0) throw namedError("ConcurrentCommand", "nested command");
    if (!validKey(key)) throw namedError("Serialization", "helper key is invalid");
    const identity = `${kind}:${key}`;
    if (seenHelpers.has(identity)) throw namedError("DuplicateStepKey", "duplicate helper key");
    const position = nextPos;
    const expected = { kind, key };
    const historical = commands[position];
    if (historical && !sameHelper(historical.descriptor, expected)) {
      throw nondeterminism(position, historical.descriptor, expected);
    }
    seenHelpers.add(identity);
    nextPos += 1;
    inStep = true;
    try {
      if (historical?.result !== undefined) return publishHelper(kind, historical.result);
      const recorded = await postIntent(options, sessionId, runId, fence, {
        kind: "record_helper",
        helperKind: kind,
        key,
        expectedCommandPos: String(position),
      }, options.completionRequestPath, control);
      return publishHelper(kind, recorded.outcome);
    } finally {
      inStep = false;
    }
  }

  async function waitForEvent(name: string, wait: { timeoutMs: number }): Promise<Json> {
    if (control.suspended) throw namedError("Suspended", "wait suspended the run");
    if (inStep || inCallback > 0) throw namedError("ConcurrentCommand", "nested command");
    if (!validKey(name)) throw namedError("Serialization", "wait name is invalid");
    if (!Number.isSafeInteger(wait.timeoutMs) || wait.timeoutMs < 0) {
      throw namedError("Serialization", "timeoutMs must be a nonnegative safe integer");
    }
    const identity = `wait:${name}`;
    if (seenHelpers.has(identity)) throw namedError("DuplicateStepKey", "duplicate wait name");
    const position = nextPos;
    const expected = { kind: "wait" as const, key: name, timeoutMs: wait.timeoutMs };
    const historical = commands[position];
    if (historical && !sameWait(historical.descriptor, expected)) {
      throw nondeterminism(position, historical.descriptor, expected);
    }
    seenHelpers.add(identity);
    nextPos += 1;
    if (historical?.completed) return validateRelayJson(historical.result ?? null);
    inStep = true;
    try {
      const recorded = await postIntent(options, sessionId, runId, fence, {
        kind: "schedule_wait",
        key: name,
        timeoutMs: wait.timeoutMs,
        expectedCommandPos: String(position),
      }, options.completionRequestPath, control);
      if (recorded.suspended === true) {
        control.suspended = true;
        throw namedError("Suspended", "wait suspended the run");
      }
      return validateRelayJson(recorded.result ?? null);
    } finally {
      inStep = false;
    }
  }

  async function sleep(key: string, durationMs: number): Promise<void> {
    if (control.suspended) throw namedError("Suspended", "timer suspended the run");
    if (inStep || inCallback > 0) throw namedError("ConcurrentCommand", "nested command");
    if (!validKey(key)) throw namedError("Serialization", "timer key is invalid");
    if (!Number.isSafeInteger(durationMs) || durationMs < 0) {
      throw namedError("Serialization", "durationMs must be a nonnegative safe integer");
    }
    const identity = `sleep:${key}`;
    if (seenHelpers.has(identity)) throw namedError("DuplicateStepKey", "duplicate timer key");
    const position = nextPos;
    const expected = { kind: "sleep" as const, key, durationMs };
    const historical = commands[position];
    if (historical && !sameSleep(historical.descriptor, expected)) {
      throw nondeterminism(position, historical.descriptor, expected);
    }
    seenHelpers.add(identity);
    nextPos += 1;
    if (historical?.completed) return;
    inStep = true;
    try {
      const recorded = await postIntent(options, sessionId, runId, fence, {
        kind: "schedule_timer",
        key,
        durationMs,
        expectedCommandPos: String(position),
      }, options.completionRequestPath, control);
      if (recorded.suspended === true) {
        control.suspended = true;
        throw namedError("Suspended", "timer suspended the run");
      }
    } finally {
      inStep = false;
    }
  }

  try {
    let output: Json;
    try {
      output = await options.workflow.fn({
      step: Object.assign(step, { parallel }),
      now: (key) => recordHelper("now", key),
      random: (key) => recordHelper("random", key),
      uuid: (key) => recordHelper("uuid", key),
      sleep,
      waitForEvent,
    }, input);
    } catch (error) {
      if (shouldFailRun(error)) {
        beats.stop();
        const message = exhaustionMessage(error);
        await postIntent(options, sessionId, runId, fence, {
          kind: "fail_run",
          error: { code: "StepExecutionError", message },
        }, options.completionRequestPath, control);
      }
      throw error;
    }
    beats.stop();
    if (control.suspended) throw namedError("Suspended", "timer suspended the run");
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
  apiKey: string | undefined;
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
        await postJson(`${options.serverUrl}/worker/v1/heartbeat`, body, credentialHeader(options.apiKey));
        if (stopped) return;
        await hitFailpoint(options.points, "heartbeat.after-commit-before-response", { runId: options.runId }, options.barrierDir);
        if (stopped) return;
        current = options.schedule(options.intervalMs, tick);
      } catch (error) {
        if (stopped) return;
        if (error instanceof WorkerHttpError && error.message.includes("run is terminal")) {
          stopped = true;
          options.abort.abort();
          return;
        }
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
  options: { serverUrl: string; apiKey?: string },
  sessionId: string,
  runId: string,
  fence: string,
  intent: Json,
  completionRequestPath: string | undefined,
  control: { lost: boolean },
  activity?: "replay" | "callback",
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
  return postJson(
    `${options.serverUrl}/worker/v1/events`,
    body,
    { ...(activity ? { "x-relay-activity": activity } : {}), ...credentialHeader(options.apiKey) },
  );
}

function isComplete(intent: Json): boolean {
  return typeof intent === "object" && intent !== null && !Array.isArray(intent) && intent.kind === "complete_step";
}

async function sendStreamBatch(
  options: { serverUrl: string; apiKey?: string; barrierDir?: string },
  control: { lost: boolean },
  points: Parameters<typeof hitFailpoint>[0],
  sessionId: string,
  runId: string,
  fence: string,
  stepKey: string,
  attempt: number,
  attemptToken: string,
  text: string,
  batchSeq: number,
): Promise<void> {
  if (control.lost) throw new Error("lease lost");
  await postJson(`${options.serverUrl}/worker/v1/stream`, {
    protocolVersion: PROTOCOL_VERSION,
    operationId: randomUUID(),
    workerSessionId: sessionId,
    runId,
    fence,
    stepKey,
    attempt,
    attemptToken,
    batchSeq: String(batchSeq),
    text,
  }, credentialHeader(options.apiKey));
  await hitFailpoint(points, "stream.after-batch-commit-before-response", { runId, stepKey, attempt }, options.barrierDir);
}

async function postJson(
  url: string,
  body: Json,
  headers?: { [key: string]: string },
): Promise<{ [key: string]: Json }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
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

async function getHistory(
  serverUrl: string,
  runId: string,
  sessionId: string,
  watermark: string,
  apiKey: string | undefined,
): Promise<HistoryEvent[]> {
  const url = new URL(`${serverUrl}/worker/v1/runs/${runId}/history`);
  url.searchParams.set("workerSessionId", sessionId);
  url.searchParams.set("after", "0");
  url.searchParams.set("through", watermark);
  const response = await fetch(url, { headers: credentialHeader(apiKey) });
  if (!response.ok) throw new Error(`history failed ${response.status}: ${await response.text()}`);
  const body = await response.json() as { events: HistoryEvent[] };
  return body.events;
}

function credentialHeader(apiKey: string | undefined): { [key: string]: string } {
  if (!apiKey) return {};
  return { authorization: `Bearer ${apiKey}` };
}

function inputFromHistory(events: HistoryEvent[]): Json {
  const created = events.find((event) => event.type === "RunCreated");
  if (!created) throw new Error("RunCreated is missing from the lease snapshot");
  return validateRelayJson(created.payload.input ?? null);
}

function indexCommands(events: HistoryEvent[]): CommandHistory[] {
  const commands: CommandHistory[] = [];
  for (const event of events) {
    if (event.type === "CommandRecorded" && event.payload.descriptor && typeof event.payload.descriptor === "object" && !Array.isArray(event.payload.descriptor)) {
      const descriptor = event.payload.descriptor;
      const position = Number(event.commandPos ?? commands.length);
      const helper = descriptor.kind === "now" || descriptor.kind === "random" || descriptor.kind === "uuid";
      const recorded = Object.prototype.hasOwnProperty.call(event.payload, "outcome");
      commands[position] = {
        descriptor,
        completed: helper && recorded,
        result: helper && recorded ? event.payload.outcome : undefined,
      };
    }
    if (event.type === "WaitResolved" && event.commandPos !== null) {
      const command = commands[Number(event.commandPos)];
      if (command) {
        command.completed = true;
        command.result = event.payload.result;
      }
    }
    if (event.type === "TimerFired" && event.commandPos !== null) {
      const command = commands[Number(event.commandPos)];
      if (command) {
        command.completed = true;
        command.result = event.payload.wakeAt;
      }
    }
    if (event.type === "StepCompleted" && event.stepKey) {
      const command = commands.find((item) => item?.descriptor.kind === "step" && item.descriptor.key === event.stepKey);
      if (command) {
        command.completed = true;
        command.result = event.payload.result;
      }
    }
  }
  return commands;
}

function indexChildren(events: HistoryEvent[]): Map<string, ChildHistory> {
  const children = new Map<string, ChildHistory>();
  for (const event of events) {
    if (!event.stepKey) continue;
    const current = children.get(event.stepKey);
    if ((event.type === "StepStarted" || event.type === "StepAttemptAbandoned") && current?.terminal !== "completed" && current?.terminal !== "failed") {
      children.set(event.stepKey, { terminal: "open" });
    }
    if (event.type === "StepCompleted") {
      children.set(event.stepKey, { terminal: "completed", result: event.payload.result });
    }
    if (event.type === "StepFailed") {
      if (event.payload.exhausted === false) {
        children.set(event.stepKey, { terminal: "waiting" });
      } else if (event.payload.explicitRetry === true) {
        const message = failureMessage(event.payload.error);
        children.set(event.stepKey, {
          terminal: "failed",
          error: { code: "StepExecutionError", message },
        });
      } else {
        children.set(event.stepKey, { terminal: "failed", error: event.payload.error });
      }
    }
  }
  return children;
}

type ChildOutcome = {
  status: "ok";
  result: Json;
} | {
  status: "failed";
  error: { code: "StepFailed" | "StepExecutionError"; message: string };
} | {
  status: "waiting";
};

async function settleParallel(input: {
  children: readonly ParallelChild[];
  concurrency: number;
  historical: boolean;
  childStates: Map<string, ChildHistory>;
  onFatal: () => void;
  runChild: (child: ParallelChild, restart: boolean) => Promise<ChildOutcome>;
}): Promise<ChildOutcome[]> {
  const outcomes: Array<ChildOutcome | undefined> = new Array(input.children.length);
  let cursor = 0;
  let running = 0;
  let fatal: unknown;
  let settled = false;
  await new Promise<void>((resolve, reject) => {
    const finish = () => {
      if (settled) return;
      if (running === 0 && (cursor >= input.children.length || fatal)) {
        settled = true;
        if (fatal) reject(fatal);
        else resolve();
      }
    };
    const pump = () => {
      if (settled) return;
      while (!fatal && running < input.concurrency && cursor < input.children.length) {
        const index = cursor;
        cursor += 1;
        const child = input.children[index];
        if (!child) continue;
        const record = input.historical ? input.childStates.get(child.name) : undefined;
        if (record?.terminal === "completed") {
          outcomes[index] = { status: "ok", result: validateRelayJson(record.result ?? null) };
          continue;
        }
        if (record?.terminal === "failed") {
          outcomes[index] = { status: "failed", error: storedFailure(record.error) };
          continue;
        }
        running += 1;
        void input.runChild(child, record?.terminal === "open")
          .then((outcome) => {
            outcomes[index] = outcome;
          })
          .catch((error: unknown) => {
            fatal = fatal ?? error;
            input.onFatal();
          })
          .finally(() => {
            running -= 1;
            pump();
            finish();
          });
      }
      finish();
    };
    pump();
  });
  return outcomes.map((outcome) => {
    if (!outcome) throw new Error("missing parallel outcome");
    return outcome;
  });
}

function storedFailure(error: Json | undefined): { code: "StepFailed" | "StepExecutionError"; message: string } {
  if (typeof error === "object" && error !== null && !Array.isArray(error) && typeof error.message === "string" && error.message.length > 0) {
    if (error.code === "StepExecutionError") return { code: "StepExecutionError", message: error.message };
    if (error.code === "StepFailed") return { code: "StepFailed", message: error.message };
  }
  return { code: "StepFailed", message: "step failed" };
}

interface ChildFingerprint {
  explicitInput: boolean;
  inputHash?: string;
}

interface ChildRetry {
  explicitRetry: boolean;
  retry?: RetryPolicy;
}

function sameRetry(recorded: Json | undefined, expected: RetryPolicy | undefined): boolean {
  if (expected === undefined) return recorded === undefined;
  if (typeof recorded !== "object" || recorded === null || Array.isArray(recorded)) return false;
  const codes = recorded.retryableCodes;
  return recorded.maxAttempts === expected.maxAttempts
    && recorded.initialDelayMs === expected.initialDelayMs
    && recorded.maxDelayMs === expected.maxDelayMs
    && recorded.multiplier === expected.multiplier
    && recorded.jitter === expected.jitter
    && Array.isArray(codes)
    && codes.length === expected.retryableCodes.length
    && expected.retryableCodes.every((code, index) => codes[index] === code);
}

function sameChildRetries(recorded: Json | undefined, expected: ChildRetry[]): boolean {
  const any = expected.some((child) => child.explicitRetry);
  if (!any) return recorded === undefined;
  if (!Array.isArray(recorded) || recorded.length !== expected.length) return false;
  return expected.every((child, index) => {
    const row = recorded[index];
    if (typeof row !== "object" || row === null || Array.isArray(row)) return false;
    if (row.explicitRetry !== child.explicitRetry) return false;
    if (!child.explicitRetry) return row.retry === undefined;
    return sameRetry(row.retry, child.retry);
  });
}

function sameWait(
  descriptor: { [key: string]: Json },
  expected: { kind: "wait"; key: string; timeoutMs: number },
): boolean {
  return descriptor.kind === expected.kind
    && descriptor.key === expected.key
    && descriptor.timeoutMs === expected.timeoutMs;
}

function sameSleep(
  descriptor: { [key: string]: Json },
  expected: { kind: "sleep"; key: string; durationMs: number },
): boolean {
  return descriptor.kind === expected.kind
    && descriptor.key === expected.key
    && descriptor.durationMs === expected.durationMs;
}

function sameHelper(
  descriptor: { [key: string]: Json },
  expected: { kind: "now" | "random" | "uuid"; key: string },
): boolean {
  return descriptor.kind === expected.kind && descriptor.key === expected.key;
}

function publishHelper(kind: "now" | "random" | "uuid", value: Json | undefined): string | number {
  const parsed = validateRelayJson(value ?? null);
  if (kind === "random") {
    if (typeof parsed !== "number" || !Number.isFinite(parsed) || parsed < 0 || parsed >= 1) {
      throw namedError("Serialization", "random helper value is invalid");
    }
    return parsed;
  }
  if (typeof parsed !== "string") throw namedError("Serialization", "helper value is invalid");
  if (kind === "now" && !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(parsed)) {
    throw namedError("Serialization", "clock helper value is invalid");
  }
  if (kind === "uuid" && !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(parsed)) {
    throw namedError("Serialization", "uuid helper value is invalid");
  }
  return parsed;
}

function sameStep(
  descriptor: { [key: string]: Json },
  expected: { kind: "step"; key: string; explicitInput: boolean; inputHash?: string; retry?: RetryPolicy },
): boolean {
  if (descriptor.kind !== expected.kind || descriptor.key !== expected.key) return false;
  if (!sameRetry(descriptor.retry, expected.retry)) return false;
  const recorded = descriptor.explicitInput === true;
  if (recorded !== expected.explicitInput) return false;
  if (!expected.explicitInput) return true;
  return descriptor.inputHash === expected.inputHash;
}

function sameParallel(
  descriptor: { [key: string]: Json },
  expected: { kind: "parallel"; key: string; children: string[]; concurrency: number; explicitInput: false; childInputs: ChildFingerprint[]; childRetries: ChildRetry[] },
): boolean {
  const shape = descriptor.kind === expected.kind
    && descriptor.key === expected.key
    && descriptor.explicitInput === expected.explicitInput
    && descriptor.concurrency === expected.concurrency
    && Array.isArray(descriptor.children)
    && descriptor.children.length === expected.children.length
    && descriptor.children.every((child, index) => child === expected.children[index]);
  if (!shape || !sameChildRetries(descriptor.childRetries, expected.childRetries)) return false;
  const any = expected.childInputs.some((child) => child.explicitInput);
  const recorded = descriptor.childInputs;
  if (!any) return recorded === undefined;
  if (!Array.isArray(recorded) || recorded.length !== expected.childInputs.length) return false;
  return expected.childInputs.every((child, index) => {
    const row = recorded[index];
    if (typeof row !== "object" || row === null || Array.isArray(row)) return false;
    if (row.explicitInput !== child.explicitInput) return false;
    if (!child.explicitInput) return row.inputHash === undefined;
    return row.inputHash === child.inputHash;
  });
}

function resolveStepCall(
  fnOrOptions: ((stepCtx: StepContext) => Promise<Json>) | StepOptions,
  maybeFn: ((stepCtx: StepContext, input: Json) => Promise<Json>) | undefined,
): {
  fn: (stepCtx: StepContext, input?: Json) => Promise<Json>;
  explicit: { input: Json; hash: string; frozen: Json } | null;
  retry: RetryPolicy | null;
} {
  if (typeof fnOrOptions === "function") {
    if (maybeFn !== undefined) throw namedError("Serialization", "step options are invalid");
    return { fn: (stepCtx) => fnOrOptions(stepCtx), explicit: null, retry: null };
  }
  if (typeof maybeFn !== "function" || !isPlainObject(fnOrOptions)) {
    throw namedError("Serialization", "step options are invalid");
  }
  const keys = Object.keys(fnOrOptions);
  if (keys.length === 0 || keys.some((key) => key !== "input" && key !== "retry")) {
    throw namedError("Serialization", "step options are invalid");
  }
  const retry = "retry" in fnOrOptions && fnOrOptions.retry !== undefined ? readRetry(fnOrOptions.retry) : null;
  if (!("input" in fnOrOptions)) {
    const callback = maybeFn as (stepCtx: StepContext) => Promise<Json>;
    return { fn: (stepCtx) => callback(stepCtx), explicit: null, retry };
  }
  const input = validateRelayJson(fnOrOptions.input ?? null);
  return {
    fn: (stepCtx, value) => maybeFn(stepCtx, value === undefined ? input : value),
    explicit: { input, hash: canonicalHash(input), frozen: freezeJson(input) },
    retry,
  };
}

function freezeJson(value: Json): Json {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) {
    for (const item of value) freezeJson(item);
    return Object.freeze(value) as Json;
  }
  for (const key of Object.keys(value)) freezeJson(value[key] as Json);
  return Object.freeze(value) as Json;
}

function isPlainObject(value: object): value is { [key: string]: Json | undefined } {
  return !Array.isArray(value);
}

function validKey(value: string): boolean {
  return typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= 256;
}

function readRetry(value: Json): RetryPolicy {
  try {
    return canonicalRetryPolicy(value);
  } catch (error) {
    if (error instanceof RelayJsonError) throw namedError("Serialization", error.message);
    throw error;
  }
}

function engineFailure(error: unknown): boolean {
  return error instanceof Error && (
    error.name === "ConcurrentCommand"
    || error.name === "Nondeterminism"
    || error.name === "DuplicateStepKey"
    || error.name === "Serialization"
    || error.name === "Suspended"
  );
}

function callbackFailure(error: unknown): { code: string; message: string } {
  const message = error instanceof Error && error.message.length > 0 ? error.message : "step failed";
  const code = error instanceof Error ? (error as { code?: unknown }).code : undefined;
  if (typeof code === "string" && code.length > 0 && code !== "StepExecutionError" && Buffer.byteLength(code) <= 64) {
    return { code, message };
  }
  return { code: "StepFailed", message };
}

function failureMessage(error: Json | undefined): string {
  if (typeof error === "object" && error !== null && !Array.isArray(error) && typeof error.message === "string" && error.message.length > 0) {
    return error.message;
  }
  return "step failed";
}

function executionError(error: Json | undefined): StepExecutionError {
  return new StepExecutionError(failureMessage(error));
}

function shouldFailRun(error: unknown): boolean {
  if (error instanceof StepExecutionError) return true;
  return error instanceof ParallelStepError
    && error.failures.some((failure) => failure.error.code === "StepExecutionError");
}

function exhaustionMessage(error: unknown): string {
  if (error instanceof StepExecutionError && error.message.length > 0) return error.message;
  if (error instanceof ParallelStepError) {
    const found = error.failures.find((failure) => failure.error.code === "StepExecutionError");
    if (found && found.error.message.length > 0) return found.error.message;
  }
  return "step failed";
}

function namedError(name: string, message: string): Error {
  const error = new Error(message);
  error.name = name;
  return error;
}

function nondeterminism(position: number, expected: unknown, actual: unknown, reason = "command descriptor mismatch"): Error {
  const error = new Error(reason) as Error & { details: Json };
  error.name = "Nondeterminism";
  error.details = {
    position: String(position),
    expected: expected as Json,
    actual: actual as Json,
    reason,
  };
  return error;
}
