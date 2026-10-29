import type { Json, RetryPolicy } from "@relay/protocol";

export type { RetryPolicy };

export interface StepContext {
  readonly idempotencyKey: string;
  readonly attempt: number;
  readonly attemptToken: string;
  readonly signal: AbortSignal;
  emit(text: string): Promise<void>;
}

export interface StepOptions {
  input?: Json;
  retry?: RetryPolicy;
}

export interface ParallelChild {
  name: string;
  input?: Json;
  retry?: RetryPolicy;
  run: (stepCtx: StepContext, input?: Json) => Promise<Json>;
}

export interface ParallelFailure {
  index: number;
  stepKey: string;
  error: { code: "StepFailed" | "StepExecutionError"; message: string };
}

export class StepExecutionError extends Error {
  readonly code = "StepExecutionError" as const;

  constructor(message: string) {
    super(message);
    this.name = "StepExecutionError";
  }
}

export class ParallelStepError extends Error {
  readonly failures: readonly ParallelFailure[];
  readonly results: readonly (Json | null)[];

  constructor(failures: readonly ParallelFailure[], results: readonly (Json | null)[]) {
    super("parallel step failed");
    this.name = "ParallelStepError";
    this.failures = failures;
    this.results = results;
  }
}

export interface WorkflowContext {
  step: {
    (name: string, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json>;
    (name: string, options: { retry: RetryPolicy }, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json>;
    (name: string, options: { input: Json; retry?: RetryPolicy }, fn: (stepCtx: StepContext, input: Json) => Promise<Json>): Promise<Json>;
    parallel(
      group: string,
      children: readonly ParallelChild[],
      options?: { concurrency?: number },
    ): Promise<Json[]>;
  };
  now(key: string): Promise<string>;
  random(key: string): Promise<number>;
  uuid(key: string): Promise<string>;
  sleep(key: string, durationMs: number): Promise<void>;
  waitForEvent(name: string, options: { timeoutMs: number }): Promise<Json>;
}

export interface WorkflowDefinition {
  name: string;
  version: string;
  fn: (ctx: WorkflowContext, input: Json) => Promise<Json>;
}

export function defineWorkflow(
  id: { name: string; version: string },
  fn: WorkflowDefinition["fn"],
): WorkflowDefinition {
  return { name: id.name, version: id.version, fn };
}
