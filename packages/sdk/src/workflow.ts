import type { Json } from "@relay/protocol";

export interface StepContext {
  readonly idempotencyKey: string;
  readonly attempt: number;
  readonly attemptToken: string;
  readonly signal: AbortSignal;
  emit(text: string): Promise<void>;
}

export interface StepOptions {
  input: Json;
}

export interface ParallelChild {
  name: string;
  input?: Json;
  run: (stepCtx: StepContext, input?: Json) => Promise<Json>;
}

export interface ParallelFailure {
  index: number;
  stepKey: string;
  error: { code: "StepFailed"; message: string };
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
    (name: string, options: StepOptions, fn: (stepCtx: StepContext, input: Json) => Promise<Json>): Promise<Json>;
    parallel(
      group: string,
      children: readonly ParallelChild[],
      options?: { concurrency?: number },
    ): Promise<Json[]>;
  };
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
