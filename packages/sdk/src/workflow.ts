import type { Json } from "@relay/protocol";

export interface StepContext {
  readonly idempotencyKey: string;
  readonly attempt: number;
  readonly attemptToken: string;
  readonly signal: AbortSignal;
}

export interface WorkflowContext {
  step(name: string, fn: (stepCtx: StepContext) => Promise<Json>): Promise<Json>;
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
