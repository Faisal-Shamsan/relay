import type { Json } from "./json.js";

export const PROTOCOL_VERSION = 1;
export const SEEDED_TENANT_ID = "11111111-1111-4111-8111-111111111111";

export type WorkerIntent =
  | {
      kind: "declare_and_start";
      stepKey: string;
      expectedCommandPos: string;
    }
  | {
      kind: "restart_attempt";
      stepKey: string;
    }
  | {
      kind: "complete_step";
      stepKey: string;
      attempt: number;
      attemptToken: string;
      result: Json;
    }
  | {
      kind: "finish_run";
      output: Json;
    };

export interface WorkerEventRequest {
  protocolVersion: number;
  operationId: string;
  workerSessionId: string;
  runId: string;
  fence: string;
  intent: WorkerIntent;
}

export interface ErrorBody {
  code: string;
  message: string;
  details: Json;
  requestId: string;
}
