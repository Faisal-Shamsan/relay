import { canonicalHash } from "./hash.js";

/** Logical step key. Attempt and fence are intentionally absent (spec §2.3). */
export function logicalStepIdempotencyKey(tenantId: string, runId: string, stepKey: string): string {
  return `relay-step-v1:${canonicalHash([tenantId, runId, stepKey])}`;
}
