import { appendFileSync, existsSync, readFileSync } from "node:fs";

export interface FakeProviderCall {
  attempt: number;
  attemptToken: string;
  idempotencyKey: string;
}

interface ProviderRow {
  kind?: string;
  idempotencyKey?: string;
}

export function recordFakeProvider(logPath: string, call: FakeProviderCall, dedupe: boolean): void {
  const alreadyApplied = dedupe && effectRecorded(logPath, call.idempotencyKey);
  appendFileSync(logPath, `${JSON.stringify({ kind: "invocation", ...call })}\n`);
  if (alreadyApplied) return;
  appendFileSync(logPath, `${JSON.stringify({ kind: "effect", ...call })}\n`);
}

function effectRecorded(logPath: string, idempotencyKey: string): boolean {
  if (!existsSync(logPath)) return false;
  for (const line of readFileSync(logPath, "utf8").split("\n")) {
    if (line.length === 0) continue;
    const row = JSON.parse(line) as ProviderRow;
    if (row.kind === "effect" && row.idempotencyKey === idempotencyKey) return true;
  }
  return false;
}
