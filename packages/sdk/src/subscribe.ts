import type { Json } from "@relay/protocol";

export interface StreamEvent {
  seq: string;
  kind: string;
  data: { [key: string]: Json | undefined };
}

export interface AttemptView {
  stepKey: string;
  attempt: number;
  text: string;
  superseded: boolean;
}

export interface StreamView {
  attempts: AttemptView[];
  runState: string | null;
}

export async function* subscribe(options: {
  serverUrl: string;
  runId: string;
  after?: string;
  signal?: AbortSignal;
}): AsyncGenerator<StreamEvent> {
  const url = new URL(`${options.serverUrl}/v1/runs/${options.runId}/stream`);
  if (options.after !== undefined) url.searchParams.set("after", options.after);
  const response = await fetch(url, { signal: options.signal });
  if (!response.ok || response.body === null) {
    throw new Error(`stream failed ${response.status}: ${await response.text()}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const read = await reader.read();
      if (read.done) return;
      buffer += decoder.decode(read.value, { stream: true });
      let split = buffer.indexOf("\n\n");
      while (split >= 0) {
        const block = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        const event = parseBlock(block);
        if (event) yield event;
        split = buffer.indexOf("\n\n");
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

export function reduceStream(events: readonly StreamEvent[]): StreamView {
  const attempts = new Map<string, AttemptView>();
  let last = -1n;
  let runState: string | null = null;
  for (const event of events) {
    const seq = BigInt(event.seq);
    if (seq <= last) continue;
    last = seq;
    if (event.kind === "chunk") {
      const stepKey = stringField(event.data.stepKey);
      const attempt = numberField(event.data.attempt);
      const current = attemptView(attempts, stepKey, attempt);
      if (!current.superseded) current.text += stringField(event.data.text);
    } else if (event.kind === "superseded") {
      const stepKey = stringField(event.data.stepKey);
      const attempt = numberField(event.data.oldAttempt);
      attemptView(attempts, stepKey, attempt).superseded = true;
    } else if (event.kind === "run_terminal") {
      const state = event.data.state;
      if (typeof state === "string") runState = state;
    }
  }
  return {
    attempts: [...attempts.values()].sort((left, right) => left.stepKey.localeCompare(right.stepKey) || left.attempt - right.attempt),
    runState,
  };
}

function attemptView(attempts: Map<string, AttemptView>, stepKey: string, attempt: number): AttemptView {
  const key = `${stepKey}:${attempt}`;
  const current = attempts.get(key);
  if (current) return current;
  const created = { stepKey, attempt, text: "", superseded: false };
  attempts.set(key, created);
  return created;
}

function parseBlock(block: string): StreamEvent | null {
  let seq = "";
  let kind = "";
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith(":")) continue;
    if (line.startsWith("id:")) seq = line.slice(3).trim();
    else if (line.startsWith("event:")) kind = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).trim());
  }
  if (seq.length === 0 || kind.length === 0 || data.length === 0) return null;
  const parsed = JSON.parse(data.join("\n")) as Json;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
  return { seq, kind, data: parsed };
}

function stringField(value: Json | undefined): string {
  return typeof value === "string" ? value : "";
}

function numberField(value: Json | undefined): number {
  return typeof value === "number" ? value : Number(value);
}
