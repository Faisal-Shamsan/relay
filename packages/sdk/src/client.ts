import { PROTOCOL_VERSION, type Json } from "@relay/protocol";

export async function signal(options: {
  serverUrl: string;
  runId: string;
  name: string;
  payload: Json;
  idempotencyKey: string;
  apiKey?: string;
}): Promise<{ ingressSeq: string; name: string; idempotencyKey: string }> {
  const headers: { [key: string]: string } = { "content-type": "application/json" };
  if (options.apiKey) headers.authorization = `Bearer ${options.apiKey}`;
  const response = await fetch(`${options.serverUrl}/v1/runs/${options.runId}/signal`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      protocolVersion: PROTOCOL_VERSION,
      name: options.name,
      payload: options.payload,
      idempotencyKey: options.idempotencyKey,
    }),
  });
  const text = await response.text();
  const parsed = text.length === 0 ? null : JSON.parse(text) as Json;
  if (!response.ok) {
    throw new Error(`signal failed ${response.status}: ${text}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) || typeof parsed.ingressSeq !== "string") {
    throw new Error("signal response did not include an ingress sequence");
  }
  return {
    ingressSeq: parsed.ingressSeq,
    name: typeof parsed.name === "string" ? parsed.name : options.name,
    idempotencyKey: options.idempotencyKey,
  };
}

export async function cancel(options: {
  serverUrl: string;
  runId: string;
  apiKey?: string;
}): Promise<{ state: string; output: Json | null; error: Json | null }> {
  const headers: { [key: string]: string } = { "content-type": "application/json" };
  if (options.apiKey) headers.authorization = `Bearer ${options.apiKey}`;
  const response = await fetch(`${options.serverUrl}/v1/runs/${options.runId}/cancel`, {
    method: "POST",
    headers,
    body: JSON.stringify({ protocolVersion: PROTOCOL_VERSION }),
  });
  const text = await response.text();
  const parsed = text.length === 0 ? null : JSON.parse(text) as Json;
  if (!response.ok) {
    throw new Error(`cancel failed ${response.status}: ${text}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) || typeof parsed.state !== "string") {
    throw new Error("cancel response did not include a state");
  }
  return {
    state: parsed.state,
    output: "output" in parsed ? parsed.output ?? null : null,
    error: "error" in parsed ? parsed.error ?? null : null,
  };
}
