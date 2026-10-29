const LOCAL_CLIENT_KEY = "relay_dev_client_key";

export function serverUrl(): string {
  return process.env.RELAY_SERVER_URL ?? "http://127.0.0.1:8080";
}

export function clientKey(): string {
  return process.env.RELAY_API_KEY ?? LOCAL_CLIENT_KEY;
}

export async function readJson<T>(path: string): Promise<T> {
  const response = await fetch(`${serverUrl()}${path}`, {
    cache: "no-store",
    headers: { authorization: `Bearer ${clientKey()}` },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${path}`);
  }
  return response.json() as Promise<T>;
}

export interface RunSummary {
  runId: string;
  state: string;
  workflowName: string;
  workflowVersion: string;
}

export interface RunDetail {
  runId: string;
  workflowName: string;
  workflowVersion: string;
  state: string;
  output: unknown;
  error: unknown;
  ownership: {
    fence: string;
    queueState: string | null;
    leaseOwner: string | null;
    leaseExpiresAt: string | null;
  };
}

export interface StepView {
  stepKey: string;
  state: string;
  attempt: number;
  childIndex: number;
  explicitInput: boolean;
  input?: unknown;
  output: unknown;
  error: unknown;
  attempts: Array<{ attempt: number; state: string; fence: string }>;
}

export interface HistoryEvent {
  seq: string;
  fence: string;
  type: string;
  stepKey: string | null;
  attempt: number | null;
  payload: { exhausted?: unknown; reason?: unknown };
}
