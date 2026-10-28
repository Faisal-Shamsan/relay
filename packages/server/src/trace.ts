export interface RequestSpan {
  requestId: string;
  method: string;
  target: string;
  status: number;
  runId?: string;
  fence?: string;
  stepKey?: string;
  attempt?: string;
  activity?: string;
}

export async function exportRequestSpan(endpoint: string | undefined, span: RequestSpan): Promise<void> {
  if (!endpoint) return;
  const attributes = [
    attribute("relay.request_id", span.requestId),
    attribute("http.request.method", span.method),
    attribute("url.path", span.target),
  ];
  if (span.runId) attributes.push(attribute("relay.run_id", span.runId));
  if (span.fence) attributes.push(attribute("relay.fence", span.fence));
  if (span.stepKey) attributes.push(attribute("relay.step_key", span.stepKey));
  if (span.attempt) attributes.push(attribute("relay.attempt", span.attempt));
  if (span.activity) attributes.push(attribute("relay.activity", span.activity));
  const traceId = span.requestId.replaceAll("-", "").padEnd(32, "0").slice(0, 32);
  const body = {
    resourceSpans: [{
      scopeSpans: [{
        spans: [{
          traceId,
          spanId: traceId.slice(0, 16),
          name: `${span.method} ${span.target.split("?")[0]}`,
          attributes,
        }],
      }],
    }],
  };
  try {
    await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(500),
    });
  } catch {
    // A lost trace is not a lost commit.
  }
}

function attribute(key: string, value: string): { key: string; value: { stringValue: string } } {
  return { key, value: { stringValue: value } };
}
