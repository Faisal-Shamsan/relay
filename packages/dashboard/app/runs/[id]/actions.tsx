"use client";

import { useState, type FormEvent } from "react";
import { useRouter } from "next/navigation";

export function RunActions({ runId }: { runId: string }) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ text: string; runId?: string }>({ text: "" });

  async function send(action: "signal" | "cancel" | "redrive", body: unknown): Promise<void> {
    setBusy(true);
    try {
      const response = await fetch(`/relay/v1/runs/${runId}/${action}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await response.json() as {
        message?: string;
        runId?: string;
        ingressSeq?: string;
        state?: string;
      };
      if (!response.ok) {
        setResult({ text: payload.message ?? `${response.status}` });
        return;
      }
      if (action === "redrive" && typeof payload.runId === "string") {
        setResult({ text: "New run", runId: payload.runId });
      } else if (action === "signal") {
        setResult({ text: `Signal stored at ${payload.ingressSeq ?? ""}` });
      } else {
        setResult({ text: `Cancel recorded ${payload.state ?? ""}` });
      }
      router.refresh();
    } catch (error) {
      setResult({ text: error instanceof Error ? error.message : "request failed" });
    } finally {
      setBusy(false);
    }
  }

  function onSignal(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    let payload: unknown;
    try {
      payload = JSON.parse(String(data.get("payload") ?? ""));
    } catch {
      setResult({ text: "payload is not JSON" });
      return;
    }
    void send("signal", {
      name: String(data.get("name") ?? ""),
      payload,
      idempotencyKey: String(data.get("idempotencyKey") ?? ""),
    });
  }

  function onCancel(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    void send("cancel", { protocolVersion: 1 });
  }

  function onRedrive(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const raw = String(data.get("input") ?? "").trim();
    const body: { idempotencyKey: string; input?: unknown } = {
      idempotencyKey: String(data.get("idempotencyKey") ?? ""),
    };
    if (raw !== "") {
      try {
        body.input = JSON.parse(raw);
      } catch {
        setResult({ text: "input is not JSON" });
        return;
      }
    }
    void send("redrive", body);
  }

  return (
    <>
      <h2>Actions</h2>
      <form onSubmit={onSignal}>
        <label>
          Signal name
          <input name="name" required autoComplete="off" />
        </label>
        <label>
          Payload
          <input name="payload" required defaultValue="{}" autoComplete="off" />
        </label>
        <label>
          Idempotency key
          <input name="idempotencyKey" required autoComplete="off" />
        </label>
        <button type="submit" disabled={busy}>Signal</button>
      </form>
      <form onSubmit={onCancel}>
        <button type="submit" disabled={busy}>Cancel</button>
      </form>
      <form onSubmit={onRedrive}>
        <label>
          Redrive key
          <input name="idempotencyKey" required autoComplete="off" />
        </label>
        <label>
          Input
          <input name="input" placeholder="stored input" autoComplete="off" />
        </label>
        <button type="submit" disabled={busy}>Redrive</button>
      </form>
      <p id="action-result">
        {result.text}
        {result.runId ? <> <a href={`/runs/${result.runId}`}>{result.runId}</a></> : null}
      </p>
    </>
  );
}
