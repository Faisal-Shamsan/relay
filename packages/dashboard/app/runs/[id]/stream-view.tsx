"use client";

import { useEffect, useState } from "react";

const kinds = ["chunk", "superseded", "step_completed", "step_failed", "run_terminal", "expired"];

export function StreamView({ runId }: { runId: string }) {
  const [lines, setLines] = useState<string[]>([]);
  useEffect(() => {
    const source = new EventSource(`/relay/v1/runs/${runId}/stream`);
    const add = (event: MessageEvent) => {
      setLines((current) => [...current, event.data].slice(-40));
    };
    for (const kind of kinds) source.addEventListener(kind, add);
    return () => source.close();
  }, [runId]);
  return <pre>{lines.length === 0 ? "No stream events yet." : lines.join("\n")}</pre>;
}
