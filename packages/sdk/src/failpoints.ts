import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

export interface FailpointArm {
  point: string;
  match?: { runId?: string; stepKey?: string; attempt?: number };
  occurrence: number;
  action: "crash-process" | "pause-on-barrier";
}

interface ArmedPoint extends FailpointArm {
  seen: number;
}

export function loadFailpointPlan(plan: { points: FailpointArm[] } | null): ArmedPoint[] {
  if (plan === null) return [];
  if (process.env.RELAY_ALLOW_FAILPOINTS !== "1") {
    throw new Error("failpoint activation is rejected");
  }
  return plan.points.map((point) => ({ ...point, seen: 0 }));
}

export async function hitFailpoint(
  points: ArmedPoint[],
  point: string,
  match: { runId: string; stepKey?: string; attempt?: number },
  barrierDir: string | undefined,
): Promise<void> {
  for (const armed of points) {
    if (armed.point !== point) continue;
    if (armed.match?.runId !== undefined && armed.match.runId !== match.runId) continue;
    if (armed.match?.stepKey !== undefined && armed.match.stepKey !== match.stepKey) continue;
    if (armed.match?.attempt !== undefined && armed.match.attempt !== match.attempt) continue;
    armed.seen += 1;
    if (armed.seen !== armed.occurrence) continue;
    if (armed.action === "crash-process") {
      process.exit(78);
    }
    if (armed.action === "pause-on-barrier") {
      if (!barrierDir) throw new Error("barrier directory is required");
      await mkdir(barrierDir, { recursive: true });
      await writeFile(join(barrierDir, "hit.json"), JSON.stringify({ point, ...match }));
      while (!existsSync(join(barrierDir, "release"))) {
        await new Promise((resolve) => setTimeout(resolve, 15));
      }
    }
  }
}

export async function readFailpointFile(path: string): Promise<{ points: FailpointArm[] }> {
  return JSON.parse(await readFile(path, "utf8")) as { points: FailpointArm[] };
}
