import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../..");

export function startWorker(env: NodeJS.ProcessEnv, entry = "packages/sdk/dist/src/test-worker.js"): {
  child: ChildProcess;
  done: Promise<{ code: number; stderr: string }>;
} {
  const child = spawn(process.execPath, ["--import", "./scripts/test-auth.mjs", join(repoRoot, entry)], {
    cwd: repoRoot,
    env: { ...process.env, ...env },
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const done = new Promise<{ code: number; stderr: string }>((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code) => resolve({ code: code ?? 1, stderr }));
  });
  return { child, done };
}

export async function waitForFile(path: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (existsSync(path)) return;
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  throw new Error(`timed out waiting for ${path}`);
}
