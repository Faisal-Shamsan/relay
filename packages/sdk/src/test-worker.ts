import { appendFileSync } from "node:fs";
import { readFailpointFile } from "./failpoints.js";
import { runActivation } from "./worker.js";
import { defineWorkflow } from "./workflow.js";

const serverUrl = requiredEnv("RELAY_SERVER_URL");
const invocationLog = requiredEnv("RELAY_INVOCATION_LOG");
const failpointFile = process.env.RELAY_FAILPOINT_FILE;
if (failpointFile) process.env.RELAY_ALLOW_FAILPOINTS = "1";

const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
  return ctx.step("only", async (stepCtx) => {
    appendFileSync(
      invocationLog,
      `${JSON.stringify({ attempt: stepCtx.attempt, idempotencyKey: stepCtx.idempotencyKey })}\n`,
    );
    return { ok: true };
  });
});

const failpoints = failpointFile ? await readFailpointFile(failpointFile) : null;
await runActivation({
  serverUrl,
  workflow,
  finishRun: process.env.RELAY_FINISH_RUN !== "0",
  failpoints,
  barrierDir: process.env.RELAY_BARRIER_DIR,
  completionRequestPath: process.env.RELAY_COMPLETION_REQUEST_PATH,
});

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
