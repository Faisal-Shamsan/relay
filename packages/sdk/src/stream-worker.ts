import { appendFileSync } from "node:fs";
import { readFailpointFile } from "./failpoints.js";
import { runActivation } from "./worker.js";
import { defineWorkflow } from "./workflow.js";

const serverUrl = requiredEnv("RELAY_SERVER_URL");
const invocationLog = requiredEnv("RELAY_INVOCATION_LOG");
const text = requiredEnv("RELAY_STREAM_TEXT");
const mode = process.env.RELAY_STREAM_MODE ?? "single";
const failpointFile = process.env.RELAY_FAILPOINT_FILE;
if (failpointFile) process.env.RELAY_ALLOW_FAILPOINTS = "1";

const workflow = mode === "parallel"
  ? defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      const results = await ctx.step.parallel("group", [
        {
          name: "a",
          run: async (stepCtx) => {
            appendFileSync(invocationLog, `${JSON.stringify({ step: "a", attempt: stepCtx.attempt })}\n`);
            await stepCtx.emit(text);
            return { item: "a" };
          },
        },
        {
          name: "b",
          run: async (stepCtx) => {
            appendFileSync(invocationLog, `${JSON.stringify({ step: "b", attempt: stepCtx.attempt })}\n`);
            await stepCtx.emit(text);
            return { item: "b" };
          },
        },
      ], { concurrency: 2 });
      return { results };
    })
  : defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
      return ctx.step("only", async (stepCtx) => {
        appendFileSync(invocationLog, `${JSON.stringify({ step: "only", attempt: stepCtx.attempt })}\n`);
        await stepCtx.emit(text);
        return { ok: true };
      });
    });

const failpoints = failpointFile ? await readFailpointFile(failpointFile) : null;
await runActivation({
  serverUrl,
  workflow,
  failpoints,
  barrierDir: process.env.RELAY_BARRIER_DIR,
  completionRequestPath: process.env.RELAY_COMPLETION_REQUEST_PATH,
});

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
