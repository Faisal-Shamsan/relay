import { appendFileSync, writeFileSync } from "node:fs";
import type { Json } from "@relay/protocol";
import { readFailpointFile } from "./failpoints.js";
import { runActivation } from "./worker.js";
import { defineWorkflow, ParallelStepError } from "./workflow.js";

const serverUrl = requiredEnv("RELAY_SERVER_URL");
const invocationLog = requiredEnv("RELAY_INVOCATION_LOG");
const resultFile = requiredEnv("RELAY_RESULT_FILE");
const failpointFile = process.env.RELAY_FAILPOINT_FILE;
if (failpointFile) process.env.RELAY_ALLOW_FAILPOINTS = "1";

const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
  try {
    const results = await ctx.step.parallel("group", [
      {
        name: "a",
        run: async (stepCtx) => {
          appendFileSync(invocationLog, `${JSON.stringify({ step: "a", attempt: stepCtx.attempt })}\n`);
          return { item: "a" };
        },
      },
      {
        name: "b",
        run: async (stepCtx) => {
          appendFileSync(invocationLog, `${JSON.stringify({ step: "b", attempt: stepCtx.attempt })}\n`);
          return { item: "b" };
        },
      },
      {
        name: "c",
        run: async (stepCtx) => {
          appendFileSync(invocationLog, `${JSON.stringify({ step: "c", attempt: stepCtx.attempt })}\n`);
          throw new Error("c failed");
        },
      },
    ], { concurrency: 3 });
    return { failures: [], results };
  } catch (error) {
    if (!(error instanceof ParallelStepError)) throw error;
    const output: Json = {
      failures: error.failures.map((failure) => ({
        index: failure.index,
        stepKey: failure.stepKey,
        message: failure.error.message,
      })),
      results: [...error.results],
    };
    writeFileSync(resultFile, JSON.stringify(output));
    return output;
  }
});

const failpoints = failpointFile ? await readFailpointFile(failpointFile) : null;
await runActivation({
  serverUrl,
  workflow,
  failpoints,
  barrierDir: process.env.RELAY_BARRIER_DIR,
});

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
