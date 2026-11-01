import { defineWorkflow, runActivation } from "../../packages/sdk/dist/src/index.js";

const serverUrl = process.env.RELAY_SERVER_URL ?? "http://127.0.0.1:8080";
const apiKey = process.env.RELAY_WORKER_KEY ?? "relay_dev_worker_key";

const workflow = defineWorkflow({ name: "approval", version: "approval-v1" }, async (ctx) => {
  const decision = await ctx.waitForEvent("approval", { timeoutMs: 3_600_000 });
  return ctx.step("apply", async () => decision);
});

let waiting = false;
for (;;) {
  try {
    const output = await runActivation({
      serverUrl,
      apiKey,
      workflow,
      finishRun: true,
    });
    waiting = false;
    console.log(JSON.stringify(output));
  } catch (error) {
    const name = error instanceof Error ? error.name : "";
    const message = error instanceof Error ? error.message : String(error);
    if (name === "Suspended") {
      waiting = false;
      console.log("parked until a signal or a cancel");
      await delay(1_000);
      continue;
    }
    if (message.includes("no compatible ready run") || message.includes("run is terminal")) {
      if (!waiting) console.log("waiting for an approval run");
      waiting = true;
      await delay(1_000);
      continue;
    }
    throw error;
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
