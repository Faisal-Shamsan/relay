import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { loadFailpointPlan } from "../src/failpoints.js";

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), "../../src");

test("sdk source does not import the server", () => {
  const packageJson = readFileSync(join(sourceDir, "../package.json"), "utf8");
  assert.equal(packageJson.includes("relay-server"), false);
  for (const file of readdirSync(sourceDir)) {
    if (!file.endsWith(".ts")) continue;
    const source = readFileSync(join(sourceDir, file), "utf8");
    assert.equal(source.includes("relay-server"), false, file);
  }
});

test("production failpoint activation is rejected", () => {
  const previous = process.env.RELAY_ALLOW_FAILPOINTS;
  delete process.env.RELAY_ALLOW_FAILPOINTS;
  try {
    assert.throws(
      () => loadFailpointPlan({ points: [{ point: "step.after-user-function-before-completion", occurrence: 1, action: "crash-process" }] }),
      /rejected/,
    );
    assert.deepEqual(loadFailpointPlan(null), []);
  } finally {
    if (previous === undefined) delete process.env.RELAY_ALLOW_FAILPOINTS;
    else process.env.RELAY_ALLOW_FAILPOINTS = previous;
  }
});
