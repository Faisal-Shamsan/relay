import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), "../../src");

test("server source does not load workflow functions or expose a failpoint route", () => {
  for (const file of readdirSync(sourceDir)) {
    if (!file.endsWith(".ts")) continue;
    const source = readFileSync(join(sourceDir, file), "utf8");
    assert.equal(source.includes("defineWorkflow"), false, file);
    assert.equal(source.includes("@relay/sdk"), false, file);
    assert.equal(source.includes("/failpoint"), false, file);
    assert.equal(source.includes("/v1/runs/:id/events"), false, file);
  }
});
