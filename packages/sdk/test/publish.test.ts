import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync, unlinkSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const sdkDir = join(dirname(fileURLToPath(import.meta.url)), "../..");
const protocolDir = join(sdkDir, "../protocol");

const PUBLIC = [
  "AttemptView",
  "HeartbeatSchedule",
  "LintFinding",
  "LintRule",
  "ParallelChild",
  "ParallelFailure",
  "ParallelStepError",
  "RetryPolicy",
  "StepContext",
  "StepExecutionError",
  "StepOptions",
  "StreamEvent",
  "StreamExpiredError",
  "StreamView",
  "WorkflowContext",
  "WorkflowDefinition",
  "defineWorkflow",
  "lintWorkflow",
  "reduceStream",
  "runActivation",
  "cancel",
  "signal",
  "subscribe",
];

const RESERVED = ["loadFailpointPlan", "rebuild", "metrics"];

test("the sdk entry exports only the implemented surface", () => {
  const source = readFileSync(join(sdkDir, "src/index.ts"), "utf8");
  const names = [...source.matchAll(/[A-Za-z][A-Za-z0-9]*/g)].map((match) => match[0]);
  const exported = PUBLIC.filter((name) => names.includes(name));
  assert.deepEqual(exported, PUBLIC);
  for (const name of RESERVED) {
    assert.equal(new RegExp(`\\b${name}\\b`).test(source), false, name);
  }
  const declarations = readFileSync(join(sdkDir, "dist/src/index.d.ts"), "utf8");
  for (const name of PUBLIC) {
    assert.equal(declarations.includes(name), true, name);
  }
  for (const name of RESERVED) {
    assert.equal(new RegExp(`\\b${name}\\b`).test(declarations), false, name);
  }
});

test("the sdk and protocol packages pack without tests or future entry points", () => {
  const protocol = JSON.parse(readFileSync(join(protocolDir, "package.json"), "utf8")) as { version: string; private?: boolean };
  const sdk = JSON.parse(readFileSync(join(sdkDir, "package.json"), "utf8")) as {
    version: string;
    private?: boolean;
    dependencies: { "@relay/protocol": string };
  };
  assert.equal(protocol.private, undefined);
  assert.equal(sdk.private, undefined);
  assert.equal(sdk.version, protocol.version);
  assert.equal(sdk.dependencies["@relay/protocol"], protocol.version);
  const protocolFiles = packedFiles(protocolDir);
  const sdkFiles = packedFiles(sdkDir);
  assert.equal(protocolFiles.some((file) => file.endsWith("dist/src/index.js")), true);
  assert.equal(protocolFiles.some((file) => file.endsWith("dist/src/index.d.ts")), true);
  assert.equal(sdkFiles.some((file) => file.endsWith("dist/src/index.js")), true);
  assert.equal(sdkFiles.some((file) => file.endsWith("dist/src/index.d.ts")), true);
  for (const file of [...protocolFiles, ...sdkFiles]) {
    assert.equal(file.includes("dist/test"), false, file);
    assert.equal(file.endsWith(".ts") && !file.endsWith(".d.ts"), false, file);
  }
});

function packedFiles(directory: string): string[] {
  const result = spawnSync("npm", ["pack", "--dry-run", "--json"], {
    cwd: directory,
    encoding: "utf8",
    shell: true,
  });
  assert.equal(result.status, 0, result.stderr);
  for (const name of readdirSync(directory)) {
    if (name.endsWith(".tgz")) unlinkSync(join(directory, name));
  }
  const packed = JSON.parse(result.stdout) as Array<{ files: Array<{ path: string }> }>;
  return packed[0]?.files.map((file) => file.path.replaceAll("\\", "/")) ?? [];
}
