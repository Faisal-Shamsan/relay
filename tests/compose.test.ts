import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("compose starts the dashboard without a database and leaves the example outside the server image", () => {
  const root = repoRoot();
  const compose = readFileSync(join(root, "compose.yaml"), "utf8");
  const dashboardAt = compose.indexOf("\n  dashboard:");
  assert.notEqual(dashboardAt, -1);
  const dashboard = compose.slice(dashboardAt);
  assert.match(dashboard, /RELAY_SERVER_URL/);
  assert.match(dashboard, /RELAY_API_KEY/);
  assert.doesNotMatch(dashboard, /DATABASE_URL|RELAY_DATABASE_URL|RELAY_ADMIN_URL/);
  assert.match(compose, /RELAY_DATABASE_URL/);
  assert.match(compose, /RELAY_ADMIN_URL/);

  const serverImage = readFileSync(join(root, "deploy", "server.Dockerfile"), "utf8");
  assert.match(serverImage, /packages\/server/);
  assert.match(serverImage, /packages\/protocol/);
  assert.doesNotMatch(serverImage, /examples/);

  const worker = readFileSync(join(root, "examples", "approval", "worker.mjs"), "utf8");
  const example = readFileSync(join(root, "examples", "approval", "README.md"), "utf8");
  assert.match(worker, /waitForEvent\("approval"/);
  assert.doesNotMatch(worker, /from ["']pg["']|relay-server|@relay\/server/);
  assert.match(example, /\/signal/);
  assert.match(example, /\/cancel/);
  assert.match(example, /relay_dev_client_key/);
  assert.match(example, /relay_dev_worker_key/);
});

function repoRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 6; i += 1) {
    try {
      readFileSync(join(dir, "compose.yaml"));
      return dir;
    } catch {
      dir = dirname(dir);
    }
  }
  throw new Error("repository root was not found");
}
