import { spawnSync } from "node:child_process";
import pg from "pg";

const url = process.env.RELAY_ADMIN_URL ?? "postgres://postgres@127.0.0.1:54329/postgres";
let lastError = "PostgreSQL did not become ready";

if (await canConnect(url)) {
  process.exit(0);
}

const started = spawnSync(
  "docker",
  [
    "run",
    "-d",
    "--name",
    "relay-m1-pg",
    "-e",
    "POSTGRES_HOST_AUTH_METHOD=trust",
    "-e",
    "POSTGRES_DB=postgres",
    "-p",
    "54329:5432",
    "postgres:16",
  ],
  { encoding: "utf8" },
);
if (started.status !== 0 && !String(started.stderr).includes("already in use")) {
  spawnSync("docker", ["start", "relay-m1-pg"], { stdio: "inherit" });
}

const deadline = Date.now() + 60_000;
while (Date.now() < deadline) {
  if (await canConnect(url)) process.exit(0);
  await new Promise((resolve) => setTimeout(resolve, 500));
}
console.error(lastError);
process.exit(1);

async function canConnect(connectionString) {
  const client = new pg.Client({ connectionString });
  try {
    await client.connect();
    await client.query("SELECT 1");
    await client.end();
    return true;
  } catch (error) {
    lastError = error instanceof Error ? error.message : String(error);
    try {
      await client.end();
    } catch {
      // The client may not have connected.
    }
    return false;
  }
}
