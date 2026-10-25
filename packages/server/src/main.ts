import { migrate } from "./migrate.js";
import { verifyProjections } from "./projections.js";
import { startServer } from "./server.js";

const args = process.argv.slice(2);
if (args[0] === "projections" && args[1] === "verify") {
  const runId = args[2];
  const databaseUrl = process.env.RELAY_DATABASE_URL ?? process.env.RELAY_ADMIN_URL;
  if (!runId || !databaseUrl) {
    console.error("usage: node packages/server/dist/src/main.js projections verify <run-id>");
    process.exit(2);
  }
  const result = await verifyProjections(databaseUrl, runId);
  console.log(JSON.stringify(result, null, 2));
  process.exit(result.ok ? 0 : 1);
}

const app = process.env.RELAY_DATABASE_URL;
if (!app) throw new Error("RELAY_DATABASE_URL is required");
if (process.env.RELAY_ADMIN_URL) await migrate(process.env.RELAY_ADMIN_URL);
const port = process.env.PORT === undefined ? 8080 : Number(process.env.PORT);
if (!Number.isInteger(port) || port < 0 || port > 65535) {
  throw new Error("PORT must be an integer from 0 to 65535");
}
const server = await startServer({
  appDatabaseUrl: app,
  leaseDurationMs: 30_000,
  port,
});
console.log(server.url);
