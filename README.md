# Relay

Relay is a self-hostable TypeScript durable-execution engine. [docs/spec.md](docs/spec.md) is authoritative. [PROJECT.md](PROJECT.md) says what the project is for. This file is how to run the Phase 1 server.

Phase 1 stores workflow history in one Postgres database and recovers a crashed worker from that history. It does not claim exactly-once side effects.

## Requirements

- Node.js 22
- Docker, used to start Postgres 16 when it is not already listening

## Test

```sh
npm install
npm test
```

`npm test` starts the local Postgres container if needed, builds the packages, and runs the suite against real databases. The failure demo prints a ledger. Those timings are measurements from that run, not a performance promise. Stream catch-up, 1,000-run, and 10,000-run figures are not measured.

To reprint the demo ledger:

```sh
node --test --test-concurrency=1 tests/dist/demo.test.js
```

## Run a server

Start Postgres the same way the tests do:

```sh
docker run -d --name relay-m1-pg -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=postgres -p 54329:5432 postgres:16
docker exec relay-m1-pg psql -U postgres -c "CREATE DATABASE relay"
npm run build
```

PowerShell:

```powershell
$env:RELAY_ADMIN_URL = "postgres://postgres@127.0.0.1:54329/relay"
$env:RELAY_DATABASE_URL = "postgres://relay_app:relay_app_dev@127.0.0.1:54329/relay"
npm start
```

`npm start` applies the SQL migrations with the admin URL, then listens on port 8080 as `relay_app`. The application role can read tables and call the Relay functions. It cannot write the tables directly.

Create a run:

```sh
curl -s -X POST http://127.0.0.1:8080/v1/runs -H "content-type: application/json" -d "{\"workflowName\":\"demo\",\"workflowVersion\":\"demo-v1\",\"input\":{}}"
```

Drive it with a worker. After `npm run build`, this completes one step:

```javascript
import { defineWorkflow, runActivation } from "@relay/sdk";

const workflow = defineWorkflow({ name: "demo", version: "demo-v1" }, async (ctx) => {
  return ctx.step("only", async () => ({ ok: true }));
});

await fetch("http://127.0.0.1:8080/v1/runs", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ workflowName: "demo", workflowVersion: "demo-v1", input: {} }),
});
console.log(await runActivation({ serverUrl: "http://127.0.0.1:8080", workflow }));
```

Read it back:

```sh
curl -s http://127.0.0.1:8080/v1/runs/<run-id>
curl -s "http://127.0.0.1:8080/v1/runs/<run-id>/events?limit=100"
curl -s http://127.0.0.1:8080/v1/workflows
```

Check projections without changing them:

```sh
node packages/server/dist/src/main.js projections verify <run-id>
```

The command compares commands, steps, attempts, and the business fields of the run with the event history. It also checks that the event, command, step, fence, and stream counters still match, and that chunk batches and completion controls line up with the attempts. An expired stream matches when its rows are gone and its sequence counter still points past the last allocated id. It does not rebuild rows, and it does not rewind leases or receipts. Exit 0 means the comparison matched.

## Limits

- A step callback can run more than once. A committed result is not run again. A fake provider may skip a repeated effect; Relay does not make the external call exactly once.
- Phase 1 does not retry a callback that threw. Crash recovery stops at the recovery budget stored on the step. The default is 100.
- Workers advertise an exact workflow version. A run pinned to v1 stays pending while only a v2 worker is alive.
- Replay compares command shape. A changed captured value with the same command shape is not detected.
- A worker can append a durable stream batch, and a client can resume that log over SSE. A finished run's stream log expires seven days after completion. The run result remains, and a later read returns 410. There is no timer, signal, dashboard, or metrics endpoint.
- JSON follows the rules in `packages/protocol/src/json.ts`: the root counts as depth 1, unsafe integers are rejected, the receipt digest is SHA-256, and strings are not Unicode-normalized.
- Invalid JSON is rejected with a path. It is not rewritten into a stored success.

## Failover

Relay uses one Postgres primary. A response means that transaction committed. With Postgres `synchronous_commit` left at its default, `on`, that commit is durable on the primary. Relay does not ship a replica. If you add an async replica, its lag is your recovery point, and Relay does not measure that lag.

A crash can lose a transaction that had not committed. If a request ends without a response, do not assume it rolled back and do not send a new operation id. Read the receipt or `GET /v1/runs/<id>/events`. The same operation id returns the stored result when the commit won, and performs the transition when it did not.
