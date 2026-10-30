# Changelog

## Unreleased

- A bearer token binds each public and worker request to one tenant. The database stores only the SHA-256 of the key. A client key and a worker key are different kinds. Another tenant's run is the same 404 as a missing run.
- At the same priority, a lease prefers the tenant that has waited longer since its last grant. A higher priority still comes first.
- `POST /v1/runs/:id/signal` stores a deduplicated signal. `ctx.waitForEvent` consumes the oldest matching signal or one recorded timeout.
- `POST /v1/runs/:id/cancel` races completion. The winner is the only terminal record. A later worker write is rejected. An effect the callback already performed is not undone.
- The dashboard attaches `RELAY_API_KEY` on the server. The browser does not receive the key.
- Worker history and the public event list are ordered by the numeric event sequence.
