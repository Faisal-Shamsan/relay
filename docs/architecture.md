# Architecture

> Current architecture summary. The technical specification remains authoritative.

This page restates the ownership model in [spec.md](spec.md) §1. It does not add modules, package layouts, or deployment files.

## Components

**User application.** Hosts workflow code and starts SDK workers. The Relay server never receives or executes workflow source code.

**`@relay/sdk`.** Workflow registry and in-process worker: lease and heartbeat loop, replay engine, and step functions. Step functions call providers, tools, or the user's own database. The SDK does not access Relay's database and does not import server internals.

**`relay-server`.** Public read API, versioned worker protocol, scheduler, lease reaper, and SSE fan-out. It validates requests, performs transitions, allocates sequences, checks ownership, routes work, and serves reads. It does not own user workflow code or provider-specific semantics. The server image contains no user code. Multiple server processes may share one database.

**`relay-dashboard`.** Inspection and presentation through the server read API and SSE. It does not hold direct database credentials or a second authorization rule. The server-side proxy attaches `RELAY_API_KEY`. The browser does not receive that key.

**PostgreSQL.** Durable commits, constraints, row locking, and authoritative time. It does not deliver external side effects. The database is reachable by the server and by migration tooling, not by browsers or SDK clients.

## Protocols and APIs

**Worker protocol.** Public, versioned HTTP API between the SDK worker and the server. Shared runtime schemas belong in a small protocol package, as specified. Phase 1 routes are in [spec.md](spec.md) §10.3.

**Public HTTP API.** Client and operator HTTP API. The routes are in [spec.md](spec.md) §10.2, including signal and cancel. The dashboard uses this API. A bearer token binds the request to one tenant. The database stores only the SHA-256 of the key.

## Ownership boundaries

| Component | Owns | Does not own |
|---|---|---|
| SDK worker | Workflow execution, replay checking, callback isolation, buffering, provider calls | Database access or lease authority |
| Server | Validation, transitions, sequence allocation, ownership, routing, scheduling, reads | User workflow code or provider-specific semantics |
| PostgreSQL | Durable commits, constraints, row locking, authoritative time | Delivery of external side effects |
| Dashboard | Inspection and presentation through server APIs | Direct database credentials or separate authorization rules |

## Stored responsibilities

These are the specification's storage responsibilities, not a migration or package layout.

- `run_events` — authoritative workflow history
- `run_stream_events` — stream chunks and stream control events (Phase 2); authoritative for stream content
- `runs`, `commands`, `steps`, `step_attempts` — projections and coordination
- `run_queue`, `operation_receipts` — scheduling and request recovery
- `api_credentials` — SHA-256 hashes of tenant API keys; `relay_app` cannot read this table
- `signal_inbox`, `wait_deadlines` — the signal projection and a parked wait's recorded wakeup
- `tenants.last_granted_at` — same-priority grant order; coordination state, not rebuilt from history

Business projections are rebuildable from workflow history. Fence, sequence allocators, live lease deadlines, queue availability, request receipts, retention markers, and `last_granted_at` are coordination state. Rebuilding a timeline must not resurrect a live lease, lower a fence, rewind a sequence counter, delete a deduplication receipt, or move a tenant's last grant time.

A caller authenticated as one tenant cannot read, stream, or mutate another tenant's run. A missing run and another tenant's run look the same. Public internet exposure and untrusted worker registration are still not a supported deployment. The specification says Compose starts Postgres and the server, and a documented example application starts its SDK workers separately. This repository does not yet contain those artifacts. Same-priority grants prefer the tenant that has waited longer. A higher priority still comes first. Signals, waits, and cancellation are implemented. A terminal run does not become runnable again. Explicit redrive creates a new run with the same pinned version and leaves the original run terminal.
