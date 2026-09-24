# Architecture

> Current architecture summary. The technical specification remains authoritative.

This page restates the ownership model in [spec.md](spec.md) §1. It does not add modules, package layouts, or deployment files.

## Components

**User application.** Hosts workflow code and starts SDK workers. The Relay server never receives or executes workflow source code.

**`@relay/sdk`.** Workflow registry and in-process worker: lease and heartbeat loop, replay engine, and step functions. Step functions call providers, tools, or the user's own database. The SDK does not access Relay's database and does not import server internals.

**`relay-server`.** Public read API, versioned worker protocol, scheduler, lease reaper, and SSE fan-out. It validates requests, performs transitions, allocates sequences, checks ownership, routes work, and serves reads. It does not own user workflow code or provider-specific semantics. The server image contains no user code. Multiple server processes may share one database.

**`relay-dashboard`.** Phase 3. Inspection and presentation through the server read API and SSE. It does not hold direct database credentials or separate authorization rules.

**PostgreSQL.** Durable commits, constraints, row locking, and authoritative time. It does not deliver external side effects. The database is reachable by the server and by migration tooling, not by browsers or SDK clients.

## Protocols and APIs

**Worker protocol.** Public, versioned HTTP API between the SDK worker and the server. Shared runtime schemas belong in a small protocol package, as specified. Phase 1 routes are in [spec.md](spec.md) §10.3.

**Public HTTP API.** Client and operator HTTP API. Phase 1 routes are in [spec.md](spec.md) §10.2. The dashboard uses this API.

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

Business projections are rebuildable from workflow history. Fence, sequence allocators, live lease deadlines, queue availability, request receipts, and retention markers are coordination state. Rebuilding a timeline must not resurrect a live lease, lower a fence, rewind a sequence counter, or delete a deduplication receipt.

Before Phase 4, the specified deployment is a trusted, single-tenant environment with one seeded tenant. Tenant columns preserve data boundaries, but that is not implemented multi-tenancy. Public internet exposure and untrusted worker registration are not supported in that mode. The specification says Compose starts Postgres and the server in Phase 1, and adds the dashboard in Phase 3. A documented example application starts its SDK workers separately. This repository does not yet contain those artifacts.
