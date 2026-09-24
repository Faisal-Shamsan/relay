# Relay — Technical Specification v3

**Status:** Authoritative design baseline; implementation status is tracked separately.  
**Date:** 2026-09-24.  
**Supersedes:** `relay-spec-v2.md` and conflicting examples in the earlier build prompt.  
**Sources:** The full v2 attachment from “Write master prompt” and its substantive backend/recruiting review. The attachment originally identified as `/mnt/data/relay-spec-v2.md` was recovered from that conversation.  
**Normative language:** MUST, MUST NOT, SHOULD, and MAY specify implementation requirements. Phase tags define when a requirement ships, not whether it is optional within that phase.

## 0. Purpose, positioning, and scope

Relay is an open-source, self-hostable TypeScript durable execution engine. It runs ordinary async workflows, records step outcomes, reconstructs workflow state through replay, and supports resumable streaming. User code executes in the user's application process, inside an SDK worker. The Relay server never receives or executes workflow source code.

The project's central claim is demonstrable correctness under failure: implement replay, leases, fencing, idempotency, versioning, and recovery; deliberately break execution at meaningful boundaries; publish the evidence and limitations. Feature count and lines of generated code are not success metrics. An AI research agent is a useful example workload, not a reason to introduce an LLM abstraction layer.

Relay is a from-scratch engineering project, not a claim of production parity with established durable execution systems. Production adopters should independently evaluate established alternatives such as DBOS, Inngest, Restate, and Temporal. No comparative capability or reliability claims are implied here.

Success artifacts:

1. A runnable engine and repeatable failure demonstration using approximately 100 runs.
2. Dated ADRs tied to actual decisions and implementation changes.
3. An architecture document and invariant ledger that match the code.
4. A reproducible benchmark report including the first bottleneck and failed assumptions.
5. A three-minute recording showing a failure, its event history, and recovery.

**In scope by phase:** durable steps and replay; bounded parallel steps; leases and fencing; version pinning; retries; timers; resumable streams; inspection; observability; conditional signals, cancellation, and tenant isolation.

**Out of scope:** hosted billing, LLM/provider abstraction, visual builders, YAML/DSL workflows, vector search, custom consensus, Kubernetes orchestration, multi-region active-active, a plugin marketplace, and custom session cryptography.

**Measurement-gated:** Redis, brokers, object storage, replicas, partitioning, PgBouncer, sharding, and execution caches. Postgres-first remains intentional. Do not claim its throughput ceiling is irrelevant before measuring it.

## 1. Architecture and ownership

```text
User application
  @relay/sdk
    workflow registry + in-process worker
    lease/heartbeat loop + replay engine
    step functions ------------------------> providers / tools / user's DB
           |
           | versioned HTTP worker protocol
           v
  relay-server
    public/read API + worker protocol
    scheduler + lease reaper + SSE fan-out
           |
           v
       PostgreSQL
    run_events              workflow history
    run_stream_events       stream chunks AND stream control events
    runs / commands / steps / step_attempts     projections + coordination
    run_queue / operation_receipts             scheduling + request recovery

  relay-dashboard (Phase 3) ---> server read API and SSE
```

| Component | Owns | Does not own |
|---|---|---|
| SDK worker | Workflow execution, replay checking, callback isolation, buffering, provider calls | Database access or lease authority |
| Server | Validation, transitions, sequence allocation, ownership, routing, scheduling, reads | User workflow code or provider-specific semantics |
| PostgreSQL | Durable commits, constraints, row locking, authoritative time | Delivery of external side effects |
| Dashboard | Inspection and presentation through server APIs | Direct database credentials or separate authorization rules |

The server image contains no user code. Compose starts Postgres and the server in Phase 1; the dashboard is added in Phase 3. A documented example application starts its SDK workers separately. Multiple server processes may share one database.

The worker protocol is public, versioned API. Shared runtime schemas belong in a small protocol package. The SDK does not import server internals. The database is reachable by the server and migration tooling, not by browsers or SDK clients.

Before Phase 4, deployment is a trusted, single-tenant environment with one seeded tenant. Tenant columns preserve data boundaries but do not constitute implemented multi-tenancy. Public internet exposure and untrusted worker registration are not supported in that mode. Phase 4 adds API credentials, authorization, and RLS together.

## 2. Guarantees and explicit limits

### 2.1 What Relay guarantees

Subject to retained durable database history, compatible workflow code, and eventual database/worker availability:

- A step whose `StepCompleted` transaction committed is memoized. Recovery does not authorize or invoke a new attempt of that logical step in the same run. This does not revoke an older process's already-authorized callback.
- An unfinished attempt may run again. External effects are at-least-once attempts, not exactly-once delivery.
- Only the current unexpired lease holder may introduce worker-originated durable changes. The check is enforced inside database transition functions.
- A logical transition's history, projections, scheduling changes, and request receipt commit together.
- A run is pinned to one immutable workflow version.
- Runtime replay checking detects command-structure divergence. Optional explicit-input fingerprints detect changes in those supplied inputs.
- SSE can reconstruct retained committed stream events, including control events, from one per-run stream cursor. Transport delivery can repeat; clients apply events idempotently.

### 2.2 What Relay does not guarantee

- It cannot atomically commit a third-party effect and a Postgres completion record.
- It cannot detect arbitrary changed closures, environment values, mutated globals, provider behavior, or semantically different code with an unchanged command shape.
- Fencing prevents stale database writes; it cannot revoke an HTTP request already sent to a provider or forcibly stop arbitrary JavaScript. A process paused after checking ownership can later enter an already-authorized callback after another worker has completed the step. There is no atomic boundary between a database lease check and arbitrary user code.
- Streaming retries can generate different output and incur additional provider charges. Provider-specific idempotency or retrieval features may help, but Relay supplies no generic recovery guarantee for them.
- Database restore to an earlier point can lose acknowledged history. Async replication or failover configurations can also have nonzero RPO. Recovery guarantees do not cover lost committed history.
- Eventual completion assumes finite work, appropriate retry budgets, and eventual availability. A hung callback, missing compatible worker, or permanent provider failure does not imply eventual success.

### 2.3 Stable provider idempotency

`stepCtx.idempotencyKey` identifies the **logical step**, remaining unchanged across attempts and lease epochs:

```text
relay-step-v1:<hex SHA-256 of UTF-8 JCS([tenant_id, run_id, step_key])>
```

The versioned derivation is fixed for a run's protocol version. Never include the attempt or fence. This corrects v2's attempt-dependent key, which could not deduplicate successive attempts of the same effect.

`attemptToken` is a separate server-generated UUID identifying one invocation authorization. It changes on each new attempt. A user performing several independent effects inside one step must derive distinct provider keys using stable effect names, or split them into separate steps. Effectively-once outcomes require the provider's own deduplication contract, retention window, and identical request semantics; the SDK key alone does not provide them.

## 3. Serialization and explicit input contract [Phase 1]

### 3.1 Relay JSON

Workflow inputs/outputs, step results, explicit step inputs, signal payloads, command arguments, and metadata MUST use this contract:

```ts
type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
```

Runtime validation is stricter than that TypeScript alias:

- Allow finite IEEE-754 numbers; reject unsafe integer values. Encode larger integers as decimal strings. Normalize negative zero to zero.
- Allow valid Unicode strings; reject unpaired surrogates and U+0000 in values and property names to remain compatible with Postgres `jsonb`.
- Allow dense arrays and plain objects with own enumerable string data properties. Reject accessors, symbol keys, hidden properties, custom prototypes, and cycles. Shared references are serialized as repeated values, without identity preservation.
- Reject `undefined`, functions, symbols, `bigint`, `NaN`, infinities, `Date`, `Map`, `Set`, `Buffer`, typed arrays, `Error`, and class instances. Do not silently call `toJSON`, omit values, or coerce them to `null`.
- A callback with no useful result MUST explicitly return `null`; implicit `undefined` is an error.
- Raw HTTP JSON with duplicate object keys MUST be rejected before ordinary object parsing loses that information.

Validate before invoking a callback when an explicit input is supplied, and before committing any result. Initial limits: 1 MiB canonical UTF-8 per value, depth 64, 256 bytes per step/group/timer key, 1,000 children per parallel group, and 4 MiB per HTTP request. Boundary schemas impose smaller limits where specified. Changing these defaults requires a compatibility note; exceeding them produces a structured error, never truncation.

The canonical representation is RFC 8785 JCS over the validated value. Object-key order is not application data; array order is. SDK-visible values are canonicalized, parsed clones on both first execution and replay, so callers must not depend on object identity, original prototypes, or insertion order. Implement a validated canonicalizer, not `JSON.stringify` as a validator. JCS provides deterministic serialization for hashing; Relay's stricter allowed-value rules are its own contract. [RFC 8785](https://www.rfc-editor.org/rfc/rfc8785.html)

All database `bigint` identifiers—including event sequence, stream sequence, and fence—use decimal strings on JSON/HTTP boundaries. Do not coerce them to JavaScript numbers. Ordinary JSON business values follow the numeric rule above.

### 3.2 Explicit input fingerprints [Phase 2]

```ts
const prices = await ctx.step(
  "fetch-prices",
  { input: { region: input.region } },
  async (stepCtx, args) => fetchPrices(args.region, stepCtx.idempotencyKey)
);
```

Persist the normalized input and `sha256-jcs-v1` fingerprint with the command declaration. On replay, compare the supplied input with the recorded fingerprint **before returning a memoized result or starting an attempt**. Omitted input and explicit `null` differ. Switching between the simple and input-bearing overload is a descriptor mismatch. The callback receives a deeply frozen clone of the declared input.

Fingerprinting is optional at the call site, not a claim that closures are checked. `() => fetchPrices(process.env.REGION)` remains outside this protection. No source-code hashing or closure introspection is implied.

### 3.3 Errors

Thrown callback values are converted into bounded structured records:

```ts
type ErrorRecord = {
  name: string;
  code: string;
  message: string;
  details: Json;
  stack?: string; // diagnostics only, never part of workflow control flow
};
```

Initial and replayed failures both surface as `StepExecutionError` carrying this record, not the original custom exception prototype. Workflow branching may use stable `code` and `details`; it MUST NOT rely on stacks, object identity, or provider exception classes. Runtime errors such as divergence, invalid serialization, duplicate keys, and lease loss are separately classified. Serialization failures after an external effect do not undo that effect.

## 4. Execution, identity, and replay [Phase 1]

### 4.1 Workflow registration and context

```ts
const invoice = defineWorkflow(
  { name: "invoice", version: "invoice-v1" },
  async (ctx, input: { invoiceId: string }) => {
    const details = await ctx.step("load", async () => loadInvoice(input.invoiceId));
    return await ctx.step("send", async (s) => {
      await sendInvoice(details, { idempotencyKey: s.idempotencyKey });
      return { sent: true };
    });
  }
);
```

Versions are explicit deployment identifiers or build-supplied hashes, not a runtime guess from a git checkout. A registry maps the identifier to immutable code and determinism-relevant configuration. Running changed code under the same version is a deployment violation even when replay checks cannot detect it.

Top-level workflow code runs again from the start on a cold resume. Completed steps return stored values. Timers/helpers return journaled outcomes. Control flow reconstructs local state. Continuing in memory while retaining the same valid lease is ordinary execution, not a speculative cache optimization.

The workflow context owns durable commands. A separate `StepContext` owns attempt-scoped operations:

```ts
interface StepContext {
  readonly idempotencyKey: string; // logical step, stable across attempts
  readonly attempt: number;
  readonly attemptToken: string;
  readonly signal: AbortSignal;   // cooperative; not forced cancellation
  emit(text: string): Promise<void>; // Phase 2
  log(fields: Json): void;            // diagnostic, not replay state
}
```

Use `stepCtx.emit`, replacing ambiguous workflow-global `ctx.emit`. This makes parallel stream attribution explicit. `ctx.idempotencyKey` is likewise replaced by `stepCtx.idempotencyKey`. Durable commands MUST NOT be called from step callbacks. The SDK rejects nested steps, sleeps, helpers, and parallel groups in a callback. Detached callbacks and fire-and-forget work are unsupported.

### 4.2 Three different orderings

| Identifier | Meaning | Assigned by |
|---|---|---|
| `command_pos` | Position in deterministic top-level command history, starting at 0 | Server accepts SDK's expected next position under run lock |
| `first_seen_ordinal` | Logical step discovery order, starting at 0; parallel children take consecutive positions in array order | Server during command declaration |
| `run_events.seq` | Persistence order of all workflow events, starting at 1 | Server allocator; may reflect child completion timing |

These MUST NOT be conflated. A command can produce many events. A parallel command declares many steps. Heartbeats produce no replay command. Stream events have a fourth, separate sequence described in §8.

A step's memoization identity is `(run_id, step_key)`, never its position. Step keys are unique across the entire run, including every parallel group. Reaching the same key twice in one workflow evaluation is `DuplicateStepKeyError`; encountering that key at its recorded position on a new replay is expected. Maintain a fresh evaluation-local seen-key set and check it before memo lookup.

Loops/fan-out require stable discriminators, such as `fetch:${item.id}`. Naming alone does not make changed branches compatible; structural checking remains mandatory.

### 4.3 Command journal

`CommandRecorded` events store immutable descriptors. A `commands` projection accelerates replay. The journal contains commands, not arbitrary JavaScript operations:

```text
pos 0  step     { key: "plan", firstSeenOrdinal: 0 }
pos 1  parallel { key: "searches", children: ["search:a", "search:b"],
                  firstSeenOrdinals: [1, 2], concurrency: 2 }
pos 2  now      { key: "report-time", value: "2026-09-24T12:00:00.000Z" }
pos 3  sleep    { key: "cooldown", durationMs: 30000 }
pos 4  step     { key: "synthesize", firstSeenOrdinal: 3 }
```

For each command, the descriptor includes its kind, stable key, semantic arguments, explicit-input mode/hash if present, and resolved durable policy such as retry settings. Descriptor schemas are versioned. Comparison excludes server-assigned outcomes/timestamps, but includes arguments that alter behavior.

The SDK MUST await each durable top-level operation before issuing the next. Raw `Promise.all([ctx.step(...), ...])`, command races, overlapping unawaited commands, and nested workflow commands are rejected with `ConcurrentCommandError`; `ctx.step.parallel` is the only supported command-level fan-out.

At each position:

1. If a historical command exists, compare its descriptor before observing its result.
2. If it differs, halt with `NondeterminismError`, recording expected/actual descriptors and the position. Do not run the new callback.
3. If it matches, consume it and use its persisted state.
4. A new descriptor may append only at the recorded frontier, never before unconsumed history.
5. Before a workflow return or ordinary uncaught error becomes terminal, verify there are no unconsumed historical commands and no unresolved commands.

An engine suspension for sleep/retry or lease loss is not a workflow return. It must not report “missing commands” merely because execution paused. The SDK treats suspension/ownership errors as run-control state, not user-catchable business outcomes: even if application `catch`/`finally` observes an internal rejection, the evaluation is marked unusable and cannot append further commands or terminal output.

### 4.4 Determinism limits

Outside callbacks, disallow wall-clock reads, random generation, UUID generation, I/O, environment-dependent values, mutation shared between runs, and completion-order-dependent branching. Pass stable configuration in input or pin it with the version. Phase 2 linting catches common syntactic violations, not all indirect effects.

Changing `process.env.REGION` while calling the same `fetch-prices` key is **not necessarily detected**. Changing the command kind/order/key or a supplied fingerprint is detected. Reordering two identical unlabelled random calls could evade position-only checking; v3 therefore requires explicit helper keys (§7). Even labels do not prove semantic equivalence.

## 5. Step lifecycle and parallel semantics

### 5.1 Attempt protocol [Phase 1]

For a new step, declaration, step projection creation, and its first `StepStarted` may share one transaction. A callback MUST NOT start until the worker has received an acknowledged `StepStarted` with an attempt token while still owning the lease.

```text
declare/validate command
  -> commit StepStarted(attempt, attemptToken, fence)
  -> invoke callback outside database transaction
  -> validate/canonicalize result
  -> flush and acknowledge stream buffers [Phase 2]
  -> commit StepCompleted + projections + receipt [+ stream control]
  -> return canonical stored result to workflow
```

Each new callback invocation authorization increments the step's attempt, starting at 1. Retrying an HTTP request with the same operation ID does not create a new attempt or invoke the callback again. On ambiguous acknowledgement, resolve/retry the original request; never optimistically run another callback.

On takeover, a previously running attempt is marked `abandoned` with `StepAttemptAbandoned`; its step becomes `interrupted` until a fresh attempt is authorized. These changes can share the restart transaction. A completion that already committed wins and prevents restart. Old attempts cannot later complete. A callback exception commits `StepFailed`; Phase 1 marks the logical step exhausted immediately. On first execution and replay, exhausted steps throw the same structured error. User code may catch business failures and continue with new keys; an unhandled failure commits `RunFailed`.

Fatal runtime/protocol failures (including divergence, duplicate commands, invalid serialization, and recovery-budget exhaustion) invalidate the evaluation rather than becoming catchable business errors. Their `RunFailed` transaction abandons any still-running attempts, marks affected steps interrupted, and removes the lease/queue; callbacks are signalled to abort. This is the explicit exception to waiting for ordinary parallel business outcomes. Persisted successful/failed child outcomes remain intact. A stale worker without ownership cannot commit even this failure; it must stop and let current ownership recover.

Lease loss stops command submission and signals callbacks to abort. Graceful release revokes the ability to write before handing work to another worker; callbacks that ignore abort may still produce external effects.

### 5.2 `ctx.step.parallel` [Phase 1]

```ts
const results = await ctx.step.parallel(
  "searches",
  [
    { name: "search:a", run: async (s) => search("a", s.signal) },
    { name: "search:b", run: async (s) => search("b", s.signal) },
    { name: "search:c", run: async (s) => search("c", s.signal) }
  ],
  { concurrency: 2 }
);
```

The group key is explicit and unique among parallel groups in a run. Child step keys remain globally unique, not scoped by the group. No nested groups or arbitrary callback trees in v3.

**Declaration:** Snapshot and validate the complete ordered array, duplicate keys, explicit inputs, policies, and positive concurrency before running any child. Under one transaction, record the group command and all child declarations, assigning ordinals in array order. Do not partially register a group. Replay compares the entire ordered descriptor. An empty group records an empty command and returns `[]`.

**Scheduling:** Effective callback concurrency is at most the requested bound (default 4, server maximum 64) and worker capacity. Among runnable unstarted children, schedule in input order; actual finish order is unrestricted. Children MUST NOT coordinate through shared mutable state or depend on one another's start/finish order. Ordinary provider concurrency inside a callback remains the application's responsibility.

**Persistence:** Each child's attempt start and terminal outcome is committed independently. Events from concurrent children serialize through the run lock. Do not wait to commit successful children until all siblings succeed. Results return in input-array order, never completion order.

**Failure policy:** Settle all children. A business failure does not cancel siblings, prevent later siblings from starting, or reject the group early. In Phase 2, eligible retries are resolved before the child is considered exhausted. When every child is completed or exhausted, return the ordered result tuple if all succeeded; otherwise throw `ParallelStepError` with failures and successful results ordered by child index. Its error shape and ordering are stable on replay. Application code may catch it. There is no implicit rollback or compensation.

**Crash example:** A completes, B is still running, C has a durable failure, then the worker dies:

- A returns its memoized value; its callback does not run again.
- B's unfinished attempt is abandoned; B starts a new attempt.
- In Phase 1, C remains exhausted and its callback does not rerun. In Phase 2, C retries only if its persisted policy/budget permits it.
- Declared children never started before the crash remain eligible to start.
- The group settles after these outcomes are resolved and reports any exhausted failures in input order.

A new lease does not reset retry counts or replace a persisted failure with success. If all children completed but the group result was never delivered, replay derives the same ordered result without invoking children. No separate group completion event is required: group outcome is a deterministic fold of terminal child states.

When remaining children are all waiting for retry deadlines and no callbacks are active, atomically schedule the run for the earliest deadline and relinquish the lease. Do not schedule/release while a sibling callback is still running. Independent siblings continue while one child's retry is delayed.

## 6. Storage, locking, and atomic transitions [Phase 1 baseline]

### 6.1 Truth versus coordination state

`run_events` is the append-only source of workflow history: creation input/version, command declarations, attempt outcomes, timer outcomes, and terminal state. `commands`, `steps`, `step_attempts`, and the business fields of `runs` are rebuildable projections.

`runs.current_fence`, allocator counters, live lease deadlines, queue availability, request receipts, and retention markers are coordination state. Do not claim that every field in these tables is merely a replay projection. Rebuilding a timeline must never resurrect a live lease, lower a fence, rewind a sequence counter, or delete a deduplication receipt.

Stream content is authoritative in `run_stream_events`, under its separate retention policy. Workflow replay never requires that content: full step results live in workflow history.

### 6.2 Locking rule and sequence allocators

Every transaction touching an existing run MUST lock its `runs` row first, then its queue row if needed, then step/attempt rows. Code that touches multiple runs locks run IDs in a stable order, or processes one run per transaction. Discovery queries may read without locks but MUST recheck predicates after acquiring locks.

Use PostgreSQL READ COMMITTED for transition transactions with these explicit locks; retry deadlocks or transient transaction failures using the original request identity. No lock is held while user code runs. A queue-discovery query selects one candidate ordered by priority descending, then availability and run ID; starvation is possible until Phase 4 fairness is explicitly implemented.

`runs.next_event_seq` and `runs.next_stream_seq` begin at 1. While holding the run lock, allocate an exact range and insert those rows in the same transaction. Rollback rolls back the counter. Neither log uses `MAX(seq)+1`, client-supplied sequences, or a cluster-global sequence for this purpose.

```sql
-- Within a transition transaction, after locking/validating the run:
UPDATE runs
SET next_event_seq = next_event_seq + :event_count
WHERE id = :run_id
RETURNING next_event_seq - :event_count AS first_allocated_seq;
```

Allocate only for new rows, after checking request receipts. Each committed per-run workflow sequence is contiguous while unarchived; stream sequences are contiguous before retention. There is no meaningful cross-log ordering by comparing their numbers. Relationships use command, attempt, and explicit stream-watermark fields.

`next_command_pos` and `next_step_ordinal` advance under the same lock when declarations commit. New commands must match the expected position. Concurrent child completion does not modify discovery order.

### 6.3 Leases, fencing, and database time

The server grants a lease only for a compatible workflow version and runnable run. Candidate selection joins queue to runs and uses `FOR UPDATE OF r SKIP LOCKED` on the run row, then locks/rechecks the queue. All other transitions follow the same run-before-queue order. `SKIP LOCKED` is appropriate for avoiding contention in queue-like access; it does not provide general fairness. [PostgreSQL SELECT](https://www.postgresql.org/docs/current/sql-select.html)

On each grant, increment `runs.current_fence`, set the worker session owner and expiry, append `LeaseGranted`, and set the run running in one transaction. Keep the fence on `runs` so deleting/recreating a queue entry cannot reset its lifetime epoch. Initial fence is 0; first grant is 1.

Every worker mutation checks all of:

```text
run is nonterminal
queue.state == leased
queue.lease_owner == request.workerSessionId
runs.current_fence == request.fence       (exact equality)
queue.lease_expires_at > database_now
attempt identity/state/fence match, when an attempt is involved
```

Acquire locks first, then capture `clock_timestamp()` once for that transition's ownership decision. Use that captured Postgres value for expiry comparison, renewal, due checks, and new deadline calculation. `now()` is fixed at transaction start; a request that waited for a lock must not validate ownership using a stale transaction-start timestamp. [PostgreSQL time functions](https://www.postgresql.org/docs/current/functions-datetime.html)

The linearization point is the ownership check under the run lock. No takeover can interleave before that transaction completes. Transactions MUST be short, bounded by lock/statement/idle-in-transaction timeouts, with no provider or network calls inside them. Worker clocks may schedule heartbeat attempts but cannot authorize ownership. A late heartbeat cannot revive an expired lease, even if no reaper has processed it.

Default lease duration: 30 seconds; heartbeat target: every 10 seconds. Configuration is validated and returned to workers. Reaper transactions recheck database time, append `LeaseExpired`, clear ownership, and make the run ready. Explicit release similarly appends `LeaseReleased`. Granting a new lease always advances the fence. System transitions use their own restricted database functions and current fence, not a forged worker identity.

### 6.4 Transition transactions

Run/queue mappings are enforced by transition functions: `pending -> ready`, `running -> leased`, `sleeping or retrying -> scheduled`, `waiting -> parked` [P4], and terminal run -> no queue row. On grant, `available_at` becomes NULL; on scheduling/readiness, it becomes the recorded due time/database now. A terminal run never returns to a runnable state. Explicit redrive creates a different run ID.

| Transition | MUST commit atomically |
|---|---|
| Create run | Immutable input/version, `RunCreated`, initial run projection, ready queue row, trigger deduplication identity |
| Grant lease | Fence increment, owner/expiry, `LeaseGranted`, running state |
| Declare command/group | `CommandRecorded`, command/step projections, discovery counters, receipt |
| Start/restart attempt | Abandon old unfinished attempt if applicable, `StepStarted`, attempt/step projections, receipt; stream supersession control in Phase 2 |
| Complete step | `StepCompleted` and result, step/attempt projections, receipt; validated final stream watermark and stream completion control in Phase 2 |
| Fail step | `StepFailed`, normalized error, exhausted/retry state, retry deadline if any, receipt; stream failure control in Phase 2 |
| Suspend run | `RunSuspended`, run state, scheduled queue deadline or parked wait, ownership removal, receipt |
| Fire timer | `TimerFired`, command outcome, pending run state, ready queue row |
| Finish run | `RunCompleted`/`RunFailed`, output/error and terminal timestamp, queue deletion, receipt; run-terminal stream control in Phase 2 |
| Expire/release lease | Ownership removal, event, pending state and ready queue availability |
| Cancel/signal [P4] | Authorized inbox/cancellation event, affected projections, wakeup/queue changes and stream controls as applicable |

Heartbeat renewal is an operational update, not a workflow command and not a mandatory history event. It still follows ownership checks and lock order. A failed child does not mark the run failed until workflow error handling is resolved. A retrying child does not suspend a run while sibling callbacks remain active.

### 6.5 Request idempotency and uncertain commits

Worker transition requests carry `operationId` (UUID), protocol version, worker session, run ID, fence, and typed intent. The server maps allowed intents to events; workers cannot append arbitrary event types, sequences, timestamps, or system transitions.

Under the run lock, look up `(run_id, operation_id)`. An identical validated request returns its stored response without new writes. A different request body with that ID returns `409 IdempotencyConflict`. Store the hash of the canonical request and the response in the same transaction as its effects. Receipts remain at least as long as workflow history. Structural uniqueness constraints additionally protect logical transitions against new operation IDs.

An authenticated exact duplicate may receive its historical receipt after ownership has changed. That response acknowledges an old commit, **not current ownership**. No new state change is allowed, and the SDK must not resume user code without a current valid lease. Registration and lease-acquisition request deduplication use separate worker-session receipts; a replayed lease response must be revalidated before use.

Trigger idempotency is `(tenant_id, Idempotency-Key)`, not tenant plus workflow. Reuse with the same canonical input and workflow-version ID returns the same run; differing intent returns 409. Concurrency is resolved by the database unique constraint. Requests without a key create distinct runs.

### 6.6 Database enforcement and recovery tooling

Application roles MUST NOT have direct mutation privileges on history, projection, queue, or allocator tables. Expose reviewed transition functions that perform validation, fence checks, inserts, and updates atomically; migrations have separate privileges. Do not mistake a `CHECK` constraint for enforcement of cross-table lifecycle rules. Function ownership, `search_path`, and grants must be explicit.

`relay projections verify <run>` [P1] folds events without executing user code and compares business projections. `relay projections rebuild <run>` [P3] requires a quiesced run/maintenance lock, repairs only supported projections, verifies the result, and records operator diagnostics. It is recovery tooling, never a repair loop needed for normal correctness. Do not use this command to rewrite history or “fix” a nondeterministic workflow.

## 7. Helpers, timers, and retries [Phase 2]

### 7.1 Journaled helpers

```ts
const at = await ctx.now("report-time");     // UTC ISO string, millisecond precision
const sample = await ctx.random("sample");  // finite number in [0, 1)
const correlationId = await ctx.uuid("correlation-id"); // UUID v4 string
```

All three are async and require a stable explicit key. Their identities are `(run_id, command_pos, kind, key)`; helper keys are unique within each helper kind per evaluation. On first use the server generates a value and commits it in `CommandRecorded` before returning it. `now` uses database time; random/UUID values are generated server-side using standard runtime primitives and recorded, not regenerated on replay. `random` is a workflow sampling utility, not a cryptographic API.

If the reply is lost, replay/retry returns the stored value. If the transaction rolled back, generating a new value is safe because the workflow never observed an acknowledged value. Calling the same kind with a different key at an existing position is divergence. Helpers are prohibited in step callbacks; nondeterministic work inside a callback is represented by its recorded outcome.

### 7.2 Durable timers

`await ctx.sleep("cooldown", 30_000)` takes a nonnegative safe integer duration in milliseconds. String-duration parsing is not part of the v3 core API. Record duration and absolute database-derived wakeup time once, append `TimerScheduled`, set run sleeping, schedule its queue row, and remove ownership in one transaction. The workflow yields without holding a worker slot. Zero duration completes immediately through recorded timer events.

The scheduler locks the run, checks database time, records `TimerFired` once, marks the command complete, and makes the run ready. Replay returns from a fired timer without rescheduling. The deadline is never recomputed from a worker clock. Timer waits are top-level commands, not permitted inside parallel step callbacks.

### 7.3 Durable retry policy

Phase 1 has no automatic retry for a committed callback failure. Recovery of an unfinished attempt is separate and already required. Phase 2 adds explicit per-step policy, persisted on declaration:

```ts
type RetryPolicy = {
  maxAttempts: number;   // >= 1; includes first invocation and abandoned attempts
  initialDelayMs: number;
  maxDelayMs: number;
  multiplier: number;    // >= 1
  jitter: "none" | "full";
  retryableCodes: string[];
};
```

Default `maxAttempts = 1` for committed business failures; workflow authors opt in. Infrastructure recovery has a separately configured ceiling (default 100 abandoned attempts per logical step in Phase 1); reaching it terminally fails the run with `RecoveryBudgetExceeded` instead of authorizing another attempt. In Phase 2, an explicit `maxAttempts` bounds all invocations, including abandoned ones; lack of explicit retry policy preserves the Phase 1 recovery ceiling and no business retries. Transport retries do not consume an invocation budget.

If interruption consumes the last permitted invocation, recovery records the abandonment and terminally fails the run with `RecoveryBudgetExceeded`; it does not fabricate a callback failure or execute beyond the budget. This is a fatal runtime outcome, whereas a committed callback failure at its budget limit becomes an ordinary exhausted step error. All budget decisions are persisted with the relevant transition, not re-derived from current server defaults.

For a retryable failure after invocation number `a`, compute `cap = min(maxDelayMs, initialDelayMs * multiplier^(a-1))` with overflow-safe arithmetic. Full jitter chooses an integer delay in `[0, cap]`. Persist the chosen delay and database-derived `retry_at` in the failure event and projection in the same commit. Replays and duplicate requests must not draw new jitter or reset the deadline. Expired in-flight attempts are classified as interrupted recovery, not fabricated provider errors.

Until the durable retry budget is exhausted, workflow code does not receive the transient callback error. Exhaustion yields `StepExecutionError`. An unhandled exhausted error marks the run failed and visible in the DLQ view. A handled step failure in a completed run is not a DLQ run. DLQ is initially a query over failed runs, not another queue. Redrive creates a new run and links the original; it never erases history or silently changes provider idempotency semantics.

## 8. Streaming protocol [Phase 2]

### 8.1 A separate log with one complete client cursor

`run_stream_events` contains **both** buffered chunks and client-visible control events. Its `seq` is the sole SSE cursor. No client joins `run_events.seq` to it or compares those numbers.

Kinds: `chunk`, `superseded`, `step_completed`, `step_failed`, and `run_terminal`. Audit events may reference stream controls, but control delivery does not depend on the workflow log. Stream sequences are per run and allocated through §6.2.

### 8.2 Batching, backpressure, and idempotency

`await stepCtx.emit(text)` is allowed only during an active step callback. The SDK serializes calls for that attempt. It accumulates Unicode text and flushes at roughly 100 ms, at 4 KiB UTF-8, or when the callback settles. Oversized emissions are split at valid Unicode boundaries. Empty emissions do nothing.

The implementation permits at most one unacknowledged buffer per active attempt: a flush freezes that buffer, and subsequent emit calls block until it is durably acknowledged. It must not create an unbounded second queue behind an in-flight request. Backpressure extends to the provider iterator; detached ignored `emit` promises violate the SDK contract. `emit` may return before its current small buffer is durable, but final step settlement always joins outstanding emits and flushes.

Consequently a worker crash can lose at most one **uncommitted buffer per active attempt**, not one buffer for the whole run and not a hard 100 ms of output. Network stalls and many parallel steps invalidate the stronger claim. Browser disconnection alone loses no retained committed chunks.

Each persisted chunk row represents one batch with identity:

```text
(run_id, step_key, attempt, batch_seq)    batch_seq starts at 0
```

The SDK retains the exact payload for retries. A request includes attempt token, fence, batch sequence, and text. Server logic under the run lock:

1. Check for an existing identity. Identical payload returns the original stream sequence. Different payload returns `409 StreamBatchConflict`.
2. For a new batch, check the live lease and active attempt. Require `batch_seq = last_batch_seq + 1`; reject gaps/out-of-order requests. Initial `last_batch_seq = -1`.
3. Allocate one stream sequence, insert the batch, and update `last_batch_seq` atomically.
4. Acknowledge only after commit. Publish wakeups after/with commit, never publish uncommitted chunks.

As with transition receipts, an exact duplicate can acknowledge an old commit after the attempt ends; it cannot authorize new output. Database uniqueness enforces deduplication even if an HTTP response is dropped.

### 8.3 Flush before completion

The SDK closes emission for the attempt, waits for emit calls, flushes its last buffer, and receives durable acknowledgements before requesting `StepCompleted`. The request carries `finalBatchSeq` (`-1` if none).

The completion transaction requires the attempt's contiguous persisted `last_batch_seq` to equal `finalBatchSeq`, marks the attempt completed (thereby sealing emission), appends `StepCompleted` with the result and final stream watermark, updates projections, and appends a `step_completed` stream event. These changes and the receipt commit together. No later new batch for that attempt is accepted.

The SDK invariant prevents claiming completion while retaining unseen local data; the server can validate only the declared manifest and committed rows. Treat SDK conformance as part of the protocol tests. A forged early final watermark cannot be discovered by database magic.

On ordinary callback failure, close/join/flush emitted output before recording `StepFailed` and a `step_failed` control. If durable flush cannot complete before lease loss, do not commit the failure from that worker; recovery treats the attempt as interrupted. A process crash can leave only its committed prefix.

### 8.4 Supersession with parallel output

Before a new attempt starts, its transaction appends a `superseded` stream control for the prior incomplete/failed attempt, then authorizes the new attempt. The control commits before any new-attempt chunk can commit:

```json
{
  "stepKey": "search:a",
  "oldAttempt": 1,
  "newAttempt": 2,
  "reason": "attempt_restarted"
}
```

Clients partition their rendered output by `(step_key, attempt)`. A supersession invalidates **only that old attempt's output**. Do not truncate the global stream at an earlier sequence: chunks from other parallel steps can be interleaved and remain valid. Apply a tombstone even if the client has not seen any old chunks. Rendering may group by logical step order; transport arrival order is not the workflow's result order.

Failed output remains visibly marked failed until superseded. Completed attempts are never superseded. A run-terminal control marks unfinished output incomplete/cancelled as applicable; it does not make partial output a successful step result.

### 8.5 SSE resumption and notification races

```http
GET /v1/runs/6a.../stream
Last-Event-ID: 101
```

```text
id: 102
event: superseded
data: {"stepKey":"search:a","oldAttempt":1,"newAttempt":2,"reason":"attempt_restarted"}

id: 103
event: chunk
data: {"stepKey":"search:a","attempt":2,"batchSeq":"0","text":"New result"}

```

The header is a validated nonnegative decimal cursor; absent means 0. An optional `after` query supports a client reload; reject conflicting header/query values. Reject a cursor beyond the current high watermark with 409. Query retained rows using `seq > cursor ORDER BY seq`; send the sequence as SSE `id`. SSE supports `Last-Event-ID` for reconnection, but application state still needs an idempotent reducer. [HTML SSE standard](https://html.spec.whatwg.org/multipage/server-sent-events.html)

Establish and commit a small shared pool of database LISTEN connections before catch-up reads. Register the local subscriber before reading; then query durable rows, drain through the watermark, and treat notifications solely as wakeups to query again. Coalesce wakeups without losing a dirty flag during draining. Periodic catch-up polling and immediate reads after listener reconnection cover missed notifications. LISTEN does not become active until transaction commit. [PostgreSQL LISTEN](https://www.postgresql.org/docs/current/sql-listen.html)

Never use one Postgres listener per browser. Slow browsers get bounded queues and are disconnected to resume from their applied cursor; they must not stall database writes. SSE keepalive comments have no event ID. Client reducers discard already-applied IDs and apply chunk/control events in order. A reload without a matching saved view must reconstruct from retained history rather than send only an old cursor and display an empty view.

Default stream retention is seven days after run termination; active-run streams are not expired. A retention transaction locks the run, deletes all its stream rows, and sets a durable expired marker. A concurrent SSE read uses a coherent snapshot of marker and rows; subsequent drain detects expiration. Expired streams return `410 StreamExpired`, including terminal run metadata; SDKs stop automatic reconnect and offer the stored final result through the status API. Do not silently resume from a missing prefix. Workflow events/results and stream-batch receipts must have compatible retention rules; after stream expiration, requests for new batches are rejected and no stream IDs are reused.

## 9. Relational schema and event contracts

### 9.1 Schema example

This is a coherent **Phase 1 + Phase 2 target DDL example**, not a claim that migrations already exist. Implement Phase 1 tables/fields first and add Phase 2 fields/kinds through explicit migrations. Phase 4 states/events shown below are reserved vocabulary, rejected by earlier protocol validators; no Phase 4 endpoints or tables are authorized by their appearance here. Migration review must also implement the transition functions and role grants in §6.6; these table definitions alone do not enforce the protocol.

```sql
CREATE TABLE tenants (
  id uuid PRIMARY KEY,
  name text NOT NULL CHECK (length(name) > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE workflow_versions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  name text NOT NULL CHECK (length(name) > 0),
  version text NOT NULL CHECK (length(version) > 0),
  protocol_version integer NOT NULL CHECK (protocol_version > 0),
  registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (tenant_id, name, version),
  UNIQUE (tenant_id, id)
);

CREATE TABLE runs (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  workflow_version_id uuid NOT NULL,
  state text NOT NULL CHECK (state IN
    ('pending','running','sleeping','retrying','waiting',
     'completed','failed','cancelled')),
  input jsonb NOT NULL,
  output jsonb,
  error jsonb,
  idempotency_key text CHECK
    (idempotency_key IS NULL OR octet_length(idempotency_key) BETWEEN 1 AND 256),
  trigger_hash text NOT NULL CHECK (trigger_hash ~ '^[0-9a-f]{64}$'),
  current_fence bigint NOT NULL DEFAULT 0 CHECK (current_fence >= 0),
  next_event_seq bigint NOT NULL DEFAULT 1 CHECK (next_event_seq >= 1),
  next_command_pos bigint NOT NULL DEFAULT 0 CHECK (next_command_pos >= 0),
  next_step_ordinal bigint NOT NULL DEFAULT 0 CHECK (next_step_ordinal >= 0),
  next_stream_seq bigint NOT NULL DEFAULT 1 CHECK (next_stream_seq >= 1), -- P2
  stream_expired_at timestamptz, -- P2
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz,
  FOREIGN KEY (tenant_id, workflow_version_id)
    REFERENCES workflow_versions(tenant_id, id),
  UNIQUE (tenant_id, idempotency_key),
  CHECK ((state IN ('completed','failed','cancelled')) =
         (completed_at IS NOT NULL)),
  CHECK ((state = 'completed') = (output IS NOT NULL)),
  CHECK ((state = 'failed') = (error IS NOT NULL)),
  CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),
  CHECK (stream_expired_at IS NULL OR completed_at IS NOT NULL)
);

CREATE TABLE run_events (
  run_id uuid NOT NULL REFERENCES runs(id),
  seq bigint NOT NULL CHECK (seq >= 1),
  fence bigint NOT NULL CHECK (fence >= 0),
  actor text NOT NULL CHECK (actor IN ('worker','system','client')),
  type text NOT NULL CHECK (type IN (
    'RunCreated','LeaseGranted','LeaseReleased','LeaseExpired',
    'CommandRecorded','StepStarted','StepCompleted','StepFailed',
    'StepAttemptAbandoned','RunSuspended',
    'TimerScheduled','TimerFired','SignalReceived','WaitResolved',
    'RunCompleted','RunFailed','RunCancelled'
  )),
  command_pos bigint CHECK (command_pos >= 0),
  step_key text CHECK (octet_length(step_key) BETWEEN 1 AND 256),
  attempt integer CHECK (attempt >= 1),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, seq),
  CHECK ((attempt IS NULL) OR (step_key IS NOT NULL)),
  CHECK (type NOT IN ('CommandRecorded','TimerScheduled','TimerFired','WaitResolved')
    OR command_pos IS NOT NULL),
  CHECK (type NOT IN ('StepStarted','StepCompleted','StepFailed','StepAttemptAbandoned')
    OR (command_pos IS NOT NULL AND step_key IS NOT NULL AND attempt IS NOT NULL)),
  CHECK (type NOT IN ('RunCreated','RunCompleted','RunFailed','RunCancelled')
    OR (step_key IS NULL AND attempt IS NULL))
);

CREATE UNIQUE INDEX run_one_terminal_event ON run_events(run_id)
  WHERE type IN ('RunCompleted','RunFailed','RunCancelled');
CREATE UNIQUE INDEX run_one_creation ON run_events(run_id)
  WHERE type = 'RunCreated';
CREATE UNIQUE INDEX run_one_command_event ON run_events(run_id, command_pos)
  WHERE type = 'CommandRecorded';
CREATE UNIQUE INDEX step_one_completion ON run_events(run_id, step_key)
  WHERE type = 'StepCompleted';
CREATE UNIQUE INDEX attempt_one_start ON run_events(run_id, step_key, attempt)
  WHERE type = 'StepStarted';
CREATE UNIQUE INDEX attempt_one_terminal ON run_events(run_id, step_key, attempt)
  WHERE type IN ('StepCompleted','StepFailed','StepAttemptAbandoned');

CREATE TABLE commands (
  run_id uuid NOT NULL REFERENCES runs(id),
  command_pos bigint NOT NULL CHECK (command_pos >= 0),
  kind text NOT NULL CHECK (kind IN
    ('step','parallel','now','random','uuid','sleep','wait')),
  command_key text NOT NULL CHECK (octet_length(command_key) BETWEEN 1 AND 256),
  descriptor jsonb NOT NULL CHECK (jsonb_typeof(descriptor) = 'object'),
  outcome jsonb, -- recorded helper value / timer or wait result; SQL NULL = absent
  recorded_event_seq bigint NOT NULL,
  PRIMARY KEY (run_id, command_pos),
  UNIQUE (run_id, kind, command_key),
  FOREIGN KEY (run_id, recorded_event_seq) REFERENCES run_events(run_id, seq)
);

CREATE TABLE steps (
  run_id uuid NOT NULL REFERENCES runs(id),
  step_key text NOT NULL CHECK (octet_length(step_key) BETWEEN 1 AND 256),
  command_pos bigint NOT NULL,
  child_index integer NOT NULL DEFAULT 0 CHECK (child_index >= 0),
  first_seen_ordinal bigint NOT NULL CHECK (first_seen_ordinal >= 0),
  state text NOT NULL CHECK (state IN
    ('declared','running','interrupted','retry_wait','completed','exhausted')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  input_present boolean NOT NULL DEFAULT false, -- P2 fingerprinting
  input jsonb,
  input_hash text CHECK (input_hash ~ '^[0-9a-f]{64}$'),
  retry_policy jsonb NOT NULL CHECK (jsonb_typeof(retry_policy) = 'object'),
  retry_at timestamptz,
  output jsonb,
  error jsonb,
  PRIMARY KEY (run_id, step_key),
  UNIQUE (run_id, first_seen_ordinal),
  UNIQUE (run_id, command_pos, child_index),
  FOREIGN KEY (run_id, command_pos) REFERENCES commands(run_id, command_pos),
  CHECK ((input_present AND input IS NOT NULL AND input_hash IS NOT NULL)
    OR (NOT input_present AND input IS NULL AND input_hash IS NULL)),
  CHECK ((state = 'completed') = (output IS NOT NULL)),
  CHECK ((state = 'retry_wait') = (retry_at IS NOT NULL)),
  CHECK ((state IN ('retry_wait','exhausted')) = (error IS NOT NULL)),
  CHECK (error IS NULL OR jsonb_typeof(error) = 'object'),
  CHECK ((state = 'declared' AND attempt = 0)
    OR (state <> 'declared' AND attempt >= 1))
);

CREATE TABLE step_attempts (
  run_id uuid NOT NULL,
  step_key text NOT NULL,
  attempt integer NOT NULL CHECK (attempt >= 1),
  attempt_token uuid NOT NULL UNIQUE,
  fence bigint NOT NULL CHECK (fence >= 1),
  state text NOT NULL CHECK (state IN
    ('running','completed','failed','abandoned')),
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  last_batch_seq bigint NOT NULL DEFAULT -1 CHECK (last_batch_seq >= -1), -- P2
  final_batch_seq bigint CHECK (final_batch_seq >= -1), -- P2
  PRIMARY KEY (run_id, step_key, attempt),
  FOREIGN KEY (run_id, step_key) REFERENCES steps(run_id, step_key),
  CHECK ((state = 'running') = (ended_at IS NULL)),
  CHECK ((state IN ('completed','failed')) = (final_batch_seq IS NOT NULL)),
  CHECK (final_batch_seq IS NULL OR final_batch_seq = last_batch_seq)
);

CREATE TABLE run_queue (
  run_id uuid PRIMARY KEY REFERENCES runs(id),
  state text NOT NULL CHECK (state IN ('ready','leased','scheduled','parked')),
  priority integer NOT NULL DEFAULT 0,
  available_at timestamptz,
  lease_owner uuid, -- worker session ID, not a reusable process label
  lease_expires_at timestamptz,
  CHECK (
    (state = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (state <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK ((state IN ('ready','scheduled')) = (available_at IS NOT NULL))
);
CREATE INDEX queue_ready ON run_queue(priority DESC, available_at, run_id)
  WHERE state = 'ready';
CREATE INDEX queue_due ON run_queue(available_at, run_id)
  WHERE state = 'scheduled';
CREATE INDEX queue_expired ON run_queue(lease_expires_at, run_id)
  WHERE state = 'leased';
CREATE INDEX runs_workflow ON runs(workflow_version_id, state, id);

CREATE TABLE operation_receipts (
  run_id uuid NOT NULL REFERENCES runs(id),
  operation_id uuid NOT NULL,
  request_hash text NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  response jsonb NOT NULL CHECK (jsonb_typeof(response) = 'object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, operation_id)
);

-- Entire table is Phase 2. Control events and batches share one cursor.
CREATE TABLE run_stream_events (
  run_id uuid NOT NULL REFERENCES runs(id),
  seq bigint NOT NULL CHECK (seq >= 1),
  kind text NOT NULL CHECK (kind IN
    ('chunk','superseded','step_completed','step_failed','run_terminal')),
  step_key text,
  attempt integer CHECK (attempt >= 1),
  batch_seq bigint CHECK (batch_seq >= 0),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  payload_hash text CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, seq),
  FOREIGN KEY (run_id, step_key, attempt)
    REFERENCES step_attempts(run_id, step_key, attempt),
  CHECK (
    (kind = 'run_terminal' AND step_key IS NULL AND attempt IS NULL)
    OR
    (kind <> 'run_terminal' AND step_key IS NOT NULL AND attempt IS NOT NULL)
  ),
  CHECK ((kind = 'chunk') = (batch_seq IS NOT NULL)),
  CHECK ((kind = 'chunk') = (payload_hash IS NOT NULL))
);
CREATE UNIQUE INDEX stream_batch_identity
  ON run_stream_events(run_id, step_key, attempt, batch_seq)
  WHERE kind = 'chunk';
CREATE UNIQUE INDEX stream_attempt_control
  ON run_stream_events(run_id, step_key, attempt, kind)
  WHERE kind IN ('superseded','step_completed','step_failed');
CREATE UNIQUE INDEX stream_run_terminal ON run_stream_events(run_id)
  WHERE kind = 'run_terminal';
```

For supersession rows, `attempt` identifies the **old** attempt; `payload.newAttempt` identifies the successor. A failed attempt can have both a failure control and a later supersession control. A completed attempt cannot have supersession; enforce that cross-row rule in the transition function.

SQL NULL means absence; JSON `null` is a valid present result (`'null'::jsonb`). Do not conflate them in drivers or checks. `CHECK` constraints on nullable expressions can pass UNKNOWN, so the type-specific checks explicitly require non-NULL event fields. Helpers use `CommandRecorded`, whose position is required. Payload schemas are validated at both worker/public boundaries and transition-function boundaries.

Worker sessions, capabilities, and session-level lease receipts are operational tables omitted from this focused DDL example. Their migration MUST include immutable session IDs, server-issued expiry, unique version memberships referencing `workflow_versions`, protocol capabilities, and unique request IDs. Timer due/outcome fields may use typed command-projection columns; do not rely on an unindexed JSON scan for scheduled work. Phase 4 requires separate inbox/wait tables and authorization migrations before activation.

### 9.2 No duplicated queue workflow authority

The queue stores **only `run_id` for workflow identity**. Derive tenant and workflow-version ID through `runs`, and name/version through `workflow_versions`. There is no independent queue workflow name, tenant, version string, or copied fence. The composite foreign key prevents a run from referencing another tenant's workflow version.

If measurements later justify queue denormalization, add an ADR, query plans, and a database-enforced relationship. “The application always copies it correctly” is insufficient.

### 9.3 Event payload examples

Events carry a payload schema version, and enough information to rebuild business projections without calling providers:

```json
{
  "type": "StepStarted",
  "stepKey": "search:a",
  "commandPos": "1",
  "attempt": 2,
  "payload": {
    "schemaVersion": 1,
    "attemptToken": "b90c7629-2dd5-40a0-b282-bd5cc8e5407b",
    "firstSeenOrdinal": "1",
    "reason": "recovery"
  }
}
```

```json
{
  "type": "StepCompleted",
  "stepKey": "search:a",
  "commandPos": "1",
  "attempt": 2,
  "payload": {
    "schemaVersion": 1,
    "result": { "url": "https://example.org/result" },
    "finalBatchSeq": "3",
    "lastChunkStreamSeq": "28",
    "completionStreamSeq": "29"
  }
}
```

A non-streaming step uses `finalBatchSeq: "-1"` and `lastChunkStreamSeq: null` in Phase 2. `RunCreated` includes tenant, pinned workflow identity, input, trigger hash/key, creation time, and resolved run policies. `CommandRecorded` includes the full descriptor, child keys/ordinals, explicit inputs and policies, and any helper outcome. `StepFailed` includes normalized error, exhaustion decision, resolved retry delay/deadline, and final batch watermark. `StepAttemptAbandoned` records the reason and old ownership. Run terminal events include final output/error, terminal timestamp, and consumed command count.

History event schemas and protocol schemas are independently versioned. Additive optional fields require reader compatibility tests. Reinterpreting a historical command or rewriting old payloads in place is prohibited; use versioned readers or explicit offline migration design.

## 10. API contracts and phase gates

### 10.1 SDK surface

| API | Phase | Contract |
|---|---|---|
| `defineWorkflow({name, version}, fn)` | 1 | Explicit immutable version identity |
| Worker start/register/stop | 1 | Version capabilities, bounded run concurrency, graceful release |
| `ctx.step(name, fn)` | 1 | Unique name, strict JSON result, attempt-scoped context |
| `ctx.step.parallel(group, children, {concurrency})` | 1 | Ordered declaration, bounded callbacks, settle-all semantics |
| `ctx.log(fields)` / `stepCtx.log(fields)` | 1 | Diagnostics; top-level logs may repeat on replay and are tagged `replay` |
| `client.trigger(workflow, input, {idempotencyKey})` | 1 | Returns stable run ID on an identical retry |
| `client.getRun(runId)` | 1 | Status, pinned version, output/error, event pagination |
| `ctx.step(name, {input, retry}, fn)` | 2 | Explicit-input fingerprinting and persisted retry policy |
| Parallel child `input` / `retry` options | 2 | Same validation and policy semantics as ordinary steps |
| `stepCtx.emit(text)` | 2 | Awaitable, scoped to one active attempt, bounded buffer |
| `client.subscribe(runId, {after})` | 2 | Ordered chunk/control reducer and reconnect |
| `ctx.sleep(name, durationMs)` | 2 | Durable timer, no worker slot during suspension |
| `ctx.now(key)`, `ctx.random(key)`, `ctx.uuid(key)` | 2 | Async, named, journaled values |
| Inspection/read API and projection rebuild | 3 | Used by dashboard and diagnostics |
| Optional explicit metadata | 3, opt-in extension | See §11; no inferred provider cost |
| `ctx.waitForEvent(name, {timeoutMs})` | 4, reserved | Requires accepted inbox/wait ADR before implementation |
| `client.signal(runId, name, payload, {idempotencyKey})` | 4, reserved | Durable deduplicated inbox |
| `client.cancel(runId)` | 4, reserved | Durable cancellation; cooperative provider abort |

Do not ship future-phase stubs that imply support. SDK/server capability negotiation rejects unsupported features before user code executes. Reserved does not mean implemented.

The Phase 2 options overload also permits `{retry}` without `input`; its callback receives only `StepContext`, and `input_present` stays false. Explicit input uses `{input, retry?}` and passes the normalized input as the second callback argument. Parallel child descriptors use the same optional fields. Retry policy changes are checked through their resolved command descriptor regardless of overload syntax.

### 10.2 Public HTTP API

| Method and path | Phase | Behavior |
|---|---|---|
| `POST /v1/runs` | 1 | Input plus explicit workflow name/version; optional `Idempotency-Key` |
| `GET /v1/runs/:id` | 1 | Pinned version, state, output/error, active ownership summary |
| `GET /v1/runs/:id/events?after=&limit=` | 1 | Paginated immutable history; workflow-event cursor only |
| `GET /v1/workflows` | 1 | Registered versions and compatible-worker availability |
| `GET /v1/runs/:id/stream` | 2 | SSE using stream cursor only |
| `GET /v1/runs?state=&workflow=&version=&cursor=` | 3 | Inspection/filtering, including failed-run/DLQ view |
| `GET /v1/runs/:id/steps` | 3 | Step and attempt inspection |
| `GET /v1/metrics` | 3 | Prometheus-format operational metrics |
| `POST /v1/runs/:id/signal` | 4 | Authorized deduplicated signal ingress |
| `POST /v1/runs/:id/cancel` | 4 | Authorized durable cancellation |

Creation returns 201 for a new run and 200 for an identical idempotency replay. All requests use runtime validation. Pagination cursors are opaque or validated decimal strings, never raw SQL fragments. Phase 1 history APIs are for trusted operators; later authorization applies consistently to reads, SSE, and writes.

### 10.3 Worker HTTP protocol

| Method and path | Phase | Behavior |
|---|---|---|
| `POST /worker/v1/register` | 1 | Register worker session, workflow versions, protocol/capabilities |
| `POST /worker/v1/lease` | 1 | Acquire compatible run; return fence, lease deadline, immutable history snapshot watermark |
| `POST /worker/v1/heartbeat` | 1 | Renew exact active ownership; 409 on lost/expired lease |
| `GET /worker/v1/runs/:id/history?after=&through=` | 1 | Paginate a fixed history prefix through the lease snapshot watermark |
| `POST /worker/v1/events` | 1 | Typed transition intents with operation receipts; not arbitrary event append |
| `POST /worker/v1/release` | 1 | Revoke ownership and requeue safely |
| `POST /worker/v1/stream` | 2 | Idempotent attempt-local batch append |

History can be paginated; do not require a giant lease response. The worker must finish loading and checking the required prefix before executing new code. Late page reads cannot cause a mixed replay snapshot. Worker-owned events after that watermark are tracked locally with acknowledged receipts; recovery always reloads a consistent history.

Example completion intent (Phase 2):

```json
{
  "protocolVersion": 1,
  "operationId": "20d7aaaf-cb3c-4528-a71f-d514c9f47d19",
  "workerSessionId": "93b8ade8-5d2f-4662-a0a2-67ad2d22e42e",
  "runId": "572ebfc1-a79f-4c34-bd92-d8669d61f34a",
  "fence": "7",
  "intent": {
    "kind": "complete_step",
    "stepKey": "search:a",
    "attempt": 2,
    "attemptToken": "b90c7629-2dd5-40a0-b282-bd5cc8e5407b",
    "finalBatchSeq": "3",
    "result": { "url": "https://example.org/result" }
  }
}
```

Errors use `{code, message, details, requestId}`. Required distinct codes include `DuplicateStepKey`, `Nondeterminism`, `ConcurrentCommand`, `Serialization`, `LeaseLost`, `FenceViolation`, `AttemptMismatch`, `WorkflowVersionMismatch`, `IdempotencyConflict`, `StreamBatchConflict`, `StreamGap`, `StreamExpired`, `InvalidRunState`, and `UnsupportedCapability`. Use 400 for malformed requests, 404 for unavailable runs, 409 for ownership/state/idempotency conflicts, 410 for expired stream history, 413 for limits, and 422 for validly encoded but invalid domain values. Retriable infrastructure failures must not masquerade as committed step failures.

### 10.4 Phase 4 behavioral minimums

Before activating signals/waits, an ADR and tests MUST define FIFO matching by durable ingress sequence, a signal's stable deduplication identity, early-arriving inbox storage, single consumption, named wait identity, timeout outcomes, and signal-versus-timeout races. Resolve these under the run lock; whichever eligible transition commits first determines the recorded wait outcome. Signals must not be dropped merely because the worker has not yet reached the wait.

Cancellation is a server transition serialized with completion. If completion committed first, cancellation returns already terminal. If cancellation wins, mark the run cancelled, remove its queue lease, abandon unfinished attempts, and emit terminal stream control atomically. Reject all subsequent new worker writes. Cooperative abort cannot undo already sent effects. Terminal records remain immutable; cancellation is not history deletion.

## 11. Versioning, observability, and resource limits

### 11.1 Version routing is Phase 1

Workers advertise exact registered `(tenant, workflow_name, version)` capabilities. The server resolves them to IDs and leases only compatible runs. No fallback to “latest.” If the pinned version has no live compatible worker, the run remains pending and inspection reports that reason. Deploy old and new workers side by side until the old version drains.

Version pinning and routing are core correctness, not postponed to Phase 4 as in v2. In-code patch markers or historical version migration remain out of scope until a separate ADR defines their semantics. Runtime divergence detection is an additional guard, not a substitute for deployment discipline.

### 11.2 Logs, tracing, and dashboard

Structured logs from Phase 1 include tenant ID, run ID, pinned workflow identity, command position, step key, attempt/token, worker session, fence, request ID, and event/stream sequences when known. Log decisions with enough context to reconstruct ownership and transitions. Redact secrets and avoid automatically duplicating all input/output payloads into logs.

Phase 3 adds OpenTelemetry, metrics, version-drain counts, and a Next.js dashboard through server APIs. The timeline distinguishes business attempts, lease epochs, retries, and abandoned work. Inputs are shown only when explicitly recorded; do not invent a step's captured inputs. Replay is diagnostic history folding, not executing provider calls in an inspector.

Long-running workflows may use linked traces per activation and spans per attempt; do not require an in-memory trace or open span to survive a process death. Persist correlation identifiers and label replay activity separately from original callbacks.

### 11.3 Remove inferred cost accounting

There is no `cost_micros` in the core schema and no cost promise in the initial status API. Relay cannot infer the price of arbitrary callbacks.

An optional Phase 3 extension may accept explicit user-reported attempt metadata, for example `{usage: {currency: "USD", amountMicros: "1200", source: "provider-reported"}}`. Before implementation, define a typed metadata-update intent with attempt identity, report ID, immutable payload hash, and durable deduplication. Missing data means unknown, never zero. Preserve reports for failed/abandoned attempts; only aggregate within the same currency and attribution model. Reports lost in a crash remain unknown. Do not represent estimates as authoritative billing data.

### 11.4 Replay and payload growth

Cold replay is O(recorded commands plus loaded history/payload bytes) per activation. O(n²) total replay work arises when a workflow accumulates n commands over roughly n cold resumptions; it is not inevitable for every long run. Measure activation count, history bytes, and replay duration separately.

Phase 1 separates durable replay state from transient execution state so optimizations do not alter semantics. Phase 3 may cache immutable history/results. A suspended JavaScript continuation cannot be reused across lost/reacquired leases as though ownership never changed. Any sticky continuation optimization needs a lease-scoped validity rule, failpoint tests, and before/after measurements.

Inline JSON is the baseline. The v2 32 KiB object-storage threshold becomes a measurement candidate, not an unimplemented promise. Values above the 1 MiB contract limit are rejected until an ADR specifies immutable content-addressed blobs, upload-before-reference ordering, read verification, authorization, retention, and orphan cleanup. Never commit a completion referring to a blob that was not durably stored.

Enforce finite worker run concurrency, parallel callback limits, request/payload limits, and bounded SSE queues from the phase that introduces each resource. Expose counters early; do not add fair queuing or sharding before contention is measured.

## 12. Failure injection and validation

### 12.1 Deterministic failpoints [Phase 1 foundation]

Failure behavior is a first-class deliverable. Build test/dev-only named hooks at transaction and callback boundaries. Production builds MUST reject failpoint activation and expose no remote fault-control endpoint.

```ts
await harness.arm({
  point: "step.after-user-function-before-completion",
  match: { runId, stepKey: "charge", attempt: 1 },
  occurrence: 1,
  action: "crash-process"
});
```

Actions include pause-on-barrier, crash process, drop committed HTTP response, close connection, and inject a specific provider error. Select by run/step/attempt/operation, hit count, and recorded seed. Use barriers and explicit acknowledgements rather than fragile sleeps. A seeded scenario is reproducible only with recorded fault targets/interleavings and inputs; a PRNG seed alone does not reproduce arbitrary OS scheduling.

Required hook names:

```text
lease.after-lock-before-validation
lease.after-update-before-commit
lease.after-commit-before-response
heartbeat.after-update-before-commit
heartbeat.after-commit-before-response
command.after-declaration-before-commit
step.after-start-commit-before-callback
step.after-user-function-before-completion
step.before-completion-commit
step.after-completion-commit-before-response
transition.after-event-insert-before-projections
transition.after-projections-before-queue
transition.before-commit
stream.after-batch-commit-before-response                 [P2]
stream.after-final-flush-before-completion                [P2]
stream.after-supersession-commit-before-new-chunk          [P2]
timer.after-fire-before-commit                           [P2]
sse.after-subscriber-registration-before-catchup          [P2]
```

Crashing before commit tests rollback; crashing after commit tests ambiguous acknowledgement. The harness must label the distinction. A fake provider records invocation counts and implements configurable idempotency to show both duplicated effects without provider support and deduplicated effects with it. Never assert exactly-once external calls when testing Relay's actual at-least-once contract.

### 12.2 Required failure/test matrix

Every row becomes an executable test or explicitly tracked later-phase acceptance test. Use real PostgreSQL for locking, isolation, constraints, notifications, and transaction behavior.

| ID / phase | Failure or interleaving | Required assertion |
|---|---|---|
| F01 / P1 | Kill worker during callback | Unfinished attempt restarts; committed successes do not rerun |
| F02 / P1; stream extension P2 | Pause A past expiry; grant B; resume A | A's new event, completion, heartbeat, release, and stream writes are rejected; B remains owner |
| F03 / P1 | Lease expired but reaper has not run | Old holder cannot renew or write even with unchanged fence |
| F04 / P1 | Request waits on run lock past expiry | Post-lock database time rejects stale ownership |
| F05 / P1 | External effect succeeds; crash before completion | Callback may run twice; stable provider key unchanged; dedupe only when fake provider supports it |
| F06 / P1 | Commit completion; drop HTTP response | Retry returns receipt; one completion and no callback restart |
| F07 / P1 | Two workers race for one run | At most one current ownership grant; monotonically increasing fences |
| F08 / P1 | Parallel A completes, B is active, C fails, then crash | A memoized, B recovered, C replayed as exhausted in P1; deterministic group error order |
| F09 / P1 | Parallel children finish simultaneously in reversed orders | Unique ordered event sequences; identical discovery ordinals and returned result order |
| F10 / P1 | Duplicate child or sequential step key | Error before duplicate callback; no partial group declaration |
| F11 / P1 | Insert/remove/reorder historical commands; early return | Precise divergence report, no mismatched memo hit or new callback |
| F12 / P1 | Captured value changes but command shape is identical | Test documents that structural detection does not detect it |
| F13 / P2 | Same case with explicit input fingerprint | Divergence before memoized result is returned |
| F14 / P2 | Reorder helper keys or lose helper response | Mismatch detected; identical retry/replay returns original committed value |
| F15 / P1 | Invalid JSON values including undefined, Date, BigInt, cycles, unsafe integers | Rejected with path/context; no silently transformed success record |
| F16 / P1 | Crash after event insert, projection update, or queue update but before commit | All changes roll back together; no stranded event/projection/queue state |
| F17 / P1 | Concurrent identical triggers and conflicting key reuse | One run for identical intent; 409 for conflicting intent |
| F18 / P1 | Changed body with same operation ID | Conflict; original history and receipt preserved |
| F19 / P1 | Deploy v2 while v1 runs exist; remove v1 worker | Correct routing; v1 waits visibly rather than running on v2 |
| F20 / P1 | Worker clocks intentionally skewed | Lease/grant/expiry decisions depend only on Postgres time |
| F21 / P1 | Direct invalid state/tenant FK/unauthorized DML | Constraint or privilege rejection; no persisted invalid row |
| F22 / P1 | Database disconnect/failover around commit | Unknown outcome resolved by receipts/history; no assumption of rollback; disclose failover RPO |
| F23 / P2 | Commit stream batch; drop response and retry | One batch row, same stream sequence, unchanged last-batch counter |
| F24 / P2 | Reuse batch ID with different text or send a gap | Conflict/gap rejected; no partial update |
| F25 / P2 | Crash after final flush but before completion | Durable old output superseded on recovery; new attempt authorized |
| F26 / P2 | Request completion before final batch persisted | Manifest validation rejects it; SDK tests ensure flush is awaited |
| F27 / P2 | Crash after completion commit | All declared final chunks and completion control present; no new attempt |
| F28 / P2 | Interleave A/B streams; restart A | Supersession removes only A's old output; B remains unchanged |
| F29 / P2 | Disconnect across a supersession; reconnect after last chunk | Control event replayed through same cursor; reducer matches continuous connection |
| F30 / P2 | Commit between SSE setup and catch-up; drop notifications | Catch-up/polling still delivers all retained events in order |
| F31 / P2 | Slow browser and listener reconnect | Bounded buffers, no per-browser DB connection, durable resumption |
| F32 / P2 | Expire terminal stream history | 410, no silent partial reconstruction, step results still available |
| F33 / P2 | Provider 429 / deterministic poison callback | Persisted backoff/jitter reused, budgets enforced; unhandled exhaustion enters failed-run view |
| F34 / P2 | Retry deadline while a parallel sibling is active | Sibling can finish; run is not released prematurely |
| F35 / P2 | Crash during timer fire, race schedulers | One timer outcome and atomic wakeup; no recomputed deadline |
| F36 / P3 | Projection rebuild or cached-history mismatch | Business projections match fold; live ownership/counters never rewind |
| F37 / P4 | Signal vs timeout vs cancellation vs completion | One recorded outcome under run lock; no double consumption or post-cancel writes |
| F38 / P4 | Cross-tenant read/stream/worker mutation | Uniform authorization denial and RLS coverage |

### 12.3 Demonstration and measurements

The Phase 1 demo starts approximately 100 finite workflows with a controllable provider, crashes/pauses workers, includes lost acknowledgements and version routing, then removes injected faults. Assert all runs reach valid terminal states within the test's recovery budget, zero stale mutations commit, no new attempt is authorized after durable completion, and recovery does not re-invoke a memoized step. Separately report callback counts and external-effect counts so the at-least-once gap is visible.

Store the build commit, database/runtime versions, fixture data, seed, fault script, event history, invocation ledger, assertion output, and reproduction command. The ledger must distinguish a stale previously authorized invocation (even if it starts late) from a newly authorized attempt after completion. Do not make the false assertion that fencing prevented every stale external effect.

Publish measured—not promised—p50/p95/p99 step overhead, lease throughput, recovery latency, replay at 10/100/1,000/10,000 commands, history bytes per step, stream catch-up time, and Postgres CPU/connections. Expand load levels from 100 to 1,000 to 10,000 concurrent runs only when resources allow; include hardware, configuration, workload, errors, and saturation point. Report what broke first and whether the bottleneck was callback/provider, worker, API, locking, database I/O, or browser consumption.

## 13. Roadmap with completion gates

Phases are ordered deliverables, not calendar promises. Dates/hours are estimated after inspecting the repository and measuring progress. Do not implement future-phase APIs while the current correctness gate is unresolved.

| Phase | Scope | Exit evidence |
|---|---|---|
| **0 — Design baseline** | This spec, core ADRs, project map, invariant/test ledger, inspect any existing code | Decisions and gaps recorded; next smallest implementation milestone selected |
| **1 — Core engine** | Monorepo/CI, migrations/constraints, server/SDK split, strict JSON, event allocator, atomic transitions, receipts, queue, leases/heartbeats/reaper/fencing, pinned versions, sequential and bounded parallel steps, structural replay checks, basic inspection, deterministic failpoint harness | Real-Postgres integration suite; approximately 100-run failure demo; verified event/projection consistency; runnable README and honest guarantees |
| **2 — Streaming and developer surface** | Unified stream log, idempotent batches, backpressure, flush ordering, supersession reducer, SSE/retention, helpers, explicit input fingerprints, timers, retries/backoff/jitter, failed-run/DLQ view, lint checks, publishable SDK | Agent demo survives worker failure and client reconnect; all P2 matrix rows pass; no unsupported future APIs exported |
| **3 — Inspection and evidence** | API-backed dashboard, attempt timeline, recorded input/output inspection, OTel/metrics, projection repair tooling, reproducible performance report; cache or metadata only if justified | Three-minute debugging recording; benchmark report with bottleneck; any optimization proves unchanged failure semantics |
| **4 — Conditional extension** | Auth/API keys/RLS, tenant fairness, signals/waits, cancellation, larger load tests; accepted ADRs before adding semantics | Cross-tenant tests, race tests, deployment/runbook review, measured capacity report |

Phase 0 is artifact readiness, not mandatory educational exercises. Do not require toy implementations, readings, quizzes, or restating fundamentals when the design is already captured. Phase 1 is the primary interview-ready artifact; later phases are optional investment. It is valid to finish at a sound phase, publish limitations and a retrospective, and leave a repository a stranger can run.

A Phase 2 agent example may plan, run parallel searches, stream synthesis, and notify a destination through a step. Human approval is a Phase 4 demonstration only. Example code must not quietly pull later features into an earlier milestone.

## 14. ADR register

ADRs live in `docs/adr/` with date, status, context, decision, alternatives, consequences, affected invariant IDs, and validation links. This register specifies decisions to capture; it does not claim individual ADR files have been authored or approved. When changing a decision, add a superseding ADR and update this specification in the same change.

| ADR | Decision and tradeoff |
|---|---|
| 0001 | Server plus in-process SDK worker: centralized coordination, extra operational component; library-only is a valid alternative, not inherently missing lease/replay capability |
| 0002 | Postgres queue and transactional transitions: one commit boundary, measurable locking/throughput limits versus an external broker |
| 0003 | Replay versus explicit state machines: ordinary functions with strict deterministic command constraints |
| 0004 | Name-based logical steps plus positional command checking: stable memo identity with explicit structural compatibility rules |
| 0005 | At-least-once execution and stable logical-step provider keys: external effects remain outside the database transaction |
| 0006 | TypeScript server/SDK: one ecosystem and shared schemas; document actual runtime tradeoffs without asserting another language is universally better |
| 0007 | No Kubernetes or hosted untrusted code execution: keep deployment aligned with the in-process execution model |
| 0008 | Per-run lock, two sequence allocators, Postgres clock, and exact live-lease fencing: simple proof surface, serialized writes within one run |
| 0009 | Commands/helpers/JSON/fingerprints: async journaled helpers, explicit labels, restricted values, and stated nondeterminism limits |
| 0010 | Parallel declaration and settle-all behavior: ordered discovery, independent commits, no sibling rollback, bounded callbacks |
| 0011 | Separate stream lifecycle, unified chunk/control cursor, scoped supersession, batch deduplication, and flush-before-completion |
| 0012 | SSE plus durable catch-up and shared notifications: one-way transport, at-least-once client application, bounded fan-out |
| 0013 | Immutable workflow versions and side-by-side drain: deployment storage/operations cost versus silent code replacement |
| 0014 | Event/projection/queue atomicity and operational-state distinction: receipts and maintenance recovery without rewriting history |
| 0015 | Phase gates and deterministic failpoints: correctness evidence before optional scope |
| 0016 | Dashboard through server APIs; remove inferred cost: one authorization/schema boundary, explicit metadata only |
| Later | Signals/waits/cancellation; tenant isolation/fairness; blob storage; cache/continuation safety; retention/archive/restore, each only when activated |

## 15. Maintainer workflow: stay aware without turning work into lessons

Maintain a small set of current records. Each implementation change updates only the records it affects; avoid parallel narratives that drift.

| Record | Contents | Update trigger |
|---|---|---|
| `docs/project-map.md` | Components, entry points, data flow, schema ownership, commands to run/debug, links to key files | Module boundary, protocol, or deployment changes |
| `docs/invariants.md` | Invariant ID, exact claim, enforcing function/constraint, regression tests, known limitation | Any semantic or ownership change |
| `docs/adr/` | Material choices and alternatives, with consequences | Architecture/API/persistence decisions |
| `docs/status.md` | Active phase, current milestone, implemented vs planned, blockers, next concrete move | End of each work session |
| `CHANGELOG.md` | User-visible behavior and compatibility changes; migration notes | Each meaningful merged change |
| `docs/test-matrix.md` | F01–F38 mapping to real tests, failpoints, last relevant evidence and reproduction command | Failure fix or gate completion |

Initial invariant ledger:

| ID | Exact claim | Primary evidence |
|---|---|---|
| I01 | User code executes only in the SDK application's process | Package boundaries and deployment smoke test |
| I02 | Committed step success prevents new attempt authorization and recovery re-execution for that run/key | F01, F06, F08, F27 |
| I03 | New worker mutations require the exact current unexpired ownership | F02–F04, F20 |
| I04 | Event, business projections, queue effects, and receipt are one transition commit | F16, F18, F35 |
| I05 | Each log has one atomic per-run sequence allocator | F09, F16, F23 |
| I06 | Discovery/command identity never derives from child completion order | F08–F11, F14 |
| I07 | Logical provider key is stable across attempts; external exactly-once is not promised | F05 |
| I08 | All durable values satisfy the same serialization contract on first execution and replay | F15 |
| I09 | Every client-visible stream control shares the chunk cursor | F28–F32 |
| I10 | A successful streaming step has all declared final batches committed and sealed | F23–F27 |
| I11 | Runs execute only on compatible pinned versions | F19 |
| I12 | Runtime guarantees do not claim arbitrary semantic nondeterminism detection | F12–F14 |

**At the start of a work session:** read `docs/status.md`, the relevant map section, and affected invariants. Choose one smallest coherent change with a visible result. No generic architecture re-planning.

**For each change:** require a compact change note: problem and resulting behavior; touched boundary/invariants; migration or compatibility effect; exact validation and outcome; remaining risk. Inspect the implementation around state transitions and the test that would fail if the claimed invariant were broken. A green test count alone is not evidence that the right case was tested.

**Weekly, 20–30 minutes:** review the diff since the last review; compare the project map with code; examine one actual failure trace and any newly changed invariant; inspect phase-gate progress, open ADRs, current benchmark changes, and scope growth. Record decisions and the next milestone in `docs/status.md`. Do not turn this into a tutorial or duplicate all session notes. The cadence is a workflow recommendation, not a scheduled automation created by this document.

**Before a phase is declared done:** run its failure matrix and reproducible demo, verify docs against exported APIs/migrations, archive evidence with the commit, and list limitations. Unsupported APIs remain absent. The user should be able to answer what exists, what changed, which guarantee is supported by which test, and what is still unknown from these records.

### Rules for AI coding agents

Place these rules in the repository's `AGENTS.md` and reference this spec:

1. Read repository state, current phase/status, relevant code, and affected invariants before editing. Do not recreate existing modules under new names.
2. Implement the smallest coherent milestone. Do not stop at a plan when implementation is authorized. Keep the repository runnable.
3. Treat v3 as authoritative. Do not silently alter semantics or implement future phases. For material changes, write the concrete proposed ADR/spec amendment before asking the maintainer to resolve a genuine decision.
4. Use strict TypeScript, runtime protocol validation, explicit transactions, readable SQL, and small modules. Do not introduce frameworks, brokers, abstractions, or dependencies without a concrete requirement.
5. For concurrency changes, state the interleaving and lock order. For persistence changes, state the atomic boundary. For replay changes, show expected versus recorded command identity.
6. Add the relevant failure regression, run appropriate checks, and report exactly what ran. Do not claim unexecuted checks passed or present authored tests as verified behavior.
7. Preserve user changes. Surface blockers or conflicting evidence promptly; choose routine implementation details without repeatedly asking permission.
8. Update affected status, invariant, ADR, change-log, and test-matrix entries in the same change. Do not generate speculative documentation for features not implemented.
9. Report concisely: changed behavior; key decision; validation/evidence; remaining risk; next concrete move. No lessons, praise, or feature-count claims.
10. Never weaken a failing assertion to hide an invariant violation. Diagnose the failure using history, receipts, ownership, and the failpoint trace; change the contract only through an explicit reviewed decision.

## 16. v2-to-v3 decision trace

| Review change | v3 resolution |
|---|---|
| Stream/control cursor mismatch | §8: one stream log for chunks and controls, independent of workflow history |
| Underspecified parallel replay/failure | §5.2: atomic ordered declaration, independent child commits, settle-all, bounded concurrency |
| Event sequence allocation | §6.2: run-row lock and transactional per-log counters |
| Event/projection/queue split writes | §6.4–6.6: atomic transition functions, receipts, enforced mutation boundary |
| Overstated nondeterminism guarantees | §2/§4: structural limits; §3.2 optional explicit-input fingerprints |
| Unidentified now/random/uuid calls | §7.1: named async commands with position/kind/key and committed values |
| Serialization ambiguity | §3: strict Relay JSON, canonical clones, errors and limits |
| Worker-clock lease risk | §6.3: post-lock Postgres time and exact ownership checks |
| Weak constraints/queue identity duplication | §9: checks, indexes, composite tenant FK, queue identity derived solely through run ID |
| Duplicate stream batches | §8.2: immutable attempt-local batch identity and payload conflict detection |
| Completion before final stream flush | §8.3: acknowledged flush, contiguous manifest, sealed attempt and atomic control |
| LISTEN/SSE setup race | §8.5: committed shared listener, subscriber registration, durable catch-up, polling |
| Roadmap/API contradictions | §10/§13: every API tagged; version routing P1, signals/cancellation P4 |
| Unsupported inferred cost | §11.3: removed core column; explicit metadata extension only |
| Dashboard database coupling | §1/§11.2: dashboard uses server read APIs from its introduction |
| Failure injection and recruiting evidence | §12: deterministic hooks, test matrix, invocation ledger, benchmark artifacts |
| Stay aware without teaching | §15: project map, invariants, ADRs, status review, change log, evidence, agent rules |
| Additional v2 idempotency flaw | §2.3: stable logical-step provider key, separate attempt token |
| Preserve architecture and roadmap strengths | In-process SDK, code-free server, Postgres, replay, name-based steps, fencing, separate stream retention, version drain, phase gates, honest at-least-once positioning |

This specification establishes design requirements and acceptance criteria. It does not assert that the engine, migrations, tests, benchmark results, or ADR files have already been implemented.
