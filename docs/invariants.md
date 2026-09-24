# Invariants

Initial ledger. Statements are taken from [spec.md](spec.md). No enforcing code or regression tests exist yet.

`INV-001` through `INV-008` are the bootstrap set. `INV-009` through `INV-015` are the remaining claims from the specification's initial ledger (§15), kept so this file does not drop a specified claim. The correspondence is listed at the end.

## INV-001

**Statement.** A step whose `StepCompleted` event has been durably committed must never execute its user function again during replay.

**Why it matters.** Memoization is the recovery guarantee for a finished logical step in the same run. Recovery must not authorize or invoke a new attempt of that step. An unfinished attempt may still run again. A completion that already committed wins over restart, and an old attempt cannot later complete. This does not revoke an older process's already-authorized callback, and it does not make external effects exactly-once. Specification §2.1, §2.2, §4.3, §5.1, and §5.2.

**How we expect to prove it.** Phase 1 failure rows F01, F06, and F08, plus Phase 2 row F27, on real PostgreSQL. Kill or drop the acknowledgement around completion and show that a committed success is not authorized again. The Phase 1 demonstration must report that recovery does not re-invoke a memoized step, while still showing callback counts separately from external-effect counts.

## INV-002

**Statement.** A worker holding a stale fence must never successfully mutate durable run state.

**Why it matters.** After a new lease, the fence has advanced. Exact fence equality is part of every worker mutation check inside the database transition. Fencing stops stale database writes. It does not revoke an HTTP call already sent to a provider, and it does not forcibly stop arbitrary JavaScript. Specification §2.1, §2.2, and §6.3.

**How we expect to prove it.** F02 and F07. After a newer lease is granted, the old worker's event, completion, heartbeat, release, and (Phase 2) stream writes are rejected, and the new owner remains owner. Concurrent grants produce at most one current ownership and monotonically increasing fences.

## INV-003

**Statement.** Only a worker capable of executing the run's pinned workflow version may lease that run.

**Why it matters.** A run is pinned to one immutable workflow version. The server leases only a compatible registered version. There is no fallback to "latest." If no compatible worker is live, the run stays pending and inspection reports that reason. Version routing is Phase 1 correctness. Specification §2.1, §6.3, and §11.1.

**How we expect to prove it.** F19. With v1 runs still present, a v2-only worker must not execute them. The v1 runs wait visibly until a compatible worker exists.

## INV-004

**Statement.** `run_events` is the authoritative workflow history.

**Why it matters.** Workflow history is the append-only record of creation, command declarations, attempt outcomes, timer outcomes, and terminal state. Business projections (`commands`, `steps`, `step_attempts`, and the business fields of `runs`) are rebuildable from it. Stream content is separately authoritative in `run_stream_events`. Fence, allocators, live leases, queue availability, receipts, and retention markers are coordination state and must not be treated as replay projections. Specification §6.1.

**How we expect to prove it.** F16 for atomic history writes, and Phase 3 row F36 for projection verification. Folding events without executing user code must match business projections. Rebuild must not rewrite history, resurrect a live lease, lower a fence, rewind a sequence counter, or delete a receipt.

## INV-005

**Statement.** A durable state transition must not leave workflow history, projections, and queue state logically contradictory.

**Why it matters.** A logical transition commits its history, projections, scheduling changes, and request receipt together. A crash before commit must roll all of those changes back. Specification §2.1, §6.4, and §6.5.

**How we expect to prove it.** F16, F18, and Phase 2 row F35. Inject failure after event insert, after projection update, or after queue update, and before commit. The database must not keep a stranded event, projection, or queue row. A conflicting reuse of an operation ID must leave the original history and receipt unchanged.

## INV-006

**Statement.** When Relay detects structural replay divergence, it must fail loudly rather than return a potentially incorrect memoized result.

**Why it matters.** At a historical command position, the descriptor is compared before the stored result is observed. A mismatch stops the run with a nondeterminism error and must not run the new callback or return the old result as if it matched. This applies to detected command-structure differences, and in Phase 2 to an explicit-input fingerprint mismatch. It is not a claim that unchanged command shape proves the callback is semantically the same. Specification §2.1, §2.2, §3.2, and §4.3.

**How we expect to prove it.** F11, Phase 2 rows F13 and F14, and the negative case F12. Inserting, removing, reordering, or returning early against historical commands yields a precise divergence report and no mismatched memo hit. F12 must document that a captured-value change with an identical command shape is not detected. F13 must show that an explicit-input fingerprint mismatch is reported before a memoized result is returned.

## INV-007

**Statement.** Lease ownership and expiry decisions use PostgreSQL/database time rather than worker-local clocks.

**Why it matters.** Ownership is decided under the run lock using a Postgres timestamp captured after the lock is acquired (`clock_timestamp()`), not a worker clock and not a transaction-start `now()` that went stale while waiting. A late heartbeat cannot revive an expired lease. Worker clocks may only decide when to attempt a heartbeat. Specification §6.3.

**How we expect to prove it.** F03, F04, and F20. An expired lease is rejected even when the reaper has not run and the fence is unchanged. A request that waits on the run lock past expiry is rejected using post-lock database time. Skewed worker clocks do not grant or extend ownership.

## INV-008

**Statement.** For streaming steps, `StepCompleted` may not be committed until all final stream buffers for that attempt have been durably acknowledged.

**Why it matters.** The SDK must close emission, join outstanding emits, flush the last buffer, and receive durable acknowledgements before it requests completion. The completion transaction accepts that request only when the attempt's contiguous persisted batch sequence equals the declared final batch sequence, then seals the attempt and appends the completion control in the same commit. The server can check the declared manifest and committed rows; a local buffer the SDK never sent cannot be discovered from the database alone. This is a Phase 2 rule. Specification §8.3.

**How we expect to prove it.** F26 and F27, with F25 for the crash after a durable flush but before completion. A completion request whose final batch is not yet persisted is rejected. After a committed completion, every declared final chunk and the completion control are present, and no new attempt is authorized. SDK tests must show that flush is awaited before the completion request.

## INV-009

**Statement.** User code executes only in the SDK application's process.

**Why it matters.** The server never receives or executes workflow source code. The server image contains no user code. Specification §1 and §15 (I01).

**How we expect to prove it.** Package boundaries, once they exist, and a deployment smoke test that the server process does not load workflow functions. No such packages exist yet.

## INV-010

**Statement.** Each log has one atomic per-run sequence allocator.

**Why it matters.** `runs.next_event_seq` and `runs.next_stream_seq` allocate exact ranges in the same transaction as the inserted rows, while the run lock is held. Rollback rolls back the counter. Sequences are not taken from `MAX(seq)+1`, from the client, or from a cluster-global sequence. Specification §6.2 and §15 (I05).

**How we expect to prove it.** F09, F16, and Phase 2 row F23. Concurrent child completions receive unique ordered event sequences. A crash before commit does not leave a consumed sequence or a partial row. A retried stream batch keeps the original stream sequence.

## INV-011

**Statement.** Discovery and command identity never derive from child completion order.

**Why it matters.** `command_pos`, `first_seen_ordinal`, and `run_events.seq` are different orderings. Step memoization identity is `(run_id, step_key)`. Parallel children take consecutive discovery positions in declaration-array order. Results return in input order. Specification §4.2, §5.2, and §15 (I06).

**How we expect to prove it.** F08, F09, F11, and Phase 2 row F14. Reversed completion order still yields the same discovery ordinals and the same returned result order. Group outcome is a fold of terminal child states.

## INV-012

**Statement.** The logical provider idempotency key is stable across attempts. Relay does not promise exactly-once external effects.

**Why it matters.** `stepCtx.idempotencyKey` identifies the logical step and must not include the attempt or fence. `attemptToken` identifies one invocation authorization and changes per attempt. A crash after an external effect and before the completion commit may run the callback again. Deduplication happens only when the provider supports it. Specification §2.2, §2.3, and §15 (I07).

**How we expect to prove it.** F05, using a fake provider that can record invocations and optionally deduplicate. Assert at-least-once attempts. Do not assert exactly-once external calls. The Phase 1 invocation ledger must separate a stale previously authorized invocation from a newly authorized attempt after completion.

## INV-013

**Statement.** All durable values satisfy the same serialization contract on first execution and replay.

**Why it matters.** Workflow inputs and outputs, step results, explicit step inputs, signal payloads, command arguments, and metadata use Relay JSON. Invalid values are rejected. Nothing is silently coerced, dropped, or passed through `toJSON`. Callers observe canonical parsed clones. Specification §3 and §15 (I08).

**How we expect to prove it.** F15. Reject `undefined`, `Date`, `bigint`, cycles, unsafe integers, and the other excluded values, with path and context. No silently transformed success record is stored.

## INV-014

**Statement.** Every client-visible stream control shares the chunk cursor.

**Why it matters.** `run_stream_events` holds both chunks and client-visible control events. Its sequence is the only SSE cursor. Clients must not join that cursor to `run_events.seq`. Specification §8.1 and §15 (I09).

**How we expect to prove it.** Phase 2 rows F28 through F32. Reconnect and catch-up return retained chunk and control events through one ordered cursor. Supersession invalidates only the named old attempt's output. A slow client is disconnected and resumes from its applied cursor, without a database listener per browser.

## INV-015

**Statement.** Runtime guarantees do not claim detection of arbitrary semantic nondeterminism.

**Why it matters.** Replay checking detects command-structure divergence. Optional explicit-input fingerprints detect changes in those supplied inputs. Changed closures, environment values, mutated globals, provider behavior, or different code with the same command shape may go undetected. Specification §2.2, §4.4, and §15 (I12).

**How we expect to prove it.** F12, with F13 and F14 as the cases that are detected. The test must document the undetected case rather than treating it as a passed semantic guarantee.

## Correspondence to the specification ledger

| This ledger | Specification §15 |
|---|---|
| INV-001 | I02 |
| INV-002, INV-007 | I03 |
| INV-003 | I11 |
| INV-004 | §6.1 (history authority; not a separate I-id) |
| INV-005 | I04 |
| INV-006 | §4.3 fail-loud structural checking |
| INV-015 | I12 |
| INV-008 | I10 |
| INV-009 | I01 |
| INV-010 | I05 |
| INV-011 | I06 |
| INV-012 | I07 |
| INV-013 | I08 |
| INV-014 | I09 |
