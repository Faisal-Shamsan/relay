# Test matrix

Starter matrix from [spec.md](spec.md) §12.2. Every row is a required failure scenario. F01 and F06 are covered by the M1 exit test. F03, F04, F07, and F20 are covered by the M2 fencing tests. F02 is covered except stream writes, which stay Phase 2. F05, F16, and F18 are covered by the M3 proofs. Every other row is still not implemented.

Invariant IDs refer to [invariants.md](invariants.md). Where no ledger ID is the primary claim, the invariant cell is `—`.

| Scenario | Expected behaviour | Invariant | Test | Status |
|---|---|---|---|---|
| F01 (Phase 1): Worker killed mid-step, during the callback | Unfinished attempt restarts. Committed successes do not rerun. | INV-001 | tests/exit.test.ts | Implemented |
| F02 (Phase 1; stream extension Phase 2): Stale worker wakes after lease expiry and a new grant | The old worker's new event, completion, heartbeat, release, and stream writes are rejected. The new worker remains owner. | INV-002 | packages/server/test/fencing.test.ts | Implemented except stream writes |
| F03 (Phase 1): Lease expired, reaper has not run | The old holder cannot renew or write, even with an unchanged fence. | INV-007 | packages/server/test/fencing.test.ts | Implemented |
| F04 (Phase 1): Request waits on the run lock past expiry | Post-lock database time rejects stale ownership. | INV-007 | packages/server/test/fencing.test.ts | Implemented |
| F05 (Phase 1): Crash after an external side effect but before the completion commit | The callback may run twice. The stable provider key is unchanged. Deduplication occurs only when the fake provider supports it. | INV-012 | tests/effects.test.ts | Implemented |
| F06 (Phase 1): Completion commits, then the HTTP response is dropped | Retry returns the receipt. There is one completion and no callback restart. | INV-001 | tests/exit.test.ts | Implemented |
| F07 (Phase 1): Concurrent lease attempts by two workers for one run | At most one current ownership grant. Fences increase monotonically. | INV-002 | packages/server/test/fencing.test.ts | Implemented |
| F08 (Phase 1): Parallel child A completes, B is active, C fails, then the worker crashes | A is memoized. B is recovered. In Phase 1, C is replayed as exhausted. Group error order is deterministic. | INV-001, INV-011 | — | Not implemented |
| F09 (Phase 1): Parallel children finish simultaneously in reversed orders | Event sequences are unique and ordered. Discovery ordinals and returned result order stay identical. | INV-010, INV-011 | — | Not implemented |
| F10 (Phase 1): Duplicate step key, child or sequential | Error before the duplicate callback. No partial group declaration. | — | — | Not implemented |
| F11 (Phase 1): Replay divergence from inserted, removed, or reordered historical commands, or an early return | Precise divergence report. No mismatched memoized hit and no new callback. | INV-006 | — | Not implemented |
| F12 (Phase 1): Captured value changes while the command shape stays identical | The test documents that structural detection does not detect this case. | INV-015 | — | Not implemented |
| F13 (Phase 2): Same captured-input change with an explicit input fingerprint | Divergence is reported before a memoized result is returned. | INV-006 | — | Not implemented |
| F14 (Phase 2): Helper keys reordered, or a helper response is lost | Mismatch is detected. An identical retry or replay returns the original committed value. | INV-006, INV-011 | — | Not implemented |
| F15 (Phase 1): Invalid JSON values, including undefined, Date, BigInt, cycles, and unsafe integers | Rejected with path and context. No silently transformed success record. | INV-013 | — | Not implemented |
| F16 (Phase 1): Crash after event insert, projection update, or queue update, but before commit | All changes roll back together. No stranded event, projection, or queue state. | INV-005, INV-010 | packages/server/test/atomicity.test.ts | Implemented |
| F17 (Phase 1): Concurrent identical triggers, and conflicting reuse of an idempotency key | One run for identical intent. 409 for conflicting intent. | — | — | Not implemented |
| F18 (Phase 1): Changed body with the same operation ID | Conflict. Original history and receipt are preserved. | INV-005 | packages/server/test/idempotency.test.ts | Implemented |
| F19 (Phase 1): Incompatible workflow version. Deploy v2 while v1 runs exist and remove the v1 worker | Routing stays on the pinned version. v1 waits visibly and does not run on v2. | INV-003 | — | Not implemented |
| F20 (Phase 1): Worker clocks intentionally skewed | Lease, grant, and expiry decisions depend only on Postgres time. | INV-007 | packages/server/test/fencing.test.ts, tests/heartbeat.test.ts | Implemented |
| F21 (Phase 1): Direct invalid state, tenant foreign key, or unauthorized DML | Constraint or privilege rejection. No persisted invalid row. | — | — | Not implemented |
| F22 (Phase 1): Database disconnect or failover around commit | Unknown outcome is resolved from receipts and history. No assumption of rollback. Failover RPO is disclosed. | INV-004 | — | Not implemented |
| F23 (Phase 2): Duplicate stream-batch retry. Commit a batch, drop the response, and retry | One batch row, the same stream sequence, and an unchanged last-batch counter. | INV-008, INV-010 | — | Not implemented |
| F24 (Phase 2): Reuse a batch ID with different text, or send a gap | Conflict or gap is rejected. No partial update. | INV-008 | — | Not implemented |
| F25 (Phase 2): Streaming step retry. Crash after the final flush but before completion | Durable old output is superseded on recovery. A new attempt is authorized. | INV-008 | — | Not implemented |
| F26 (Phase 2): Request completion before the final batch is persisted | Manifest validation rejects it. SDK tests ensure the flush is awaited. | INV-008 | — | Not implemented |
| F27 (Phase 2): Crash after the completion commit | All declared final chunks and the completion control are present. No new attempt. | INV-001, INV-008 | — | Not implemented |
| F28 (Phase 2): Streaming supersession. Interleave streams A and B, then restart A | Supersession removes only A's old output. B remains unchanged. | INV-014 | — | Not implemented |
| F29 (Phase 2): Stream reconnect. Disconnect across a supersession and reconnect after the last chunk | The control event is replayed through the same cursor. The reducer matches a continuous connection. | INV-014 | — | Not implemented |
| F30 (Phase 2): Commit between SSE setup and catch-up, and drop notifications | Catch-up and polling still deliver all retained events in order. | INV-014 | — | Not implemented |
| F31 (Phase 2): Slow browser and listener reconnect | Bounded buffers. No per-browser database connection. Durable resumption. | INV-014 | — | Not implemented |
| F32 (Phase 2): Expire terminal stream history | 410. No silent partial reconstruction. Step results remain available. | INV-014 | — | Not implemented |
| F33 (Phase 2): Provider 429 or a deterministic poison callback | Persisted backoff and jitter are reused. Budgets are enforced. Unhandled exhaustion enters the failed-run view. | — | — | Not implemented |
| F34 (Phase 2): Retry deadline while a parallel sibling is active | The sibling can finish. The run is not released prematurely. | — | — | Not implemented |
| F35 (Phase 2): Crash during timer fire, with racing schedulers | One timer outcome and an atomic wakeup. The deadline is not recomputed. | INV-005 | — | Not implemented |
| F36 (Phase 3): Projection rebuild or cached-history mismatch | Business projections match the event fold. Live ownership and counters never rewind. | INV-004 | — | Not implemented |
| F37 (Phase 4): Signal versus timeout versus cancellation versus completion | One recorded outcome under the run lock. No double consumption and no writes after cancel. | — | — | Not implemented |
| F38 (Phase 4): Cross-tenant read, stream, or worker mutation | Uniform authorization denial and RLS coverage. | — | — | Not implemented |
