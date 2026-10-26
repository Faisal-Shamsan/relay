# Project state

## Current phase

Phase 1 complete. Phase 2 is in progress: the stream log, live SSE, seven-day retention, and explicit inputs are implemented. Helpers, timers, and retries are not.

## Completed

- Project specification reviewed.
- Engineering workflow defined.
- Repository control structure created.
- M1: one sequential step. A crash before completion restarts the attempt. A later attempt completes. Replay returns the stored result without calling the user function. A duplicate completion returns the stored receipt.
- M2: heartbeat, explicit release, and a reaper. A stale owner cannot renew or write after expiry or a newer grant. Client clocks do not choose deadlines.
- M3: a crash after an external effect and before completion runs the callback again with the same provider key. Finish-run crashes before commit roll back together. A changed body with the same operation id conflicts.
- M4: duplicate step keys fail before the second callback. Reordered, removed, and inserted commands diverge before the new callback. A captured value with the same command shape is not detected.
- M5: a parallel group declares every child in one transaction. A completed child is memoized, an active child restarts, and a failed child stays exhausted. Reversed completion order does not change discovery ordinals or the returned result order.
- M6: an identical trigger returns the same run, and a different intent conflicts. A pinned v1 run stays pending when only a v2 worker is live. Direct invalid writes do not persist. Exceeding the stored recovery budget fails the run instead of starting another attempt.
- M7: the remaining Phase 1 failpoint hooks roll back with the transaction. Projection verification reports drift without rewriting history. Run and event reads are available. A dropped database connection is resolved from the receipt, and the failover recovery point is disclosed. The failure demo completes 100 runs.
- M8: a durable per-run stream log. A retried batch keeps one sequence. A conflicting or gapped batch writes nothing. Completion requires the flushed batch number. A crash after that flush supersedes only the interrupted attempt, and a crash after completion does not start another. A sibling's chunks stay in place.
- M9: live SSE on the stream sequence. A shared listener only wakes readers. A client that reconnects across supersession matches one that stayed connected. A missed notification is recovered by catch-up or polling. A client that falls behind a bounded queue is disconnected, and a later batch still commits.
- M10: seven-day stream retention. A terminal run's stream rows are deleted in the same locked transaction that sets `stream_expired_at`. The workflow history, step result, and stream sequence counter stay. A reader gets `410 StreamExpired` with the stored result, including a connection that was already open. An active run is left alone.
- M11: explicit step inputs. `ctx.step(name, {input}, fn)` and a parallel child's `input` persist the normalized value and its canonical hash with the command. Replay compares that fingerprint before a memoized return or another attempt. An omitted input and an explicit `null` differ. The callback receives a frozen clone. A closure change with the same command shape is still not detected.

## In progress

Nothing.

## Implemented

One sequential `ctx.step`. Crash before completion, lease expiry, a later attempt, replay without re-running the user function, and a duplicate completion receipt. Verified by the M1 exit test on real PostgreSQL (`tests/exit.test.ts`).

Heartbeat renewal, explicit release, and a server reaper that expires one due lease per transaction. Fencing proofs F02, F03, F04, F07, and F20 are in `packages/server/test/fencing.test.ts`. The SDK heartbeat loop is covered by `tests/heartbeat.test.ts`.

A fake provider in the test worker records invocations and effects. F05 is `tests/effects.test.ts`. Finish-run failpoints abort the transaction in `packages/server/sql/003_m3.sql`; F16 is `packages/server/test/atomicity.test.ts`. F18 is `packages/server/test/idempotency.test.ts`.

Structural replay compares the command at each position before calling the user function. F10, F11, and F12 are in `tests/structure.test.ts`.

`ctx.step.parallel` records the group and its children before any callback. Each child start, completion, and failure commits independently. F08 and F09 are in `tests/parallel.test.ts`. Phase 1 does not retry an exhausted child.

Trigger identity is the tenant plus the idempotency key. F17 is `packages/server/test/admission.test.ts`. `GET /v1/workflows` reports live compatible workers. F19 is in the same file. F21 rejects invalid state, a missing tenant, and direct application writes. A step stores its recovery ceiling at declaration. Exhaustion is `tests/recovery.test.ts`. The default ceiling is 100.

Phase 1 hooks for grant, heartbeat, declaration, and completion abort the open transaction through the existing failpoint helper. The server still does not expose a failpoint route. `lease.after-commit-before-response` is in the SDK, after the lease response. `GET /v1/runs/:id` and paginated event reads are on the HTTP server. `relay_verify_projections` compares commands, steps, attempts, business run fields, and the stream log with history. It does not repair them. F22 is `packages/server/test/failover.test.ts`; the recovery-point disclosure is in `README.md`. The 100-run demo is `tests/demo.test.ts`. It prints measured activation times and separates callback counts from provider effects. Invalid JSON is rejected with a path in `packages/protocol/test/json.test.ts`.

`stepCtx.emit` appends UTF-8 batches to `run_stream_events` on `POST /worker/v1/stream`. One unacknowledged buffer is in flight. It flushes at about 100 ms, at 4 KiB, and before completion or failure. Completion, failure, finish, and restart commit `step_completed`, `step_failed`, `run_terminal`, or `superseded` in the same transaction as the workflow transition. F23 through F28 are in `tests/stream.test.ts`. `GET /v1/runs/:id/stream` serves those rows as SSE. One shared `LISTEN` connection wakes readers; it is not a connection per client. `subscribe` reduces chunks and controls in order and drops a sequence it already applied. A `superseded` control invalidates only that attempt. F29 through F31 are in `tests/sse.test.ts`. A finished run's stream log expires seven days after completion. `relay_expire_streams` locks one due run, deletes its stream rows, and leaves `next_stream_seq` where it is. `GET /v1/runs/:id` still returns the output. A new batch is rejected and an identical operation receipt is replayed without allocating a sequence. F32 is `tests/retention.test.ts`. `ctx.step` accepts an explicit input, and a parallel child can too. The normalized value and its SHA-256 fingerprint are stored on the command and the step. A different fingerprint, or a switch between the plain step and the input-bearing step, raises nondeterminism before the stored result is returned and before another attempt starts. F13 is `tests/input.test.ts`. F12 remains the case that is not detected. There is no timer, signal, dashboard, or metrics endpoint.

## Known issues

- Stream catch-up, 1,000-run, and 10,000-run figures are not measured.
- Recorded JSON ambiguities remain: the root counts as depth 1; unsafe integers are rejected rather than rewritten; SHA-256 is the receipt digest; no Unicode normalization.
- External effects are at least once. Provider deduplication is not an exactly-once guarantee.

## Next milestone

Helpers, timers, retries, and the failed-run view. Phase 2 is not complete.

## Next task

None. Do not start the rest of Phase 2 until asked.
