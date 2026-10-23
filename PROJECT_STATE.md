# Project state

## Current phase

Phase 1, milestone M5 complete. Phase 1 is not complete.

## Completed

- Project specification reviewed.
- Engineering workflow defined.
- Repository control structure created.
- M1: one sequential step. A crash before completion restarts the attempt. A later attempt completes. Replay returns the stored result without calling the user function. A duplicate completion returns the stored receipt.
- M2: heartbeat, explicit release, and a reaper. A stale owner cannot renew or write after expiry or a newer grant. Client clocks do not choose deadlines.
- M3: a crash after an external effect and before completion runs the callback again with the same provider key. Finish-run crashes before commit roll back together. A changed body with the same operation id conflicts.
- M4: duplicate step keys fail before the second callback. Reordered, removed, and inserted commands diverge before the new callback. A captured value with the same command shape is not detected.
- M5: a parallel group declares every child in one transaction. A completed child is memoized, an active child restarts, and a failed child stays exhausted. Reversed completion order does not change discovery ordinals or the returned result order.

## In progress

Nothing.

## Implemented

One sequential `ctx.step`. Crash before completion, lease expiry, a later attempt, replay without re-running the user function, and a duplicate completion receipt. Verified by the M1 exit test on real PostgreSQL (`tests/exit.test.ts`).

Heartbeat renewal, explicit release, and a server reaper that expires one due lease per transaction. Fencing proofs F03, F04, F07, and F20, plus F02 without stream writes, are in `packages/server/test/fencing.test.ts`. The SDK heartbeat loop is covered by `tests/heartbeat.test.ts`.

A fake provider in the test worker records invocations and effects. F05 is `tests/effects.test.ts`. Finish-run failpoints abort the transaction in `packages/server/sql/003_m3.sql`; F16 is `packages/server/test/atomicity.test.ts`. F18 is `packages/server/test/idempotency.test.ts`.

Structural replay compares the command at each position before calling the user function. F10, F11, and F12 are in `tests/structure.test.ts`.

`ctx.step.parallel` records the group and its children before any callback. Each child start, completion, and failure commits independently. F08 and F09 are in `tests/parallel.test.ts`. Phase 1 does not retry an exhausted child.

## Known issues

- F02 stream writes are not implemented. They belong to Phase 2.
- `heartbeat.after-update-before-commit` is not implemented. That hook is inside the database transaction.
- Recorded JSON ambiguities remain: the root counts as depth 1; unsafe integers are rejected rather than rewritten; SHA-256 is the receipt digest; no Unicode normalization.

## Next milestone

M6: admission and recovery budget.

## Next task

Identical triggers, conflicting keys, pinned version routing, and direct invalid writes (F17, F19, F21), plus recovery-budget exhaustion.
