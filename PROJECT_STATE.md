# Project state

## Current phase

Phase 1, milestone M2 complete. Phase 1 is not complete.

## Completed

- Project specification reviewed.
- Engineering workflow defined.
- Repository control structure created.
- M1: one sequential step. A crash before completion restarts the attempt. A later attempt completes. Replay returns the stored result without calling the user function. A duplicate completion returns the stored receipt.
- M2: heartbeat, explicit release, and a reaper. A stale owner cannot renew or write after expiry or a newer grant. Client clocks do not choose deadlines.

## In progress

Nothing.

## Implemented

One sequential `ctx.step`. Crash before completion, lease expiry, a later attempt, replay without re-running the user function, and a duplicate completion receipt. Verified by the M1 exit test on real PostgreSQL (`tests/exit.test.ts`).

Heartbeat renewal, explicit release, and a server reaper that expires one due lease per transaction. Fencing proofs F03, F04, F07, and F20, plus F02 without stream writes, are in `packages/server/test/fencing.test.ts`. The SDK heartbeat loop is covered by `tests/heartbeat.test.ts`.

## Known issues

- F02 stream writes are not implemented. They belong to Phase 2.
- `heartbeat.after-update-before-commit` is not implemented. That hook is inside the database transaction.
- Recorded JSON ambiguities remain: the root counts as depth 1; unsafe integers are rejected rather than rewritten; SHA-256 is the receipt digest; no Unicode normalization.

## Next milestone

M3: effects and atomicity.

## Next task

Provider-effect idempotency, intra-transaction rollback, and conflicting operation-id proofs (F05, F16, F18).
