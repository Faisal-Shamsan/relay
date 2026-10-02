# Project state

## Current phase

Phase 1, milestone M1 complete. Phase 1 is not complete.

## Completed

- Project specification reviewed.
- Engineering workflow defined.
- Repository control structure created.
- M1: one sequential step. A crash before completion restarts the attempt. A later attempt completes. Replay returns the stored result without calling the user function. A duplicate completion returns the stored receipt.

## In progress

Nothing.

## Implemented

One sequential `ctx.step`. Crash before completion, lease expiry, a later attempt, replay without re-running the user function, and a duplicate completion receipt. Verified by the M1 exit test on real PostgreSQL (`tests/exit.test.ts`).

## Known issues

- M2 fencing proofs are not implemented: heartbeat, background reaper, and stale-owner interleavings (F02 without stream writes, F03, F04, F07, F20).
- Recorded JSON ambiguities remain: the root counts as depth 1; unsafe integers are rejected rather than rewritten; SHA-256 is the receipt digest; no Unicode normalization.

## Next milestone

M2: lease lifetime and fencing.

## Next task

Heartbeat, reaper scheduling, and fencing interleavings.
