# 0017 Signals, waits, and cancellation

Date: 2026-10-29

Status: Accepted

## Context

Phase 4 activates the reserved signal, wait, and cancellation vocabulary. Specification §10.4 requires one committed outcome under the run lock, durable early signals, and cancellation that does not delete history.

## Decision

A signal's identity is `(run_id, idempotency_key)`. The same canonical payload replays the stored `SignalReceived` event. A different payload conflicts. The inbox is a projection of that event and is rebuilt with the other business projections. Signals that arrive before a wait are kept. A wait named `name` consumes the oldest unconsumed signal of that name. The same wait name cannot be declared twice in one run.

`ctx.waitForEvent(name, {timeoutMs})` is a top-level `wait` command. The deadline is database time captured after the run lock, once. An already stored signal is consumed in that transaction and the run does not park. Otherwise the run is `waiting` and the queue is `parked`. `relay_fire_one_wait` records timeout without choosing a new deadline. The stored result is `{outcome:"signal", payload}` or `{outcome:"timeout"}`. Replay returns it. If a signal and a timeout both try to resolve the wait, the first committed `WaitResolved` wins and the loser does not consume another signal.

`POST /v1/runs/:id/cancel` races completion under the run lock. An already terminal run is returned unchanged. A winning cancel appends `RunCancelled`, sets `cancelled` and `completed_at`, leaves output and error null, deletes the queue row, abandons unfinished attempts, and appends one `run_terminal` stream control. Later worker writes are rejected. The callback `AbortSignal` is cooperative. An effect that already left the process is not undone. Timer, retry, and wait firing do nothing once the run is terminal.

## Alternatives

Dropping a signal that arrives before the wait loses an approval that the workflow has not reached. Treating timeout as a second consumption can pair one signal with two outcomes. Deleting events on cancel makes the run look like it never happened.

## Consequences

Rebuild must restore inbox consumption from `WaitResolved` and must not move fences, queue leases, receipts, or allocators. Schedulers must notice a terminal run. The SDK exports `signal`, `waitForEvent`, and `cancel` only in the milestone that implements each one.

## Invariants

INV-004. F37.

## Validation

`packages/server/test/signals.test.ts`, `packages/server/test/waits.test.ts`, and `packages/server/test/cancel.test.ts`.
