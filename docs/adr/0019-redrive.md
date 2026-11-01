# 0019 Redrive

Date: 2026-10-31

Status: Accepted

## Context

A terminal run never returns to a runnable state. Explicit redrive creates a different run id. The caller needs a way to start that run from a completed, failed, or cancelled run, with the same tenant and the same pinned workflow version. Repeating the request must not start a second run. The original history stays readable and unchanged.

## Decision

`POST /v1/runs/:id/redrive` requires `idempotencyKey`. `input` is optional. An omitted input copies the stored input. A present input, including null, is the new run's input. The request hash is the canonical hash of the key plus the input when the field is present, and of the key alone when it is omitted. The same key and the same body return the stored new run id. A different body is `409 IdempotencyConflict`.

The new run is `pending`, queued `ready`, and has a new id. Its `workflow_version_id` and `tenant_id` are copied from the source row. Its `idempotency_key` stays null, so the redrive key does not use the trigger idempotency namespace. The identity is a `run_redrives` row: source run, key, request hash, and new run id. That row is not a projection of the source history.

The function reads the source run and does not take a lock on it. A missing run and another tenant's run are both `404 InvalidRunState` / `run not found`. A run that is not terminal is `404` / `run is not terminal`. No event is appended to the source. Rebuild of the source must leave the redrive row in place.

The first accept is HTTP 201. A replay is HTTP 200. Both bodies are `{ "runId": "<new run id>" }`.

## Alternatives

Moving the source back to `pending` would make a terminal run runnable. Appending a redrive event to the source would change history that the caller did not ask to extend. Storing the redrive key on `runs.idempotency_key` would collide with trigger admission. Locking the source `FOR UPDATE` would block cancellation and completion of a row this operation does not write.

## Consequences

Dashboard controls are a later milestone. The SDK does not export a redrive client. A second redrive of the same source uses another key and creates another run. The new run is an ordinary pending run and can be leased.

## Invariants

A terminal run stays terminal. Another tenant cannot learn that the source exists or read the new run id.

## Validation

`tests/redrive.test.ts` on real PostgreSQL.
