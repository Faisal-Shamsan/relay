# 0020 Payload growth

Date: 2026-10-31

Status: Accepted

## Context

Specification §11.4 keeps inline JSON as the baseline. A history cache and content-addressed blobs are allowed only after measurement. A sticky JavaScript continuation is not a candidate: a lost lease must not be treated as if ownership never changed.

`docs/capacity.md` records the run on this machine at commit `97cf173`. The largest completed history is 10,000 sequential steps. Each step returns its index and emits one character.

| | 10,000 commands |
|---|---|
| First activation | 201,154.4 ms |
| Replay | 892.7 ms |
| Replay callbacks | 0 |
| History bytes | 3,330,407 |
| Bytes per command | 333.0 |

The same report's 10,000 one-step runs took 171,213.1 ms of wall time. Replay of the long history is under one second. The first activation of that history is about 225 times longer.

The Relay JSON limit is 1 MiB of canonical UTF-8 per value (`CANONICAL_UTF8_LIMIT` in `packages/protocol/src/json.ts`). The 10,000-command journal is about 3.2 MiB in total and 333 bytes per command. No measured value is a single JSON body at the 1 MiB limit. Stream catch-up past the 10-command history was not measured, because the SSE connection closed. That gap is not a payload size.

## Decision

Inline JSON stays. This milestone adds no history cache and no content-addressed blobs.

Replay of the largest completed history does not dominate the report. The time is in the first activation: the step callbacks, the one-character emits, and the database round trips. A lease-scoped cache would not change that cost, and it would have to be dropped whenever the fence changes.

No measured value reaches the 1 MiB contract limit, so there is no blob to upload, verify, authorize, or retain. Values above that limit continue to be rejected. A completion is never stored as a pointer to an object that is not in the journal.

A sticky continuation is not shipped.

## Alternatives

A lease-scoped cache of immutable history would avoid re-reading a long journal after a crash. The measured replay is already under a second at 10,000 commands, and the cache would be discarded on lease loss. Adding it before replay dominates would add a staleness rule the measurements do not need.

Content-addressed blobs would move large values out of the row. The measured commands are hundreds of bytes, not mebibytes. Introducing upload-before-reference, read verification, tenant authorization, retention, and orphan cleanup for payloads that fit inline would be a second store with no measured value to put in it.

## Consequences

Step results, inputs, and signal payloads remain inline JSON. The 1 MiB canonical limit is unchanged. A later measurement can reopen this decision if replay of a completed history dominates, or if a real value reaches 1 MiB.

## Validation

`docs/capacity.md`. No new engine path was added, so no new test was added. The existing suite still rejects an oversized canonical value through `packages/protocol/src/json.ts`.
