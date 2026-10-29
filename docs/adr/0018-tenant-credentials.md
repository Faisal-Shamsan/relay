# 0018 Tenant credentials, row security, and same-priority fairness

Date: 2026-10-29

Status: Accepted

## Context

Before Phase 4 the HTTP server assumes the seeded tenant. Tenant columns exist, but a caller can read or mutate another tenant by knowing an id. Specification §1 and §13 require API credentials, authorization, and row-level security together. Queue selection is `priority DESC, available_at, run_id`, which can keep one tenant's ready runs ahead of another's at the same priority.

## Decision

`Authorization: Bearer` maps to one tenant. Only the SHA-256 hash of the key is stored. A `client` key calls the public routes. A `worker` key calls the worker routes. A missing, revoked, or wrong-kind key is 401 `Unauthorized`. A run or stream that is missing or belongs to another tenant is 404 `InvalidRunState` / `not found`. A body `tenantId` that differs from the key is the same 401. The server sets `app.tenant_id` for the request transaction.

`relay_app` SELECT policies hide other tenants. Child-table policies call `relay_run_visible`, a `SECURITY DEFINER` helper, so they do not recurse through `runs`. A missing setting yields no rows. Transition functions remain `SECURITY DEFINER`. When the setting is present they reject a different tenant before a lease or terminal check. When it is absent, maintenance jobs and the existing in-process SQL tests keep their current behavior. The admin connection used by migrations and projection rebuild is not an HTTP path. Holding the database password can still set the GUC; the tenant boundary for callers is the HTTP credential.

The dashboard sends `RELAY_API_KEY` from the server side, including the stream proxy. The browser does not receive the key and does not open the database.

Same-priority leasing orders by `tenants.last_granted_at ASC NULLS FIRST`, then availability and run id. Priority still comes first. The timestamp is updated in the grant transaction after the run lock. It is coordination state. A lower priority can still wait. This is not a throughput or sharding result.

## Alternatives

A role per tenant would make the GUC unnecessary and would multiply database roles for every local tenant. Skipping the setting on worker routes would let a worker key name another tenant's session. Fair queuing that ignores priority would change who runs a higher-priority run.

## Consequences

Existing HTTP tests send the seeded local keys. Direct table writes by `relay_app` stay revoked. Rebuild must not move `last_granted_at`.

## Invariants

F38. The fairness proof is `packages/server/test/fairness.test.ts`.

## Validation

`packages/server/test/auth.test.ts` and `packages/server/test/fairness.test.ts`.
