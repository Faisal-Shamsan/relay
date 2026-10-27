import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import {
  canonicalHash,
  canonicalRelayJson,
  canonicalRetryPolicy,
  HTTP_BODY_LIMIT,
  parseRelayJson,
  validateRelayJson,
  PROTOCOL_VERSION,
  RelayJsonError,
  SEEDED_TENANT_ID,
  type Json,
} from "@relay/protocol";
import { callJson, createPool, withTransaction } from "./db.js";
import { decimalString, fromDatabaseError, HttpError } from "./errors.js";
import { createStreamHub, type StreamHub } from "./stream-hub.js";

export interface StartedServer {
  url: string;
  close: () => Promise<void>;
  suppressStreamNotifications(): void;
}

export async function startServer(options: {
  appDatabaseUrl: string;
  leaseDurationMs: number;
  heartbeatIntervalMs?: number;
  reaperIntervalMs?: number;
  timerIntervalMs?: number;
  recoveryBudget?: number;
  port?: number;
  streamPollMs?: number;
  streamKeepaliveMs?: number;
  streamQueueLimit?: number;
  socketHighWaterMark?: number;
  streamRetentionIntervalMs?: number;
  streamRetentionHorizonMs?: number;
  beforeStreamCatchup?: (runId: string) => Promise<void>;
}): Promise<StartedServer> {
  const leaseDurationMs = requirePositiveInteger(options.leaseDurationMs, "leaseDurationMs");
  const heartbeatIntervalMs = requirePositiveInteger(options.heartbeatIntervalMs ?? 10_000, "heartbeatIntervalMs");
  const reaperIntervalMs = requireNonNegativeInteger(options.reaperIntervalMs ?? 1_000, "reaperIntervalMs");
  const timerIntervalMs = requireNonNegativeInteger(options.timerIntervalMs ?? 1_000, "timerIntervalMs");
  const recoveryBudget = requireRecoveryBudget(options.recoveryBudget ?? 100);
  const streamRetentionIntervalMs = requireNonNegativeInteger(
    options.streamRetentionIntervalMs ?? 60_000,
    "streamRetentionIntervalMs",
  );
  const streamRetentionHorizonMs = requireNonNegativeInteger(
    options.streamRetentionHorizonMs ?? 7 * 24 * 60 * 60 * 1000,
    "streamRetentionHorizonMs",
  );
  if (options.socketHighWaterMark !== undefined) {
    requirePositiveInteger(options.socketHighWaterMark, "socketHighWaterMark");
  }
  const pool = createPool(options.appDatabaseUrl);
  const stream = await createStreamHub({
    appDatabaseUrl: options.appDatabaseUrl,
    pool,
    pollMs: requirePositiveInteger(options.streamPollMs ?? 1_000, "streamPollMs"),
    keepaliveMs: requirePositiveInteger(options.streamKeepaliveMs ?? 15_000, "streamKeepaliveMs"),
    queueLimit: requirePositiveInteger(options.streamQueueLimit ?? 32, "streamQueueLimit"),
    beforeCatchup: options.beforeStreamCatchup,
  });
  const server = createServer(
    options.socketHighWaterMark === undefined ? {} : { highWaterMark: options.socketHighWaterMark },
    async (req, res) => {
    const requestId = randomUUID();
    try {
      await route(pool, stream, leaseDurationMs, heartbeatIntervalMs, recoveryBudget, req, res, requestId);
    } catch (error) {
      const mapped = error instanceof HttpError ? error : error instanceof RelayJsonError
        ? new HttpError(422, "Serialization", error.message)
        : fromDatabaseError(error);
      if (!mapped) {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ code: "Internal", message: "internal error", details: {}, requestId }));
        return;
      }
      sendJson(res, mapped.status, { code: mapped.code, message: mapped.message, details: mapped.details, requestId });
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(options.port ?? 0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("server did not bind a TCP port");
  }
  let reaper: NodeJS.Timeout | undefined;
  let timers: NodeJS.Timeout | undefined;
  let retention: NodeJS.Timeout | undefined;
  if (reaperIntervalMs > 0) {
    const tick = () => {
      void reapExpiredLeases(pool, 32).catch((error: unknown) => {
        console.error("reaper failed", error);
      });
    };
    tick();
    reaper = setInterval(tick, reaperIntervalMs);
    reaper.unref();
  }
  if (timerIntervalMs > 0) {
    const tick = () => {
      void fireDueTimers(pool, 32)
        .then(() => fireDueRetries(pool, 32))
        .catch((error: unknown) => {
          console.error("timer sweep failed", error);
        });
    };
    timers = setInterval(tick, timerIntervalMs);
    timers.unref();
  }
  if (streamRetentionIntervalMs > 0) {
    const sweep = () => {
      void expireDueStreams(pool, streamRetentionHorizonMs).catch((error: unknown) => {
        console.error("stream retention failed", error);
      });
    };
    retention = setInterval(sweep, streamRetentionIntervalMs);
    retention.unref();
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      if (reaper) clearInterval(reaper);
      if (timers) clearInterval(timers);
      if (retention) clearInterval(retention);
      await stream.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      await pool.end();
    },
    suppressStreamNotifications: () => stream.suppressNotifications(),
  };
}

async function route(
  pool: ReturnType<typeof createPool>,
  stream: StreamHub,
  leaseDurationMs: number,
  heartbeatIntervalMs: number,
  recoveryBudget: number,
  req: IncomingMessage,
  res: ServerResponse,
  requestId: string,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://127.0.0.1");
  if (req.method === "POST" && url.pathname === "/v1/runs") {
    const body = await readJson(req);
    const workflowName = requiredString(body, "workflowName");
    const workflowVersion = requiredString(body, "workflowVersion");
    if (!("input" in body)) throw new HttpError(422, "Serialization", "input is required");
    const input = body.input as Json;
    const idempotencyKey = idempotencyHeader(req);
    const triggerHash = canonicalHash([workflowName, workflowVersion, input]);
    const runId = randomUUID();
    const result = await callJson(
      pool,
      `SELECT relay_create_run($1,$2,$3,$4::jsonb,$5,$6,$7) AS result`,
      [SEEDED_TENANT_ID, workflowName, workflowVersion, JSON.stringify(input), triggerHash, runId, idempotencyKey],
    );
    const created = result as { runId?: unknown; created?: unknown } | null;
    if (typeof created !== "object" || created === null || Array.isArray(created) || typeof created.runId !== "string") {
      throw new Error("create response was not an object");
    }
    sendJson(res, created.created === false ? 200 : 201, { runId: created.runId });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/workflows") {
    const rows = await withTransaction(pool, async (client) => {
      const listed = await client.query(
        `SELECT wv.name,
                wv.version,
                (SELECT count(*)::int
                 FROM worker_session_versions m
                 JOIN worker_sessions s ON s.id = m.worker_session_id
                 WHERE m.workflow_version_id = wv.id
                   AND s.expires_at > clock_timestamp()) AS "compatibleWorkers",
                (SELECT count(*)::int
                 FROM runs r
                 WHERE r.workflow_version_id = wv.id
                   AND r.state = 'pending') AS "pendingRuns"
         FROM workflow_versions wv
         WHERE wv.tenant_id = $1
         ORDER BY wv.name, wv.version`,
        [SEEDED_TENANT_ID],
      );
      return listed.rows;
    });
    sendJson(res, 200, { workflows: rows });
    return;
  }

  const runMatch = /^\/v1\/runs\/([0-9a-fA-F-]{36})$/.exec(url.pathname);
  if (req.method === "GET" && runMatch) {
    const runId = runMatch[1];
    const row = await withTransaction(pool, async (client) => {
      const found = await client.query(
        `SELECT r.state,
                r.output,
                r.error,
                r.current_fence::text AS fence,
                wv.name AS "workflowName",
                wv.version AS "workflowVersion",
                q.state AS "queueState",
                q.lease_owner::text AS "leaseOwner",
                to_jsonb(q.lease_expires_at) #>> '{}' AS "leaseExpiresAt"
         FROM runs r
         JOIN workflow_versions wv ON wv.id = r.workflow_version_id
         LEFT JOIN run_queue q ON q.run_id = r.id
         WHERE r.id = $1 AND r.tenant_id = $2`,
        [runId, SEEDED_TENANT_ID],
      );
      return found.rows[0] as {
        state: string;
        output: Json | null;
        error: Json | null;
        fence: string;
        workflowName: string;
        workflowVersion: string;
        queueState: string | null;
        leaseOwner: string | null;
        leaseExpiresAt: string | null;
      } | undefined;
    });
    if (!row) throw new HttpError(404, "InvalidRunState", "run not found");
    sendJson(res, 200, {
      runId,
      workflowName: row.workflowName,
      workflowVersion: row.workflowVersion,
      state: row.state,
      output: row.output,
      error: row.error,
      ownership: {
        fence: row.fence,
        queueState: row.queueState,
        leaseOwner: row.leaseOwner,
        leaseExpiresAt: row.leaseExpiresAt,
      },
    });
    return;
  }

  const eventsMatch = /^\/v1\/runs\/([0-9a-fA-F-]{36})\/events$/.exec(url.pathname);
  if (req.method === "GET" && eventsMatch) {
    const runId = eventsMatch[1];
    const after = url.searchParams.get("after") ?? "0";
    if (!/^[0-9]+$/.test(after)) throw new HttpError(422, "Serialization", "after must be a decimal string");
    const limit = pageLimit(url.searchParams.get("limit"));
    const page = await withTransaction(pool, async (client) => {
      const exists = await client.query(`SELECT 1 FROM runs WHERE id = $1 AND tenant_id = $2`, [runId, SEEDED_TENANT_ID]);
      if (exists.rowCount === 0) return null;
      const events = await client.query(
        `SELECT seq::text AS seq, fence::text AS fence, actor, type,
                command_pos::text AS "commandPos", step_key AS "stepKey", attempt, payload
         FROM run_events
         WHERE run_id = $1 AND seq > $2::bigint
         ORDER BY seq
         LIMIT $3`,
        [runId, after, limit + 1],
      );
      return events.rows as Array<{ seq: string }>;
    });
    if (page === null) throw new HttpError(404, "InvalidRunState", "run not found");
    const more = page.length > limit;
    const events = more ? page.slice(0, limit) : page;
    sendJson(res, 200, { events, next: more ? events[events.length - 1]?.seq ?? null : null });
    return;
  }

  const streamMatch = /^\/v1\/runs\/([0-9a-fA-F-]{36})\/stream$/.exec(url.pathname);
  if (req.method === "GET" && streamMatch) {
    const runId = streamMatch[1];
    if (runId === undefined) throw new HttpError(404, "InvalidRunState", "run not found");
    await stream.serve(req, res, runId, url);
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/register") {
    const body = await readJson(req);
    requireProtocol(body);
    const result = await callJson(
      pool,
      `SELECT relay_register_worker($1,$2,$3,$4,$5,$6::jsonb) AS result`,
      [
        SEEDED_TENANT_ID,
        requiredString(body, "workerSessionId"),
        requiredString(body, "operationId"),
        canonicalHash(body),
        PROTOCOL_VERSION,
        JSON.stringify(body.workflows),
      ],
    );
    sendJson(res, 200, result);
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/lease") {
    const body = await readJson(req);
    requireProtocol(body);
    const result = await callJson(
      pool,
      `SELECT relay_grant_lease($1,$2,$3,$4::interval) AS result`,
      [
        requiredString(body, "workerSessionId"),
        requiredString(body, "operationId"),
        canonicalHash(body),
        `${leaseDurationMs} milliseconds`,
      ],
    );
    if (typeof result !== "object" || result === null || Array.isArray(result)) {
      throw new HttpError(500, "Internal", "lease response was not an object");
    }
    sendJson(res, 200, { ...result, leaseDurationMs, heartbeatIntervalMs });
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/heartbeat") {
    const body = await readJson(req);
    requireProtocol(body);
    const result = await callJson(
      pool,
      `SELECT relay_heartbeat($1,$2,$3::bigint,$4,$5,$6::interval) AS result`,
      [
        requiredString(body, "workerSessionId"),
        requiredString(body, "runId"),
        decimalString(body.fence, "fence"),
        requiredString(body, "operationId"),
        canonicalHash(body),
        `${leaseDurationMs} milliseconds`,
      ],
    );
    sendJson(res, 200, result);
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/release") {
    const body = await readJson(req);
    requireProtocol(body);
    const result = await callJson(
      pool,
      `SELECT relay_release_lease($1,$2,$3::bigint,$4,$5) AS result`,
      [
        requiredString(body, "workerSessionId"),
        requiredString(body, "runId"),
        decimalString(body.fence, "fence"),
        requiredString(body, "operationId"),
        canonicalHash(body),
      ],
    );
    sendJson(res, 200, result);
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/stream") {
    const body = await readJson(req);
    requireProtocol(body);
    const text = requiredString(body, "text");
    if (text.length === 0) throw new HttpError(422, "Serialization", "stream batch text is empty");
    if (typeof body.attempt !== "number" || !Number.isInteger(body.attempt)) {
      throw new HttpError(422, "Serialization", "attempt must be an integer");
    }
    const result = await callJson(
      pool,
      `SELECT relay_append_stream_batch($1,$2,$3,$4::bigint,$5,$6,$7,$8::bigint,$9,$10,$11) AS result`,
      [
        requiredString(body, "operationId"),
        requiredString(body, "workerSessionId"),
        requiredString(body, "runId"),
        decimalString(body.fence, "fence"),
        requiredString(body, "stepKey"),
        body.attempt,
        requiredString(body, "attemptToken"),
        decimalString(body.batchSeq, "batchSeq"),
        text,
        canonicalHash(text),
        canonicalHash(body),
      ],
    );
    sendJson(res, 200, result);
    return;
  }

  const history = /^\/worker\/v1\/runs\/([0-9a-fA-F-]{36})\/history$/.exec(url.pathname);
  if (req.method === "GET" && history) {
    const runId = history[1];
    const sessionId = url.searchParams.get("workerSessionId");
    const after = url.searchParams.get("after") ?? "0";
    const through = url.searchParams.get("through");
    if (!sessionId || !through) throw new HttpError(422, "Serialization", "workerSessionId and through are required");
    decimalString(after, "after");
    decimalString(through, "through");
    const rows = await withTransaction(pool, async (client) => {
      const gate = await client.query(
        `SELECT s.history_watermark::text AS watermark, q.lease_owner::text AS owner, q.state
         FROM worker_sessions s
         JOIN run_queue q ON q.run_id = $2
         WHERE s.id = $1`,
        [sessionId, runId],
      );
      const row = gate.rows[0] as { watermark: string; owner: string; state: string } | undefined;
      if (!row || row.state !== "leased" || row.owner !== sessionId || row.watermark !== through) {
        throw new HttpError(409, "LeaseLost", "history watermark is not the lease snapshot");
      }
      const events = await client.query(
        `SELECT seq::text AS seq, fence::text AS fence, type, step_key AS "stepKey", attempt,
                command_pos::text AS "commandPos", payload
         FROM run_events
         WHERE run_id = $1 AND seq > $2::bigint AND seq <= $3::bigint
         ORDER BY seq`,
        [runId, after, through],
      );
      return events.rows;
    });
    sendJson(res, 200, { events: rows });
    return;
  }

  if (req.method === "POST" && url.pathname === "/worker/v1/events") {
    const body = await readJson(req);
    requireProtocol(body);
    const intent = body.intent;
    if (typeof intent !== "object" || intent === null || Array.isArray(intent)) {
      throw new HttpError(422, "Serialization", "intent is required");
    }
    const kind = (intent as { kind?: unknown }).kind;
    const sessionId = requiredString(body, "workerSessionId");
    const operationId = requiredString(body, "operationId");
    const runId = requiredString(body, "runId");
    const fence = decimalString(body.fence, "fence");
    const hash = canonicalHash(body);
    const result = await dispatchIntent(pool, kind, intent as { kind: string; [key: string]: Json }, {
      sessionId,
      operationId,
      runId,
      fence,
      hash,
      recoveryBudget,
    });
    sendJson(res, 200, result);
    return;
  }

  sendJson(res, 404, { code: "InvalidRunState", message: "not found", details: {}, requestId });
}

async function dispatchIntent(
  pool: ReturnType<typeof createPool>,
  kind: unknown,
  intent: { kind: string; [key: string]: Json | undefined },
  ctx: { sessionId: string; operationId: string; runId: string; fence: string; hash: string; recoveryBudget: number },
): Promise<unknown> {
  if (kind === "declare_and_start") {
    const stepKey = requiredString(intent, "stepKey");
    const expected = decimalString(intent.expectedCommandPos, "expectedCommandPos");
    const explicit = explicitInput(intent);
    const retry = readRetry(intent.retry);
    return callJson(
      pool,
      `WITH cfg AS (SELECT set_config('relay.recovery_budget', $11, true))
       SELECT relay_declare_and_start($1,$2,$3,$4::bigint,$5,$6::bigint,$7,$8,$9::jsonb,$10,$12::jsonb) AS result FROM cfg`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        stepKey,
        expected,
        ctx.hash,
        explicit.present,
        explicit.present ? JSON.stringify(explicit.input) : null,
        explicit.hash,
        String(ctx.recoveryBudget),
        retry === null ? null : JSON.stringify(retry),
      ],
    );
  }
  if (kind === "declare_parallel") {
    const rawChildren = intent.children;
    if (!Array.isArray(rawChildren) || rawChildren.some((child) => typeof child !== "string")) {
      throw new HttpError(422, "Serialization", "children must be an array of strings");
    }
    const children = rawChildren as string[];
    if (typeof intent.concurrency !== "number" || !Number.isInteger(intent.concurrency)) {
      throw new HttpError(422, "Serialization", "concurrency must be an integer");
    }
    const childInputs = childInputFingerprints(children, intent.childInputs);
    const childRetries = childRetryPolicies(children, intent.childRetries);
    return callJson(
      pool,
      `WITH cfg AS (SELECT set_config('relay.recovery_budget', $11, true))
       SELECT relay_declare_parallel($1,$2,$3,$4::bigint,$5,$6::bigint,$7::jsonb,$8,$9,$10::jsonb,$12::jsonb) AS result FROM cfg`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        requiredString(intent, "groupKey"),
        decimalString(intent.expectedCommandPos, "expectedCommandPos"),
        JSON.stringify(children),
        intent.concurrency,
        ctx.hash,
        childInputs === null ? null : JSON.stringify(childInputs),
        String(ctx.recoveryBudget),
        childRetries === null ? null : JSON.stringify(childRetries),
      ],
    );
  }
  if (kind === "record_helper") {
    const helperKind = requiredString(intent, "helperKind");
    if (helperKind !== "now" && helperKind !== "random" && helperKind !== "uuid") {
      throw new HttpError(422, "Serialization", "helper kind is invalid");
    }
    return callJson(
      pool,
      `SELECT relay_record_helper($1,$2,$3,$4::bigint,$5,$6,$7::bigint,$8) AS result`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        helperKind,
        requiredString(intent, "key"),
        decimalString(intent.expectedCommandPos, "expectedCommandPos"),
        ctx.hash,
      ],
    );
  }
  if (kind === "schedule_timer") {
    if (typeof intent.durationMs !== "number" || !Number.isSafeInteger(intent.durationMs) || intent.durationMs < 0) {
      throw new HttpError(422, "Serialization", "durationMs must be a nonnegative safe integer");
    }
    return callJson(
      pool,
      `SELECT relay_schedule_timer($1,$2,$3,$4::bigint,$5,$6::bigint,$7::bigint,$8) AS result`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        requiredString(intent, "key"),
        String(intent.durationMs),
        decimalString(intent.expectedCommandPos, "expectedCommandPos"),
        ctx.hash,
      ],
    );
  }
  if (kind === "start_attempt") {
    return callJson(
      pool,
      `SELECT relay_start_attempt($1,$2,$3,$4::bigint,$5,$6) AS result`,
      [ctx.operationId, ctx.sessionId, ctx.runId, ctx.fence, requiredString(intent, "stepKey"), ctx.hash],
    );
  }
  if (kind === "restart_attempt") {
    const restarted = await callJson(
      pool,
      `SELECT relay_restart_attempt($1,$2,$3,$4::bigint,$5,$6) AS result`,
      [ctx.operationId, ctx.sessionId, ctx.runId, ctx.fence, requiredString(intent, "stepKey"), ctx.hash],
    );
    if (
      typeof restarted === "object"
      && restarted !== null
      && !Array.isArray(restarted)
      && (restarted as { code?: unknown }).code === "RecoveryBudgetExceeded"
    ) {
      throw new HttpError(409, "RecoveryBudgetExceeded", "recovery budget is exhausted");
    }
    return restarted;
  }
  if (kind === "complete_step") {
    if (typeof intent.attempt !== "number" || !Number.isInteger(intent.attempt)) {
      throw new HttpError(422, "Serialization", "attempt must be an integer");
    }
    if (!Object.hasOwn(intent, "result")) {
      throw new HttpError(422, "Serialization", "result is required");
    }
    return callJson(
      pool,
      `SELECT relay_complete_step($1,$2,$3,$4::bigint,$5,$6,$7,$8::jsonb,$9,$10::bigint) AS result`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        requiredString(intent, "stepKey"),
        intent.attempt,
        requiredString(intent, "attemptToken"),
        JSON.stringify(intent.result ?? null),
        ctx.hash,
        finalBatchSeq(intent.finalBatchSeq),
      ],
    );
  }
  if (kind === "fail_step") {
    if (typeof intent.attempt !== "number" || !Number.isInteger(intent.attempt)) {
      throw new HttpError(422, "Serialization", "attempt must be an integer");
    }
    if (typeof intent.error !== "object" || intent.error === null || Array.isArray(intent.error)) {
      throw new HttpError(422, "Serialization", "error is required");
    }
    return callJson(
      pool,
      `SELECT relay_fail_step($1,$2,$3,$4::bigint,$5,$6,$7,$8::jsonb,$9) AS result`,
      [
        ctx.operationId,
        ctx.sessionId,
        ctx.runId,
        ctx.fence,
        requiredString(intent, "stepKey"),
        intent.attempt,
        requiredString(intent, "attemptToken"),
        JSON.stringify(intent.error),
        ctx.hash,
      ],
    );
  }
  if (kind === "fail_run") {
    if (!isJsonObject(intent.error) || intent.error.code !== "StepExecutionError" || typeof intent.error.message !== "string") {
      throw new HttpError(422, "Serialization", "run failure must be a StepExecutionError");
    }
    return callJson(
      pool,
      `SELECT relay_fail_run($1,$2,$3,$4::bigint,$5::jsonb,$6) AS result`,
      [ctx.operationId, ctx.sessionId, ctx.runId, ctx.fence, JSON.stringify(intent.error), ctx.hash],
    );
  }
  if (kind === "finish_run") {
    return callJson(
      pool,
      `SELECT relay_finish_run($1,$2,$3,$4::bigint,$5::jsonb,$6) AS result`,
      [ctx.operationId, ctx.sessionId, ctx.runId, ctx.fence, JSON.stringify(intent.output ?? null), ctx.hash],
    );
  }
  throw new HttpError(422, "UnsupportedCapability", "intent is not supported");
}

function requireProtocol(body: Json): void {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new HttpError(422, "Serialization", "object body is required");
  }
  if (body.protocolVersion !== PROTOCOL_VERSION) {
    throw new HttpError(422, "UnsupportedCapability", "protocol version is not supported");
  }
}

function readRetry(value: Json | undefined): Json | null {
  if (value === undefined) return null;
  return canonicalRetryPolicy(value);
}

function childRetryPolicies(children: string[], raw: Json | undefined): Json[] | null {
  if (raw === undefined) return null;
  if (!Array.isArray(raw) || raw.length !== children.length) {
    throw new HttpError(422, "Serialization", "child retries must align with children");
  }
  let any = false;
  const specs: Json[] = [];
  for (const item of raw) {
    if (!isJsonObject(item) || typeof item.explicitRetry !== "boolean") {
      throw new HttpError(422, "Serialization", "child retry policy is invalid");
    }
    if (!item.explicitRetry) {
      if ("retry" in item) throw new HttpError(422, "Serialization", "child retry policy is invalid");
      specs.push({ explicitRetry: false });
      continue;
    }
    if (!("retry" in item) || item.retry === undefined) {
      throw new HttpError(422, "Serialization", "child retry policy is invalid");
    }
    any = true;
    specs.push({ explicitRetry: true, retry: canonicalRetryPolicy(item.retry) });
  }
  return any ? specs : null;
}

function explicitInput(intent: { [key: string]: Json | undefined }): { present: boolean; input: Json; hash: string | null } {
  if (intent.explicitInput === undefined || intent.explicitInput === false) {
    if ("input" in intent) throw new HttpError(422, "Serialization", "input requires an explicit input");
    return { present: false, input: null, hash: null };
  }
  if (intent.explicitInput !== true) throw new HttpError(422, "Serialization", "explicitInput must be boolean");
  if (!("input" in intent)) throw new HttpError(422, "Serialization", "explicit input is missing");
  const input = validateRelayJson(intent.input ?? null);
  return { present: true, input, hash: canonicalHash(input) };
}

function childInputFingerprints(children: string[], raw: Json | undefined): Json[] | null {
  if (raw === undefined) return null;
  if (!Array.isArray(raw) || raw.length !== children.length) {
    throw new HttpError(422, "Serialization", "child inputs must align with children");
  }
  let any = false;
  const specs: Json[] = [];
  for (const item of raw) {
    if (!isJsonObject(item) || typeof item.explicitInput !== "boolean") {
      throw new HttpError(422, "Serialization", "child input is invalid");
    }
    if (!item.explicitInput) {
      if ("input" in item || "inputHash" in item) {
        throw new HttpError(422, "Serialization", "input requires an explicit input");
      }
      specs.push({ explicitInput: false });
      continue;
    }
    if (!("input" in item)) throw new HttpError(422, "Serialization", "explicit input is missing");
    any = true;
    const input = validateRelayJson(item.input ?? null);
    specs.push({ explicitInput: true, input, inputHash: canonicalHash(input) });
  }
  return any ? specs : null;
}

function isJsonObject(value: Json | undefined): value is { [key: string]: Json } {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(body: object, field: string): string {
  const value = (body as Record<string, unknown>)[field];
  if (typeof value !== "string" || value.length === 0) {
    throw new HttpError(422, "Serialization", `${field} is required`);
  }
  return value;
}

async function readJson(req: IncomingMessage): Promise<{ [key: string]: Json }> {
  const raw = await readBody(req);
  let parsed: Json;
  try {
    parsed = parseRelayJson(raw);
  } catch (error) {
    if (error instanceof RelayJsonError) throw new HttpError(422, "Serialization", error.message);
    throw error;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new HttpError(422, "Serialization", "JSON object is required");
  }
  canonicalRelayJson(parsed);
  return parsed;
}

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
    size += buffer.length;
    if (size > HTTP_BODY_LIMIT) {
      throw new HttpError(413, "Serialization", "request exceeds 4 MiB");
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function expireDueStreams(pool: ReturnType<typeof createPool>, horizonMs: number): Promise<void> {
  for (let n = 0; n < 32; n += 1) {
    const status = await callJson(
      pool,
      `SELECT relay_expire_streams($1::interval) AS result`,
      [`${horizonMs} milliseconds`],
    );
    if (status !== "expired") return;
  }
}

async function fireDueRetries(pool: ReturnType<typeof createPool>, limit: number): Promise<void> {
  for (let n = 0; n < limit; n += 1) {
    const status = await callJson(pool, `SELECT relay_fire_one_retry() AS result`, []);
    if (status !== "fired") return;
  }
}

async function fireDueTimers(pool: ReturnType<typeof createPool>, limit: number): Promise<void> {
  for (let n = 0; n < limit; n += 1) {
    const status = await callJson(pool, `SELECT relay_fire_one_timer() AS result`, []);
    if (status !== "fired") return;
  }
}

async function reapExpiredLeases(pool: ReturnType<typeof createPool>, limit: number): Promise<void> {
  let expired = 0;
  let tries = 0;
  while (expired < limit && tries < limit + 100) {
    tries += 1;
    const status = await callJson(pool, `SELECT relay_reap_expired_leases(1) AS result`, []);
    if (status === "none") return;
    if (status === "expired") expired += 1;
  }
}

function requirePositiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function requireRecoveryBudget(value: number): number {
  if (!Number.isInteger(value) || value < 1 || value > 2147483647) {
    throw new Error("recoveryBudget must be a positive integer");
  }
  return value;
}

function idempotencyHeader(req: IncomingMessage): string | null {
  const header = req.headers["idempotency-key"];
  if (header === undefined) return null;
  if (typeof header !== "string" || Buffer.byteLength(header) < 1 || Buffer.byteLength(header) > 256) {
    throw new HttpError(422, "Serialization", "Idempotency-Key length is invalid");
  }
  return header;
}

function finalBatchSeq(value: unknown): string {
  if (value === undefined) return "-1";
  if (value === "-1") return "-1";
  return decimalString(value, "finalBatchSeq");
}

function pageLimit(raw: string | null): number {
  const text = raw ?? "100";
  if (!/^[0-9]+$/.test(text)) throw new HttpError(422, "Serialization", "limit must be a positive integer");
  const limit = Number(text);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new HttpError(422, "Serialization", "limit must be from 1 to 1000");
  }
  return limit;
}

function requireNonNegativeInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}
