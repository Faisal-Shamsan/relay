import type { IncomingMessage, ServerResponse } from "node:http";
import pg from "pg";
import type { Json } from "@relay/protocol";
import { withTransaction } from "./db.js";
import { decimalString, HttpError } from "./errors.js";

const CHANNEL = "relay_stream";

interface StreamRow {
  seq: string;
  kind: string;
  stepKey: string | null;
  attempt: number | null;
  batchSeq: string | null;
  payload: Json;
}

interface Subscriber {
  runId: string;
  cursor: string;
  dirty: boolean;
  closed: boolean;
  inflight: boolean;
  pending: string[];
  response: ServerResponse;
  wake: () => void;
  onIdle: (() => void) | null;
}

export interface StreamHub {
  close(): Promise<void>;
  serve(req: IncomingMessage, res: ServerResponse, runId: string, url: URL, tenantId: string): Promise<void>;
  suppressNotifications(): void;
}

export async function createStreamHub(options: {
  appDatabaseUrl: string;
  pool: pg.Pool;
  pollMs: number;
  keepaliveMs: number;
  queueLimit: number;
  beforeCatchup?: (runId: string) => Promise<void>;
}): Promise<StreamHub> {
  const listeners = new Map<string, Set<Subscriber>>();
  const stops = new Set<() => void>();
  const listenClient = new pg.Client({ connectionString: options.appDatabaseUrl });
  let suppressed = false;
  let closed = false;

  await listenClient.connect();
  await listenClient.query(`LISTEN ${CHANNEL}`);
  listenClient.on("notification", (message) => {
    if (suppressed || message.channel !== CHANNEL || !message.payload) return;
    const group = listeners.get(message.payload);
    if (!group) return;
    for (const subscriber of group) {
      subscriber.dirty = true;
      subscriber.wake();
    }
  });

  function remove(subscriber: Subscriber): void {
    const group = listeners.get(subscriber.runId);
    if (!group) return;
    group.delete(subscriber);
    if (group.size === 0) listeners.delete(subscriber.runId);
  }

  return {
    suppressNotifications() {
      suppressed = true;
    },
    async close() {
      closed = true;
      for (const stopSubscriber of [...stops]) stopSubscriber();
      await listenClient.end();
    },
    async serve(req, res, runId, url, tenantId) {
      if (closed) throw new HttpError(404, "InvalidRunState", "run not found");
      const cursor = streamCursor(req, url);
      const opened = await loadSnapshot(options.pool, runId, cursor, tenantId);
      if (!opened.found) throw new HttpError(404, "InvalidRunState", "run not found");
      if (opened.expired) {
        throw new HttpError(410, "StreamExpired", "stream history has expired", expiredDetails(runId, opened));
      }
      if (BigInt(cursor) > BigInt(opened.high)) {
        throw new HttpError(409, "StreamCursor", "stream cursor is past the log");
      }

      let wake = (): void => undefined;
      const subscriber: Subscriber = {
        runId,
        cursor,
        dirty: false,
        closed: false,
        inflight: false,
        pending: [],
        response: res,
        wake: () => wake(),
        onIdle: null,
      };
      const group = listeners.get(runId) ?? new Set<Subscriber>();
      group.add(subscriber);
      listeners.set(runId, group);
      const keepalive = setInterval(() => writeKeepalive(subscriber), options.keepaliveMs);
      keepalive.unref();
      let headersSent = false;
      const stopSubscriber = (): void => {
        if (subscriber.closed) return;
        clearInterval(keepalive);
        subscriber.closed = true;
        subscriber.onIdle?.();
        subscriber.onIdle = null;
        wake();
        remove(subscriber);
        stops.delete(stopSubscriber);
        if (!res.writableEnded) res.destroy();
      };
      stops.add(stopSubscriber);
      res.on("close", stopSubscriber);
      res.on("error", stopSubscriber);

      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
      });
      headersSent = true;
      try {
        await options.beforeCatchup?.(runId);
        while (!subscriber.closed) {
          subscriber.dirty = false;
          const page = await loadSnapshot(options.pool, runId, subscriber.cursor, tenantId);
          if (subscriber.closed) return;
          if (page.expired) {
            const notice = `event: expired\ndata: ${JSON.stringify({
              code: "StreamExpired",
              message: "stream history has expired",
              details: expiredDetails(runId, page),
            })}\n\n`;
            await new Promise<void>((resolve) => {
              subscriber.onIdle = resolve;
              subscriber.pending.push(notice);
              pump(subscriber, stopSubscriber);
            });
            if (!subscriber.closed && !res.writableEnded) res.end();
            return;
          }
          for (const row of page.rows) {
            if (!enqueue(subscriber, frame(row), options.queueLimit, stopSubscriber)) return;
            subscriber.cursor = row.seq;
          }
          if (subscriber.dirty || subscriber.closed) continue;
          await new Promise<void>((resolve) => {
            const timer = setTimeout(finish, options.pollMs);
            wake = () => finish();
            function finish(): void {
              clearTimeout(timer);
              wake = () => undefined;
              resolve();
            }
          });
        }
      } catch (error) {
        if (!headersSent) throw error;
      } finally {
        stopSubscriber();
      }
    },
  };
}

function streamCursor(req: IncomingMessage, url: URL): string {
  const header = req.headers["last-event-id"];
  const query = url.searchParams.get("after");
  const headerCursor = header === undefined || header === "" ? null : header;
  if (Array.isArray(headerCursor)) {
    throw new HttpError(422, "Serialization", "Last-Event-ID must be one value");
  }
  if (headerCursor !== null && query !== null && headerCursor !== query) {
    throw new HttpError(422, "Serialization", "Last-Event-ID and after disagree");
  }
  const selected = headerCursor ?? query ?? "0";
  return decimalString(selected, headerCursor === null ? "after" : "Last-Event-ID");
}

interface Snapshot {
  found: boolean;
  expired: boolean;
  state: string;
  output: Json | null;
  error: Json | null;
  high: string;
  rows: StreamRow[];
}

interface SnapshotRow {
  expired: boolean;
  state: string;
  output: Json | null;
  error: Json | null;
  high: string;
  seq: string | null;
  kind: string | null;
  stepKey: string | null;
  attempt: number | null;
  batchSeq: string | null;
  payload: Json | null;
}

async function loadSnapshot(pool: pg.Pool, runId: string, cursor: string, tenantId: string): Promise<Snapshot> {
  const result = await withTransaction(pool, async (client) => client.query<SnapshotRow>(
    `SELECT (r.stream_expired_at IS NOT NULL) AS expired,
            r.state,
            r.output,
            r.error,
            COALESCE((SELECT max(seq) FROM run_stream_events m WHERE m.run_id = r.id), 0)::text AS high,
            e.seq::text AS seq,
            e.kind,
            e.step_key AS "stepKey",
            e.attempt,
            e.batch_seq::text AS "batchSeq",
            e.payload
     FROM runs r
     LEFT JOIN run_stream_events e
       ON e.run_id = r.id
      AND r.stream_expired_at IS NULL
      AND e.seq > $3::bigint
     WHERE r.id = $1 AND r.tenant_id = $2
     ORDER BY e.seq`,
    [runId, tenantId, cursor],
  ));
  const first = result.rows[0];
  if (!first) return { found: false, expired: false, state: "", output: null, error: null, high: "0", rows: [] };
  const rows = result.rows.flatMap((row) => {
    if (row.seq === null || row.kind === null || row.payload === null) return [];
    return [{
      seq: row.seq,
      kind: row.kind,
      stepKey: row.stepKey,
      attempt: row.attempt,
      batchSeq: row.batchSeq,
      payload: row.payload,
    }];
  });
  return {
    found: true,
    expired: first.expired,
    state: first.state,
    output: first.output,
    error: first.error,
    high: first.high,
    rows,
  };
}

function expiredDetails(runId: string, page: Snapshot): Record<string, unknown> {
  return { runId, state: page.state, output: page.output, error: page.error };
}

function frame(row: StreamRow): string {
  return `id: ${row.seq}\nevent: ${row.kind}\ndata: ${JSON.stringify(eventData(row))}\n\n`;
}

function eventData(row: StreamRow): Json {
  const payload = row.payload;
  if (row.kind === "chunk" && isObject(payload)) {
    return {
      stepKey: row.stepKey,
      attempt: row.attempt,
      batchSeq: row.batchSeq,
      text: typeof payload.text === "string" ? payload.text : "",
    };
  }
  return isObject(payload) ? payload : {};
}

function enqueue(subscriber: Subscriber, next: string, limit: number, stop: () => void): boolean {
  if (subscriber.closed) return false;
  const waiting = subscriber.pending.length + (subscriber.inflight || subscriber.response.writableLength > 0 ? 1 : 0);
  if (waiting >= limit) {
    stop();
    return false;
  }
  subscriber.pending.push(next);
  pump(subscriber, stop);
  return true;
}

function pump(subscriber: Subscriber, stop: () => void): void {
  if (subscriber.closed || subscriber.inflight) return;
  if (subscriber.pending.length === 0) {
    const idle = subscriber.onIdle;
    subscriber.onIdle = null;
    idle?.();
    return;
  }
  const next = subscriber.pending.shift();
  if (next === undefined) return;
  subscriber.inflight = true;
  const accepted = subscriber.response.write(next);
  const release = (): void => {
    subscriber.inflight = false;
    if (!subscriber.closed) pump(subscriber, stop);
  };
  if (accepted) setImmediate(release);
  else subscriber.response.once("drain", release);
}

function writeKeepalive(subscriber: Subscriber): void {
  if (subscriber.closed || subscriber.inflight || subscriber.pending.length > 0) return;
  subscriber.response.write(": keepalive\n\n");
}

function isObject(value: Json): value is { [key: string]: Json } {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
