# Capacity

These numbers were measured on this machine before redrive, dashboard controls, a history cache, or blobs. They describe that engine. They are not a throughput promise.

## Machine

- Recorded: 2026-10-30T22:45:08.430Z
- Commit: 97cf17326960396768863e364f66c042866e0140
- Node: v22.17.0
- Hardware: 16 x 12th Gen Intel(R) Core(TM) i7-12650H, 16085 MiB RAM, win32 10.0.26300
- Postgres: PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
- Server pool: 8 connections (`packages/server/src/db.ts`).
- Lease: 120 seconds. Heartbeat: 10 seconds. Reaper and timer loops were off for this command.
- Run ladder concurrency: 4 in-process activations.
- Reproduction: `npm run capacity`. This command is not part of `npm test`. The 100-run failure demo remains `tests/demo.test.ts`.

## Workload

Replay climbs one run through 10, 100, 1,000, and 10,000 sequential steps. Each step returns its index and emits one character. The first activation runs the callbacks and does not finish the run. Catch-up reads the stream rows that activation stored. The lease is then released, and the second activation replays the journal and finishes the run.

The run ladder creates 100, then 1,000, then 10,000 one-step runs and activates them four at a time. Activation time includes register, lease, the step, and finish. It is not a separate lease-only rate.

A level that does not finish inside its budget is not measured. Later levels of that ladder are not started.

Stream catch-up is the public SSE read. The server drops that connection when 32 frames are queued. A longer stream can fail catch-up while journal replay still finishes.

## Replay

| Commands | First activation ms | Replay ms | History bytes | Bytes per command | Stream rows | Catch-up ms | Replay callbacks | Max connections | Postgres CPU |
|---|---|---|---|---|---|---|---|---|---|
| 10 | 337.0 | 34.9 | 3581 | 358.1 | 20 | 23.8 | 0 | 4 | one sample after the level 0.02% |
| 100 | 2754.8 | 38.2 | 32703 | 327.0 | 200 | not measured (fetch failed: other side closed) | 0 | 4 | max 51.61% across 12 docker stats samples |
| 1000 | 23218.4 | 83.5 | 328405 | 328.4 | 2000 | not measured (fetch failed: other side closed) | 0 | 5 | max 36.74% across 92 docker stats samples |
| 10000 | 201154.4 | 892.7 | 3330407 | 333.0 | 20000 | not measured (fetch failed: other side closed) | 0 | 6 | max 93.56% across 808 docker stats samples |

## Concurrent runs

| Runs | Activation p50 ms | p95 ms | p99 ms | Wall ms | Activations per second | History bytes | Max connections | Postgres CPU |
|---|---|---|---|---|---|---|---|---|
| 100 | 73.9 | 148.3 | 179.6 | 2067.3 | 48.37 | 79381 | 7 | max 103.37% across 8 docker stats samples |
| 1000 | 66.3 | 93.7 | 109.6 | 17298.8 | 57.81 | 795772 | 8 | max 165.65% across 98 docker stats samples |
| 10000 | 65.3 | 91.2 | 116.5 | 171213.1 | 58.41 | 7977798 | 8 | max 220.49% across 904 docker stats samples |

## What the run showed

Every replay and run-ladder level finished inside its budget. Replay did not call the step function again.

Stream catch-up completed for the 10-command history. At 100, 1,000, and 10,000 commands the SSE connection closed before the last stream row, so those catch-up times are not measured.

Connection counts include the admin connection used to sample `pg_stat_activity`. CPU is a `docker stats` sample of the `relay-m1-pg` container during that level. A level with no sample says not readable.

External effects are still at least once. This command does not change that. A lower-priority run can still wait behind a higher priority. The JSON body limit is unchanged.
