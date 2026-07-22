# Snapshot-conflict analysis & plan: DuckLake commits on a Postgres catalog

**Status:** ANALYSIS — phases 2/3 not implemented
**Date:** 2026-07-22
**Context:** Leen prod sees snapshot-conflict retries/failures under concurrent worker commits.
Related: `pg-server-side-metadata-pr2-pr4.md` (PR2–PR4), PR5 (catalog-load + conflict-path
server-side reads, commit on `pg-server-side-metadata-v1.5`).

## 1. How DuckLake commits, and why conflicts happen without logical conflicts

Client commit loop (`src/storage/ducklake_transaction_state.cpp` `Commit`, called from
`RunCommitLoop` in `ducklake_transaction.cpp`):

1. Attempt 0: read the latest snapshot (`GetLatestSnapshotQuery`, already server-side on PG),
   take `snapshot_id + 1`, build the whole commit as one SQL batch (insert snapshot row, data
   files, stats updates, `ducklake_snapshot_changes` row), execute, `COMMIT` the PG transaction.
2. The arbiter is the **primary key on `ducklake_snapshot.snapshot_id`**: two committers that
   read the same latest snapshot both try to insert id N+1; the loser gets a PK violation,
   rolls back, and retries.
3. Retry attempt i>0: `CheckForConflicts` runs `GetSnapshotAndStatsAndChanges` (latest snapshot +
   all-tables stats + `STRING_AGG` of `changes_made` since our snapshot), parses other
   transactions' change sets, and applies the compatibility matrix (`ConflictCheck`): appends to
   the same table are compatible; delete-vs-compact, delete-vs-delete-same-file, schema-change-vs-
   anything conflict and abort. If deletes overlap it additionally runs
   `GetFilesDeletedOrDroppedAfterSnapshot`. Then it re-attempts with the new snapshot id after a
   jittered exponential backoff:
   `sleep = retry_wait_ms * rand[0.5,1.0] * retry_backoff^i`.

Key consequence: **every concurrent committer except one pays a retry per round, even when all
changes are logically compatible.** With n workers committing simultaneously, ~O(n²) attempts.
"Snapshot conflict" errors surface only when `max_retry_count` (default 10) is exhausted or a
real incompatibility exists.

Settings (per DuckDB connection): `ducklake_max_retry_count` = 10, `ducklake_retry_wait_ms` =
100, `ducklake_retry_backoff` = 1.5 (`ducklake_transaction.hpp:159`).

### What PR5 already changed

Pre-PR5, every retry attempt full-scanned `ducklake_table_stats` ⋈ `ducklake_table_column_stats`
(and on delete overlap, `ducklake_data_file` + `ducklake_delete_file`) over the attached
catalog — hundreds of ms per attempt on bloated heaps, which widened the race window and made
collisions self-amplifying. Post-PR5 those queries run server-side with predicates in Postgres
(a few ms each). Expect materially fewer retries from the shorter window; measure before
building more (see §3).

### Server-side commit exists upstream — but not for Postgres

v1.5 has a full server-side commit path (`ducklake_server_side_commit.cpp`: stage changes, then
apply staged data + conflict re-check in one server round trip). It is only wired up for the
"quack" backend: `QuackMetadataManager` sets `retrials_server_side` and implements
`FlushChangesServerSide`; the base class throws (`ducklake_metadata_manager.cpp:79`). On PG the
client loop is always used.

## 2. Options

**A. App-level: fewer, bigger commits (no fork change; biggest lever).**
Each DuckLake `COMMIT` is a catalog round trip and a collision candidate. Batch writes per
worker (accumulate rows, one commit per flush interval instead of per activity), and cap
concurrent committers per catalog via Temporal task-queue concurrency / a worker-side semaphore.
Appends to the *same* table don't conflict logically, so serialization is purely about collision
efficiency, not correctness.

**B. Retry tuning (config only).**
Post-PR5 an attempt is cheap, so favor more, faster retries:
`SET ducklake_max_retry_count = 30; SET ducklake_retry_wait_ms = 25;` (keep backoff 1.5,
jitter is built in). Set alongside the existing connection setup in `leen_ducklake`.
This converts "conflict" errors into slightly slower commits.

**C. Fork, small: serialize committers with a Postgres advisory lock.**
Take `pg_advisory_xact_lock(<catalog-scoped key>)` as the first statement of the commit's PG
transaction (PG-manager-specific prologue to `execute_commit_batch`, before the latest-snapshot
read on each attempt). Committers then queue FIFO on the PG side instead of racing: the PK
collision path becomes unreachable for same-catalog commits, retries only remain for
transactions that overlapped before the lock. Lock releases automatically at PG commit/rollback
(xact-scoped → no leak on crash). Cost: commits serialize (~they already do, at the snapshot
counter) and one extra statement; risk: low; effort: small (one override + a test).
Make it an ATTACH/setting opt-in (e.g. `ducklake_pg_commit_lock = true`) to stay
upstreamable.

**D. Fork, large: port server-side commit to the PG backend.**
Implement `PostgresMetadataManager::FlushChangesServerSide` for data-only commits
(`IsDataOnlyCommit`), reusing the staged-commit machinery + `DuckLakeServerSideCommit` SQL via
`postgres_query`/a server-side function. Collapses the entire retry loop into one PG
transaction: no client round trips between snapshot read and snapshot insert → conflict window
≈ 0. This is where upstream (quack/MotherDuck) is heading; check whether upstream grows PG
support before building it. Effort: large (weeks, careful dialect work); highest payoff.

## 3. Measurement (do this first, after PR5 r2 + maintenance land)

- Retry rate ≈ `pg_stat_statements` calls of the server-side conflict query (the
  `STRING_AGG(changes_made...)` COPY) — each call is one retry attempt fleet-wide.
- Collision rate ≈ PK-violation errors on `ducklake_snapshot` in PG logs
  (`duplicate key value violates unique constraint`).
- App-side: count commit errors containing "Exceeded the maximum retry count".

## 4. Recommended sequence

1. Measure for a few days with PR5 (r2) + compacted catalog + retry tuning (B) in place.
2. In parallel, do (A) where cheap — batch flushes in the highest-frequency writers.
3. If retries still hurt: implement (C) — small, safe, and mostly eliminates the O(n²) races.
4. Only if commit latency/conflicts remain a top problem: scope (D), first checking upstream
   progress on PG server-side commits.
