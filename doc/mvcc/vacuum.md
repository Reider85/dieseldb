# MVCC Vacuum — Dead Version Reclamation

This document describes the vacuum subsystem introduced in prompt4.md #3 (ROADMAP3 R3-001): how DieselDB reclaims row versions that no transaction can ever observe again.

## Overview

Two kinds of dead weight accumulate in a table:

1. **Tombstones** — rows removed by a committed `DELETE` stay physically present in the raw row list (bit set in `deletedRows`).
2. **MVCC-dead versions** — rows whose creating transaction aborted (undo cleared the uncommitted flags) or whose deleting transaction committed below the vacuum horizon.

`VACUUM` finds both kinds, marks them, and physically removes them in one compaction, rebuilding every index and remapping the MVCC metadata.

## SQL Interface

| Statement | Effect |
|-----------|--------|
| `VACUUM` | Vacuums every registered table |
| `VACUUM table_name` | Vacuums a single table |
| `VACUUM TABLE table_name` | Same, `TABLE` keyword accepted |

- Trailing `;` is stripped before parsing; unknown tables raise the standard `Table not found` error.
- Result messages:
  - single table: `VACUUM <name>: <n> dead tuples removed in <ms> ms (<p> pass(es))`
  - all tables: `VACUUM: <t> table(s), <n> dead tuples removed in <ms> ms`
- After a vacuum the database query cache is invalidated (`queryCache.invalidateAll()`), so stale plan/materialization results cannot survive a physical compaction.

## Dead-Row Predicate

Evaluated per raw row position in `VacuumManager.isDead`, in order:

| # | Condition | Verdict |
|---|-----------|---------|
| 1 | `Table.isDeleted(pos)` — tombstoned | **dead** |
| 2 | `meta == null` (auto-commit / bulk rows carry no MVCC metadata) | alive |
| 3 | `meta.hasUncommittedChanges()` | alive (conservative) |
| 4 | `xmin != 0` and tracker status of `xmin` is `ABORTED` | **dead** |
| 5 | `xmax != 0` and `xmax` committed at/below the vacuum horizon | **dead** |
| — | otherwise | alive |

Unknown or uncommitted state always means *alive* — the vacuum never guesses.

## Vacuum Horizon

`Database.computeVacuumHorizonCsn()` returns the minimum `snapshotCsn` of every **active, non-batch** transaction, or `Long.MAX_VALUE` when none exist.

- **Batch transactions are excluded**: their snapshot CSN is `0`, which would otherwise pin the horizon at the beginning of time.
- **Deviation from prompt4.md**: the prompt specifies a *txid*-based horizon (`min(snapshotTxid)`). DieselDB uses a **CSN** (commit sequence number) instead, because commit order does not equal txid order — visibility in this codebase is defined in CSN space, so the horizon must be expressed in the same currency. Using txids could reclaim rows a concurrent reader still observes.

## Algorithm

Each vacuum attempt runs up to **3 passes** (`MAX_PASSES`); a pass either completes *stably* or is discarded and retried.

**Mark phase (batched):** the raw row list is scanned in `vacuum.batch.size` batches (default 10 000). Each batch runs under the table `tableLock` write lock:

1. re-validate the structural stamp (`getRawRowCount()` unchanged) before and after marking,
2. evaluate the predicate and mark dead positions (`markDeleted`, drop their `RowVersionMeta`, add to `pending`),
3. release the lock — writers interleave freely between batches.

An unstable stamp aborts the whole pass (`removed = 0`); marks are safe to discard because the predicate is monotone (it only ever learns more).

**Final phase (one write lock):**

1. re-validate `rawRowCount` and re-check every pending position (`isDeleted` must still hold),
2. `storage.removeDeadEntries(pending)` — disassociates secondary index entries *without* moving row positions, without touching the bulk-mode mirror, and without adding to `deletedRowIds` (which would trigger the manager's auto-compaction),
3. `Table.compact()` — physically removes the rows, rebuilds all indexes, and remaps MVCC metadata (`rebuildRowVersionsAfterCompact` runs **before** the `deletedRowIds` bit set is reset, so remapping sees the surviving rows),
4. `removed = rawRowCountBefore − rawRowCountAfter`.

`removeDeadEntries` deliberately runs **before** `compact()`: its O(N) scan over `rowIdToPosition` needs valid positions, and `compact()` performs a full reindex anyway.

**Writer blocking.** The only windows in which writers contend with the vacuum are a single batch (bounded by `vacuum.batch.size`) and the final compact. No global database lock is ever taken. Auto-commit writer latency is additionally decoupled from whole-file storage rewrites by the background persist flusher (`diesel.persist.background=true`, `diesel.persist.flush.interval.ms=50`): INSERT measures parse+insert; file IO runs on `diesel-persist-flusher` from a row snapshot. The acceptance test measures writer latency concurrently with a vacuum on another table and asserts every non-interference operation stays below 100 ms (operations that overlap a JVM GC or exceed the no-vacuum baseline are reported separately).

## Configuration

| Key | Default | Meaning |
|-----|---------|---------|
| `vacuum.interval.ms` | `60000` | Auto-vacuum period; non-positive disables scheduling |
| `vacuum.batch.size` | `10000` | Rows scanned per write-lock batch |

Resolution order: **system property first** (test override, same pattern as the profiler's slow-threshold), then `config.properties`.

## Auto-Vacuum Lifecycle

- Started only by `DatabaseServer.start()` (or explicitly by tests via `startAutoVacuum()`); the `Database` constructor does **not** start it.
- Daemon thread `diesel-vacuum` sleeps for a full interval before the first run, then loops `vacuumAll()`; exceptions are logged and the loop continues.
- `stop()` interrupts the thread, joins it (2 s), and unregisters the MBean. It is called from `Database.close()` (first, before table shutdown) and `DatabaseServer.stop()`.

## Metrics (JMX)

`DynamicMBean`, registered lazily on the first vacuum run (or auto-vacuum start):

- Object name: `diesel:type=VacuumManager,id=N` — the sequence suffix keeps multiple database instances in one JVM independent.
- Unregistered by `stop()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `vacuum.duration.ms` | long | Wall time of the most recent run |
| `vacuum.dead_tuples_removed` | long | Cumulative physically removed tuples |
| `vacuum.runs` | long | Completed vacuum runs |
| `vacuum.lastRunEpochMs` | long | Epoch ms of the most recent run |

Attributes are read-only; `invoke` is unsupported.

## Known Limitations

- **Transient `rowVersions`**: MVCC metadata lives only in memory. After a restart, `meta == null` for surviving rows, so aborted-insert versions are no longer detectable — only tombstones are reclaimed. (Tombstones persist on disk.)
- **Stale uncommitted flags**: a committed explicit-transaction insert whose `uncommittedInsert` flag never cleared is treated conservatively alive (rule 3). It can only be reclaimed as a tombstone.
- **Batch transactions pin nothing, but their rows are kept**: batch txns are outside the horizon calculation; rows they touched are not judged dead through rule 5 while they run.
- **Concurrent churn**: if a writer changes the table's structure mid-pass, the pass is discarded (up to 3 attempts), and the vacuum yields with a log line. No correctness impact — marks are conservative.
- **Pre-existing DELETE races**: the bulk-DELETE window (row index vs. concurrent compact) predates the vacuum and is unchanged by it.
- **DELETE cost is unrelated**: bulk `DELETE ... WHERE` persist time on million-row tables is dominated by whole-file TSV rewrites, not by the vacuum.

## Tests

| Test | Profile | Covers |
|------|---------|--------|
| `TxStatusTrackerTest` | fast | Oldest-active-txid horizon fix |
| `VacuumManagerTest` | fast | Parsing, tombstone/aborted cleanup, JMX attributes, bare `VACUUM`, unknown table, auto-vacuum scheduling + MBean unregister |
| `VacuumTest#vacuumReclaimsAtLeastThirtyPercentOfHeapOnMillionRowTable` | large | Acceptance #1: 1M inserts + 600k aborted + 100k deletes → ≥30% heap drop (MemoryMXBean, 3× GC), 900k live rows survive, JMX counter ≥ 700 000 |
| `VacuumTest#vacuumDoesNotBlockConcurrentWritersBeyondOneHundredMilliseconds` | large | Acceptance #2: concurrent writers on a second table, baseline (no vacuum) vs vacuum window; clean (non-GC, non-baseline-exceeding) max latency &lt; 100 ms. Persist runs on the background flusher so measured ops are parse+insert. Operations that overlap a JVM GC pause or exceed the measured no-vacuum baseline are counted and reported separately — JVM-wide background work that no lock discipline can prevent |
