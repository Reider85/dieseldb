# MVCC Tuple Visibility Rules

This document describes the visibility logic for versioned rows in DieselDB's MVCC implementation (prompt4.md #1).

## Overview

DieselDB currently uses Copy-on-Write table cloning for transaction isolation, which has O(n) overhead per `BEGIN TRANSACTION` on large tables. This foundation introduces row-level versioning with `xmin`/`xmax`/`commandId` fields, enabling lightweight snapshot isolation without table copies.

## Field Semantics

Each `Row` instance carries MVCC metadata:

| Field | Type | Description |
|-------|------|-------------|
| `xmin` | `long` | Transaction ID that created this version; 0 = bootstrap/initial state |
| `xmax` | `long` | Transaction ID that deleted this version; 0 = row is alive |
| `commandId` | `long` | Statement ordinal within the creating transaction; 0 = unused in step 1 |

## Visibility Contract

A row is visible to a transaction T at snapshot S if:

1. **Own insert** — `xmin == currentTxid` → visible (self-write visibility)
2. **Own delete** — `xmax == currentTxid` → invisible (row logically gone for its deleter)
3. **Created after snapshot** — `xmin > snapshotTxid` → invisible
4. **Deleted at/before snapshot by committed tx** — `xmax != 0 && xmax <= snapshotTxid && txCommitted.test(xmax)` → invisible
5. **Insert by uncommitted tx** — `xmin != currentTxid && !txCommitted.test(xmin)` → invisible
6. **Otherwise** → visible

This satisfies the core contract: `xmin > snapshot` → invisible; `xmax <= snapshot && xmax != 0` → invisible; else visible.

## Isolation Level Semantics

| Level | Visibility Rule | Snapshot Point |
|-------|----------------|----------------|
| `READ_UNCOMMITTED` | Always visible (dirty reads allowed) | N/A |
| `READ_COMMITTED` | Sees data committed before statement start | Statement-start snapshot |
| `REPEATABLE_READ` | Sees data committed before transaction start | Transaction-start snapshot |
| `SERIALIZABLE` | Same visibility as REPEATABLE_READ | Transaction-start snapshot |

**Note:** SERIALIZABLE isolation uses the same visibility rules as REPEATABLE_READ; conflict detection (detect/abort) is implemented in prompt4.md #5.

## Test Scenarios (24 total)

The test suite covers 8 cases × 3 isolation levels (READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE):

| # | Case | Row state | currentTxid | snapshot | committed set | Expected |
|---|------|-----------|-------------|----------|---------------|----------|
| 1 | committed insert | xmin=10, xmax=0 | 20 | 15 | {10} | visible |
| 2 | uncommitted insert by other | xmin=11, xmax=0 | 20 | 15 | {10} | invisible |
| 3 | own uncommitted insert | xmin=20, xmax=0 | 20 | 15 | {10} | visible |
| 4 | committed delete | xmin=10, xmax=12 | 20 | 15 | {10,12} | invisible |
| 5 | uncommitted delete by other | xmin=10, xmax=13 | 20 | 15 | {10} | visible |
| 6 | own delete | xmin=10, xmax=20 | 20 | 15 | {10} | invisible |
| 7 | insert after snapshot | xmin=16, xmax=0 | 20 | 15 | {16} | invisible |
| 8 | rolled-back insert | xmin=11, xmax=0 | 20 | 15 | {10} (11 aborted) | invisible |

READ_UNCOMMITTED mirrors cases 2 and 8 as visible (dirty reads allowed).

## CommandId Usage

The `commandId` field is stored and round-trips through serialization. In step 1, it does not affect visibility (own-write visibility is required by test scenarios). It is reserved for:

- Statement-level snapshots within a transaction (future cursor semantics)
- Detecting changes within a single statement (step 2+)

## Runtime MVCC (prompt4.md #2 — implemented)

Step 2 replaces per-`BEGIN` table cloning with row-version metadata plus an undo log. The
visibility rules above are now evaluated at runtime, not just in tests.

### Row version sidecar

Per-row MVCC state lives in `Table.rowVersions` (`Map<Integer, RowVersionMeta>`, transient —
not persisted), keyed by physical row index. `RowVersionMeta` is `Serializable` (spill
serialization) and carries `xmin`/`xmax` plus flags (`uncommittedInsert`, `uncommittedDelete`,
`uncommittedUpdate`, `aborted`). `Table.markInsert`/`markDelete`/`markUpdate` maintain it.
Bootstrap rows have no meta: absence of a meta is treated as `{xmin=0, xmax=0}` (committed,
alive).

### TransactionTableSnapshot

`Transaction.getSnapshot(table)` returns a lazy `TransactionTableSnapshot` view — no
`cloneTable()`, O(1) per `BEGIN`. Visibility dispatches to `TupleVisibility` with the
transaction's snapshot txid and committed-set; bootstrap rows are resolved through
`Table.getVisibleRowForReader(int, Map)` so the meta map is optional. Own deletes
(`xmax == currentTxid`, including the bootstrap branch where `meta.getXmax() == currentTxid`)
hide the row for its deleter.

### UndoLog and ROLLBACK

Every MVCC-logged mutation appends an undo record (`InsertUndo`, `UpdateUndo`, `DeleteUndo`)
to the transaction's `UndoLog`. Memory usage is bounded by `undo.spill.threshold.mb`
(config, default 1; constructor takes **bytes**). Above the threshold the log spills to a
temp file (`diesel-undo-*.log`, created via `Files.createTempFile`); the file uses trailer
framing — `[data][4-byte length]` per record — so `applySpilledRecordsReverse` can walk it
backwards. ROLLBACK applies records in reverse order (memory first, then spilled file from
the end) and calls `RowVersionMeta.markAborted()` on aborted inserts, then the SQL
`ROLLBACK` statement sets auto-commit back on.

### Hybrid isolation policy (INSERT vs UPDATE/DELETE) — step 2, replaced by step 4

Step 2 used a hybrid: `INSERT` in-place MVCC, `UPDATE`/`DELETE` copy-on-write with
version swap at COMMIT. **prompt4.md #4 removed the hybrid** — UPDATE/DELETE now run
in-place MVCC like INSERT (see the step 4 section below). The description below is
kept for historical reference only.

| Statement | Mechanism (step 2) | Why (step 2) |
|-----------|-----------|-----|
| `INSERT` | in-place MVCC: physical append + `markInsert`; invisible to others until COMMIT | O(1), no table copy |
| `UPDATE` / `DELETE` | copy-on-write table copy (pre-step-2 behaviour), version swap at COMMIT | preserves commit-time conflict detection (`checkCommitConflicts`) until prompt4 step 4 adapts DML |

COMMIT: the undo log is discarded (records no longer needed), pending CoW copies publish
with conflict checks, and `rowVersions` entries for the transaction's inserts become
committed.

### Known limitations (step 2; items marked ✅ fixed in step 4)

- **INSERT + UPDATE in one transaction:** ✅ fixed by prompt4 #4 — UPDATE no longer
  replaces the shared entry with a CoW copy; `markUpdate` + undo records keep the
  version meta intact, rollback restores the pre-image.
- `insertIntoClusteredPosition` (non-monotonic primary keys) does not shift `rowVersions`
  keys for rows after the insertion point, so metas can point at the wrong row for
  out-of-order inserts.
- `rowVersions` is transient: MVCC state is lost across JVM restarts (undo log is
  per-transaction in-memory/temp-file anyway).
- Commit-time `saveToFile` can persist rows from other uncommitted transactions sharing the
  table (pre-existing write-behind behaviour, unchanged).

## Runtime MVCC (prompt4.md #4 — implemented)

Step 4 adapts SELECT/INSERT/UPDATE/DELETE to the row-version model and replaces the
step-2 CoW path for UPDATE/DELETE.

### Shadow-model UPDATE

`Table.markUpdate(rowIndex, txid, oldValues)` stamps the meta with
`uncommittedUpdate`/`ownerTxid` and retains the pre-image in `committedValues`. The
physical row always holds its owner's latest values; readers see the retained pre-image
(`RowVersionMeta.getCommittedValues()`) when the physical values are not theirs to see —
a pending change by another transaction, or a commit that landed after their snapshot
(`Table.getVisibleRowForReader`). On COMMIT, `markRowCommitted` clears the flags and
advances `lastCommittedCsn` (pre-image retained for older snapshots); on ROLLBACK,
`markRowRolledBack` clears them and the undo log restores the pre-image values.

### Optimistic write-write conflicts

Concurrent writers of the same row fail fast at the writer instead of at COMMIT:
`Table.checkWriteWriteConflict(rowIndex, txid, snapshotCsn)` throws
`TransactionException` when another transaction holds a pending change on the row
(`ownerTxid != 0 && ownerTxid != current`), or a committed change landed after the
writer's snapshot (`lastCommittedCsn > snapshotCsn`). INSERT/UPDATE/DELETE all call it
before mutating; unique secondary-key conflicts additionally reject duplicate pending
keys owned by others (row-level check) while committed duplicates are handled by
keep-index DELETE below.

### READ COMMITTED statement snapshot

`Database.dispatch` calls `refreshReadCommittedSnapshot` per statement: an active
`READ_COMMITTED` transaction's snapshot CSN advances to the current commit CSN at each
statement start (batch transactions and other levels keep the BEGIN-time snapshot).
`TxStatusTracker` provides `isCommittedBefore(txid, snapshotCsn)`; visibility decisions
delegate to `TupleVisibility.visibleByStatus` (status-based: txid space for creators/
deleters, commit-CSN space for snapshot membership).

### Keep-index DELETE + unique-slot eviction

DELETE no longer removes rows physically at statement time. It stamps an MVCC tombstone
(`xmax`/`ownerTxid`) and keeps the row in place so old snapshots still resolve
pre-images; index entries for the tombstoned row are removed (`Table.deleteRow` →
`syncIndexDelete`) — the unique slot becomes free at DELETE time, not COMMIT. Re-inserting
the same unique key after a committed DELETE succeeds; `Table.evictDeadUniqueEntries`
cleans committed-but-not-yet-vacuumed entries. Vacuum removes tombstoned rows
unconditionally only past the vacuum horizon (`VacuumManager.isDead`, horizon =
`computeVacuumHorizonCsn`); readers whose snapshot predates the delete still see the
row (`isRowVisibleToReader` tombstone branch). See `vacuum.md`.

### Read paths

`SelectQuery` scans filter every row through `Table.isRowVisibleToReader` +
`getVisibleRowForReader` (mirror fast path — `Table.updateRowInPlace` keeps the `rows`
mirror in sync with the storage backend; `getLiveRows` iterates shared maps, index
lookups use O(1) `readPhysicalRow`). Auto-commit readers and readers without MVCC meta
keep the pre-MVCC fast paths; explicit-transaction readers get the full status-based
visibility rules above.

## Forward Pointers

This foundation is step 1 of 5 for MVCC in prompt4.md #1:

1. ✅ **Row versioning container** (this step) — `xmin`/`xmax`/`commandId` + visibility logic
2. ✅ **Undo log + TransactionTableSnapshot** (prompt4.md #2) — no cloning; hybrid
   INSERT=MVCC / UPDATE+DELETE=CoW until step 4
3. ✅ **Vacuum Manager** (prompt4.md #3) — background cleanup of dead versions, horizon-aware
4. ✅ **DML adaptation** (prompt4.md #4) — SELECT/INSERT/UPDATE/DELETE on row versions,
   optimistic write-write conflicts, keep-index DELETE, RC statement snapshots
5. 📋 **SERIALIZABLE conflict detection** (prompt4.md #5) — detect rw-conflicts and abort victims

## Serialization

All fields are included in serialization (restart-stable). The `Row` class implements `Serializable` and defensive copying ensures immutability of column values.

---

*Generated by prompt4.md #1 implementation; runtime contract section added by prompt4.md #2; step-4 section (shadow-model UPDATE, optimistic conflicts, keep-index DELETE, RC snapshots) added by prompt4.md #4*