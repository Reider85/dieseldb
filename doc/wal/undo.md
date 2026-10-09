# ARIES Undo Phase (prompt 4 #19, R3-004 step 4/4)

Reverses the logical DML records of every transaction that was still active
when the engine crashed, so uncommitted work is invisible again after the
restart. Completes ARIES: analysis (#17) → redo (#18) → undo (#19), orchestrated
by `ARIESAlgorithm` and driven at startup by `RecoveryManager`.

## Components

| File | Role |
|------|------|
| `diesel/recovery/UndoPhase.java` | Reverse-LSN scan delivering logical undos to the sink |
| `diesel/recovery/UndoResult.java` | Immutable bucket counters of one undo pass |
| `diesel/recovery/MvccUndoSink.java` | Callback interface (insert/update/delete undo) |
| `diesel/recovery/ARIESAlgorithm.java` | analysis → redo → undo orchestration |
| `diesel/recovery/RecoveryResult.java` | Combined result of the three phases |
| `diesel/DatabaseRecoverySink.java` | Production sink: restores MVCC state on the Database |
| `diesel/RecoveryManager.java` | Startup orchestrator + `recovery.duration.ms` JMX metric |
| `diesel/wal/DmlPayload.java` | Codec for logical INSERT/UPDATE/DELETE records |
| `diesel/TxStatusTracker.java` | `registerRecoveredCommitted/Abort`, counter flooring |

## Engine-side WAL logging (prerequisite)

When `wal.enabled=true`, explicit MVCC transactions append logical DML records:

- `BEGIN` → `executeBeginTransaction` (policy-aware coordinator path);
- `INSERT` → after-image `DmlPayload(table, rowIndex, values)` (`InsertQuery`);
- `UPDATE` → before + after images (`UpdateQuery.markRowsUpdated`);
- `DELETE` → before-image (`DeleteQuery.markRowsDeleted`);
- `ABORT` → `executeRollback` (a cleanly rolled-back tx is not undone again);
- `COMMIT` → unchanged since #18 (payload = modified/deleted row indexes + CSN).

DML appends are non-blocking enqueues into the writer's FIFO queue — successive
puts from one thread are ordered, so per-transaction program order holds; the
statement does not wait, and COMMIT still blocks for durability behind them.
Auto-commit fast-path and batch-mode inserts stay unlogged (their durability is
the existing synchronous persist).

## Algorithm

1. **Active set** comes from `AnalysisPhase` (a txid with DML/`BEGIN` and no
   `COMMIT`/`ABORT` in the log window).
2. **Scan** the WAL segments in **reverse LSN order** (newest segment first,
   entries within a segment backwards). Unlike analysis/redo, undo covers the
   **whole log**: an active transaction's pre-checkpoint operations are just as
   uncommitted and must be reversed too. Memory stays bounded by one segment.
3. **Deliver** each active-txid record to the `MvccUndoSink`:
   - `INSERT` → after-image → `onInsertUndo`;
   - `UPDATE` → before + after images → `onUpdateUndo`;
   - `DELETE` → before-image → `onDeleteUndo`;
   - everything else (non-DML ops, committed/aborted txids, missing or
     undecodable images) counts as ignored — undo never fails the pass.
4. **MVCC effect** (`DatabaseRecoverySink`): an undone INSERT re-stamps
   `RowVersionMeta(xmin = aborted txid)` — `TupleVisibility.visibleByStatus`
   hides the row from every reader because the creator is ABORTED in the
   tracker; vacuum reclaims it later (dead predicate: xmin ABORTED). An undone
   UPDATE restores the before-values when the row currently holds the
   after-image (lenient comparison, see below). An undone DELETE is a verified
   no-op: commit-time tombstones mean an uncommitted delete leaves the
   recovered row alive, which is the correct final state. The redo sink
   additionally re-applies commit-time tombstones from COMMIT payloads
   (`Table.deletedRows` is transient) and registers committed txids with their
   original CSN; active txids are registered ABORTED and the tracker counters
   are floored past every recovered id, so post-restart txids/CSNs never
   collide.

## Deviations from classical ARIES (documented)

- **No CLRs**: undo runs exactly once at startup, before any client is
  accepted, so a crash cannot interrupt it halfway; compensation log records
  are unnecessary.
- **Logical, not physical undo**: the row store's WAL carries logical row
  records; page-image physical undo (before-images in `PAGE_IMAGE`) is not
  emitted by the engine yet.
- **Index-based commit restore**: `markRowCommitted(rowIndex)` assumes table
  files reflect all WAL-committed changes (COMMIT forces a persist). Full
  cross-restart durability for every commit lands with the checkpoint
  machinery (#20-22).
- **Lenient value guards**: delimited storage may reload numeric columns with
  different boxed widths, so undo guards compare numbers by numeric value and
  other values by string form before strict equality; a mismatch skips the row
  with a warning rather than corrupting the wrong row.

## Acceptance tests

| Test | Criterion |
|------|-----------|
| `RecoveryIntegrationTest#mixedTransactionsAfterKillRecoverToCommittedOnly` | 100 tx (50 commit / 50 not) → kill → restart+recover → exactly the 50 committed ids visible, tracker 50 COMMITTED / 50 ABORTED, second recover idempotent |
| `RecoveryIntegrationTest#updateAndDeleteOfActiveTransactionAreUndone` | uncommitted UPDATE restored, DELETE undone (row alive), INSERT hidden |
| `RecoveryWithLongTransactionTest` (`@LargeTest`) | 1M inserts in one active tx → kill → restart+recover → 0 rows visible, 1M undo operations applied (measured ~7 min end-to-end) |
| `RecoveryPerformanceTest` (`perf`) | full analysis+redo+undo over a ~1 GiB WAL < 30 s (measured ~16 s) |
| `UndoPhaseTest` | reverse-LSN delivery, active-txid-only filtering, ignored opcodes, corrupt-payload tolerance, multi-segment order |
| `RecoveryManagerTest` | JMX register/unregister, `recovery.duration.ms` ≥ 0, empty-WAL recover, idempotent re-run |
| `DmlPayloadTest` | codec round-trip (all type tags + Serializable fallback), malformed → IAE |

## Startup wiring

`DatabaseServer.start()` → `loadTablesFromDisk()` → **`database.runRecovery()`**
(strictly before `new ServerSocket(...)`) → auto-vacuum start. With
`wal.enabled=false` (default) `runRecovery()` is a no-op and the
`RecoveryManager` is never created — zero impact on existing behaviour.
