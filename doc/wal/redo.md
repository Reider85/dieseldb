# ARIES Redo Phase (prompt 4 #18, R3-004 step 3/4)

Replays committed physical changes from the WAL onto the page store after a
crash, before the undo phase (step 4, prompt 4 #19) reverses the changes of
uncommitted transactions.

## Components

| File | Role |
|------|------|
| `diesel/recovery/RedoPhase.java` | Scans the WAL window and drives the replay |
| `diesel/recovery/RedoResult.java` | Bucket counters of one redo pass |
| `diesel/recovery/MvccRedoSink.java` | Callback that receives replayed COMMIT payloads |
| `diesel/wal/CommitPayload.java` | Single codec for the COMMIT payload (writer + replayer) |
| `diesel/storage/page/PageManager.java` | `applyRedo(WALEntry)` — idempotent page installation |
| `diesel/wal/WALOpcode.java` | `PAGE_IMAGE(8)` — physical page after-image record |

## Algorithm

1. **Window**: `startLsn = checkpoint.lastLSN + 1`, `endLsn = wal.getLastLsn()`
   (snapshot once, exactly like `AnalysisPhase`). No checkpoint → the whole
   log from LSN 1.
2. **Scan** segment-by-segment (`WALSegment.readAll()`): only one segment body
   is materialized at a time, so memory stays bounded by the segment size even
   for multi-gigabyte WALs.
3. **Replay** each record in the window:
   - `PAGE_IMAGE` → `PageManager.applyRedo(entry)`: the after-image (a full
     serialized page) overwrites the target page and stamps the page LSN with
     the record's LSN. The page LSN check makes the pass idempotent — a page
     whose persisted LSN is already `>= entry.lsn` is skipped (`false`).
     Redo may create a page beyond the end of file (allocation redo); the
     allocator watermark `nextFilePageNum` is advanced so a later
     `allocatePage` cannot address it.
   - `COMMIT` with a payload → decoded via `CommitPayload` and delivered to
     the optional `MvccRedoSink` (MVCC xmin/xmax restoration — task 4 of
     #18). Payload-less/legacy commits and decode failures count as ignored
     (a warning is logged); physical redo never fails on them.
   - everything else (`BEGIN`, DML, `ABORT`, `CHECKPOINT`) → ignored.
4. **`pages.flush()`** — every applied image becomes durable before recovery
   continues to undo.

`RedoResult` buckets are exhaustive: `applied + skipped + commitsReplayed +
ignored == totalRecords` for the scanned window.

## MVCC integration status (task 4)

The redo plumbing delivers the exact commit bookkeeping (txid, commit CSN,
modified/deleted row indexes) that `Database.executeCommit` stamped at commit
time. Full xmin/xmax visibility restoration needs the startup orchestration of
prompt 4 #19 (`RecoveryManager`): `Table.rowVersions` is transient by design
and is rebuilt when tables load, and `TxStatusTracker` state is rebuilt from
the analysis result — both wiring points live in #19.

## Acceptance tests

| Test | Criterion |
|------|-----------|
| `RedoTest#thousandMixedOpsAfterKillRedoMatchesLastCommittedState` | 1000 mixed records → kill (150 durable + 150 lost) → redo → every page matches the last committed image and LSN (750 applied / 150 skipped / 100 ignored) |
| `RedoTest#redoWindowStartsAfterCheckpoint` | pre-checkpoint records are excluded by the window, not just by the LSN check |
| `RedoTest#redoCreatesPageBeyondFileEnd` | allocation redo + allocator watermark |
| `RedoTest#commitPayloadsAreReplayedToMvccSink` | COMMIT payload round-trips through the real codec into the sink |
| `IdempotencyTest#secondRedoOnAlreadyRedonePagesIsNoOp` | re-running redo applies nothing, page state byte-identical |
| `IdempotencyTest#redoAfterCrashWithoutFlushReapplies` | a redo that never reached disk is fully re-applied on the next pass |
| `RedoPerformanceTest` (`perf`) | redo of a ~1 GiB WAL of `PAGE_IMAGE` records < 20 s (override: `-Ddiesel.redo.perf.bytes=N`) |

## Wire compatibility

`PAGE_IMAGE` is code 8 — appended after the codes mandated by prompt 11, so
logs written by earlier versions never contain it and stay readable. Old code
reading a new log rejects byte 8 at the WAL layer (unknown opcode), which is
the intended forward-failure direction.
