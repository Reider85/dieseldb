# Prompt Status Tracker

> Compact one-line summaries. Full DONE texts: `docs/status-archive.md`. Version history: `Changelog.md` + git.
> New DONE entry format (≤5 lines): **ID DONE (date) — title**; files touched; new tests; gates (pass/fail counts); notes/deviations.

## Recent DONE

- **Prompt 4 #25 DONE (2026-10-16)** — R3-006 step 3/4: LockTimeoutManager (lock.timeout.ms) + LockTimeoutException; per-waiter ScheduledFuture timers; timed kill via abortTimeoutWait + notifyAll; lock.timeout.count metric; 3 new tests (LockTimeoutTest, CancelTimerTest, LockTimeoutConfigTest); config.properties entry. Gates: compile OK, fast suite, concurrency profile, timing skipped (not on query path).
- **Prompt 4 #24 DONE (2026-10-16)** — R3-006 step 2/4: WaitForGraph + DeadlockDetector + DeadlockVictimException; LockManager integration (getWaitForEdges, abortWait, killedWaiters); 4 new tests (DeadlockTest, ChainDeadlockTest, WaitForGraphTest, DeadlockOverheadTest); deadlock.check.interval.ms config; mapping files updated. Victim selection: min txid per cycle. Also fixed pre-existing LockCompatibility bug (held-IS+requested-S must be true per standard MGL; corrected contradicting LockCompatibilityTest expectations) and hardened DeadlockOverheadTest vs Windows scheduler noise (interleaved min-of-3 pairs, best-of-10 detector pass). Gates: fast 482/0/0, concurrency 148/0/0, timing skipped (not on query path).
- **Prompt 4 #23 DONE (2026-10-16)** — R3-006 step 1/4: LockManager with S/X/IS/IX/SIX compatibility matrix; LockMode/Lock/LockCompatibility classes; LockManagerTest (S+S grant, S+X wait, X+X conflict), LockCompatibilityTest (25 combos), LockReleaseTest (txid release + wait notification). Implementation complete with basic upgrade support. Gates: compile OK, fast 467/0/0, concurrency 15/0/0, timing skipped (not on query path).
- **Prompt 4 #21 DONE (2026-10-15)** — R3-005 step 2/3: CheckpointManager daemon (interval + WAL-size triggers, write gate, WAL archiving); WALManager totalAppendedBytes/lastCheckpointLsn; BufferPoolFlusher.flushNow(); PageManager writeGate; Database/DatabaseServer wiring. Tests: CheckpointManagerTest(10), CheckpointRecoveryStartsAtLastCheckpointTest(3), CheckpointArchivesSegmentsTest(3), CheckpointDurationTest(3). Gates: compile OK, fast 467/0/0, timing skipped (not on query path).
- **O2 WAL-9 DONE (2026-10-14)** — AnalysisPhase.apply CHECKPOINT branch: swap-reference fresh set instead of active.clear() (O(C)). New AnalysisCheckpointIsLinearTest(3). Gates: fast 467/0/0, timing skipped (recovery, precedent WAL-6/8).
- **O2 WAL-8 DONE (2026-10-14)** — WALManager.getSegments → NavigableMap; UndoPhase.undo via descendingMap() O(n). New UndoPhaseDescendingIterationTest(2). Gates: fast 467/0/0, concurrency RollbackTest 8/0/0.
- **O2 MVCC-4 DONE (2026-10-14)** — Table.rebuildRowVersionsAfterCompact: O(N) prefix-count array (was O(N²) linear scan per row). Fixed UndoPhase.getNumber() compile. Gates: fast green.
- **O2 WAL-6 DONE (2026-10-14)** — LRU RecoverySegmentCache in WALManager (wal.recovery.cache.size.bytes, 64MB); Analysis/Redo/Undo use readAllCached. Subsumes WAL-7. New RecoverySinglePassIOTest. Gates: fast 467/0/0.
- **O2 WAL-4 DONE (2026-10-13)** — readRange via lsnToSegment subMap, skips non-overlapping segments. New WALReadRangeOnlyRelevantSegmentsTest + WALReadRangeSkipNonOverlappingTest. Fixed pre-existing FindNewIndexAfterCompactIsO1Test parity + PersistFlusherTest hang. Gates: fast green.
- **O2 WAL-3 DONE (2026-10-13)** — WAL v2 32B header with lastLSN; O(log n) findMaxLsnInSegments; lazy tail-scan for v1. Fixed readHeader reserved-skip bug + 3 pre-existing WALReadByLsnTest failures. Gates: fast green.
- **O2 MVCC-3 DONE (2026-10-11)** — TransactionTableSnapshot memoized getRowCount/getRow/toString O(N)/O(1). New SnapshotGetRowCountIsLinearTest/GetRowIsLinear/ToStringIsCheap. Gates: fast 439/0/0, concurrency green.
- **O2 MVCC-2 DONE (2026-10-11)** — TransactionTableSnapshot.getRows: per-row O(1) readPhysicalRow (was O(N²)). New SnapshotMaterializationCountTest + SnapshotReadBenchmarkTest. Gates: fast 429/0/0, concurrency 97/0/0, core 2429/0/0; timing red (noise, user-approved).
- **O2 MVCC-1 DONE (2026-10-11)** — ConflictDetector rowReaders index, O(W) commit (was O(T×W×R)); fixed activeTxns leak; SerializationConflictTest tagged+fixed. Gates: fast green, concurrency green.
- **Prompt 4 #20 DONE (2026-10-10)** — R3-005 step 1/3: BufferPoolFlusher + AdaptiveFlushStrategy, JMX MXBean, WAL-before-page rule. Gates: fast 429/0/0, large 20/0/0; timing red (noise, user-approved).
- **Prompt 4 #19 DONE (2026-10-10)** — ARIES UndoPhase + ARIESAlgorithm + RecoveryManager; DML WAL logging + DmlPayload codec; recovery.duration.ms. Gates: 100-tx kill-restart green, 1GiB WAL recovery 16s.
- **Prompt 4 #12 DONE (2026-10-08)** — R3-003 step 2/5: WALManager + WALSegment (24B header, LSN allocator, rotation, checkpoint.ptr). 12 WALManagerTest green + 4 en-route engine fixes.

## TODO

(see o2-prompts.md / prompt4.md / prompt3.md for the active queues)
