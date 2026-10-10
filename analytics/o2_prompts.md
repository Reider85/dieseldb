# o2_prompts.md — O(n²)+ complexity remediation prompts for dieseldb

Repository: https://github.com/Reider85/dieseldb
Analysis scope: MVCC, WAL, Buffer loading, and storage modes `csv` / `tsv` / `jsonl` / `avro`.
Total hotspots identified: **65** (4 MVCC + 9 WAL + 8 Buffer + 7 CSV + 7 TSV + 11 JSONL + 19 Avro).

Each section below contains a self-contained **remediation prompt** that can be fed directly to a coding agent (or used by a developer) to eliminate the hotspot. The prompts follow a uniform template:

```
PROMPT-ID — TITLE
Severity: Critical | High | Medium
Subsystem: <MVCC | WAL | Buffer | CSV | TSV | JSONL | Avro>
Files: <paths>
Pattern: <code snippet>
Complexity: <Big-O with definition of variables>
Root cause: <2–4 sentences>
Required changes: <numbered actionable steps>
Acceptance criteria: <verifiable success conditions>
Test additions: <new tests or regressions to add>
```

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [MVCC Remediation Prompts (4)](#2-mvcc-remediation-prompts)
3. [WAL Remediation Prompts (9)](#3-wal-remediation-prompts)
4. [Buffer Loading Remediation Prompts (8)](#4-buffer-loading-remediation-prompts)
5. [CSV Storage Remediation Prompts (7)](#5-csv-storage-remediation-prompts)
6. [TSV Storage Remediation Prompts (7 — shared CSV+TSV)](#6-tsv-storage-remediation-prompts)
7. [JSONL Storage Remediation Prompts (11)](#7-jsonl-storage-remediation-prompts)
8. [Avro Storage Remediation Prompts (19)](#8-avro-storage-remediation-prompts)
9. [Cross-cutting Recommendations](#9-cross-cutting-recommendations)

---

## 1. Executive Summary

The analysis surfaced **65 distinct O(n²)+ complexity hotspots** across the seven subsystems. They cluster around a small number of root causes that, when fixed, eliminate multiple hotspots simultaneously:

| Root cause | Subsystems affected | Hotspots collapsed |
|---|---|---|
| Missing reverse (position → rowId) map | CSV, TSV, JSONL | CSV 1,2,3,5 + TSV 1,2,3,5,7 + JSONL 2,3,4 |
| `ArrayList.remove(Object)` / `List<Long>.remove(rowId)` on secondary-index posting lists | CSV, TSV, JSONL | CSV 6 + TSV 6 + JSONL 6 |
| `ArrayList.add(int, …)` / `remove(int)` shifts on every row insert/delete without tombstones | CSV, TSV, JSONL, Avro | CSV 4,5,7 + TSV 4,5 + JSONL 7,8 + Avro 11,12 |
| Per-record Avro `GenericRecord.get(String)` / `Schema.getField(String)` linear scans inside per-column loops | Avro | Avro 1, 2, 3 |
| Case-insensitive `equalsIgnoreCase` scans over `Map.entrySet()` instead of a pre-built case-insensitive lookup | Avro | Avro 13, 14, 15, 16, 17 |
| `TreeMap.replaceAll` / linear `shiftPositions` over the whole index per insert/delete | Avro | Avro 9, 10 |
| Catalog schema list with no name index (linear scan per lookup) | Buffer | Buffer 1, 2, 3 |
| BufferPool tracks dirty pages by full-frame scan instead of a dirty-page set | Buffer | Buffer 4 |
| LRU eviction scans from head when many pinned frames are clustered at the head | Buffer | Buffer 5 |
| Slotted page never reuses tombstoned slot ids and never compacts the slot directory on defrag | Buffer | Buffer 6, 7, 8 |
| MVCC conflict detection does a Cartesian write-set × read-set comparison per commit | MVCC | MVCC 1 |
| MVCC snapshot re-materializes the entire row list per row during visibility check | MVCC | MVCC 2, 3 |
| MVCC post-compaction re-keying linearly rescans the deleted-bitset per entry | MVCC | MVCC 4 |
| WAL manager does a full-segment `readAll()` per LSN lookup and per `findMaxLsn` | WAL | WAL 1, 2, 3, 4 |
| WAL recovery performs 4 separate full passes over the WAL (loadCheckpoint + analysis + redo + undo) | WAL | WAL 6, 7, 8 |
| Analysis-phase CHECKPOINT branch clears and re-seeds the active set per checkpoint | WAL | WAL 9 |
| JSONL schema manager does a pairwise dot-prefix check across all columns | JSONL | JSONL 1 |
| JSONL delta manager does a content scan over base rows per delete | JSONL | JSONL 5, 6 |
| JSONL row reader does a per-field scan over projection slots | JSONL | JSONL 10 |
| JSONL path resolver rebuilds the column TreeMap per call | JSONL | JSONL 11 |

Severity breakdown:
- **Critical** (15): MVCC-1, MVCC-2, MVCC-3, MVCC-4, WAL-1, WAL-2, WAL-4, Buffer-4, Buffer-7, Avro-1, Avro-2, Avro-11, Avro-12, Avro-13, Avro-19
- **High** (32): WAL-3, WAL-5, WAL-6, WAL-7, WAL-8, WAL-9, Buffer-1, Buffer-2, Buffer-3, Buffer-5, Buffer-6, Buffer-8, CSV-1, CSV-2, CSV-3, CSV-4, CSV-5, CSV-6, CSV-7, TSV-1, TSV-2, TSV-3, TSV-4, TSV-5, TSV-6, TSV-7, JSONL-2, JSONL-3, JSONL-4, JSONL-5, JSONL-7, JSONL-8
- **Medium** (18): JSONL-1, JSONL-6, JSONL-9, JSONL-10, JSONL-11, Avro-3, Avro-4, Avro-5, Avro-6, Avro-7, Avro-8, Avro-9, Avro-10, Avro-14, Avro-15, Avro-16, Avro-17, Avro-18

---

## 2. MVCC Remediation Prompts

### MVCC-1 — Eliminate O(T×W×R) Cartesian conflict detection in `ConflictDetector.noteCommit`

**Severity:** Critical
**Subsystem:** MVCC
**Files:** `diesel/concurrency/ConflictDetector.java`
**Method:** `public void noteCommit(long txid, long commitCsn, java.util.Map<String, java.util.Set<Integer>> writeSet)`
**Lines:** 131–154

**Pattern:**
```java
for (TxnTracking otherTxn : activeTxns.values()) {           // O(T)
    if (otherTxn.txid == txid) continue;
    for (RowRef ourWrite : thisTxn.writeSet) {               // O(W)
        for (RowRef otherRead : otherTxn.readSet) {          // O(R)
            if (ourWrite.table().equals(otherRead.table())
                    && ourWrite.rowIndex() == otherRead.rowIndex()) {
                throw new SerializationFailureException(...);
            }
        }
    }
}
```

**Complexity:** O(T × W × R) per commit; cumulatively O(T² × W × R) across a workload of T committing transactions. Effectively O(n³) when T, W, R all grow with data size.

**Root cause:** Naive Cartesian intersection between the committer's write set and every other SERIALIZABLE transaction's read set. Each (table, rowIndex) pair is compared by string-equals and int-equals, with no indexing structure. Every commit literally compares its writes against every read of every other active transaction.

**Required changes:**
1. Maintain a global `ConcurrentHashMap<RowRef, Set<Long>> rowReaders` mapping each `(table, rowIndex)` to the set of txids currently reading it. Update it inside `noteRead(...)` on every read registration (O(1) put into a `ConcurrentHashMap<RowRef, ConcurrentHashSet<Long>>` or a `ConcurrentSkipListSet`).
2. Update `noteCommit(...)` to, for each `(table, rowIndex)` in this transaction's `writeSet`, do an O(1) lookup `rowReaders.get(rowRef)` and throw `SerializationFailureException` if the returned set contains any txid other than `txid`. This eliminates the outer T loop entirely.
3. On `noteAbort(txid)` and on commit-end (after the conflict check has succeeded), remove all entries this transaction registered in `rowReaders`. Use a per-transaction `Set<RowRef> myReads` (already present as `TxnTracking.readSet`) to drive cleanup in O(R).
4. Verify thread-safety: use `ConcurrentHashMap.computeIfAbsent` with a `ConcurrentHashMap.newKeySet()` for the inner set, and `ConcurrentHashMap.KeySetView` removal.
5. Update `TxnTracking.readSet` field comment to reflect that it now drives cleanup, not O(W × R) comparison.

**Acceptance criteria:**
- `ConflictDetectorTest`, `ConcurrentConflictTest`, `SerializationConflictTest`, `IsolationSemanticsTest` all pass without modification.
- A new benchmark `ConflictDetectorThroughputTest` with N=100 concurrent transactions each reading R=1000 rows and committing with W=10 writes shows commit latency ≤ O(W) (no measurable increase as T grows from 10 → 1000).
- `activeTxns.values()` is no longer iterated inside `noteCommit`.

**Test additions:**
- `ConflictDetectorCartesianBenchmarkTest`: assert `noteCommit` time stays flat as the number of active SERIALIZABLE readers grows.
- `RowReadersCleanupTest`: assert `rowReaders` map is empty after all transactions have committed or aborted.

---

### MVCC-2 — Stop re-materializing the entire row list per row in `TransactionTableSnapshot.getRows`

**Severity:** Critical
**Subsystem:** MVCC
**Files:** `diesel/TransactionTableSnapshot.java`, `diesel/Table.java`
**Methods:** `public List<Map<String, Object>> getRows()` (lines 58–76), `private boolean isRowVisible(int, RowVersionMeta)` (lines 120–148), `private Map<String,Object> getVisibleRowValues(int, RowVersionMeta)` (lines 161–171)
**Underlying cause:** `diesel/Table.java::getRows()` (line 1236) — defensive-copy API returning `new ArrayList<>(rows)` or `storage.scan()`.

**Pattern:**
```java
for (int i = 0; i < rowCount; i++) {                        // O(N)
    if (!table.isDeleted(i)) {
        RowVersionMeta meta = table.getRowVersionMeta(i);
        if (isRowVisible(i, meta)) {                        // calls table.getRows().get(rowIndex) inside
            result.add(getVisibleRowValues(i, meta));       // calls table.getRows().get(rowIndex) inside
        }
    }
}
```

**Complexity:** O(N²) per call (N = `table.getRawRowCount()`). Two `table.getRows()` invocations per row × N rows = O(2N²) per snapshot read.

**Root cause:** The visibility decision for every row re-materializes the entire row list. `Table.getRows()` is a defensive-copy API and is being misused inside a hot per-row loop. The snapshot already iterates `table.getRawRowCount()` rows, but instead of fetching each row at its own index from the underlying list (which would be O(1) per row), it asks for the full table copy every time.

**Required changes:**
1. Add a new public method to `Table`: `public Map<String, Object> readPhysicalRow(int rowIndex)` that returns the row at the given index without copying the entire list. For in-memory storage this returns `rows.get(rowIndex)`; for on-disk storage it calls `storage.scanRow(int)` (add this method to `RowStorage` if missing — the Avro path already has it via `AvroReadIterator`).
2. In `TransactionTableSnapshot.getRows()`, capture the row list ONCE at the top via `table.getRows()` (or even better, replace the per-row fetch with `table.readPhysicalRow(i)` calls — preferred because it avoids the O(N) upfront copy entirely).
3. Pass the cached row / the row index down to `isRowVisible(int, RowVersionMeta, Map<String,Object> row)` and `getVisibleRowValues(int, RowVersionMeta, Map<String,Object> row)` so they no longer invoke `table.getRows()`.
4. Make `isRowVisible` accept the row map as a parameter rather than re-fetching it.

**Acceptance criteria:**
- `TransactionTableSnapshotTest`, `MvccDeleteTest`, `MvccUpdateTest`, `IsolationSemanticsTest`, `CopyOnWriteIsolationTest`, `TupleVisibilityTest` all pass without modification.
- `SnapshotReadBenchmarkTest` (new): a table with 100k rows and 1000 active snapshots shows snapshot read time ≤ 2× the time of `table.getRows()` (i.e. the snapshot is O(N), not O(N²)).
- A profile (`-XX:+PrintCompilation` / async-profiler) confirms `Table.getRows()` is called at most once per `TransactionTableSnapshot.getRows()` invocation.

**Test additions:**
- `SnapshotMaterializationCountTest`: assert (via a counter injected into `Table.getRows()`) that `getRows()` is called at most once per snapshot read.
- `SnapshotReadBenchmarkTest`: N=10⁵ rows, K=10³ snapshots, total time < 1 s.

---

### MVCC-3 — Make `getRowCount`, `getRow(int)`, `toString` O(N) instead of O(N²) in `TransactionTableSnapshot`

**Severity:** Critical
**Subsystem:** MVCC
**Files:** `diesel/TransactionTableSnapshot.java`
**Methods:** `public int getRowCount()` (lines 83–86), `public Map<String,Object> getRow(int index)` (lines 95–101), `public String toString()` (lines 177–186)

**Pattern:**
```java
public int getRowCount() { return getRows().size(); }   // O(N²) to produce one int!
public Map<String, Object> getRow(int index) {
    List<Map<String, Object>> rows = getRows();         // O(N²) to fetch one row!
    ...
}
@Override public String toString() {
    return "TransactionTableSnapshot{ ... rowCount=" + getRowCount() + " ... }";  // O(N²) per log line
}
```

**Complexity:** O(N²) per call (inherited from `getRows()`).

**Root cause:** All three methods invoke the O(N²) `getRows()` to produce a tiny result (a single int, a single row, a debug string). A caller that logs every snapshot via `toString()` in a loop pays O(N²) per log line. The author already noticed — see the comment "For performance, we could cache this if needed".

**Required changes:**
1. Implement `private int countVisibleRows()`: a single O(N) walk over the table that increments a counter for each row passing `isRowVisible` (using the O(1) `readPhysicalRow` accessor added in MVCC-2 — no full-table copy).
2. Implement `private Map<String,Object> getVisibleRow(int index)`: a single O(N) walk that returns the index-th visible row (using the same accessor).
3. Replace the bodies of `getRowCount()` and `getRow(int)` with calls to the new O(N) methods.
4. In `toString()`, remove the `getRowCount()` call — replace with `"rowCount=<lazy>"` or compute via a new `O(1)` maintained counter (incremented in the snapshot constructor by walking once and cached in a `final int visibleRowCount` field).
5. Cache the visible row count and the materialized `List<Map<String,Object>>` lazily in the snapshot (memoize on first call to `getRows()`, `getRowCount()`, or `getRow(int)`), so callers that ask for count and then rows do not pay twice.

**Acceptance criteria:**
- `TransactionTableSnapshotTest` passes.
- New `SnapshotCountIsLinearTest`: calling `getRowCount()` on a 50k-row table with 50% visibility completes in < 50 ms (today: seconds).
- New `SnapshotToStringIsCheapTest`: calling `toString()` 1000 times in a loop on a 10k-row snapshot completes in < 100 ms total.

**Test additions:**
- `SnapshotGetRowCountIsLinearTest`
- `SnapshotGetRowIsLinearTest`
- `SnapshotToStringIsCheapTest`

---

### MVCC-4 — Replace `findNewIndexAfterCompact` linear scan with O(N) prefix-count array

**Severity:** Critical
**Subsystem:** MVCC
**Files:** `diesel/Table.java`
**Methods:** `public void rebuildRowVersionsAfterCompact()` (lines 1911–1938), `private int findNewIndexAfterCompact(int oldIndex)` (lines 1944–1952)

**Pattern:**
```java
public void rebuildRowVersionsAfterCompact() {
    if (uncommittedMvccRows != null && !uncommittedMvccRows.isEmpty()) {
        java.util.Set<Integer> remapped = ConcurrentHashMap.newKeySet();
        for (Integer oldIndex : uncommittedMvccRows) {             // O(R1)
            int newIndex = findNewIndexAfterCompact(oldIndex);     // O(oldIndex) <= O(N)
            ...
        }
    }
    rowVersions.forEach((oldIndex, meta) -> {                      // O(R2) entries
        int newIndex = findNewIndexAfterCompact(oldIndex);          // O(oldIndex) <= O(N)
        ...
    });
}

private int findNewIndexAfterCompact(int oldIndex) {
    int newIndex = 0;
    for (int i = 0; i < oldIndex; i++) {                           // O(oldIndex) per call
        if (!isDeleted(i)) { newIndex++; }
    }
    return isDeleted(oldIndex) ? -1 : newIndex;
}
```

**Complexity:** O(R × N) per `compact()` call (R = MVCC-versioned rows, N = total raw row count). Worst case R ≈ N → **O(N²)** per compaction.

**Root cause:** After `compact()` physically drops tombstoned rows, every retained MVCC version entry must be re-keyed from its old row index to its new post-compact index. The re-key walks `findNewIndexAfterCompact(oldIndex)`, which counts non-deleted rows preceding `oldIndex` by linearly scanning the `deletedRows` BitSet from position 0 up to `oldIndex`. Σ k over all entries is O(N²) when entries span the whole table.

**Required changes:**
1. At the start of `rebuildRowVersionsAfterCompact`, build a prefix-sum array `int[] prefixAlive = new int[N + 1]` where `prefixAlive[i + 1] = prefixAlive[i] + (isDeleted(i) ? 0 : 1)`. This is O(N) to construct.
2. Replace the body of `findNewIndexAfterCompact(int oldIndex)` with `return isDeleted(oldIndex) ? -1 : prefixAlive[oldIndex];` — O(1) per call.
3. Alternatively (and equivalently): walk `deletedRows` once with `nextSetBit` and emit an `int[] oldToNew = new int[N]` in a single O(N) pass; then `findNewIndexAfterCompact(oldIndex) = oldToNew[oldIndex]` (or -1 if the row was deleted).
4. Pass `prefixAlive` (or `oldToNew`) into `rebuildRowVersionsAfterCompact` as a local variable, NOT as a field — it is only valid for the duration of one compaction.

**Acceptance criteria:**
- `DefragTest`, `VacuumTest`, `VacuumManagerTest`, `MvccDeleteTest`, `MvccUpdateTest` all pass.
- New `CompactWithLargeRowVersionsTest`: a table with N=10⁵ rows where 80% have MVCC metadata and 50% are deleted — `compact()` completes in < 500 ms (today: seconds).
- A microbenchmark confirms `findNewIndexAfterCompact` is O(1) (no loop, no recursion).

**Test additions:**
- `CompactWithLargeRowVersionsTest`
- `FindNewIndexAfterCompactIsO1Test`

---

## 3. WAL Remediation Prompts

### WAL-1 — Maintain in-memory LSN→segment index; eliminate `findSegmentForLsn` full-segment scans

**Severity:** Critical
**Subsystem:** WAL
**Files:** `diesel/wal/WALManager.java`, `diesel/wal/WALSegment.java`, `diesel/wal/WALFormat.java`
**Methods:** `private WALSegment findSegmentForLsn(long lsn)` (lines 442–464); related: `WALSegment.append` / `appendBatch`, `WALSegment.readAll`, segment header writes

**Pattern:**
```java
for (WALSegment segment : segments.descendingMap().values()) {   // outer: n segments
    List<WALEntry> entries = segment.readAll();                   // inner: reads ALL m entries
    if (!entries.isEmpty()) {
        long firstLsn = entries.get(0).getLsn();
        long lastLsn = entries.get(entries.size() - 1).getLsn();
        if (lsn >= firstLsn && lsn <= lastLsn) return segment;
    }
}
```

**Complexity:** O(n × m) per call (n = segments, m = entries per segment). Worst case k = O(N) ⇒ O(N²).

**Root cause:** The segment header stores `firstLsn` (cheap) but not `lastLsn`. The code re-reads every entry of every segment just to compute `lastLsn` for the range check. The inline comment explicitly acknowledges: "Future optimization: maintain per-segment LSN ranges".

**Required changes:**
1. Add a `private long lastLsn;` field to `WALSegment`, updated on every `append(WALEntry)` and `appendBatch(List<WALEntry>)` call to `entries.get(entries.size() - 1).getLsn()`.
2. Optionally persist `lastLsn` into the segment header — increase `WALFormat.SEGMENT_HEADER_SIZE` from 24 to 32 bytes (add 8 bytes for `lastLsn`). On open, read it back; if 0 (legacy segment), compute it lazily by reading the last entry only.
3. Maintain an in-memory `TreeMap<Long, WALSegment> lsnToSegment` (or `NavigableMap<Long, Integer> lsnToSegmentNumber`) in `WALManager`, updated on every `append`, `appendBatch`, and `rotateSegment` so that `lsnToSegment.put(firstLsn, segment)` is kept. On open, populate it from `segment.getFirstLsn()`.
4. Replace the body of `findSegmentForLsn(long lsn)` with `Map.Entry<Long, WALSegment> e = lsnToSegment.floorEntry(lsn); return e == null ? null : e.getValue();` — O(log n).
5. Add a regression test for legacy segments (header size 24) — ensure backward compatibility by computing `lastLsn` lazily on first access.

**Acceptance criteria:**
- `WALManagerTest`, `WALWriterTest`, `WALCrashRecoveryTest`, `AsyncWALWriterTest`, `RecoveryManagerTest`, `RecoveryIntegrationTest` all pass.
- New `WALFindSegmentIsLogNTest`: with 1000 segments, `findSegmentForLsn` completes in < 1 µs (today: hundreds of ms).
- Backward compatibility: a WAL written with the previous format is still readable.

**Test additions:**
- `WALFindSegmentIsLogNTest`
- `WALLastLsnHeaderBackwardCompatTest`

---

### WAL-2 — Make `readByLsn` reuse the segment read from `findSegmentForLsn`

**Severity:** Critical
**Subsystem:** WAL
**Files:** `diesel/wal/WALManager.java`, `diesel/wal/WALSegment.java`
**Methods:** `public WALEntry readByLsn(long lsn)` (lines 414–434)

**Pattern:**
```java
public WALEntry readByLsn(long lsn) throws IOException {
    WALSegment segment = findSegmentForLsn(lsn);    // O(n*m)
    if (segment == null) return null;
    List<WALEntry> entries = segment.readAll();    // O(m) — RE-READS the matched segment
    for (WALEntry entry : entries) {               // O(m) linear scan for the LSN
        if (entry.getLsn() == lsn) return entry;
        if (entry.getLsn() > lsn) break;
    }
    return null;
}
```

**Complexity:** O(n × m) per call. The matched segment is read TWICE — once inside `findSegmentForLsn` (along with every other segment), and once again via `segment.readAll()` here.

**Root cause:** `findSegmentForLsn` already paid the I/O cost to read every entry of the matched segment, but the result is thrown away and `segment.readAll()` is called again.

**Required changes:**
1. Add a new method to `WALSegment`: `public WALEntry readByLsn(long lsn)` that streams the segment file with a `ByteBuffer`, decoding entries one at a time and returning on first match (early-exit). This is O(position-of-LSN-within-segment) and avoids the full `readAll()`.
2. Replace the body of `WALManager.readByLsn(long lsn)` with:
   ```java
   WALSegment segment = findSegmentForLsn(lsn);   // O(log n) after WAL-1 fix
   return segment == null ? null : segment.readByLsn(lsn);
   ```
3. Alternatively, change `findSegmentForLsn` to return a `Pair<WALSegment, List<WALEntry>>` so the caller can reuse the decoded entry list — but the streaming `readByLsn` is preferable as it avoids decoding entries past the target.

**Acceptance criteria:**
- `WALManagerTest`, `RecoveryManagerTest`, `CheckpointRecordTest`, `CheckpointIntegrationTest` all pass.
- New `WALReadByLsnSingleReadTest`: byte-level read counter (via a counting FileChannel wrapper) confirms the matched segment is read at most once per `readByLsn` call.
- Combined with WAL-1: `readByLsn` is O(log n + position-of-LSN-within-segment).

**Test additions:**
- `WALReadByLsnSingleReadTest`
- `WALReadByLsnEarlyExitTest`

---

### WAL-3 — Persist `lastLsn` in segment header; collapse `findMaxLsnInSegments` to O(log n)

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/wal/WALManager.java`, `diesel/wal/WALSegment.java`, `diesel/wal/WALFormat.java`
**Method:** `private long findMaxLsnInSegments()` (lines 172–187)

**Pattern:**
```java
long maxLsn = 0;
for (WALSegment segment : segments.values()) {            // n segments
    List<WALEntry> entries = segment.readAll();           // reads ALL m entries
    if (!entries.isEmpty()) {
        long segmentMax = entries.stream()
            .mapToLong(WALEntry::getLsn).max().orElse(0);
        maxLsn = Math.max(maxLsn, segmentMax);
    }
}
return maxLsn;
```

**Complexity:** O(n × m) per call. Called once at `WALManager` construction.

**Root cause:** Reads every entry of every segment from disk just to compute `max(entries[i].getLsn())`. Since entries within a segment are LSN-ascending, the segment max is simply the LSN of the last entry — no scan needed. The segment header stores `firstLsn` but not `lastLsn`, so there is no O(1) way to skip the full read.

**Required changes:**
1. Implement the WAL-1 fix (persist `lastLsn` in segment header; maintain in-memory index).
2. Replace the body of `findMaxLsnInSegments()` with:
   ```java
   Map.Entry<Integer, WALSegment> last = segments.lastEntry();
   return last == null ? 0 : last.getValue().getLastLsn();
   ```
   — O(log n).
3. As a transitional fix without a format change, only read the LAST entry of the last segment: seek to `writePosition - lastEntrySize` and decode one entry. This is O(m_last) instead of O(n × m).
4. Ensure the WAL segment header is updated atomically on `append`/`appendBatch` so a crash leaves a consistent `lastLsn` (or a sentinel 0 that triggers lazy computation on next open).

**Acceptance criteria:**
- `WALManagerTest`, `WALWriterTest`, `WALCrashRecoveryTest` pass.
- New `WALRecoverLsnIsOLogNTest`: starting `WALManager` with 1000 segments completes in < 10 ms (today: seconds).

**Test additions:**
- `WALRecoverLsnIsOLogNTest`
- `WALLastLsnCrashConsistencyTest`

---

### WAL-4 — Skip non-overlapping segments in `readRange`

**Severity:** Critical
**Subsystem:** WAL
**Files:** `diesel/wal/WALManager.java`
**Method:** `public List<WALEntry> readRange(long fromLsnInclusive, long toLsnInclusive)` (lines 494–511)

**Pattern:**
```java
List<WALEntry> rangeEntries = new ArrayList<>();
for (WALSegment segment : segments.values()) {           // outer: n segments — NO skip filter
    List<WALEntry> entries = segment.readAll();          // reads ALL m entries
    for (WALEntry entry : entries) {
        long lsn = entry.getLsn();
        if (lsn >= fromLsnInclusive && lsn <= toLsnInclusive) {
            rangeEntries.add(entry);
        } else if (lsn > toLsnInclusive) {
            break;
        }
    }
}
return Collections.unmodifiableList(rangeEntries);
```

**Complexity:** O(n × m) per call regardless of how small the requested LSN range is. A query for a 10-LSN window still reads the entire WAL.

**Root cause:** The method never uses `segment.getFirstLsn()` to skip segments whose LSN range falls entirely outside `[fromLsnInclusive, toLsnInclusive]`. Compare to `AnalysisPhase.analyze` and `RedoPhase.redo`, both of which DO short-circuit using `if (segment.getFirstLsn() > endLsn && segment.getFirstLsn() != 0) break;`.

**Required changes:**
1. After WAL-1 is implemented, use `lsnToSegment.subMap(fromLsnInclusive, true, toLsnInclusive, true)` to obtain only the segments that could contain entries in the requested range. Iterate only those.
2. For each selected segment, add a pre-filter:
   ```java
   if (segment.getFirstLsn() > toLsnInclusive && segment.getFirstLsn() != 0) break;
   if (segment.getLastLsn() < fromLsnInclusive) continue;
   ```
3. The inner `for (WALEntry entry : entries)` loop can keep its existing break-on-exceed-upper-bound.

**Acceptance criteria:**
- `WALManagerTest` passes (add a `readRange` test if none exists).
- New `WALReadRangeOnlyRelevantSegmentsTest`: with 100 segments each containing 1000 entries, `readRange(50_000, 50_010)` opens at most 2 segments (verified via a counting FileChannel wrapper).

**Test additions:**
- `WALReadRangeOnlyRelevantSegmentsTest`
- `WALReadRangeSkipNonOverlappingTest`

---

### WAL-5 — Delete redundant `allEntries.sort` in `readAll`

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/wal/WALManager.java`
**Method:** `public List<WALEntry> readAll()` (lines 472–484)

**Pattern:**
```java
List<WALEntry> allEntries = new ArrayList<>();
for (WALSegment segment : segments.values()) {            // segments TreeMap iterates in key order
    List<WALEntry> entries = segment.readAll();           // entries are LSN-ascending within a segment
    allEntries.addAll(entries);
}
allEntries.sort((e1, e2) -> Long.compare(e1.getLsn(), e2.getLsn()));   // UNNECESSARY
return Collections.unmodifiableList(allEntries);
```

**Complexity:** O(n × m × log(n × m)) = O(N log N) due to the sort; without it O(n × m) = O(N).

**Root cause:** `segments` is a `TreeMap<Integer, WALSegment>` iterating in ascending segment-number order. Segment numbers are assigned monotonically on rotation, and within a segment entries are appended in strictly increasing LSN order. Therefore `allEntries` is already globally LSN-ascending before the sort. The `sort` call performs O(N log N) comparisons on already-sorted data.

**Required changes:**
1. Delete the `allEntries.sort(...)` line.
2. To be defensive against future changes to segment iteration, add an assertion gated by a debug flag: `assert isSorted(allEntries) : "WAL entries out of order";` where `isSorted` is a single O(N) check.
3. Update the method's Javadoc to document the post-condition that the returned list is LSN-ascending and explain why the sort was removed.

**Acceptance criteria:**
- `WALManagerTest.readAll` (add if missing) passes.
- New `WALReadAllIsAlreadySortedTest`: asserts the returned list is strictly LSN-ascending.
- A benchmark shows `readAll` time drops by the log factor.

**Test additions:**
- `WALReadAllIsAlreadySortedTest`

---

### WAL-6 — Cache decoded segment bodies across recovery phases in `AnalysisPhase.analyze`

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/recovery/AnalysisPhase.java`, `diesel/recovery/ARIESAlgorithm.java`, `diesel/wal/WALManager.java`
**Method:** `public static AnalysisResult analyze(WALManager wal, CheckpointRecord checkpoint)` (lines 81–116)

**Pattern:**
```java
for (WALSegment segment : wal.getSegments().values()) {   // n segments
    if (segment.getFirstLsn() > endLsn && segment.getFirstLsn() != 0) break;
    List<WALEntry> entries = segment.readAll();           // m entries — full disk read + decode
    for (WALEntry entry : entries) {                      // m iterations
        long lsn = entry.getLsn();
        if (lsn > endLsn) break;
        if (lsn < startLsn) continue;
        apply(entry, committed, active);                  // O(1) HashSet ops
    }
}
```

**Complexity:** O(n × m) per call = O(N) single-pass. Recovery runs 4 separate full passes (loadCheckpoint + analyze + redo + undo), so the WAL is read 4 times from disk.

**Root cause:** Each phase (`AnalysisPhase`, `RedoPhase`, `UndoPhase`, `WALManager.loadCheckpointRecord`) calls `segment.readAll()` independently, re-reading and re-decoding every entry from disk. There is no cross-phase cache for the decoded entries.

**Required changes:**
1. Introduce an in-memory `Map<Integer, List<WALEntry>> decodedSegmentCache` in `WALManager` (size-bounded by `wal.recovery.cache.size.bytes` config; default 64 MB; LRU eviction by segment size).
2. Add a `public List<WALEntry> readAllCached(int segmentNumber)` method that checks the cache first, populates it on miss, and returns a shared immutable reference (do NOT copy — recovery phases do not mutate entries).
3. Refactor `AnalysisPhase.analyze`, `RedoPhase.redo`, `UndoPhase.undo`, and `WALManager.loadCheckpointRecord` to use `readAllCached` instead of `segment.readAll()`.
4. For very large WALs that exceed the cache size, evict cold segments (LRU) — recovery still works correctly, it just re-reads evicted segments from disk.
5. Reset the cache at the end of `ARIESAlgorithm.recover` to free memory before the database starts serving traffic.

**Acceptance criteria:**
- `AnalysisTest`, `AnalysisPerformanceTest`, `RedoTest`, `RedoPerformanceTest`, `UndoPhaseTest`, `RecoveryPerformanceTest`, `RecoveryIntegrationTest` all pass.
- New `RecoverySinglePassIOTest`: with a WAL of 1000 segments × 1000 entries (10⁶ entries, ~100 MB), recovery reads the WAL from disk at most once per cached segment (verified via a counting FileChannel wrapper).

**Test additions:**
- `RecoverySinglePassIOTest`
- `RecoveryCacheLRUEvictionTest`

---

### WAL-7 — Cache decoded segment bodies in `RedoPhase.redo` (same fix as WAL-6)

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/recovery/RedoPhase.java`
**Method:** `public static RedoResult redo(WALManager wal, PageManager pages, CheckpointRecord checkpoint, MvccRedoSink mvccSink)` (lines 108–173)

**Pattern:** Same shape as WAL-6 — a single forward pass that calls `segment.readAll()` for every segment.

**Complexity:** O(n × m) per call = O(N) single-pass. This is the SECOND full read of the WAL during recovery.

**Root cause:** Same as WAL-6 — no cross-phase cache for decoded segment bodies.

**Required changes:** Apply the WAL-6 fix. Once `WALManager.readAllCached(int segmentNumber)` exists, replace `segment.readAll()` with `wal.readAllCached(segment.getSegmentNumber())` in `RedoPhase.redo`.

**Acceptance criteria:**
- `RedoTest`, `RedoPerformanceTest`, `RecoveryIntegrationTest` pass.
- Combined with WAL-6: `RecoverySinglePassIOTest` confirms at most one disk read per segment across all four recovery phases.

**Test additions:**
- (Covered by WAL-6's `RecoverySinglePassIOTest`)

---

### WAL-8 — Use `descendingMap()` directly in `UndoPhase.undo` instead of `Collections.sort` + TreeMap.get

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/recovery/UndoPhase.java`
**Method:** `public static UndoResult undo(WALManager wal, Set<Long> activeTxids, MvccUndoSink sink)` (lines 82–180)

**Pattern:**
```java
List<Integer> segmentNumbers = new ArrayList<>(wal.getSegments().keySet());  // O(n) copy
Collections.sort(segmentNumbers, Comparator.reverseOrder());                 // O(n log n) sort

for (Integer segmentNumber : segmentNumbers) {                              // n segments
    WALSegment segment = wal.getSegments().get(segmentNumber);             // O(log n) per TreeMap.get
    ...
}
```

**Complexity:** O(n × m) dominated by the entry scan; the O(n log n) sort and O(n log n) TreeMap.get calls are pure overhead.

**Root cause:** The code copies the TreeMap key set into an ArrayList, sorts it in reverse order, then re-looks-up each segment by number. `TreeMap.descendingMap()` already provides a reverse-ordered view of the entries with no copy or sort.

**Required changes:**
1. Replace the loop with:
   ```java
   for (WALSegment segment : wal.getSegments().descendingMap().values()) {
       if (segment.getFirstLsn() > endLsn && segment.getFirstLsn() != 0) continue;
       List<WALEntry> entries = wal.readAllCached(segment.getSegmentNumber());   // see WAL-6
       for (int i = entries.size() - 1; i >= 0; i--) { ... }
   }
   ```
2. Eliminates the O(n) ArrayList copy, the O(n log n) sort, and the O(n log n) per-iteration TreeMap.get.

**Acceptance criteria:**
- `UndoPhaseTest`, `UndoLogSpillTest`, `RollbackTest`, `RecoveryIntegrationTest` pass.
- New `UndoPhaseNoCopySortTest`: asserts no `ArrayList` allocation in the segment iteration path (via a test-mode counter).

**Test additions:**
- `UndoPhaseNoCopySortTest`

---

### WAL-9 — Use swap-reference instead of `active.clear()` in `AnalysisPhase.apply` CHECKPOINT branch

**Severity:** High
**Subsystem:** WAL
**Files:** `diesel/recovery/AnalysisPhase.java`
**Method:** `private static void apply(WALEntry entry, Set<Long> committed, Set<Long> active)` — CHECKPOINT case (lines 129–143)

**Pattern:**
```java
if (op == WALOpcode.CHECKPOINT) {
    CheckpointRecord record = CheckpointRecord.fromBytes(entry.getAfterImage());
    active.clear();                                       // O(A) — clears entire active set
    for (long checkpointTxid : record.getActiveTxids()) {  // O(C)
        active.add(checkpointTxid);
    }
    return;
}
```

**Complexity:** O(A + C) per CHECKPOINT record. Across K CHECKPOINT records in the WAL, total is O(K × (A_avg + C_avg)). Worst case K = O(N), A = O(N) → **O(N²)**.

**Root cause:** Each CHECKPOINT record wipes the in-progress `active` set by `LinkedHashSet.clear()` (which nulls every bucket) and re-seeds it from the checkpoint's embedded txid list. With frequent checkpoints and large active sets, this dominates the analysis pass.

**Required changes:**
1. Refactor `apply(...)` so the `active` set is owned by the caller (`AnalysisPhase.analyze`) as a mutable field, not a parameter.
2. In the CHECKPOINT branch, replace `active.clear(); for (...) active.add(...)` with:
   ```java
   active = new LinkedHashSet<>(record.getActiveTxidCount());
   for (long checkpointTxid : record.getActiveTxids()) active.add(checkpointTxid);
   ```
   This is O(C) per CHECKPOINT (the old set is dropped on GC, no per-bucket nulling).
3. Expose the new `active` reference back to the caller — the cleanest API is to make `apply` return a (possibly new) `Set<Long>` for `active`, OR to use a single-element array holder `Set<Long>[] activeHolder` to allow swap.
4. As a sanity check, ensure `CheckpointRecord.getActiveTxidCount()` exists (add it if not — derives from `getActiveTxids().length`).

**Acceptance criteria:**
- `AnalysisTest`, `AnalysisPerformanceTest`, `CheckpointRecordTest`, `CheckpointIntegrationTest`, `RecoveryIntegrationTest` pass.
- New `AnalysisCheckpointIsLinearTest`: a WAL with 1000 CHECKPOINT records each embedding 1000 active txids — `AnalysisPhase.analyze` completes in O(N), not O(N²).

**Test additions:**
- `AnalysisCheckpointIsLinearTest`

---

## 4. Buffer Loading Remediation Prompts

### Buffer-1 — Replace `CatalogTable.schemas` list with a `HashMap<String, CatalogSchema>` keyed by table name

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/CatalogTable.java`
**Methods:** `public CatalogSchema getTableSchema(String tableName)` (lines 139–146)

**Pattern:**
```java
public CatalogSchema getTableSchema(String tableName) {
    synchronized (lock) {
        return schemas.stream()
                .filter(schema -> schema.getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }
}
```

**Complexity:** O(N) per call. Over N table lookups the cumulative cost is O(N²).

**Root cause:** The catalog holds schemas in a plain `ArrayList<CatalogSchema>` and resolves by name via a linear stream scan. Every DDL operation calls `getTableSchema` at least once.

**Required changes:**
1. Add a `private final Map<String, CatalogSchema> schemasByName = new HashMap<>();` field. Update it on every `add`/`remove`/`replace` operation on `schemas`.
2. Replace the body of `getTableSchema(String)` with `return schemasByName.get(tableName);` — O(1).
3. Apply the same fix to `tableExists(String)` (line 155) — currently delegates to `getTableSchema != null`.
4. Make sure `CatalogSchema.getTableName()` is stable (not mutated after creation) — if it can change, the index must be invalidated.

**Acceptance criteria:**
- `CatalogTest`, `TablespaceTest`, `PersistenceTest` pass.
- New `CatalogLookupIsO1Test`: with 10⁵ tables in the catalog, `getTableSchema` completes in O(1) (microbenchmark).

**Test additions:**
- `CatalogLookupIsO1Test`

---

### Buffer-2 — Use `HashMap.put` for schema replace in `CatalogTable.updateTableSchema(CatalogSchema)`

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/CatalogTable.java`
**Method:** `public void updateTableSchema(CatalogSchema schema)` (lines 85–94)

**Pattern:**
```java
synchronized (lock) {
    schemas.removeIf(existing -> schema.getTableName().equals(existing.getTableName()));
    schemas.add(schema);
    flush();
}
```

**Complexity:** O(N) per call due to `removeIf`. Over N DDL updates: O(N²).

**Root cause:** Insert-replace is implemented as `List.removeIf(...)` over the schema list — no name index, so dedup-by-name requires touching every entry.

**Required changes:**
1. After implementing Buffer-1's `schemasByName` map, replace the body with:
   ```java
   synchronized (lock) {
       schemasByName.put(schema.getTableName(), schema);
       // Keep the list in sync if it is still used for ordered serialization:
       schemas.removeIf(existing -> schema.getTableName().equals(existing.getTableName()));
       schemas.add(schema);
       flush();
   }
   ```
2. Better: remove the `schemas` list entirely if it is only used for ordered serialization — replace the serializer with `schemasByName.values().iterator()`.
3. Update `updateTableSchema(String tableName, ...)` (line 104) to use `schemasByName.containsKey(tableName)` instead of the current `getTableSchema != null` precheck.

**Acceptance criteria:**
- `CatalogTest`, `TablespaceTest`, `PersistenceTest` pass.
- New `CatalogUpdateIsO1Test`: with 10⁵ tables, `updateTableSchema` completes in O(1).

**Test additions:**
- `CatalogUpdateIsO1Test`

---

### Buffer-3 — Use `HashMap.remove` for `CatalogTable.dropTableSchema`

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/CatalogTable.java`
**Method:** `public void dropTableSchema(String tableName)` (lines 114–120)

**Pattern:**
```java
synchronized (lock) {
    if (schemas.removeIf(existing -> tableName.equals(existing.getTableName()))) {
        flush();
    }
}
```

**Complexity:** O(N) per call. Over N drops: O(N²).

**Root cause:** Same `List.removeIf` pattern as Buffer-2; no name index.

**Required changes:**
1. After Buffer-1's `schemasByName` map exists, replace the body with:
   ```java
   synchronized (lock) {
       if (schemasByName.remove(tableName) != null) {
           // Keep list in sync if it is still used:
           schemas.removeIf(existing -> tableName.equals(existing.getTableName()));
           flush();
       }
   }
   ```
2. Better: drop the `schemas` list entirely as in Buffer-2.

**Acceptance criteria:**
- `CatalogTest`, `TablespaceTest`, `PersistenceTest` pass.
- New `CatalogDropIsO1Test`: with 10⁵ tables, dropping one completes in O(1).

**Test additions:**
- `CatalogDropIsO1Test`

---

### Buffer-4 — Maintain a `LinkedHashSet<PageId>` of dirty pages in `BufferPool`

**Severity:** Critical
**Subsystem:** Buffer
**Files:** `diesel/storage/page/BufferPool.java`, `diesel/storage/page/Page.java`
**Methods:** `public int getDirtyPageCount()` (lines 422–435), `public int flushDirtyMatching(Predicate<Page> eligible)` (lines 447–478), `private IOException flushResidentDirty()` (lines 553–579)

**Pattern:**
```java
// getDirtyPageCount()
for (BufferFrame frame : frames.values()) {
    if (frame.page.isDirty()) { count++; }
}

// flushDirtyMatching()
List<Page> dirty = new ArrayList<>();
for (BufferFrame frame : frames.values()) {
    if (frame.page.isDirty() && frame.pinCount == 0 && eligible.test(frame.page)) {
        dirty.add(frame.page);
    }
}
```

**Complexity:** O(F) per call (F = `frames.size()` up to `capacityPages`). The flusher calls `getDirtyPageCount` AND `flushDirtyMatching` every cycle — O(F) + O(F) per cycle. Over C cycles with F ≈ capacity: O(C × F) ≈ O(F²) when cycles scale with workload.

**Root cause:** The pool keeps a `HashMap<PageId, BufferFrame>` but no separate dirty-page index. Every dirty-page query walks all resident frames.

**Required changes:**
1. Add `private final LinkedHashSet<PageId> dirtyPageIds = new LinkedHashSet<>();` to `BufferPool` — preserves FIFO flush order.
2. Add a hook on `Page.setDirty(boolean)`: when transitioning from `false` to `true`, the pool must add the page's `PageId` to `dirtyPageIds`. The cleanest way: make `BufferPool` intercept dirty transitions via a `PageDirtyListener` callback (or expose `BufferPool.markDirty(PageId)` and have callers — primarily `Page.setDirty(true)` — invoke it).
3. After a successful `persistDirty(Page)`, remove the page's `PageId` from `dirtyPageIds`.
4. Replace `getDirtyPageCount()` body with `return dirtyPageIds.size();` — O(1).
5. Replace `flushDirtyMatching(...)` body with:
   ```java
   List<Page> dirty = new ArrayList<>();
   for (PageId pid : dirtyPageIds) {
       BufferFrame frame = frames.get(pid);
       if (frame != null && frame.pinCount == 0 && eligible.test(frame.page)) {
           dirty.add(frame.page);
       }
   }
   // flush dirty ...
   ```
   — O(D) where D ≤ F, instead of O(F).
6. Similarly update `flushResidentDirty()` to iterate `dirtyPageIds` instead of `frames.values()`.
7. Optionally maintain two sets — `dirtyPinned` and `dirtyUnpinned` — so the flusher can grab unpinned pages without re-checking `pinCount`.

**Acceptance criteria:**
- `BufferPoolTest`, `BufferPoolStressTest`, `BufferPoolFlusherTest`, `BufferPoolHitRateTest`, `BufferPoolThroughputTest` (if exists) all pass.
- New `DirtyPageCountIsO1Test`: with 10⁵ resident pages and 10³ dirty, `getDirtyPageCount` completes in < 100 ns.
- New `FlushDirtyOnlyIteratesDirtyTest`: byte-level counter confirms only dirty pages are touched during flush (verified via a counter on `Page.isDirty()` calls).

**Test additions:**
- `DirtyPageCountIsO1Test`
- `FlushDirtyOnlyIteratesDirtyTest`

---

### Buffer-5 — Maintain an unpinned-page queue in `LruEvictionPolicy` for O(1) eviction under any pin distribution

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/LruEvictionPolicy.java`, `diesel/storage/page/BufferPool.java`
**Methods:** `PageId pollEvictable(Predicate<PageId> evictable)` (LruEvictionPolicy.java lines 92–101); `private void evictForRoomLocked()` (BufferPool.java lines 514–536)

**Pattern:**
```java
PageId pollEvictable(Predicate<PageId> evictable) {
    for (Node node = head; node != null; node = node.next) {   // O(F) worst case
        if (evictable.test(node.pageId)) {
            detach(node);
            nodes.remove(node.pageId);
            return node.pageId;
        }
    }
    return null;
}
```

**Complexity:** O(F) worst case per call. Under a pin pattern that leaves many pages pinned at the head of the recency list, cumulative cost is O(F²).

**Root cause:** The LRU already uses a doubly-linked list + HashMap for O(1) `add`/`touch`/`remove`. But `pollEvictable` linearly scans from `head` until it finds an unpinned node. When many pinned frames are at the head (common after a long-lived batch pins the older pages), each eviction re-walks them. The `unpin` path does not move the page to the tail, so the LRU order can leave the oldest pages pinned at the head indefinitely.

**Required changes:**
1. Add a separate `ArrayDeque<PageId> unpinnedQueue` (or a second doubly-linked list) to `LruEvictionPolicy`.
2. On `unpin(PageId)` when `pinCount` transitions 1 → 0, push the pageId to the back of `unpinnedQueue`.
3. On `pin(PageId)` when `pinCount` transitions 0 → 1, mark the pageId as "stale" in the queue (lazy deletion on next eviction).
4. Replace `pollEvictable` body with:
   ```java
   while (!unpinnedQueue.isEmpty()) {
       PageId candidate = unpinnedQueue.poll();
       BufferFrame frame = frames.get(candidate);
       if (frame != null && frame.pinCount == 0 && evictable.test(candidate)) {
           // Remove from LRU list (it may be anywhere in the recency list)
           Node node = nodes.remove(candidate);
           if (node != null) detach(node);
           return candidate;
       }
       // else: stale entry — drop it
   }
   return null;
   ```
5. Make sure `BufferPool.evictForRoomLocked` still falls back correctly when `pollEvictable` returns null (all pages pinned — throw `BufferPoolFullException`).

**Acceptance criteria:**
- `BufferPoolTest`, `BufferPoolStressTest`, `PageManagerTest` pass.
- New `EvictionUnderPinnedHeadTest`: 1000 pages, 900 pinned at the LRU head, 100 unpinned at the tail — eviction of all 100 unpinned pages completes in O(100), not O(900 × 100).
- New `EvictionIsO1AmortizedTest`: microbenchmark confirms amortized O(1) per eviction.

**Test additions:**
- `EvictionUnderPinnedHeadTest`
- `EvictionIsO1AmortizedTest`

---

### Buffer-6 — Compact the slot directory in `SlottedPageLayout.defragment`

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/SlottedPageLayout.java`
**Method:** `public static int defragment(byte[] data, PageHeader header)` (lines 188–234)

**Pattern:**
```java
List<byte[]> liveTuples = new ArrayList<>();
List<Integer> liveSlotIds = new ArrayList<>();
for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
    int offset = readSlotOffset(data, slotId);
    if (offset != EMPTY_SLOT_OFFSET) {
        byte[] tuple = readTuple(data, header, slotId);
        liveTuples.add(tuple);
        liveSlotIds.add(slotId);
    }
}
// ... rewrites tuples to compacted positions but keeps liveSlotIds unchanged ...
// header.setFreeSpaceEnd(newFreeSpaceEnd) — slotCount NOT decremented, slot directory NOT compacted
```

**Complexity:** O(S) per call (S = `header.getSlotCount()`, grows monotonically with inserts). After heavy insert/delete churn, the slot directory becomes mostly tombstones and the O(S) scan is paid on every defrag.

**Root cause:** `defragment` compacts the tuple bytes against the page end, but leaves the slot directory untouched: tombstone slots stay in the directory and `slotCount` is never decremented. Each subsequent defrag still iterates every slot id ever allocated.

**Required changes:**
1. During defrag, also compact the slot directory: walk the live slots in order, rewrite each live slot entry at a new contiguous slotId starting from 0, and set `slotCount = liveTuples.size()`.
2. The mapping `oldSlotId → newSlotId` (for `liveSlotIds`) should be returned or applied so any external references (e.g., index pointers) can be updated. If the engine uses slot ids as stable identifiers (unlikely — physical slot ids are not stable), provide an `int[] oldToNew` remap.
3. Reclaim tombstone slots: `header.setFreeSpaceStart(PageHeader.HEADER_SIZE + liveTuples.size() * SLOT_ENTRY_SIZE)`.

**Acceptance criteria:**
- `PageTest`, `PageManagerTest`, `SlottedPageLayoutTest` (add if missing) pass.
- New `DefragCompactsSlotDirectoryTest`: after a workload of 1000 inserts + 900 deletes + 1 defrag, `header.getSlotCount()` equals 100 (the live count), not 1000.
- New `DefragIsOLiveTest`: defrag cost is proportional to live tuple count, not historical insert count.

**Test additions:**
- `DefragCompactsSlotDirectoryTest`
- `DefragIsOLiveTest`

---

### Buffer-7 — Reuse tombstoned slot ids in `SlottedPageLayout.insertTuple` via a free-slot stack

**Severity:** Critical
**Subsystem:** Buffer
**Files:** `diesel/storage/page/SlottedPageLayout.java`
**Method:** `public static int insertTuple(byte[] data, PageHeader header, byte[] tuple)` (lines 148–173)

**Pattern:**
```java
int tupleOffset = header.getFreeSpaceEnd() - tupleLength;
System.arraycopy(tuple, 0, data, tupleOffset, tupleLength);

int slotId = header.getSlotCount();            // always the next sequential id
writeSlot(data, slotId, tupleOffset, tupleLength);

header.setSlotCount(slotId + 1);               // never decremented, never reused
```

**Complexity:** O(1) per insert, but `slotCount` grows by 1 on every insert and is never decremented or reused, so after I inserts `slotCount = I`. Every O(slotCount) operation (`defragment`, `validateLayout`, full page scans) therefore costs O(I), not O(L) (L = live tuple count). Over a workload of I inserts interleaved with O(I) full-page scans/defrags, cumulative cost is O(I²). Also a memory/space leak: the slot directory consumes free space forever on churn-heavy pages.

**Root cause:** Deleted slots are tombstones (`offset = -1`), but `insertTuple` always takes `slotId = header.getSlotCount()` (the next unused id) rather than searching for a free tombstone. The slot directory never shrinks and never reuses ids.

**Required changes:**
1. Add a `private static final int FREE_SLOT_STACK_OFFSET` location in the page header (or use a small region after `PageHeader.HEADER_SIZE` reserved for a free-slot stack — bump `HEADER_SIZE` accordingly).
2. Maintain a free-slot stack: on `deleteTuple(slotId)`, push `slotId` onto the stack (write at the stack top position); on `insertTuple`, pop from the stack if non-empty, otherwise allocate `slotCount++`.
3. Update `defragment` (Buffer-6) to clear the free-slot stack after compacting the directory.
4. On page load, initialize the free-slot stack by scanning the slot directory for tombstones (O(slotCount) once per load — acceptable).
5. Alternatively (simpler): maintain a `BitSet freeSlots` of size `slotCount` in the page header area; `insertTuple` finds the first set bit (O(1) with `BitSet.nextSetBit`), `deleteTuple` sets the bit. The `BitSet` is persisted alongside the slot directory.

**Acceptance criteria:**
- `PageTest`, `SlottedPageLayoutTest`, `DefragTest` pass.
- New `InsertReusesTombstoneSlotsTest`: a workload of 1000 inserts + 500 deletes + 500 inserts on one page ends with `slotCount == 1000`, not 1500.
- New `SlotDirectoryDoesNotGrowUnboundedlyTest`: 10⁵ insert/delete cycles on one page leave `slotCount` bounded by max concurrent live tuples, not 10⁵.

**Test additions:**
- `InsertReusesTombstoneSlotsTest`
- `SlotDirectoryDoesNotGrowUnboundedlyTest`

---

### Buffer-8 — Make `validateLayout` and full-page scans O(live count) instead of O(historical inserts)

**Severity:** High
**Subsystem:** Buffer
**Files:** `diesel/storage/page/SlottedPageLayout.java`
**Method:** `public static boolean validateLayout(byte[] data, PageHeader header)` (lines 244–286)

**Pattern:**
```java
for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
    int offset = readSlotOffset(data, slotId);
    int length = readSlotLength(data, slotId);
    if (offset == EMPTY_SLOT_OFFSET) { ... continue; }
    ...
}
```

**Complexity:** O(S) per call where S = `header.getSlotCount()` (historical inserts). Over a workload of V validations / scans on a page with I historical inserts: O(V × I), which becomes O(I²) when V ~ I.

**Root cause:** `validateLayout` iterates all slot ids 0..slotCount-1, skipping tombstones but still paying the per-slot cost. Because `slotCount` grows monotonically with insert churn (Buffer-7) and `defragment` does not compact the directory (Buffer-6), validation and full-page scans cost O(I) where I = historical inserts, even when only L < I tuples are live.

**Required changes:**
1. Apply the Buffer-6 fix (defrag compacts the directory) and the Buffer-7 fix (insertTuple reuses tombstones). After both fixes, `slotCount` tracks the high-water mark of concurrent live slots, and `validateLayout` / full-page scans become O(L) where L = live tuples.
2. As a defensive measure, add a `liveCount` field to `PageHeader` (maintained on insert/delete) so `validateLayout` can short-circuit if the page is empty (`liveCount == 0`).

**Acceptance criteria:**
- `PageTest`, `SlottedPageLayoutTest` pass.
- New `ValidateLayoutIsOLiveTest`: validate cost is proportional to live tuple count.
- New `FullPageScanIsOLiveTest`: a full-page tuple iteration touches only live slots.

**Test additions:**
- `ValidateLayoutIsOLiveTest`
- `FullPageScanIsOLiveTest`

---

## 5. CSV Storage Remediation Prompts

The CSV storage backend (`CsvRowStorage`) is a thin wrapper that delegates all indexing to `DelimitedIndexManager` (via `AbstractRowStorage.syncIndex*`). All structural O(n²) hotspots live in `DelimitedIndexManager`. The single root architectural cause: `rowIdToPosition` is a one-way `NavigableMap<Long,Integer>` (rowId → position) with **no inverse map**. Resolving position → rowId, shifting positions on insert/delete, and rebuilding a secondary index all exploit a linear scan of the entry set instead of an O(1) lookup.

### CSV-1 — Add inverse `position → rowId` map; eliminate O(n) `positionToRowId` linear scan

**Severity:** Critical
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `private Long positionToRowId(int position)` (lines 579–586)

**Pattern:**
```java
private Long positionToRowId(int position) {
    for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
        if (e.getValue() == position) return e.getKey();
    }
    return null;
}
```

**Complexity:** O(n) per call; O(n²) when called inside a row loop (createIndex, updateRow, deleteRow).

**Root cause:** `rowIdToPosition` is a `TreeMap<Long,Integer>` keyed by rowId. The inverse lookup (position → rowId) is implemented by a full entry-set scan because no inverse `Map<Integer,Long>` is maintained.

**Required changes:**
1. Add `private final Map<Integer, Long> positionToRowIdMap = new HashMap<>();`.
2. Keep it in sync with `rowIdToPosition` on every mutation:
   - `insertAtShared(int, Object[])`: after `rowIdToPosition.put(rid, rowIndex)`, do `positionToRowIdMap.put(rowIndex, rid)`. Then for the position-shift loop, also update `positionToRowIdMap` (or replace the shift loop entirely using the new map — see CSV-4).
   - `deleteRow(int)`: before removing from `rowIdToPosition`, remove `positionToRowIdMap.remove(rowIndex)`. For the position-shift loop, update `positionToRowIdMap` accordingly.
   - `reindex()` / `compact()`: rebuild `positionToRowIdMap` from `rowIdToPosition` in O(n).
3. Replace the body of `positionToRowId(int)` with `return positionToRowIdMap.get(position);` — O(1).

**Acceptance criteria:**
- `CsvStorageTest`, `CsvStorageAdvancedTest`, `TsvStorageTest`, `TsvStorageAdvancedTest`, `CsvIndexManagerTest`, `CsvTsvHeaderMappingTest` all pass.
- New `PositionToRowIdIsO1Test`: with 10⁵ rows, `positionToRowId` completes in < 100 ns.
- New `InverseMapConsistencyTest`: after every insert/delete/update/reindex/compact, assert `positionToRowIdMap` and `rowIdToPosition` are inverses.

**Test additions:**
- `PositionToRowIdIsO1Test`
- `InverseMapConsistencyTest`

---

### CSV-2 — Fix `createIndex(String)` to use the inverse map (O(n log n) instead of O(n²))

**Severity:** Critical
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `public boolean createIndex(String column)` (lines 221–245)

**Pattern:**
```java
for (int i = 0; i < rows.size(); i++) {
    Long rid = positionToRowId(i);            // O(n) per iteration (CSV-1)
    if (rid == null) continue;
    Object key = rowColumns.get(rows.get(i), canonical);
    if (key != null) {
        index.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
    }
}
```

**Complexity:** O(n²) per single call. Building m secondary indexes makes it O(m × n²).

**Root cause:** For each row, `positionToRowId(i)` linearly scans `rowIdToPosition.entrySet()`. On a freshly built table the entries are sorted by rowId, so locating the entry with value `== i` takes O(i) — giving Σ i ≈ n²/2 iterations.

**Required changes:**
1. After CSV-1, `positionToRowId(i)` is O(1), so the loop becomes O(n log c) for the TreeMap insert (c = columns, constant) → O(n log n) per index.
2. Alternatively, drive `createIndex` off `rowIdToPosition.entrySet()` directly (which already yields (rowId, position) pairs) rather than iterating positions and reverse-resolving:
   ```java
   for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
       long rid = e.getKey();
       int position = e.getValue();
       Object key = rowColumns.get(rows.get(position), canonical);
       if (key != null) {
           index.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
       }
   }
   ```
3. Either approach is correct; the second avoids depending on the new map.

**Acceptance criteria:**
- `CsvIndexManagerTest`, `TsvStorageTest`, `AutoWhereIndexTest` pass.
- New `CreateIndexIsONLogNTest`: with 10⁵ rows, `createIndex` completes in < 100 ms (today: seconds).

**Test additions:**
- `CreateIndexIsONLogNTest`

---

### CSV-3 — Fix `updateRow` to use the inverse map (O(log n + k) instead of O(n))

**Severity:** Critical
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `public void updateRow(Object[] oldRow, int rowIndex, Object[] newRow)` (lines 346–353)

**Pattern:**
```java
public void updateRow(Object[] oldRow, int rowIndex, Object[] newRow) {
    Long rid = positionToRowId(rowIndex);   // O(n) linear scan
    if (rid == null) return;
    removeIndexedRow(oldRow, rid);           // O(k) per secondary key list (CSV-6)
    insertIndexedRow(newRow, rid);           // O(m log n) for m indexes
}
```

**Complexity:** O(n + k + m log n) per single update. Driven by an engine row-by-row UPDATE loop (n rows), total is O(n²).

**Root cause:** The dominant cost is `positionToRowId(rowIndex)`. Compounded by `removeIndexedRow`'s O(k) posting-list scan (CSV-6).

**Required changes:**
1. Apply CSV-1: `positionToRowId(rowIndex)` becomes O(1).
2. Apply CSV-6: `removeIndexedRow` becomes O(1) per secondary index via `Set.remove` instead of `List.remove(Object)`.
3. After both fixes, single update becomes O(log n + 1) = O(log n); batch of n updates becomes O(n log n).

**Acceptance criteria:**
- `UpdateTest`, `CsvStorageAdvancedTest`, `TsvStorageAdvancedTest` pass.
- New `UpdateRowIsOLogNTest`: with 10⁵ rows, batch update of 10³ rows completes in O(10³ × log n), not O(10⁵ × 10³).

**Test additions:**
- `UpdateRowIsOLogNTest`

---

### CSV-4 — Defer `insertAtShared` position-shift to `reindex()` via bulk mode, or use the inverse map

**Severity:** Critical
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `void insertAtShared(int rowIndex, Object[] row)` (lines 382–403)

**Pattern:**
```java
if (rowIndex >= rows.size()) {
    rows.add(row);
} else {
    rows.add(rowIndex, row);
    for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {  // O(n) scan
        if (e.getValue() >= rowIndex && e.getKey() != rid) {
            e.setValue(e.getValue() + 1);
        }
    }
}
```

**Complexity:** O(n) per single call. Driven by an engine row-by-row INSERT, n insertions in a loop are O(n²).

**Root cause:** When inserting at any position other than the tail, every later row's position in `rowIdToPosition` must be bumped by +1. The implementation walks the entire entry set and calls `setValue` on those whose value ≥ `rowIndex`.

**Required changes:**
1. After CSV-1, the inverse `positionToRowIdMap` exists. Replace the entry-set walk with a position-indexed shift:
   ```java
   // Shift positions in the inverse map (using a tail-iteration of a TreeMap<Integer,Long> if positionToRowIdMap is a TreeMap):
   for (int p = rows.size() - 1; p >= rowIndex; p--) {
       Long rid2 = positionToRowIdMap.remove(p);
       if (rid2 != null) {
           positionToRowIdMap.put(p + 1, rid2);
           rowIdToPosition.put(rid2, p + 1);
       }
   }
   ```
   This is still O(n) per insert in the worst case but with cache-friendly linear access and no per-entry boxing.
2. Better: wrap every multi-statement mutation in `beginBulkUpdate()` / `endBulkUpdate()` — the bulk path defers this to a single `reindex()` at the end. Make the engine use the bulk path for batch INSERT statements (set `bulkMode = true` for the duration of the batch).
3. For the residual per-row insert-at-position path, consider switching the live-rows representation to `ArrayList<Long> positionToRowIdList` (indexed by position) so a shift becomes `positionToRowIdList.add(rowIndex, rid)` — O(n) but with `System.arraycopy` backing it (much smaller constant).

**Acceptance criteria:**
- `InsertQueryTest`, `BulkInsertTest`, `CsvStorageTest`, `TsvStorageTest` pass.
- New `BatchInsertIsONTest`: 10⁵ rows inserted as a batch complete in O(n), not O(n²).
- New `SingleInsertIsONTest`: 10³ single-row inserts complete in O(n) total (today: O(n²)).

**Test additions:**
- `BatchInsertIsONTest`
- `SingleInsertIsONTest`

---

### CSV-5 — Defer `deleteRow` position-shift to `reindex()` via bulk mode, or use the inverse map

**Severity:** Critical
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `public void deleteRow(int rowIndex)` (lines 418–444)

**Pattern:**
```java
Long rid = positionToRowId(rowIndex);             // O(n) (CSV-1)
Object[] row = rows.remove(rowIndex);
removeIndexedRow(row, rid);                        // O(k) (CSV-6)
rowIdToPosition.remove(rid);
deletedRowIds.add(rid);
for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {  // O(n) scan
    if (e.getValue() > rowIndex) {
        e.setValue(e.getValue() - 1);
    }
}
```

**Complexity:** O(n + k) per single call (two O(n) entry-set scans). Driven by an engine row-by-row DELETE loop, n deletions are O(n²).

**Root cause:** Two O(n) entry-set scans per delete — first `positionToRowId(rowIndex)` to look up the rowId, then a second pass to decrement every later row's position by 1.

**Required changes:**
1. Apply CSV-1: `positionToRowId(rowIndex)` becomes O(1).
2. Replace the position-shift loop with the inverse-map-based shift (mirror of CSV-4):
   ```java
   for (int p = rowIndex; p < rows.size(); p++) {
       Long rid2 = positionToRowIdMap.remove(p + 1);
       if (rid2 != null) {
           positionToRowIdMap.put(p, rid2);
           rowIdToPosition.put(rid2, p);
       }
   }
   ```
3. Better: wrap engine deletes in `beginBulkUpdate()` / `endBulkUpdate()` — the bulk path already defers to a single `reindex()`.
4. The compaction trigger (`COMPACTION_THRESHOLD`) is fine; just make sure the threshold check uses `positionToRowIdMap.size()` for O(1) instead of `rowIdToPosition.size()` (also O(1) for TreeMap, so no change).

**Acceptance criteria:**
- `DeleteQuery` tests, `CsvStorageTest`, `TsvStorageTest`, `LazyDeleteTest` pass.
- New `BatchDeleteIsONTest`: 10⁵ rows deleted as a batch complete in O(n).

**Test additions:**
- `BatchDeleteIsONTest`

---

### CSV-6 — Replace secondary-index posting lists `List<Long>` with `LinkedHashSet<Long>` (O(1) remove)

**Severity:** High
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `public void removeIndexedRow(Object[] row, long rowId)` (lines 316–336)

**Pattern:**
```java
for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
    Object key = rowColumns.get(row, entry.getKey());
    if (key == null) continue;
    List<Long> ids = entry.getValue().get(key);
    if (ids != null) {
        ids.remove(rowId);                            // O(k) ArrayList.remove(Object)
        if (ids.isEmpty()) {
            entry.getValue().remove(key);
        }
    }
}
```

**Complexity:** O(k) per call per secondary index, where k = number of rowIds sharing the same secondary-key value. For low-cardinality indexes (e.g. foreign keys, status flags), k = O(n). Combined with CSV-3 / CSV-5, n updates/deletes in a loop are O(n × k) = O(n²).

**Root cause:** `ids` is `ArrayList<Long>`. `ArrayList.remove(Object)` linearly scans the list to find the first element `.equals(rowId)` and then calls `System.arraycopy` to close the gap.

**Required changes:**
1. Change the secondary index value type from `NavigableMap<Object, List<Long>>` to `NavigableMap<Object, LinkedHashSet<Long>>` (preserves insertion order, O(1) remove). Update all read sites that iterate `ids` (they iterate just the same with `LinkedHashSet`).
2. If `rangeSearch` requires sorted iteration by rowId, switch to `ConcurrentSkipListSet<Long>` (still O(log k) remove, but sorted).
3. If the index must remain a `List` for serialization compatibility, maintain a side `HashMap<Long, Integer> rowIdToSlotInIndex` per posting list and use swap-remove: `int slot = rowIdToSlotInIndex.remove(rowId); int last = ids.size() - 1; ids.set(slot, ids.get(last)); ids.remove(last);` — O(1).
4. Update `insertIndexedRow` to push to the new collection type.
5. Update serialization (if any) of the secondary index to handle the new type.

**Acceptance criteria:**
- `CsvIndexManagerTest`, `TsvIndexManagerTest`, `AutoJoinIndexTest`, `CoveringIndexTest`, `CompositeIndexTest` pass.
- New `RemoveIndexedRowIsO1Test`: with 10⁵ rows under a low-cardinality key, `removeIndexedRow` completes in O(1) per call.

**Test additions:**
- `RemoveIndexedRowIsO1Test`

---

### CSV-7 — Wrap engine multi-row mutations in `beginBulkUpdate()` / `endBulkUpdate()`

**Severity:** High
**Subsystem:** CSV (and TSV — shared)
**Files:** `diesel/storage/CsvRowStorage.java`, `diesel/storage/AbstractRowStorage.java`, `diesel/Table.java` (engine path)
**Methods:** `insertAt(int, Map)` (lines 98–104), `update(int, Map)` (lines 106–112), `delete(int)` (lines 114–117)

**Pattern:**
```java
@Override
public synchronized void insertAt(int rowIndex, Map<String, Object> row) {
    Object[] arr = rowColumns.fromMap(row);
    rows.add(rowIndex, arr);                     // O(n - rowIndex) ArrayList shift
    syncIndexInsert(arr, rowIndex);              // → manager.insertAtShared (CSV-4) — O(n)
}
```

**Complexity:** O(n) per single call. Driven by an engine loop performing n operations → O(n²).

**Root cause:** Two superimposed O(n) costs: (1) `ArrayList.add(int, E)` / `ArrayList.remove(int)` themselves shift up to n subsequent elements; (2) the delegated `DelimitedIndexManager` mutation adds another O(n) from CSV-3/CSV-4/CSV-5. The bulk path (`beginBulkUpdate()` / `endBulkUpdate()`) is the only one that batches; row-by-row statements bypass it.

**Required changes:**
1. In `Table.insert` / `Table.update` / `Table.delete`, detect batch operations (e.g., INSERT with multiple VALUES, UPDATE with a predicate matching many rows, DELETE with a predicate matching many rows) and wrap them in `storage.beginBulkUpdate()` ... `storage.endBulkUpdate()`.
2. Add a public `AbstractRowStorage.runInBulkMode(Runnable)` helper that wraps the bulk-mode lifecycle for callers.
3. For non-batch single-row statements, the per-row O(n) cost is acceptable (one row, one shift). The optimization targets multi-statement batches.
4. After CSV-1, CSV-4, CSV-5, CSV-6 fixes, the residual per-row cost is dominated by the inherent `ArrayList` shift, which is fine for a single row but not for n rows.

**Acceptance criteria:**
- `BulkInsertTest`, `BatchQueryTest`, `BatchExecutionTest`, `StorageBulkUpdateTest` pass.
- New `EngineBatchInsertIsONTest`: a single SQL `INSERT INTO t VALUES (...), (...), ..., (...)` with 10⁵ rows completes in O(n).

**Test additions:**
- `EngineBatchInsertIsONTest`

---

## 6. TSV Storage Remediation Prompts

**Key finding:** The four TSV-specific files (`TsvRowStorage`, `TsvRowReader`, `TsvRowWriter`, `TsvIndexManager`) contain NO direct O(n²) hotspots. `TsvIndexManager` is a 7-line subclass of `DelimitedIndexManager` with zero method overrides. Every O(n²) hotspot lives in the shared `DelimitedIndexManager`, used by both `CsvIndexManager` and `TsvIndexManager`.

Therefore the TSV remediation prompts are identical to CSV-1 through CSV-7 — applying those fixes also fixes TSV. They are repeated here for completeness with explicit "shared CSV+TSV" scope markings.

### TSV-1 — Same as CSV-1 (shared CSV+TSV)

Apply CSV-1. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageTest`, `TsvStorageAdvancedTest`, `CsvTsvHeaderMappingTest`.

### TSV-2 — Same as CSV-2 (shared CSV+TSV)

Apply CSV-2. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageTest`, `TsvStorageAdvancedTest`.

### TSV-3 — Same as CSV-3 (shared CSV+TSV)

Apply CSV-3. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageAdvancedTest`, `UpdateTest`.

### TSV-4 — Same as CSV-4 (shared CSV+TSV)

Apply CSV-4. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageTest`, `InsertQueryTest`, `BulkInsertTest`.

### TSV-5 — Same as CSV-5 (shared CSV+TSV)

Apply CSV-5. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageTest`, `LazyDeleteTest`.

### TSV-6 — Same as CSV-6 (shared CSV+TSV)

Apply CSV-6. **Scope:** shared CSV+TSV. **Verification tests:** `TsvStorageAdvancedTest`, `AutoJoinIndexTest`, `CoveringIndexTest`, `CompositeIndexTest`.

### TSV-7 — `removeDeadEntries` uses the inverse map (shared CSV+TSV)

**Severity:** High
**Subsystem:** TSV (and CSV — shared)
**Files:** `diesel/storage/DelimitedIndexManager.java`
**Method:** `public int removeDeadEntries(Set<Integer> deadPositions)` (lines 476–497)

**Pattern:**
```java
List<Long> deadRowIds = new ArrayList<>(deadPositions.size());
for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {  // O(n) full walk
    if (deadPositions.contains(e.getValue())) {                  // O(1)
        deadRowIds.add(e.getKey());
    }
}
for (Long rid : deadRowIds) {
    Integer pos = rowIdToPosition.remove(rid);
    if (pos == null) continue;
    Object[] row = (pos >= 0 && pos < rows.size()) ? rows.get(pos) : null;
    if (row != null) removeIndexedRow(row, rid);                  // O(s * k) — CSV-6
}
return deadRowIds.size();
```

**Complexity:** O(n + d × s × k) per call. When d = O(n) and k = O(n), it is O(n²).

**Root cause:** Even when the caller passes in a tiny `deadPositions` set, the first loop walks the entire `rowIdToPosition` map (n entries) to find which rowIds match the dead positions — the same reverse-lookup problem as `positionToRowId` but in bulk.

**Required changes:**
1. After CSV-1, the inverse `positionToRowIdMap` exists. Replace the first loop with:
   ```java
   for (int pos : deadPositions) {
       Long rid = positionToRowIdMap.get(pos);
       if (rid != null) deadRowIds.add(rid);
   }
   ```
   — O(d) instead of O(n).
2. After CSV-6, the inner `removeIndexedRow` is O(s) per call (instead of O(s × k)). Combined: O(d × s).
3. Make sure `positionToRowIdMap` and `rowIdToPosition` are both updated in the second loop (remove from both).

**Acceptance criteria:**
- `VacuumTest`, `VacuumManagerTest`, `DefragTest`, `LazyDeleteTest` pass.
- New `RemoveDeadEntriesIsODTest`: with 10⁵ rows and d = 10² dead positions, `removeDeadEntries` completes in O(d), not O(n).

**Test additions:**
- `RemoveDeadEntriesIsODTest`

---

## 7. JSONL Storage Remediation Prompts

### JSONL-1 — Replace pairwise dot-prefix check in `JsonlSchemaManager.validateFlattenSchema` with sorted-walk

**Severity:** Medium
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlSchemaManager.java`
**Method:** `private void validateFlattenSchema()` (lines 118–132)

**Pattern:**
```java
for (int i = 0; i < this.columns.size(); i++) {
    for (int j = 0; j < this.columns.size(); j++) {
        if (i == j) continue;
        String a = this.columns.get(i);
        String b = this.columns.get(j);
        if (isDotPrefix(a, b)) {
            throw new DieselIOException("flatten schema conflict: ...");
        }
    }
}
```

**Complexity:** O(C²) where C = number of schema columns. Each pair is also checked twice — once as `(a,b)` and once as `(b,a)`.

**Root cause:** A brute-force pairwise dot-prefix check executed unconditionally from the constructor whenever `jsonl.nested.mode=FLATTEN` (the default). For wide tables (e.g., 5000 dot-notation leaf columns), this is 25 million string comparisons on every `JsonlSchemaManager` construction.

**Required changes:**
1. Sort the columns once: `List<String> sorted = new ArrayList<>(this.columns); sorted.sort(null);` — O(C log C).
2. Walk the sorted list comparing only adjacent pairs: `for (int i = 0; i + 1 < sorted.size(); i++) { if (isDotPrefix(sorted.get(i), sorted.get(i + 1))) throw ...; }` — O(C).
3. Or build a `TreeSet<String>` of column names and for each column `c` check only its ancestors `c.substring(0, dot)` membership — O(C log C).
4. Either way the check drops to O(C log C).

**Acceptance criteria:**
- `JsonlSchemaModeTest`, `JsonlNestedModeTest`, `JsonlStorageTest`, `JsonlSchemaProjectionTest` pass.
- New `ValidateFlattenSchemaIsONLogNTest`: with 5000 columns, `validateFlattenSchema` completes in < 50 ms (today: seconds).

**Test additions:**
- `ValidateFlattenSchemaIsONLogNTest`

---

### JSONL-2 — Add inverse position→rowId map in `JsonlIndexManager` (same as CSV-1)

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlIndexManager.java`
**Method:** `private Long positionToRowId(int position)` (lines 782–789)

**Pattern:**
```java
private Long positionToRowId(int position) {
    for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
        if (e.getValue() == position) return e.getKey();
    }
    return null;
}
```

**Complexity:** O(R) per call. Called from `updateRow` (line 252) and `deleteRow` (line 275), so K updates/deletes cost O(K × R) → O(n²) when K = O(R).

**Root cause:** Same as CSV-1 — reverse linear scan of the `rowIdToPosition` map. There is no reverse index.

**Required changes:**
1. Apply the same fix as CSV-1: add `private final Map<Integer, Long> positionToRowIdMap = new HashMap<>();` and keep it in sync with `rowIdToPosition` inside `appendRow`, `insertAt`, `deleteRow`, `reindex`, `adoptSidecar`.
2. Replace the body of `positionToRowId(int)` with `return positionToRowIdMap.get(position);` — O(1).

**Acceptance criteria:**
- `JsonlStorageTest`, `JsonlStorageAdvancedTest`, `JsonlIndexTest`, `JsonlLoadModeTest` pass.
- New `JsonlPositionToRowIdIsO1Test`: with 10⁵ rows, `positionToRowId` completes in < 100 ns.

**Test additions:**
- `JsonlPositionToRowIdIsO1Test`

---

### JSONL-3 — Defer `insertAt` and `deleteRow` position-shift to bulk mode or use inverse map (same as CSV-4 / CSV-5)

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlIndexManager.java`
**Methods:** `public void insertAt(int rowIndex, Object[] row)` (lines 230–245), `public void deleteRow(int rowIndex)` (lines 266–292)

**Pattern:** Same as CSV-4 / CSV-5 — an entry-set walk on `rowIdToPosition` to bump positions ≥ `rowIndex`.

**Complexity:** O(R) per insert/delete; O(K × R) = O(n²) for K = O(R) inserts/deletes.

**Root cause:** Same as CSV-4 / CSV-5.

**Required changes:**
1. Apply JSONL-2 first (inverse map exists).
2. Replace the entry-set walk with the inverse-map-based shift (mirror of CSV-4 / CSV-5):
   ```java
   for (int p = rows.size() - 1; p >= rowIndex; p--) {       // insert: shift up
       Long rid2 = positionToRowIdMap.remove(p);
       if (rid2 != null) {
           positionToRowIdMap.put(p + 1, rid2);
           rowIdToPosition.put(rid2, p + 1);
       }
   }
   ```
   And for delete:
   ```java
   for (int p = rowIndex; p < rows.size(); p++) {            // delete: shift down
       Long rid2 = positionToRowIdMap.remove(p + 1);
       if (rid2 != null) {
           positionToRowIdMap.put(p, rid2);
           rowIdToPosition.put(rid2, p);
       }
   }
   ```
3. Better: route all bulk inserts/deletes through `beginBulkUpdate()` / `endBulkUpdate()` (already implemented for `loadFromFile`).

**Acceptance criteria:**
- `JsonlStorageTest`, `JsonlAppendModeTest`, `JsonlLoadModeTest` pass.
- New `JsonlBatchInsertIsONTest`: 10⁵ rows inserted as a batch complete in O(n).

**Test additions:**
- `JsonlBatchInsertIsONTest`

---

### JSONL-4 — Switch secondary-index posting lists from `List<Long>` to `LinkedHashSet<Long>` (same as CSV-6)

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlIndexManager.java`
**Methods:** `insertIndexedRow`, `removeIndexedRow`, the field declaration of secondary indexes

**Pattern:** Same as CSV-6 — `ArrayList.remove(Object)` on the posting list per indexed-row removal.

**Complexity:** O(k) per call per secondary index; O(n × k) = O(n²) for low-cardinality keys.

**Root cause:** Same as CSV-6.

**Required changes:**
1. Apply the same fix as CSV-6: change `NavigableMap<Object, List<Long>>` to `NavigableMap<Object, LinkedHashSet<Long>>`.
2. Update `insertIndexedRow`, `removeIndexedRow`, and any serialization of secondary indexes.
3. If range queries require sorted iteration by rowId, use `ConcurrentSkipListSet<Long>` instead.

**Acceptance criteria:**
- `JsonlIndexTest`, `JsonlStorageAdvancedTest`, `JsonlParallelLoadTest` pass.
- New `JsonlRemoveIndexedRowIsO1Test`: with 10⁵ rows under a low-cardinality key, `removeIndexedRow` completes in O(1).

**Test additions:**
- `JsonlRemoveIndexedRowIsO1Test`

---

### JSONL-5 — Add `IdentityHashMap<Object[], Integer> rowToBaseLine` in `JsonlDeltaManager`

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlDeltaManager.java`
**Method:** `private int findBaseLineForValue(Object[] target)` (lines 117–127)

**Pattern:**
```java
private int findBaseLineForValue(Object[] target) {
    for (int i = 0; i < baseRows.size(); i++) {
        if (deletedBaseLines.contains(i)) continue;
        if (Arrays.equals(baseRows.get(i), target)) return i;
    }
    return -1;
}
```

**Complexity:** O(B × m) per call where B = `baseRows.size()` and m = column count. K base-row deletes cost O(K × B × m) → O(n² × m) when K = O(B).

**Root cause:** In append mode (`jsonl.write.mode=APPEND`), `onDelete` does not track which base line a row came from, so it falls back to a full content scan over `baseRows` to find the line number to tombstone.

**Required changes:**
1. In `onLoad(List<Object[]> baseRows, ...)`, build an `IdentityHashMap<Object[], Integer> rowToBaseLine` that maps each loaded row reference to its base-line index. (Use identity — the rows are not cloned between `baseRows` and the live `rows`.)
2. In `findBaseLineForValue(Object[] target)`, replace the linear scan with `return rowToBaseLine.getOrDefault(target, -1);` — O(1).
3. On `onDelete(...)` for a base-row branch, use the identity lookup. For pending rows, the existing `pendingNewRows.remove(int)` path is used.
4. If the storage clones rows between load and delete (verify by reading `JsonlRowStorage.ensureMaterialized`), use a content-based `HashMap<List<Object>, Integer>` instead — wrap each row in `Arrays.asList(row)` for hashing.

**Acceptance criteria:**
- `JsonlAppendModeTest`, `JsonlLoadModeTest`, `JsonlStorageAdvancedTest` pass.
- New `JsonlFindBaseLineIsO1Test`: with 10⁵ base rows and 10³ deletes, `findBaseLineForValue` completes in O(1).

**Test additions:**
- `JsonlFindBaseLineIsO1Test`

---

### JSONL-6 — Make `JsonlDeltaManager.onDelete` O(1) via the identity map (depends on JSONL-5)

**Severity:** Medium
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlDeltaManager.java`
**Method:** `public void onDelete(int rowIndex, List<Object[]> currentRows)` (lines 93–111)

**Pattern:**
```java
if (rowIndex >= pendingOffset && rowIndex - pendingOffset < pendingNewRows.size()) {
    int pendingIdx = rowIndex - pendingOffset;
    pendingNewRows.remove(pendingIdx);          // O(P - pendingIdx) ArrayList shift
    pendingNewPresence.remove(pendingIdx);
    return;
}
int lineNum = findBaseLineForValue(row);       // O(B * m) — JSONL-5
if (lineNum >= 0) { deletedBaseLines.add(lineNum); ... }
```

**Complexity:** O(P) for pending-row deletes; O(B × m) for base-row deletes. K deletes cost O(K × (B × m + P)) → O(n² × m) when K = O(B).

**Root cause:** `JsonlRowStorage.update` and `JsonlRowStorage.delete` call this on every mutation in append mode. Combined with the O(R) ArrayList shifts in `rows.remove` / `rowPresence.remove` (JSONL-8), a bulk delete in append mode against a large base file is O(n² × m).

**Required changes:**
1. Apply JSONL-5 first (identity map exists).
2. For the pending-row branch, swap `pendingNewRows` / `pendingNewPresence` to a single `ArrayList<PendingRow>` of (row, presence) pairs so one `remove(int)` covers both.
3. Even better, use `LinkedHashMap<Object[], Boolean>` keyed by row reference to make pending delete O(1) — but iterate ordering must be preserved.

**Acceptance criteria:**
- `JsonlAppendModeTest`, `JsonlLoadModeTest`, `JsonlStorageAdvancedTest` pass.
- New `JsonlDeleteInAppendModeIsO1Test`: with 10⁵ base rows and 10³ deletes, `onDelete` completes in O(1) per call (plus the inherent ArrayList shift cost).

**Test additions:**
- `JsonlDeleteInAppendModeIsO1Test`

---

### JSONL-7 — Replace per-tombstone `rows.remove(lineNum)` in `applyDelta` with a single two-pointer compaction

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlRowStorage.java`
**Method:** `private void applyDelta(String deltaPath)` (lines 830–857)

**Pattern:**
```java
List<Integer> sortedDeletes = new ArrayList<>(deltaManager.getDeletedBaseLines());
sortedDeletes.sort((a, b) -> b - a); // reverse order
for (int lineNum : sortedDeletes) {
    if (lineNum < rows.size()) {
        rows.remove(lineNum);          // O(R - lineNum) ArrayList shift
        rowPresence.remove(lineNum);   // O(R - lineNum) ArrayList shift
    }
}
```

**Complexity:** O(D × R) per load where D = number of recorded deleted base lines and R = current row count → O(n²) when D = O(R).

**Root cause:** Every load in append mode replays the delta: for each tombstoned base line it calls `rows.remove(lineNum)` and `rowPresence.remove(lineNum)`, both of which shift the trailing tail by one. With many deletions accumulated across saves, the load becomes quadratic in the dead-row count.

**Required changes:**
1. Build a `boolean[] deleted` mask over the loaded rows in O(D + R):
   ```java
   boolean[] deleted = new boolean[rows.size()];
   for (int lineNum : deltaManager.getDeletedBaseLines()) {
       if (lineNum < deleted.length) deleted[lineNum] = true;
   }
   ```
2. Compact once with a two-pointer sweep:
   ```java
   int writeIdx = 0;
   for (int readIdx = 0; readIdx < rows.size(); readIdx++) {
       if (!deleted[readIdx]) {
           if (writeIdx != readIdx) {
               rows.set(writeIdx, rows.get(readIdx));
               rowPresence.set(writeIdx, rowPresence.get(readIdx));
           }
           writeIdx++;
       }
   }
   // Truncate
   while (rows.size() > writeIdx) rows.remove(rows.size() - 1);
   while (rowPresence.size() > writeIdx) rowPresence.remove(rowPresence.size() - 1);
   ```
3. This replaces the D `ArrayList.remove` calls with a single O(R) linear pass.

**Acceptance criteria:**
- `JsonlAppendModeTest`, `JsonlLoadModeTest`, `JsonlStorageAdvancedTest` pass.
- New `ApplyDeltaIsORTest`: with R = 10⁵ rows and D = 10⁴ deletes, `applyDelta` completes in O(R), not O(D × R).

**Test additions:**
- `ApplyDeltaIsORTest`

---

### JSONL-8 — Funnel engine deletes/inserts through `beginBulkUpdate()` / `endBulkUpdate()` (same as CSV-7)

**Severity:** High
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlRowStorage.java`, `diesel/storage/AbstractRowStorage.java`, `diesel/Table.java` (engine path)
**Methods:** `delete(int rowIndex)` (lines 262–271), `insertAt(int rowIndex, Map<String,Object> row)` (lines 232–242)

**Pattern:** Standard `ArrayList.remove(int)` / `add(int, E)` shift cost, compounded by JSONL-3 and JSONL-6.

**Complexity:** O(R) per call; K calls cost O(K × R) → O(n²) when K = O(R).

**Root cause:** Standard in-memory row-store cost — each clustered delete/insert shifts the trailing tail of an `ArrayList`. The JSONL backend inherits it (same as CSV/TSV) but compounds it with `JsonlIndexManager` (JSONL-3) and `JsonlDeltaManager` (JSONL-6).

**Required changes:**
1. Apply the same fix as CSV-7: in `Table.insert` / `Table.update` / `Table.delete`, detect batch operations and wrap them in `storage.beginBulkUpdate()` ... `storage.endBulkUpdate()`.
2. For high-churn clustered workloads, consider switching the live row store to a gap-buffered list or a `LinkedHashMap` keyed by rowId — but this is a larger architectural change.

**Acceptance criteria:**
- `JsonlStorageTest`, `BulkInsertTest`, `BatchQueryTest` pass.
- New `JsonlEngineBatchInsertIsONTest`: a single SQL `INSERT INTO t VALUES (...), ..., (...)` with 10⁵ rows completes in O(n).

**Test additions:**
- `JsonlEngineBatchInsertIsONTest`

---

### JSONL-9 — Use a `TreeSet<String>` (case-insensitive) for duplicate suppression in `planSchemaForLoad`

**Severity:** Medium
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlRowStorage.java`
**Method:** `private static int indexOfIgnoreCase(List<String> names, String name)` (lines 1067–1074), called from `planSchemaForLoad` (lines 975–991)

**Pattern:**
```java
private static int indexOfIgnoreCase(List<String> names, String name) {
    for (int i = 0; i < names.size(); i++) {
        if (names.get(i).equalsIgnoreCase(name)) return i;
    }
    return -1;
}

// In planSchemaForLoad:
for (JsonlSchemaManager.SchemaColumn column : sidecar.columns()) {
    if (indexOfIgnoreCase(planColumns, column.name()) < 0) {     // O(C_p)
        planColumns.add(column.name());
        ...
    }
}
```

**Complexity:** O(C_s × C_p) per load → O(C²) when both lists are O(C). Runs once per load in `HYBRID` (default) and `INFERRED` modes.

**Root cause:** `planSchemaForLoad` builds the merged schema by linear-scanning `planColumns` for every sidecar/inferred column. As the schema grows, this becomes a quadratic duplicate-suppression pass on every load.

**Required changes:**
1. Build a `TreeSet<String>` (with `String.CASE_INSENSITIVE_ORDER`) or `HashSet<String>` (lowercased) from `planColumns` once at the start of `planSchemaForLoad` — O(C_p).
2. Replace `indexOfIgnoreCase(planColumns, name) < 0` with `!planColumnsSet.contains(name)` — O(1) (HashSet) or O(log C_p) (TreeSet).
3. Keep `planColumns` as the ordered list (for schema column order), and update both the list and the set on every `add`.
4. The whole merge drops to O(C_s + C_p) = O(C).

**Acceptance criteria:**
- `JsonlSchemaModeTest`, `JsonlSchemaProjectionTest`, `SchemaEvolutionTest`, `JsonlLoadModeTest` pass.
- New `PlanSchemaForLoadIsONTest`: with 10⁴ sidecar columns and 10⁴ plan columns, `planSchemaForLoad` completes in O(C).

**Test additions:**
- `PlanSchemaForLoadIsONTest`

---

### JSONL-10 — Precompute `Map<Integer, List<ProjectionSlot>> columnIndexToSlots` in `JsonlRowReader.setProjection`

**Severity:** Medium
**Subsystem:** JSONL
**Files:** `diesel/storage/JsonlRowReader.java`
**Method:** `private void nextProjectedJsonColumn(JsonStreamParser p, Object[] values, int slots, boolean[] seenColumn)` (lines 736–781)

**Pattern:**
```java
while (p.nextToken() != JsonEvent.END_OBJECT) {                  // outer: F fields
    ...
    Integer idx = indexByName.get(field);
    ...
    Object value = parseFieldValue(p, idx, field, valueToken);
    for (int s = 0; s < slots; s++) {                            // inner: S slots
        JsonlSchemaManager.ProjectionSlot slot = projectionSlots.get(s);
        if (slot.columnIndex() != idx) { continue; }
        ...
    }
}
```

**Complexity:** O(F × S) per row where F = number of needed fields parsed and S = projection slot count → O(R × C²) per scan when S = O(F) = O(C).

**Root cause:** In `json_column` mode, every parsed field linearly scans the whole projection slot list to find slots that map to its column index. A wide projection (e.g. `SELECT col1, ..., colN FROM t` over many JSON columns) turns each row's parse into a nested loop.

**Required changes:**
1. In `setProjection`, precompute `private Map<Integer, List<ProjectionSlot>> columnIndexToSlots = new HashMap<>();` populated by iterating `projectionSlots` once and bucketing each slot by its `columnIndex()`.
2. Replace the inner loop with:
   ```java
   for (ProjectionSlot slot : columnIndexToSlots.getOrDefault(idx, List.of())) {
       ...
   }
   ```
   — O(slots-for-this-column) per field, typically O(1).
3. The per-row cost drops from O(F × S) to O(F + S).

**Acceptance criteria:**
- `JsonlProjectionPushdownTest`, `JsonlIndexTest`, `JsonlStorageAdvancedTest` pass.
- New `JsonlProjectionIsOFPlusSTest`: with F = 100 fields and S = 100 projection slots, per-row parse completes in O(F + S).

**Test additions:**
- `JsonlProjectionIsOFPlusSTest`

---

### JSONL-11 — Make `JsonPathResolver.resolve` an instance method that reuses a schema index

**Severity:** Medium
**Subsystem:** JSONL
**Files:** `diesel/storage/json/JsonPathResolver.java`, `diesel/storage/JsonlSchemaManager.java`
**Method:** `public static ResolvedPath resolve(Collection<String> columns, String path)` (lines 66–95)

**Pattern:**
```java
public static ResolvedPath resolve(Collection<String> columns, String path) {
    ...
    List<String> orderedColumns = new ArrayList<>(columns);
    TreeMap<String, Integer> index = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (int i = 0; i < orderedColumns.size(); i++) {                  // O(C)
        index.putIfAbsent(orderedColumns.get(i), i);                   // O(log C)
    }
    Integer exact = index.get(path.trim());
    if (exact != null) { return new ResolvedPath(exact, orderedColumns.get(exact), List.of()); }
    String[] parts = path.split("\\.");
    for (int prefix = parts.length; prefix > 0; prefix--) {            // O(L)
        String joined = parts[0];
        for (int k = 1; k < prefix; k++) {                             // O(L) per iteration
            joined = joined + "." + parts[k];
        }
        ...
    }
}
```

**Complexity:** O(C log C + L²) per call. Called once per projection item in `JsonlRowReader.setProjection` and twice per item in `JsonlRowStorage.readProjectedArrays` (lines 1180, 1206), and once per `createIndex` in `JsonlIndexManager` → O(P × C log C) per projection setup → O(n² log n) when P scales with C.

**Root cause:** Every call rebuilds the case-insensitive `TreeMap` of column names. With wide tables and many projection items (or many dot-path indexes), the index rebuild dominates the per-call cost.

**Required changes:**
1. Introduce a `JsonPathResolver.SchemaIndex` value object: `record SchemaIndex(List<String> orderedColumns, TreeMap<String, Integer> indexByName)`. Build it once per schema (the `JsonlSchemaManager` already has `indexByName` — pass it in directly).
2. Make `resolve` an instance method: `public ResolvedPath resolve(String path)` on `SchemaIndex`, reusing the pre-built map. O(log C) per call.
3. Cache the prefix strings incrementally: build `joined` once for the full path, then `joined.substring(0, lastDot)` to step down. Avoids the L² string concat.
4. Update all callers to construct the `SchemaIndex` once and reuse it.

**Acceptance criteria:**
- `JsonlSchemaProjectionTest`, `JsonlProjectionPushdownTest`, `JsonlIndexTest` pass.
- New `JsonPathResolveIsOLogCTest`: with C = 10⁴ columns and P = 10⁴ projection items, the full projection setup completes in O(P × log C), not O(P × C log C).

**Test additions:**
- `JsonPathResolveIsOLogCTest`

---

## 8. Avro Storage Remediation Prompts

### Avro-1 — Pre-compute `Schema.Field[]` and `int[] fieldPositions` in `AvroRowStorage.fromRecord`

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRowStorage.java`
**Method:** `static Object[] fromRecord(GenericRecord avroRecord, List<String> columns, Class<?>[] targetTypes)` (lines 570–580)

**Pattern:**
```java
Object[] row = new Object[columns.size()];
Schema schema = avroRecord.getSchema();
for (int i = 0; i < columns.size(); i++) {
    String col = columns.get(i);
    Object avroValue = avroRecord.get(col);          // O(fields) linear scan
    Schema.Field field = schema.getField(col);       // O(fields) linear scan
    row[i] = fromAvroValue(avroValue, targetTypes[i], field != null ? field.schema() : null);
}
```

**Complexity:** O(rows × columns × fields). When `columns ≈ fields` (typical), this collapses to **O(rows × columns²)**.

**Root cause:** Apache Avro's `GenericData.Record.get(String)` and `Schema.getField(String)` are both linear scans of the field list. Called inside an outer per-record loop and an inner per-column loop. The method is on the hot read path (`AvroReadIterator.next()`, `AvroBatchOperator.importFromReader`, `AvroParallelReader.readSequential`, `AvroInputFormatCompat.readSplit`).

**Required changes:**
1. Introduce a per-schema cache: `private static final ConcurrentHashMap<Schema, SchemaIndex> SCHEMA_INDEX = ...;` where `SchemaIndex` contains `Map<String, Integer> nameToPos` and `Map<String, Schema.Field> nameToField`.
2. In `fromRecord`, look up the `SchemaIndex` for `avroRecord.getSchema()` once (O(1) after first build). Then in the inner loop:
   ```java
   Integer pos = schemaIndex.nameToPos.get(col);
   Object avroValue = (pos != null) ? avroRecord.get(pos) : null;        // O(1)
   Schema.Field field = schemaIndex.nameToField.get(col);                 // O(1)
   row[i] = fromAvroValue(avroValue, targetTypes[i], field != null ? field.schema() : null);
   ```
3. The `SchemaIndex` is built once per schema (O(fields)) and reused across all records of that schema. For schemas with evolving versions, the cache key is the full `Schema` object (Avro's `Schema.equals` is well-defined).
4. Add a similar cache for the writer schema in `toRecord` if needed.

**Acceptance criteria:**
- `AvroStorageTest`, `AvroRowStorageTest`, `AvroReadIteratorTest`, `AvroParallelReaderTest`, `AvroBatchOperatorTest`, `AvroInputFormatCompatTest` pass.
- New `FromRecordIsOColumnsTest`: with 10⁴ records and 10² columns, `fromRecord` per record completes in O(columns), not O(columns²).

**Test additions:**
- `FromRecordIsOColumnsTest`
- `SchemaIndexCacheHitTest`

---

### Avro-2 — Cache field index map per query in `AvroQueryExecutor.convertRecordToMap`

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroQueryExecutor.java`
**Method:** `private static Map<String, Object> convertRecordToMap(GenericRecord avroRecord, List<String> allColumns, Map<String, Class<?>> columnTypes)` (lines 227–274)

**Pattern:**
```java
Schema recordSchema = avroRecord.getSchema();
Map<String, Object> map = new LinkedHashMap<>(...);
for (String col : allColumns) {
    if (recordSchema.getField(col) != null) {     // O(fields) per col
        Object val = avroRecord.get(col);          // O(fields) per col
        ... BigDecimal scale lookup via recordSchema.getField(col) again ...
    }
}
```

**Complexity:** O(rows × columns × fields) = O(rows × columns²) when `columns ≈ fields`.

**Root cause:** Called per record by `readWithPushdown`, `readWithProjection`, and `readFull` — every query path. The BigDecimal branch re-calls `recordSchema.getField(col)` a third time. Per record per column: up to 3 × O(fields).

**Required changes:**
1. Refactor `convertRecordToMap` to accept a pre-built `AvroSchemaIndex` (from Avro-1's cache) parameter. Or build the index once at the start of the query when `allColumns` and `recordSchema` are known, and reuse it across all records.
2. Use `avroRecord.get(int)` for O(1) access:
   ```java
   for (int i = 0; i < allColumns.size(); i++) {
       String col = allColumns.get(i);
       Integer pos = schemaIndex.nameToPos.get(col);
       if (pos == null) continue;
       Object val = avroRecord.get(pos);
       Schema.Field field = schemaIndex.nameToField.get(col);
       ... handle BigDecimal using field.schema() (no re-lookup) ...
   }
   ```
3. Build the `AvroSchemaIndex` once at the start of `readWithPushdown`, `readWithProjection`, `readFull`.

**Acceptance criteria:**
- `AvroQueryExecutorTest`, `AvroStorageTest`, `AvroStorageAdvancedTest` pass.
- New `ConvertRecordToMapIsOColumnsTest`: with 10⁴ records and 10² columns, per-record conversion completes in O(columns).

**Test additions:**
- `ConvertRecordToMapIsOColumnsTest`

---

### Avro-3 — Pre-compute `Schema.Field[]` and `Schema.Type[]` in `AvroStatistics.collectColumnStatistics`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroStatistics.java`
**Method:** `public AvroStatistics collectColumnStatistics(File avroFile, List<String> columns, Map<String, Class<?>> columnTypes)` (lines 204–236)

**Pattern:**
```java
while (reader.hasNext()) {
    GenericRecord avroRecord = reader.nextRecord();
    rowCount++;
    for (String col : columns) {
        Object value = avroRecord.get(col);              // O(fields)
        ColumnStats cs = columnStatistics.get(col);
        if (value == null || Schema.Type.NULL.equals(
                getBaseType(avroRecord.getSchema(), col))) {  // O(fields)
            cs.nullCount++;
        } else {
            updateMinMax(cs, value);
        }
    }
}
```

**Complexity:** O(rows × columns × fields) = O(rows × columns²) when `columns ≈ fields`.

**Root cause:** Two O(fields) Avro lookups per record × per column (`avroRecord.get(col)` and `getBaseType`'s `schema.getField(col)`). Triggered by `ANALYZE TABLE` or on-demand column-statistics collection.

**Required changes:**
1. Before the read loop, build `Schema.Field[] fields = new Schema.Field[columns.size()]`, `int[] fieldPositions = new int[columns.size()]`, and `Schema.Type[] baseTypes = new Schema.Type[columns.size()]` from the file's schema (one-time O(fields + columns)).
2. Inside the loop, use `avroRecord.get(fieldPositions[i])` and `baseTypes[i]`:
   ```java
   for (int i = 0; i < columns.size(); i++) {
       Object value = avroRecord.get(fieldPositions[i]);   // O(1)
       ColumnStats cs = columnStatistics.get(columns.get(i));
       if (value == null || Schema.Type.NULL.equals(baseTypes[i])) {
           cs.nullCount++;
       } else {
           updateMinMax(cs, value);
       }
   }
   ```

**Acceptance criteria:**
- `AvroStatisticsTest` (add if missing), `AnalyzeTableTest` pass.
- New `CollectColumnStatisticsIsORowsPlusColumnsTest`: with 10⁴ rows and 10² columns, stats collection completes in O(rows × columns), not O(rows × columns²).

**Test additions:**
- `CollectColumnStatisticsIsORowsPlusColumnsTest`

---

### Avro-4 — Build a frequency table for `AdaptiveCompressionManager.analyzeDataPattern` instead of O(sample²) re-scan

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AdaptiveCompressionManager.java`
**Method:** `public static String analyzeDataPattern(List<?> rows)` (lines 374–431), `private static int countMatches(List<?> rows, int sample, String value)` (lines 411–425)

**Pattern:**
```java
for (int i = 0; i < sample; i++) {
    for (Object v : extractValues(rows.get(i))) {
        if (v instanceof String s) distinctStrings.add(s);
    }
}
for (int i = 0; i < sample; i++) {
    for (Object v : extractValues(rows.get(i))) {
        if (v instanceof String s && !s.isEmpty()) {
            if (distinctStrings.size() <= 8 && countMatches(rows, sample, s) >= 2) {
                repeatedValues++;
            }
        }
    }
}
// countMatches re-iterates the sample for every value
```

**Complexity:** O(sample² × row_size) per call.

**Root cause:** For every distinct string found in the outer scan, `countMatches` re-iterates the entire sample to count occurrences.

**Required changes:**
1. Pre-compute a `Map<String, Integer> frequency` in a single O(sample × row_size) pass:
   ```java
   Map<String, Integer> frequency = new HashMap<>();
   for (int i = 0; i < sample; i++) {
       for (Object v : extractValues(rows.get(i))) {
           if (v instanceof String s) frequency.merge(s, 1, Integer::sum);
       }
   }
   ```
2. Replace `countMatches(rows, sample, s) >= 2` with `frequency.getOrDefault(s, 0) >= 2` — O(1).
3. The `distinctStrings` set becomes `frequency.keySet()`.

**Acceptance criteria:**
- `AdaptiveCompressionManagerTest` pass.
- New `AnalyzeDataPatternIsOSampleTest`: with sample = 100 and row_size = 100, `analyzeDataPattern` completes in O(sample × row_size).

**Test additions:**
- `AnalyzeDataPatternIsOSampleTest`

---

### Avro-5 — Bucket rows by target sub-range in `RangePartitionStrategy.redistributeRows`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/RangePartitionStrategy.java`
**Method:** `public RedistributionResult redistributeRows(String tableName, List<Map<String, Object>> rows, List<AvroRangePartitioner.RangeBoundary> subRanges, Set<Path> written)` (lines 63–102)

**Pattern:**
```java
for (Map<String, Object> row : rows) {
    Object value = extractPartitionValue(row);   // O(row_size) case-insensitive scan
    Double numeric = toNumeric(value);
    AvroRangePartitioner.RangeBoundary targetSub = findTargetSubRange(numeric, subRanges);  // O(subRanges)
    Path subDir = getPartitionDir(tableName, targetSub);
    try {
        if (!written.contains(subDir)) {
            ...
            AvroRowStorage subStorage = newPartitionStorage(subDir.toString());
            List<Map<String, Object>> existing = subStorage.scan();
            existing.add(row);
            subStorage.setRows(existing);
            subStorage.saveToFile(tableName);    // writes the WHOLE partition
            written.add(subDir);
            rangesCreated++;
        } else {
            AvroRowStorage subStorage = newPartitionStorage(subDir.toString());
            subStorage.loadFromFile(tableName);  // reads the WHOLE partition back
            List<Map<String, Object>> existing = subStorage.scan();
            existing.add(row);
            subStorage.setRows(existing);
            subStorage.saveToFile(tableName);    // writes the WHOLE partition back
        }
    } catch (Exception e) { ... }
}
```

**Complexity:** O(rows × (subRanges + partition_rows + partition_io)). The else branch re-reads and re-writes the entire partition file for every row, so when n rows all land in one sub-range the cost is `1 + 2 + ... + n = O(n²)` disk reads/writes.

**Root cause:** Instead of batching rows per target partition, the strategy loads + appends + saves the whole partition per row.

**Required changes:**
1. Bucket the rows by target sub-range in a single pass:
   ```java
   Map<Path, List<Map<String, Object>>> buckets = new HashMap<>();
   for (Map<String, Object> row : rows) {
       Object value = extractPartitionValue(row);
       Double numeric = toNumeric(value);
       AvroRangePartitioner.RangeBoundary targetSub = findTargetSubRange(numeric, subRanges);
       Path subDir = getPartitionDir(tableName, targetSub);
       buckets.computeIfAbsent(subDir, k -> new ArrayList<>()).add(row);
   }
   ```
2. For each bucket, load the existing partition once, append all rows, save once:
   ```java
   for (Map.Entry<Path, List<Map<String, Object>>> e : buckets.entrySet()) {
       AvroRowStorage subStorage = newPartitionStorage(e.getKey().toString());
       if (written.contains(e.getKey())) subStorage.loadFromFile(tableName);
       List<Map<String, Object>> existing = subStorage.scan();
       existing.addAll(e.getValue());
       subStorage.setRows(existing);
       subStorage.saveToFile(tableName);
       written.add(e.getKey());
   }
   ```
3. Apply Avro-6 to make `findTargetSubRange` O(log subRanges).

**Acceptance criteria:**
- `AvroRangePartitionerTest`, `AvroDatePartitionerTest` (if it uses redistributeRows) pass.
- New `RedistributeRowsIsORowsPlusPartitionsTest`: with 10⁵ rows and 10 sub-ranges, `redistributeRows` performs at most 10 partition reads + 10 writes (not 10⁵ of each).

**Test additions:**
- `RedistributeRowsIsORowsPlusPartitionsTest`

---

### Avro-6 — Use binary search in `AvroRangePartitioner.resolveRangeForValue`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRangePartitioner.java`
**Method:** `public RangeBoundary resolveRangeForValue(Object value)` (lines 207–218)

**Pattern:**
```java
public RangeBoundary resolveRangeForValue(Object value) {
    Double numeric = toNumeric(value);
    if (numeric == null) return null;
    for (RangeBoundary b : boundaries) {                       // O(boundaries)
        if (numeric >= b.lowerInclusive() && numeric <= b.upperInclusive()) {
            return b;
        }
    }
    return null;
}
```

**Complexity:** O(rows × boundaries) when called per row.

**Root cause:** Every row's partition lookup linearly scans the boundaries list.

**Required changes:**
1. Keep `boundaries` sorted by `lowerInclusive` (already has a `reindexBoundaries` helper that sorts).
2. Replace the linear scan with `Collections.binarySearch` on a sorted `List<RangeBoundary>` using a comparator by `lowerInclusive`:
   ```java
   int idx = Collections.binarySearch(boundaries, numeric, (b, v) -> {
       double lower = ((RangeBoundary) b).lowerInclusive();
       double upper = ((RangeBoundary) b).upperInclusive();
       double val = (Double) v;
       if (val < lower) return 1;
       if (val > upper) return -1;
       return 0;
   });
   return idx >= 0 ? boundaries.get(idx) : null;
   ```
3. Or use `TreeMap<Double, RangeBoundary>` keyed by `lowerInclusive` and `floorEntry(numeric)` — verify the floor entry's `upperInclusive >= numeric`.

**Acceptance criteria:**
- `AvroRangePartitionerTest` passes.
- New `ResolveRangeForValueIsOLogBoundariesTest`: with 10⁵ boundaries and 10⁵ rows, total lookup completes in O(rows × log boundaries).

**Test additions:**
- `ResolveRangeForValueIsOLogBoundariesTest`

---

### Avro-7 — Cache the parsed manifest of `best` in `AvroRestoreManager.findBestBackup`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRestoreManager.java`
**Method:** `private static File findBestBackup(File backupRoot, Instant pointInTime)` (lines 312–332)

**Pattern:**
```java
File best = null;
for (File child : children) {
    File manifestFile = new File(child, AvroFileConstants.MANIFEST_FILE);
    if (!child.isDirectory() || !manifestFile.isFile()) continue;
    try {
        AvroBackupManager.BackupManifest manifest = AvroBackupManager.readManifest(manifestFile);
        if (!manifest.startedAt().isAfter(pointInTime)
                && (best == null || manifest.startedAt().isAfter(
                        AvroBackupManager.readManifest(new File(best, AvroFileConstants.MANIFEST_FILE)).startedAt()))) {
            best = child;
        }
    } catch (Exception e) { ... }
}
```

**Complexity:** O(backups²) file reads.

**Root cause:** For every backup directory examined, the inner `isAfter` check re-parses the manifest of the current `best` from disk again. With n backups: 1 + 2 + ... + n = O(n²) disk reads.

**Required changes:**
1. Cache the parsed `BackupManifest` of `best` in a local variable:
   ```java
   File best = null;
   BackupManifest bestManifest = null;
   for (File child : children) {
       File manifestFile = new File(child, AvroFileConstants.MANIFEST_FILE);
       if (!child.isDirectory() || !manifestFile.isFile()) continue;
       try {
           AvroBackupManager.BackupManifest manifest = AvroBackupManager.readManifest(manifestFile);
           if (!manifest.startedAt().isAfter(pointInTime)
                   && (bestManifest == null || manifest.startedAt().isAfter(bestManifest.startedAt()))) {
               best = child;
               bestManifest = manifest;
           }
       } catch (Exception e) { ... }
   }
   ```
2. The cost drops to O(backups) manifest reads.

**Acceptance criteria:**
- `AvroRestoreManagerTest` passes.
- New `FindBestBackupIsOBackupsTest`: with 10² backups, `findBestBackup` performs at most 10² manifest reads (not 10⁴).

**Test additions:**
- `FindBestBackupIsOBackupsTest`

---

### Avro-8 — Use `LinkedHashSet<String>` for duplicate detection in `AvroEnumHandler.createEnumSchema`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroEnumHandler.java`
**Method:** `public static Schema createEnumSchema(String name, List<String> symbols, String namespace)` (lines 53–62)

**Pattern:**
```java
List<String> clean = new ArrayList<>(symbols.size());
for (String symbol : symbols) {
    if (symbol == null || symbol.isBlank()) { ... }
    if (clean.contains(symbol)) {                                  // O(n) ArrayList scan
        throw new IllegalArgumentException("Duplicate enum symbol: " + symbol);
    }
    clean.add(symbol);
}
```

**Complexity:** O(symbols²).

**Root cause:** `ArrayList.contains` is O(n); inside an O(n) loop, it's O(n²).

**Required changes:**
1. Use `LinkedHashSet<String> seen = new LinkedHashSet<>(symbols.size() * 2);` for duplicate detection and order preservation:
   ```java
   LinkedHashSet<String> seen = new LinkedHashSet<>(symbols.size() * 2);
   for (String symbol : symbols) {
       if (symbol == null || symbol.isBlank()) { ... }
       if (!seen.add(symbol)) {
           throw new IllegalArgumentException("Duplicate enum symbol: " + symbol);
       }
   }
   List<String> clean = new ArrayList<>(seen);
   ```

**Acceptance criteria:**
- `AvroEnumHandlerTest` (add if missing) passes.
- New `CreateEnumSchemaIsOSymbolsTest`: with 10⁵ enum symbols, schema creation completes in O(symbols).

**Test additions:**
- `CreateEnumSchemaIsOSymbolsTest`

---

### Avro-9 — Use `tailSet` / `subMap` for `AvroSecondaryIndex.shiftPositions` instead of full scan

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroSecondaryIndex.java`
**Method:** `public void shiftPositions(int rowIndex, int delta)` (lines 106–117)

**Pattern:**
```java
public void shiftPositions(int rowIndex, int delta) {
    if (delta == 0) return;
    for (List<Integer> rows : indexMap.values()) {           // O(uniqueKeys)
        for (int i = 0; i < rows.size(); i++) {               // O(rowsPerKey)
            int position = rows.get(i);
            if (position >= rowIndex) {
                rows.set(i, position + delta);
            }
        }
    }
}
```

**Complexity:** O(total_entries) per call. Called on every `insertAt` and `delete` from `AvroRowStorage`. For n such operations: O(n × total_entries) ≈ O(n²).

**Root cause:** Each call walks every key in the index map and every row index in each list, rewriting positions. No early-exit.

**Required changes:**
1. Maintain the row indices in each key's list as a sorted `TreeSet<Integer>` (or `ConcurrentSkipListSet`):
   ```java
   // Replace List<Integer> with TreeSet<Integer> in the indexMap value type
   NavigableMap<Object, TreeSet<Integer>> indexMap = ...;
   ```
2. Use `tailSet(rowIndex)` to fetch only the affected positions:
   ```java
   public void shiftPositions(int rowIndex, int delta) {
       if (delta == 0) return;
       for (TreeSet<Integer> positions : indexMap.values()) {
           // tailSet returns a VIEW; we need to extract, modify, and re-add because TreeSet is sorted
           NavigableSet<Integer> tail = positions.tailSet(rowIndex, true);
           // Take a snapshot to avoid ConcurrentModificationException
           List<Integer> snapshot = new ArrayList<>(tail);
           tail.clear();
           for (int p : snapshot) positions.add(p + delta);
       }
   }
   ```
3. Alternative: maintain a global "delta per offset" version counter and apply shifts lazily on read. This is more complex but eliminates the per-shift cost entirely.

**Acceptance criteria:**
- `AvroSecondaryIndexTest`, `AvroRowStorageTest`, `AvroStorageTest` pass.
- New `ShiftPositionsIsOAffectedTest`: with 10⁵ rows under one key, `shiftPositions(rowIndex=100, delta=1)` touches only 10⁵ - 100 entries (not 10⁵ × 10⁵).

**Test additions:**
- `ShiftPositionsIsOAffectedTest`

---

### Avro-10 — Use `subMap` for `AvroPrimaryKeyIndex.shiftPositions` instead of `TreeMap.replaceAll`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroPrimaryKeyIndex.java`
**Method:** `public void shiftPositions(int rowIndex, int delta)` (lines 305–309)

**Pattern:**
```java
public void shiftPositions(int rowIndex, int delta) {
    if (delta == 0 || pkColumnIndex < 0) return;
    primaryKeyMap.replaceAll((key, position) -> position >= rowIndex ? position + delta : position);
    invalidatePageCache();
}
```

**Complexity:** O(total_entries) per call. Called on every `insertAt` and `delete`. For n such operations: O(n × total_entries) ≈ O(n²).

**Root cause:** The whole primary-key map is rescanned on every positional shift.

**Required changes:**
1. Add an auxiliary `TreeMap<Integer, Object> positionToKey` (row → key) to `AvroPrimaryKeyIndex`. Keep it in sync with `primaryKeyMap` on every `insert` / `delete`.
2. Use `positionToKey.subMap(rowIndex, true, Integer.MAX_VALUE, true).keySet()` to fetch only the affected rows:
   ```java
   public void shiftPositions(int rowIndex, int delta) {
       if (delta == 0 || pkColumnIndex < 0) return;
       // Take a snapshot of affected positions
       List<Integer> affectedPositions = new ArrayList<>(positionToKey.subMap(rowIndex, true, Integer.MAX_VALUE, true).keySet());
       // Update each entry in primaryKeyMap
       for (int p : affectedPositions) {
           Object key = positionToKey.remove(p);
           int newPos = p + delta;
           primaryKeyMap.put(key, newPos);
           positionToKey.put(newPos, key);
       }
       invalidatePageCache();
   }
   ```
3. Or even simpler: switch to logical row positions (append-only with a deletion tombstone list) so positional shifts never happen — the engine addresses rows by logical rowId, not physical position.

**Acceptance criteria:**
- `AvroPrimaryKeyIndexTest`, `AvroRowStorageTest` pass.
- New `PrimaryKeyShiftPositionsIsOAffectedTest`: with 10⁵ rows, `shiftPositions(rowIndex=100, delta=1)` touches 10⁵ - 100 entries, not 10⁵ × 10⁵.

**Test additions:**
- `PrimaryKeyShiftPositionsIsOAffectedTest`

---

### Avro-11 — Switch `AvroRowStorage.delete` to tombstones + `subMap`-based index shifts (or `TreeMap<Integer, Object[]>` row store)

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRowStorage.java`, `diesel/storage/avro/AvroPrimaryKeyIndex.java`, `diesel/storage/avro/AvroSecondaryIndex.java`
**Method:** `public synchronized void delete(int rowIndex)` (lines 247–257)

**Pattern:**
```java
Object[] row = rows.get(rowIndex);
rows.remove(rowIndex);                           // O(rows) ArrayList shift
syncIndexDelete(rowIndex);
if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
    primaryKeyIndex.delete(row, rowIndex);
    primaryKeyIndex.shiftPositions(rowIndex, -1); // O(total_entries) — Avro-10
}
secondaryIndexManager.syncOnDelete(toMap(row), rowIndex);  // O(indexes × row_size) — Avro-14
secondaryIndexManager.shiftPositions(rowIndex, -1);         // O(total_entries × indexes) — Avro-9
```

**Complexity:** O(rows + total_entries × indexes) per delete. For n deletes from the middle: O(n × rows) ≈ O(n²).

**Root cause:** `ArrayList.remove(int)` shifts every subsequent element left by one. Both the primary-key index and every secondary index then linearly rescan their full contents to shift positions. A loop of `delete(i)` calls is the canonical O(n²) ArrayList anti-pattern.

**Required changes:**
1. Mark deletes as tombstones instead of physically removing from the list:
   - Add a `BitSet deletedRows` to `AvroRowStorage`. `delete(int rowIndex)` sets `deletedRows.set(rowIndex)` instead of `rows.remove(rowIndex)`.
   - Compaction runs in batches (when `deletedRows.cardinality() > COMPACTION_THRESHOLD × rows.size()`), reclaiming tombstones.
2. Replace `ArrayList` with `TreeMap<Integer, Object[]>` keyed by row id — inserts/deletes are O(log n) without shifting neighbours.
3. Make `shiftPositions` use `subMap`/`tailMap` views to update only the affected entries (depends on Avro-9 and Avro-10).
4. Update `scan()`, `getRows()`, `setRows()`, and any other iteration paths to skip tombstones.

**Acceptance criteria:**
- `AvroRowStorageTest`, `AvroStorageTest`, `AvroStorageAdvancedTest`, `AvroRecoveryTest`, `AvroBatchOperatorTest` pass.
- New `AvroDeleteIsOLogNTest`: with 10⁵ rows, deleting 10³ rows from the middle completes in O(10³ × log n), not O(10³ × 10⁵).

**Test additions:**
- `AvroDeleteIsOLogNTest`

---

### Avro-12 — Switch `AvroRowStorage.insertAt` to tombstones + `subMap`-based index shifts (depends on Avro-11)

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRowStorage.java`
**Method:** `public synchronized void insertAt(int rowIndex, Map<String, Object> row)` (lines 215–228)

**Pattern:**
```java
Object[] arr = fromMap(row);
if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
    primaryKeyIndex.validateInsert(arr, rowIndex);
}
rows.add(rowIndex, arr);                          // O(rows) ArrayList shift
syncIndexInsert(arr, rowIndex);
if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
    primaryKeyIndex.shiftPositions(rowIndex, 1);  // O(total_entries) — Avro-10
    primaryKeyIndex.insert(arr, rowIndex);
}
secondaryIndexManager.shiftPositions(rowIndex, 1);  // O(total_entries × indexes) — Avro-9
secondaryIndexManager.syncOnInsert(row, rowIndex);
```

**Complexity:** O(rows + total_entries × indexes) per insert. For n middle inserts: O(n × rows) ≈ O(n²).

**Root cause:** Same structure as `delete`: `ArrayList.add(int, E)` shifts every subsequent element right, then both indexes are full-scanned to bump positions.

**Required changes:**
1. Apply Avro-11 first (tombstone-based row storage or `TreeMap<Integer, Object[]>`-backed rows).
2. Make `shiftPositions` use `subMap`/`tailMap` (depends on Avro-9 and Avro-10).
3. If the engine maintains clustered-PK order, prefer appending at the tail whenever possible (the `insertAt` API's `rowIndex >= rows.size()` branch is already O(1) for the ArrayList shift).

**Acceptance criteria:**
- `AvroRowStorageTest`, `AvroStorageTest`, `InsertQueryTest` pass.
- New `AvroInsertIsOLogNTest`: with 10⁵ rows, inserting 10³ rows in the middle completes in O(10³ × log n).

**Test additions:**
- `AvroInsertIsOLogNTest`

---

### Avro-13 — Pre-compute case-insensitive lookup map per row in `AvroSecondaryIndexManager.rebuildAllIndexes`

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroSecondaryIndexManager.java`
**Method:** `public synchronized void rebuildAllIndexes(List<Map<String, Object>> allRows)` (lines 235–249)
**Helper:** `private Object getRowValue(Map<String, Object> row, String columnName)`

**Pattern:**
```java
for (int rowIndex = 0; rowIndex < allRows.size(); rowIndex++) {
    Map<String, Object> row = allRows.get(rowIndex);
    for (AvroSecondaryIndex index : indexes.values()) {
        Object key = buildKeyForIndex(row, index);   // -> getRowValue: O(row_size) case-insensitive scan
        if (key != null) {
            index.insert(key, rowIndex);
        }
    }
}

private Object getRowValue(Map<String, Object> row, String columnName) {
    if (row.containsKey(columnName)) return row.get(columnName);
    for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(columnName)) {
            return entry.getValue();
        }
    }
    return null;
}
```

**Complexity:** O(rows × indexes × row_size). If `row_size ≈ columns`, this is O(rows × indexes × columns) = O(n × m²).

**Root cause:** For every row, for every index, `buildKeyForIndex` calls `getRowValue` for each covered column, and `getRowValue` does a case-insensitive scan of the row's entries when the column name isn't an exact match. With many indexes × wide rows, this dominates index rebuild time.

**Required changes:**
1. Pre-compute a `TreeMap(String.CASE_INSENSITIVE_ORDER)` view of each row once per row, before the index loop:
   ```java
   for (int rowIndex = 0; rowIndex < allRows.size(); rowIndex++) {
       Map<String, Object> row = allRows.get(rowIndex);
       TreeMap<String, Object> ciRow = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
       ciRow.putAll(row);
       for (AvroSecondaryIndex index : indexes.values()) {
           Object key = buildKeyForIndexCI(ciRow, index);   // O(coveredColumns × log row_size)
           if (key != null) index.insert(key, rowIndex);
       }
   }
   ```
2. Update `buildKeyForIndex` to accept the case-insensitive map (or refactor `getRowValue` to use the map).
3. Better: pass the row as `Object[]` plus a `Map<String, Integer>` column-position map (the `Object[]` form is already used by `AvroRowStorage` internally). Then `getRowValue` becomes `row[positionMap.get(columnName)]` — O(log columns) or O(1).

**Acceptance criteria:**
- `AvroSecondaryIndexManagerTest`, `AvroStorageTest`, `AvroBatchOperatorTest` pass.
- New `RebuildAllIndexesIsORowsTimesIndexesTest`: with 10⁴ rows, 10² columns, and 10 indexes, `rebuildAllIndexes` completes in O(rows × indexes × log columns), not O(rows × indexes × columns).

**Test additions:**
- `RebuildAllIndexesIsORowsTimesIndexesTest`

---

### Avro-14 — Pre-compute case-insensitive lookup map per row in `AvroSecondaryIndexManager.syncOn*`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroSecondaryIndexManager.java`
**Methods:** `syncOnInsert`, `syncOnUpdate`, `syncOnDelete` (lines 191–230)

**Pattern:**
```java
public synchronized void syncOnInsert(Map<String, Object> row, int rowIndex) {
    for (AvroSecondaryIndex index : indexes.values()) {
        Object key = buildKeyForIndex(row, index);   // -> getRowValue: O(row_size) case-insensitive scan
        if (key != null) index.insert(key, rowIndex);
    }
}
// syncOnUpdate calls buildKeyForIndex twice per index (oldRow + newRow)
```

**Complexity:** O(indexes × row_size) per row update. For n updates: O(n × indexes × columns).

**Root cause:** Each row mutation triggers an index sync that rebuilds the index key via `getRowValue`, which performs a case-insensitive scan over `row.entrySet()`.

**Required changes:**
1. Apply Avro-13's fix: cache the case-insensitive lookup map once per row at the call site (`AvroRowStorage.insert` / `update` / `delete`) and pass it down to all `syncOn*` methods.
2. Or build a `Map<String, Integer>` column-name-to-index map at `AvroSecondaryIndexManager` construction time and use position-based row access (`Object[]` form already used by `AvroRowStorage` internally).
3. For `syncOnUpdate`, share the same case-insensitive map between the oldRow and newRow lookups if they share the same key set (typically yes).

**Acceptance criteria:**
- `AvroSecondaryIndexManagerTest`, `AvroStorageTest` pass.
- New `SyncOnInsertIsOIndexesTest`: with 10² indexes and 10² columns, `syncOnInsert` completes in O(indexes × log columns).

**Test additions:**
- `SyncOnInsertIsOIndexesTest`

---

### Avro-15 — Build a `Map<String, Integer> columnIndex` in `AvroSecondaryIndexManager.isCompatible`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroSecondaryIndexManager.java`
**Method:** `public synchronized boolean isCompatible(List<String> expectedColumns, Map<String, Class<?>> expectedTypes)` (lines 139–163)
**Helper:** `private int findColumnIndex(String columnName)`

**Pattern:**
```java
for (AvroSecondaryIndex index : indexes.values()) {                  // O(indexes)
    List<String> coveredColumns = index.getCoversColumns();
    for (String column : coveredColumns) {                          // O(coveredColumns)
        if (findColumnIndex(column) < 0) {                          // O(columns) linear scan
            return false;
        }
    }
    ...
}

private int findColumnIndex(String columnName) {
    for (int i = 0; i < columns.size(); i++) {                       // O(columns)
        if (columns.get(i).equalsIgnoreCase(columnName)) return i;
    }
    return -1;
}
```

**Complexity:** O(indexes × coveredColumns × columns). When all scale with table width, O(N³) worst case; typically O(I × N²).

**Root cause:** The compatibility check is called on every load of a sidecar (`AvroRowStorage.loadSecondaryIndexes` line 814). It linearly scans `columns` for every covered column of every index. No HashMap index.

**Required changes:**
1. Build a `TreeMap(String.CASE_INSENSITIVE_ORDER) columnIndex` once at `AvroSecondaryIndexManager` construction time (and rebuild it when `columns` changes — e.g., on `setColumns(...)`).
2. Replace `findColumnIndex(column)` with `columnIndex.get(column)` — O(log columns).
3. Update the outer `columns.get(i).equalsIgnoreCase(expectedColumns.get(i))` loop to use the case-insensitive `columnIndex` as well: `columnIndex.containsKey(expectedColumns.get(i))` plus position check.

**Acceptance criteria:**
- `AvroSecondaryIndexManagerTest` passes.
- New `IsCompatibleIsOIndexesTimesCoveredTest`: with 10² indexes, 10² covered columns each, and 10³ total columns, `isCompatible` completes in O(indexes × coveredColumns × log columns).

**Test additions:**
- `IsCompatibleIsOIndexesTimesCoveredTest`

---

### Avro-16 — Lowercase the partition column once in `AvroHashPartitioner.extractPartitionValue`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroHashPartitioner.java`
**Method:** `private Object extractPartitionValue(Map<String, Object> row)` (lines 431–441)

**Pattern:**
```java
private Object extractPartitionValue(Map<String, Object> row) {
    if (config.getPartitionColumn().isEmpty()) return null;
    for (Map.Entry<String, Object> e : row.entrySet()) {            // O(row_size)
        if (e.getKey().equalsIgnoreCase(config.getPartitionColumn())) {
            return e.getValue();
        }
    }
    return null;
}
```

**Complexity:** O(rows × row_size) per `bucketize` call.

**Root cause:** For every row in a rebalance, the partition column is found by case-insensitive scan of all row entries.

**Required changes:**
1. At partitioner construction, capture `String partitionColumnLower = config.getPartitionColumn().toLowerCase(Locale.ROOT);` once.
2. At row processing, check `row.get(partitionColumnLower)` after lowercasing each key once — or build a `TreeMap(String.CASE_INSENSITIVE_ORDER)` from the row once.
3. Better: pass the row as `Object[]` plus a `Map<String, Integer>` column-position map so the partition column is accessed by position.

**Acceptance criteria:**
- `AvroHashPartitionerTest`, `AvroHashPartitionerTest` (add if missing) pass.
- New `ExtractPartitionValueIsO1Test`: with 10² columns, `extractPartitionValue` completes in O(1) per row.

**Test additions:**
- `ExtractPartitionValueIsO1Test`

---

### Avro-17 — Lowercase the partition column once in `RangePartitionStrategy.extractPartitionValue`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/RangePartitionStrategy.java`
**Method:** `private Object extractPartitionValue(Map<String, Object> row)` (lines 204–214)

**Pattern:** Identical to Avro-16.

**Complexity:** O(rows × row_size) inside `redistributeRows` and `splitRange`/`mergeRanges`.

**Root cause:** Identical pattern to Avro-16.

**Required changes:**
1. Apply the same fix as Avro-16: lowercase the partition column name once at construction; build a case-insensitive lookup map per row.

**Acceptance criteria:**
- `AvroRangePartitionerTest`, `AvroDatePartitionerTest` pass.
- New `RangeExtractPartitionValueIsO1Test`: with 10² columns, `extractPartitionValue` completes in O(1).

**Test additions:**
- `RangeExtractPartitionValueIsO1Test`

---

### Avro-18 — Run `detectCrash` once for batch recovery in `AvroRecoveryManager.recoverFile`

**Severity:** Medium
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRecoveryManager.java`
**Method:** `public FileRecovery recoverFile(File avroFile)` (lines 238–255)

**Pattern:**
```java
public FileRecovery recoverFile(File avroFile) {
    long t0 = System.nanoTime();
    CorruptedFile cf;
    try {
        List<CorruptedFile> found = detector.detectCrash(avroFile.getParentFile()).corruptedFiles()
                .stream().filter(c -> c.file().equals(avroFile))
                .toList();
        cf = found.isEmpty() ? null : found.get(0);
    } catch (IOException e) {
        return failure(avroFile, "detection failed: " + e.getMessage(), t0);
    }
    ...
}
```

**Complexity:** O(files²) when called for many files.

**Root cause:** `detectCrash(File)` walks every file in the directory and validates each `.avro` file's integrity. Calling `recoverFile` per file in a loop pays O(files) × O(files) = O(files²) integrity scans.

**Required changes:**
1. Add `public List<FileRecovery> recoverFiles(List<File> avroFiles)` that runs `detectCrash` once and iterates the artifacts:
   ```java
   public List<FileRecovery> recoverFiles(List<File> avroFiles) {
       Set<File> requested = new HashSet<>(avroFiles);
       long t0 = System.nanoTime();
       CrashDetectionResult detection = detector.detectCrash(avroFiles.get(0).getParentFile());
       List<FileRecovery> results = new ArrayList<>();
       for (CorruptedFile cf : detection.corruptedFiles()) {
           if (requested.contains(cf.file())) {
               results.add(recoverFromCorruptedFile(cf, t0));
           }
       }
       // Also process requested files that were not detected as corrupted (no-op recovery)
       for (File f : avroFiles) {
           if (detection.corruptedFiles().stream().noneMatch(c -> c.file().equals(f))) {
               results.add(success(f, t0));
           }
       }
       return results;
   }
   ```
2. Update callers that loop `recoverFile` to use `recoverFiles` instead.

**Acceptance criteria:**
- `AvroRecoveryManagerTest`, `AvroRecoveryTest`, `AvroIntegrityCheckerTest` pass.
- New `RecoverFilesIsOFilesTest`: with 10² avro files, `recoverFiles` performs at most 1 `detectCrash` call (not 10²).

**Test additions:**
- `RecoverFilesIsOFilesTest`

---

### Avro-19 — Apply Avro-13 fix to `AvroRowStorage.setRows` (delegate already covered)

**Severity:** Critical
**Subsystem:** Avro
**Files:** `diesel/storage/avro/AvroRowStorage.java`
**Method:** `public synchronized void setRows(List<Map<String, Object>> newRows)` (lines 260–271)

**Pattern:**
```java
public synchronized void setRows(List<Map<String, Object>> newRows) {
    rows.clear();
    for (Map<String, Object> row : newRows) {
        rows.add(fromMap(row));
    }
    syncIndexBulkFromArrays(rows);
    if (primaryKeyIndex != null && primaryKeyIndex.isEnabled()) {
        primaryKeyIndex.buildIndex(rows);
    }
    secondaryIndexManager.rebuildAllIndexes(newRows);  // O(rows × indexes × row_size) — Avro-13
}
```

**Complexity:** O(rows × indexes × row_size) via the delegate call to `rebuildAllIndexes`.

**Root cause:** `setRows` is invoked by `loadFromFile`, `AvroBatchOperator.rollbackBatch`, the partitioners' `writePartitionRows`, `AvroHashPartitioner.rebalance`, etc. Every bulk load pays the rebuild cost. The cost is inherited from Avro-13.

**Required changes:**
1. Apply Avro-13's fix (case-insensitive lookup map per row in `rebuildAllIndexes`).
2. Additionally, change `secondaryIndexManager.rebuildAllIndexes(newRows)` to accept the already-built `Object[]` form (`rows`), plus a pre-built column-position array — removes the `Map.entrySet()` scan entirely.
3. Add an overload: `rebuildAllIndexes(List<Object[]> allRows, Map<String, Integer> columnPositions)` and have `setRows` call the new overload.

**Acceptance criteria:**
- `AvroRowStorageTest`, `AvroStorageTest`, `AvroBatchOperatorTest`, `AvroHashPartitionerTest` pass.
- New `SetRowsIsORowsTimesIndexesTest`: with 10⁴ rows, 10² columns, and 10 indexes, `setRows` completes in O(rows × indexes × log columns).

**Test additions:**
- `SetRowsIsORowsTimesIndexesTest`

---

## 9. Cross-cutting Recommendations

### 9.1 Eliminate the reverse-lookup antipattern (CSV-1, TSV-1, JSONL-2)

Every delimited and JSONL index manager maintains `NavigableMap<Long, Integer> rowIdToPosition` (rowId → position) but no inverse. The same one-line fix — adding `Map<Integer, Long> positionToRowIdMap` and keeping it in sync — eliminates 5 hotspots in CSV (CSV-1, CSV-2, CSV-3, CSV-5, CSV-7's underlying cost), the same 5 in TSV (via shared `DelimitedIndexManager`), and 3 in JSONL (JSONL-2, JSONL-3, JSONL-4's underlying cost). **Recommended order:** fix CSV-1 first; the others fall out almost for free.

### 9.2 Replace `ArrayList.remove(Object)` on posting lists with `Set.remove` (CSV-6, TSV-6, JSONL-4)

All three delimited-style backends use `NavigableMap<Object, List<Long>>` for secondary indexes, where the inner `List<Long>` is an `ArrayList`. `ArrayList.remove(Object)` is O(k). Switching to `LinkedHashSet<Long>` (preserves insertion order, O(1) remove) or `ConcurrentSkipListSet<Long>` (sorted, O(log k) remove, supports range queries) eliminates another hotspot per backend. If serialization compatibility forbids the type change, maintain a side `HashMap<Long, Integer> rowIdToSlotInIndex` per posting list and use swap-remove.

### 9.3 Use tombstones instead of physical removal for row storage (CSV-7, TSV-4, TSV-5, JSONL-8, Avro-11, Avro-12)

Every row-store backend (CSV, TSV, JSONL, Avro) pays O(rows) per `ArrayList.add(int, E)` / `remove(int)` shift. For batch operations, the bulk path (`beginBulkUpdate()` / `endBulkUpdate()`) already exists — but the engine does not always use it. Two complementary fixes:

1. **Engine-level:** detect batch SQL statements (INSERT with multiple VALUES, UPDATE/DELETE with a predicate matching many rows) and wrap them in `beginBulkUpdate()` / `endBulkUpdate()`.
2. **Storage-level:** mark deletes as tombstones instead of physically removing from the list; compaction reclaims tombstones in batches. Inserts always append at the tail. This eliminates the O(rows) ArrayList shift entirely at the cost of O(1) tombstone-set operations. The trade-off is a slightly more complex `scan()` that skips tombstones.

The tombstone approach is more invasive but pays off in the long run for high-churn workloads. For low-churn workloads, the bulk-mode fix at the engine level is sufficient.

### 9.4 Pre-compute Avro schema field positions (Avro-1, Avro-2, Avro-3)

All three Avro read-path hotspots share the same root cause: `GenericRecord.get(String)` and `Schema.getField(String)` are linear scans. Introduce a single `AvroSchemaIndex` cache (keyed by `Schema`) containing `Map<String, Integer> nameToPos` and `Map<String, Schema.Field> nameToField`. Build it once per schema; reuse across all records and all queries. This eliminates three O(rows × columns²) hotspots with one fix.

### 9.5 Pre-compute case-insensitive column lookup per row (Avro-13, Avro-14, Avro-15, Avro-16, Avro-17)

Five Avro hotspots all share the same root cause: case-insensitive `equalsIgnoreCase` scans over `Map.entrySet()`. Two complementary fixes:

1. **Per-row:** at the call site (e.g., `AvroRowStorage.insert` / `update` / `delete`), build a `TreeMap(String.CASE_INSENSITIVE_ORDER)` view of the row once and pass it down to all `syncOn*` methods. O(row_size) once per row, then O(log columns) per lookup.
2. **Per-storage:** maintain a `Map<String, Integer> columnPosition` at `AvroSecondaryIndexManager` construction time (case-insensitive `TreeMap`), and access rows in `Object[]` form via position. O(1) per lookup after the row is in `Object[]` form (which `AvroRowStorage` already uses internally).

The second approach is preferable because `AvroRowStorage` already maintains rows as `Object[]` — the `Map<String, Object>` form is only used at the API boundary.

### 9.6 Cache decoded WAL segments across recovery phases (WAL-6, WAL-7, WAL-8)

The recovery flow reads the WAL from disk 4 times (loadCheckpoint + analysis + redo + undo). Add an in-memory `Map<Integer, List<WALEntry>>` cache in `WALManager` (size-bounded, LRU eviction). Recovery phases use `wal.readAllCached(segmentNumber)` instead of `segment.readAll()`. For WALs that fit in cache, this is a 4× I/O reduction. For WALs that exceed cache size, evicted segments are re-read on demand — still correct, just slower. Reset the cache at the end of `ARIESAlgorithm.recover` to free memory.

### 9.7 Maintain a dirty-page set in BufferPool (Buffer-4) and a free-slot stack in SlottedPageLayout (Buffer-7)

Two simple data-structure additions eliminate four Buffer hotspots:

- `LinkedHashSet<PageId> dirtyPageIds` in `BufferPool` (Buffer-4): O(1) `getDirtyPageCount`, O(D) `flushDirtyMatching`. Maintained via a hook on `Page.setDirty(true)` and removal on `persistDirty`.
- Free-slot stack (or `BitSet`) in `SlottedPageLayout` (Buffer-6, Buffer-7, Buffer-8): tombstoned slot ids are reused on insert; `defragment` compacts the slot directory; `validateLayout` and full-page scans become O(live count) instead of O(historical inserts).

### 9.8 MVCC conflict detection needs a global row-readers index (MVCC-1)

The single most impactful MVCC fix: maintain a global `ConcurrentHashMap<RowRef, Set<Long>> rowReaders` mapping each (table, rowIndex) to the set of txids currently reading it. Updated on `noteRead`; queried on `noteCommit` for each entry in the committer's write set (O(W) per commit). This eliminates the O(T × W × R) Cartesian scan. Cleanup on commit/abort is O(R) per transaction (driven by the transaction's own `readSet`).

### 9.9 MVCC snapshot needs to stop re-materializing the row list per row (MVCC-2, MVCC-3)

`TransactionTableSnapshot.getRows()` calls `table.getRows()` (a defensive-copy API returning `new ArrayList<>(rows)`) inside a per-row loop — O(N²). Add `Table.readPhysicalRow(int rowIndex)` returning the row at the given index without copying. Then have the snapshot use `readPhysicalRow` (or capture the row list once at the top). The same fix enables O(N) `getRowCount()`, `getRow(int)`, and `toString()` (MVCC-3) via single-pass walks or a memoized list.

### 9.10 Compaction re-keying needs a prefix-count array (MVCC-4)

`Table.rebuildRowVersionsAfterCompact` walks `findNewIndexAfterCompact(oldIndex)` for each MVCC entry, and `findNewIndexAfterCompact` linearly scans `deletedRows` from 0 to `oldIndex`. Build `int[] prefixAlive` in O(N) once, then `findNewIndexAfterCompact(oldIndex) = prefixAlive[oldIndex]` — O(1) per entry, O(N + R) total.

### 9.11 Suggested implementation order (priority sequence)

The following order maximizes leverage (each fix unlocks the next):

1. **CSV-1** (inverse map in `DelimitedIndexManager`) — also fixes TSV-1, JSONL-2 (analogous).
2. **CSV-6** (LinkedHashSet posting lists) — also fixes TSV-6, JSONL-4 (analogous).
3. **CSV-4 / CSV-5** (inverse-map-based shift) — also fixes TSV-4 / TSV-5, JSONL-3 (analogous).
4. **CSV-7** (engine bulk-mode wrapping) — also fixes TSV (no separate TSV prompt needed), JSONL-8.
5. **Avro-1** (AvroSchemaIndex cache) — also enables Avro-2, Avro-3, Avro-19.
6. **Avro-13** (case-insensitive lookup map per row) — also enables Avro-14, Avro-15, Avro-16, Avro-17.
7. **Avro-11** (tombstones + `subMap` shifts) — also enables Avro-12, depends on Avro-9, Avro-10.
8. **Buffer-4** (dirty-page set) and **Buffer-7** (free-slot stack) — independent of each other; fix in parallel.
9. **Buffer-1** (catalog HashMap) — also fixes Buffer-2, Buffer-3.
10. **Buffer-6** (slot directory compaction) — depends on Buffer-7.
11. **MVCC-1** (row-readers index) — single highest-impact MVCC fix.
12. **MVCC-2** (`readPhysicalRow`) — enables MVCC-3.
13. **MVCC-4** (prefix-count array).
14. **WAL-1** (LSN→segment index + `lastLsn` in header) — enables WAL-2, WAL-3, WAL-4.
15. **WAL-6** (cross-phase segment cache) — enables WAL-7, WAL-8.
16. **WAL-9** (swap-reference for active set on CHECKPOINT).
17. **WAL-5** (delete redundant sort) — trivial.
18. **JSONL-1, JSONL-5, JSONL-6, JSONL-7, JSONL-9, JSONL-10, JSONL-11** — localized fixes, do in any order.
19. **Avro-4, Avro-5, Avro-6, Avro-7, Avro-8, Avro-18** — localized fixes, do in any order.

---

End of `o2_prompts.md` — 65 hotspots, 65 remediation prompts.
