# Prompt Status Tracker

> Latest: **Prompt 29 (Section 1a) DONE (2026-09-12)** — Deterministic encoding and line endings: replaced platform-default `FileReader`/`FileWriter` in `diesel/storage` with `Files.newBufferedReader`/`Files.newBufferedWriter` via a new `StorageConfig` helper honoring the new `storage.charset` config (default UTF-8, system-property overridable); fixed `\n` line endings instead of platform `newLine()` in CsvRowWriter/TsvRowWriter. New CharsetEncodingTest (5 tests): Unicode round-trips, byte-level UTF-8 + no-CR checks, windows-1251 charset override. Quick suite 155/0/0/3, full suite 155/0/0/0 BUILD SUCCESS, timing exit 0.

> Latest: **Prompt 30 (Section 1a) DONE (2026-09-12)** — Atomic file writes + fsync: new `diesel/storage/AtomicFileWriter` (temp+rename pattern: write to `<target>.tmp`, `FileChannel.force(true)`, `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)`); CsvRowStorage/TsvRowStorage saveCsv/saveTsv/saveSerialized and Table.writeLegacyCsv/saveToSerializedFile now route through it, so a crash mid-save can never truncate the previous valid file; missing-target + orphan `.tmp` → WARNING on load. New AtomicFileWriteTest (7 tests). Quick suite 162/0/0/3, full suite 162/0/0/0 BUILD SUCCESS, timing no heavy-query regressions (exit 0).

> Latest: **Prompt 31 (Section 1a) DONE (2026-09-12)** - Reader correctness fixes: strict boolean parsing — new `DelimitedRowReader.parseBooleanStrict()` parses true/false/1/0/yes/no/t/f (case-insensitive, trimmed) instead of silently `Boolean.parseBoolean`-mapping arbitrary strings to false; invalid values raise `IllegalArgumentException` flowing into the existing file:line:column diagnostics + `storage.load.error.mode` (fail|skip_row|skip_value). CsvRowReader/TsvRowReader now log a single WARNING per file (with exact line number) when a data row has more fields than schema columns, then ignore the extras. CsvRowStorage/TsvRowStorage `insert()` keep a ref to the copied map — removed redundant `rows.get(rows.size()-1)`. New ReaderCorrectnessTest (11 tests). Quick 173/0/0/3, full suite 173/0/0/0 BUILD SUCCESS, timing no heavy-query (>100ms) regressions (exit 0).

## Priority Queue (Pareto 20% - Critical First)

| ??? | ??T�-T?T?T? | ??T????-T???T�?T? | ??-????T? | ??T??-?-?????-?- |
|---|--------|-----------|-------|----------|
| 1 | ??? DONE (2026-09-02) | CRITICAL | SelectQuery.java, QueryParser.java | JOIN OR ??? OOM (?+?????-T?T�-?-?- ??T??-?????-???+???-????) - verified: hash join retained for OR-ON joins with equality key (hasOrInOnConditions warning present), 600x600 ORDER BY OR joins return 600 rows, heavy suite 42/42 green |
| 5 | ??? DONE | CRITICAL | QueryParser.java | IN + AND ?????-?-T???T?T??T?T?T? (??T???T�?TG-?-T? TH???T?T?T??-T??T?) - verified: evaluateConditions3vl AND-binds-tighter-than-OR, InTest 61/61 green |
| 3 | ??? DONE (2026-09-06) | HIGH | DeleteQuery.java | Refactored DeleteQuery.java: extract methods validateConditions, prepareDelete, executeDelete, updateIndexes to reduce cognitive complexity |
| 81 | ??? DONE (2026-09-03) | MEDIUM | Cursor.java, SelectQuery.java, Database.java, DatabaseServer.java, DatabaseClient.java | Query result pagination (server-side cursors, keyset pagination WHERE id > last_seen_id LIMIT N, stateless pagination LIMIT/OFFSET with caching) - verified: CursorTest 12/12 green, quick gate 734/0/0/2 BUILD SUCCESS |
| 4 | ??? DONE (2026-09-06) | HIGH | SqlLexer.java | Refactored SqlLexer.java: apply State Machine pattern with methods handleIdentifier, handleNumber, handleString |
| 22 | ???? IN_PROGRESS | HIGH | Multiple (13 ?-??T?T?) | Null Pointer Dereference |
| 17 | ✅ DONE (2026-09-10) | MEDIUM | pom.xml, tests | Reduced test heap: split QuantitativeTest into 10 small classes (100 rows), heavy joins @LargeTest (600 rows), AbstractDieselTest base, surefire includes, Makefile quick-test. 99/99 pass, 3 skipped |
| 29 | ??? DONE (2026-08-17) | MEDIUM | SelectQuery.java | Refactor execute() complexity=59 |
| 28 | ??? DONE (2026-08-17) | MEDIUM | QueryParser.java | Cognitive Complexity ?-??T�??-?????-T??T? |
| 41 | ??? DONE (2026-08-19) | HIGH | QueryParser.java, SubqueryParser.java, SelectQuery.java | ??-?-???-?- '[A-Za-z0-9_]' ?-?- '\w' ?- regex (S6353, 119 ??T??-?-?????-) |
| 42 | ??? DONE (2026-08-19) | CRITICAL | QueryParser.java, SubqueryParser.java, SelectQuery.java | ??TH-??T�-T????-?? ?-??T�-?+?-?- T? ?-T?T??-???-?? Cognitive Complexity (S3776, 92 ??T??-?-?????-T?) |
| 43 | ??? DONE (2026-08-19) | MEDIUM | Database.java, SelectQuery.java, ConditionEvaluator.java, ExplainQuery.java, DatabaseClient.java, CliRepl.java, SubqueryParser.java, Table.java, BTreeIndex.java, BTreeClusteredIndex.java, DatabaseServer.java, DeleteQuery.java | Pattern matching ?+??T? instanceof (S6201, 55 ??T????-?-T??-???-?-?-?-????) |
| 44 | ??? DONE (2026-08-19) | HIGH | QueryParser.java, SubqueryParser.java, SelectQuery.java, RegexRobustnessTest.java | ??T?T?T??-?-???-???? T?????T?T?T????-?-T?T? ???-T?T�?T??-?-?- ?- regex (S5998, 57 ??T??-?-?????-) + fix parseRightPart bug |
| 45 | ? DONE (2026-08-19) | MEDIUM | 14 engine files | ?????????? ????????????? ????????? ????????? ? ErrorMessages (S1192) |
| 46 | ? DONE (2026-08-19) | LOW | 7 files | ???????? ?????????????? ???????? (S1128, 13 issues) |
| 47 | ? DONE (2026-08-19) | LOW | SelectQuery, QueryParser, ConditionEvaluator, SubqueryParser | ??????????? break/continue ? ?????? (S135) |
| 48 | ? DONE (2026-08-19) | MEDIUM | QueryParser, SelectQuery, SubqueryParser | ???????? ?????????????? ?????????? ??????? (S1172, 10 params) |
| 49 | ? DONE (2026-08-19) | HIGH | SelectQuery, 10 test files | ?????????? ?????? ?????? ???? (S108, 25 blocks) |
| 50 | ? DONE (2026-08-19) | MEDIUM | ? | Deprecated setScale() (S1874) ? audit, 0 issues |

## Full Status (Prompts 1-100)

### Section 0: Priority Retrospective Fixes (1-20)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 1 | ???? IN_PROGRESS | JOIN T? OR ?- T?T????-?-???? (OOM) | CRITICAL |
| 2 | ??? DONE (2026-09-06) | ???T�??-?????-T??T? ???-?-T?T�? Cross Join (streaming) | HIGH |
| 3 | ??? DONE (2026-09-06) | GROUP BY T? T?-?????-??T?-T?-?? ???-?-TG??-??T?-?? | HIGH |
| 4 | ??? DONE (2026-09-06) | HIGH | SqlLexer.java | Refactored SqlLexer.java: apply State Machine pattern with methods handleIdentifier, handleNumber, handleString |
| 5 | ??? DONE | IN T? ?+?-???-???-??T�???T?-T?-?? T?T????-?-??T?-?? (AND/OR) | CRITICAL |
| 6 | ???? IN_PROGRESS | LIMIT ?-???? OFFSET | MEDIUM |
| 7 | ???? IN_PROGRESS | OFFSET ?-???? LIMIT | MEDIUM |
| 8 | ???? IN_PROGRESS | LIMIT + OFFSET ?-?-??T?T�? | MEDIUM |
| 9 | ✅ DONE (2026-09-10) | LIMIT в подзапросах | MEDIUM |
| 10 | ???? IN_PROGRESS | Hash Join ?-??T�??-?????-T??T? | MEDIUM |
| 11 | ???? IN_PROGRESS | EXPLAIN ?????-?- ?-T???-???-???-??T? | LOW |
| 12 | ???? IN_PROGRESS | ????-??T? ?-?- ?-?-??T?. ???-????TG?T?T�-?- T?T?T??-?? | MEDIUM |
| 13 | ???? IN_PROGRESS | ??T?T?T???-???? ?-T???-?-?? OOM | MEDIUM |
| 14 | ??? DONE (2026-09-08) | ?�-T�-?-?-T�?TG?T????-T? T?T�-T�?T?T�????- | LOW |
| 15 | ???? IN_PROGRESS | ??-?+????T?T? ?+??T? JOIN | MEDIUM |
| 16 | 🔄 IN_PROGRESS | Незавершённые NullPointer-исправления | LOW |
| 17 | ✅ DONE (2026-09-10) | MEDIUM | pom.xml, tests | Reduced test heap: split QuantitativeTest into 10 small classes (100 rows), heavy joins @LargeTest (600 rows), AbstractDieselTest base, surefire includes, Makefile quick-test. 99/99 pass, 3 skipped |
| 18 | ??? DONE (2026-08-16) | ??T??-TH?????T??-?-T???? ??T??-?????-?-?+??T�???T?-?-T?T�? | LOW |
| 19 | ✅ DONE (2026-09-10) | ??T?T?T? ?-?- T?????T???T?T???T? | MEDIUM |
| 20 | ??? DONE (2026-08-16) | ??-??T?-???-T�-T??T? ?-??T??-?-??TG??-???? | LOW |

### Section 1: Sonar Code Smells (21-40)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 21 | ??? DONE (2026-08-16) | StackOverflow ?- regex (S5998) | HIGH |
| 22 | ??? DONE (2026-08-16) | Null Pointer Dereference (S2259) | HIGH |
| 23 | ??? DONE (2026-08-16) | ??+?-?????-???? ?-T?T?T�-?-???- ???-?+?- (S2583, S108, S1144, S1068) | LOW |
| 24 | ??? DONE (2026-08-16) | Double Brace Initialization (S3599) | LOW |
| 25 | ??? DONE (2026-08-16) | ????-?-T???T??-?-?-?-???? ?-?-???-T??-T?-???-T?T? ???-?-TG??-???? (S899) | MEDIUM |
| 26 | ??? DONE (2026-08-17) | Regex grouping (S5850) | MEDIUM |
| 27 | ??? DONE (2026-08-17) | Regex repeated patterns (S5842) | MEDIUM |
| 28 | ??? DONE (2026-08-17) | Cognitive Complexity QueryParser (S3776) | MEDIUM |
| 29 | ??? DONE (2026-08-17) | Refactor SelectQuery.execute() (complexity=59) | HIGH |
| 30 | ??? DONE (2026-08-17) | ???T�??-?????-T??T? regex (S5869, S6353) | LOW |
| 31 | ??? DONE (2026-08-17) | String literals ?- ???-?-T?T�-?-T?T? (S1192) | LOW |
| 32 | ??? DONE (2026-08-18) | ??-T??-?-??T?T?T? ?-??T�-?+?-?- (S107) | LOW |
| 33 | ??? DONE (2026-08-18) | Boolean null (S2447) | MEDIUM |
| 34 | ??? DONE (2026-08-18) | Serializable ???-??T? (S1948) | LOW |
| 35 | ??? DONE (2026-08-18) | Logger ?-?-??T?T�- System.out (S106) | LOW |
| 36 | ??? DONE (2026-08-18) | ????T??TH?TG-T?? ??T?????T?TG??-??T? (S112) | LOW |
| 37 | ??? DONE (2026-08-18) | ??-T??-?-?-T�??- ??T?????T?TG??-???? (S2139, S1141) | LOW |
| 38 | ??? DONE (2026-08-18) | Unused ???-T??-?-??T?T?T?/????T????-???-?-T??/???-???-T?T?T? | LOW |
| 39 | ??? DONE (2026-08-18) | ??T??-T???-???? T?T????-?-???? | LOW |
| 40 | ??? DONE (2026-08-18) | ???-?-??T?-?-T? ?-TG?T?T�??- CODE_SMELL | LOW |

### Section 1a: CSV/TSV Storage Improvements (24-38)

| # | Status | Description |
|---|--------|-------------|
| 24 | ✅ DONE (2026-09-11) | CSV/TSV header column mapping — readHeader() parses file header, strips BOM, builds column mapping by name (case-insensitive), validates against schema. Configurable mismatch mode (fail|warn). Quick 133/0/0. |
| 25 | ✅ DONE (2026-09-11) | Stable row-id instead of positional indexes in DelimitedIndexManager — key→rowId indexes + rowId→position map, insertAt shifts only positions (no rebuild), deleteRow uses tombstones with 25% compaction threshold; cluster insert / search correctness regression tests. Quick 138/0/0, full suite 138/0/0. |
| 26 | ✅ DONE (2026-09-11) | Distinguish NULL from empty string in CSV/TSV (sentinel \N) — new storage.null.representation=legacy\|sentinel config; sentinel mode: TSV null→\N (literal \N escaped to \\N), CSV ""→quoted field / null→unquoted empty; readers decode losslessly; legacy keeps old semantics. New NullSentinelTest (10 tests). Quick 138/0/0, full suite 138/0/0 green. |
| 27 | ✅ DONE (2026-09-11) | Load error handling and diagnostics (file:line:column) — CsvRowReader/TsvRowReader wrap conversion failures in DieselIOException with file:line:column context (bad.csv:line 3: column 'AGE': cannot parse "xyz" as Integer); unterminated CSV quoted fields detected at EOF; new storage.load.error.mode = fail|skip_row|skip_value (default fail) with skip modes logging + skipping; loadCsv/loadTsv now transactional (rollback to previous rows on DieselIOException/IOException, then rethrow); getLineNumber() added to DelimitedRowReader. New LoadErrorHandlingTest (10 tests). Quick 148/0/0/3, full suite 148/0/0/0 green, timing no regressions (exit 0). |
| 28 | ✅ DONE (2026-09-11) | Escape column names in writeHeader — CsvRowWriter.writeHeader() and TsvRowWriter.writeHeader() escape each column name via escapeValue() (RFC 4180 quoting for CSV, backslash-escaping for TSV) instead of a bare String.join; TsvRowReader.readHeader() unescapes parsed header names so escaped names (a\tb) re-match the schema (pairs with Prompt 24). New tests csvEscapedHeaderRoundTrip/tsvEscapedHeaderRoundTrip ("price, rub", "a\tb"). Quick 150/0/0/3. |
| 29 | ✅ DONE (2026-09-12) | Deterministic encoding and line endings — added storage.charset (default UTF-8); new StorageConfig helper resolves the charset (system property → config.properties) and provides newReader/newWriter via Files.newBufferedReader/newBufferedWriter; CsvRowStorage/TsvRowStorage, DelimitedIndexManager (3 sites) and CsvIndexManager no longer use platform-default FileReader/FileWriter; CsvRowWriter/TsvRowWriter write explicit \n instead of newLine(). New CharsetEncodingTest (5 tests): Unicode round-trip, byte-level UTF-8 / no-CR checks, windows-1251 override. Quick 155/0/0/3, full suite 155/0/0/0 BUILD SUCCESS, timing exit 0. |
| 30 | ✅ DONE (2026-09-12) | Atomic file writes + fsync — new diesel/storage/AtomicFileWriter (temp+rename: <target>.tmp → FileChannel.force(true) → Files.move(ATOMIC_MOVE, REPLACE_EXISTING)); CsvRowStorage/TsvRowStorage (saveCsv/saveTsv/saveSerialized) and Table (writeLegacyCsv/saveToSerializedFile) write through it, so an interrupted save never truncates the previous valid file; load paths warn (WARNING) when a target is missing but an orphan .tmp exists. New AtomicFileWriteTest (7 tests): CSV/TSV interrupted save keeps previous file, discard-vs-commit semantics, orphan tmp WARNING. Quick 162/0/0/3, full suite 162/0/0/0 BUILD SUCCESS, timing no heavy-query (>100ms) regressions, compare-timing exit 0. |
| 31 | ✅ DONE (2026-09-12) | Reader correctness fixes — DelimitedRowReader.parseBooleanStrict() parses true/false/1/0/yes/no/t/f strictly (case-insensitive, trimmed) instead of silent Boolean.parseBoolean->false; invalid values throw IllegalArgumentException flowing into existing file:line:column diagnostics + storage.load.error.mode (fail|skip_row|skip_value). CsvRowReader/TsvRowReader log one WARNING per file (with line number) when a data row has more fields than schema columns, then ignore the extras. CsvRowStorage/TsvRowStorage insert() keeps a ref to the copied map — removed redundant rows.get(rows.size()-1). New ReaderCorrectnessTest (11 tests). Quick 173/0/0/3, full suite 173/0/0/0 BUILD SUCCESS, timing no heavy-query (>100ms) regressions, compare-timing exit 0. |

### Section 2: Sonar Top-10 Pareto (41-50)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 41 | ??? DONE (2026-08-19) | ??-?-???-?- '[A-Za-z0-9_]' ?-?- '\w' ?- regex (S6353, 119 ??T??-?-?????-) | HIGH |
| 42 | ??? DONE (2026-08-19) | ??TH-??T�-T????-?? ?-??T�-?+?-?- T? ?-T?T??-???-?? Cognitive Complexity (S3776, 92 ??T??-?-?????-T?) | CRITICAL |
| 43 | ??? DONE (2026-08-19) | Pattern matching ?+??T? instanceof (S6201, 55 ??T????-?-T??-???-?-?-?-????) | MEDIUM |
| 44 | ??? DONE (2026-08-19) | ??T?T?T??-?-???-???? T?????T?T?T????-?-T?T? ???-T?T�?T??-?-?- ?- regex (S5998, 57 ??T??-?-?????-) + fix parseRightPart bug | HIGH |
| 45 | ? DONE (2026-08-19) | ?????????? ????????????? ????????? ????????? ? ErrorMessages (S1192) | MEDIUM |
| 46 | ? DONE (2026-08-19) | ???????? ?????????????? ???????? (S1128, 13 issues) | LOW |
| 47 | ? DONE (2026-08-19) | ??????????? break/continue ? ?????? (S135) | LOW |
| 48 | ? DONE (2026-08-19) | ???????? ?????????????? ?????????? ??????? (S1172, 10 params) | MEDIUM |

| 49 | ? DONE (2026-08-19) | ?????????? ?????? ?????? ???? (S108, 25 blocks) | HIGH |
| 50 | ? DONE (2026-08-19) | Deprecated setScale() (S1874) ? audit, 0 issues | MEDIUM |
### Section 3: Performance Optimizations (41-60)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 41 |? DONE (2026-08-19)| updateIndicesAfterInsert O(n+?m+?log n) | HIGH |
| 42 |? DONE (2026-08-19)| Nested Loop ??? Hash Join | HIGH |
| 43 | ???? IN_PROGRESS | ??-?+????T?T? ???-T????? DELETE | MEDIUM |
| 44 | ???? IN_PROGRESS | ????-T?T�?T??????-?-?-?-?-T?? ???-?+????T? T??-???+?-?-???? | MEDIUM |
| 45 | ???? IN_PROGRESS | ??-????T�-?-T? ?-T?T�-?-???- ??T??? ???-??T?T?????? | MEDIUM |
| 46 | ???? IN_PROGRESS | ??-?+????T?T? ?+??T? WHERE T?T????-?-???? | MEDIUM |
| 47 | ? DONE (2026-08-23) | ??-T?T??-?-T?? UPDATE | MEDIUM |
| 48 | ? DONE (2026-08-23) | indexDefinitions T???T????-???????-T??T? | MEDIUM |
| 49 | ???? IN_PROGRESS | ??-T?T�-T?-???? ???-?+????T??-?- ?- T???T????-???????-T???? | MEDIUM |
| 50 | ???? IN_PROGRESS | Copy-on-Write ?+??T? T?T??-?-???-??T???? | HIGH |
| 51 | ???? IN_PROGRESS | ??-T??-????????T?-?-?? ?-T???-???-???-???? ???-??T??-T??-?- | LOW |
| 52 | ???? IN_PROGRESS | ??T????-T?T??-?-?-T?? I/O | LOW |
| 53 | DONE (2026-09-01) | Compression ??T? T???T�? (GZIP negotiation + metrics) | LOW |
| 54 | DONE (2026-09-02) | Prepared Statements caching (implemented and fully tested as part of prompt 79) | MEDIUM |
| 55 | ???? IN_PROGRESS | Batch execution | MEDIUM |
| 56 | ???? IN_PROGRESS | Pagination T?????T??T?T�-T�-?- | MEDIUM |
| 57 | ???? IN_PROGRESS | Adaptive query execution | LOW |
| 58 | ???? IN_PROGRESS | Index-only scans | LOW |
| 59 |  DONE (2026-08-23) | Serialized index persistence | LOW |
| 60 | DONE (2026-08-23) | Copy-on-Write transaction isolation | LOW |

### Section 4: Parquet Integration (61-92)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 61 | ???? IN_PROGRESS | Apache Parquet ???-T�???T??-T??T? | HIGH |
| 62 | DONE (2026-08-23) | HIGH | QueryParser.java, SelectQuery.java | Cognitive Complexity S3776 refactoring | HIGH |
| 63 | ???? IN_PROGRESS | Columnar storage | HIGH |
| 64 | DONE (2026-08-23) | CRITICAL | QueryParser.java, SubqueryParser.java, SelectQuery.java | Recursive regex patterns S5998 | CRITICAL |
| 65 | DONE (2026-08-23) | CRITICAL | SqlKeywords.java, QueryParser.java, SqlLexer.java, SubqueryParser.java, Database.java | Extract repeated string literals S1192 | CRITICAL |
| 66 | ???? IN_PROGRESS | Compression codecs | MEDIUM |
| 67 | DONE (2026-08-23) | Remove unused method parameters S1172 | MEDIUM |
| 68 | DONE (2026-08-24) | Reduce break/continue in loops S135 | MINOR |
| 69 | DONE (2026-08-24) | Fill/remove empty code blocks S108 | MEDIUM |
| 70 | DONE (2026-08-24) | Remove deprecated setScale() S1874 | MEDIUM |
| 71 | DONE (2026-08-28) | Remove unused local variables (S1481) | MINOR |
| 72 | DONE (2026-08-28) | Remove useless assignments (S1854) | MEDIUM |
| 73 | DONE (2026-08-29) | ??-?-?-?????+?-T??T? ??T??? UPDATE | MEDIUM |
| 74 | DONE (2026-08-29) | ??-?-?-?????+?-T??T? ??T??? DELETE | MEDIUM |
| 75 | ??? DONE (2026-08-29) | ??-?-?-?????+?-T??T? ??T??? DDL | MEDIUM |
| 76 | ??? DONE (2026-08-31) | Parallel execution of independent queries | LOW |
| 77 | ???? IN_PROGRESS | ??T?T�?T??-?-?-?-???? Parquet | HIGH |
| 78 | ???? IN_PROGRESS | ??T?T�?T??-?-?-?-???? QueryCache | MEDIUM |
| 79 | DONE (2026-09-02) | Prepared Statements caching | HIGH |
| 80 | DONE (2026-09-02) | Batch execution support (BEGIN BATCH ... END BATCH) | MEDIUM |
| 81 | ???? IN_PROGRESS | ??-?-TH???T?T??-T??T? Parquet ?-?- T?T??-?-?-?? T�-?-????T?T? | MEDIUM |
| 82 | ✅ DONE (2026-09-03) | MEDIUM | QueryOptimizer.java, SelectQuery.java, QueryOptimizerTest.java | Adaptive query execution (monitor estimated vs actual rows, replan joins on deviation, LRU plan cache with fingerprint normalization) - verified: QueryOptimizerTest 10/10 green, quick gate 42/0/0/2 BUILD SUCCESS |
| 83 | ✅ DONE (2026-09-03) | Index-only scans | MEDIUM | SelectQuery.java, QueryProfiler.java, Database.java, ExplainQuery.java, CoveringIndexTest.java | Added index-only scan metrics: lastIndexLookupCount/lastIndexOnlyScanCount counters in SelectQuery, exposed via EXPLAIN ANALYZE and QueryProfiler JMX MBean, with tests - quick gate 42/0 BUILD SUCCESS |
| 84 | ✅ DONE (2026-09-03) | Parallel index scan | LOW | BTreeIndex.java, SelectQuery.java, config.properties, ParallelIndexScanTest.java | Added parallel index scanning: BTreeIndex gains rangeSearchParallel/rangeSearchLowParallel/rangeSearchHighParallel using a dedicated ForkJoinPool that splits work by root subtrees (subtreeMayContainInRange), estimates result size (countAllKeys/countKeysAbove/countKeysBelow/estimateBoundedRangeSize) with a configurable parallel.index.scan.threshold (default 10000) below which sequential scan is used; SelectQuery.lookupBTreeRange now calls the parallel variants; new ParallelIndexScanTest (8 tests) verifies parallel results match sequential for 20k-row indexes, boundary values, null bounds, empty ranges, and ascending order. Quick gate 42/0/0/0 BUILD SUCCESS (incl. ParallelIndexScanTest 8/8) |
| 85 | DONE (2026-09-04) | SIMD vectorization for aggregates | LOW |
| 86 | ???? IN_PROGRESS | Database.java ?+??T? Parquet default | MEDIUM |
| 87 | ???? IN_PROGRESS | ??-T??-?-?-T�??- ?-T???-?-?? ?-????T??-T???? | MEDIUM |
| 88 | ???? IN_PROGRESS | Partitioned tables ?- Parquet | MEDIUM |
| 89 | ???? IN_PROGRESS | Dictionary encoding ?+??T? T?T?T??-?? | LOW |
| 90 | ???? IN_PROGRESS | Compression tuning (ZSTD) | LOW |
| 91 | ???? IN_PROGRESS | Row group size tuning | LOW |
| 92 | ???? IN_PROGRESS | Column statistics metadata | LOW |

### Section 5: Advanced Features (93-100)

| ??? | ??T�-T?T?T? | ??-???-?-?-???? | ??T????-T???T�?T? |
|---|--------|----------|-----------|
| 93 | ???? IN_PROGRESS | Bloom filters ?+??T? Parquet | MEDIUM |
| 94 | ???? IN_PROGRESS | Cache warm-up strategy | LOW |
| 95 | ???? IN_PROGRESS | Adaptive TTL ?+??T? ??T?T?- | LOW |
| 96 | ???? IN_PROGRESS | Query normalization improvements | MEDIUM |
| 97 | ???? IN_PROGRESS | Parameterized query caching | MEDIUM |
| 98 | ???? IN_PROGRESS | Multi-level cache (L1/L2) | LOW |
| 99 | ???? IN_PROGRESS | Cache persistence across restarts | LOW |
| 100 | ???? IN_PROGRESS | Final integration testing & docs | HIGH |

## Legend

- ???? IN_PROGRESS - Not started
- ???? IN_PROGRESS - Currently working
- ??? DONE - Completed
- ?????? BLOCKED - Blocked by dependency

## How to Update

1. Choose next prompt from Priority Queue (top of table)
2. Change status to ???? IN_PROGRESS
3. After implementation and tests pass, change to ??? DONE
4. Add date completed in format `??? DONE (2025-01-15)`


