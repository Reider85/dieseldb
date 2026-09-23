# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-09-23 14:30
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 6.2.1.4610 (JAVA_HOME=JDK21)
**Java Version:** 21.0.11 Axiom JSC
**Analysis:** Live SonarQube server scan (localhost:9000)

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 40723 |
| Files | 177 |
| Functions | 3291 |
| Classes | 380 |
| Duplicated Lines Density | 5.4% |
| Comment Lines Density | 18.5% |
| Test Coverage | 0.0% |
| Tests | 1198 |
| Complexity | 10580 |
| Cognitive Complexity | 9287 |

## Issue Summary by Severity (open, unresolved)

| Severity | Count |
|----------|-------|
| BLOCKER | 5 |
| CRITICAL | 205 |
| MAJOR | 542 |
| MINOR | 167 |
| INFO | 45 |

## Issue Summary by Type (open, unresolved)

| Type | Count |
|------|-------|
| BUG | 45 |
| CODE_SMELL | 919 |
| VULNERABILITY | 0 |
| SECURITY_HOTSPOT | 0 |

## Issues by Severity and Type

| Severity/Type | BUG | CODE_SMELL | VULNERABILITY | SECURITY_HOTSPOT |
|---------------|-----|------------|---------------|------------------|
| BLOCKER | 2 | 3 | 0 | 0 |
| CRITICAL | 5 | 200 | 0 | 0 |
| MAJOR | 25 | 517 | 0 | 0 |
| MINOR | 13 | 154 | 0 | 0 |
| INFO | 0 | 45 | 0 | 0 |

**Total open issues: 964**

## Top 30 Rules by Count

| # | Rule | Type | Severity | Count |
|---|------|------|----------|-------|
| 1 | java:S3776 | CODE_SMELL | CRITICAL | 100 |
| 2 | java:S1192 | CODE_SMELL | CRITICAL | 66 |
| 3 | java:S6213 | CODE_SMELL | MAJOR | 53 |
| 4 | java:S108 | CODE_SMELL | MAJOR | 41 |
| 5 | java:S1172 | CODE_SMELL | MAJOR | 38 |
| 6 | java:S1168 | CODE_SMELL | MAJOR | 29 |
| 7 | java:S1068 | CODE_SMELL | MAJOR | 27 |
| 8 | java:S6201 | CODE_SMELL | MINOR | 24 |
| 9 | java:S1854 | CODE_SMELL | MAJOR | 21 |
| 10 | java:S135 | CODE_SMELL | MINOR | 21 |
| 11 | java:S6885 | CODE_SMELL | MAJOR | 19 |
| 12 | java:S6485 | CODE_SMELL | MAJOR | 18 |
| 13 | java:S1481 | CODE_SMELL | MINOR | 17 |
| 14 | java:S1128 | CODE_SMELL | MINOR | 17 |
| 15 | java:S5869 | CODE_SMELL | MAJOR | 17 |
| 16 | java:S1905 | CODE_SMELL | MINOR | 17 |
| 17 | java:S6126 | CODE_SMELL | MINOR | 16 |
| 18 | java:S1948 | CODE_SMELL | CRITICAL | 16 |
| 19 | java:S1123 | CODE_SMELL | MINOR | 15 |
| 20 | java:S6355 | CODE_SMELL | MINOR | 15 |
| 21 | java:S1133 | CODE_SMELL | MINOR | 15 |
| 22 | java:S1066 | CODE_SMELL | MAJOR | 15 |
| 23 | java:S1144 | CODE_SMELL | MAJOR | 15 |
| 24 | java:S107 | CODE_SMELL | MAJOR | 13 |
| 25 | java:S6208 | CODE_SMELL | MINOR | 12 |
| 26 | java:S1141 | CODE_SMELL | MAJOR | 12 |
| 27 | java:S1117 | CODE_SMELL | MAJOR | 12 |
| 28 | java:S6541 | CODE_SMELL | INFO | 11 |
| 29 | java:S2259 | BUG | MAJOR | 10 |
| 30 | java:S127 | CODE_SMELL | MAJOR | 10 |

## Top 30 Files by Count

| File | Issues |
|------|--------|
| diesel/SelectQuery.java | 76 |
| diesel/QueryParser.java | 67 |
| diesel/SubqueryParser.java | 41 |
| diesel/storage/JsonlRowReader.java | 26 |
| diesel/Table.java | 25 |
| diesel/storage/avro/AvroRangePartitioner.java | 23 |
| diesel/storage/avro/SnappyOptimizedCodec.java | 23 |
| diesel/storage/DelimitedIndexManager.java | 22 |
| diesel/storage/avro/AvroMetrics.java | 22 |
| diesel/storage/avro/AvroRowStorage.java | 19 |
| diesel/storage/JsonlSchemaManager.java | 18 |
| diesel/storage/avro/AvroBackupManager.java | 18 |
| diesel/storage/DelimitedByteParser.java | 16 |
| diesel/storage/CsvRowStorage.java | 15 |
| diesel/storage/TsvRowStorage.java | 15 |
| diesel/storage/JsonlIndexManager.java | 14 |
| diesel/storage/avro/AvroPrimaryKeyIndex.java | 14 |
| diesel/storage/avro/AvroTransactionManager.java | 13 |
| diesel/QueryOptimizer.java | 12 |
| diesel/Database.java | 11 |
| diesel/storage/CsvRowReader.java | 11 |
| diesel/storage/avro/SchemaConflictResolver.java | 11 |
| diesel/BTreeIndex.java | 10 |
| diesel/DatabaseServer.java | 10 |
| diesel/storage/avro/AvroDatePartitioner.java | 10 |
| diesel/storage/avro/AvroHashPartitioner.java | 10 |
| diesel/storage/avro/AvroSecondaryIndexManager.java | 10 |
| diesel/storage/json/JsonSchemaInference.java | 10 |
| diesel/storage/avro/AdaptiveCompressionManager.java | 9 |
| diesel/storage/avro/AvroAuditLogger.java | 9 |

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, and example locations.

### java:S3776 — 100 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 100

**What's wrong:** Refactor this method to reduce its Cognitive Complexity from N to the 15 allowed.

**Example locations:**

- `diesel/InsertQuery.java`:88 — Cognitive Complexity 27 (limit 15)
- `diesel/QueryParser.java`:1838 — Cognitive Complexity 17 (limit 15)
- `diesel/QueryParser.java`:2217 — Cognitive Complexity 18 (limit 15)
- `diesel/QueryParser.java`:3141 — Cognitive Complexity 22 (limit 15)
- `diesel/SelectQuery.java`:1510 — Cognitive Complexity 23 (limit 15)

### java:S1192 — 66 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 66

**What's wrong:** Define a constant instead of duplicating this literal.

**Example locations:**

- `diesel/QueryParser.java`:214 — Literal "Condition column must not be null" duplicated 6 times
- `diesel/SubqueryParser.java`:213 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating
- `diesel/storage/avro/AvroAuditLogger.java`:559 — Literal "Invalid {} = \"{}\", using default {}" duplicated 3 times
- `diesel/storage/avro/AvroMetrics.java`:319 — Literal "[AVRO-METRIC] {}" duplicated 4 times
- `diesel/storage/avro/AvroMetrics.java`:362 — Literal "counter" duplicated 6 times

### java:S6213 — 53 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 53

**What's wrong:** Rename this variable/method to not match a restricted identifier.

**Example locations:**

- `diesel/storage/avro/AvroAuditLogger.java`:383
- `diesel/storage/avro/AvroQueryExecutor.java`:156, 180, 199, 218

### java:S108 — 41 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 41

**What's wrong:** Either remove or fill this block of code.

**Example locations:**

- `diesel/Database.java`:912
- `src/test/java/diesel/AllTestsSampleTest.java`:1075
- `src/test/java/diesel/PreparedStatementTest.java`:58
- `src/test/java/diesel/ServerConnectionLimitTest.java`:64, 65

### java:S1172 — 38 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 38

**What's wrong:** Remove this unused method parameter.

**Example locations:**

- `diesel/DeleteQuery.java`:243 — parameter "deletedCount"
- `diesel/InsertQuery.java`:200 — parameter "column"
- `diesel/QueryParser.java`:1292 — parameter "colDef"
- `diesel/QueryParser.java`:1374 — parameter "input"
- `diesel/SubqueryParser.java`:1651 — parameters "i", "havingClause"

### java:S1168 — 29 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 29

**What's wrong:** Return an empty collection instead of null.

**Example locations:**

- `diesel/QueryParser.java`:3240
- `diesel/SelectQuery.java`:2937
- `diesel/storage/avro/AvroTransactionManager.java`:405
- `diesel/storage/avro/AvroSecondaryIndex.java`:269
- `diesel/storage/avro/AvroPrimaryKeyIndex.java`:403

### java:S1068 — 27 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 27

**What's wrong:** Remove this unused "private" field.

**Example locations:**

- `diesel/storage/avro/AvroMetrics.java`:103 — "lastReadBytes"
- `diesel/storage/avro/AvroMetrics.java`:104 — "lastReadNanos"
- `diesel/storage/avro/AvroMetrics.java`:105 — "lastWriteBytes"
- `diesel/storage/avro/AvroMetrics.java`:106 — "lastWriteNanos"
- `diesel/storage/avro/AvroMetrics.java`:117 — "prometheusEnabled"

### java:S6201 — 24 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 24

**What's wrong:** Replace this instanceof check and cast with 'instanceof Type var'.

**Example locations:**

- `diesel/SelectQuery.java`:2710, 2727
- `diesel/storage/avro/AvroRangePartitioner.java`:231, 234, 237

### java:S1854 — 21 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 21

**What's wrong:** Remove this useless assignment to local variable.

**Example locations:**

- `diesel/DatabaseServer.java`:426 — "pendingInput"
- `diesel/Table.java`:1328 — "oldSize"
- `diesel/UpdateQuery.java`:242 — "rowsToUpdate"
- `diesel/storage/avro/AvroBackupManager.java`:462 — "totalFiles"

### java:S135 — 21 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 21

**What's wrong:** Reduce the total number of break and continue statements in this loop to use at most one.

**Example locations:**

- `diesel/SubqueryParser.java`:464
- `diesel/storage/avro/AvroBloomFilter.java`:350
- `diesel/storage/avro/AvroPrimaryKeyIndex.java`:428
- `diesel/storage/avro/AvroRestoreManager.java`:234, 273

### java:S6885 — 19 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 19

**What's wrong:** Use "Math.clamp" instead of "Math.min" or "Math.max".

**Example locations:**

- `diesel/SelectQuery.java`:1807
- `diesel/storage/avro/AvroBloomFilter.java`:484
- `diesel/storage/avro/AvroBloomFilterConfig.java`:92, 108, 109

### java:S6485 — 18 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 18

**What's wrong:** Replace this call to the constructor with the better suited static method HashMap.newHashMap(int numMappings).

**Example locations:**

- `diesel/SelectQuery.java`:963, 1610, 1616, 1629, 1677

### java:S1481 — 17 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 17

**What's wrong:** Remove this unused local variable.

**Example locations:**

- `diesel/Table.java`:1328 — "oldSize"
- `src/test/java/diesel/OomHandlingTest.java`:214 — "marker"
- `diesel/storage/avro/AvroPrimaryKeyIndex.java`:406 — "loadedPkIndex"
- `diesel/storage/avro/AvroBackupManager.java`:445 — "totalFiles"

### java:S1128 — 17 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 17

**What's wrong:** Remove this unused import.

**Example locations:**

- `diesel/BTreeIndex.java`:3, 4 — `java.io.File`, `java.io.FileInputStream`
- `diesel/QueryOptimizer.java`:3, 4 — `java.io.FileInputStream`, `java.io.File`
- `diesel/QueryParser.java`:3 — `java.io.IOException`

### java:S5869 — 17 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 17

**What's wrong:** Remove duplicates in this character class.

**Example locations:**

- `diesel/CreateAvroIndexQuery.java`:16 (17 repeated occurrences)
- `diesel/QueryParser.java`:76

### java:S1905 — 17 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 17

**What's wrong:** Remove this unnecessary cast.

**Example locations:**

- `diesel/storage/avro/AvroAuditLogger.java`:338 — cast to "long"
- `diesel/storage/avro/AvroMetrics.java`:256, 259 — cast to "double"

### java:S1948 — 16 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 16

**What's wrong:** Make this field transient or serializable.

**Example locations:**

- `diesel/storage/avro/AvroSecondaryIndex.java`:27 — "indexMap"
- `diesel/storage/avro/AvroSecondaryIndexManager.java`:22 — "columnTypes"
- `diesel/storage/avro/AvroDataValidator.java`:164 — "errors"

### java:S6541 — 11 occurrences

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 11

**What's wrong:** A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC, Complexity, Nesting Level, Number of Variables.

**Example locations:**

- `diesel/SubqueryParser.java`:841 — LOC 77, Complexity 22
- `diesel/storage/avro/AvroRangePartitioner.java`:477 — LOC 107, Complexity 19
- `diesel/storage/avro/AvroBloomFilter.java`:335 — LOC 83, Complexity 28

### java:S2259 — 10 occurrences

**Severity:** MAJOR | **Type:** BUG | **Found:** 10

**What's wrong:** A "NullPointerException" could be thrown; "<var>" is nullable here.

**Example locations:**

- `diesel/QueryParser.java`:1898 — NPE when invoking "extractOffset()"
- `diesel/Table.java`:1808 — NPE when invoking "validateRowForBulk()"
- `diesel/storage/json/JsonPathResolver.java`:110 — "path" is nullable here

### java:S5843 — 9 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**What's wrong:** Simplify this regular expression to reduce its complexity.

**Example locations:**

- `diesel/QueryParser.java`:92 — complexity 22 (limit 20)
- `diesel/QueryParser.java`:95 — complexity 23 (limit 20)
- `diesel/QueryParser.java`:124 — complexity 35 (limit 20)

### java:S107 — 13 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**What's wrong:** Method has N parameters, which is greater than 7 authorized.

**Example locations:**

- `diesel/QueryParser.java`:1569 — 9 parameters
- `diesel/QueryParser.java`:1775 — 11 parameters
- `diesel/QueryParser.java`:1796 — 11 parameters

### java:S3008 — 8 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 8

**What's wrong:** Rename this field to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Example locations:**

- `diesel/BloomFilter.java`:19 — "DEFAULT_NUM_HASHES"
- `diesel/BloomFilter.java`:20 — "DEFAULT_FPP"
- `diesel/DatabaseServer.java`:226 — "DEFAULT_COMPRESSION_THRESHOLD"

### java:S3457 — 6 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 6

**What's wrong:** Format strings should be used correctly / first argument is not used.

**Example locations:**

- `diesel/Table.java`:1612 — first argument is not used
- `diesel/SubqueryParser.java`:1139 — 2nd and 4th arguments are not used

### java:S2925 — 8 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**What's wrong:** Remove this use of "Thread.sleep()".

**Example locations:**

- `src/test/java/diesel/AvroTransactionManagerTest.java`:389, 547
- `src/test/java/diesel/AvroBackupManagerTest.java`:354

### java:S106 — 5 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 5

**What's wrong:** Replace this use of System.out by a logger.

**Example locations:**

- `diesel/AggregateFunctions.java`:378, 379, 441

## Quality Gate Status

| Condition | Status | Threshold | Actual |
|-----------|--------|-----------|--------|
| New Line Coverage | ERROR | LT 80 | 0.0 |
| New Duplicated Lines Density | ERROR | GT 3 | 3.52 |
| New Security Hotspots Reviewed | ERROR | LT 100 | 0.0 |
| New Violations | ERROR | GT 0 | 789 |
| **Overall Condition** | **ERROR** | - | - |

## Remediation Effort

- Estimated remediation effort (sqale_index): 8834 min (= 147.2 h)
- Debt ratio: 1.0%
- Reliability rating: E (5.0) — 45 bugs
- Security rating: A (1.0) — 0 vulnerabilities
- Maintainability rating: A (1.0) — 919 code smells
- Security hotspots: 21 (0 reviewed)

## Evolution of Key Metrics (vs analytics/sonar7.md)

| Metric | Previous Value (sonar7) | Current Value | Change |
|--------|------------------------|---------------|--------|
| Lines of Code (ncloc) | 15209 | 40723 | +25514 (+167.8%) |
| Files | 71 | 177 | +106 (+149.3%) |
| Functions | 915 | 3291 | +2376 (+259.7%) |
| Classes | 120 | 380 | +260 (+216.7%) |
| Duplicated Lines Density | 3.5 | 5.4 | +1.9 (+54.3%) |
| Comment Lines Density | 14.8 | 18.5 | +3.7 (+25.0%) |
| Test Coverage | 0 | 0 | 0 |
| Tests | 753 | 1198 | +445 (+59.1%) |
| Complexity | 3988 | 10580 | +6592 (+165.3%) |
| Cognitive Complexity | 4802 | 9287 | +4485 (+93.4%) |
| BLOCKER Issues | 0 | 5 | +5 (new) |
| CRITICAL Issues | 133 | 205 | +72 (+54.1%) |
| MAJOR Issues | 311 | 542 | +231 (+74.3%) |
| MINOR Issues | 68 | 167 | +99 (+145.6%) |
| INFO Issues | 17 | 45 | +28 (+164.7%) |
| **Total open issues** | **529** | **964** | **+435 (+82.2%)** |

> **Note on scope change:** sonar7 scanned only `diesel/` main sources (71 files, 15209 ncloc). sonar8 scanned both `diesel/` (177 main files) and `src/test/java/` (148 test files). The large increase in metrics is primarily due to including test files and the newly added AVRO storage module (~40 new source files).

## Notes

- Analysis performed with SonarScanner CLI 6.2.1.4610 against the live SonarQube server 10.7.0.96327 (localhost:9000), using a freshly generated admin token (sqa_a627c2c0...).
- 964 open issues reported (919 code smells, 45 bugs, 0 vulnerabilities).
- 21 security hotspots are open and unreviewed; 0 vulnerabilities.
- Quality gate is ERROR: 789 new violations introduced since the leak period baseline, 0% new coverage, 0% of security hotspots reviewed, and new duplication (3.52%) exceeds the 3% threshold.
- The codebase grew significantly since sonar7: ncloc 15209 -> 40723, files 71 -> 177, driven by the addition of the full AVRO storage module and test files now being included in analysis scope.
- Top remediation targets: java:S3776 (100 high cognitive complexity), java:S1192 (66 duplicated literals), java:S6213 (53 restricted identifiers), java:S108 (41 empty blocks), java:S1172 (38 unused parameters).
- New high-count rules in sonar8 (not prominent in sonar7): java:S6213 (53 restricted identifier names), java:S108 (41 empty blocks), java:S6201 (24 pattern matching), java:S6885 (19 Math.clamp suggestions), java:S6485 (18 HashMap.newHashMap suggestions).
- Rules that dropped significantly from sonar7: java:S5869 (102 -> 17), java:S5843 (17 -> 9), java:S2925 (15 -> 8), java:S3008 (13 -> 8), java:S3457 (13 -> 6) — these decreases are due to the previous scan counting 102 occurrences of S5869 as repeated regex character class issues in one compiled pattern location.
