# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-09-06 15:00
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 6.2.1.4610 (JAVA_HOME=JDK21)
**Java Version:** 21.0.11
**Analysis:** Live SonarQube server scan (localhost:9000), report rebuilt from the live server state

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 15209 |
| Files | 71 |
| Functions | 915 |
| Classes | 120 |
| Duplicated Lines Density | 3.5% |
| Comment Lines Density | 14.8% |
| Test Coverage | 0.0% |
| Tests | 753 |
| Complexity | 3988 |
| Cognitive Complexity | 4802 |

## Issue Summary by Severity (open, unresolved)

| Severity | Count |
|----------|-------|
| CRITICAL | 133 |
| MAJOR | 311 |
| MINOR | 68 |
| INFO | 17 |
| BLOCKER | 0 |

## Issue Summary by Type (open, unresolved)

| Type | Count |
|------|-------|
| BUG | 19 |
| CODE_SMELL | 510 |
| VULNERABILITY | 0 |
| SECURITY_HOTSPOT | 0 |

## Top 30 Rules by Count

| # | Rule | Type | Severity | Count |
|---|------|------|----------|-------|
| 1 | java:S5869 | CODE_SMELL | MAJOR | 102 |
| 2 | java:S3776 | CODE_SMELL | CRITICAL | 86 |
| 3 | java:S1192 | CODE_SMELL | CRITICAL | 25 |
| 4 | java:S5843 | CODE_SMELL | MAJOR | 17 |
| 5 | java:S2925 | CODE_SMELL | MAJOR | 15 |
| 6 | java:S3008 | CODE_SMELL | MINOR | 13 |
| 7 | java:S3457 | CODE_SMELL | MAJOR | 13 |
| 8 | java:S1068 | CODE_SMELL | MAJOR | 12 |
| 9 | java:S112 | CODE_SMELL | MAJOR | 12 |
| 10 | java:S6541 | CODE_SMELL | INFO | 12 |
| 11 | java:S135 | CODE_SMELL | MINOR | 12 |
| 12 | java:S3358 | CODE_SMELL | MAJOR | 11 |
| 13 | java:S2259 | BUG | MAJOR | 11 |
| 14 | java:S6213 | CODE_SMELL | MAJOR | 10 |
| 15 | java:S1948 | CODE_SMELL | CRITICAL | 10 |
| 16 | java:S127 | CODE_SMELL | MAJOR | 10 |
| 17 | java:S1172 | CODE_SMELL | MAJOR | 9 |
| 18 | java:S1168 | CODE_SMELL | MAJOR | 9 |
| 19 | java:S2629 | CODE_SMELL | MAJOR | 9 |
| 20 | java:S1141 | CODE_SMELL | MAJOR | 8 |
| 21 | java:S1905 | CODE_SMELL | MINOR | 8 |
| 22 | java:S1452 | CODE_SMELL | CRITICAL | 7 |
| 23 | java:S3740 | CODE_SMELL | MAJOR | 6 |
| 24 | java:S1066 | CODE_SMELL | MAJOR | 6 |
| 25 | java:S1117 | CODE_SMELL | MAJOR | 5 |
| 26 | java:S1144 | CODE_SMELL | MAJOR | 5 |
| 27 | java:S1128 | CODE_SMELL | MINOR | 5 |
| 28 | java:S108 | CODE_SMELL | MAJOR | 5 |
| 29 | java:S1450 | CODE_SMELL | MINOR | 5 |
| 30 | java:S106 | CODE_SMELL | MAJOR | 5 |

## Top 25 Files by Count

| File | Issues |
|------|--------|
| diesel/QueryParser.java | 126 |
| diesel/SubqueryParser.java | 110 |
| diesel/SelectQuery.java | 73 |
| diesel/Table.java | 28 |
| diesel/Database.java | 21 |
| diesel/DatabaseServer.java | 20 |
| diesel/BTreeIndex.java | 16 |
| diesel/QueryOptimizer.java | 10 |
| diesel/AggregateFunctions.java | 9 |
| diesel/QueryExecutor.java | 8 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 8 |
| diesel/BTreeClusteredIndex.java | 8 |
| diesel/DatabaseClient.java | 8 |
| diesel/ConditionEvaluator.java | 7 |
| diesel/SqlLexer.java | 6 |
| src/test/java/diesel/OomHandlingTest.java | 5 |
| diesel/CompositeBTreeIndex.java | 5 |
| diesel/InsertQuery.java | 4 |
| diesel/PreparedStatement.java | 4 |
| diesel/BloomFilter.java | 4 |
| diesel/Cursor.java | 4 |
| src/test/java/diesel/AutoWhereIndexTest.java | 3 |
| src/test/java/diesel/PerformanceTest.java | 3 |
| src/test/java/diesel/SocketTimeoutTest.java | 3 |
| diesel/CoveringBTreeIndex.java | 3 |

## Issues by Severity and Type

| Severity/Type | BUG | CODE_SMELL | VULNERABILITY | SECURITY_HOTSPOT |
|---------------|-----|------------|---------------|------------------|
| CRITICAL | 1 | 132 | 0 | 0 |
| MAJOR | 14 | 297 | 0 | 0 |
| MINOR | 4 | 64 | 0 | 0 |
| INFO | 0 | 17 | 0 | 0 |

## Evolution of Key Metrics (vs analytics/sonar6.md)

| Metric | Previous Value (sonar6) | Current Value | Change |
|--------|------------------------|---------------|--------|
| Lines of Code (ncloc) | 12771 | 15209 | +2438 (+19.1%) |
| Files | 56 | 71 | +15 (+26.8%) |
| Functions | 742 | 915 | +173 (+23.3%) |
| Classes | 100 | 120 | +20 (+20.0%) |
| Duplicated Lines Density | 3.9 | 3.5 | -0.4 (-10.3%) |
| Comment Lines Density | 13.7 | 14.8 | +1.1 (+8.0%) |
| Test Coverage | 0 | 0 | 0 |
| Tests | 697 | 753 | +56 |
| Complexity | 3392 | 3988 | +596 (+17.6%) |
| Cognitive Complexity | 4189 | 4802 | +613 (+14.6%) |
| CRITICAL Issues | 114 | 133 | +19 (+16.7%) |
| MAJOR Issues | 293 | 311 | +18 (+6.1%) |
| MINOR Issues | 33 | 68 | +35 (+106.1%) |
| INFO Issues | 18 | 17 | -1 (-5.6%) |
| **Total open issues** | **458** | **529** | **+71 (+15.5%)** |

## Quality Gate Status

| Condition | Status | Threshold | Actual |
|-----------|--------|-----------|--------|
| New Line Coverage | ERROR | LT 80 | 0.0 |
| New Duplicated Lines Density | OK | GT 3 | 1.16629 |
| New Security Hotspots Reviewed | ERROR | LT 100 | 0.0 |
| New Violations | ERROR | GT 0 | 100 |
| **Overall Condition** | **ERROR** | - | - |

- Leak period: PREVIOUS_VERSION (baseline version `0.5.0-SNAPSHOT`, dated 2026-08-30).

## Remediation Effort

- Estimated remediation effort (sqale_index): 6684 min (= 111.4 h)
- Debt ratio: 1.5%
- Reliability rating: D (4.0) — 19 bugs, remediation 185 min
- Security rating: A (1.0) — 0 vulnerabilities
- Maintainability rating: A (1.0) — 510 code smells
- Security hotspots: 35 (0 reviewed)

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, and the list of locations (file:line — message).

### java:S5869 — 102 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 102

**Example message:** Remove duplicates in this character class.

**Locations:**

- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class. (102 repeated occurrences on the same line)
- `... and 101 more in the same compiled pattern location`

### java:S3776 — 86 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 86

**Example message:** Refactor this method to reduce its Cognitive Complexity from N to the 15 allowed.

**Locations:**

- `diesel/DeleteQuery.java`:56 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/InsertQuery.java`:64 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/UpdateQuery.java`:85 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- `diesel/UpdateQuery.java`:173 — Refactor this method to reduce its Cognitive Complexity from 50 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:57 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:154 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:209 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/CliRepl.java`:166 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/SqlLexer.java`:108 — Refactor this method to reduce its Cognitive Complexity from 80 to the 15 allowed.
- `diesel/QueryParser.java`:239 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/QueryParser.java`:1040 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- `diesel/QueryParser.java`:1179 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/QueryParser.java`:1546 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/QueryParser.java`:1927 — Refactor this method to reduce its Cognitive Complexity from 58 to the 15 allowed.
- `diesel/QueryParser.java`:2379 — Refactor this method to reduce its Cognitive Complexity from 62 to the 15 allowed.
- `diesel/QueryParser.java`:3055 — Refactor this method to reduce its Cognitive Complexity from 70 to the 15 allowed.
- `diesel/QueryParser.java`:3155 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/QueryParser.java`:3311 — Refactor this method to reduce its Cognitive Complexity from 51 to the 15 allowed.
- `diesel/SelectQuery.java`:1026 — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- `diesel/SelectQuery.java`:1435 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/SelectQuery.java`:1602 — Refactor this method to reduce its Cognitive Complexity from 62 to the 15 allowed.
- `diesel/SelectQuery.java`:2575 — Refactor this method to reduce its Cognitive Complexity from 58 to the 15 allowed.
- `diesel/SelectQuery.java`:3274 — Refactor this method to reduce its Cognitive Complexity from 36 to the 15 allowed.
- `diesel/SubqueryParser.java`:180 — Refactor this method to reduce its Cognitive Complexity from 29 to the 15 allowed.
- `diesel/SubqueryParser.java`:614 — Refactor this method to reduce its Cognitive Complexity from 36 to the 15 allowed.
- `diesel/SubqueryParser.java`:1584 — Refactor this method to reduce its Cognitive Complexity from 63 to the 15 allowed.
- `diesel/Table.java`:1890 — Refactor this method to reduce its Cognitive Complexity from 87 to the 15 allowed.
- `diesel/Table.java`:2046 — Refactor this method to reduce its Cognitive Complexity from 89 to the 15 allowed.
- `diesel/Database.java`:833 — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- `diesel/Database.java`:949 — Refactor this method to reduce its Cognitive Complexity from 53 to the 15 allowed.
- `diesel/DatabaseServer.java`:439 — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.
- `diesel/BTreeIndex.java`:641 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/BTreeClusteredIndex.java`:328 — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- `diesel/QueryExecutor.java`:52 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `... and 52 more`

### java:S1192 — 25 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 25

**Example message:** Define a constant instead of duplicating this literal.

**Locations:**

- `diesel/QueryParser.java`:1238 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/QueryParser.java`:1271 — Define a constant instead of duplicating this literal "quotedString" 3 times.
- `diesel/QueryParser.java`:1279 — Define a constant instead of duplicating this literal "openParen" 3 times.
- `diesel/QueryParser.java`:1283 — Define a constant instead of duplicating this literal "closeParen" 3 times.
- `diesel/QueryParser.java`:1413 — Define a constant instead of duplicating this literal "(?i)^(" 9 times.
- `diesel/QueryParser.java`:2391 — Define a constant instead of duplicating this literal "(?i)(" 7 times.
- `diesel/QueryParser.java`:2972 — Define a constant instead of duplicating this literal "(SELECT" 5 times.
- `diesel/SubqueryParser.java`:182 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/SubqueryParser.java`:268 — Define a constant instead of duplicating this literal "(?i)^(" 8 times.
- `diesel/SubqueryParser.java`:286 — Define a constant instead of duplicating this literal "SUBQUERY_" 3 times.
- `diesel/SubqueryParser.java`:973 — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- `diesel/SubqueryParser.java`:1280 — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 times.
- `diesel/Table.java`:383 — Define a constant instead of duplicating this literal "Column " 6 times.
- `diesel/Table.java`:643 — Define a constant instead of duplicating this literal "Duplicate key '" 3 times.
- `diesel/Table.java`:643 — Define a constant instead of duplicating this literal "' found in column " 3 times.
- `diesel/Table.java`:1506 — Define a constant instead of duplicating this literal " in column " 3 times.
- `diesel/Database.java`:780 — Define a constant instead of duplicating this literal "(?i)FROM\\s+" 4 times.
- `diesel/DatabaseClient.java`:106 — Define a constant instead of duplicating this literal "Client is not connected: call connect() first" 8 times.
- `diesel/DatabaseClient.java`:141 — Define a constant instead of duplicating this literal "Transaction started: " 3 times.
- `diesel/DatabaseServer.java`:515 — Define a constant instead of duplicating this literal "Error: " 4 times.
- `diesel/AggregateFunctions.java`:441 — Define a constant instead of duplicating this literal "ms, Speedup: " 3 times.
- `... and 4 more`

### java:S5843 — 17 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 17

**Example message:** Simplify this regular expression to reduce its complexity.

**Locations:**

- `diesel/SubqueryParser.java`:74 — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- `diesel/SubqueryParser.java`:117 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- `diesel/SubqueryParser.java`:269 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:270 — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- `diesel/SubqueryParser.java`:812 — Simplify this regular expression to reduce its complexity from 53 to the 20 allowed.
- `diesel/SubqueryParser.java`:847 — Simplify this regular expression to reduce its complexity from 32 to the 20 allowed.
- `diesel/SubqueryParser.java`:973 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:975 — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- `diesel/SubqueryParser.java`:977 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:983 — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- `diesel/SubqueryParser.java`:1119 — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- `diesel/SubqueryParser.java`:1648 — Simplify this regular expression to reduce its complexity from 45 to the 20 allowed.
- `diesel/QueryParser.java`:1412 — Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.
- `diesel/QueryParser.java`:1503 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- `diesel/QueryParser.java`:1763 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.
- `diesel/QueryParser.java`:2417 — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- `diesel/QueryParser.java`:2651 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.

### java:S2925 — 15 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 15

**Example message:** Remove this use of "Thread.sleep()".

**Locations:**

- `src/test/java/diesel/CursorTest.java`:263 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PreparedStatementTest.java`:265 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AnalyzeTableTest.java`:154 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/OomHandlingTest.java`:160 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AllTestsSampleTest.java`:1053 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AllTestsSampleTest.java`:1246 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:987 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:1180 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/GracefulShutdownTest.java`:99 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/SocketTimeoutTest.java`:59 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:61 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:80 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:84 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:321 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:360 — Remove this use of "Thread.sleep()".

### java:S3008 — 13 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 13

**Example message:** Rename this field to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Locations:**

- `diesel/BTreeIndex.java`:29 — Rename this field "PARALLEL_INDEX_SCAN_THRESHOLD".
- `diesel/BloomFilter.java`:19 — Rename this field "DEFAULT_NUM_HASHES".
- `diesel/BloomFilter.java`:20 — Rename this field "DEFAULT_FPP".
- `diesel/DatabaseServer.java`:42 — Rename this field "POOL_SIZE".
- `diesel/DatabaseServer.java`:43 — Rename this field "QUEUE_CAPACITY".
- `diesel/DatabaseServer.java`:238 — Rename this field "DEFAULT_COMPRESSION_THRESHOLD".
- `diesel/DatabaseServer.java`:239 — Rename this field "DEFAULT_COMPRESSION_LEVEL".
- `diesel/SelectQuery.java`:191 — Rename this field "MAX_IN_MEMORY_ROWS".
- `diesel/SelectQuery.java`:201 — Rename this field "MAX_HASH_TABLE_SIZE_BYTES".
- `diesel/SelectQuery.java`:212 — Rename this field "MAX_RESULT_ROWS".
- `diesel/SelectQuery.java`:221 — Rename this field "HASH_JOIN_OVERHEAD_ROWS".
- `diesel/SelectQuery.java`:307 — Rename this field "MEMORY_SAMPLE_INTERVAL".
- `diesel/PreparedStatement.java`:42 — Rename this field "MAX_CACHE_SIZE".

### java:S3457 — 13 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Example message:** Format strings should be used correctly / first argument is not used.

**Locations:**

- `diesel/DatabaseServer.java`:387 — String contains no format specifiers.
- `diesel/Table.java`:1485 — first argument is not used.
- `diesel/Table.java`:1505 — first argument is not used.
- `diesel/QueryParser.java`:2486 — first argument is not used.
- `diesel/SubqueryParser.java`:1015 — 2nd argument is not used.
- `diesel/SubqueryParser.java`:1015 — 4th argument is not used.
- `diesel/SubqueryParser.java`:1020 — 3rd argument is not used.
- `src/test/java/diesel/SocketTimeoutTest.java`:88 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:74 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:85 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:102 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:105 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:113 — Format specifiers or lambda should be used instead of string concatenation.

### java:S1068 — 12 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 12

**Example message:** Remove this unused "private" field.

**Locations:**

- `diesel/DatabaseServer.java`:218 — Remove this unused "socketTimeout" private field.
- `diesel/DatabaseServer.java`:261 — Remove this unused "compressionAlgorithm" private field.
- `diesel/SelectQuery.java`:66 — Remove this unused "lastJoinEstimatedRows" private field.
- `diesel/SelectQuery.java`:67 — Remove this unused "lastJoinActualRows" private field.
- `diesel/QueryOptimizer.java`:47 — Remove this unused "cacheSize" private field.
- `diesel/QueryOptimizer.java`:48 — Remove this unused "samplingRows" private field.
- `diesel/QueryOptimizer.java`:161 — Remove this unused "lastEstimatedRows" private field.
- `diesel/QueryOptimizer.java`:162 — Remove this unused "lastActualRows" private field.
- `diesel/QueryExecutor.java`:21 — Remove this unused "LOGGER" private field.
- `diesel/Table.java`:194 — Remove this unused "COMPACT_THRESHOLD" private field.
- `diesel/BloomFilter.java`:19 — Remove this unused "DEFAULT_NUM_HASHES" private field.
- `src/test/java/diesel/LimitOffsetTest.java`:21 — Remove this unused "DATE_FORMATTER" private field.

### java:S2259 — 11 occurrences

**Severity:** MAJOR | **Type:** BUG | **Found:** 11

**Example message:** A "NullPointerException" could be thrown; "<var>" is nullable here.

**Locations:**

- `diesel/SqlParsingUtils.java`:55 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/QueryParser.java`:812 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/QueryParser.java`:886 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.
- `diesel/QueryParser.java`:905 — A "NullPointerException" could be thrown; "innerNormalized" is nullable here.
- `diesel/QueryParser.java`:1351 — A "NullPointerException" could be thrown; "original" is nullable here.
- `diesel/QueryParser.java`:1697 — "NullPointerException" will be thrown when invoking method "extractLimit()".
- `diesel/QueryParser.java`:1707 — "NullPointerException" will be thrown when invoking method "extractOffset()".
- `diesel/QueryParser.java`:1715 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:1748 — A "NullPointerException" could be thrown; "groupByClause" is nullable here.
- `diesel/QueryParser.java`:3406 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/SelectQuery.java`:3387 — A "NullPointerException" could be thrown; "buildTable" is nullable here.

### java:S3358 — 11 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 11

**Example message:** Extract this nested ternary operation into an independent statement.

**Locations:**

- `diesel/ConditionEvaluator.java`:69 — Extract this nested ternary operation into an independent statement.
- `diesel/ConditionEvaluator.java`:211 — Extract this nested ternary operation into an independent statement.
- `diesel/DeleteQuery.java`:89 — Extract this nested ternary operation into an independent statement.
- `diesel/UpdateQuery.java`:214 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:354 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:1447 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:3229 — Extract this nested ternary operation into an independent statement.
- `diesel/SubqueryParser.java`:1671 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2826 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2938 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2943 — Extract this nested ternary operation into an independent statement.

### java:S6213 — 10 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 10

**Example message:** Rename this variable/method to not match a restricted identifier.

**Locations:**

- `diesel/QueryProfiler.java`:110 — Rename this method to not match a restricted identifier.
- `src/test/java/diesel/AutoWhereIndexTest.java`:98 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/AutoJoinIndexTest.java`:35 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/AutoJoinIndexTest.java`:93 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:100 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:127 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:131 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:141 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:170 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:189 — Rename this variable to not match a restricted identifier.

### java:S1948 — 10 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 10

**Example message:** Make this field transient or serializable.

**Locations:**

- `diesel/ExecutePreparedMessage.java`:20 — Make "params" transient or serializable.
- `diesel/CoveringBTreeIndex.java`:20 — Make "coverData" transient or serializable.
- `diesel/BTreeIndex.java`:52 — Make "keys" transient or serializable.
- `diesel/BTreeIndex.java`:53 — Make "rowIndices" private or transient.
- `diesel/BTreeIndex.java`:54 — Make "children" private or transient.
- `diesel/BTreeClusteredIndex.java`:22 — Make "keys" transient or serializable.
- `diesel/BTreeClusteredIndex.java`:23 — Make "rowIndices" private or transient.
- `diesel/BTreeClusteredIndex.java`:24 — Make "children" private or transient.
- `diesel/UniqueIndex.java`:15 — Make "indexMap" transient or serializable.
- `diesel/HashIndex.java`:15 — Make "indexMap" transient or serializable.

### java:S127 — 10 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 10

**Example message:** Refactor the code in order to not assign to this loop counter from within the loop body.

**Locations:**

- `diesel/SqlLexer.java`:89 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:92 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:546 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1561 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1567 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:1190 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:1195 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3122 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3126 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SelectQuery.java`:2788 — Refactor the code in order to not assign to this loop counter from within the loop body.

### java:S1172 — 9 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Example message:** Remove this unused method parameter.

**Locations:**

- `diesel/QueryOptimizer.java`:131 — Remove this unused method parameter "durationNanos".
- `diesel/SubqueryParser.java`:1128 — Remove this unused method parameter "not".
- `diesel/SubqueryParser.java`:1165 — Remove this unused method parameter "not".
- `diesel/SubqueryParser.java`:1431 — Remove this unused method parameter "tableAliases".
- `diesel/QueryParser.java`:2735 — Remove this unused method parameter "not".
- `diesel/QueryParser.java`:3019 — Remove this unused method parameter "tableAliases".
- `diesel/ConditionEvaluator.java`:154 — Remove this unused method parameter "columnTypes".
- `diesel/SelectQuery.java`:2436 — Remove this unused method parameter "combinedColumnTypes".
- `src/test/java/diesel/PerformanceTest.java`:112 — Remove this unused method parameter "random".

### java:S1168 — 9 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Example message:** Return an empty collection instead of null.

**Locations:**

- `diesel/Table.java`:108 — Return an empty map instead of null.
- `diesel/SelectQuery.java`:1820 — Return an empty map instead of null.
- `diesel/SelectQuery.java`:2438 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2446 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2479 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2548 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2568 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2613 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2652 — Return an empty collection instead of null.

### java:S1141 — 8 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Example message:** Extract this nested try block into a separate method.

**Locations:**

- `diesel/DatabaseClient.java`:114 — Extract this nested try block into a separate method.
- `diesel/DatabaseClient.java`:463 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:163 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:166 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:171 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:453 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:509 — Extract this nested try block into a separate method.
- `diesel/QueryParser.java`:60 — Extract this nested try block into a separate method.

### java:S6541 — 12 occurrences

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 12

**Example message:** A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC, Complexity, Nesting Level, Number of Variables.

**Locations:**

- `diesel/DeleteQuery.java`:56 — A "Brain Method" was detected (LOC 91, Complexity 41).
- `diesel/InsertQuery.java`:64 — A "Brain Method" was detected (LOC 107, Complexity 32).
- `diesel/SqlLexer.java`:108 — A "Brain Method" was detected (LOC 116, Complexity 34).
- `diesel/Table.java`:1323 — A "Brain Method" was detected (LOC 102).
- `diesel/Database.java`:833 — A "Brain Method" was detected (LOC 89, Complexity 28).
- `diesel/Database.java`:949 — A "Brain Method" was detected (LOC 98, Complexity 24).
- `diesel/QueryParser.java`:2379 — A "Brain Method" was detected (LOC 136, Complexity 22).
- `diesel/QueryParser.java`:3055 — A "Brain Method" was detected (LOC 95, Complexity 40).
- `diesel/QueryParser.java`:3155 — A "Brain Method" was detected (LOC 95, Complexity 29).
- `diesel/SelectQuery.java`:1435 — A "Brain Method" was detected (LOC 110, Complexity 23).
- `diesel/SelectQuery.java`:1602 — A "Brain Method" was detected (LOC 86, Complexity 26).
- `diesel/SubqueryParser.java`:1584 — A "Brain Method" was detected (LOC 104, Complexity 34).

## Notes

- Analysis performed with SonarScanner CLI 6.2.1.4610 against the live SonarQube server 10.7.0.96327 (localhost:9000), using a generated admin token.
- 529 open issues reported on the current analysis (510 code smells, 19 bugs, 0 vulnerabilities) out of 1494 total issues fetched (open + resolved).
- 35 security hotspots are open and unreviewed; 0 vulnerabilities.
- Quality gate is ERROR: 100 new violations introduced since the sonar6 baseline (2026-08-30), 0% new coverage, and 0% of security hotspots reviewed. The new-duplication condition (GT 3) PASSED at 1.17%.
- The codebase grew significantly since sonar6: ncloc 12771 -> 15209 (+19.1%), files 56 -> 71 (+26.8%), classes 100 -> 120, functions 742 -> 915. Open issues rose from 458 to 529 (+15.5%), driven mostly by new code; duplication density improved (3.9% -> 3.5%).
- Top remaining remediation targets: java:S5869 (102 duplicated regex character classes), java:S3776 (86 high cognitive complexity), java:S1192 (25 duplicated literals), java:S5843 (17 complex regexes).
- New high-count rules appearing in sonar7 that were small/absent before: java:S3008 (13 static-field naming), java:S1068 (12 unused fields), java:S112 (12 generic exceptions), java:S1948 (10 non-serializable fields on Serializable classes).
