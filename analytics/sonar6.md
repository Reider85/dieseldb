# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-08-30 02:48:37
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 6.2.1.4610 (JAVA_HOME=JDK21)
**Java Version:** 21.0.11

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 12771 |
| Files | 56 |
| Functions | 742 |
| Classes | 100 |
| Duplicated Lines Density | 3.9% |
| Comment Lines Density | 13.7% |
| Test Coverage | 0.0% |
| Tests | 697 |
| Complexity | 3392 |
| Cognitive Complexity | 4189 |

## Issue Summary by Severity

| Severity | Count |
|----------|-------|
| CRITICAL | 114 |
| MAJOR | 293 |
| MINOR | 33 |
| INFO | 18 |

## Issue Summary by Type

| Type | Count |
|------|-------|
| BUG | 19 |
| CODE_SMELL | 439 |
| VULNERABILITY | 0 |
| SECURITY_HOTSPOT | 0 |

## Top 30 Rules by Count

| # | Rule | Type | Severity | Count |
|---|------|------|----------|-------|
| 1 | java:S5869 | CODE_SMELL | MAJOR | 102 |
| 2 | java:S3776 | CODE_SMELL | CRITICAL | 80 |
| 3 | java:S5843 | CODE_SMELL | MAJOR | 17 |
| 4 | java:S1192 | CODE_SMELL | CRITICAL | 16 |
| 5 | java:S2925 | CODE_SMELL | MAJOR | 13 |
| 6 | java:S6541 | CODE_SMELL | INFO | 12 |
| 7 | java:S3457 | CODE_SMELL | MAJOR | 12 |
| 8 | java:S135 | CODE_SMELL | MINOR | 11 |
| 9 | java:S2259 | BUG | MAJOR | 11 |
| 10 | java:S3358 | CODE_SMELL | MAJOR | 11 |
| 11 | java:S6213 | CODE_SMELL | MAJOR | 10 |
| 12 | java:S127 | CODE_SMELL | MAJOR | 10 |
| 13 | java:S1168 | CODE_SMELL | MAJOR | 9 |
| 14 | java:S1948 | CODE_SMELL | CRITICAL | 9 |
| 15 | java:S1172 | CODE_SMELL | MAJOR | 8 |
| 16 | java:S6485 | CODE_SMELL | MAJOR | 8 |
| 17 | java:S6206 | CODE_SMELL | MAJOR | 7 |
| 18 | java:S1141 | CODE_SMELL | MAJOR | 7 |
| 19 | java:S3740 | CODE_SMELL | MAJOR | 6 |
| 20 | java:S1905 | CODE_SMELL | MINOR | 5 |
| 21 | java:S1066 | CODE_SMELL | MAJOR | 5 |
| 22 | java:S1452 | CODE_SMELL | CRITICAL | 5 |
| 23 | java:S2629 | CODE_SMELL | MAJOR | 5 |
| 24 | java:S6880 | CODE_SMELL | MAJOR | 4 |
| 25 | java:S6395 | CODE_SMELL | MAJOR | 4 |
| 26 | java:S2139 | CODE_SMELL | MAJOR | 4 |
| 27 | java:S6204 | CODE_SMELL | MAJOR | 4 |
| 28 | java:S1144 | CODE_SMELL | MAJOR | 4 |
| 29 | java:S112 | CODE_SMELL | MAJOR | 4 |
| 30 | java:S1068 | CODE_SMELL | MAJOR | 4 |

## Top 25 Files by Count

| File | Issues |
|------|--------|
| diesel/QueryParser.java | 128 |
| diesel/SubqueryParser.java | 111 |
| diesel/SelectQuery.java | 78 |
| diesel/Table.java | 28 |
| diesel/DatabaseServer.java | 9 |
| diesel/ConditionEvaluator.java | 8 |
| diesel/Database.java | 8 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 8 |
| diesel/BTreeClusteredIndex.java | 8 |
| diesel/BTreeIndex.java | 8 |
| diesel/CompositeBTreeIndex.java | 7 |
| diesel/SqlLexer.java | 6 |
| src/test/java/diesel/OomHandlingTest.java | 5 |
| diesel/InsertQuery.java | 4 |
| diesel/DeleteQuery.java | 3 |
| src/test/java/diesel/PerformanceTest.java | 3 |
| diesel/CoveringBTreeIndex.java | 3 |
| src/test/java/diesel/AutoWhereIndexTest.java | 3 |
| diesel/UpdateQuery.java | 3 |
| src/test/java/diesel/AllTestsSampleTest.java | 2 |
| src/test/java/diesel/QuantitativeTest.java | 2 |
| src/test/java/diesel/SocketTimeoutTest.java | 2 |
| diesel/CliRepl.java | 2 |
| diesel/QueryProfiler.java | 2 |
| diesel/SqlParsingUtils.java | 2 |

## Issues by Severity and Type

| Severity/Type | BUG | CODE_SMELL | VULNERABILITY | SECURITY_HOTSPOT |
|---------------|-----|------------|---------------|------------------|
| CRITICAL | 1 | 113 | 0 | 0 |
| MAJOR | 14 | 279 | 0 | 0 |
| MINOR | 4 | 29 | 0 | 0 |
| INFO | 0 | 18 | 0 | 0 |

## Evolution of Key Metrics (vs analytics/sonar4.md)

| Metric | Previous Value | Current Value | Change |
|--------|----------------|---------------|--------|
| Lines of Code (ncloc) | 10894 | 12771 | +1877 (+17.2%) |
| Files | 51 | 56 | +5 (+9.8%) |
| Functions | 614 | 742 | +128 (+20.8%) |
| Classes | 84 | 100 | +16 (+19.0%) |
| Duplicated Lines Density | 1.8 | 3.9 | +2.1 (+116.7%) |
| Comment Lines Density | 13.3 | 13.7 | +0.4 (+3.0%) |
| Test Coverage | 0 | 0 | 0 |
| CRITICAL Issues | 165 | 114 | -51 (-30.9%) |
| MAJOR Issues | 728 | 293 | -435 (-59.8%) |
| MINOR Issues | 363 | 33 | -330 (-90.9%) |
| INFO Issues | 41 | 18 | -23 (-56.1%) |

## Quality Gate Status

| Condition | Status | Threshold | Actual |
|-----------|--------|-----------|--------|
| New Line Coverage | ERROR | LT 80 | 0.0 |
| New Duplicated Lines Density | ERROR | GT 3 | 3.12096 |
| New Security Hotspots Reviewed | ERROR | LT 100 | 0.0 |
| New Violations | ERROR | GT 0 | 319 |
| **Overall Condition** | **ERROR** | - | - |

## Remediation Effort

- Estimated remediation effort (sqale_index): 5934 min (= 98.9 h)
- Debt ratio: 1.5%
- Reliability rating: D (4.0) — 19 bugs, remediation 185 min
- Security rating: A (1.0) — 0 vulnerabilities
- Maintainability rating: A (1.0) — 439 code smells
- Security hotspots: 34 (0 reviewed)

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, and the list of locations (file:line — message).

### java:S5869 — 102 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 102

**Example message:** Remove duplicates in this character class.

**Locations:**

- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- `diesel/SubqueryParser.java`:27 — Remove duplicates in this character class.
- ... and 87 more

### java:S3776 — 80 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 80

**Example message:** Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.

**Locations:**

- `diesel/DeleteQuery.java`:56 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:57 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- `diesel/InsertQuery.java`:64 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/UpdateQuery.java`:85 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- `diesel/SqlLexer.java`:108 — Refactor this method to reduce its Cognitive Complexity from 80 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:154 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/BTreeClusteredIndex.java`:156 — Refactor this method to reduce its Cognitive Complexity from 26 to the 15 allowed.
- `diesel/BTreeIndex.java`:160 — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- `diesel/CliRepl.java`:166 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/UpdateQuery.java`:173 — Refactor this method to reduce its Cognitive Complexity from 50 to the 15 allowed.
- `diesel/SubqueryParser.java`:180 — Refactor this method to reduce its Cognitive Complexity from 29 to the 15 allowed.
- `diesel/BTreeIndex.java`:203 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:209 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/QueryParser.java`:239 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/DatabaseServer.java`:247 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- ... and 65 more

### java:S5843 — 17 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 17

**Example message:** Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.

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
- `diesel/QueryParser.java`:1390 — Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.
- `diesel/QueryParser.java`:1481 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- `diesel/SubqueryParser.java`:1648 — Simplify this regular expression to reduce its complexity from 45 to the 20 allowed.
- `diesel/QueryParser.java`:1741 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.
- ... and 2 more

### java:S1192 — 16 occurrences

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 16

**Example message:** Define a constant instead of duplicating this literal "' found in column " 3 times.

**Locations:**

- `diesel/SubqueryParser.java`:182 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/SubqueryParser.java`:268 — Define a constant instead of duplicating this literal "(?i)^(" 8 times.
- `diesel/SubqueryParser.java`:286 — Define a constant instead of duplicating this literal "SUBQUERY_" 3 times.
- `diesel/Table.java`:383 — Define a constant instead of duplicating this literal "Column " 6 times.
- `diesel/Table.java`:643 — Define a constant instead of duplicating this literal "' found in column " 3 times.
- `diesel/Table.java`:643 — Define a constant instead of duplicating this literal "Duplicate key '" 3 times.
- `diesel/SubqueryParser.java`:973 — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- `diesel/QueryParser.java`:1216 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/QueryParser.java`:1249 — Define a constant instead of duplicating this literal "quotedString" 3 times.
- `diesel/QueryParser.java`:1257 — Define a constant instead of duplicating this literal "openParen" 3 times.
- `diesel/QueryParser.java`:1261 — Define a constant instead of duplicating this literal "closeParen" 3 times.
- `diesel/SubqueryParser.java`:1280 — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 times.
- `diesel/QueryParser.java`:1391 — Define a constant instead of duplicating this literal "(?i)^(" 9 times.
- `diesel/Table.java`:1504 — Define a constant instead of duplicating this literal " in column " 3 times.
- `diesel/QueryParser.java`:2369 — Define a constant instead of duplicating this literal "(?i)(" 7 times.
- ... and 1 more

### java:S2925 — 13 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Example message:** Remove this use of "Thread.sleep()".

**Locations:**

- `src/test/java/diesel/SocketTimeoutTest.java`:58 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:61 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:80 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:84 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/GracefulShutdownTest.java`:99 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AnalyzeTableTest.java`:154 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/OomHandlingTest.java`:160 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:321 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:360 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:985 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AllTestsSampleTest.java`:1049 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:1160 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AllTestsSampleTest.java`:1224 — Remove this use of "Thread.sleep()".

### java:S6541 — 12 occurrences

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 12

**Example message:** A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 73 to 64, Complexity from 19 to 14, Nesting Level from 5 to 2, Number of Variables from 33 to 6.

**Locations:**

- `diesel/DeleteQuery.java`:56 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 91 to 64,...
- `diesel/InsertQuery.java`:64 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 107 to 64...
- `diesel/SqlLexer.java`:108 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 116 to 64...
- `diesel/Table.java`:1323 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 102 to 64...
- `diesel/SelectQuery.java`:1333 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 110 to 64...
- `diesel/SelectQuery.java`:1500 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 86 to 64,...
- `diesel/SubqueryParser.java`:1513 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 66 to 64,...
- `diesel/SubqueryParser.java`:1584 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 104 to 64...
- `diesel/QueryParser.java`:2357 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 136 to 64...
- `diesel/SelectQuery.java`:2672 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 73 to 64,...
- `diesel/QueryParser.java`:3033 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 95 to 64,...
- `diesel/QueryParser.java`:3133 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 95 to 64,...

### java:S3457 — 12 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 12

**Example message:** first argument is not used.

**Locations:**

- `src/test/java/diesel/ServerConnectionLimitTest.java`:74 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/SocketTimeoutTest.java`:82 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:85 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:102 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:105 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:113 — Format specifiers or lambda should be used instead of string concatenation.
- `diesel/SubqueryParser.java`:1015 — 2nd argument is not used.
- `diesel/SubqueryParser.java`:1015 — 4th argument is not used.
- `diesel/SubqueryParser.java`:1020 — 3rd argument is not used.
- `diesel/Table.java`:1483 — first argument is not used.
- `diesel/Table.java`:1503 — first argument is not used.
- `diesel/QueryParser.java`:2464 — first argument is not used.

### java:S135 — 11 occurrences

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 11

**Example message:** Reduce the total number of break and continue statements in this loop to use at most one.

**Locations:**

- `diesel/CliRepl.java`:106 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:116 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:129 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:189 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/DatabaseServer.java`:252 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/Database.java`:367 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:883 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1200 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1401 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1522 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1592 — Reduce the total number of break and continue statements in this loop to use at most one.

### java:S2259 — 11 occurrences

**Severity:** MAJOR | **Type:** BUG | **Found:** 11

**Example message:** "NullPointerException" will be thrown when invoking method "extractLimit()".

**Locations:**

- `diesel/SqlParsingUtils.java`:55 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/QueryParser.java`:790 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/QueryParser.java`:864 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.
- `diesel/QueryParser.java`:883 — A "NullPointerException" could be thrown; "innerNormalized" is nullable here.
- `diesel/QueryParser.java`:1329 — A "NullPointerException" could be thrown; "original" is nullable here.
- `diesel/QueryParser.java`:1675 — "NullPointerException" will be thrown when invoking method "extractLimit()".
- `diesel/QueryParser.java`:1685 — "NullPointerException" will be thrown when invoking method "extractOffset()".
- `diesel/QueryParser.java`:1693 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:1726 — A "NullPointerException" could be thrown; "groupByClause" is nullable here.
- `diesel/SelectQuery.java`:3245 — A "NullPointerException" could be thrown; "buildTable" is nullable here.
- `diesel/QueryParser.java`:3384 — A "NullPointerException" could be thrown; "normalized" is nullable here.

### java:S3358 — 11 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 11

**Example message:** Extract this nested ternary operation into an independent statement.

**Locations:**

- `diesel/ConditionEvaluator.java`:69 — Extract this nested ternary operation into an independent statement.
- `diesel/DeleteQuery.java`:89 — Extract this nested ternary operation into an independent statement.
- `diesel/ConditionEvaluator.java`:211 — Extract this nested ternary operation into an independent statement.
- `diesel/UpdateQuery.java`:214 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:354 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:1425 — Extract this nested ternary operation into an independent statement.
- `diesel/SubqueryParser.java`:1671 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2684 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2796 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2801 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:3207 — Extract this nested ternary operation into an independent statement.

### java:S6213 — 10 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 10

**Example message:** Rename this variable to not match a restricted identifier.

**Locations:**

- `src/test/java/diesel/AutoJoinIndexTest.java`:35 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/AutoJoinIndexTest.java`:93 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/AutoWhereIndexTest.java`:98 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:100 — Rename this variable to not match a restricted identifier.
- `diesel/QueryProfiler.java`:100 — Rename this method to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:127 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:131 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:141 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:170 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:189 — Rename this variable to not match a restricted identifier.

### java:S127 — 10 occurrences

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 10

**Example message:** Refactor the code in order to not assign to this loop counter from within the loop body.

**Locations:**

- `diesel/SqlLexer.java`:89 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:92 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:546 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:1168 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:1173 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1561 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1567 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SelectQuery.java`:2646 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3100 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3104 — Refactor the code in order to not assign to this loop counter from within the loop body.

## Notes

- Analysis performed with SonarScanner CLI 6.2.1.4610 against the live SonarQube server 10.7.0.96327 (localhost:9000).
- 458 open issues reported on the current analysis (439 code smells, 19 bugs, 0 vulnerabilities).
- 34 security hotspots are open and unreviewed; the previous scan reported 0 vulnerabilities as well.
- Quality gate is ERROR: new code introduces 319 violations, 0% new coverage, 3.12% new duplication, and 0% of security hotspots reviewed.
- Duplicated-lines density jumped from 1.8% to 3.9% — new index/join code (hash index, CPD) contributed duplicate blocks.
- Top remaining remediation targets: java:S5869 (102 duplicated regex character classes), java:S3776 (80 high cognitive complexity), java:S5843 (17 complex regexes), java:S1192 (16 duplicated literals).

