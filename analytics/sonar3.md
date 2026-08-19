# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-08-19
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 6.2.1.4610
**Java Version:** 21.0.11 (Axiom JSC)

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 10894 |
| Files | 51 |
| Functions | 614 |
| Classes | 84 |
| Duplicated Lines Density | 1.8% |
| Comment Lines Density | 13.3% |
| Test Coverage | 0% |

## Issue Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 97 |
| MAJOR | 285 |
| MINOR | 68 |
| INFO | 17 |
| **Total** | **467** |

## Issue Types

| Type | Count |
|------|-------|
| CODE_SMELL | 452 |
| BUG | 15 |
| VULNERABILITY | 0 |

## Top Rules by Issue Count

| Count | Rule | Name | Severity | Type |
|-------|------|------|----------|------|
| 114 | java:S5869 | Character classes in regular expressions should not contain the same character twice | MAJOR | CODE_SMELL |
| 68 | java:S3776 | Cognitive Complexity of methods should not be too high | CRITICAL | CODE_SMELL |
| 17 | java:S135 | Loops should not contain more than a single "break" or "continue" statement | MINOR | CODE_SMELL |
| 15 | java:S1192 | String literals should not be duplicated | CRITICAL | CODE_SMELL |
| 14 | java:S1481 | Unused local variables should be removed | MINOR | CODE_SMELL |
| 13 | java:S5843 | Regular expressions should not be too complicated | MAJOR | CODE_SMELL |
| 13 | java:S2925 | "Thread.sleep" should not be used in tests | MAJOR | CODE_SMELL |
| 12 | java:S1172 | Unused method parameters should be removed | MAJOR | CODE_SMELL |
| 12 | java:S3457 | Format strings should be used correctly | MAJOR | CODE_SMELL |
| 12 | java:S1854 | Unused assignments should be removed | MAJOR | CODE_SMELL |
| 12 | java:S6541 | Methods should not perform too many tasks (aka Brain method) | INFO | CODE_SMELL |
| 11 | java:S127 | "for" loop stop conditions should be invariant | MAJOR | CODE_SMELL |
| 11 | java:S5857 | Character classes should be preferred over reluctant quantifiers in regular expressions | MINOR | CODE_SMELL |
| 9 | java:S6213 | Restricted Identifiers should not be used as Identifiers | MAJOR | CODE_SMELL |
| 9 | java:S2259 | Null pointers should not be dereferenced | MAJOR | BUG |
| 9 | java:S3358 | Ternary operators should not be nested | MAJOR | CODE_SMELL |
| 8 | java:S1141 | Try-catch blocks should not be nested | MAJOR | CODE_SMELL |
| 8 | java:S6485 | Hash-based collections with known capacity should be initialized with the proper static method | MAJOR | CODE_SMELL |
| 8 | java:S1948 | Fields in a "Serializable" class should either be transient or serializable | CRITICAL | CODE_SMELL |
| 6 | java:S3740 | Raw types should not be used | MAJOR | CODE_SMELL |
| 5 | java:S1452 | Generic wildcard types should not be used in return types | CRITICAL | CODE_SMELL |
| 4 | java:S6395 | Non-capturing groups without quantifier should not be used | MAJOR | CODE_SMELL |
| 4 | java:S6204 | "Stream.toList()" method should be used instead of "collectors" | MAJOR | CODE_SMELL |
| 4 | java:S6353 | Regular expression quantifiers and character classes should be used concisely | MINOR | CODE_SMELL |
| 4 | java:S2629 | "Preconditions" and logging arguments should not require evaluation | MAJOR | CODE_SMELL |
| 4 | java:S2139 | Exceptions should be either logged or rethrown but not both | MAJOR | CODE_SMELL |
| 4 | java:S1168 | Empty arrays and collections should be returned instead of null | MAJOR | CODE_SMELL |
| 4 | java:S1066 | Mergeable "if" statements should be combined | MAJOR | CODE_SMELL |
| 4 | java:S3626 | Jump statements should not be redundant | MINOR | CODE_SMELL |
| 3 | java:S2589 | Boolean expressions should not be gratuitous | MAJOR | CODE_SMELL |
| 3 | java:S6880 | Use switch instead of if-else chain | MAJOR | CODE_SMELL |
| 3 | java:S125 | Sections of code should not be commented out | MAJOR | CODE_SMELL |
| 3 | java:S1905 | Redundant casts should not be used | MINOR | CODE_SMELL |
| 3 | java:S3008 | Static non-final field names should comply with a naming convention | MINOR | CODE_SMELL |
| 2 | java:S6208 | Comma-separated labels should be used in Switch with colon case | INFO | CODE_SMELL |
| 2 | java:S6539 | Classes should not depend on an excessive number of classes (Monster Class) | INFO | CODE_SMELL |
| 2 | java:S3824 | "Map.get" and value test should be replaced with single method call | MAJOR | CODE_SMELL |
| 2 | java:S107 | Methods should not have too many parameters | MAJOR | CODE_SMELL |
| 2 | java:S5850 | Alternatives in regular expressions should be grouped when used with anchors | MAJOR | BUG |
| 2 | java:S2737 | "catch" clauses should do more than rethrow | MINOR | CODE_SMELL |
| 2 | java:S1068 | Unused "private" fields should be removed | MAJOR | CODE_SMELL |
| 2 | java:S2293 | The diamond operator ("<>") should be used | MINOR | CODE_SMELL |
| 2 | java:S1144 | Unused "private" methods should be removed | MAJOR | CODE_SMELL |
| 1 | java:S6201 | Pattern Matching for "instanceof" should be used | MINOR | CODE_SMELL |
| 1 | java:S5164 | "ThreadLocal" variables should be cleaned up | MAJOR | BUG |
| 1 | java:S1118 | Utility classes should not have public constructors | MAJOR | CODE_SMELL |
| 1 | java:S2147 | Catches should be combined | MINOR | CODE_SMELL |
| 1 | java:S3400 | Methods should not return constants | MINOR | CODE_SMELL |
| 1 | java:S2864 | "entrySet()" should be iterated when both key and value are needed | MAJOR | CODE_SMELL |
| 1 | java:S1157 | Case insensitive string comparisons without intermediate casing | MINOR | CODE_SMELL |
| 1 | java:S6548 | Singleton design pattern should be used with care | INFO | CODE_SMELL |
| 1 | java:S899 | Return values should not be ignored when they contain operation status code | MINOR | BUG |
| 1 | java:S2272 | "Iterator.next()" should throw "NoSuchElementException" | MINOR | BUG |
| 1 | java:S2676 | "Math.abs" should not be used on numbers that could be "MIN_VALUE" | MINOR | BUG |
| 1 | java:S6397 | Character classes should not contain only one character | MAJOR | CODE_SMELL |
| 1 | java:S6885 | Use built-in "Math.clamp" methods | MAJOR | CODE_SMELL |
| 1 | java:S1130 | Exceptions in "throws" clauses should not be superfluous | MINOR | CODE_SMELL |
| 1 | java:S1117 | Local variables should not shadow class fields | MAJOR | CODE_SMELL |
| 1 | java:S2093 | Try-with-resources should be used | CRITICAL | CODE_SMELL |

## Top Files by Issue Count

| File | Issues |
|------|--------|
| diesel/QueryParser.java | 162 |
| diesel/SubqueryParser.java | 116 |
| diesel/SelectQuery.java | 64 |
| diesel/Table.java | 12 |
| diesel/BTreeClusteredIndex.java | 9 |
| diesel/BTreeIndex.java | 9 |
| diesel/DatabaseServer.java | 9 |
| src/test/java/diesel/PerformanceTest.java | 9 |
| diesel/ConditionEvaluator.java | 8 |
| diesel/Database.java | 8 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 8 |
| diesel/SqlLexer.java | 6 |
| diesel/ExplainQuery.java | 5 |
| src/test/java/diesel/OomHandlingTest.java | 5 |
| diesel/InsertQuery.java | 4 |
| src/test/java/diesel/StringOpsBenchmarkTest.java | 4 |
| diesel/DeleteQuery.java | 3 |
| diesel/CliRepl.java | 2 |
| diesel/QueryProfiler.java | 2 |
| diesel/SqlParsingUtils.java | 2 |

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, how to fix, and full list of locations (file:line — message).

### java:S5869 — Character classes in regular expressions should not contain the same character twice

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 114

**Problem:** Duplicate characters in regex character classes (e.g. `[0-90-9]` or overlapping ranges).

**Recommendation:** Remove duplicate characters in the regex character class.

**Locations:**

- diesel/QueryParser.java:81 — Remove duplicates in this character class. (×114)

---

### java:S3776 — Cognitive Complexity of methods should not be too high

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 68

**Problem:** Methods exceeding the 15-point Cognitive Complexity threshold.

**Recommendation:** Extract sub-methods, flatten conditionals, reduce nesting.

**Locations:**

- diesel/QueryParser.java:966 — Cognitive Complexity 38 (max 15)
- diesel/QueryParser.java:2199 — Cognitive Complexity 52 (max 15)
- diesel/QueryParser.java:1100 — Cognitive Complexity 25 (max 15)
- diesel/QueryParser.java:1517 — Cognitive Complexity 41 (max 15)
- diesel/QueryParser.java:1628 — Cognitive Complexity 31 (max 15)
- diesel/QueryParser.java:2474 — Cognitive Complexity 23 (max 15)
- diesel/QueryParser.java:2974 — Cognitive Complexity 73 (max 15)
- diesel/QueryParser.java:3083 — Cognitive Complexity 47 (max 15)
- diesel/QueryParser.java:2304 — Cognitive Complexity 43 (max 15)
- diesel/QueryParser.java:1952 — Cognitive Complexity 22 (max 15)
- diesel/QueryParser.java:1156 — Cognitive Complexity 24 (max 15)
- diesel/QueryParser.java:1328 — Cognitive Complexity 100 (max 15)
- diesel/SelectQuery.java:736 — Cognitive Complexity 41 (max 15)
- diesel/SelectQuery.java:841 — Cognitive Complexity 20 (max 15)
- diesel/SelectQuery.java:883 — Cognitive Complexity 40 (max 15)
- diesel/SelectQuery.java:956 — Cognitive Complexity 45 (max 15)
- diesel/SelectQuery.java:1116 — Cognitive Complexity 47 (max 15)
- diesel/SelectQuery.java:1281 — Cognitive Complexity 62 (max 15)
- diesel/SelectQuery.java:1877 — Cognitive Complexity 28 (max 15)
- diesel/SelectQuery.java:2178 — Cognitive Complexity 23 (max 15)
- diesel/SelectQuery.java:2246 — Cognitive Complexity 44 (max 15)
- diesel/SelectQuery.java:2691 — Cognitive Complexity 36 (max 15)
- diesel/SelectQuery.java:1748 — Cognitive Complexity 47 (max 15)
- diesel/SelectQuery.java:2455 — Cognitive Complexity 21 (max 15)
- diesel/SubqueryParser.java:180 — Cognitive Complexity 29 (max 15)
- diesel/SubqueryParser.java:261 — Cognitive Complexity 31 (max 15)
- diesel/SubqueryParser.java:322 — Cognitive Complexity 26 (max 15)
- diesel/SubqueryParser.java:469 — Cognitive Complexity 19 (max 15)
- diesel/SubqueryParser.java:584 — Cognitive Complexity 23 (max 15)
- diesel/SubqueryParser.java:613 — Cognitive Complexity 36 (max 15)
- diesel/SubqueryParser.java:716 — Cognitive Complexity 29 (max 15)
- diesel/SubqueryParser.java:875 — Cognitive Complexity 20 (max 15)
- diesel/SubqueryParser.java:961 — Cognitive Complexity 20 (max 15)
- diesel/SubqueryParser.java:1046 — Cognitive Complexity 26 (max 15)
- diesel/SubqueryParser.java:1283 — Cognitive Complexity 16 (max 15)
- diesel/SubqueryParser.java:1314 — Cognitive Complexity 19 (max 15)
- diesel/SubqueryParser.java:1396 — Cognitive Complexity 20 (max 15)
- diesel/SubqueryParser.java:1511 — Cognitive Complexity 40 (max 15)
- diesel/SubqueryParser.java:1582 — Cognitive Complexity 63 (max 15)
- diesel/Table.java:789 — Cognitive Complexity 21 (max 15)
- diesel/Table.java:859 — Cognitive Complexity 22 (max 15)
- diesel/Table.java:918 — Cognitive Complexity 26 (max 15)
- diesel/ConditionEvaluator.java:57 — Cognitive Complexity 42 (max 15)
- diesel/ConditionEvaluator.java:155 — Cognitive Complexity 19 (max 15)
- diesel/ConditionEvaluator.java:210 — Cognitive Complexity 18 (max 15)
- diesel/CliRepl.java:166 — Cognitive Complexity 23 (max 15)
- diesel/DatabaseServer.java:247 — Cognitive Complexity 19 (max 15)
- diesel/Database.java:548 — Cognitive Complexity 19 (max 15)
- diesel/InsertQuery.java:64 — Cognitive Complexity 65 (max 15)
- diesel/SqlLexer.java:108 — Cognitive Complexity 80 (max 15)
- ... and 18 more

---

### java:S135 — Loops should not contain more than a single "break" or "continue" statement

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 17

**Problem:** Multiple break/continue in one loop reduces readability.

**Recommendation:** Restructure with guard clauses or extract helper methods.

**Locations:**

- diesel/QueryParser.java:1106
- diesel/QueryParser.java:2348
- diesel/QueryParser.java:2985
- diesel/QueryParser.java:2701
- diesel/QueryParser.java:2880
- diesel/QueryParser.java:3284
- diesel/CliRepl.java:106
- diesel/DatabaseServer.java:252
- diesel/SubqueryParser.java:1590
- diesel/SubqueryParser.java:189
- diesel/SubqueryParser.java:882
- diesel/SubqueryParser.java:1198
- diesel/SubqueryParser.java:1399
- diesel/SubqueryParser.java:1520
- diesel/SqlLexer.java:116
- diesel/SqlLexer.java:129
- diesel/SelectQuery.java:1345

---

### java:S1192 — String literals should not be duplicated

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 15

**Problem:** Same literal string duplicated N times across the codebase.

**Recommendation:** Extract into a named `static final` constant.

**Locations:**

- diesel/QueryParser.java:1335 — "|\\(.*?\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" duplicated 5 times
- diesel/QueryParser.java:2308 — "Quoted String" duplicated 3 times
- diesel/QueryParser.java:1340 — "(?i)^(" duplicated 9 times
- diesel/QueryParser.java:2316 — "(?i)(" duplicated 7 times
- diesel/QueryParser.java:1164 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN'
- diesel/QueryParser.java:1197 — "quotedString" duplicated 3 times
- diesel/QueryParser.java:1205 — "openParen" duplicated 3 times
- diesel/QueryParser.java:1209 — "closeParen" duplicated 3 times
- diesel/QueryParser.java:2889 — "(SELECT" duplicated 5 times
- diesel/SubqueryParser.java:1278 — "Unbalanced parentheses in subquery: " duplicated 3 times
- diesel/SubqueryParser.java:972 — "(?i)(" duplicated 6 times
- diesel/SubqueryParser.java:268 — "(?i)^(" duplicated 8 times
- diesel/SubqueryParser.java:286 — "SUBQUERY_" duplicated 3 times
- diesel/SubqueryParser.java:182 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN'
- diesel/Table.java:221 — "Column " duplicated 3 times

---

### java:S1481 — Unused local variables should be removed

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 14

**Problem:** Local variable assigned but never read.

**Recommendation:** Remove or use the variable.

**Locations:**

- diesel/BTreeClusteredIndex.java:343 — unused "ck2"
- diesel/BTreeIndex.java:286 — unused "ck2"
- diesel/Database.java:224 — unused "q"
- diesel/Database.java:227 — unused "q"
- diesel/ExplainQuery.java:76 — unused "iq"
- diesel/ExplainQuery.java:78 — unused "uq"
- diesel/Table.java:402 — unused "ck2"
- diesel/QueryParser.java:2288 — unused "conditions"
- src/test/java/diesel/StringOpsBenchmarkTest.java:129 — unused "ignored"
- src/test/java/diesel/StringOpsBenchmarkTest.java:137 — unused "ignored"
- src/test/java/diesel/AdvancedTest.java:35 — unused "random"
- src/test/java/diesel/PerformanceTest.java:155 — unused "columns"
- src/test/java/diesel/PerformanceTest.java:156 — unused "random"
- src/test/java/diesel/PerformanceTest.java:302 — unused "random"

---

### java:S5843 — Regular expressions should not be too complicated

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Problem:** Regex complexity exceeds the 20-point threshold.

**Recommendation:** Simplify regex, extract named parts, or split into multiple patterns.

**Locations:**

- diesel/QueryParser.java:1680 — complexity 23 (max 20)
- diesel/QueryParser.java:2342 — complexity 35 (max 20)
- diesel/QueryParser.java:2565 — complexity 22 (max 20)
- diesel/SubqueryParser.java:74 — complexity 46 (max 20)
- diesel/SubqueryParser.java:117 — complexity 26 (max 20)
- diesel/SubqueryParser.java:270 — complexity 24 (max 20)
- diesel/SubqueryParser.java:811 — complexity 29 (max 20)
- diesel/SubqueryParser.java:972 — complexity 31 (max 20)
- diesel/SubqueryParser.java:974 — complexity 34 (max 20)
- diesel/SubqueryParser.java:976 — complexity 31 (max 20)
- diesel/SubqueryParser.java:982 — complexity 26 (max 20)
- diesel/SubqueryParser.java:1118 — complexity 48 (max 20)
- diesel/SubqueryParser.java:1646 — complexity 21 (max 20)

---

### java:S2925 — "Thread.sleep" should not be used in tests

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Problem:** Thread.sleep in tests is flaky and slow.

**Recommendation:** Use Awaitility, CountDownLatch, or conditional polling.

**Locations:**

- src/test/java/diesel/AnalyzeTableTest.java:154
- src/test/java/diesel/OomHandlingTest.java:160
- src/test/java/diesel/AllTestsSampleTest.java:1224
- src/test/java/diesel/AllTestsSampleTest.java:1049
- src/test/java/diesel/QuantitativeTest.java:1160
- src/test/java/diesel/QuantitativeTest.java:985
- src/test/java/diesel/GracefulShutdownTest.java:99
- src/test/java/diesel/SocketTimeoutTest.java:58
- src/test/java/diesel/ServerConnectionLimitTest.java:61
- src/test/java/diesel/ServerConnectionLimitTest.java:80
- src/test/java/diesel/ServerConnectionLimitTest.java:84
- src/test/java/diesel/PerformanceTest.java:325
- src/test/java/diesel/PerformanceTest.java:364

---

### java:S1172 — Unused method parameters should be removed

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 12

**Problem:** Method parameter never used in the method body.

**Recommendation:** Remove the parameter or use it.

**Locations:**

- diesel/SubqueryParser.java:1127 — unused "not"
- diesel/SubqueryParser.java:1164 — unused "not"
- diesel/SubqueryParser.java:124 — unused "normalizedQuery"
- diesel/SubqueryParser.java:1429 — unused "tableAliases"
- diesel/QueryParser.java:853 — unused "normalized"
- diesel/QueryParser.java:2649 — unused "not"
- diesel/QueryParser.java:2938 — unused "tableAliases"
- diesel/QueryParser.java:1265 — unused "normalized"
- diesel/ConditionEvaluator.java:155 — unused "columnTypes"
- diesel/SelectQuery.java:404 — unused "columnTypes"
- diesel/SelectQuery.java:2057 — unused "combinedColumnTypes"
- src/test/java/diesel/PerformanceTest.java:112 — unused "random"

---

### java:S3457 — Format strings should be used correctly

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 12

**Problem:** SLF4J/Log4j format string mismatch or string concatenation in logging.

**Recommendation:** Use format specifiers or lambda for lazy evaluation.

**Locations:**

- diesel/Table.java:912 — first argument not used
- diesel/Table.java:896 — first argument not used
- diesel/SubqueryParser.java:1014 — 2nd argument not used
- diesel/SubqueryParser.java:1014 — 4th argument not used
- diesel/SubqueryParser.java:1019 — 3rd argument not used
- diesel/QueryParser.java:2399 — first argument not used
- src/test/java/diesel/SocketTimeoutTest.java:82 — string concatenation instead of format
- src/test/java/diesel/ServerConnectionLimitTest.java:74 — string concatenation instead of format
- src/test/java/diesel/ServerConnectionLimitTest.java:85 — string concatenation instead of format
- src/test/java/diesel/ServerConnectionLimitTest.java:102 — string concatenation instead of format
- src/test/java/diesel/ServerConnectionLimitTest.java:105 — string concatenation instead of format
- src/test/java/diesel/ServerConnectionLimitTest.java:113 — string concatenation instead of format

---

### java:S1854 — Unused assignments should be removed

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 12

**Problem:** Variable is assigned a value that is never read.

**Recommendation:** Remove the useless assignment.

**Locations:**

- diesel/QueryParser.java:1469 — useless assignment to "joins"
- diesel/QueryParser.java:2288 — useless assignment to "conditions"
- diesel/QueryParser.java:2371 — useless assignment to "matchedPatternName"
- diesel/QueryParser.java:2373 — useless assignment to "matched"
- diesel/QueryParser.java:1534 — useless assignment to "onClausePart"
- diesel/SelectQuery.java:743 — useless assignment to "newJoinedRows"
- src/test/java/diesel/StringOpsBenchmarkTest.java:129 — useless assignment to "ignored"
- src/test/java/diesel/StringOpsBenchmarkTest.java:137 — useless assignment to "ignored"
- src/test/java/diesel/AdvancedTest.java:35 — useless assignment to "random"
- src/test/java/diesel/PerformanceTest.java:155 — useless assignment to "columns"
- src/test/java/diesel/PerformanceTest.java:156 — useless assignment to "random"
- src/test/java/diesel/PerformanceTest.java:302 — useless assignment to "random"

---

### java:S6541 — Methods should not perform too many tasks (Brain method)

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 12

**Problem:** Methods with too many responsibilities (LOC, Complexity, Nesting, Variables).

**Recommendation:** Extract sub-methods for each distinct task.

**Locations:**

- diesel/QueryParser.java:2199 — LOC 75, Complexity 33, Nesting 5, Variables 11
- diesel/QueryParser.java:2974 — LOC 104, Complexity 38, Nesting 6, Variables 22
- diesel/QueryParser.java:3083 — LOC 95, Complexity 29, Nesting 5, Variables 33
- diesel/QueryParser.java:2304 — LOC 124, Complexity 19, Nesting 5, Variables 20
- diesel/QueryParser.java:1328 — LOC 133, Complexity 37, Nesting 3, Variables 52
- diesel/SelectQuery.java:1116 — LOC 108, Complexity 23, Nesting 6, Variables 44
- diesel/SelectQuery.java:1281 — LOC 88, Complexity 26, Nesting 5, Variables 28
- diesel/SubqueryParser.java:1511 — LOC 66, Complexity 18, Nesting 6, Variables 18
- diesel/SubqueryParser.java:1582 — LOC 104, Complexity 34, Nesting 4, Variables 37
- diesel/SqlLexer.java:108 — LOC 116, Complexity 34, Nesting 5, Variables 18
- diesel/DeleteQuery.java:56 — LOC 98, Complexity 39, Nesting 5, Variables 35
- diesel/InsertQuery.java:64 — LOC 107, Complexity 32, Nesting 4, Variables 17

---

### java:S127 — "for" loop stop conditions should be invariant

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 11

**Problem:** Loop counter assigned from within the loop body.

**Recommendation:** Use while loop or restructure the logic.

**Locations:**

- diesel/QueryParser.java:1111
- diesel/QueryParser.java:1118
- diesel/QueryParser.java:3044
- diesel/QueryParser.java:3049
- diesel/QueryParser.java:3055
- diesel/SelectQuery.java:2220
- diesel/SubqueryParser.java:546
- diesel/SubqueryParser.java:1559
- diesel/SubqueryParser.java:1565
- diesel/SqlLexer.java:89
- diesel/SqlLexer.java:92

---

### java:S5857 — Character classes should be preferred over reluctant quantifiers

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 11

**Problem:** Reluctant quantifier `.*?` can be replaced with a negated character class `[^X]*+` for performance.

**Recommendation:** Replace `.*?` with `[^)]*+` (possessive negated class).

**Locations:**

- diesel/QueryParser.java:1335
- diesel/QueryParser.java:1336
- diesel/QueryParser.java:1337
- diesel/QueryParser.java:1338
- diesel/QueryParser.java:1339
- diesel/QueryParser.java:1957
- diesel/QueryParser.java:3122
- diesel/SubqueryParser.java:270
- diesel/SubqueryParser.java:811
- diesel/SubqueryParser.java:846
- diesel/SubqueryParser.java:1646

---

### java:S2259 — Null pointers should not be dereferenced

**Severity:** MAJOR | **Type:** BUG | **Found:** 9

**Problem:** Potential NullPointerException on nullable return value.

**Recommendation:** Add null checks or use Optional.

**Locations:**

- diesel/QueryParser.java:843 — "toUpperCasePreservingQuotedIdentifiers()" can return null
- diesel/QueryParser.java:862 — "innerNormalized" is nullable
- diesel/QueryParser.java:1725 — "tableAndJoinsOriginal" is nullable
- diesel/QueryParser.java:1741 — "tableAndJoinsOriginal" is nullable
- diesel/QueryParser.java:769 — "normalized" is nullable
- diesel/QueryParser.java:3334 — "normalized" is nullable
- diesel/QueryParser.java:1277 — "original" is nullable
- diesel/SelectQuery.java:2804 — "buildTable" is nullable
- diesel/SqlParsingUtils.java:55 — "unquoted" is nullable

---

### java:S3358 — Ternary operators should not be nested

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Problem:** Nested ternary reduces readability.

**Recommendation:** Extract into separate variables or use if-else.

**Locations:**

- diesel/QueryParser.java:3157
- diesel/QueryParser.java:354
- diesel/SelectQuery.java:2258
- diesel/SelectQuery.java:2370
- diesel/SelectQuery.java:2375
- diesel/SubqueryParser.java:1669
- diesel/ConditionEvaluator.java:69
- diesel/ConditionEvaluator.java:212
- diesel/DeleteQuery.java:88

---

### java:S6213 — Restricted identifiers should not be used

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Problem:** Variable/method name conflicts with Java restricted identifiers (e.g. `var`, `record`, `yield`).

**Recommendation:** Rename to avoid restricted keywords.

**Locations:**

- diesel/QueryProfiler.java:100 — method name matches restricted identifier
- src/test/java/diesel/AutoJoinIndexTest.java:35 — variable name matches restricted identifier
- src/test/java/diesel/AutoJoinIndexTest.java:93 — variable name matches restricted identifier
- src/test/java/diesel/OomHandlingTest.java:100 — variable name matches restricted identifier
- src/test/java/diesel/OomHandlingTest.java:127 — variable name matches restricted identifier
- src/test/java/diesel/OomHandlingTest.java:131 — variable name matches restricted identifier
- src/test/java/diesel/OomHandlingTest.java:141 — variable name matches restricted identifier
- src/test/java/diesel/MaxResultRowsTest.java:170 — variable name matches restricted identifier
- src/test/java/diesel/MaxResultRowsTest.java:189 — variable name matches restricted identifier

---

### java:S1141 — Try-catch blocks should not be nested

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Nested try-catch reduces readability.

**Recommendation:** Extract inner try-catch into a separate method.

**Locations:**

- diesel/DatabaseServer.java:148
- diesel/DatabaseServer.java:151
- diesel/DatabaseServer.java:156
- diesel/DatabaseServer.java:254
- diesel/DatabaseServer.java:274
- diesel/QueryParser.java:60
- diesel/QueryParser.java:2228
- diesel/DatabaseClient.java:163

---

### java:S6485 — Use static factory method HashMap.newHashMap(int) instead of constructor

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** `new HashMap<>(capacity)` should use `HashMap.newHashMap(capacity)` for clarity.

**Recommendation:** Replace `new HashMap<>(n)` with `HashMap.newHashMap(n)`.

**Locations:**

- diesel/SelectQuery.java:685
- diesel/SelectQuery.java:1246
- diesel/SelectQuery.java:1252
- diesel/SelectQuery.java:1265
- diesel/SelectQuery.java:1324
- diesel/SelectQuery.java:1503
- diesel/SelectQuery.java:2142
- diesel/SelectQuery.java:2157

---

### java:S1948 — Fields in "Serializable" class should be transient or serializable

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Non-serializable fields in a Serializable class.

**Recommendation:** Mark fields as `transient` or make their types Serializable.

**Locations:**

- diesel/BTreeIndex.java:21 — "keys"
- diesel/BTreeIndex.java:22 — "rowIndices"
- diesel/BTreeIndex.java:23 — "children"
- diesel/BTreeClusteredIndex.java:22 — "keys"
- diesel/BTreeClusteredIndex.java:23 — "rowIndices"
- diesel/BTreeClusteredIndex.java:24 — "children"
- diesel/UniqueIndex.java:15 — "indexMap"
- diesel/HashIndex.java:15 — "indexMap"

---

### java:S3740 — Raw types should not be used

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 6

**Problem:** Raw generic types bypass type safety.

**Recommendation:** Add type parameter to generic type.

**Locations:**

- diesel/BTreeClusteredIndex.java:343 (×2)
- diesel/BTreeIndex.java:286 (×2)
- diesel/Table.java:402 (×2)

---

### java:S1452 — Generic wildcard types should not be used in return types

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Wildcard return types make API harder to use.

**Recommendation:** Use concrete type parameter.

**Locations:**

- diesel/QueryParser.java:567
- diesel/QueryParser.java:762
- diesel/QueryCache.java:154
- diesel/ExplainQuery.java:35
- diesel/SubqueryParser.java:55

---

### java:S6395 — Non-capturing groups without quantifier should not be used

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Non-capturing group `(?:...)` without a quantifier is unnecessary.

**Recommendation:** Remove unnecessary grouping or add quantifier.

**Locations:**

- diesel/QueryParser.java:1697
- diesel/QueryParser.java:1727
- diesel/SubqueryParser.java:700
- diesel/SubqueryParser.java:705

---

### java:S6204 — Use Stream.toList() instead of collect(Collectors.toList())

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Unnecessary `Collectors.toList()` when `Stream.toList()` suffices.

**Recommendation:** Replace `.collect(Collectors.toList())` with `.toList()`.

**Locations:**

- diesel/QueryParser.java:2053
- diesel/SelectQuery.java:892
- diesel/SelectQuery.java:1779
- diesel/SelectQuery.java:1816

---

### java:S6353 — Use concise character class syntax

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** `[0-9]` should be `\d`, `[a-zA-Z0-9_]` should be `\w`.

**Recommendation:** Use `\d` instead of `[0-9]`.

**Locations:**

- diesel/QueryParser.java:2330 (×2)
- diesel/SubqueryParser.java:982 (×2)

---

### java:S2629 — Preconditions and logging arguments should not require evaluation

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Expensive string concatenation in logging preconditions.

**Recommendation:** Use SLF4J format `{}` or lazy lambda.

**Locations:**

- diesel/SelectQuery.java:348
- diesel/SelectQuery.java:2050
- diesel/SelectQuery.java:2243
- diesel/QueryParser.java:64

---

### java:S2139 — Exceptions should be either logged or rethrown but not both

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Exception is logged and then rethrown (double handling).

**Recommendation:** Either log or rethrow, not both.

**Locations:**

- diesel/SubqueryParser.java:86
- diesel/SubqueryParser.java:941
- diesel/InsertQuery.java:165
- diesel/QueryParser.java:783

---

### java:S1168 — Return empty arrays/collections instead of null

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Returning null instead of empty collection forces null checks.

**Recommendation:** Return `Collections.emptyList()` or `Map.of()`.

**Locations:**

- diesel/SelectQuery.java:1501
- diesel/SelectQuery.java:2059
- diesel/SelectQuery.java:2067
- diesel/SelectQuery.java:2118

---

### java:S1066 — Mergeable "if" statements should be combined

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Consecutive if-statements with no logic between them.

**Recommendation:** Combine into single `if (a && b)`.

**Locations:**

- diesel/ConditionEvaluator.java:39
- diesel/Table.java:911
- diesel/SelectQuery.java:929
- diesel/BTreeClusteredIndex.java:332

---

### java:S3626 — Redundant jump statements

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Return/break/continue at end of block is unnecessary.

**Recommendation:** Remove redundant jump.

**Locations:**

- diesel/SubqueryParser.java:1408
- diesel/SubqueryParser.java:1411
- diesel/QueryParser.java:2893
- diesel/QueryParser.java:2899

---

### java:S2589 — Boolean expressions should not be gratuitous

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Expression always evaluates to true — dead code.

**Recommendation:** Remove or simplify the expression.

**Locations:**

- diesel/QueryParser.java:1781
- diesel/QueryParser.java:1785
- diesel/QueryParser.java:1794

---

### java:S6880 — Use switch instead of if-else chain

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** if-else chain comparing a variable against multiple string values.

**Recommendation:** Use switch expression.

**Locations:**

- diesel/ConditionEvaluator.java:141
- diesel/ExplainQuery.java:76
- diesel/ExplainQuery.java:86

---

### java:S125 — Sections of code should not be commented out

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Commented-out code blocks.

**Recommendation:** Remove commented-out code.

**Locations:**

- diesel/SelectQuery.java:630
- diesel/QueryParser.java:1228
- diesel/QueryParser.java:1802

---

### java:S1905 — Redundant casts should not be used

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Unnecessary cast to Comparable.

**Recommendation:** Remove redundant cast.

**Locations:**

- diesel/BTreeClusteredIndex.java:345
- diesel/BTreeIndex.java:288
- diesel/Table.java:404

---

### java:S3008 — Static non-final field naming convention

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Static non-final fields should use camelCase.

**Recommendation:** Rename to match `^[a-z][a-zA-Z0-9]*$`.

**Locations:**

- diesel/SelectQuery.java:161 — "MAX_IN_MEMORY_ROWS"
- diesel/SelectQuery.java:171 — "MAX_HASH_TABLE_SIZE_BYTES"
- diesel/SelectQuery.java:182 — "MAX_RESULT_ROWS"

---

### java:S107 — Methods should not have too many parameters

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Constructor exceeds 7 parameter limit.

**Recommendation:** Use Builder pattern or parameter object.

**Locations:**

- diesel/SelectQuery.java:382 — 14 parameters
- diesel/SelectQuery.java:398 — 15 parameters

---

### java:S5850 — Group regex alternatives with anchors

**Severity:** MAJOR | **Type:** BUG | **Found:** 2

**Problem:** Regex alternation `a|b|c` with anchors has unintended precedence.

**Recommendation:** Group alternatives: `(?:a|b|c)`.

**Locations:**

- diesel/QueryParser.java:1697
- diesel/SubqueryParser.java:700

---

### java:S1068 — Unused "private" fields should be removed

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Private field declared but never read.

**Recommendation:** Remove the field.

**Locations:**

- src/test/java/diesel/LimitOffsetTest.java:22 — "DATE_FORMATTER"
- diesel/DatabaseServer.java:203 — "socketTimeout"

---

### java:S2293 — Diamond operator ("<>") should be used

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Redundant type specification in constructor.

**Recommendation:** Replace `new ArrayList<String>()` with `new ArrayList<>()`.

**Locations:**

- diesel/Table.java:1120
- diesel/Database.java:122

---

### java:S1144 — Unused "private" methods should be removed

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Private method never called.

**Recommendation:** Remove the method.

**Locations:**

- diesel/QueryParser.java:2862 — "resolveRightColumn"
- diesel/SelectQuery.java:382 — constructor

---

### java:S6208 — Comma-separated labels in Switch

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Multiple case labels can be merged.

**Recommendation:** Use comma-separated labels: `case A, B, C ->`.

**Locations:**

- diesel/QueryParser.java:3167
- diesel/QueryParser.java:1054

---

### java:S6539 — Monster Class (too many dependencies)

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Class depends on >20 other classes.

**Recommendation:** Split into smaller, more focused classes.

**Locations:**

- diesel/Database.java:38 — 24 dependencies (max 20)
- diesel/QueryParser.java:38 — 23 dependencies (max 20)

---

### java:S3824 — Replace Map.get()/containsKey() with computeIfAbsent()

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Manual get-check-put pattern instead of atomic map operation.

**Recommendation:** Use `map.computeIfAbsent(key, k -> ...)`.

**Locations:**

- diesel/SelectQuery.java:2418
- diesel/SelectQuery.java:1743

---

### java:S2737 — "catch" clauses should do more than rethrow

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Catch block only rethrows without logging or adding context.

**Recommendation:** Add logging, context, or use try-with-resources.

**Locations:**

- diesel/InsertQuery.java:138
- diesel/QueryParser.java:2271

---

### java:S2147 — Combine identical catch blocks

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Two catch blocks have the same body.

**Recommendation:** Merge into single catch.

**Locations:**

- diesel/Database.java:203

---

### java:S3400 — Methods should not return constants

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Method just returns a constant value.

**Recommendation:** Make it a constant field.

**Locations:**

- diesel/SelectQuery.java:1021

---

### java:S1157 — Case insensitive string comparisons

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Using toUpperCase() + equals() instead of equalsIgnoreCase().

**Recommendation:** Use `str1.equalsIgnoreCase(str2)`.

**Locations:**

- diesel/SubqueryParser.java:544

---

### java:S2864 — Iterate entrySet() when both key and value needed

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Iterating keySet() then calling get() is wasteful.

**Recommendation:** Use `entrySet()` iteration.

**Locations:**

- diesel/SelectQuery.java:901

---

### java:S6397 — Character class should not contain only one character

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Single-character class `[X]` is equivalent to just `X`.

**Recommendation:** Remove unnecessary character class brackets.

**Locations:**

- diesel/SubqueryParser.java:982

---

### java:S6885 — Use built-in Math.clamp methods

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Manual min/max clamping instead of `Math.clamp()` (Java 21+).

**Recommendation:** Use `Math.clamp(value, min, max)`.

**Locations:**

- diesel/SelectQuery.java:1427

---

### java:S5164 — ThreadLocal variables should be cleaned up

**Severity:** MAJOR | **Type:** BUG | **Found:** 1

**Problem:** ThreadLocal not removed after use → potential memory leak.

**Recommendation:** Call `.remove()` in finally block.

**Locations:**

- diesel/SelectQuery.java:275

---

### java:S1118 — Utility classes should not have public constructors

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Utility class with only static methods has implicit public constructor.

**Recommendation:** Add private constructor.

**Locations:**

- diesel/SqlParsingUtils.java:12

---

### java:S6201 — Use pattern matching for instanceof

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Old-style instanceof + cast.

**Recommendation:** Use `instanceof Type name` (Java 16+).

**Locations:**

- diesel/SelectQuery.java:2378

---

### java:S1117 — Local variables should not shadow class fields

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Local variable has same name as class field.

**Recommendation:** Rename local variable.

**Locations:**

- diesel/Database.java:336 — "autoCommit" shadows field at line 44

---

### java:S1130 — Superfluous throws declarations

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Declared exception cannot be thrown from constructor body.

**Recommendation:** Remove from throws clause.

**Locations:**

- diesel/SelectQuery.java:1589

---

### java:S899 — Return values should not be ignored

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** hasNext() return value ignored.

**Recommendation:** Use the return value.

**Locations:**

- diesel/SelectQuery.java:1655

---

### java:S2272 — Iterator.next() should throw NoSuchElementException

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** next() should throw when iteration exhausted.

**Recommendation:** Add NoSuchElementException.

**Locations:**

- diesel/SelectQuery.java:1647

---

### java:S2676 — Math.abs should not be used on MIN_VALUE

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** `Math.abs(Integer.MIN_VALUE)` returns negative.

**Recommendation:** Use `Math.absExact()` or bounds check.

**Locations:**

- diesel/SubqueryParser.java:491

---

### java:S6548 — Singleton design pattern should be used with care

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Singleton detected — verify it's truly needed.

**Recommendation:** Ensure thread-safety and consider alternatives.

**Locations:**

- diesel/QueryProfiler.java:45

---

### java:S2093 — Try-with-resources should be used

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Resource opened in try but not auto-closed.

**Recommendation:** Use try-with-resources.

**Locations:**

- diesel/DatabaseServer.java:248

---

## Comparison with Previous Scan (sonar2.md, 2026-08-18)

| Metric | sonar2.md | sonar3.md | Delta |
|--------|-----------|-----------|-------|
| Total Issues | 1149 | 467 | **-682 (-59.3%)** |
| CRITICAL | 161 | 97 | -64 |
| MAJOR | 601 | 285 | -316 |
| MINOR | 347 | 68 | -279 |
| INFO | 40 | 17 | -23 |
| Lines of Code | 10876 | 10894 | +18 |
| Files | 50 | 51 | +1 |

### Key Fixes Applied (prompts 41-50)

| Rule | sonar2.md | sonar3.md | Fixed | Notes |
|------|-----------|-----------|-------|-------|
| S6201 (instanceof pattern) | 84 | 1 | **83** | Prompt 43 |
| S1192 (duplicate strings) | 41 | 15 | **26** | Prompt 45 |
| S1128 (unused imports) | 36 | 0 | **36** | Prompt 46 |
| S135 (break/continue) | 30 | 17 | **13** | Prompt 47 |
| S1172 (unused params) | 28 | 12 | **16** | Prompt 48 |
| S108 (empty blocks) | 28 | 0 | **28** | Prompt 49 |
| S3776 (cognitive complexity) | 92 | 68 | **24** | Prompt 42 |
| S6353 (concise regex) | 119 | 4 | **115** | Prompt 41 |
| S5857 (reluctant quantifiers) | 5 | 11 | -6 | Sonar re-count (some moved here from S6353) |
| S1874 (deprecated setScale) | 28 | 0 | **28** | Prompt 50 (audit) |
| S1948 (serializable fields) | 8 | 8 | 0 | Already fixed pre-prompt24 |

### Remaining Top Priority

1. **S5869** (114) — Duplicate regex character classes in QueryParser.java:81
2. **S3776** (68) — Cognitive complexity — largest source of issues
3. **S5843** (13) — Complex regex patterns
4. **S2259** (9) — Null pointer dereference (BUG)
5. **S1948** (8) — Serializable fields (CRITICAL)
