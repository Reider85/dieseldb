# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-08-18 23:04:38
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** Maven sonar:sonar / SonarScanner CLI
**Java Version:** 21.0.11

## Summary Metrics

| Metric | Value |
|--------|-------|
| Lines of Code (ncloc) | 10876 |
| Files | 50 |
| Functions | 604 |
| Classes | 82 |
| Duplicated Lines Density | 1.8% |
| Comment Lines Density | 13.3% |
| Test Coverage | 0% |

## Issue Summary

| Severity | Count |
|----------|-------|| CRITICAL | 161 |
| MAJOR | 601 |
| MINOR | 347 |
| INFO | 40 |
## Issue Types

| Type | Count |
|------|-------|
| BUG |                                                                                            .Count |
| CODE_SMELL |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 .Count |
| VULNERABILITY | .Count |
| SECURITY_HOTSPOT | .Count |

## Top Rules by Issue Count

| Count | Rule | Name | Severity | Type |
|-------|------|------|----------|------|| 228 | java:S5869 | Remove duplicates in this character class. | MAJOR | CODE_SMELL |
| 119 | java:S6353 | Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'. | MINOR | CODE_SMELL |
| 92 | java:S3776 | Refactor this method to reduce its Cognitive Complexity from 25 to the 15 all... | CRITICAL | CODE_SMELL |
| 84 | java:S6201 | Replace this instanceof check and cast with 'instanceof SelectQuery selectquery' | MINOR | CODE_SMELL |
| 57 | java:S5998 | Refactor this repetition that can lead to a stack overflow for large inputs. | MAJOR | BUG |
| 41 | java:S1192 | Define a constant instead of duplicating this literal " does not exist" 4 times. | CRITICAL | CODE_SMELL |
| 36 | java:S1128 | Remove this unused import 'diesel.ThreeValuedLogic.TRUE'. | MINOR | CODE_SMELL |
| 30 | java:S135 | Reduce the total number of break and continue statements in this loop to use ... | MINOR | CODE_SMELL |
| 28 | java:S1172 | Remove this unused method parameter "normalized". | MAJOR | CODE_SMELL |
| 28 | java:S108 | Either remove or fill this block of code. | MAJOR | CODE_SMELL |
| 28 | java:S1874 | Remove this use of "setScale"; it is deprecated. | MINOR | CODE_SMELL |
| 23 | java:S3457 | first argument is not used. | MAJOR | CODE_SMELL |
| 23 | java:S1854 | Remove this useless assignment to local variable "joins". | MAJOR | CODE_SMELL |
| 22 | java:S107 | Constructor has 14 parameters, which is greater than 7 authorized. | MAJOR | CODE_SMELL |
| 21 | java:S5843 | Simplify this regular expression to reduce its complexity from 23 to the 20 a... | MAJOR | CODE_SMELL |
| 20 | java:S2925 | Remove this use of "Thread.sleep()". | MAJOR | CODE_SMELL |
| 19 | java:S1481 | Remove this unused "ignored" local variable. | MINOR | CODE_SMELL |
| 19 | java:S6541 | A "Brain Method" was detected. Refactor it to reduce at least one of the foll... | INFO | CODE_SMELL |
| 18 | java:S2259 | A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentif... | MAJOR | BUG |
| 16 | java:S5786 | Remove this 'public' modifier. | INFO | CODE_SMELL |
| 13 | java:S3358 | Extract this nested ternary operation into an independent statement. | MAJOR | CODE_SMELL |
| 11 | java:S127 | Refactor the code in order to not assign to this loop counter from within the... | MAJOR | CODE_SMELL |
| 9 | java:S6213 | Rename this method to not match a restricted identifier. | MAJOR | CODE_SMELL |
| 8 | java:S112 | Define and throw a dedicated exception instead of using a generic one. | MAJOR | CODE_SMELL |
| 8 | java:S2139 | Either log this exception and handle it, or rethrow it with some contextual i... | MAJOR | CODE_SMELL |
| 8 | java:S1948 | Make "rowIndices" private or transient. | CRITICAL | CODE_SMELL |
| 8 | java:S106 | Replace this use of System.out by a logger. | MAJOR | CODE_SMELL |
| 8 | java:S2447 | Null is returned but a "Boolean" is expected. | CRITICAL | CODE_SMELL |
| 8 | java:S6485 | Replace this call to the constructor with the better suited static method Has... | MAJOR | CODE_SMELL |
| 8 | java:S1141 | Extract this nested try block into a separate method. | MAJOR | CODE_SMELL |
## Top Files by Issue Count

| File | Issues |
|------|--------|| diesel/QueryParser.java | 396 |
| diesel/SubqueryParser.java | 277 |
| diesel/SelectQuery.java | 113 |
| diesel/Database.java | 45 |
| diesel/DeleteQuery.java | 27 |
| diesel/UpdateQuery.java | 23 |
| src/test/java/diesel/PerformanceTest.java | 23 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 21 |
| diesel/Table.java | 20 |
| diesel/DatabaseClient.java | 16 |
| diesel/ConditionEvaluator.java | 14 |
| src/test/java/diesel/AllTestsSampleTest.java | 10 |
| diesel/SqlLexer.java | 10 |
| diesel/DatabaseServer.java | 9 |
| src/test/java/diesel/QuantitativeTest.java | 9 |
| src/test/java/diesel/OomHandlingTest.java | 9 |
| diesel/ExplainQuery.java | 8 |
| src/test/java/diesel/AdvancedTest.java | 8 |
| src/test/java/diesel/AliasesTest.java | 7 |
| src/test/java/diesel/OrderByTest.java | 6 |
| src/test/java/diesel/GroupByTest.java | 6 |
| src/test/java/diesel/MaxResultRowsTest.java | 6 |
| src/test/java/diesel/JoinTest.java | 5 |
| src/test/java/diesel/SocketTimeoutTest.java | 5 |
| src/test/java/diesel/SubqueriesTest.java | 5 |

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, how to fix, and full list of locations (file:line — message).

### java:S5869 — Remove duplicates in this character class.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 228

**Problem:** Remove duplicates in this character class.

**Recommendation:** Remove duplicate characters in the regex character class (e.g. [0-90-9] → [0-9]).

**Locations:**

- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- $file:82 — Remove duplicates in this character class.
- ... and 178 more

### java:S6353 — Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 119

**Problem:** Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.

**Recommendation:** Use concise character class syntax like \w instead of [a-zA-Z0-9_], \d instead of [0-9] etc.

**Locations:**

- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- $file:82 — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- ... and 69 more

### java:S3776 — Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 92

**Problem:** Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.

**Recommendation:** Fix according to rule java:S3776.

**Locations:**

- $file:156 — Refactor this method to reduce its Cognitive Complexity from 26 to the 15 allowed.
- $file:160 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:198 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- $file:166 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- $file:59 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- $file:157 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:212 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 63 to the 15 allowed.
- $file:527 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:549 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:247 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 53 to the 15 allowed.
- $file:62 — Refactor this method to reduce its Cognitive Complexity from 64 to the 15 allowed.
- $file:64 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 73 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 49 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- $file:240 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- $file:970 — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.
- $file:1104 — Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.
- $file:1160 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- $file:1332 — Refactor this method to reduce its Cognitive Complexity from 100 to the 15 allowed.
- $file:1521 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- $file:1632 — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- $file:1752 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- $file:1879 — Refactor this method to reduce its Cognitive Complexity from 58 to the 15 allowed.
- $file:1956 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- $file:2015 — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- $file:2203 — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- $file:2308 — Refactor this method to reduce its Cognitive Complexity from 43 to the 15 allowed.
- $file:2478 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- $file:2799 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- $file:2875 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- $file:2966 — Refactor this method to reduce its Cognitive Complexity from 73 to the 15 allowed.
- $file:3075 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- $file:3181 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- $file:3231 — Refactor this method to reduce its Cognitive Complexity from 44 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 37 to the 15 allowed.
- $file:N/A — Refactor this method to reduce its Cognitive Complexity from 157 to the 15 allowed.
- $file:736 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- $file:841 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- $file:883 — Refactor this method to reduce its Cognitive Complexity from 40 to the 15 allowed.
- $file:956 — Refactor this method to reduce its Cognitive Complexity from 45 to the 15 allowed.
- ... and 42 more

### java:S6201 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 84

**Problem:** Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'

**Recommendation:** Use pattern matching for instanceof (Java 16+): if (x instanceof Foo f) { ... f.method() ... } — removes separate cast.

**Locations:**

- $file:143 — Replace this instanceof check and cast with 'instanceof Float float'
- $file:145 — Replace this instanceof check and cast with 'instanceof Double double'
- $file:147 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:218 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:219 — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:222 — Replace this instanceof check and cast with 'instanceof Float float'
- $file:225 — Replace this instanceof check and cast with 'instanceof Double double'
- $file:N/A — Replace this instanceof check and cast with 'instanceof CreateUniqueClusteredIndexQuery indexQuery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof CreateUniqueIndexQuery indexQuery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery autoCommitQuery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof CreateTableQuery createQuery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof CreateHashIndexQuery indexQuery'
- $file:N/A — Replace this instanceof check and cast with 'instanceof CreateIndexQuery indexQuery'
- $file:184 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:215 — Replace this instanceof check and cast with 'instanceof SetIsolationLevelQuery setisolationlevelq...
- $file:218 — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery setautocommitquery'
- $file:221 — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- $file:230 — Replace this instanceof check and cast with 'instanceof CreateTableQuery createtablequery'
- $file:233 — Replace this instanceof check and cast with 'instanceof CreateIndexQueryBase createindexquerybase'
- $file:236 — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- $file:239 — Replace this instanceof check and cast with 'instanceof AnalyzeTableQuery analyzetablequery'
- $file:255 — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- $file:260 — Replace this instanceof check and cast with 'instanceof SelectQuery select'
- $file:321 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:323 — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- $file:325 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:425 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:425 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:458 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:458 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- $file:89 — Replace this instanceof check and cast with 'instanceof String string'
- $file:89 — Replace this instanceof check and cast with 'instanceof String string'
- $file:91 — Replace this instanceof check and cast with 'instanceof String string'
- $file:91 — Replace this instanceof check and cast with 'instanceof String string'
- $file:95 — Replace this instanceof check and cast with 'instanceof String string'
- $file:95 — Replace this instanceof check and cast with 'instanceof String string'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- $file:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- $file:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- $file:78 — Replace this instanceof check and cast with 'instanceof BTreeIndex btreeindex'
- $file:49 — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- ... and 34 more

### java:S5998 — Refactor this repetition that can lead to a stack overflow for large inputs.

**Severity:** MAJOR | **Type:** BUG | **Found:** 57

**Problem:** Refactor this repetition that can lead to a stack overflow for large inputs.

**Recommendation:** Fix according to rule java:S5998.

**Locations:**

- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- $file:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- ... and 7 more

### java:S1192 — Define a constant instead of duplicating this literal " does not exist" 4 times.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 41

**Problem:** Define a constant instead of duplicating this literal " does not exist" 4 times.

**Recommendation:** Fix according to rule java:S1192.

**Locations:**

- $file:N/A — Define a constant instead of duplicating this literal "Table part after split: {0}" 5 times.
- $file:N/A — Define a constant instead of duplicating this literal " does not exist" 3 times.
- $file:120 — Define a constant instead of duplicating this literal "Table " 5 times.
- $file:431 — Define a constant instead of duplicating this literal " does not exist" 4 times.
- $file:667 — Define a constant instead of duplicating this literal ".table" 3 times.
- $file:126 — Define a constant instead of duplicating this literal "none (full scan)" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "UPDATE" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "Quoted String" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "ID=U.ID" 4 times.
- $file:N/A — Define a constant instead of duplicating this literal "SELECT " 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "COUNT" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "ID = U.ID" 4 times.
- $file:N/A — Define a constant instead of duplicating this literal "NOT LIKE" 7 times.
- $file:N/A — Define a constant instead of duplicating this literal "LIMIT" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "SELECT" 4 times.
- $file:1168 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- $file:1201 — Define a constant instead of duplicating this literal "quotedString" 3 times.
- $file:1209 — Define a constant instead of duplicating this literal "openParen" 3 times.
- $file:1213 — Define a constant instead of duplicating this literal "closeParen" 3 times.
- $file:1339 — Define a constant instead of duplicating this literal "|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 ti...
- $file:1344 — Define a constant instead of duplicating this literal "(?i)^(" 9 times.
- $file:2061 — Define a constant instead of duplicating this literal "Table not found: " 3 times.
- $file:2085 — Define a constant instead of duplicating this literal "Unknown column: " 3 times.
- $file:2223 — Define a constant instead of duplicating this literal "' does not match column type: " 4 times.
- $file:2239 — Define a constant instead of duplicating this literal "Numeric value '" 5 times.
- $file:2312 — Define a constant instead of duplicating this literal "Quoted String" 3 times.
- $file:2320 — Define a constant instead of duplicating this literal "(?i)(" 7 times.
- $file:2890 — Define a constant instead of duplicating this literal "(SELECT" 5 times.
- $file:620 — Define a constant instead of duplicating this literal "Table " 4 times.
- $file:620 — Define a constant instead of duplicating this literal " is not attached to a database" 4 times.
- $file:977 — Define a constant instead of duplicating this literal "result" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- $file:N/A — Define a constant instead of duplicating this literal "<end>" 3 times.
- $file:N/A — Define a constant instead of duplicating this literal "SELECT" 10 times.
- $file:183 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- $file:269 — Define a constant instead of duplicating this literal "(?i)^(" 8 times.
- $file:287 — Define a constant instead of duplicating this literal "SUBQUERY_" 3 times.
- $file:976 — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- $file:1262 — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 ti...
- $file:188 — Define a constant instead of duplicating this literal " does not exist" 3 times.
- $file:222 — Define a constant instead of duplicating this literal "Column " 3 times.

### java:S1128 — Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 36

**Problem:** Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:2 — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:3 — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:3 — Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.
- $file:4 — Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.
- $file:5 — Remove this unused import 'diesel.ThreeValuedLogic.UNKNOWN'.
- $file:7 — Remove this unused import 'java.math.BigDecimal'.
- $file:13 — Remove this unused import 'java.util.Objects'.
- $file:2 — Remove this unused import 'java.util'.
- $file:16 — Remove this unused import 'java.util.Objects'.
- $file:11 — Remove this unused import 'java.util.Objects'.
- $file:21 — Remove this unused import 'java.util.Objects'.
- $file:3 — Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.
- $file:4 — Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.
- $file:5 — Remove this unused import 'diesel.ThreeValuedLogic.UNKNOWN'.
- $file:7 — Remove this unused import 'java.math.BigDecimal'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertTrue'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'java.util.stream.IntStream'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'java.util.stream.Collectors'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- $file:N/A — Remove this unused import 'java.util.concurrent.TimeUnit'.
- $file:N/A — Remove this unused import 'java.util.concurrent.CountDownLatch'.
- $file:N/A — Remove this unnecessary import: same package classes are always implicitly imported.

### java:S135 — Reduce the total number of break and continue statements in this loop to use at most one.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 30

**Problem:** Reduce the total number of break and continue statements in this loop to use at most one.

**Recommendation:** Fix according to rule java:S135.

**Locations:**

- $file:106 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:31 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:252 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:802 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1110 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2352 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2481 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2712 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2880 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2977 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:3276 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1344 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2067 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2197 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:2831 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:116 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:129 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:190 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:565 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:886 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1056 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1191 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1243 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1401 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1522 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:1592 — Reduce the total number of break and continue statements in this loop to use at most one.
- $file:N/A — Reduce the total number of break and continue statements in this loop to use at most one.

### java:S1874 — Remove this use of "setScale"; it is deprecated.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 28

**Problem:** Remove this use of "setScale"; it is deprecated.

**Recommendation:** Fix according to rule java:S1874.

**Locations:**

- $file:N/A — Remove this use of "divide"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.
- $file:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- $file:N/A — Remove this use of "setScale"; it is deprecated.

### java:S108 — Either remove or fill this block of code.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 28

**Problem:** Either remove or fill this block of code.

**Recommendation:** Fix according to rule java:S108.

**Locations:**

- $file:1044 — Either remove or fill this block of code.
- $file:1668 — Either remove or fill this block of code.
- $file:1674 — Either remove or fill this block of code.
- $file:1679 — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:1098 — Either remove or fill this block of code.
- $file:68 — Either remove or fill this block of code.
- $file:44 — Either remove or fill this block of code.
- $file:48 — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:46 — Either remove or fill this block of code.
- $file:50 — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:41 — Either remove or fill this block of code.
- $file:48 — Either remove or fill this block of code.
- $file:52 — Either remove or fill this block of code.
- $file:38 — Either remove or fill this block of code.
- $file:42 — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:1034 — Either remove or fill this block of code.
- $file:33 — Either remove or fill this block of code.
- $file:47 — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.
- $file:N/A — Either remove or fill this block of code.

### java:S1172 — Remove this unused method parameter "normalized".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 28

**Problem:** Remove this unused method parameter "normalized".

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- $file:157 — Remove this unused method parameter "columnTypes".
- $file:N/A — Remove this unused method parameter "columnTypes".
- $file:N/A — Remove these unused method parameters "originalQuery", "not".
- $file:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- $file:N/A — Remove this unused method parameter "conditionStr".
- $file:857 — Remove this unused method parameter "normalized".
- $file:970 — Remove this unused method parameter "normalized".
- $file:1269 — Remove this unused method parameter "normalized".
- $file:2203 — Remove this unused method parameter "conditionColumn".
- $file:2660 — Remove this unused method parameter "not".
- $file:2800 — Remove this unused method parameter "conditionStr".
- $file:2930 — Remove this unused method parameter "tableAliases".
- $file:404 — Remove this unused method parameter "columnTypes".
- $file:1012 — Remove these unused method parameters "mainRows", "tables".
- $file:1936 — Remove this unused method parameter "combinedColumnTypes".
- $file:2053 — Remove this unused method parameter "combinedColumnTypes".
- $file:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases", "not".
- $file:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- $file:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- $file:N/A — Remove these unused method parameters "originalQuery", "columnAliases".
- $file:125 — Remove this unused method parameter "normalizedQuery".
- $file:810 — Remove this unused method parameter "columnAliases".
- $file:847 — Remove this unused method parameter "columnAliases".
- $file:1123 — Remove this unused method parameter "not".
- $file:1431 — Remove this unused method parameter "tableAliases".
- $file:N/A — Remove this unused method parameter "columnTypes".
- $file:N/A — Remove this unused method parameter "random".
- $file:112 — Remove this unused method parameter "random".

### java:S1854 — Remove this useless assignment to local variable "joins".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 23

**Problem:** Remove this useless assignment to local variable "joins".

**Recommendation:** Fix according to rule java:S1854.

**Locations:**

- $file:N/A — Remove this useless assignment to local variable "column".
- $file:N/A — Remove this useless assignment to local variable "indexPart".
- $file:N/A — Remove this useless assignment to local variable "currentToken".
- $file:N/A — Remove this useless assignment to local variable "indexPart".
- $file:N/A — Remove this useless assignment to local variable "indexPart".
- $file:N/A — Remove this useless assignment to local variable "indexPart".
- $file:1473 — Remove this useless assignment to local variable "joins".
- $file:1538 — Remove this useless assignment to local variable "onClausePart".
- $file:2292 — Remove this useless assignment to local variable "conditions".
- $file:2375 — Remove this useless assignment to local variable "matchedPatternName".
- $file:2377 — Remove this useless assignment to local variable "matched".
- $file:743 — Remove this useless assignment to local variable "newJoinedRows".
- $file:N/A — Remove this useless assignment to local variable "startPos".
- $file:N/A — Remove this useless assignment to local variable "random".
- $file:35 — Remove this useless assignment to local variable "random".
- $file:N/A — Remove this useless assignment to local variable "random".
- $file:N/A — Remove this useless assignment to local variable "random".
- $file:N/A — Remove this useless assignment to local variable "columns".
- $file:155 — Remove this useless assignment to local variable "columns".
- $file:156 — Remove this useless assignment to local variable "random".
- $file:302 — Remove this useless assignment to local variable "random".
- $file:129 — Remove this useless assignment to local variable "ignored".
- $file:137 — Remove this useless assignment to local variable "ignored".

### java:S3457 — first argument is not used.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 23

**Problem:** first argument is not used.

**Recommendation:** Fix according to rule java:S3457.

**Locations:**

- $file:N/A — first argument is not used.
- $file:N/A — 6th argument is not used.
- $file:N/A — String contains no format specifiers.
- $file:N/A — 5th argument is not used.
- $file:2403 — first argument is not used.
- $file:1018 — 4th argument is not used.
- $file:1018 — 2nd argument is not used.
- $file:1023 — 3rd argument is not used.
- $file:N/A — first argument is not used.
- $file:895 — first argument is not used.
- $file:911 — first argument is not used.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:74 — Format specifiers or lambda should be used instead of string concatenation.
- $file:85 — Format specifiers or lambda should be used instead of string concatenation.
- $file:102 — Format specifiers or lambda should be used instead of string concatenation.
- $file:105 — Format specifiers or lambda should be used instead of string concatenation.
- $file:113 — Format specifiers or lambda should be used instead of string concatenation.
- $file:N/A — Format specifiers or lambda should be used instead of string concatenation.
- $file:82 — Format specifiers or lambda should be used instead of string concatenation.

### java:S107 — Constructor has 14 parameters, which is greater than 7 authorized.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 22

**Problem:** Constructor has 14 parameters, which is greater than 7 authorized.

**Recommendation:** Fix according to rule java:S107.

**Locations:**

- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 8 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 11 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 8 parameters, which is greater than 7 authorized.
- $file:N/A — Constructor has 16 parameters, which is greater than 7 authorized.
- $file:N/A — Constructor has 15 parameters, which is greater than 7 authorized.
- $file:382 — Constructor has 14 parameters, which is greater than 7 authorized.
- $file:398 — Constructor has 15 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 10 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 9 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 8 parameters, which is greater than 7 authorized.
- $file:N/A — Method has 8 parameters, which is greater than 7 authorized.

### java:S5843 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 21

**Problem:** Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.

**Recommendation:** Fix according to rule java:S5843.

**Locations:**

- $file:N/A — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- $file:1684 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.
- $file:2346 — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- $file:2576 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- $file:N/A — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- $file:75 — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- $file:118 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- $file:271 — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- $file:815 — Simplify this regular expression to reduce its complexity from 29 to the 20 allowed.
- $file:976 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- $file:978 — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- $file:980 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- $file:986 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- $file:1125 — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- $file:1648 — Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.

### java:S2925 — Remove this use of "Thread.sleep()".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 20

**Problem:** Remove this use of "Thread.sleep()".

**Recommendation:** Fix according to rule java:S2925.

**Locations:**

- $file:1049 — Remove this use of "Thread.sleep()".
- $file:1223 — Remove this use of "Thread.sleep()".
- $file:154 — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:99 — Remove this use of "Thread.sleep()".
- $file:156 — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:325 — Remove this use of "Thread.sleep()".
- $file:364 — Remove this use of "Thread.sleep()".
- $file:985 — Remove this use of "Thread.sleep()".
- $file:1159 — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:61 — Remove this use of "Thread.sleep()".
- $file:80 — Remove this use of "Thread.sleep()".
- $file:84 — Remove this use of "Thread.sleep()".
- $file:N/A — Remove this use of "Thread.sleep()".
- $file:58 — Remove this use of "Thread.sleep()".

### java:S6541 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 75 to 64, Complexity from 33 to 14, Nesting Level from 5 to 2, Number of Variables from 12 to 6.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 19

**Problem:** A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 75 to 64, Complexity from 33 to 14, Nesting Level from 5 to 2, Number of Variables from 12 to 6.

**Recommendation:** Split the method into smaller methods with single responsibility.

**Locations:**

- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:62 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:64 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:1332 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:2203 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:2308 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:2966 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:3075 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:1115 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:1280 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:108 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:1513 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:1584 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- $file:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...

### java:S1481 — Remove this unused "ignored" local variable.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 19

**Problem:** Remove this unused "ignored" local variable.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- $file:N/A — Remove this unused "inQuotes" local variable.
- $file:N/A — Remove this unused "currentToken" local variable.
- $file:N/A — Remove this unused "column" local variable.
- $file:N/A — Remove this unused "indexPart" local variable.
- $file:N/A — Remove this unused "indexPart" local variable.
- $file:N/A — Remove this unused "indexPart" local variable.
- $file:N/A — Remove this unused "indexPart" local variable.
- $file:2292 — Remove this unused "conditions" local variable.
- $file:N/A — Remove this unused "startPos" local variable.
- $file:N/A — Remove this unused "random" local variable.
- $file:35 — Remove this unused "random" local variable.
- $file:N/A — Remove this unused "columns" local variable.
- $file:N/A — Remove this unused "random" local variable.
- $file:N/A — Remove this unused "random" local variable.
- $file:155 — Remove this unused "columns" local variable.
- $file:156 — Remove this unused "random" local variable.
- $file:302 — Remove this unused "random" local variable.
- $file:129 — Remove this unused "ignored" local variable.
- $file:137 — Remove this unused "ignored" local variable.

### java:S2259 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.

**Severity:** MAJOR | **Type:** BUG | **Found:** 18

**Problem:** A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.

**Recommendation:** Fix according to rule java:S2259.

**Locations:**

- $file:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- $file:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- $file:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- $file:N/A — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- $file:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- $file:N/A — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- $file:N/A — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- $file:770 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- $file:847 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return n...
- $file:866 — A "NullPointerException" could be thrown; "innerNormalized" is nullable here.
- $file:1281 — A "NullPointerException" could be thrown; "original" is nullable here.
- $file:1729 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- $file:1745 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- $file:3326 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- $file:2810 — A "NullPointerException" could be thrown; "buildTable" is nullable here.
- $file:55 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- $file:N/A — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- $file:N/A — A "NullPointerException" could be thrown; "sequences" is nullable here.

### java:S5786 — Remove this 'public' modifier.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 16

**Problem:** Remove this 'public' modifier.

**Recommendation:** In JUnit5, test classes and methods can be package-private — remove the public modifier.

**Locations:**

- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.
- $file:N/A — Remove this 'public' modifier.

### java:S3358 — Extract this nested ternary operation into an independent statement.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Problem:** Extract this nested ternary operation into an independent statement.

**Recommendation:** Fix according to rule java:S3358.

**Locations:**

- $file:71 — Extract this nested ternary operation into an independent statement.
- $file:214 — Extract this nested ternary operation into an independent statement.
- $file:N/A — Extract this nested ternary operation into an independent statement.
- $file:94 — Extract this nested ternary operation into an independent statement.
- $file:N/A — Extract this nested ternary operation into an independent statement.
- $file:355 — Extract this nested ternary operation into an independent statement.
- $file:3149 — Extract this nested ternary operation into an independent statement.
- $file:2263 — Extract this nested ternary operation into an independent statement.
- $file:2375 — Extract this nested ternary operation into an independent statement.
- $file:2380 — Extract this nested ternary operation into an independent statement.
- $file:N/A — Extract this nested ternary operation into an independent statement.
- $file:1671 — Extract this nested ternary operation into an independent statement.
- $file:N/A — Extract this nested ternary operation into an independent statement.

### java:S127 — Refactor the code in order to not assign to this loop counter from within the loop body.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 11

**Problem:** Refactor the code in order to not assign to this loop counter from within the loop body.

**Recommendation:** Fix according to rule java:S127.

**Locations:**

- $file:1115 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:1122 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:3036 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:3041 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:3047 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:2224 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:89 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:92 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:548 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:1561 — Refactor the code in order to not assign to this loop counter from within the loop body.
- $file:1567 — Refactor the code in order to not assign to this loop counter from within the loop body.

### java:S6213 — Rename this method to not match a restricted identifier.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Problem:** Rename this method to not match a restricted identifier.

**Recommendation:** Fix according to rule java:S6213.

**Locations:**

- $file:100 — Rename this method to not match a restricted identifier.
- $file:35 — Rename this variable to not match a restricted identifier.
- $file:90 — Rename this variable to not match a restricted identifier.
- $file:168 — Rename this variable to not match a restricted identifier.
- $file:185 — Rename this variable to not match a restricted identifier.
- $file:98 — Rename this variable to not match a restricted identifier.
- $file:123 — Rename this variable to not match a restricted identifier.
- $file:127 — Rename this variable to not match a restricted identifier.
- $file:137 — Rename this variable to not match a restricted identifier.

### java:S2447 — Null is returned but a "Boolean" is expected.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Null is returned but a "Boolean" is expected.

**Recommendation:** Fix according to rule java:S2447.

**Locations:**

- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.
- $file:N/A — Null is returned but a "Boolean" is expected.

### java:S6485 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int numMappings)

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Replace this call to the constructor with the better suited static method HashMap.newHashMap(int numMappings)

**Recommendation:** Fix according to rule java:S6485.

**Locations:**

- $file:685 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:1245 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:1251 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:1264 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:1323 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:1502 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:2143 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- $file:2158 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...

### java:S1141 — Extract this nested try block into a separate method.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Extract this nested try block into a separate method.

**Recommendation:** Fix according to rule java:S1141.

**Locations:**

- $file:163 — Extract this nested try block into a separate method.
- $file:148 — Extract this nested try block into a separate method.
- $file:151 — Extract this nested try block into a separate method.
- $file:156 — Extract this nested try block into a separate method.
- $file:254 — Extract this nested try block into a separate method.
- $file:275 — Extract this nested try block into a separate method.
- $file:61 — Extract this nested try block into a separate method.
- $file:2232 — Extract this nested try block into a separate method.

### java:S106 — Replace this use of System.out by a logger.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Replace this use of System.out by a logger.

**Recommendation:** Fix according to rule java:S106.

**Locations:**

- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.
- $file:N/A — Replace this use of System.out by a logger.

### java:S112 — Define and throw a dedicated exception instead of using a generic one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Define and throw a dedicated exception instead of using a generic one.

**Recommendation:** Fix according to rule java:S112.

**Locations:**

- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.
- $file:N/A — Define and throw a dedicated exception instead of using a generic one.

### java:S2139 — Either log this exception and handle it, or rethrow it with some contextual information.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Either log this exception and handle it, or rethrow it with some contextual information.

**Recommendation:** Fix according to rule java:S2139.

**Locations:**

- $file:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:165 — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:784 — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:87 — Either log this exception and handle it, or rethrow it with some contextual information.
- $file:945 — Either log this exception and handle it, or rethrow it with some contextual information.

### java:S1948 — Make "rowIndices" private or transient.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Make "rowIndices" private or transient.

**Recommendation:** Fix according to rule java:S1948.

**Locations:**

- $file:22 — Make "keys" transient or serializable.
- $file:23 — Make "rowIndices" private or transient.
- $file:24 — Make "children" private or transient.
- $file:21 — Make "keys" transient or serializable.
- $file:22 — Make "rowIndices" private or transient.
- $file:23 — Make "children" private or transient.
- $file:15 — Make "indexMap" transient or serializable.
- $file:15 — Make "indexMap" transient or serializable.

### java:S1186 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException or complete the implementation.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 6

**Problem:** Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException or complete the implementation.

**Recommendation:** Fix according to rule java:S1186.

**Locations:**

- $file:40 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- $file:44 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- $file:173 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- $file:177 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- $file:103 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- $file:107 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...

### java:S1068 — Remove this unused "DATE_FORMATTER" private field.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Remove this unused "DATE_FORMATTER" private field.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- $file:203 — Remove this unused "socketTimeout" private field.
- $file:N/A — Remove this unused "originalQuery" private field.
- $file:N/A — Remove this unused "OPERATORS" private field.
- $file:N/A — Remove this unused "subQueries" private field.
- $file:22 — Remove this unused "DATE_FORMATTER" private field.

### java:S1452 — Remove usage of generic wildcard type.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Remove usage of generic wildcard type.

**Recommendation:** Fix according to rule java:S1452.

**Locations:**

- $file:35 — Remove usage of generic wildcard type.
- $file:154 — Remove usage of generic wildcard type.
- $file:568 — Remove usage of generic wildcard type.
- $file:763 — Remove usage of generic wildcard type.
- $file:56 — Remove usage of generic wildcard type.

### java:S5857 — Replace this use of a reluctant quantifier with "[^\\)]*+".

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Replace this use of a reluctant quantifier with "[^\\)]*+".

**Recommendation:** Fix according to rule java:S5857.

**Locations:**

- $file:1961 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- $file:271 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- $file:815 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- $file:850 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- $file:1648 — Replace this use of a reluctant quantifier with "[^\\)]*+".

### java:S5850 — Group parts of the regex together to make the intended operator precedence explicit.

**Severity:** MAJOR | **Type:** BUG | **Found:** 5

**Problem:** Group parts of the regex together to make the intended operator precedence explicit.

**Recommendation:** Fix according to rule java:S5850.

**Locations:**

- $file:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- $file:1701 — Group parts of the regex together to make the intended operator precedence explicit.
- $file:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- $file:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- $file:704 — Group parts of the regex together to make the intended operator precedence explicit.

### java:S125 — This block of commented-out lines of code should be removed.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** This block of commented-out lines of code should be removed.

**Recommendation:** Fix according to rule java:S125.

**Locations:**

- $file:1232 — This block of commented-out lines of code should be removed.
- $file:1806 — This block of commented-out lines of code should be removed.
- $file:630 — This block of commented-out lines of code should be removed.
- $file:N/A — This block of commented-out lines of code should be removed.

### java:S6395 — Unwrap this unnecessarily grouped subpattern.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Unwrap this unnecessarily grouped subpattern.

**Recommendation:** Fix according to rule java:S6395.

**Locations:**

- $file:1701 — Unwrap this unnecessarily grouped subpattern.
- $file:1731 — Unwrap this unnecessarily grouped subpattern.
- $file:704 — Unwrap this unnecessarily grouped subpattern.
- $file:709 — Unwrap this unnecessarily grouped subpattern.

### java:S2589 — Remove this expression which always evaluates to "true"

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Remove this expression which always evaluates to "true"

**Recommendation:** Fix according to rule java:S2589.

**Locations:**

- $file:N/A — Remove this expression which always evaluates to "true"
- $file:1785 — Remove this expression which always evaluates to "true"
- $file:1789 — Remove this expression which always evaluates to "true"
- $file:1798 — Remove this expression which always evaluates to "true"

### java:S3626 — Remove this redundant jump.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Remove this redundant jump.

**Recommendation:** Fix according to rule java:S3626.

**Locations:**

- $file:2893 — Remove this redundant jump.
- $file:2899 — Remove this redundant jump.
- $file:1410 — Remove this redundant jump.
- $file:1413 — Remove this redundant jump.

### java:S2629 — Use the built-in formatting to construct this argument.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Use the built-in formatting to construct this argument.

**Recommendation:** Fix according to rule java:S2629.

**Locations:**

- $file:65 — Use the built-in formatting to construct this argument.
- $file:348 — Use the built-in formatting to construct this argument.
- $file:2046 — Use the built-in formatting to construct this argument.
- $file:2248 — Invoke method(s) only conditionally.

### java:S1144 — Remove this unused private "SelectQuery" constructor.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Remove this unused private "SelectQuery" constructor.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- $file:N/A — Remove this unused private "areSubQueriesEquivalent" method.
- $file:N/A — Remove this unused private "splitOrderByClause" method.
- $file:N/A — Remove this unused private "parseLimitClause" method.
- $file:382 — Remove this unused private "SelectQuery" constructor.

### java:S1066 — Merge this if statement with the enclosing one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Merge this if statement with the enclosing one.

**Recommendation:** Fix according to rule java:S1066.

**Locations:**

- $file:332 — Merge this if statement with the enclosing one.
- $file:929 — Merge this if statement with the enclosing one.
- $file:N/A — Merge this if statement with the enclosing one.
- $file:910 — Merge this if statement with the enclosing one.

### java:S6204 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.

**Recommendation:** Fix according to rule java:S6204.

**Locations:**

- $file:2057 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- $file:892 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- $file:1775 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- $file:1812 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...

### java:S1168 — Return an empty map instead of null.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Return an empty map instead of null.

**Recommendation:** Fix according to rule java:S1168.

**Locations:**

- $file:1500 — Return an empty map instead of null.
- $file:2055 — Return an empty collection instead of null.
- $file:2063 — Return an empty collection instead of null.
- $file:2119 — Return an empty collection instead of null.

### java:S2583 — Change this condition so that it does not always evaluate to "true"

**Severity:** MAJOR | **Type:** BUG | **Found:** 3

**Problem:** Change this condition so that it does not always evaluate to "true"

**Recommendation:** Fix according to rule java:S2583.

**Locations:**

- $file:N/A — Change this condition so that it does not always evaluate to "true"
- $file:N/A — Change this condition so that it does not always evaluate to "true"
- $file:N/A — Change this condition so that it does not always evaluate to "true"

### java:S899 — Do something with the "boolean" value returned by "hasNext".

**Severity:** MINOR | **Type:** BUG | **Found:** 3

**Problem:** Do something with the "boolean" value returned by "hasNext".

**Recommendation:** Fix according to rule java:S899.

**Locations:**

- $file:N/A — Do something with the "boolean" value returned by "delete".
- $file:N/A — Do something with the "boolean" value returned by "delete".
- $file:1654 — Do something with the "boolean" value returned by "hasNext".

### java:S3008 — Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Recommendation:** Fix according to rule java:S3008.

**Locations:**

- $file:161 — Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.
- $file:171 — Rename this field "MAX_HASH_TABLE_SIZE_BYTES" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.
- $file:182 — Rename this field "MAX_RESULT_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

### java:S2737 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Add logic to this catch clause or eliminate it and rethrow the exception automatically.

**Recommendation:** Fix according to rule java:S2737.

**Locations:**

- $file:138 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.
- $file:2275 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.

### java:S6208 — Merge the previous cases into this one using comma-separated label.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Merge the previous cases into this one using comma-separated label.

**Recommendation:** Fix according to rule java:S6208.

**Locations:**

- $file:1058 — Merge the previous cases into this one using comma-separated label.
- $file:3159 — Merge the previous cases into this one using comma-separated label.

### java:S6539 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies on other classes from 24 to the maximum authorized 20 or less.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies on other classes from 24 to the maximum authorized 20 or less.

**Recommendation:** Fix according to rule java:S6539.

**Locations:**

- $file:38 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies ...
- $file:39 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies ...

### java:S3824 — Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

**Recommendation:** Fix according to rule java:S3824.

**Locations:**

- $file:1739 — Replace this "Map.containsKey()" with a call to "Map.computeIfAbsent()".
- $file:2423 — Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

### java:S1157 — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

**Recommendation:** Use equalsIgnoreCase() instead of 	oLowerCase()/toUpperCase() + equals().

**Locations:**

- $file:N/A — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.
- $file:546 — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

### java:S3599 — Use another way to initialize this instance.

**Severity:** MINOR | **Type:** BUG | **Found:** 2

**Problem:** Use another way to initialize this instance.

**Recommendation:** Fix according to rule java:S3599.

**Locations:**

- $file:N/A — Use another way to initialize this instance.
- $file:N/A — Use another way to initialize this instance.

### java:S2293 — Replace the type specification in this constructor call with the diamond operator ("<>").

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace the type specification in this constructor call with the diamond operator ("<>").

**Recommendation:** Use diamond operator: 
ew HashMap<>() instead of 
ew HashMap<String, String>().

**Locations:**

- $file:122 — Replace the type specification in this constructor call with the diamond operator ("<>").
- $file:1119 — Replace the type specification in this constructor call with the diamond operator ("<>").

### java:S1171 — Move the contents of this initializer to a standard constructor or to field initializers.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Move the contents of this initializer to a standard constructor or to field initializers.

**Recommendation:** Fix according to rule java:S1171.

**Locations:**

- $file:N/A — Move the contents of this initializer to a standard constructor or to field initializers.
- $file:N/A — Move the contents of this initializer to a standard constructor or to field initializers.

### java:S4042 — Use "java.nio.file.Files#delete" here for better messages on error conditions.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Use "java.nio.file.Files#delete" here for better messages on error conditions.

**Recommendation:** Fix according to rule java:S4042.

**Locations:**

- $file:N/A — Use "java.nio.file.Files#delete" here for better messages on error conditions.
- $file:N/A — Use "java.nio.file.Files#delete" here for better messages on error conditions.

### java:S2864 — Iterate over the "entrySet" instead of the "keySet".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Iterate over the "entrySet" instead of the "keySet".

**Recommendation:** Fix according to rule java:S2864.

**Locations:**

- $file:N/A — Iterate over the "entrySet" instead of the "keySet".
- $file:901 — Iterate over the "entrySet" instead of the "keySet".

### java:S1488 — Immediately return this expression instead of assigning it to the temporary variable "result".

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Immediately return this expression instead of assigning it to the temporary variable "result".

**Recommendation:** Fix according to rule java:S1488.

**Locations:**

- $file:N/A — Immediately return this expression instead of assigning it to the temporary variable "result".

### java:S6397 — Replace this character class by the character itself.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Replace this character class by the character itself.

**Recommendation:** Fix according to rule java:S6397.

**Locations:**

- $file:986 — Replace this character class by the character itself.

### java:S5842 — Rework this part of the regex to not match the empty string.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Rework this part of the regex to not match the empty string.

**Recommendation:** Fix according to rule java:S5842.

**Locations:**

- $file:N/A — Rework this part of the regex to not match the empty string.

### java:S5785 — Use assertSame instead.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use assertSame instead.

**Recommendation:** Fix according to rule java:S5785.

**Locations:**

- $file:N/A — Use assertSame instead.

### java:S1118 — Add a private constructor to hide the implicit public one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Add a private constructor to hide the implicit public one.

**Recommendation:** In JUnit5, test classes and methods can be package-private — remove the public modifier.

**Locations:**

- $file:12 — Add a private constructor to hide the implicit public one.

### java:S2676 — Use the original value instead.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Use the original value instead.

**Recommendation:** Fix according to rule java:S2676.

**Locations:**

- $file:493 — Use the original value instead.

### java:S5961 — Refactor this method to reduce the number of assertions from 26 to less than 25.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Refactor this method to reduce the number of assertions from 26 to less than 25.

**Recommendation:** Fix according to rule java:S5961.

**Locations:**

- $file:N/A — Refactor this method to reduce the number of assertions from 26 to less than 25.

### java:S2272 — Add a "NoSuchElementException" for iteration beyond the end of the collection.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Add a "NoSuchElementException" for iteration beyond the end of the collection.

**Recommendation:** Fix according to rule java:S2272.

**Locations:**

- $file:1646 — Add a "NoSuchElementException" for iteration beyond the end of the collection.

### java:S6548 — A Singleton implementation was detected. Make sure the use of the Singleton pattern is required and the implementation is the right one for the context.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 1

**Problem:** A Singleton implementation was detected. Make sure the use of the Singleton pattern is required and the implementation is the right one for the context.

**Recommendation:** Fix according to rule java:S6548.

**Locations:**

- $file:45 — A Singleton implementation was detected. Make sure the use of the Singleton pattern is required a...

### java:S6885 — Use "Math.clamp" instead of "Math.min" or "Math.max".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use "Math.clamp" instead of "Math.min" or "Math.max".

**Recommendation:** Fix according to rule java:S6885.

**Locations:**

- $file:1426 — Use "Math.clamp" instead of "Math.min" or "Math.max".

### java:S2093 — Change this "try" to a try-with-resources.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Change this "try" to a try-with-resources.

**Recommendation:** Fix according to rule java:S2093.

**Locations:**

- $file:248 — Change this "try" to a try-with-resources.

### java:S1130 — Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from constructor's body.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from constructor's body.

**Recommendation:** Fix according to rule java:S1130.

**Locations:**

- $file:1588 — Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from con...

### java:S3400 — Remove this method and declare a constant for this value.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Remove this method and declare a constant for this value.

**Recommendation:** Fix according to rule java:S3400.

**Locations:**

- $file:1021 — Remove this method and declare a constant for this value.

### java:S5164 — Call "remove()" on "QUERY_MEMORY".

**Severity:** MAJOR | **Type:** BUG | **Found:** 1

**Problem:** Call "remove()" on "QUERY_MEMORY".

**Recommendation:** Fix according to rule java:S5164.

**Locations:**

- $file:275 — Call "remove()" on "QUERY_MEMORY".

### java:S2147 — Combine this catch with the one at line 200, which has the same body.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Combine this catch with the one at line 200, which has the same body.

**Recommendation:** Fix according to rule java:S2147.

**Locations:**

- $file:203 — Combine this catch with the one at line 200, which has the same body.

### java:S1117 — Rename "autoCommit" which hides the field declared at line 44.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Rename "autoCommit" which hides the field declared at line 44.

**Recommendation:** Fix according to rule java:S1117.

**Locations:**

- $file:337 — Rename "autoCommit" which hides the field declared at line 44.

### java:S1155 — Use isEmpty() to check whether the collection is empty or not.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use isEmpty() to check whether the collection is empty or not.

**Recommendation:** Use collection.isEmpty() instead of collection.size() == 0.

**Locations:**

- $file:N/A — Use isEmpty() to check whether the collection is empty or not.


---\n*Report generated on 2026-08-18 23:04:39*
