# SonarQube Analysis Results - DieselDB (Detailed Report)

**Date:** 2026-08-23 14:25:47
**Project:** dieseldb
**SonarQube Version:** 10.7.0.96327
**Scanner:** SonarScanner CLI 6.2.1.4610 (JAVA_HOME=JDK21)
**Java Version:** 21.0.11

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

## Issue Summary by Severity

| Severity | Count |
|----------|-------|| CRITICAL | 165 |
| MAJOR | 728 |
| MINOR | 363 |
| INFO | 41 |
## Issue Summary by Type

| Type | Count |
|------|-------|
| BUG | 92 |
| CODE_SMELL | 1205 |
| VULNERABILITY | 0 |
| SECURITY_HOTSPOT | 0 |

## Top Rules by Issue Count

| Count | Rule | Name | Severity | Type |
|-------|------|------|----------|------|| 342 | java:S5869 | Remove duplicates in this character class. | MAJOR | CODE_SMELL |
| 119 | java:S6353 | Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'. | MINOR | CODE_SMELL |
| 94 | java:S3776 | Refactor this method to reduce its Cognitive Complexity from 38 to the 15 all... | CRITICAL | CODE_SMELL |
| 84 | java:S6201 | Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery se... | MINOR | CODE_SMELL |
| 57 | java:S5998 | Refactor this repetition that can lead to a stack overflow for large inputs. | MAJOR | BUG |
| 43 | java:S1192 | Define a constant instead of duplicating this literal "|\\(.*?\\))\\s*\\)(?:\... | CRITICAL | CODE_SMELL |
| 36 | java:S1128 | Remove this unused import 'diesel.ThreeValuedLogic.FALSE'. | MINOR | CODE_SMELL |
| 30 | java:S1172 | Remove this unused method parameter "not". | MAJOR | CODE_SMELL |
| 30 | java:S135 | Reduce the total number of break and continue statements in this loop to use ... | MINOR | CODE_SMELL |
| 28 | java:S108 | Either remove or fill this block of code. | MAJOR | CODE_SMELL |
| 28 | java:S1874 | Remove this use of "setScale"; it is deprecated. | MINOR | CODE_SMELL |
| 26 | java:S1481 | Remove this unused "ck2" local variable. | MINOR | CODE_SMELL |
| 23 | java:S1854 | Remove this useless assignment to local variable "joins". | MAJOR | CODE_SMELL |
| 23 | java:S3457 | first argument is not used. | MAJOR | CODE_SMELL |
| 22 | java:S107 | Constructor has 14 parameters, which is greater than 7 authorized. | MAJOR | CODE_SMELL |
| 21 | java:S5843 | Simplify this regular expression to reduce its complexity from 23 to the 20 a... | MAJOR | CODE_SMELL |
| 20 | java:S6541 | A "Brain Method" was detected. Refactor it to reduce at least one of the foll... | INFO | CODE_SMELL |
| 20 | java:S2925 | Remove this use of "Thread.sleep()". | MAJOR | CODE_SMELL |
| 18 | java:S2259 | A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentif... | MAJOR | BUG |
| 16 | java:S5786 | Remove this 'public' modifier. | INFO | CODE_SMELL |
| 13 | java:S3358 | Extract this nested ternary operation into an independent statement. | MAJOR | CODE_SMELL |
| 11 | java:S5857 | Replace this use of a reluctant quantifier with "[^\\)]*+". | MINOR | CODE_SMELL |
| 11 | java:S127 | Refactor the code in order to not assign to this loop counter from within the... | MAJOR | CODE_SMELL |
| 9 | java:S6213 | Rename this method to not match a restricted identifier. | MAJOR | CODE_SMELL |
| 8 | java:S106 | Replace this use of System.out by a logger. | MAJOR | CODE_SMELL |
| 8 | java:S6485 | Replace this call to the constructor with the better suited static method Has... | MAJOR | CODE_SMELL |
| 8 | java:S1948 | Make "rowIndices" private or transient. | CRITICAL | CODE_SMELL |
| 8 | java:S1141 | Extract this nested try block into a separate method. | MAJOR | CODE_SMELL |
| 8 | java:S112 | Define and throw a dedicated exception instead of using a generic one. | MAJOR | CODE_SMELL |
| 8 | java:S2139 | Either log this exception and handle it, or rethrow it with some contextual i... | MAJOR | CODE_SMELL |
## Top Files by Issue Count

| File | Issues |
|------|--------|| diesel/QueryParser.java | 471 |
| diesel/SubqueryParser.java | 328 |
| diesel/SelectQuery.java | 115 |
| diesel/Database.java | 47 |
| diesel/DeleteQuery.java | 27 |
| diesel/Table.java | 24 |
| diesel/UpdateQuery.java | 23 |
| src/test/java/diesel/PerformanceTest.java | 23 |
| src/test/java/diesel/ServerConnectionLimitTest.java | 21 |
| diesel/ConditionEvaluator.java | 16 |
| diesel/DatabaseClient.java | 16 |
| diesel/ExplainQuery.java | 12 |
| diesel/SqlLexer.java | 10 |
| src/test/java/diesel/AllTestsSampleTest.java | 10 |
| diesel/DatabaseServer.java | 9 |
| src/test/java/diesel/OomHandlingTest.java | 9 |
| src/test/java/diesel/QuantitativeTest.java | 9 |
| diesel/BTreeIndex.java | 9 |
| diesel/BTreeClusteredIndex.java | 9 |
| src/test/java/diesel/AdvancedTest.java | 8 |
| src/test/java/diesel/AliasesTest.java | 7 |
| src/test/java/diesel/OrderByTest.java | 6 |
| src/test/java/diesel/GroupByTest.java | 6 |
| src/test/java/diesel/MaxResultRowsTest.java | 6 |
| src/test/java/diesel/JoinTest.java | 5 |

## Detailed Issues by Rule

> Each block shows one rule: what's wrong, how to fix, and full list of locations (file:line — message).

### java:S5869 — Remove duplicates in this character class.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 342

**Problem:** Remove duplicates in this character class.

**Recommendation:** Remove duplicate characters in the regex character class (e.g. [0-90-9] → [0-9]).

**Locations:**

- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- `diesel/QueryParser.java`:N/A — Remove duplicates in this character class.
- ... and 292 more

### java:S6353 — Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 119

**Problem:** Use concise character class syntax '\\w' instead of '[A-Za-z0-9_]'.

**Recommendation:** Use concise character class syntax like \w instead of [a-zA-Z0-9_], \d instead of [0-9] etc.

**Locations:**

- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- `diesel/QueryParser.java`:N/A — Use concise character class syntax '\\w' instead of '[a-zA-Z0-9_]'.
- ... and 69 more

### java:S3776 — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 94

**Problem:** Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.

**Recommendation:** Reduce branching/nesting; extract helper methods to lower cognitive complexity below threshold.

**Locations:**

- `diesel/BTreeClusteredIndex.java`:156 — Refactor this method to reduce its Cognitive Complexity from 26 to the 15 allowed.
- `diesel/BTreeIndex.java`:160 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/BTreeIndex.java`:198 — Refactor this method to reduce its Cognitive Complexity from 20 to the 15 allowed.
- `diesel/CliRepl.java`:166 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:57 — Refactor this method to reduce its Cognitive Complexity from 42 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:155 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/ConditionEvaluator.java`:210 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/Database.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 63 to the 15 allowed.
- `diesel/Database.java`:526 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/Database.java`:548 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/DatabaseServer.java`:247 — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/DeleteQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/DeleteQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/DeleteQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 53 to the 15 allowed.
- `diesel/DeleteQuery.java`:56 — Refactor this method to reduce its Cognitive Complexity from 64 to the 15 allowed.
- `diesel/InsertQuery.java`:64 — Refactor this method to reduce its Cognitive Complexity from 65 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 73 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 16 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 49 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 19 to the 15 allowed.
- `diesel/QueryParser.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- `diesel/QueryParser.java`:239 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/QueryParser.java`:966 — Refactor this method to reduce its Cognitive Complexity from 38 to the 15 allowed.
- `diesel/QueryParser.java`:1100 — Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.
- `diesel/QueryParser.java`:1156 — Refactor this method to reduce its Cognitive Complexity from 24 to the 15 allowed.
- `diesel/QueryParser.java`:1328 — Refactor this method to reduce its Cognitive Complexity from 100 to the 15 allowed.
- `diesel/QueryParser.java`:1517 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- `diesel/QueryParser.java`:1628 — Refactor this method to reduce its Cognitive Complexity from 31 to the 15 allowed.
- `diesel/QueryParser.java`:1748 — Refactor this method to reduce its Cognitive Complexity from 18 to the 15 allowed.
- `diesel/QueryParser.java`:1875 — Refactor this method to reduce its Cognitive Complexity from 58 to the 15 allowed.
- `diesel/QueryParser.java`:1952 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- `diesel/QueryParser.java`:2011 — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- `diesel/QueryParser.java`:2199 — Refactor this method to reduce its Cognitive Complexity from 52 to the 15 allowed.
- `diesel/QueryParser.java`:2304 — Refactor this method to reduce its Cognitive Complexity from 43 to the 15 allowed.
- `diesel/QueryParser.java`:2474 — Refactor this method to reduce its Cognitive Complexity from 23 to the 15 allowed.
- `diesel/QueryParser.java`:2875 — Refactor this method to reduce its Cognitive Complexity from 25 to the 15 allowed.
- `diesel/QueryParser.java`:2974 — Refactor this method to reduce its Cognitive Complexity from 73 to the 15 allowed.
- `diesel/QueryParser.java`:3083 — Refactor this method to reduce its Cognitive Complexity from 47 to the 15 allowed.
- `diesel/QueryParser.java`:3189 — Refactor this method to reduce its Cognitive Complexity from 22 to the 15 allowed.
- `diesel/QueryParser.java`:3239 — Refactor this method to reduce its Cognitive Complexity from 44 to the 15 allowed.
- `diesel/SelectQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 157 to the 15 allowed.
- `diesel/SelectQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 21 to the 15 allowed.
- `diesel/SelectQuery.java`:N/A — Refactor this method to reduce its Cognitive Complexity from 37 to the 15 allowed.
- `diesel/SelectQuery.java`:736 — Refactor this method to reduce its Cognitive Complexity from 41 to the 15 allowed.
- ... and 44 more

### java:S6201 — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery setautocommitquery'

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 84

**Problem:** Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery setautocommitquery'

**Recommendation:** Use pattern matching (Java 16+): if (x instanceof Foo f) { ... f.method() ... } to remove the separate cast.

**Locations:**

- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/ConditionEvaluator.java`:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateUniqueIndexQuery indexQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateTableQuery createQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateUniqueClusteredIndexQuery indexQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SetIsolationLevelQuery setisolationlevelq...
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery autoCommitQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateHashIndexQuery indexQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateIndexQuery indexQuery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof BeginTransactionQuery begintransactionquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateIndexQueryBase createindexquerybase'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof AnalyzeTableQuery analyzetablequery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SetAutoCommitQuery setautocommitquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof CreateTableQuery createtablequery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof ExplainQuery explainquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery select'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/Database.java`:N/A — Replace this instanceof check and cast with 'instanceof SelectQuery selectquery'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DatabaseClient.java`:N/A — Replace this instanceof check and cast with 'instanceof String string'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof BTreeIndex btreeindex'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Float float'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof BigDecimal bigdecimal'
- `diesel/DeleteQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof Double double'
- `diesel/ExplainQuery.java`:N/A — Replace this instanceof check and cast with 'instanceof DeleteQuery deletequery'
- ... and 34 more

### java:S5998 — Refactor this repetition that can lead to a stack overflow for large inputs.

**Severity:** MAJOR | **Type:** BUG | **Found:** 57

**Problem:** Refactor this repetition that can lead to a stack overflow for large inputs.

**Recommendation:** Fix according to rule java:S5998.

**Locations:**

- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/QueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- `diesel/SubqueryParser.java`:N/A — Refactor this repetition that can lead to a stack overflow for large inputs.
- ... and 7 more

### java:S1192 — Define a constant instead of duplicating this literal "|\\(.*?\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 times.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 43

**Problem:** Define a constant instead of duplicating this literal "|\\(.*?\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 times.

**Recommendation:** Fix according to rule java:S1192.

**Locations:**

- `diesel/Database.java`:N/A — Define a constant instead of duplicating this literal "Table part after split: {0}" 5 times.
- `diesel/Database.java`:N/A — Define a constant instead of duplicating this literal ".table" 3 times.
- `diesel/Database.java`:N/A — Define a constant instead of duplicating this literal " does not exist" 4 times.
- `diesel/Database.java`:N/A — Define a constant instead of duplicating this literal " does not exist" 3 times.
- `diesel/Database.java`:N/A — Define a constant instead of duplicating this literal "Table " 5 times.
- `diesel/ExplainQuery.java`:N/A — Define a constant instead of duplicating this literal "none (full scan)" 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "' does not match column type: " 4 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "Table not found: " 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "UPDATE" 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "ID=U.ID" 4 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "Quoted String" 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "SELECT " 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "Unknown column: " 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "ID = U.ID" 4 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "COUNT" 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "Numeric value '" 5 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "NOT LIKE" 7 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "SELECT" 4 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "LIMIT" 3 times.
- `diesel/QueryParser.java`:N/A — Define a constant instead of duplicating this literal "|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 ti...
- `diesel/QueryParser.java`:1164 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/QueryParser.java`:1197 — Define a constant instead of duplicating this literal "quotedString" 3 times.
- `diesel/QueryParser.java`:1205 — Define a constant instead of duplicating this literal "openParen" 3 times.
- `diesel/QueryParser.java`:1209 — Define a constant instead of duplicating this literal "closeParen" 3 times.
- `diesel/QueryParser.java`:1335 — Define a constant instead of duplicating this literal "|\\(.*?\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" 5 t...
- `diesel/QueryParser.java`:1340 — Define a constant instead of duplicating this literal "(?i)^(" 9 times.
- `diesel/QueryParser.java`:2308 — Define a constant instead of duplicating this literal "Quoted String" 3 times.
- `diesel/QueryParser.java`:2316 — Define a constant instead of duplicating this literal "(?i)(" 7 times.
- `diesel/QueryParser.java`:2889 — Define a constant instead of duplicating this literal "(SELECT" 5 times.
- `diesel/SelectQuery.java`:N/A — Define a constant instead of duplicating this literal " is not attached to a database" 4 times.
- `diesel/SelectQuery.java`:N/A — Define a constant instead of duplicating this literal "Table " 4 times.
- `diesel/SelectQuery.java`:N/A — Define a constant instead of duplicating this literal "result" 3 times.
- `diesel/SubqueryParser.java`:N/A — Define a constant instead of duplicating this literal "SELECT" 10 times.
- `diesel/SubqueryParser.java`:N/A — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- `diesel/SubqueryParser.java`:N/A — Define a constant instead of duplicating this literal "<end>" 3 times.
- `diesel/SubqueryParser.java`:N/A — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 ti...
- `diesel/SubqueryParser.java`:182 — Use already-defined constant 'QUOTED_IDENTIFIER_PATTERN' instead of duplicating its value here.
- `diesel/SubqueryParser.java`:268 — Define a constant instead of duplicating this literal "(?i)^(" 8 times.
- `diesel/SubqueryParser.java`:286 — Define a constant instead of duplicating this literal "SUBQUERY_" 3 times.
- `diesel/SubqueryParser.java`:972 — Define a constant instead of duplicating this literal "(?i)(" 6 times.
- `diesel/SubqueryParser.java`:1278 — Define a constant instead of duplicating this literal "Unbalanced parentheses in subquery: " 3 ti...
- `diesel/Table.java`:N/A — Define a constant instead of duplicating this literal " does not exist" 3 times.
- `diesel/Table.java`:221 — Define a constant instead of duplicating this literal "Column " 3 times.

### java:S1128 — Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 36

**Problem:** Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- `diesel/BeginTransactionQuery.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/BeginTransactionQuery.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/BeginTransactionQuery.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `diesel/DeleteQuery.java`:N/A — Remove this unused import 'java.util.Objects'.
- `diesel/DeleteQuery.java`:N/A — Remove this unused import 'java.math.BigDecimal'.
- `diesel/DeleteQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.
- `diesel/DeleteQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.UNKNOWN'.
- `diesel/DeleteQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.
- `diesel/Query.java`:N/A — Remove this unused import 'java.util'.
- `diesel/QueryParser.java`:N/A — Remove this unused import 'java.util.Objects'.
- `diesel/SubqueryParser.java`:N/A — Remove this unused import 'java.util.Objects'.
- `diesel/Table.java`:N/A — Remove this unused import 'java.util.Objects'.
- `diesel/UpdateQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.UNKNOWN'.
- `diesel/UpdateQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.TRUE'.
- `diesel/UpdateQuery.java`:N/A — Remove this unused import 'java.math.BigDecimal'.
- `diesel/UpdateQuery.java`:N/A — Remove this unused import 'diesel.ThreeValuedLogic.FALSE'.
- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/DatabaseSmokeTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/DatabaseSmokeTest.java`:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertTrue'.
- `src/test/java/diesel/GroupByTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/InTest.java`:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- `src/test/java/diesel/InTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/JoinTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/LikeTest.java`:N/A — Remove this unused import 'org.junit.jupiter.api.Assertions.assertThrows'.
- `src/test/java/diesel/LikeTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/OrderByTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused import 'java.util.stream.IntStream'.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused import 'java.util.stream.Collectors'.
- `src/test/java/diesel/PersistenceTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this unused import 'java.util.concurrent.TimeUnit'.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this unused import 'java.util.concurrent.CountDownLatch'.
- `src/test/java/diesel/SubqueriesTest.java`:N/A — Remove this unnecessary import: same package classes are always implicitly imported.

### java:S135 — Reduce the total number of break and continue statements in this loop to use at most one.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 30

**Problem:** Reduce the total number of break and continue statements in this loop to use at most one.

**Recommendation:** Fix according to rule java:S135.

**Locations:**

- `diesel/CliRepl.java`:106 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/ConditionEvaluator.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/DatabaseServer.java`:252 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/DeleteQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:1106 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2348 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2701 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2880 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:2985 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/QueryParser.java`:3284 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SelectQuery.java`:1345 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:116 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SqlLexer.java`:129 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:189 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:882 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1198 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1399 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1520 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/SubqueryParser.java`:1590 — Reduce the total number of break and continue statements in this loop to use at most one.
- `diesel/UpdateQuery.java`:N/A — Reduce the total number of break and continue statements in this loop to use at most one.

### java:S1172 — Remove this unused method parameter "not".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 30

**Problem:** Remove this unused method parameter "not".

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- `diesel/ConditionEvaluator.java`:155 — Remove this unused method parameter "columnTypes".
- `diesel/DeleteQuery.java`:N/A — Remove this unused method parameter "columnTypes".
- `diesel/QueryParser.java`:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/QueryParser.java`:N/A — Remove this unused method parameter "conditionStr".
- `diesel/QueryParser.java`:N/A — Remove these unused method parameters "originalQuery", "not".
- `diesel/QueryParser.java`:N/A — Remove this unused method parameter "conditionStr".
- `diesel/QueryParser.java`:N/A — Remove this unused method parameter "conditionColumn".
- `diesel/QueryParser.java`:N/A — Remove this unused method parameter "normalized".
- `diesel/QueryParser.java`:853 — Remove this unused method parameter "normalized".
- `diesel/QueryParser.java`:1265 — Remove this unused method parameter "normalized".
- `diesel/QueryParser.java`:2649 — Remove this unused method parameter "not".
- `diesel/QueryParser.java`:2938 — Remove this unused method parameter "tableAliases".
- `diesel/SelectQuery.java`:N/A — Remove these unused method parameters "mainRows", "tables".
- `diesel/SelectQuery.java`:N/A — Remove this unused method parameter "combinedColumnTypes".
- `diesel/SelectQuery.java`:404 — Remove this unused method parameter "columnTypes".
- `diesel/SelectQuery.java`:2057 — Remove this unused method parameter "combinedColumnTypes".
- `diesel/SubqueryParser.java`:N/A — Remove these unused method parameters "originalQuery", "columnAliases".
- `diesel/SubqueryParser.java`:N/A — Remove this unused method parameter "not".
- `diesel/SubqueryParser.java`:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases", "not".
- `diesel/SubqueryParser.java`:N/A — Remove this unused method parameter "columnAliases".
- `diesel/SubqueryParser.java`:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/SubqueryParser.java`:N/A — Remove these unused method parameters "database", "originalQuery", "columnAliases".
- `diesel/SubqueryParser.java`:N/A — Remove this unused method parameter "columnAliases".
- `diesel/SubqueryParser.java`:124 — Remove this unused method parameter "normalizedQuery".
- `diesel/SubqueryParser.java`:1127 — Remove this unused method parameter "not".
- `diesel/SubqueryParser.java`:1164 — Remove this unused method parameter "not".
- `diesel/SubqueryParser.java`:1429 — Remove this unused method parameter "tableAliases".
- `diesel/UpdateQuery.java`:N/A — Remove this unused method parameter "columnTypes".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused method parameter "random".
- `src/test/java/diesel/PerformanceTest.java`:112 — Remove this unused method parameter "random".

### java:S1874 — Remove this use of "setScale"; it is deprecated.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 28

**Problem:** Remove this use of "setScale"; it is deprecated.

**Recommendation:** Fix according to rule java:S1874.

**Locations:**

- `diesel/SelectQuery.java`:N/A — Remove this use of "divide"; it is deprecated.
- `diesel/SelectQuery.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/GroupByTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/GroupByTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/InTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/InTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/JoinTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/JoinTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/LikeTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/LikeTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/OrderByTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/OrderByTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this use of "setScale"; it is deprecated.
- `src/test/java/diesel/SubqueriesTest.java`:N/A — Remove this use of "ROUND_HALF_UP"; it is deprecated.
- `src/test/java/diesel/SubqueriesTest.java`:N/A — Remove this use of "setScale"; it is deprecated.

### java:S108 — Either remove or fill this block of code.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 28

**Problem:** Either remove or fill this block of code.

**Recommendation:** Fix according to rule java:S108.

**Locations:**

- `diesel/SelectQuery.java`:N/A — Either remove or fill this block of code.
- `diesel/SelectQuery.java`:N/A — Either remove or fill this block of code.
- `diesel/SelectQuery.java`:N/A — Either remove or fill this block of code.
- `diesel/SelectQuery.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/AliasesTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/AutoJoinIndexTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/ExplainTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/ExplainTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/GracefulShutdownTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/GroupByTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/GroupByTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/HashJoinMemoryTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/HashJoinMemoryTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/JoinTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/LimitOffsetTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/MaxResultRowsTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/MaxResultRowsTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/OomHandlingTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/OomHandlingTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/OrderByTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/OrderByTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/QueryCacheTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/QueryProfilerTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Either remove or fill this block of code.
- `src/test/java/diesel/SubqueriesTest.java`:N/A — Either remove or fill this block of code.

### java:S1481 — Remove this unused "ck2" local variable.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 26

**Problem:** Remove this unused "ck2" local variable.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- `diesel/BTreeClusteredIndex.java`:343 — Remove this unused "ck2" local variable.
- `diesel/BTreeIndex.java`:286 — Remove this unused "ck2" local variable.
- `diesel/Database.java`:224 — Remove this unused "q" local variable.
- `diesel/Database.java`:227 — Remove this unused "q" local variable.
- `diesel/ExplainQuery.java`:76 — Remove this unused "iq" local variable.
- `diesel/ExplainQuery.java`:78 — Remove this unused "uq" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "inQuotes" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "column" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "indexPart" local variable.
- `diesel/QueryParser.java`:N/A — Remove this unused "currentToken" local variable.
- `diesel/QueryParser.java`:2288 — Remove this unused "conditions" local variable.
- `diesel/SubqueryParser.java`:N/A — Remove this unused "startPos" local variable.
- `diesel/Table.java`:402 — Remove this unused "ck2" local variable.
- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this unused "random" local variable.
- `src/test/java/diesel/AdvancedTest.java`:35 — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this unused "columns" local variable.
- `src/test/java/diesel/PerformanceTest.java`:155 — Remove this unused "columns" local variable.
- `src/test/java/diesel/PerformanceTest.java`:156 — Remove this unused "random" local variable.
- `src/test/java/diesel/PerformanceTest.java`:302 — Remove this unused "random" local variable.
- `src/test/java/diesel/StringOpsBenchmarkTest.java`:129 — Remove this unused "ignored" local variable.
- `src/test/java/diesel/StringOpsBenchmarkTest.java`:137 — Remove this unused "ignored" local variable.

### java:S3457 — first argument is not used.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 23

**Problem:** first argument is not used.

**Recommendation:** Fix according to rule java:S3457.

**Locations:**

- `diesel/DatabaseClient.java`:N/A — first argument is not used.
- `diesel/QueryParser.java`:N/A — 6th argument is not used.
- `diesel/QueryParser.java`:N/A — String contains no format specifiers.
- `diesel/QueryParser.java`:N/A — 5th argument is not used.
- `diesel/QueryParser.java`:2399 — first argument is not used.
- `diesel/SubqueryParser.java`:1014 — 4th argument is not used.
- `diesel/SubqueryParser.java`:1014 — 2nd argument is not used.
- `diesel/SubqueryParser.java`:1019 — 3rd argument is not used.
- `diesel/Table.java`:N/A — first argument is not used.
- `diesel/Table.java`:896 — first argument is not used.
- `diesel/Table.java`:912 — first argument is not used.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:74 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:85 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:102 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:105 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:113 — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/SocketTimeoutTest.java`:N/A — Format specifiers or lambda should be used instead of string concatenation.
- `src/test/java/diesel/SocketTimeoutTest.java`:82 — Format specifiers or lambda should be used instead of string concatenation.

### java:S1854 — Remove this useless assignment to local variable "joins".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 23

**Problem:** Remove this useless assignment to local variable "joins".

**Recommendation:** Fix according to rule java:S1854.

**Locations:**

- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "column".
- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "currentToken".
- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:N/A — Remove this useless assignment to local variable "indexPart".
- `diesel/QueryParser.java`:1469 — Remove this useless assignment to local variable "joins".
- `diesel/QueryParser.java`:1534 — Remove this useless assignment to local variable "onClausePart".
- `diesel/QueryParser.java`:2288 — Remove this useless assignment to local variable "conditions".
- `diesel/QueryParser.java`:2371 — Remove this useless assignment to local variable "matchedPatternName".
- `diesel/QueryParser.java`:2373 — Remove this useless assignment to local variable "matched".
- `diesel/SelectQuery.java`:743 — Remove this useless assignment to local variable "newJoinedRows".
- `diesel/SubqueryParser.java`:N/A — Remove this useless assignment to local variable "startPos".
- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/AdvancedTest.java`:35 — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this useless assignment to local variable "columns".
- `src/test/java/diesel/PerformanceTest.java`:155 — Remove this useless assignment to local variable "columns".
- `src/test/java/diesel/PerformanceTest.java`:156 — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/PerformanceTest.java`:302 — Remove this useless assignment to local variable "random".
- `src/test/java/diesel/StringOpsBenchmarkTest.java`:129 — Remove this useless assignment to local variable "ignored".
- `src/test/java/diesel/StringOpsBenchmarkTest.java`:137 — Remove this useless assignment to local variable "ignored".

### java:S107 — Constructor has 14 parameters, which is greater than 7 authorized.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 22

**Problem:** Constructor has 14 parameters, which is greater than 7 authorized.

**Recommendation:** Fix according to rule java:S107.

**Locations:**

- `diesel/QueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 11 parameters, which is greater than 7 authorized.
- `diesel/QueryParser.java`:N/A — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:N/A — Constructor has 16 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:N/A — Constructor has 15 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:382 — Constructor has 14 parameters, which is greater than 7 authorized.
- `diesel/SelectQuery.java`:398 — Constructor has 15 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 10 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 9 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 8 parameters, which is greater than 7 authorized.
- `diesel/SubqueryParser.java`:N/A — Method has 8 parameters, which is greater than 7 authorized.

### java:S5843 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 21

**Problem:** Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.

**Recommendation:** Fix according to rule java:S5843.

**Locations:**

- `diesel/QueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- `diesel/QueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- `diesel/QueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- `diesel/QueryParser.java`:1680 — Simplify this regular expression to reduce its complexity from 23 to the 20 allowed.
- `diesel/QueryParser.java`:2342 — Simplify this regular expression to reduce its complexity from 35 to the 20 allowed.
- `diesel/QueryParser.java`:2565 — Simplify this regular expression to reduce its complexity from 22 to the 20 allowed.
- `diesel/SubqueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- `diesel/SubqueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- `diesel/SubqueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- `diesel/SubqueryParser.java`:N/A — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:74 — Simplify this regular expression to reduce its complexity from 46 to the 20 allowed.
- `diesel/SubqueryParser.java`:117 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- `diesel/SubqueryParser.java`:270 — Simplify this regular expression to reduce its complexity from 24 to the 20 allowed.
- `diesel/SubqueryParser.java`:811 — Simplify this regular expression to reduce its complexity from 29 to the 20 allowed.
- `diesel/SubqueryParser.java`:972 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:974 — Simplify this regular expression to reduce its complexity from 34 to the 20 allowed.
- `diesel/SubqueryParser.java`:976 — Simplify this regular expression to reduce its complexity from 31 to the 20 allowed.
- `diesel/SubqueryParser.java`:982 — Simplify this regular expression to reduce its complexity from 26 to the 20 allowed.
- `diesel/SubqueryParser.java`:1118 — Simplify this regular expression to reduce its complexity from 48 to the 20 allowed.
- `diesel/SubqueryParser.java`:1646 — Simplify this regular expression to reduce its complexity from 21 to the 20 allowed.

### java:S2925 — Remove this use of "Thread.sleep()".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 20

**Problem:** Remove this use of "Thread.sleep()".

**Recommendation:** Fix according to rule java:S2925.

**Locations:**

- `src/test/java/diesel/AllTestsSampleTest.java`:1049 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AllTestsSampleTest.java`:1224 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/AnalyzeTableTest.java`:154 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/GracefulShutdownTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/GracefulShutdownTest.java`:99 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/OomHandlingTest.java`:160 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:325 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/PerformanceTest.java`:364 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:985 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/QuantitativeTest.java`:1160 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:61 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:80 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/ServerConnectionLimitTest.java`:84 — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/SocketTimeoutTest.java`:N/A — Remove this use of "Thread.sleep()".
- `src/test/java/diesel/SocketTimeoutTest.java`:58 — Remove this use of "Thread.sleep()".

### java:S6541 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 75 to 64, Complexity from 33 to 14, Nesting Level from 5 to 2, Number of Variables from 11 to 6.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 20

**Problem:** A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC from 75 to 64, Complexity from 33 to 14, Nesting Level from 5 to 2, Number of Variables from 11 to 6.

**Recommendation:** Split the method into smaller methods with single responsibility.

**Locations:**

- `diesel/Database.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/DeleteQuery.java`:56 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/InsertQuery.java`:64 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:1328 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:2199 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:2304 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:2974 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/QueryParser.java`:3083 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SelectQuery.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SelectQuery.java`:1116 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SelectQuery.java`:1281 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SqlLexer.java`:108 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SubqueryParser.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SubqueryParser.java`:1511 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/SubqueryParser.java`:1582 — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...
- `diesel/Table.java`:N/A — A "Brain Method" was detected. Refactor it to reduce at least one of the following metrics: LOC f...

### java:S2259 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.

**Severity:** MAJOR | **Type:** BUG | **Found:** 18

**Problem:** A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return null.

**Recommendation:** Fix according to rule java:S2259.

**Locations:**

- `diesel/QueryParser.java`:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:N/A — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/QueryParser.java`:N/A — "NullPointerException" will be thrown when invoking method "resolveColumnAlias()".
- `diesel/QueryParser.java`:N/A — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:N/A — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:769 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/QueryParser.java`:843 — A "NullPointerException" could be thrown; "toUpperCasePreservingQuotedIdentifiers()" can return n...
- `diesel/QueryParser.java`:862 — A "NullPointerException" could be thrown; "innerNormalized" is nullable here.
- `diesel/QueryParser.java`:1277 — A "NullPointerException" could be thrown; "original" is nullable here.
- `diesel/QueryParser.java`:1725 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:1741 — A "NullPointerException" could be thrown; "tableAndJoinsOriginal" is nullable here.
- `diesel/QueryParser.java`:3334 — A "NullPointerException" could be thrown; "normalized" is nullable here.
- `diesel/SelectQuery.java`:2804 — A "NullPointerException" could be thrown; "buildTable" is nullable here.
- `diesel/SqlParsingUtils.java`:55 — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/SubqueryParser.java`:N/A — A "NullPointerException" could be thrown; "unquoted" is nullable here.
- `diesel/Table.java`:N/A — A "NullPointerException" could be thrown; "sequences" is nullable here.

### java:S5786 — Remove this 'public' modifier.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 16

**Problem:** Remove this 'public' modifier.

**Recommendation:** In JUnit5, test classes and methods can be package-private — remove the public modifier.

**Locations:**

- `src/test/java/diesel/AdvancedTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/AliasesTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/GracefulShutdownTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/GroupByTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/InTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/JoinTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/LikeTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/OrderByTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/PerformanceTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/PersistenceTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/QuantitativeTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/ServerConnectionLimitTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/SocketTimeoutTest.java`:N/A — Remove this 'public' modifier.
- `src/test/java/diesel/SubqueriesTest.java`:N/A — Remove this 'public' modifier.

### java:S3358 — Extract this nested ternary operation into an independent statement.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 13

**Problem:** Extract this nested ternary operation into an independent statement.

**Recommendation:** Fix according to rule java:S3358.

**Locations:**

- `diesel/ConditionEvaluator.java`:69 — Extract this nested ternary operation into an independent statement.
- `diesel/ConditionEvaluator.java`:212 — Extract this nested ternary operation into an independent statement.
- `diesel/DeleteQuery.java`:N/A — Extract this nested ternary operation into an independent statement.
- `diesel/DeleteQuery.java`:88 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:N/A — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:354 — Extract this nested ternary operation into an independent statement.
- `diesel/QueryParser.java`:3157 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2258 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2370 — Extract this nested ternary operation into an independent statement.
- `diesel/SelectQuery.java`:2375 — Extract this nested ternary operation into an independent statement.
- `diesel/SubqueryParser.java`:N/A — Extract this nested ternary operation into an independent statement.
- `diesel/SubqueryParser.java`:1669 — Extract this nested ternary operation into an independent statement.
- `diesel/UpdateQuery.java`:N/A — Extract this nested ternary operation into an independent statement.

### java:S127 — Refactor the code in order to not assign to this loop counter from within the loop body.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 11

**Problem:** Refactor the code in order to not assign to this loop counter from within the loop body.

**Recommendation:** Fix according to rule java:S127.

**Locations:**

- `diesel/QueryParser.java`:1111 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:1118 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3044 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3049 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/QueryParser.java`:3055 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SelectQuery.java`:2220 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:89 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SqlLexer.java`:92 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:546 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1559 — Refactor the code in order to not assign to this loop counter from within the loop body.
- `diesel/SubqueryParser.java`:1565 — Refactor the code in order to not assign to this loop counter from within the loop body.

### java:S5857 — Replace this use of a reluctant quantifier with "[^\\)]*+".

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 11

**Problem:** Replace this use of a reluctant quantifier with "[^\\)]*+".

**Recommendation:** Fix according to rule java:S5857.

**Locations:**

- `diesel/QueryParser.java`:1335 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:1336 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:1337 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:1338 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:1339 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:1957 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/QueryParser.java`:3122 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:270 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:811 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:846 — Replace this use of a reluctant quantifier with "[^\\)]*+".
- `diesel/SubqueryParser.java`:1646 — Replace this use of a reluctant quantifier with "[^\\)]*+".

### java:S6213 — Rename this method to not match a restricted identifier.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 9

**Problem:** Rename this method to not match a restricted identifier.

**Recommendation:** Fix according to rule java:S6213.

**Locations:**

- `diesel/QueryProfiler.java`:100 — Rename this method to not match a restricted identifier.
- `src/test/java/diesel/AutoJoinIndexTest.java`:35 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/AutoJoinIndexTest.java`:93 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:170 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/MaxResultRowsTest.java`:189 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:100 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:127 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:131 — Rename this variable to not match a restricted identifier.
- `src/test/java/diesel/OomHandlingTest.java`:141 — Rename this variable to not match a restricted identifier.

### java:S112 — Define and throw a dedicated exception instead of using a generic one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Define and throw a dedicated exception instead of using a generic one.

**Recommendation:** Fix according to rule java:S112.

**Locations:**

- `diesel/Database.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/DatabaseClient.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Table.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Table.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Transaction.java`:N/A — Define and throw a dedicated exception instead of using a generic one.
- `diesel/Transaction.java`:N/A — Define and throw a dedicated exception instead of using a generic one.

### java:S2139 — Either log this exception and handle it, or rethrow it with some contextual information.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Either log this exception and handle it, or rethrow it with some contextual information.

**Recommendation:** Fix according to rule java:S2139.

**Locations:**

- `diesel/Database.java`:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/DatabaseClient.java`:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/DatabaseClient.java`:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/InsertQuery.java`:165 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/QueryParser.java`:N/A — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/QueryParser.java`:783 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/SubqueryParser.java`:86 — Either log this exception and handle it, or rethrow it with some contextual information.
- `diesel/SubqueryParser.java`:941 — Either log this exception and handle it, or rethrow it with some contextual information.

### java:S2447 — Null is returned but a "Boolean" is expected.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Null is returned but a "Boolean" is expected.

**Recommendation:** Fix according to rule java:S2447.

**Locations:**

- `diesel/DeleteQuery.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/DeleteQuery.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/SelectQuery.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/SelectQuery.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/ThreeValuedLogic.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/ThreeValuedLogic.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/UpdateQuery.java`:N/A — Null is returned but a "Boolean" is expected.
- `diesel/UpdateQuery.java`:N/A — Null is returned but a "Boolean" is expected.

### java:S1141 — Extract this nested try block into a separate method.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Extract this nested try block into a separate method.

**Recommendation:** Fix according to rule java:S1141.

**Locations:**

- `diesel/DatabaseClient.java`:163 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:148 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:151 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:156 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:254 — Extract this nested try block into a separate method.
- `diesel/DatabaseServer.java`:274 — Extract this nested try block into a separate method.
- `diesel/QueryParser.java`:60 — Extract this nested try block into a separate method.
- `diesel/QueryParser.java`:2228 — Extract this nested try block into a separate method.

### java:S106 — Replace this use of System.out by a logger.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Replace this use of System.out by a logger.

**Recommendation:** Fix according to rule java:S106.

**Locations:**

- `diesel/DatabaseClient.java`:N/A — Replace this use of System.out by a logger.
- `diesel/DatabaseClient.java`:N/A — Replace this use of System.out by a logger.
- `diesel/DieselDatabase.java`:N/A — Replace this use of System.out by a logger.
- `diesel/DieselDatabase.java`:N/A — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:N/A — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:N/A — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:N/A — Replace this use of System.out by a logger.
- `diesel/SqlLexer.java`:N/A — Replace this use of System.out by a logger.

### java:S6485 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int numMappings)

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Replace this call to the constructor with the better suited static method HashMap.newHashMap(int numMappings)

**Recommendation:** Fix according to rule java:S6485.

**Locations:**

- `diesel/SelectQuery.java`:685 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:1246 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:1252 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:1265 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:1324 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:1503 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:2142 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...
- `diesel/SelectQuery.java`:2157 — Replace this call to the constructor with the better suited static method HashMap.newHashMap(int ...

### java:S1948 — Make "rowIndices" private or transient.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 8

**Problem:** Make "rowIndices" private or transient.

**Recommendation:** Fix according to rule java:S1948.

**Locations:**

- `diesel/BTreeClusteredIndex.java`:22 — Make "keys" transient or serializable.
- `diesel/BTreeClusteredIndex.java`:23 — Make "rowIndices" private or transient.
- `diesel/BTreeClusteredIndex.java`:24 — Make "children" private or transient.
- `diesel/BTreeIndex.java`:21 — Make "keys" transient or serializable.
- `diesel/BTreeIndex.java`:22 — Make "rowIndices" private or transient.
- `diesel/BTreeIndex.java`:23 — Make "children" private or transient.
- `diesel/HashIndex.java`:15 — Make "indexMap" transient or serializable.
- `diesel/UniqueIndex.java`:15 — Make "indexMap" transient or serializable.

### java:S3740 — Provide the parametrized type for this generic.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 6

**Problem:** Provide the parametrized type for this generic.

**Recommendation:** Fix according to rule java:S3740.

**Locations:**

- `diesel/BTreeClusteredIndex.java`:343 — Provide the parametrized type for this generic.
- `diesel/BTreeClusteredIndex.java`:343 — Provide the parametrized type for this generic.
- `diesel/BTreeIndex.java`:286 — Provide the parametrized type for this generic.
- `diesel/BTreeIndex.java`:286 — Provide the parametrized type for this generic.
- `diesel/Table.java`:402 — Provide the parametrized type for this generic.
- `diesel/Table.java`:402 — Provide the parametrized type for this generic.

### java:S1186 — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException or complete the implementation.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 6

**Problem:** Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException or complete the implementation.

**Recommendation:** Fix according to rule java:S1186.

**Locations:**

- `src/test/java/diesel/AutoJoinIndexTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- `src/test/java/diesel/AutoJoinIndexTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- `src/test/java/diesel/MaxResultRowsTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- `src/test/java/diesel/MaxResultRowsTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- `src/test/java/diesel/OomHandlingTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...
- `src/test/java/diesel/OomHandlingTest.java`:N/A — Add a nested comment explaining why this method is empty, throw an UnsupportedOperationException ...

### java:S1066 — Merge this if statement with the enclosing one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Merge this if statement with the enclosing one.

**Recommendation:** Fix according to rule java:S1066.

**Locations:**

- `diesel/BTreeClusteredIndex.java`:332 — Merge this if statement with the enclosing one.
- `diesel/ConditionEvaluator.java`:39 — Merge this if statement with the enclosing one.
- `diesel/SelectQuery.java`:929 — Merge this if statement with the enclosing one.
- `diesel/Table.java`:N/A — Merge this if statement with the enclosing one.
- `diesel/Table.java`:911 — Merge this if statement with the enclosing one.

### java:S1144 — Remove this unused private "resolveRightColumn" method.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Remove this unused private "resolveRightColumn" method.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- `diesel/QueryParser.java`:N/A — Remove this unused private "splitOrderByClause" method.
- `diesel/QueryParser.java`:N/A — Remove this unused private "areSubQueriesEquivalent" method.
- `diesel/QueryParser.java`:N/A — Remove this unused private "parseLimitClause" method.
- `diesel/QueryParser.java`:2862 — Remove this unused private "resolveRightColumn" method.
- `diesel/SelectQuery.java`:382 — Remove this unused private "SelectQuery" constructor.

### java:S1452 — Remove usage of generic wildcard type.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Remove usage of generic wildcard type.

**Recommendation:** Fix according to rule java:S1452.

**Locations:**

- `diesel/ExplainQuery.java`:35 — Remove usage of generic wildcard type.
- `diesel/QueryCache.java`:154 — Remove usage of generic wildcard type.
- `diesel/QueryParser.java`:567 — Remove usage of generic wildcard type.
- `diesel/QueryParser.java`:762 — Remove usage of generic wildcard type.
- `diesel/SubqueryParser.java`:55 — Remove usage of generic wildcard type.

### java:S1068 — Remove this unused "DATE_FORMATTER" private field.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 5

**Problem:** Remove this unused "DATE_FORMATTER" private field.

**Recommendation:** Remove the unused variable/parameter/import (IDE: Optimize Imports / Ctrl+Alt+O).

**Locations:**

- `diesel/DatabaseServer.java`:203 — Remove this unused "socketTimeout" private field.
- `diesel/QueryParser.java`:N/A — Remove this unused "originalQuery" private field.
- `diesel/QueryParser.java`:N/A — Remove this unused "OPERATORS" private field.
- `diesel/SelectQuery.java`:N/A — Remove this unused "subQueries" private field.
- `src/test/java/diesel/LimitOffsetTest.java`:22 — Remove this unused "DATE_FORMATTER" private field.

### java:S5850 — Group parts of the regex together to make the intended operator precedence explicit.

**Severity:** MAJOR | **Type:** BUG | **Found:** 5

**Problem:** Group parts of the regex together to make the intended operator precedence explicit.

**Recommendation:** Fix according to rule java:S5850.

**Locations:**

- `diesel/QueryParser.java`:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/QueryParser.java`:1697 — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/SubqueryParser.java`:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/SubqueryParser.java`:N/A — Group parts of the regex together to make the intended operator precedence explicit.
- `diesel/SubqueryParser.java`:700 — Group parts of the regex together to make the intended operator precedence explicit.

### java:S1168 — Return an empty map instead of null.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Return an empty map instead of null.

**Recommendation:** Fix according to rule java:S1168.

**Locations:**

- `diesel/SelectQuery.java`:1501 — Return an empty map instead of null.
- `diesel/SelectQuery.java`:2059 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2067 — Return an empty collection instead of null.
- `diesel/SelectQuery.java`:2118 — Return an empty collection instead of null.

### java:S6204 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure that the list is unmodified.

**Recommendation:** Fix according to rule java:S6204.

**Locations:**

- `diesel/QueryParser.java`:2053 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- `diesel/SelectQuery.java`:892 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- `diesel/SelectQuery.java`:1779 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...
- `diesel/SelectQuery.java`:1816 — Replace this usage of 'Stream.collect(Collectors.toList())' with 'Stream.toList()' and ensure tha...

### java:S125 — This block of commented-out lines of code should be removed.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** This block of commented-out lines of code should be removed.

**Recommendation:** Fix according to rule java:S125.

**Locations:**

- `diesel/QueryParser.java`:1228 — This block of commented-out lines of code should be removed.
- `diesel/QueryParser.java`:1802 — This block of commented-out lines of code should be removed.
- `diesel/SelectQuery.java`:630 — This block of commented-out lines of code should be removed.
- `src/test/java/diesel/AllTestsSampleTest.java`:N/A — This block of commented-out lines of code should be removed.

### java:S2629 — Use the built-in formatting to construct this argument.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Use the built-in formatting to construct this argument.

**Recommendation:** Fix according to rule java:S2629.

**Locations:**

- `diesel/QueryParser.java`:64 — Use the built-in formatting to construct this argument.
- `diesel/SelectQuery.java`:348 — Use the built-in formatting to construct this argument.
- `diesel/SelectQuery.java`:2050 — Use the built-in formatting to construct this argument.
- `diesel/SelectQuery.java`:2243 — Invoke method(s) only conditionally.

### java:S3626 — Remove this redundant jump.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Remove this redundant jump.

**Recommendation:** Fix according to rule java:S3626.

**Locations:**

- `diesel/QueryParser.java`:2893 — Remove this redundant jump.
- `diesel/QueryParser.java`:2899 — Remove this redundant jump.
- `diesel/SubqueryParser.java`:1408 — Remove this redundant jump.
- `diesel/SubqueryParser.java`:1411 — Remove this redundant jump.

### java:S2589 — Remove this expression which always evaluates to "true"

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Remove this expression which always evaluates to "true"

**Recommendation:** Fix according to rule java:S2589.

**Locations:**

- `diesel/DatabaseClient.java`:N/A — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1781 — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1785 — Remove this expression which always evaluates to "true"
- `diesel/QueryParser.java`:1794 — Remove this expression which always evaluates to "true"

### java:S6395 — Unwrap this unnecessarily grouped subpattern.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 4

**Problem:** Unwrap this unnecessarily grouped subpattern.

**Recommendation:** Fix according to rule java:S6395.

**Locations:**

- `diesel/QueryParser.java`:1697 — Unwrap this unnecessarily grouped subpattern.
- `diesel/QueryParser.java`:1727 — Unwrap this unnecessarily grouped subpattern.
- `diesel/SubqueryParser.java`:700 — Unwrap this unnecessarily grouped subpattern.
- `diesel/SubqueryParser.java`:705 — Unwrap this unnecessarily grouped subpattern.

### java:S1905 — Remove this unnecessary cast to "Comparable".

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Remove this unnecessary cast to "Comparable".

**Recommendation:** Fix according to rule java:S1905.

**Locations:**

- `diesel/BTreeClusteredIndex.java`:345 — Remove this unnecessary cast to "Comparable".
- `diesel/BTreeIndex.java`:288 — Remove this unnecessary cast to "Comparable".
- `diesel/Table.java`:404 — Remove this unnecessary cast to "Comparable".

### java:S899 — Do something with the "boolean" value returned by "hasNext".

**Severity:** MINOR | **Type:** BUG | **Found:** 3

**Problem:** Do something with the "boolean" value returned by "hasNext".

**Recommendation:** Fix according to rule java:S899.

**Locations:**

- `diesel/Database.java`:N/A — Do something with the "boolean" value returned by "delete".
- `diesel/Database.java`:N/A — Do something with the "boolean" value returned by "delete".
- `diesel/SelectQuery.java`:1655 — Do something with the "boolean" value returned by "hasNext".

### java:S6880 — Replace the chain of if/else with a switch expression.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Replace the chain of if/else with a switch expression.

**Recommendation:** Fix according to rule java:S6880.

**Locations:**

- `diesel/ConditionEvaluator.java`:141 — Replace the chain of if/else with a switch expression.
- `diesel/ExplainQuery.java`:76 — Replace the chain of if/else with a switch expression.
- `diesel/ExplainQuery.java`:86 — Replace the chain of if/else with a switch expression.

### java:S2583 — Change this condition so that it does not always evaluate to "true"

**Severity:** MAJOR | **Type:** BUG | **Found:** 3

**Problem:** Change this condition so that it does not always evaluate to "true"

**Recommendation:** Fix according to rule java:S2583.

**Locations:**

- `diesel/SubqueryParser.java`:N/A — Change this condition so that it does not always evaluate to "true"
- `diesel/SubqueryParser.java`:N/A — Change this condition so that it does not always evaluate to "true"
- `diesel/SubqueryParser.java`:N/A — Change this condition so that it does not always evaluate to "true"

### java:S3008 — Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 3

**Problem:** Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

**Recommendation:** Fix according to rule java:S3008.

**Locations:**

- `diesel/SelectQuery.java`:161 — Rename this field "MAX_IN_MEMORY_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.
- `diesel/SelectQuery.java`:171 — Rename this field "MAX_HASH_TABLE_SIZE_BYTES" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.
- `diesel/SelectQuery.java`:182 — Rename this field "MAX_RESULT_ROWS" to match the regular expression '^[a-z][a-zA-Z0-9]*$'.

### java:S6208 — Merge the previous cases into this one using comma-separated label.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Merge the previous cases into this one using comma-separated label.

**Recommendation:** Fix according to rule java:S6208.

**Locations:**

- `diesel/QueryParser.java`:1054 — Merge the previous cases into this one using comma-separated label.
- `diesel/QueryParser.java`:3167 — Merge the previous cases into this one using comma-separated label.

### java:S3824 — Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

**Recommendation:** Fix according to rule java:S3824.

**Locations:**

- `diesel/SelectQuery.java`:1743 — Replace this "Map.containsKey()" with a call to "Map.computeIfAbsent()".
- `diesel/SelectQuery.java`:2418 — Replace this "Map.get()" and condition with a call to "Map.computeIfAbsent()".

### java:S3599 — Use another way to initialize this instance.

**Severity:** MINOR | **Type:** BUG | **Found:** 2

**Problem:** Use another way to initialize this instance.

**Recommendation:** Fix according to rule java:S3599.

**Locations:**

- `diesel/SelectQuery.java`:N/A — Use another way to initialize this instance.
- `diesel/Table.java`:N/A — Use another way to initialize this instance.

### java:S2737 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Add logic to this catch clause or eliminate it and rethrow the exception automatically.

**Recommendation:** Fix according to rule java:S2737.

**Locations:**

- `diesel/InsertQuery.java`:138 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.
- `diesel/QueryParser.java`:2271 — Add logic to this catch clause or eliminate it and rethrow the exception automatically.

### java:S6539 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies on other classes from 24 to the maximum authorized 20 or less.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies on other classes from 24 to the maximum authorized 20 or less.

**Recommendation:** Fix according to rule java:S6539.

**Locations:**

- `diesel/Database.java`:38 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies ...
- `diesel/QueryParser.java`:38 — Split this âMonster Classâ into smaller and more specialized ones to reduce its dependencies ...

### java:S1157 — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

**Recommendation:** Use equalsIgnoreCase() instead of 	oLowerCase()/toUpperCase() + equals().

**Locations:**

- `diesel/SubqueryParser.java`:N/A — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.
- `diesel/SubqueryParser.java`:544 — Replace these toUpperCase()/toLowerCase() and equals() calls with a single equalsIgnoreCase() call.

### java:S1171 — Move the contents of this initializer to a standard constructor or to field initializers.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Move the contents of this initializer to a standard constructor or to field initializers.

**Recommendation:** Fix according to rule java:S1171.

**Locations:**

- `diesel/SelectQuery.java`:N/A — Move the contents of this initializer to a standard constructor or to field initializers.
- `diesel/Table.java`:N/A — Move the contents of this initializer to a standard constructor or to field initializers.

### java:S4042 — Use "java.nio.file.Files#delete" here for better messages on error conditions.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Use "java.nio.file.Files#delete" here for better messages on error conditions.

**Recommendation:** Fix according to rule java:S4042.

**Locations:**

- `diesel/Database.java`:N/A — Use "java.nio.file.Files#delete" here for better messages on error conditions.
- `diesel/Database.java`:N/A — Use "java.nio.file.Files#delete" here for better messages on error conditions.

### java:S2864 — Iterate over the "entrySet" instead of the "keySet".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Iterate over the "entrySet" instead of the "keySet".

**Recommendation:** Fix according to rule java:S2864.

**Locations:**

- `diesel/SelectQuery.java`:N/A — Iterate over the "entrySet" instead of the "keySet".
- `diesel/SelectQuery.java`:901 — Iterate over the "entrySet" instead of the "keySet".

### java:S2293 — Replace the type specification in this constructor call with the diamond operator ("<>").

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 2

**Problem:** Replace the type specification in this constructor call with the diamond operator ("<>").

**Recommendation:** Use diamond operator: 
ew HashMap<>() instead of 
ew HashMap<String, String>().

**Locations:**

- `diesel/Database.java`:122 — Replace the type specification in this constructor call with the diamond operator ("<>").
- `diesel/Table.java`:1120 — Replace the type specification in this constructor call with the diamond operator ("<>").

### java:S5842 — Rework this part of the regex to not match the empty string.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Rework this part of the regex to not match the empty string.

**Recommendation:** Fix according to rule java:S5842.

**Locations:**

- `diesel/SubqueryParser.java`:N/A — Rework this part of the regex to not match the empty string.

### java:S6397 — Replace this character class by the character itself.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Replace this character class by the character itself.

**Recommendation:** Fix according to rule java:S6397.

**Locations:**

- `diesel/SubqueryParser.java`:982 — Replace this character class by the character itself.

### java:S2676 — Use the original value instead.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Use the original value instead.

**Recommendation:** Fix according to rule java:S2676.

**Locations:**

- `diesel/SubqueryParser.java`:491 — Use the original value instead.

### java:S5961 — Refactor this method to reduce the number of assertions from 26 to less than 25.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Refactor this method to reduce the number of assertions from 26 to less than 25.

**Recommendation:** Fix according to rule java:S5961.

**Locations:**

- `src/test/java/diesel/PersistenceTest.java`:N/A — Refactor this method to reduce the number of assertions from 26 to less than 25.

### java:S6548 — A Singleton implementation was detected. Make sure the use of the Singleton pattern is required and the implementation is the right one for the context.

**Severity:** INFO | **Type:** CODE_SMELL | **Found:** 1

**Problem:** A Singleton implementation was detected. Make sure the use of the Singleton pattern is required and the implementation is the right one for the context.

**Recommendation:** Fix according to rule java:S6548.

**Locations:**

- `diesel/QueryProfiler.java`:45 — A Singleton implementation was detected. Make sure the use of the Singleton pattern is required a...

### java:S1488 — Immediately return this expression instead of assigning it to the temporary variable "result".

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Immediately return this expression instead of assigning it to the temporary variable "result".

**Recommendation:** Fix according to rule java:S1488.

**Locations:**

- `diesel/Database.java`:N/A — Immediately return this expression instead of assigning it to the temporary variable "result".

### java:S5785 — Use assertSame instead.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use assertSame instead.

**Recommendation:** Fix according to rule java:S5785.

**Locations:**

- `src/test/java/diesel/PersistenceTest.java`:N/A — Use assertSame instead.

### java:S1118 — Add a private constructor to hide the implicit public one.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Add a private constructor to hide the implicit public one.

**Recommendation:** In JUnit5, test classes and methods can be package-private — remove the public modifier.

**Locations:**

- `diesel/SqlParsingUtils.java`:12 — Add a private constructor to hide the implicit public one.

### java:S5164 — Call "remove()" on "QUERY_MEMORY".

**Severity:** MAJOR | **Type:** BUG | **Found:** 1

**Problem:** Call "remove()" on "QUERY_MEMORY".

**Recommendation:** Fix according to rule java:S5164.

**Locations:**

- `diesel/SelectQuery.java`:275 — Call "remove()" on "QUERY_MEMORY".

### java:S6885 — Use "Math.clamp" instead of "Math.min" or "Math.max".

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use "Math.clamp" instead of "Math.min" or "Math.max".

**Recommendation:** Fix according to rule java:S6885.

**Locations:**

- `diesel/SelectQuery.java`:1427 — Use "Math.clamp" instead of "Math.min" or "Math.max".

### java:S1130 — Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from constructor's body.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from constructor's body.

**Recommendation:** Fix according to rule java:S1130.

**Locations:**

- `diesel/SelectQuery.java`:1589 — Remove the declaration of thrown exception 'java.io.IOException', as it cannot be thrown from con...

### java:S1155 — Use isEmpty() to check whether the collection is empty or not.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Use isEmpty() to check whether the collection is empty or not.

**Recommendation:** Use collection.isEmpty() instead of collection.size() == 0.

**Locations:**

- `diesel/Table.java`:N/A — Use isEmpty() to check whether the collection is empty or not.

### java:S2093 — Change this "try" to a try-with-resources.

**Severity:** CRITICAL | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Change this "try" to a try-with-resources.

**Recommendation:** Fix according to rule java:S2093.

**Locations:**

- `diesel/DatabaseServer.java`:248 — Change this "try" to a try-with-resources.

### java:S2147 — Combine this catch with the one at line 200, which has the same body.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Combine this catch with the one at line 200, which has the same body.

**Recommendation:** Fix according to rule java:S2147.

**Locations:**

- `diesel/Database.java`:203 — Combine this catch with the one at line 200, which has the same body.

### java:S1117 — Rename "autoCommit" which hides the field declared at line 44.

**Severity:** MAJOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Rename "autoCommit" which hides the field declared at line 44.

**Recommendation:** Fix according to rule java:S1117.

**Locations:**

- `diesel/Database.java`:336 — Rename "autoCommit" which hides the field declared at line 44.

### java:S2272 — Add a "NoSuchElementException" for iteration beyond the end of the collection.

**Severity:** MINOR | **Type:** BUG | **Found:** 1

**Problem:** Add a "NoSuchElementException" for iteration beyond the end of the collection.

**Recommendation:** Fix according to rule java:S2272.

**Locations:**

- `diesel/SelectQuery.java`:1647 — Add a "NoSuchElementException" for iteration beyond the end of the collection.

### java:S3400 — Remove this method and declare a constant for this value.

**Severity:** MINOR | **Type:** CODE_SMELL | **Found:** 1

**Problem:** Remove this method and declare a constant for this value.

**Recommendation:** Fix according to rule java:S3400.

**Locations:**

- `diesel/SelectQuery.java`:1021 — Remove this method and declare a constant for this value.


---
*Report generated on 2026-08-23 14:25:48*
