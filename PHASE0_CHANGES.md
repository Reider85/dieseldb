# Phase 0 Changes Summary

This document describes all features and improvements implemented in Phase 0 of DieselDB.

## 1. SQL Parsing (SqlLexer + QueryParser)

- **SqlLexer** (`diesel/SqlLexer.java`): Tokenizes SQL into keywords (case-insensitive), identifiers (including double-quoted preserving case), string literals, numbers, operators, punctuation.
- **QueryParser** (`diesel/QueryParser.java`): Builds AST from tokens; supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX (BTREE, HASH, UNIQUE, CLUSTERED), BEGIN/COMMIT/ROLLBACK, SET AUTOCOMMIT.
- **SubqueryParser** (`diesel/SubqueryParser.java`): Parses nested subqueries in WHERE, IN, ON, SELECT list.
- **Quoted identifiers**: Double-quoted identifiers (`"MyColumn"`) preserve case; unquoted are normalized to upper case.
- **String literals**: Single-quoted (`'value'`) with proper escaping.
- **Boolean/NULL literals**: TRUE, FALSE, NULL parsed as special constants.

## 2. Case Insensitivity

- Keywords (SELECT, FROM, WHERE, etc.) recognized case-insensitively via `toUpperCase()`.
- Table/column names without quotes normalized to upper case.
- String literal values compared case-sensitively (e.g., `'John'` ≠ `'JOHN'`).

## 3. WHERE Clause & Filtering

- Comparison operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`.
- Logical operators: `AND`, `OR`, `NOT` with parentheses grouping.
- **Three-valued logic (NULL semantics)**:
  - Any comparison with NULL yields `UNKNOWN`.
  - `UNKNOWN` in WHERE filters out the row (treated as FALSE).
  - `IS NULL` / `IS NOT NULL` return TRUE/FALSE regardless of three-valued logic.
  - Truth tables for AND/OR with UNKNOWN implemented in `diesel/ThreeValuedLogic.java`.
  - Short-circuit evaluation: OR stops at first TRUE, AND stops at first FALSE.

## 4. IN Operator

- Value lists: `WHERE col IN (1, 2, 3)`.
- Subqueries: `WHERE col IN (SELECT ...)` with result caching per query execution.
- Works with strings, numbers, NULL.

## 5. JOIN Support

- Join types: INNER, LEFT, RIGHT, FULL, CROSS.
- ON conditions with AND/OR, parentheses, subqueries, IS NULL.
- Hash join optimization (disabled when ON contains OR to preserve cross-product semantics).
- Join order preserves main table as first (not last).

## 6. GROUP BY & Aggregates

- `GROUP BY col1, col2, ...` creates one group per distinct key combination.
- Aggregate functions: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX.
- NULL values skipped in SUM/AVG/MIN/MAX/COUNT(col); COUNT(*) counts all rows.
- HAVING clause with aggregate conditions (e.g., `HAVING COUNT(*) > 1`).

## 7. ORDER BY & LIMIT/OFFSET

- `ORDER BY col ASC|DESC` with multiple columns.
- `LIMIT n` and `OFFSET m` applied after filtering/grouping/ordering.
- Works with subqueries.

## 8. Subqueries

- Scalar subqueries in WHERE/SELECT: `WHERE age > (SELECT AVG(age) ...)`.
- IN subqueries with caching.
- Subqueries in ON conditions.
- Correlated and non-correlated subqueries supported.

## 9. Transactions

- **Auto-commit** (default ON): each DML (INSERT/UPDATE/DELETE) auto-commits.
- **Explicit transactions**: `BEGIN` / `START TRANSACTION` [ISOLATION LEVEL ...] sets auto-commit OFF.
- **COMMIT / ROLLBACK** end transaction; auto-commit stays OFF (PostgreSQL behavior).
- **SET AUTOCOMMIT = ON|OFF** (SESSION optional) toggles mode.
- **Isolation levels**: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE.
- SELECT never affects transaction state (read-only).

## 10. Indexes

- **BTreeIndex**: ordered, supports range scans.
- **BTreeClusteredIndex**: clustered (data stored in index order).
- **HashIndex**: equality lookups.
- **UniqueIndex / UniqueClusteredIndex**: enforce uniqueness.
- All indexes serializable with `serialVersionUID`.

## 11. Persistence & Serialization

- Tables saved as `.table` (serialized) + `.csv` (data).
- `serialVersionUID` on all Serializable classes: Table, BTreeIndex, BTreeClusteredIndex, HashIndex, UniqueIndex, Sequence, QueryMessage, indexes' Node classes.
- `formatVersion` field (initial 1) with version check on load.
- `PERSISTENCE_README.md` documents serialization requirements.

## 12. Server & Networking

- **DatabaseServer** (`diesel/DatabaseServer.java`): TCP server on port 3306 (configurable).
- **ClientHandler**: per-connection handler with `socket.setSoTimeout(30000)` (30s idle timeout, configurable via `config.properties`).
- **ThreadPoolExecutor**: fixed pool of 100 threads, bounded queue (100), `AbortPolicy` rejects excess connections.
- **Graceful shutdown**: `Runtime.addShutdownHook` closes ServerSocket, interrupts worker threads, waits 2s, then `shutdownNow()`.
- **DatabaseClient** (`diesel/DatabaseClient.java`): ObjectInputStream/ObjectOutputStream protocol for queries.

## 13. Logging (SLF4J + Logback)

- Dependencies: `slf4j-api:2.0.12`, `logback-classic:1.5.6`.
- `src/main/resources/logback.xml`: console + rolling file (`logs/diesel.log`, 10 MB max, 30 days, 100 MB total).
- Logger `diesel` at DEBUG level.
- Replaced all `System.out.println` / `System.err.println` / `java.util.logging` with SLF4J (`LoggerFactory.getLogger`).

## 14. Build & CI

- **pom.xml**: `com.dieseldb:dieseldb:0.5.0-SNAPSHOT`, Java 17, JUnit Jupiter 5.10.0, maven-surefire-plugin.
- **Maven profile `test`**: default `mvn test` runs only `AllTestsSampleTest` + `QuantitativeTest`; full suite with `mvn -Ptest test`.
- **CI** (`.github/workflows/ci.yml`): triggers on push/PR to main, sets up Temurin JDK 17, runs `mvn test`.

## 15. Test Coverage

- **AllTestsSampleTest**: 199 checks covering all SQL features, records timing per query.
- **QuantitativeTest**: validates exact row counts for every query in AllTestsSampleTest.
- **Phase0IntegrationTest**: JUnit 5 isolation test with temp directory.
- **SocketTimeoutTest**: verifies 30s idle timeout closes connection.
- **GracefulShutdownTest**: launches server process, SIGTERM, verifies clean exit + saved files.
- **ServerConnectionLimitTest**: verifies 101st connection rejected.
- **Other tests**: JoinTest, GroupByTest, InTest, LikeTest, OrderByTest, SubqueriesTest, AliasesTest, AdvancedTest, PersistenceTest, PerformanceTest, DatabaseSmokeTest.

## 16. Documentation

- **README.md**: multilingual overview, build instructions (Maven, JDK 17, commands).
- **PERSISTENCE_README.md**: serialization rules, format version.
- **Changelog.md**: detailed version history.
- **PHASE0_CHANGES.md**: this file.

## 17. Performance & Timing

- Timing reports stored in `timing/` directory (timing.md, timing1.md...).
- Baseline `timing/timing53.md`: 138 queries, 0 FAIL.
- Latest runs within ±20% of baseline (environmental noise on micro-queries).
- Subquery IN caching reduced non-correlated IN subquery from ~1250ms to ~200ms.
- **Benchmark report**: `PerformanceTest` writes `benchmark_report.md` with average/min/max/stddev timings (ms) for INSERT, UPDATE, TRANSACTION, READ_UNCOMMITTED, TRUE-condition and SELECT queries (also logged via SLF4J).

## Version
Phase 0 complete at version **2.7.57** (see Changelog.md).