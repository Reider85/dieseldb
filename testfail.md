# Failed test runs

This file records each failed test run and the analysis of why it happened.
Fixes are applied in `QueryParser` / `SubqueryParser` until the whole test
suite in `test/diesel/` passes.

## Run 3 (after prompt 42 implementation)

Date: 2026-08-05

Failing tests: `diesel.AllTestsSampleTest` (12 failed) and
`diesel.QuantitativeTest` (12 failed).

### Error

```
SEVERE: Query execution failed: Cannot invoke "diesel.Database.getTable(String)"
because "database" is null
FAIL: AliasesTest / complex select min max avg with join and group by ...
FAIL: AliasesTest / complex select with multiple inner joins ...
FAIL: GroupByTest / complex group by join string date ...
FAIL: JoinTest / simple inner join on primary key ...
FAIL: JoinTest / simple inner join on non indexed field ...
FAIL: JoinTest / complex full join on primary key ...
FAIL: JoinTest / complex inner join with and or in on ...
FAIL: OrderByTest / complex join order by primary key ...
FAIL: OrderByTest / complex join order by non indexed ...
FAIL: SubqueriesTest / simple subquery in in clause ...
FAIL: SubqueriesTest / complex subquery in column where group by having ...
FAIL: SubqueriesTest / complex subquery in column inner join on ...
```

All 12 failures are SELECT queries with JOINs / subqueries, which call
`table.getDatabase()` (`SelectQuery.java:77` and `:680`) to resolve other
tables. The `database` reference was `null`.

### Analysis

Prompt 42 wraps auto-commit DML (INSERT / UPDATE / DELETE) in an implicit
transaction: begin (snapshot), execute, commit. The snapshot and the commit
rely on `Transaction.cloneTable` (`diesel/Transaction.java`), which deep-copies
a `Table` by Java serialization.

`Table.database` is declared `transient` (`Table.java:37`), so after
serialization + deserialization the clone has `database = null`.
`Table.readObject` (`Table.java:273`) rebuilds `indexes`, `sequences`,
`clusteredIndex`, `rowLocks` from non-transient metadata, but it does NOT
restore `database`. Only `Table.loadFromFile` re-attaches it afterwards
(`Table.java:591`).

The implicit-transaction commit put such a deserialized clone back into
`Database.tables`, so the next JOIN/subquery on that table observed
`getDatabase() == null` and threw the NPE.

### Fix

- `Table.java`: add `public void attachDatabase(Database database)` so the
  reference can be restored after cloning.
- `Transaction.cloneTable`: capture `table.getDatabase()` before serialization
  and re-attach it to the clone after deserialization. This fixes both the
  implicit auto-commit path (prompt 42) and the pre-existing latent bug for
  explicit `BEGIN`/`COMMIT` transactions, which also commit cloned tables.

## Run 4 (timing regression after prompt 42, fixed)

Date: 2026-08-05

Tests passed (`AllTestsSampleTest` 62/0, `QuantitativeTest` 60/0), but the
per-query DML timings grew 2-6x vs the baseline (`timing7.md`/`timing2.md`) and
the whole run slowed from ~20 s to ~125 s per test class:

- `timing9.md` (after the NPE fix): `insert alice` 14.93 ms (baseline ~4-6 ms),
  `update set null` 12.21 ms (baseline ~2 ms), full run 125.1 s / 121.5 s.

### Analysis

The implicit auto-commit transaction performed a full deep-clone of the table
per DML statement (`Transaction.cloneTable`, Java serialization) and wrote the
serialized `.table` file on every DML. With ~2400 setup INSERTs on growing
tables this became O(n^2)-ish and dominated the run time.

### Fix

- `Transaction.java`: add `registerModifiedTable(String, Table)` that stores the
  working table reference without deep-cloning.
- `Database.executeQuery`: the implicit auto-commit path now executes the DML on
  the live table, registers it, and persists only the CSV (`saveToFile`).
  A single-statement implicit transaction needs no snapshot isolation, and the
  serialized `.table` file is written by the explicit tests / `saveTablesToDisk`
  as before.

### Result (`timing11.md`)

- `AllTestsSampleTest`: 18.05 s, `QuantitativeTest`: 14.79 s, total 42.7 s.
- `insert alice` 5.06 ms (baseline 4.01), `insert flag true` 1.92 ms (baseline
  1.55), `update set null` 2.40 ms (baseline 2.01). All values within normal
  run-to-run variation vs `timing7.md`.

## Run 1 (before fixes)

Date: 2026-08-02

Failing test: `diesel.PerformanceTest`

Failing query:
`SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE`

Error:
```
SEVERE: Unknown column: USERS.TRUE, available columns: [USERSCORE, SCORE, ACTIVE, INITIAL, RANK, BIRTHDATE, LEVEL, BALANCE, NAME, PRECISION, LASTACTION, USER_CODE, ID, SESSION_ID, LASTLOGIN, AGE]
SEVERE: Failed to parse query: SELECT NAME, AGE, BALANCE FROM USERS WHERE AGE < 30 AND ACTIVE = TRUE, Error: Unknown column: USERS.TRUE
Exception in thread "main" java.lang.IllegalArgumentException: Unknown column: USERS.TRUE
```

### Analysis

The SQL boolean literals `TRUE` / `FALSE` are not recognized as literals in
`WHERE` conditions. In `QueryParser.parseComparisonCondition`
(`diesel/QueryParser.java`) the right-hand side of a comparison such as
`ACTIVE = TRUE` was matched by the generic "column name" pattern first, so
`TRUE` was treated as a column reference (`USERS.TRUE`) instead of a boolean
literal value, and column validation failed with `Unknown column: USERS.TRUE`.

The same defect exists in `SubqueryParser.parseSingleCondition`
(`diesel/SubqueryParser.java`).

This is a regression against prompt 4 from `prompt.md` (support `TRUE`,
`FALSE`, `NULL` literals; the identifier on the left must not be confused with
the literal on the right).

Additionally, `PerformanceTest` uses the `NOT` prefix (`NOT AGE = 30`,
`NOT ACTIVE = FALSE`). `NOT` was not handled by the condition tokenizer, so
those queries would fail as well once the literal issue was fixed.

### Fix

- `QueryParser.parseComparisonCondition`: recognize `TRUE`, `FALSE`, `NULL`
  as literals before falling back to column-name parsing. `TRUE`/`FALSE` are
  parsed as `Boolean` (validated against the column type); `NULL` becomes a
  `null` value.
- `QueryParser.tokenizeConditions` / `QueryParser.parseTokenizedConditions`:
  add `NOT` keyword token support that sets the negation flag for the next
  condition.
- `SubqueryParser.parseSingleCondition`: same literal handling as above.
- `SubqueryParser.tokenizeConditions` / `parseTokenizedConditions`: same `NOT`
  keyword support.

## Run 2 (after fixes + prompt 4 implementation)

Date: 2026-08-02

All tests in `test/diesel/` passed on the first attempt after implementing
prompt 4.

### Prompt 4 implementation summary

Prompt 4 from `prompt.md`: "������ ��������� ��������� `TRUE`, `FALSE`, `NULL`
� ������ � ������. � AST ��� ������ �������������� ��� ����������� ���������.
�������, ��� � ������� `WHERE flag = TRUE` ������������� `flag` ��
���������������� ��� �������."

Changes made:

- `SqlLexer` (`diesel/SqlLexer.java`):
  - Added `LITERAL` token type to `TokenType` enum.
  - Added `LITERALS` set containing `TRUE`, `FALSE`, `NULL`.
  - Removed `TRUE`, `FALSE`, `NULL` from `KEYWORDS` set.
  - Updated `tokenize()` to emit `LITERAL` tokens for `TRUE`/`FALSE`/`NULL`
    (case-insensitive), keeping them distinct from `KEYWORD` tokens so the
    statement-type detection in `parseWithLexer` is unaffected.

- `QueryParser` (`diesel/QueryParser.java`):
  - `parseComparisonCondition`: recognizes `TRUE`, `FALSE`, `NULL` as literal
    values before falling back to column-name parsing. `TRUE`/`FALSE` are
    parsed as `Boolean` (validated against the column type); `NULL` becomes a
    `null` value. The left-hand identifier (e.g. `flag` in `flag = TRUE`) is
    correctly treated as a column reference, not a literal.
  - `parseConditionValue`: handles `NULL` literal (returns `null`) and
    `TRUE`/`FALSE` (returns `Boolean`, validated against column type).
  - `tokenizeConditions` / `parseTokenizedConditions`: `NOT` keyword token
    support sets the negation flag for the next condition.

- `SubqueryParser` (`diesel/SubqueryParser.java`):
  - `parseSingleCondition`: same literal handling as `QueryParser`.
  - `tokenizeConditions` / `parseTokenizedConditions`: same `NOT` keyword
    support.

### Test results

| Test                  | Result |
|-----------------------|--------|
| PerformanceTest       | PASS   |
| AdvancedTest          | PASS   |
| AliasesTest           | PASS   |
| GroupByTest           | PASS   |
| InTest                | PASS   |
| JoinTest              | PASS   |
| LikeTest              | PASS   |
| OrderByTest           | PASS   |
| PersistenceTest       | PASS   |
| SubqueriesTest        | PASS   |

All 10 tests passed. No further attempts needed.
