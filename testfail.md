# Failed test runs

This file records each failed test run and the analysis of why it happened.
Fixes are applied in `QueryParser` / `SubqueryParser` until the whole test
suite in `test/diesel/` passes.

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

Prompt 4 from `prompt.md`: "Добавь поддержку литералов `TRUE`, `FALSE`, `NULL`
в лексер и парсер. В AST они должны представляться как специальные константы.
Проверь, что в условии `WHERE flag = TRUE` идентификатор `flag` не
интерпретируется как литерал."

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
