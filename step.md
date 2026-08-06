# Step log with action timings

Record of the main actions for prompts 41-42 and their measured timings.

## Prompt 41: `autoCommit` flag

- Implemented `private boolean autoCommit = true` in `diesel/Database.java` plus
  `isAutoCommit()` / `setAutoCommit(boolean)`.
- No separate runtime measurement; covered by subsequent runs.

## Prompt 42: auto-commit DML

- `Database.executeQuery`: when `autoCommit == true` and the statement is DML
  (INSERT / UPDATE / DELETE) with no active transaction, an implicit transaction
  is started, the DML is executed, and the change is committed immediately
  (persisted to CSV).

## Verification runs

| # | Action | Result | Time |
|---|--------|--------|------|
| 1 | Run after prompt 42 (before NPE fix) | 12 failed in both test classes (NPE: `database is null`) | n/a |
| 2 | Fix NPE (`Table.attachDatabase` + `Transaction.cloneTable` restore) | BUILD SUCCESS, 62/0 + 60/0 | 266759 ms total |
| 3 | Optimization 1: drop per-DML snapshot clone (`registerModifiedTable`) | BUILD SUCCESS, 62/0 + 60/0 | 237061 ms total |
| 4 | Optimization 2: drop per-DML serialized-file write in implicit path | BUILD SUCCESS, 62/0 + 60/0 | 42696 ms total |
| 5 | Final clean compile | BUILD SUCCESS | 5936 ms |
| 6 | Final `mvn test` run | BUILD SUCCESS, `AllTestsSampleTest` 62 passed / 0 failed | 18.05 s |
| 7 | Final `mvn test` run | BUILD SUCCESS, `QuantitativeTest` 60 passed / 0 failed | 14.79 s |

Final full `mvn test` wall clock: ~42.7 s.

## Prompt 42 re-verification run (2026-08-06)

- Prompt 42 was already in place (`Database.executeQuery`, auto-commit DML
  path); verified and no engine changes were needed.
- Ran `mvn test` (`AllTestsSampleTest` + `QuantitativeTest`).

### Run A (11:57 MSK) - broken

Commit 2.7.18 had reduced `RECORD_COUNT` (600 -> 10 in `AllTestsSampleTest`,
600 -> 60 in `QuantitativeTest`) but the expected row counts were calibrated
for 600 rows. Result: `AllTestsSampleTest` 4 failed, `QuantitativeTest`
15 failed. Cause recorded in `testfail.md` (Run 5).

### Fix

Restored `RECORD_COUNT = 600` in both test classes.

### Run B (11:59 MSK) - pass

- `AllTestsSampleTest` 62/0, `QuantitativeTest` 60/0, total 55.1 s.
- Timing report `timing13.md` was a noisy run (clustered-index lookup 29 ms,
  inserts ~3x baseline).
- Re-run (12:0x) wrote `timing14.md`; values are within normal variation vs
  baseline `timing.md` and previous good run `timing11.md`.

## Timing reports

- `timing9.md` - after NPE fix (DML 2-6x slower, run 125.1 s / 121.5 s).
- `timing10.md` - after optimization 1 (still 116.1 s / 113.5 s).
- `timing11.md` - final (18.05 s / 14.79 s), all values within baseline of
  `timing7.md`.
- `timing12.md` - broken 10-row run (before RECORD_COUNT fix).
- `timing13.md` - noisy first re-run at 600 rows.
- `timing14.md` - verified re-run at 600 rows; within baseline.

## Prompt 55: OR truth table with UNKNOWN

- New shared class `diesel/ThreeValuedLogic.java` with `and` / `or` / `not` /
  `isTrue` and the short-circuit helpers `orIsDetermined` / `andIsDetermined`.
  `SelectQuery`, `UpdateQuery` and `DeleteQuery` now call it instead of keeping
  three private copies of `and3vl` / `or3vl` / `not3vl`.
- OR truth table: `TRUE OR UNKNOWN = TRUE`, `FALSE OR UNKNOWN = UNKNOWN`,
  `UNKNOWN OR UNKNOWN = UNKNOWN`; only `TRUE` keeps a row.
- WHERE evaluation short-circuits: the remaining OR operands are skipped once
  the accumulated value is `TRUE`, the remaining AND operands once it is
  `FALSE`, so the NULL handling adds no per-row cost.
- IN subquery caching: `SelectQuery` caches IN subquery results per query
  execution, keyed by the SQL after outer-reference substitution, so a
  non-correlated `WHERE ID IN (SELECT ...)` executes its subquery once instead
  of once per scanned row.
- Tests: 12 new checks in `AllTestsSampleTest` and `QuantitativeTest`
  (SELECT / UPDATE / DELETE with `OR` + `NULL`).

| # | Action | Result | Time |
|---|--------|--------|------|
| 1 | baseline `mvn test` (no profile) | 106 OK / 0 FAIL | `timing48.md` |
| 2 | `mvn test` after 3VL refactor | 118 OK / 0 FAIL | `timing49.md` |
| 3 | `mvn test` after IN subquery cache | 93 queries, 0 FAIL | `timing50.md` |

- No degradation vs `timing.md`: per-query values stay in the same range and the
  heaviest case, `SubqueriesTest / simple subquery in in clause`, improved from
  1250 ms (`timing.md`) to 206 ms (`timing50.md`).
