# Plan: Investigate and Fix Test Performance Regression After 2.8.0

## Problem Statement
Test execution time with `-Ptest` profile (full 351-test suite) has "multiply increased" (многократно возросло) after version 2.8.0. The `-Ptest` profile runs all `**/*Test.java` tests.

## Root Cause Analysis

### Key Changes in 2.8.x

| Version | Commit | Changes |
|---------|--------|---------|
| 2.8.1 | 2c27ad3 (2.7.63) | OR join optimizations: `hasOrInOnConditions`, `flattenInto`, short-circuit conditions, `reorderJoinsForNestedLoop` |
| **2.8.2** | **cf1b1dc** | **Streaming cross-join with spill-to-disk**: `MAX_IN_MEMORY_ROWS=10000`, `StreamingResultIterator`, `externalSort`, re-enabled 2 skipped ORDER BY tests in AllTestsSampleTest |

### The Performance Killer: 2.8.2 Streaming Implementation

**`MAX_IN_MEMORY_ROWS = 10000`** (from `config.properties`) is the threshold. When estimated result rows exceed this, the engine switches to streaming + spill-to-disk.

**With RECORD_COUNT=600 (default/test profile):**
- OR-in-ON joins estimate `600 × 600 = 360,000` rows
- **360,000 > 10,000 → streaming triggered for ALL such queries**

**Tests Affected:**

| Test Class | Tests | Why Streaming Triggered |
|------------|-------|------------------------|
| **OrderByTest** | 5 tests | OR-in-ON joins + ORDER BY → externalSort + spill |
| **JoinTest** | ~24 tests | OR-in-ON joins (no ORDER BY) → streaming join |
| **AllTestsSampleTest** | 2 tests (re-enabled) | OR-in-ON joins + ORDER BY → externalSort + spill |
| **QuantitativeTest** | 2 tests | Same as AllTestsSampleTest |

**Total: ~33 tests spill to disk** in the -Ptest profile.

### Why This Is Catastrophically Slow

1. **Spill-to-disk I/O**: 360K rows written to temp files, read back for sort/merge
2. **External sort overhead**: Chunking → sorting chunks → k-way merge
3. **FINEST logging**: `config.properties` has `java.util.logging.ConsoleHandler.level = FINEST` + `logging.level.diesel=ALL` → massive log output during 360K row operations
4. **Multiple test classes**: Each test class creates fresh Database, re-triggers streaming

### Evidence
- OrderByTest alone took **>5 minutes** (timeout at 300s) in quick test
- AllTestsSampleTest has 2 re-enabled tests that were explicitly skipped before due to OOM
- QuantitativeTest runs the same 2 heavy queries
- JoinTest has 24 OR-in-ON join tests now streaming

## Possible Solutions

### Option A: Increase Threshold for Test Runs (Recommended)
Set `max.inmemory.rows` high enough (e.g., 500,000) so 360K-row tests run **in-memory** (fast) instead of spill-to-disk (slow).

**Implementation:** 
- Add system property override in `pom.xml` for test profiles: `-Dmax.inmemory.rows=500000`
- Modify `SelectQuery.java` to check system property first, then config.properties

### Option B: Disable Heavy Tests in -Ptest
- Move the 9 heaviest tests (5 OrderByTest + 2 AllTestsSampleTest + 2 QuantitativeTest) to a separate `@Slow` category or profile
- Keep -Ptest fast for regular development

### Option C: Smart Streaming Heuristic
Modify `shouldUseStreaming()` to consider:
- Whether ORDER BY is present (externalSort is the real killer)
- Actual vs estimated row count
- Available heap

### Option D: Reduce RECORD_COUNT for Specific Tests
Run the heavy OR-join tests with smaller `RECORD_COUNT` (e.g., 50) in test profiles.

## Validation Plan

1. **Baseline**: Measure current -Ptest time (expected >20 min based on OrderByTest >5 min alone)
2. **Apply fix**: Implement Option A (threshold increase via system property)
3. **Verify**: -Ptest completes in <5 min (target: similar to pre-2.8.0)
4. **Regression check**: Run -Ptest-light (already fast), default profile (2 tests)

## Open Questions

1. **What was the -Ptest time before 2.8.0?** Need historical baseline for comparison.
2. **Is OOM risk acceptable with 500K threshold?** 360K rows × ~200 bytes/row = ~72 MB per test, well within 4GB heap.
3. **Should JoinTest OR-in-ON tests also be optimized?** They don't have ORDER BY but still stream (24 tests).

## Recommended First Step

**Implement Option A** — add `-Dmax.inmemory.rows=500000` to test profiles in `pom.xml` and update `SelectQuery.java` to read system property. This is minimal, safe (heap allows it), and directly addresses the spill-to-disk bottleneck.

Would also recommend temporarily reducing logging level for test runs (`-Djava.util.logging.ConsoleHandler.level=WARNING` in test profiles) to eliminate log I/O overhead.