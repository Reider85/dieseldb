package diesel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parallel index scanning (Prompt 84):
 * - Large range scans should produce identical results to sequential scanning
 * - Parallel scan should correctly handle open-ended ranges
 * - Results should be consistent for direct searches
 */
public class ParallelIndexScanTest {

    /**
     * Helper to build a large BTreeIndex with the given number of entries.
     */
    private BTreeIndex buildLargeIndex(int n) {
        List<Object> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            keys.add((long) i);
            indices.add(i);
        }
        BTreeIndex index = new BTreeIndex(Long.class);
        index.bulkLoad(keys, indices);
        return index;
    }

    @Test
    void rangeSearchParallel_matchesSequential_forLargeSet() {
        int n = 20000; // above the parallel threshold
        BTreeIndex index = buildLargeIndex(n);

        List<Integer> sequential = index.rangeSearch(1000L, 5000L);
        List<Integer> parallel = index.rangeSearchParallel(1000L, 5000L);

        assertEquals(sequential, parallel, "Parallel and sequential results should match");
        assertEquals(4001, parallel.size(), "Expected 4001 rows in [1000, 5000]");
    }

    @Test
    void rangeSearchLowParallel_matchesSequential() {
        int n = 20000; // above the parallel threshold
        BTreeIndex index = buildLargeIndex(n);

        List<Integer> sequential = index.rangeSearchLow(15000L);
        List<Integer> parallel = index.rangeSearchLowParallel(15000L);

        assertEquals(sequential, parallel, "Parallel and sequential low-bound results should match");
        assertEquals(5000, parallel.size(), "Expected 5000 rows >= 15000");
    }

    @Test
    void rangeSearchHighParallel_matchesSequential() {
        int n = 20000; // above the parallel threshold
        BTreeIndex index = buildLargeIndex(n);

        List<Integer> sequential = index.rangeSearchHigh(5000L);
        List<Integer> parallel = index.rangeSearchHighParallel(5000L);

        assertEquals(sequential, parallel, "Parallel and sequential high-bound results should match");
        assertEquals(5001, parallel.size(), "Expected 5001 rows <= 5000");
    }

    @Test
    void rangeSearchParallel_smallRange_usesSequentialPath() {
        // Small range below threshold should use sequential path but still work
        int n = 5000; // below the parallel threshold
        BTreeIndex index = buildLargeIndex(n);

        List<Integer> result = index.rangeSearchParallel(100L, 200L);
        assertEquals(101, result.size(), "Expected 101 rows in [100, 200]");

        // Verify the correct row indices
        assertEquals(100, result.get(0), "First row should be index 100");
        assertEquals(200, result.get(100), "Last row should be index 200");
    }

    @Test
    void rangeSearchParallel_nullBounds() {
        int n = 20000;
        BTreeIndex index = buildLargeIndex(n);

        // null low and null high = full scan
        List<Integer> allParallel = index.rangeSearchParallel(null, null);
        assertEquals(n, allParallel.size(), "Full scan should return all rows");
    }

    @Test
    void rangeSearchParallel_emptyRange() {
        int n = 20000;
        BTreeIndex index = buildLargeIndex(n);

        // Range 1000 to 500 - invalid (high < low), should return empty
        List<Integer> result = index.rangeSearchParallel(5000L, 1000L);
        assertTrue(result.isEmpty(), "Invalid range should return empty");
    }

    @Test
    void rangeSearchParallel_boundaryValues() {
        int n = 20000;
        BTreeIndex index = buildLargeIndex(n);

        // Exact boundary - key 9999
        List<Integer> result = index.rangeSearchParallel(9999L, 9999L);
        assertEquals(1, result.size(), "Exact boundary should return 1 row");
        assertEquals(9999, result.get(0), "Row index should match the key");
    }

    @Test
    void parallelScan_resultsAreOrdered() {
        int n = 20000;
        BTreeIndex index = buildLargeIndex(n);

        // Check that results from parallel scan are in ascending order
        List<Integer> result = index.rangeSearchParallel(500L, 15000L);
        for (int i = 1; i < result.size(); i++) {
            assertTrue(result.get(i) > result.get(i - 1),
                    "Results should be in ascending order at position " + i);
        }
    }
}
