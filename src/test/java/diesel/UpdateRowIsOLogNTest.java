package diesel;

import diesel.storage.CsvIndexManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 CSV-3 (shared CSV+TSV): {@code updateRow} is O(log n + m) per call, not
 * O(n + k). The dominant O(n) cost was {@code positionToRowId}'s entry-set
 * scan (o2 CSV-1 made it an O(1) inverse-map get) and the O(k) posting-list
 * removal was the {@code ArrayList.remove(Object)} scan (o2 CSV-6 made it an
 * O(1) LinkedHashSet remove), so a row-loop driven batch of updates is now
 * O(batch · log n) instead of O(n · batch). This class is the acceptance
 * guard keeping updateRow on that path.
 *
 * <p>The prod body of {@code DelimitedIndexManager.updateRow} was never
 * touched (the fix fell out of CSV-1/CSV-6 for free), so the prompt-named
 * UpdateRowIsOLogNTest is delivered as pure acceptance, following the
 * CreateIndexIsONLogNTest (CSV-2) precedent. The manager under test is the
 * format-agnostic DelimitedIndexManager via its only concrete CSV
 * instantiation (TsvIndexManager: 7-line subclass, zero overrides —
 * identical shared code path for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class UpdateRowIsOLogNTest {

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("AGE", Integer.class);
        return t;
    }

    private static Map<String, Object> row(long id, String name, int age) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("AGE", age);
        return r;
    }

    private static Object[] array(long id, String name, int age) {
        return new Object[] {id, name, age};
    }

    private static CsvIndexManager manager(int rowCount, String pk, IntFunction<Integer> ageOf) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, ageOf.apply(i)));
        }
        m.buildIndexes(rows, pk);
        return m;
    }

    /**
     * Rewrites the AGE of the first {@code updates} positions from i%10 to
     * (i+1)%10. Each call resolves the rowId via positionToRowId (O(1)
     * after CSV-1) and swaps the secondary-index membership (O(1) remove +
     * O(log c) insert per index after CSV-6).
     */
    private static long timeAgeShiftBatch(CsvIndexManager m, int updates) {
        long start = System.nanoTime();
        for (int i = 0; i < updates; i++) {
            int pos = i;
            int oldAge = pos % 10;
            int newAge = (pos + 1) % 10;
            m.updateRow(array(pos, "n" + pos, oldAge), pos, array(pos, "n" + pos, newAge));
        }
        return System.nanoTime() - start;
    }

    // ─── O(batch · log n) structural proof: N-independence ──────────

    /**
     * The prompt's acceptance criterion: a batch of 10³ updates over a
     * 10⁵-row table costs O(10³ · log n), not O(10⁵ · 10³). Identical
     * update loops on an N=1k and an N=100k fixture must take
     * approximately the same time (only the log factor differs); the old
     * per-call O(n) positionToRowId scan made the big/small ratio ~100x.
     * The ratio is load-immune (both measurements degrade equally under
     * CPU contention — DirtyPageTrackingIsODTest bound style) with an
     * absolute wall bound as secondary catastrophic-regression guard.
     * Unmeasured JIT warmup first (EvictionUnderPinnedHeadTest precedent).
     */
    @Test
    @Timeout(60)
    void batchUpdatesAreIndependentOfTableSize() {
        final int batch = 1_000;
        final double maxRatio = 10.0;
        final long absoluteBoundMillis = 2_000;

        // Warmup: hot-compile the update path so neither measurement pays
        // the JIT compilation (order would otherwise bias the ratio).
        CsvIndexManager warm = manager(10_000, "ID", i -> i % 10);
        warm.createIndex("AGE");
        timeAgeShiftBatch(warm, 500);

        CsvIndexManager small = manager(1_000, "ID", i -> i % 10);
        small.createIndex("AGE");
        CsvIndexManager big = manager(100_000, "ID", i -> i % 10);
        big.createIndex("AGE");

        // Small first: identical call pattern, both with hot code.
        long smallNanos = timeAgeShiftBatch(small, batch);
        long bigNanos = timeAgeShiftBatch(big, batch);

        long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);

        assertTrue(ratio <= maxRatio,
                "updateRow batch cost must not scale with table size: big(N=100k)="
                        + bigMillis + " ms vs small(N=1k)=" + smallMillis
                        + " ms, ratio=" + String.format(java.util.Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 CSV-3 O(log n) updateRow)");
        assertTrue(bigMillis < absoluteBoundMillis,
                batch + " updateRow calls over a 100k-row table took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis
                        + " ms (O(batch · log n), o2 CSV-3)");

        // The updates landed: position 0 (AGE 0 -> 1) and position 9
        // (AGE 9 -> 0) joined their new posting lists; PK resolution is
        // untouched by the churn.
        assertTrue(big.search("AGE", 1).contains(0), "position 0 now carries AGE=1");
        assertTrue(big.search("AGE", 0).contains(9), "position 9 now carries AGE=0");
        assertEquals(List.of(0), big.searchByPrimaryKey(0L));
        assertEquals(List.of(999), big.searchByPrimaryKey(999L));
    }

    // ─── updateRow correctness under large-k posting lists ──────────

    /**
     * updateRow must keep every maintained index atomically consistent:
     * disassociates the old keys on both secondary indexes (large k — the
     * CSV-6 O(1) remove over ~500-member posting lists), joins the new
     * ones, and drops vacated unique keys. RANGE and PK lookups must match
     * a freshly computed expectation after the full churn.
     */
    @Test
    void updateRowChurnKeepsEveryMaintainedIndexCorrect() {
        final int rowCount = 5_000;
        final int distinctAges = 10;
        CsvIndexManager m = manager(rowCount, "ID", i -> i); // unique AGE and NAME
        assertTrue(m.createIndex("AGE"));
        assertTrue(m.createIndex("NAME"));

        // Churn: AGE i -> i%10 and NAME n<i> -> "shared" + (i%10) so both
        // indexes collapse from unique keys to 10 low-cardinality keys.
        for (int i = 0; i < rowCount; i++) {
            m.updateRow(array(i, "n" + i, i), i, array(i, "shared" + (i % distinctAges), i % distinctAges));
        }

        for (int age = 0; age < distinctAges; age++) {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < rowCount; i++) {
                if (i % distinctAges == age) {
                    expected.add(i);
                }
            }
            assertEquals(expected, m.search("AGE", age), "maintained AGE index for key " + age);
            assertEquals(expected, m.search("NAME", "shared" + age),
                    "maintained NAME index for key " + age);
        }
        // Range over AGE 2..4 collects the union of their posting lists.
        List<Integer> expectedRange = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            int age = i % distinctAges;
            if (age >= 2 && age <= 4) {
                expectedRange.add(i);
            }
        }
        assertEquals(expectedRange, m.rangeSearch("AGE", 2, 4), "range after churn");
        // Vacated unique keys were dropped entirely.
        assertTrue(m.search("AGE", 15).isEmpty(), "churned-away AGE key dropped");
        assertTrue(m.search("NAME", "n42").isEmpty(), "churned-away NAME key dropped");
        // Both indexes survived (no accidental index teardown).
        assertTrue(m.getIndexColumns().contains("AGE"), "AGE index survives");
        assertTrue(m.getIndexColumns().contains("NAME"), "NAME index survives");
        // PK lookups survive the churn untouched.
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(rowCount - 1), m.searchByPrimaryKey((long) rowCount - 1));
    }

    // ─── Position alignment: updateRow after a shifting insert ──────

    /**
     * updateRow resolves its target through positionToRowId, so a mid-table
     * insert that shifts later positions must not make subsequent updates
     * hit the wrong rows — and the secondary-index entry of the updated
     * row must move with it. Exercises the shift + update interplay that
     * PositionToRowIdInverseMapTest.midTableInsertKeepsLookupsAligned
     * starts, here against a maintained secondary index.
     */
    @Test
    void updateRowAfterMidTableInsertHitsShiftedRow() {
        CsvIndexManager m = manager(6, "ID", i -> i * 10); // AGE 0,10,...,50
        assertTrue(m.createIndex("AGE"));
        assertEquals(List.of(3), m.search("AGE", 30), "AGE=30 row sits at position 3");

        // Insert at position 2: later rows shift up by one.
        m.insertAt(2, row(99, "n99", 99));
        // Positions now: 0:ID0, 1:ID1, 2:ID99, 3:ID2, 4:ID3, 5:ID4, 6:ID5

        // The row formerly at position 2 (ID2, AGE=20) now sits at 3.
        m.updateRow(array(2, "n2", 20), 3, array(2, "n2", 21));
        assertTrue(m.search("AGE", 20).isEmpty(), "old AGE key of the shifted row is gone");
        assertEquals(List.of(3), m.search("AGE", 21), "shifted row carries its new AGE");
        assertEquals(List.of(2), m.search("AGE", 99), "the inserted row is indexed at its position");
        assertEquals(List.of(4), m.search("AGE", 30), "row at position 4 untouched");
        assertEquals(List.of(0), m.search("AGE", 0), "row at position 0 untouched");

        // And the position-2 insert did not break PK resolution anywhere.
        assertEquals(List.of(3), m.searchByPrimaryKey(2L));
        assertEquals(List.of(2), m.searchByPrimaryKey(99L));
        assertEquals(List.of(6), m.searchByPrimaryKey(5L));
    }
}
