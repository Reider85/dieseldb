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
 * O2 CSV-6 (shared CSV+TSV): secondary-index posting lists are
 * {@code LinkedHashSet<Long>} instead of {@code ArrayList<Long>}, so
 * {@code removeIndexedRow} disassociates a row in O(1) hash-lookup per
 * index instead of the O(k) linear {@code ArrayList.remove(Object)} scan
 * (k = posting size, which under a low-cardinality key is ~O(n) — the old
 * shape made updateRow/deleteRow O(n²) in a row loop).
 *
 * <p>Also guards the read-site fix that came with the type change:
 * {@code collectPositions} now matches {@code instanceof Iterable<?>} —
 * a leftover {@code List} check would silently drop Set posting lists and
 * make every secondary rangeSearch return empty.
 *
 * <p>Follows the CatalogLookupIsO1Test / PositionToRowIdInverseMapTest
 * single-file precedent (prompt-named RemoveIndexedRowIsO1Test folded with
 * its correctness regressions). The manager under test is the
 * format-agnostic DelimitedIndexManager via its only concrete CSV
 * instantiation (TsvIndexManager: 7-line subclass, zero overrides —
 * identical shared code path for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class RemoveIndexedRowIsO1Test {

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

    /** Rids of the odd rows from the tail downwards: 99_999, 99_997, ... */
    private static long[] tailOddRids(int rowCount, int removes) {
        long[] rids = new long[removes];
        for (int j = 0; j < removes; j++) {
            rids[j] = (rowCount - 1) - 2L * j;
        }
        return rids;
    }

    /**
     * Times {@code removeIndexedRow} for the given rids (arrays prebuilt so
     * the measurement isolates the posting-list removal, not allocation).
     */
    private static long timeRemovals(CsvIndexManager m, IntFunction<Integer> ageOf, long[] rids) {
        Object[][] arrays = new Object[rids.length][];
        for (int j = 0; j < rids.length; j++) {
            int rid = (int) rids[j];
            arrays[j] = array(rid, "n" + rid, ageOf.apply(rid));
        }
        long start = System.nanoTime();
        for (int j = 0; j < rids.length; j++) {
            m.removeIndexedRow(arrays[j], rids[j]);
        }
        return System.nanoTime() - start;
    }

    // ─── O(1) structural proof: cost independent of posting size ────

    /**
     * Two 100k-row fixtures with identical remove loops; only the posting
     * list length differs (k~50_000 for AGE%2 vs k=2 for AGE%50_000). The
     * old ArrayList.remove scanned the whole posting list, so big/small
     * ratio was ~150-600x; the LinkedHashSet hash-lookup makes the cost
     * independent of k (ratio ~1-2x, bound 30x leaves load headroom —
     * both measurements degrade equally under CPU contention, so the ratio
     * is load-immune: DirtyPageTrackingIsODTest bound style). The absolute
     * bound is a secondary catastrophic-regression guard. Unmeasured JIT
     * warmup first (EvictionUnderPinnedHeadTest precedent).
     */
    @Test
    @Timeout(60)
    void removeIndexedRowIsO1UnderLowCardinalityKey() {
        final int tableRows = 100_000;
        final int removes = 10_000;
        final double maxRatio = 30.0;
        final long absoluteBoundMillis = 3_000;

        // Warmup: hot-compile the remove path on a small fixture so neither
        // measurement pays the JIT compilation (order would otherwise bias
        // the ratio toward whichever runs second).
        CsvIndexManager warm = manager(10_000, null, i -> i % 2);
        warm.createIndex("AGE");
        timeRemovals(warm, i -> i % 2, tailOddRids(10_000, 500));

        // Small posting lists: 100k rows, 50k distinct AGE keys (k = 2).
        CsvIndexManager small = manager(tableRows, null, i -> i % 50_000);
        small.createIndex("AGE");
        // Large posting lists: 100k rows, 2 distinct AGE keys (k = 50k).
        CsvIndexManager big = manager(tableRows, null, i -> i % 2);
        big.createIndex("AGE");

        long[] rids = tailOddRids(tableRows, removes);
        // Small first: identical call pattern, both with hot code.
        long smallNanos = timeRemovals(small, i -> i % 50_000, rids);
        long bigNanos = timeRemovals(big, i -> i % 2, rids);

        long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);

        assertTrue(ratio <= maxRatio,
                "removeIndexedRow cost must not scale with posting-list size: big(k~50k)="
                        + bigMillis + " ms vs small(k=2)=" + smallMillis
                        + " ms, ratio=" + String.format(java.util.Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 CSV-6 O(1) remove)");
        assertTrue(bigMillis < absoluteBoundMillis,
                removes + " removes against k~50k posting lists took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis + " ms");
    }

    // ─── Removal correctness: exact disassociation + empty-key drop ─

    /**
     * removeIndexedRow must disassociate exactly the targeted rowIds: a
     * fully emptied posting list drops its key, partially emptied lists keep
     * the remaining members in ascending order, and untouched keys are
     * unaffected. (This is the updateRow/deleteRow hot path — o2 CSV-3's
     * O(log n) update relies on it.)
     */
    @Test
    void removalsDisassociateExactKeysAndDropEmptyOnes() {
        final int rowCount = 100;
        final int distinctAges = 10;
        CsvIndexManager m = manager(rowCount, null, i -> i % distinctAges);
        assertTrue(m.createIndex("AGE"));

        // Drop every AGE==5 row (positions 5,15,...,95).
        for (int i = 5; i < rowCount; i += distinctAges) {
            m.removeIndexedRow(array(i, "n" + i, i % distinctAges), i);
        }
        assertTrue(m.search("AGE", 5).isEmpty(), "emptied posting list drops its key");
        assertTrue(m.rangeSearch("AGE", 5, 5).isEmpty(), "range sees the dropped key too");
        assertEquals(List.of("AGE"), m.getIndexColumns(), "the index itself survives");

        // Untouched keys keep their full posting lists, ascending.
        List<Integer> age2 = m.search("AGE", 2);
        assertEquals(10, age2.size());
        for (int i = 2, j = 0; i < rowCount; i += distinctAges, j++) {
            assertEquals(i, age2.get(j), "AGE=2 member " + j);
        }

        // Partial removal from a multi-member list keeps the rest, ascending.
        m.removeIndexedRow(array(2, "n2", 2), 2);
        List<Integer> remaining = m.search("AGE", 2);
        assertEquals(9, remaining.size(), "one member removed, nine left");
        assertEquals(Integer.valueOf(12), remaining.get(0), "members stay ascending");
        assertEquals(Integer.valueOf(92), remaining.get(remaining.size() - 1));

        // The removed row is gone from range results, the rest remain.
        List<Integer> range = m.rangeSearch("AGE", 2, 4);
        assertEquals(30 - 1, range.size(), "AGE 2 lost one member; 3 and 4 intact");
        assertTrue(!range.contains(2), "removed position excluded");
    }

    // ─── Read-site fix: rangeSearch must read Set posting lists ─────

    /**
     * collectPositions previously matched {@code instanceof List<?>}; with
     * LinkedHashSet posting lists that check would fail and rangeSearch
     * would silently return an empty result for every secondary index.
     * Exercises both-bound, low-only and high-only range paths over
     * multi-member posting lists.
     */
    @Test
    void rangeSearchReadsSetPostingLists() {
        final int rowCount = 100;
        final int distinctAges = 5;
        CsvIndexManager m = manager(rowCount, null, i -> i % distinctAges);
        assertTrue(m.createIndex("AGE"));

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            int age = i % distinctAges;
            if (age >= 1 && age <= 3) {
                expected.add(i);
            }
        }
        assertEquals(expected, m.rangeSearch("AGE", 1, 3), "both bounds");
        assertEquals(80, m.rangeSearch("AGE", null, 3).size(), "high-only bound (ages 0..3)");
        assertEquals(80, m.rangeSearch("AGE", 1, null).size(), "low-only bound (ages 1..4)");
        assertEquals(rowCount, m.rangeSearch("AGE", null, null).size(), "open range");
        assertTrue(m.rangeSearch("AGE", 7, 9).isEmpty(), "range outside all keys");
    }
}
