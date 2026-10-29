package diesel;

import diesel.storage.JsonlIndexManager;
import diesel.storage.json.JsonParserConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-4 (mirror of o2 CSV-6): secondary-index posting lists in
 * {@link JsonlIndexManager} are {@code LinkedHashSet<Long>} instead of
 * {@code ArrayList<Long>}, so disassociating a row is an O(1) hash-lookup
 * per index instead of the O(k) linear {@code ArrayList.remove(Object)} scan
 * (k = posting size; under a low-cardinality key k is ~O(n), which made
 * updateRow/deleteRow O(n^2) in a row loop).
 *
 * <p>The private {@code removeIndexedRow} is exercised through the public
 * position-stable {@code updateRow} (remove-old-posting + add-new-posting at
 * the same rowId, no position-map shift — the exact hot path the O(k) scan
 * poisoned). {@code deleteRow} is NOT the measuring vehicle: its inherent
 * O(R) position shift (o2 JSONL-3, accepted per-call cost) would dominate
 * and mask the posting-list cost.
 *
 * <p>Also guards the read-site fix that came with the type change:
 * {@code collectPositions} now matches {@code instanceof Iterable<?>} — a
 * leftover {@code List} check would silently drop Set posting lists and make
 * every secondary rangeSearch return empty.
 *
 * <p>Follows the RemoveIndexedRowIsO1Test single-file precedent (prompt-named
 * JsonlRemoveIndexedRowIsO1Test folded with its correctness regressions).
 * The nested dot-path indexes share the same posting-list type and removal
 * loop; their behaviour is covered by {@code JsonlIndexTest} in the profile
 * gate.
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlRemoveIndexedRowIsO1Test {

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Object[] array(long id, String name, int age) {
        return new Object[] {id, name, age};
    }

    private static JsonlIndexManager manager(int rowCount, IntFunction<Integer> ageOf) {
        JsonlIndexManager m = new JsonlIndexManager("T", COLS, JsonParserConfig.defaults());
        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(array(i, "n" + i, ageOf.apply(i)));
        }
        m.markDirty(rows, null);
        return m;
    }

    /**
     * Times {@code updateRow} for the given row indices (arrays prebuilt so
     * the measurement isolates the index posting-list work, not allocation).
     * Every update vacates the original low-cardinality key (AGE -&gt; -1),
     * so the remove-branch scans (old code) or hash-looks-up (new code) the
     * ORIGINAL key's posting list.
     */
    private static long timeUpdates(JsonlIndexManager m, IntFunction<Integer> ageOf, int[] indices) {
        Object[][] oldArrays = new Object[indices.length][];
        Object[][] newArrays = new Object[indices.length][];
        for (int j = 0; j < indices.length; j++) {
            int i = indices[j];
            oldArrays[j] = array(i, "n" + i, ageOf.apply(i));
            newArrays[j] = array(i, "n" + i, -1);
        }
        long start = System.nanoTime();
        for (int j = 0; j < indices.length; j++) {
            m.updateRow(oldArrays[j], indices[j], newArrays[j]);
        }
        return System.nanoTime() - start;
    }

    /** Spreads {@code count} row indices evenly over {@code rowCount}. */
    private static int[] spreadIndices(int rowCount, int count) {
        int[] indices = new int[count];
        for (int j = 0; j < count; j++) {
            indices[j] = (int) ((long) j * rowCount / count);
        }
        return indices;
    }

    // ─── O(1) structural proof: cost independent of posting size ────

    /**
     * Two 100k-row fixtures with identical update loops; only the posting
     * list length differs (k~50_000 for AGE%2 vs k=2 for AGE%50_000). The
     * old ArrayList.remove scanned the whole posting list, so big/small
     * ratio was ~10^4-10^5x; the LinkedHashSet hash-lookup makes the cost
     * independent of k (ratio ~1-2x, bound 30x leaves load headroom — both
     * measurements degrade equally under CPU contention, so the ratio is
     * load-immune: RemoveIndexedRowIsO1Test / DirtyPageTrackingIsODTest
     * bound style). The absolute bound is a secondary catastrophic-regression
     * guard. Unmeasured JIT warmup first (EvictionUnderPinnedHeadTest
     * precedent).
     */
    @Test
    @Timeout(60)
    void updateRowVacatingLowCardinalityKeyIsO1() {
        final int tableRows = 100_000;
        final int updates = 10_000;
        final double maxRatio = 30.0;
        final long absoluteBoundMillis = 3_000;

        // Warmup: hot-compile the update path on a small fixture so neither
        // measurement pays the JIT compilation (order would otherwise bias
        // the ratio toward whichever runs second).
        JsonlIndexManager warm = manager(10_000, i -> i % 2);
        warm.createIndex("AGE");
        timeUpdates(warm, i -> i % 2, spreadIndices(10_000, 500));

        // Small posting lists: 100k rows, 50k distinct AGE keys (k = 2).
        JsonlIndexManager small = manager(tableRows, i -> i % 50_000);
        small.createIndex("AGE");
        // Large posting lists: 100k rows, 2 distinct AGE keys (k = 50k).
        JsonlIndexManager big = manager(tableRows, i -> i % 2);
        big.createIndex("AGE");

        int[] indices = spreadIndices(tableRows, updates);
        // Small first: identical call pattern, both with hot code.
        long smallNanos = timeUpdates(small, i -> i % 50_000, indices);
        long bigNanos = timeUpdates(big, i -> i % 2, indices);

        long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);

        assertTrue(ratio <= maxRatio,
                "updateRow vacating a low-cardinality key must not scale with posting-list size: big(k~50k)="
                        + bigMillis + " ms vs small(k=2)=" + smallMillis
                        + " ms, ratio=" + String.format(java.util.Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 JSONL-4 O(1) posting remove)");
        assertTrue(bigMillis < absoluteBoundMillis,
                updates + " updateRows against k~50k posting lists took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis + " ms");
    }

    // ─── Removal correctness: exact disassociation + empty-key drop ─

    /**
     * Vacating every AGE==5 row must drop the emptied posting list entirely
     * (search and rangeSearch both stop seeing the key) while untouched keys
     * keep their full posting lists in ascending position order, and a
     * partial vacate keeps the remaining members. (This is the
     * updateRow/deleteRow hot path — o2 JSONL-3's position decoupling relies
     * on key -&gt; rowId entries staying exact.)
     */
    @Test
    void vacatedKeysDropAndSurvivorsStayAscending() {
        final int rowCount = 100;
        final int distinctAges = 10;
        JsonlIndexManager m = manager(rowCount, i -> i % distinctAges);
        assertTrue(m.createIndex("AGE"));

        // Vacate every AGE==5 row (positions 5,15,...,95) via updateRow.
        for (int i = 5; i < rowCount; i += distinctAges) {
            m.updateRow(array(i, "n" + i, i % distinctAges), i, array(i, "n" + i, -1));
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

        // Partial vacate from a multi-member list keeps the rest, ascending.
        m.updateRow(array(2, "n2", 2), 2, array(2, "n2", -1));
        List<Integer> remaining = m.search("AGE", 2);
        assertEquals(9, remaining.size(), "one member vacated, nine left");
        assertEquals(Integer.valueOf(12), remaining.get(0), "members stay ascending");
        assertEquals(Integer.valueOf(92), remaining.get(remaining.size() - 1));

        // The vacated row is gone from range results, the rest remain.
        List<Integer> range = m.rangeSearch("AGE", 2, 4);
        assertEquals(30 - 1, range.size(), "AGE 2 lost one member; 3 and 4 intact");
        assertTrue(!range.contains(2), "vacated position excluded");
    }

    // ─── Read-site fix: rangeSearch must read Set posting lists ─────

    /**
     * collectPositions previously matched {@code instanceof List<?>}; with
     * LinkedHashSet posting lists that check would fail and rangeSearch
     * would silently return an empty result for every secondary index.
     * Exercises both-bound, low-only, high-only and open range paths over
     * multi-member posting lists.
     */
    @Test
    void rangeSearchReadsSetPostingLists() {
        final int rowCount = 100;
        final int distinctAges = 5;
        JsonlIndexManager m = manager(rowCount, i -> i % distinctAges);
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
