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
 * O2 CSV-4 + CSV-5 (shared CSV+TSV), partial acceptance: insert/delete
 * position handling is no longer O(n²) when batched.
 *
 * <p>What already holds in the manager (delivered by o2 CSV-1):
 * <ul>
 *   <li>tail-position {@code insertAt} skips the shift entirely (append
 *       path) — n appends are O(n) + O(m log n) index inserts;</li>
 *   <li>{@code beginBulkUpdate()}/{@code endBulkUpdate()} defer all
 *       position shifting, index mutation and tombstone compaction to a
 *       single {@code reindex()} — n in-window mutations cost O(1) each
 *       plus one O(n log n) rebuild;</li>
 *   <li>outside a window, a tail-position {@code deleteRow} is O(1)
 *       (no shift loop iterations) plus geometric compaction.</li>
 * </ul>
 *
 * <p>What is <em>not</em> claimed here (engine scope = o2 CSV-7):
 * mid-table single-row insert/delete still pays one O(n) ArrayList/inverse
 * map shift per call — acceptable for single-row statements per the prompt,
 * but n mid-table statements need the engine to batch through the bulk
 * window (or delete descending by position). The prompt-named
 * BatchInsertIsONTest / SingleInsertIsONTest / BatchDeleteIsONTest are
 * consolidated into this one file (CatalogLookupIsO1Test precedent). The
 * manager under test is the format-agnostic DelimitedIndexManager via its
 * only concrete CSV instantiation (TsvIndexManager: 7-line subclass, zero
 * overrides — identical shared code path for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class InsertDeleteBulkIsOLogNTest {

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

    private static CsvIndexManager manager(int rowCount, String pk, IntFunction<Integer> ageOf) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, ageOf.apply(i)));
        }
        m.buildIndexes(rows, pk);
        return m;
    }

    /** Appends {@code inserts} new rows at the tail (live insert path). */
    private static long timeTailInserts(CsvIndexManager m, int inserts) {
        long base = m.getRowCount();
        long start = System.nanoTime();
        for (int i = 0; i < inserts; i++) {
            m.insertAt(m.getRowCount(), row(base + i, "x" + i, i % 10));
        }
        return System.nanoTime() - start;
    }

    // ─── Single-row inserts: O(1) append, cost independent of N ─────

    /**
     * The prompt's SingleInsertIsONTest criterion: single-row inserts into
     * a large table are cheap appends — the append path never walks the
     * position maps (o2 CSV-1), so the same 10³-insert batch costs the
     * same on an N=1k and an N=100k table (only the O(log n) index insert
     * differs). The old entry-set walk was only ever paid on mid-table
     * inserts; this guard keeps appends from regressing to any O(N) scan.
     * Ratio is load-immune with an absolute wall bound as secondary guard;
     * unmeasured JIT warmup first (EvictionUnderPinnedHeadTest precedent).
     */
    @Test
    @Timeout(60)
    void singleTailInsertsCostIsIndependentOfTableSize() {
        final int batch = 1_000;
        final double maxRatio = 10.0;
        final long absoluteBoundMillis = 2_000;

        // Warmup: hot-compile the insert path (incl. secondary-index
        // maintenance) so neither measurement pays the JIT compilation.
        CsvIndexManager warm = manager(10_000, "ID", i -> i % 10);
        warm.createIndex("AGE");
        timeTailInserts(warm, 500);

        CsvIndexManager small = manager(1_000, "ID", i -> i % 10);
        small.createIndex("AGE");
        CsvIndexManager big = manager(100_000, "ID", i -> i % 10);
        big.createIndex("AGE");

        // Small first: identical call pattern, both with hot code.
        long smallNanos = timeTailInserts(small, batch);
        long bigNanos = timeTailInserts(big, batch);

        long smallMillis = TimeUnit.NANOSECONDS.toMillis(smallNanos);
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        double ratio = (double) bigNanos / Math.max(1L, smallNanos);

        assertTrue(ratio <= maxRatio,
                "tail-insert batch cost must not scale with table size: big(N=100k)="
                        + bigMillis + " ms vs small(N=1k)=" + smallMillis
                        + " ms, ratio=" + String.format(java.util.Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio + " (o2 CSV-4 O(1) append)");
        assertTrue(bigMillis < absoluteBoundMillis,
                batch + " tail inserts into a 100k-row table took " + bigMillis
                        + " ms, expected < " + absoluteBoundMillis + " ms (o2 CSV-4)");

        // The appended rows are fully wired into every index.
        assertEquals(101_000, big.getRowCount());
        assertEquals(List.of(100_000), big.searchByPrimaryKey(100_000L));
        assertEquals(List.of(100_999), big.searchByPrimaryKey(100_999L));
        assertTrue(big.search("AGE", 0).contains(100_000), "first appended row indexed by AGE");
    }

    // ─── Bulk window: 10⁵ inserts = one deferred rebuild ────────────

    /**
     * The prompt's BatchInsertIsONTest criterion at the manager level:
     * 10⁵ inserts inside a bulk-update window mutate only the mirror list
     * (O(1) appends) and pay a single reindex at endBulkUpdate — the whole
     * batch completes in O(n log n). After the window closes the indexes
     * must be query-consistent: PK lookups hit the appended rows and the
     * AGE posting lists cover base and appended rows in ascending
     * position order. Note that {@code getRowCount()} is stale inside the
     * window (the bulk path skips the per-row position-map updates — index
     * state is query-inconsistent by contract), so the driver tracks the
     * append position locally, as an engine batch path must.
     */
    @Test
    @Timeout(60)
    void bulkWindowInserts100kWithinWallBound() {
        final int baseRows = 1_000;
        final int inserts = 100_000;
        final long boundMillis = 2_000;
        CsvIndexManager m = manager(baseRows, "ID", i -> i % 10);
        assertTrue(m.createIndex("AGE"));

        int appendPos = m.getRowCount(); // snapshot BEFORE the window opens
        long start = System.nanoTime();
        try {
            m.beginBulkUpdate();
            for (int i = 0; i < inserts; i++) {
                m.insertAt(appendPos++, row(baseRows + i, "x" + i, i % 10));
            }
        } finally {
            m.endBulkUpdate(); // single reindex over base + appended rows
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                inserts + " bulk-window inserts over a " + baseRows
                        + "-row base took " + elapsedMillis + " ms, expected < "
                        + boundMillis + " ms (single deferred reindex, o2 CSV-4)");

        assertEquals(baseRows + inserts, m.getRowCount());
        assertEquals(List.of(baseRows), m.searchByPrimaryKey((long) baseRows));
        assertEquals(List.of(baseRows + inserts - 1),
                m.searchByPrimaryKey((long) (baseRows + inserts - 1)));
        // AGE=0: base positions 0,10,...,990 then appended 1000,1010,...,100_990.
        List<Integer> expectedAge0 = new ArrayList<>();
        for (int p = 0; p < baseRows; p += 10) {
            expectedAge0.add(p);
        }
        for (int p = baseRows; p < baseRows + inserts; p += 10) {
            expectedAge0.add(p);
        }
        assertEquals(expectedAge0, m.search("AGE", 0), "AGE posting list spans the window");
    }

    // ─── Bulk window: 50k deletes of a 100k table = one rebuild ─────

    /**
     * The prompt's BatchDeleteIsONTest criterion at the manager level:
     * tail-position deletes inside a bulk-update window are O(1) mirror
     * pops (no shift, no per-row index mutation, no mid-flight compaction)
     * and the accumulated tombstones are resolved by the single reindex at
     * endBulkUpdate — 50k deletes over a 100k-row table complete in
     * O(n log n). The same deletes outside a window pay the per-row
     * inverse-map shift (O(n) each for mid-table positions); deleting
     * descending by position keeps even the live path at O(1) per call —
     * the ordering rule the engine batch path must adopt (o2 CSV-7). As in
     * the insert test, the driver tracks the tail position locally because
     * {@code getRowCount()} is stale inside the window.
     */
    @Test
    @Timeout(60)
    void bulkWindowDeletes50kOf100kWithinWallBound() {
        final int tableRows = 100_000;
        final int deletes = 50_000;
        final long boundMillis = 2_000;
        CsvIndexManager m = manager(tableRows, "ID", i -> i); // unique AGE
        assertTrue(m.createIndex("AGE"));

        int tailPos = m.getRowCount() - 1; // snapshot BEFORE the window opens
        long start = System.nanoTime();
        try {
            m.beginBulkUpdate();
            for (int i = 0; i < deletes; i++) {
                m.deleteRow(tailPos--); // tail position: O(1) pop
            }
        } finally {
            m.endBulkUpdate(); // single reindex over the surviving rows
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                deletes + " bulk-window deletes over a " + tableRows
                        + "-row table took " + elapsedMillis + " ms, expected < "
                        + boundMillis + " ms (single deferred reindex, o2 CSV-5)");

        assertEquals(tableRows - deletes, m.getRowCount());
        // The deleted tail (IDs 50_000..99_999) is gone from every index.
        assertTrue(m.searchByPrimaryKey(50_000L).isEmpty(), "deleted tail row is gone");
        assertTrue(m.searchByPrimaryKey(99_999L).isEmpty(), "deleted tail row is gone");
        assertTrue(m.search("AGE", 60_000).isEmpty(), "deleted tail AGE key dropped");
        // The surviving head (IDs 0..49_999) is fully query-consistent.
        assertEquals(List.of(49_999), m.searchByPrimaryKey(49_999L));
        assertEquals(List.of(12_345), m.search("AGE", 12_345));
        assertTrue(m.getIndexColumns().contains("AGE"), "AGE index rebuilt after the window");
    }
}
