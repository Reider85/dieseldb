package diesel;

import diesel.storage.JsonlIndexManager;
import diesel.storage.json.JsonParserConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 JSONL-2 (+ JSONL-3 change #2): the inverse position-&gt;rowId map makes
 * {@code positionToRowId} an O(1) HashMap get instead of the old O(R)
 * entry-set scan over {@code rowIdToPosition}, and the insert/delete position
 * shifts now walk that inverse map (mirror of the delimited manager, o2
 * CSV-1/CSV-4/CSV-5). A row-loop driven {@code updateRow} batch therefore
 * costs O(batch · log n) instead of O(R · batch), and the shift walks keep
 * both maps in lockstep so lookups after a mid-table insert/delete hit the
 * rows that physically live at those positions.
 *
 * <p>The timing batch updates <em>unique</em> NAME keys on purpose: the
 * JSONL posting lists are still {@code List} ({@code ArrayList.remove(Object)}
 * is O(k), o2 JSONL-4 — out of scope here), so a low-cardinality key would
 * make the ratio measure JSONL-4 instead of the inverse map.
 *
 * <p>Calibration deviations from the prompt: the "&lt; 100 ns" criterion on a
 * private method is unmeasurable under JVM noise — replaced by the calibrated
 * ratio-bound (N-independence, load-immune) + absolute wall-bound pair used
 * by UpdateRowIsOLogNTest/PositionToRowIdInverseMapTest. Small-of-2 rounds
 * with fresh fixtures on both sides so neither measurement pays fixture build
 * or a one-off GC pause (ratio bias guard).
 */
@Tag("storage")
@StorageType("jsonl")
class JsonlPositionToRowIdIsO1Test {

    private static final List<String> COLS = List.of("ID", "NAME", "AGE");

    private static Object[] row(long id, String name, int age) {
        return new Object[] {id, name, age};
    }

    /** Builds a fresh manager with rows id 0..rowCount-1 (NAME/AGE unique per row). */
    private static JsonlIndexManager manager(int rowCount) {
        JsonlIndexManager m = new JsonlIndexManager("T", COLS, JsonParserConfig.defaults());
        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, i));
        }
        m.markDirty(rows, "ID");
        return m;
    }

    /**
     * Renames NAME of {@code updates} rows spread evenly across the table
     * from n&lt;pos&gt; to u&lt;pos&gt;. Each call resolves the rowId via
     * positionToRowId (the metric under test) and swaps secondary-index
     * membership (O(1) posting removal — unique keys — + O(log c) insert per
     * index).
     *
     * <p>Spread, not first-k: after a reindex rid==position and the old scan
     * iterated the rid-ordered entry set, so batching only the first rows
     * would find each target early and understate the old O(R) cost. A
     * full-table row loop — the prompt's "K = O(R)" scenario — visits late
     * positions, where the old scan is Θ(pos): N-dependent by construction.
     */
    private static long timeNameShiftBatch(int tableRows, int updates) {
        JsonlIndexManager m = manager(tableRows);
        assertTrue(m.createIndex("NAME"));
        long start = System.nanoTime();
        for (int i = 0; i < updates; i++) {
            int pos = (int) ((long) i * tableRows / updates);
            m.updateRow(row(pos, "n" + pos, pos), pos, row(pos, "u" + pos, pos));
        }
        long nanos = System.nanoTime() - start;
        // The timed batch must have actually hit the live index.
        assertEquals(List.of(0), m.search("NAME", "u0"));
        assertTrue(m.search("NAME", "n0").isEmpty());
        int lastPos = (int) ((long) (updates - 1) * tableRows / updates);
        assertEquals(List.of(lastPos), m.search("NAME", "u" + lastPos));
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        int stride = tableRows / updates;
        if (stride > 1) {
            assertEquals(List.of(1), m.search("NAME", "n1"),
                    "row outside the renamed set kept its key");
        }
        return nanos;
    }

    // ─── O(batch · log n) structural proof: N-independence ──────────

    /**
     * The prompt's acceptance criterion: a batch of 10³ updates over a
     * 10⁵-row table must cost the same as over a 10³-row table (only the
     * log factor differs). The old per-call O(R) positionToRowId scan made
     * the big/small ratio ~100x (10³ · 10⁵ entry visits vs 10³ · 10³). The
     * ratio is load-immune (both measurements degrade equally under CPU
     * contention — DirtyPageTrackingIsODTest bound style); an absolute wall
     * bound guards catastrophic regressions. Unmeasured JIT warmup first
     * (EvictionUnderPinnedHeadTest precedent), then small-of-2 rounds.
     */
    @Test
    @Timeout(60)
    void updateRowBatchIsIndependentOfTableSize() {
        final int batch = 1_000;
        final int smallN = 1_000;
        final int bigN = 100_000;
        final double maxRatio = 10.0;
        final long absoluteBoundMillis = 2_000;

        // Warmup: hot-compile the update path so neither measurement pays
        // the JIT compilation (order would otherwise bias the ratio).
        timeNameShiftBatch(10_000, batch);

        long smallNanos = Math.min(timeNameShiftBatch(smallN, batch),
                timeNameShiftBatch(smallN, batch));
        long bigNanos = Math.min(timeNameShiftBatch(bigN, batch),
                timeNameShiftBatch(bigN, batch));

        double ratio = (double) bigNanos / Math.max(1L, smallNanos);
        assertTrue(ratio <= maxRatio,
                "updateRow batch of " + batch + ": N=" + bigN + " took "
                        + TimeUnit.NANOSECONDS.toMillis(bigNanos) + " ms vs N=" + smallN
                        + " took " + TimeUnit.NANOSECONDS.toMillis(smallNanos)
                        + " ms, ratio=" + String.format(Locale.ROOT, "%.1f", ratio)
                        + ", expected <= " + maxRatio
                        + " (o2 JSONL-2 O(1) positionToRowId inverse map)");
        long bigMillis = TimeUnit.NANOSECONDS.toMillis(bigNanos);
        assertTrue(bigMillis < absoluteBoundMillis,
                "updateRow batch of " + batch + " over " + bigN + " rows took "
                        + bigMillis + " ms, expected < " + absoluteBoundMillis
                        + " ms (old O(R) scan = seconds)");
    }

    // ─── Inverse-map consistency across the rewritten shift walks ───

    /**
     * Mid-table insert shifts later positions up by one and delete shifts
     * them down; the rewritten inverse-map walks must move both maps in
     * lockstep so subsequent updateRow/deleteRow at the shifted positions
     * resolve the rows that physically live there.
     */
    @Test
    void lookupsAfterMidTableShiftHitThePhysicallyShiftedRows() {
        JsonlIndexManager m = manager(10); // n0..n9 at positions 0..9
        assertTrue(m.createIndex("NAME"));
        assertTrue(m.createIndex("AGE"));

        m.insertAt(5, row(99L, "zed", 50));
        // positions now: n0..n4@0-4, zed@5, n5@6, n6@7, n7@8, n8@9, n9@10
        assertEquals(11, m.getRowCount());
        assertEquals(List.of(5), m.searchByPrimaryKey(99L));
        assertEquals(List.of(6), m.search("NAME", "n5"));
        assertEquals(List.of(7), m.search("NAME", "n6"));
        assertEquals(List.of(10), m.search("NAME", "n9"));
        assertEquals(List.of(10), m.searchByPrimaryKey(9L));

        // updateRow at shifted position 7 must hit the row living there (n6).
        m.updateRow(row(6L, "n6", 6), 7, row(6L, "u6", 66));
        assertEquals(List.of(7), m.search("NAME", "u6"));
        assertTrue(m.search("NAME", "n6").isEmpty(), "churned-away NAME key dropped");
        assertEquals(List.of(7), m.search("AGE", 66), "shifted row carries its new AGE");
        assertTrue(m.search("AGE", 6).isEmpty(), "old AGE key of the shifted row is gone");
        assertEquals(List.of(8), m.search("AGE", 7), "row at position 8 untouched");
        assertEquals(List.of(0), m.search("AGE", 0), "row at position 0 untouched");

        // Delete the inserted row: everything after position 5 shifts down.
        m.deleteRow(5);
        assertEquals(10, m.getRowCount());
        assertTrue(m.search("NAME", "zed").isEmpty());
        assertEquals(List.of(5), m.search("NAME", "n5"));
        assertEquals(List.of(6), m.search("NAME", "u6"), "update target followed the shift down");
        assertEquals(List.of(7), m.search("NAME", "n7"));
        assertEquals(List.of(9), m.searchByPrimaryKey(9L));
        assertEquals(1, m.getDeletedCount(), "1 tombstone vs 10 live rows stays under 25%");

        // deleteRow at position 6 must tombstone exactly that row (u6).
        m.deleteRow(6);
        assertEquals(9, m.getRowCount());
        assertEquals(2, m.getDeletedCount(), "2 tombstones vs 9 live rows stays under 25%");
        assertTrue(m.search("NAME", "u6").isEmpty());
        assertTrue(m.search("AGE", 66).isEmpty());
        assertEquals(List.of(6), m.search("NAME", "n7"), "rows after the hole shift down");
        assertEquals(List.of(0), m.search("NAME", "n0"), "rows before the hole untouched");
        assertEquals(List.of(5), m.search("NAME", "n5"));
    }

    // ─── Deferred bulk window: rebuilt inverse stays exact ──────────

    /**
     * Inside a bulk window mutations are deferred to a single reindex at
     * endBulkUpdate; the inverse map rebuilt there must answer point lookups
     * exactly, and point mutations after the window must keep working.
     */
    @Test
    void bulkWindowRebuildKeepsInverseMapConsistent() {
        JsonlIndexManager m = manager(5); // n0..n4 at positions 0..4
        assertTrue(m.createIndex("NAME"));
        assertTrue(m.createIndex("AGE"));

        m.beginBulkUpdate();
        m.insertAt(2, row(99L, "zed", 50));
        m.deleteRow(0);
        m.endBulkUpdate(); // bulkDirty -> single reindex, rowIds renumbered

        // mirror now: n1@0, zed@1, n2@2, n3@3, n4@4 (rowIds 0..4)
        assertEquals(5, m.getRowCount());
        assertEquals(List.of(0), m.search("NAME", "n1"));
        assertEquals(List.of(1), m.search("NAME", "zed"));
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(1), m.searchByPrimaryKey(99L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(0, m.getDeletedCount(), "reindex cleared the tombstones");

        // Point ops after the window resolve through the rebuilt inverse map.
        m.updateRow(row(2L, "n2", 2), 2, row(2L, "u2", 22));
        assertEquals(List.of(2), m.search("NAME", "u2"));
        assertTrue(m.search("NAME", "n2").isEmpty());
        assertEquals(List.of(2), m.search("AGE", 22));

        m.deleteRow(4);
        assertEquals(4, m.getRowCount());
        assertTrue(m.search("NAME", "n4").isEmpty());
        assertEquals(List.of(3), m.search("NAME", "n3"), "rows after the hole shift down");
        assertEquals(List.of(0), m.search("NAME", "n1"), "rows before the hole untouched");
    }
}
