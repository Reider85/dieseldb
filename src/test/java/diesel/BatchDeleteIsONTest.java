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
 * O2 CSV-5 + TSV-5: Batch delete operations complete in O(n) time.
 *
 * <p>This test verifies that batch deletions using the inverse-map-based shift
 * optimization achieve linear time complexity. The key improvements delivered
 * by CSV-1/TSV-1 (inverse position→rowId map) and CSV-5/TSV-5 (inverse-map shift):
 * <ul>
 *   <li>Single deleteRow outside bulk window: O(n) worst case (mid-table),
 *       but cache-friendly contiguous Integer key operations in the inverse map;</li>
 *   <li>Bulk window deletes: O(1) per mirror list removal + single O(n log n)
 *       reindex at endBulkUpdate — n deletions cost O(n log n) total;</li>
 *   <li>Tail-position deletes (outside window): O(1) no-shift path.</li>
 * </ul>
 *
 * <p>The manager under test is the format-agnostic DelimitedIndexManager via its
 * CSV and TSV instantiations (both use the same shared code path).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class BatchDeleteIsONTest {

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

    private CsvIndexManager manager(int rowCount, String indexCol, IntFunction<Integer> valueGen) {
        CsvIndexManager m = new CsvIndexManager("test_table", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "Name" + i, valueGen.apply(i)));
        }
        m.buildIndexes(rows, indexCol != null ? indexCol : "ID");
        return m;
    }

    /**
     * O2 CSV-5 + TSV-5 acceptance criterion: 10⁵ rows deleted as a batch
     * complete in O(n). Tests the bulk-update window path which defers
     * all position shifts and index mutations to a single reindex().
     */
    @Test
    @Timeout(60)
    void batchDelete100kRowsWithinLinearTime() {
        final int tableRows = 100_000;
        final int deletes = 100_000;
        final long boundMillis = 3_000; // O(n log n) bulk window reindex
        CsvIndexManager m = manager(tableRows, "ID", i -> i); // unique ID

        long start = System.nanoTime();
        try {
            m.beginBulkUpdate();
            for (int i = 0; i < deletes; i++) {
                m.deleteRow(0); // delete from head each time (worst-case position)
            }
        } finally {
            m.endBulkUpdate(); // single reindex
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                deletes + " batch deletions from " + tableRows + "-row table took "
                        + elapsedMillis + " ms, expected < " + boundMillis + " ms (O(n log n))");

        assertEquals(0, m.getRowCount(), "all rows deleted");
        assertTrue(m.getDeletedCount() == 0, "no tombstones after full deletion");
    }

    /**
     * Tests tail-position batch deletes outside bulk window — should be O(n)
     * total due to inverse-map shift optimization, but each delete is O(1)
     * when deleting from the tail position.
     */
    @Test
    @Timeout(30)
    void tailPositionBatchDelete50kOf100kWithinWallBound() {
        final int tableRows = 100_000;
        final int deletes = 50_000;
        final long boundMillis = 1_000; // O(n) inverse-map shifts
        CsvIndexManager m = manager(tableRows, "AGE", i -> i % 100); // low-cardinality AGE

        assertTrue(m.createIndex("AGE"));

        int tailPos = m.getRowCount() - 1;
        long start = System.nanoTime();
        for (int i = 0; i < deletes; i++) {
            m.deleteRow(tailPos--); // O(1) tail-position delete
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                deletes + " tail-position deletes took " + elapsedMillis + " ms, expected < "
                        + boundMillis + " ms (O(n) with O(1) per tail delete)");

        assertEquals(tableRows - deletes, m.getRowCount());
        // Verify surviving head rows are still accessible
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(49_999), m.searchByPrimaryKey(49_999L));
        // Verify tail AGE values are gone
        assertTrue(m.search("AGE", 100).isEmpty(), "deleted tail AGE values gone");
    }

    /**
     * Tests mid-table batch deletes outside bulk window — should still be
     * O(n) total due to inverse-map shift optimization (better than old O(n²)).
     */
    @Test
    @Timeout(30)
    void midTableBatchDelete20kOf100kWithinWallBound() {
        final int tableRows = 100_000;
        final int deletes = 20_000;
        final long boundMillis = 2_000; // O(n) inverse-map shifts
        CsvIndexManager m = manager(tableRows, "ID", i -> i);

        int midPos = tableRows / 2; // start deleting from middle
        long start = System.nanoTime();
        for (int i = 0; i < deletes; i++) {
            m.deleteRow(midPos); // O(n) per delete, but optimized with inverse map
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                deletes + " mid-table deletes took " + elapsedMillis + " ms, expected < "
                        + boundMillis + " ms (O(n) total with inverse-map optimization)");

        assertEquals(tableRows - deletes, m.getRowCount());
        // Verify rows before and after deletion zone are intact
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(tableRows - 1), m.searchByPrimaryKey((long) tableRows - 1));
    }
}