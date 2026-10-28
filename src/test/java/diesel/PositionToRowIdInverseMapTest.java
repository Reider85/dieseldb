package diesel;

import diesel.storage.CsvIndexManager;
import diesel.storage.DelimitedIndexManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 CSV-1 (shared CSV+TSV): the inverse position→rowId map makes
 * {@code positionToRowId} O(1) instead of the old O(n) entry-set scan, so a
 * row-loop driven updateRow/createIndex/deleteRow no longer degrades to
 * O(n²).
 *
 * <p>Consolidates the two prompt-named classes (PositionToRowIdIsO1Test,
 * InverseMapConsistencyTest) into one file, following the CatalogLookupIsO1Test
 * precedent. The manager under test is the format-agnostic
 * {@link DelimitedIndexManager}; {@link CsvIndexManager} is its only concrete
 * CSV instantiation (TsvIndexManager is a 7-line subclass with zero overrides,
 * so the shared code path is identical for TSV).
 */
@Tag("storage")
@StorageType({"csv", "tsv"})
class PositionToRowIdInverseMapTest {

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

    private static CsvIndexManager manager(int rowCount, String pk) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(row(i, "n" + i, i));
        }
        m.buildIndexes(rows, pk);
        return m;
    }

    // ─── O(1) wall-bound ────────────────────────────────────────────

    /**
     * 10k updateRow calls over a 100k-row table: each call performs one
     * positionToRowId lookup. The old O(n) scan cost ~10^9 entry comparisons
     * (seconds to minutes); the O(1) inverse-map get is milliseconds. The
     * 2000ms bound sits 30x above the expected O(1) time so the test fails a
     * regression without being load-sensitive (CatalogLookupIsO1Test bound
     * style); @Timeout catches a hang even faster.
     */
    @Test
    @Timeout(60)
    void positionToRowIdIsO1UnderRowLoop() {
        final int tableRows = 100_000;
        final int updates = 10_000;
        final long boundMillis = 2_000;
        CsvIndexManager m = manager(tableRows, "ID");
        // Secondary index maintained live by updateRow (oldRow removes the old
        // AGE key, newRow inserts the new one) — used for the post-bound check.
        m.createIndex("AGE");

        long start = System.nanoTime();
        for (int i = 0; i < updates; i++) {
            int pos = i % tableRows;
            m.updateRow(array(pos, "n" + pos, pos), pos, array(pos, "n" + pos, pos + 1000));
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < boundMillis,
                updates + " updateRow calls over a " + tableRows + "-row table took "
                        + elapsedMillis + " ms, expected < " + boundMillis
                        + " ms (O(1) positionToRowId inverse map)");
        // Spot-check the updates landed in the live secondary index: row at
        // position 0 now carries AGE 1000 (was 0; the original AGE=1000 row
        // was itself updated to 2000, so 1000 is unique).
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(9999), m.searchByPrimaryKey(9999L));
        assertEquals(List.of(0), m.search("AGE", 1000));
    }

    // ─── Inverse-map consistency ────────────────────────────────────

    /**
     * Mid-table insert shifts later positions by one; the inverse map must
     * follow so subsequent updateRow/deleteRow at shifted positions hit the
     * correct rows (they resolve rowId via positionToRowId).
     */
    @Test
    void midTableInsertKeepsLookupsAligned() {
        CsvIndexManager m = manager(5, "ID"); // IDs 0..4 at positions 0..4
        m.insertAt(2, row(99, "n99", 99));
        // Positions now: 0:ID0, 1:ID1, 2:ID99, 3:ID2, 4:ID3, 5:ID4

        assertEquals(List.of(2), m.searchByPrimaryKey(99L));
        assertEquals(List.of(4), m.searchByPrimaryKey(3L));
        assertEquals(List.of(5), m.searchByPrimaryKey(4L));

        // updateRow at the shifted position 3 must touch row ID=2 (not ID=3).
        m.updateRow(array(2, "n2", 2), 3, array(200, "n200", 2));
        assertTrue(m.searchByPrimaryKey(2L).isEmpty(), "old PK removed");
        assertEquals(List.of(3), m.searchByPrimaryKey(200L));
        assertEquals(List.of(4), m.searchByPrimaryKey(3L), "row at pos 4 untouched");
        assertEquals(List.of(1), m.searchByPrimaryKey(1L), "row at pos 1 untouched");

        // deleteRow at position 1 must remove ID=1 and shift the tail down.
        m.deleteRow(1);
        assertTrue(m.searchByPrimaryKey(1L).isEmpty());
        assertEquals(List.of(1), m.searchByPrimaryKey(99L));
        assertEquals(List.of(2), m.searchByPrimaryKey(200L));
        assertEquals(List.of(3), m.searchByPrimaryKey(3L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(5, m.getRowCount());
    }

    /**
     * LIFO churn: repeatedly inserting at position 0 and deleting position 0
     * must always resolve the newest row — a stale inverse map would make
     * deleteRow remove a wrong (older) row.
     */
    @Test
    void lifoInsertDeleteCycleResolvesNewestRow() {
        CsvIndexManager m = manager(0, "ID");
        final int cycles = 200;
        for (int i = 0; i < cycles; i++) {
            m.insertAt(0, row(1000 + i, "x" + i, i));
            assertEquals(List.of(0), m.searchByPrimaryKey(1000L + i),
                    "freshest insert must sit at position 0 (cycle " + i + ")");
            m.deleteRow(0);
            assertTrue(m.searchByPrimaryKey(1000L + i).isEmpty(),
                    "position 0 delete must remove the newest row (cycle " + i + ")");
        }
        assertEquals(0, m.getRowCount());
    }

    /**
     * Churn past the 25% compaction threshold triggers mid-flight reindex;
     * both maps are rebuilt from scratch, so every lookup must stay correct
     * across the compaction boundary.
     */
    @Test
    void compactionDuringChurnKeepsMapsConsistent() {
        CsvIndexManager m = manager(40, "ID");
        // 20 deletes over 40 live rows = 50% tombstones > threshold 25%:
        // compact()/reindex() fires during the loop.
        for (int i = 0; i < 20; i++) {
            m.deleteRow(0); // always delete the head
        }
        assertEquals(20, m.getRowCount());
        assertEquals(List.of(0), m.searchByPrimaryKey(20L));
        assertEquals(List.of(19), m.searchByPrimaryKey(39L));
        assertTrue(m.searchByPrimaryKey(0L).isEmpty());

        // Post-compaction mutations must also resolve correctly.
        m.updateRow(array(20, "n20", 20), 0, array(2020, "n2020", 20));
        assertTrue(m.searchByPrimaryKey(20L).isEmpty());
        assertEquals(List.of(0), m.searchByPrimaryKey(2020L));
        m.deleteRow(0);
        assertTrue(m.searchByPrimaryKey(2020L).isEmpty());
        assertEquals(List.of(0), m.searchByPrimaryKey(21L));
    }

    /**
     * removeDeadEntries resolves dead positions via the inverse map (was an
     * O(n) full walk). Verify it disassociates exactly the dead rows' index
     * entries without shifting live rows (no position shifting by contract —
     * a full rebuild follows).
     */
    @Test
    void removeDeadEntriesDisassociatesOnlyDeadRows() {
        CsvIndexManager m = manager(10, "ID");
        m.createIndex("NAME");

        int removed = m.removeDeadEntries(Set.of(3, 7));
        assertEquals(2, removed);

        assertTrue(m.searchByPrimaryKey(3L).isEmpty(), "dead row 3 disassociated");
        assertTrue(m.searchByPrimaryKey(7L).isEmpty(), "dead row 7 disassociated");
        // removeIndexedRow clears the dead rows' secondary-index entries too.
        assertTrue(m.search("NAME", "n3").isEmpty(), "secondary index cleared for dead row 3");
        assertTrue(m.search("NAME", "n7").isEmpty(), "secondary index cleared for dead row 7");
        // Live rows keep their physical positions (no shifting by contract).
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(List.of(9), m.searchByPrimaryKey(9L));
        assertEquals(List.of(4), m.search("NAME", "n4"), "live secondary entries intact");
        assertEquals(8, m.getRowCount());

        // The rebuild that follows reindexes the mirror list. removeDeadEntries
        // by contract does not touch the mirror (the caller prunes storage
        // first), so all 10 rows reappear with fresh sequential rowIds.
        m.reindex();
        assertEquals(10, m.getRowCount());
        assertEquals(List.of(0), m.searchByPrimaryKey(0L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(List.of(9), m.searchByPrimaryKey(9L));
        assertEquals(List.of(3), m.search("NAME", "n3"), "secondary index rebuilt");
    }

    /**
     * Bulk-window mutations only touch the mirror list; endBulkUpdate rebuilds
     * both maps via reindex. Lookups must be correct after the window closes.
     */
    @Test
    void bulkWindowRebuildKeepsMapsConsistent() {
        CsvIndexManager m = manager(5, "ID"); // IDs 0..4 at positions 0..4
        m.beginBulkUpdate();
        m.insertAt(1, row(50, "n50", 50)); // mirror: 0,50,1,2,3,4
        m.deleteRow(3); // physical position 3 = ID 2
        m.endBulkUpdate(); // reindex -> IDs 0,50,1,3,4 at positions 0..4

        assertEquals(5, m.getRowCount());
        assertEquals(List.of(1), m.searchByPrimaryKey(50L));
        assertTrue(m.searchByPrimaryKey(2L).isEmpty(), "row deleted inside the window");
        assertEquals(List.of(2), m.searchByPrimaryKey(1L));
        assertEquals(List.of(3), m.searchByPrimaryKey(3L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
    }
}
