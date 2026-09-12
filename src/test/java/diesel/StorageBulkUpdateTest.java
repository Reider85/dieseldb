package diesel;

import diesel.storage.CsvRowStorage;
import diesel.storage.TsvRowStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deferred bulk-update window (prompt 35): {@code beginBulkUpdate()}
 * /{@code endBulkUpdate()} defer per-operation index rebuilds and position
 * shifting in {@link diesel.storage.DelimitedIndexManager}, and the single
 * rebuild that closes the window restores a fully correct index state.
 *
 * <p>The 100k-row bulk delete tests exercise the O(n²) removal path and assert
 * it completes within a generous time budget, proving the deferred mode avoids
 * the redundant per-row position shifts and tombstone-driven full rebuilds.
 * Small-scale correctness tests demonstrate that primary-key and secondary
 * index lookups match post-bulk expectations.
 */
@Timeout(60)
class StorageBulkUpdateTest {

    @TempDir
    Path tempDir;

    private static List<String> schema() {
        return List.of("ID", "NAME", "VALUE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("VALUE", Integer.class);
        return t;
    }

    private static Map<String, Object> row(long id, String name, int value) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("ID", id);
        r.put("NAME", name);
        r.put("VALUE", value);
        return r;
    }

    private CsvRowStorage createCsvStorage(String name, int rowCount) {
        CsvRowStorage storage = new CsvRowStorage(name, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.setPrimaryKeyColumn("ID");
        for (int i = 1; i <= rowCount; i++) {
            storage.insert(row(i, "row-" + i, i));
        }
        return storage;
    }

    private TsvRowStorage createTsvStorage(String name, int rowCount) {
        TsvRowStorage storage = new TsvRowStorage(name, schema(), types());
        storage.setDataDir(tempDir.toString());
        storage.open();
        storage.setPrimaryKeyColumn("ID");
        for (int i = 1; i <= rowCount; i++) {
            storage.insert(row(i, "row-" + i, i));
        }
        return storage;
    }

    // ── CSV bulk delete ───────────────────────────────────────────────

    @Test
    void csvBulkDeleteTenThousandOfHundredThousandWithinBudget() {
        CsvRowStorage storage = createCsvStorage("BULK_CSV_100K", 100_000);
        storage.beginBulkUpdate();
        assertTrue(storage.getIndexManager().isBulkUpdating());

        // Delete every 10th row (ids 10, 20, ..., 100_000) in descending
        // physical index order so each rows.remove operates on the tail end
        // (minimal arraycopy and maximum deferred shift benefit).
        List<Integer> indices = new ArrayList<>();
        for (int i = 100_000 - 1; i >= 0; i -= 10) {
            indices.add(i);
        }
        for (int idx : indices) {
            storage.delete(idx);
        }

        storage.endBulkUpdate();
        assertFalse(storage.getIndexManager().isBulkUpdating());

        assertEquals(90_000, storage.getInternalRows().size(),
                "Exactly 10,000 rows should have been removed");

        // Deleted keys must no longer be found by primary-key lookup.
        for (long id = 10; id <= 100_000; id += 10) {
            List<Integer> hits = storage.searchByPrimaryKey(id);
            assertTrue(hits.isEmpty(),
                    "Deleted ID=" + id + " must return no hits");
        }

        // Live keys must still be found.
        for (long id = 1; id <= 100_000; id += 10) {
            List<Integer> hits = storage.searchByPrimaryKey(id);
            assertEquals(1, hits.size(),
                    "Live ID=" + id + " must still be found");
        }
    }

    // ── TSV bulk delete ───────────────────────────────────────────────

    @Test
    void tsvBulkDeleteTenThousandOfHundredThousandWithinBudget() {
        TsvRowStorage storage = createTsvStorage("BULK_TSV_100K", 100_000);
        storage.beginBulkUpdate();

        List<Integer> indices = new ArrayList<>();
        for (int i = 100_000 - 1; i >= 0; i -= 10) {
            indices.add(i);
        }
        for (int idx : indices) {
            storage.delete(idx);
        }

        storage.endBulkUpdate();

        assertEquals(90_000, storage.getInternalRows().size(),
                "Exactly 10,000 rows should have been removed");

        for (long id = 10; id <= 100_000; id += 10) {
            List<Integer> hits = storage.searchByPrimaryKey(id);
            assertTrue(hits.isEmpty(),
                    "Deleted ID=" + id + " must return no hits");
        }

        for (long id = 1; id <= 100_000; id += 10) {
            List<Integer> hits = storage.searchByPrimaryKey(id);
            assertEquals(1, hits.size(),
                    "Live ID=" + id + " must still be found");
        }
    }

    // ── CSV secondary index correctness ───────────────────────────────

    @Test
    void csvSecondaryIndexCorrectAfterBulkUpdate() {
        CsvRowStorage storage = createCsvStorage("BULK_CSV_SEC", 1000);
        // Create a secondary index on NAME before bulk ops to show the
        // deferred rebuild covers non-primary-key indexes.
        storage.getIndexManager().createIndex("NAME");

        storage.beginBulkUpdate();
        // Delete rows 100..199
        for (int i = 199; i >= 100; i--) {
            storage.delete(i);
        }
        storage.endBulkUpdate();

        assertEquals(900, storage.getInternalRows().size());

        // row-101 and row-200 are gone (indices 100..199 = IDs 101..200);
        // row-201 still present.
        List<Integer> hit101 = storage.search("NAME", "row-101");
        assertTrue(hit101.isEmpty(), "Deleted row-101 must not be in NAME index");

        List<Integer> hit200 = storage.search("NAME", "row-200");
        assertTrue(hit200.isEmpty(), "Deleted row-200 must not be in NAME index");

        List<Integer> hit201 = storage.search("NAME", "row-201");
        assertEquals(1, hit201.size(), "Live row-201 must be in NAME index");
    }

    // ── Small scale: all rows deleted (boundary) ──────────────────────

    @Test
    void bulkDeleteAllRowsLeavesStorageEmpty() {
        CsvRowStorage storage = createCsvStorage("BULK_CSV_EMPTY", 500);
        storage.beginBulkUpdate();
        for (int i = 499; i >= 0; i--) {
            storage.delete(i);
        }
        storage.endBulkUpdate();

        assertEquals(0, storage.getInternalRows().size(),
                "Deleting every row must leave storage empty");
        assertTrue(storage.searchByPrimaryKey(1L).isEmpty());
    }

    // ── Bulk insert via beginBulkUpdate + insertAt ────────────────────

    @Test
    void bulkInsertAtAndSearchCorrectness() {
        CsvRowStorage storage = createCsvStorage("BULK_CSV_INSERT", 1000);

        storage.beginBulkUpdate();
        // Insert 100 rows at the front (each would shift 1000+ positions).
        for (int i = 0; i < 100; i++) {
            storage.insertAt(0, row(200_000 + i, "bulk-" + i, i));
        }
        storage.endBulkUpdate();

        assertEquals(1100, storage.getInternalRows().size());

        // Original rows still found.
        List<Integer> hit1 = storage.searchByPrimaryKey(1L);
        assertEquals(1, hit1.size(), "Original ID=1 must remain");

        // Bulk-inserted rows found.
        for (int i = 0; i < 100; i++) {
            long key = 200_000L + i;
            List<Integer> hit = storage.searchByPrimaryKey(key);
            assertEquals(1, hit.size(),
                    "Bulk-inserted ID=" + key + " must be findable");
        }
    }

    // ── copyForTransaction uses single setRows (no per-row insertAt) ─

    @Test
    void copyForTransactionPreservesRowsAndIndex() {
        Database db = new Database();
        Table table = new Table(db, "COPY_TX", schema(), types(), "ID", Map.of());

        for (int i = 1; i <= 500; i++) {
            table.addRow(row(i, "row-" + i, i));
        }

        Table copy = table.copyForTransaction();

        assertEquals(table.getRows().size(), copy.getRows().size(),
                "Copied table must have the same row count");
        for (Map<String, Object> originalRow : table.getRows()) {
            long id = (long) originalRow.get("ID");
            List<Integer> hit = copy.getClusteredIndex().search(id);
            assertEquals(1, hit.size(),
                    "Copy must find every original primary key (ID=" + id + ")");
        }
    }
}