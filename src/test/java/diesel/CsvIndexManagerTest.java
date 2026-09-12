package diesel;

import diesel.storage.CsvIndexManager;
import diesel.storage.CsvRowStorage;
import diesel.storage.CsvRowWriter;
import diesel.storage.DelimitedIndexManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the CSV application of prompt 23 (indexing and search): the
 * format-agnostic {@link DelimitedIndexManager} driven through
 * {@link CsvIndexManager} and integrated into {@link CsvRowStorage}.
 */
class CsvIndexManagerTest {

    @TempDir
    Path tempDir;

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

    private static List<Map<String, Object>> sampleRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row(1L, "Alice", 25));
        rows.add(row(2L, "Bob", 30));
        rows.add(row(3L, "Carol", 35));
        rows.add(row(4L, "Dave", 40));
        rows.add(row(5L, "Eve", 30));
        return rows;
    }

    private static CsvIndexManager manager(List<Map<String, Object>> rows, String pk) {
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        m.buildIndexes(rows, pk);
        return m;
    }

    private void setCsvConfig(String blockSize, String threshold) {
        System.setProperty("csv.block.size", blockSize);
        System.setProperty("csv.parallel.read.threshold", threshold);
    }

    private void clearCsvConfig() {
        System.clearProperty("csv.block.size");
        System.clearProperty("csv.cache.max.blocks");
        System.clearProperty("csv.parallel.read.threshold");
    }

    // ─── Primary-key index ───────────────────────────────────────────

    @Test
    void pkExactSearchReturnsRowIndex() {
        CsvIndexManager m = manager(sampleRows(), "ID");
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(4), m.searchByPrimaryKey(5L));
        assertTrue(m.searchByPrimaryKey(99L).isEmpty());
        assertTrue(m.getIndexColumns().contains("ID"));
    }

    @Test
    void pkSearchWithoutConfiguredPkReturnsEmpty() {
        CsvIndexManager m = manager(sampleRows(), null);
        assertTrue(m.searchByPrimaryKey(1L).isEmpty());
        assertTrue(m.getIndexColumns().isEmpty());
    }

    @Test
    void pkRangeSearchIsInclusiveAndRespectsBounds() {
        CsvIndexManager m = manager(sampleRows(), "ID");
        assertEquals(List.of(0, 1, 2), m.rangeSearch("ID", 1L, 3L));
        assertEquals(List.of(2, 3, 4), m.rangeSearch("ID", 3L, null));
        assertEquals(List.of(0, 1), m.rangeSearch("id", null, 2L));
        assertEquals(List.of(0), m.rangeSearch("ID", 1L, 1L));
    }

    // ─── Secondary indexes ───────────────────────────────────────────

    @Test
    void secondaryIndexEqualityAndColumns() {
        CsvIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("NAME"));
        assertTrue(m.createIndex("NAME"));
        assertTrue(m.getIndexColumns().contains("NAME"));
        assertEquals(List.of(3), m.search("NAME", "Dave"));
        assertTrue(m.search("NAME", "Nobody").isEmpty());
        assertTrue(m.search("UNKNOWN", "x").isEmpty());
        // Searching by the primary-key column routes to the PK index.
        assertEquals(List.of(1), m.search("ID", 2L));
    }

    @Test
    void secondaryRangeSearch() {
        CsvIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("AGE"));
        // 30 appears at rows 1 and 4.
        assertEquals(List.of(1, 4), m.search("AGE", 30));
        assertEquals(List.of(2, 3), m.rangeSearch("AGE", 31, 40));
        assertEquals(List.of(0, 1, 4), m.rangeSearch("age", 25, 30));
        assertTrue(m.rangeSearch("AGE", null, 24).isEmpty());
    }

    @Test
    void incrementalInsertAndRemoveKeepIndexesConsistent() {
        CsvIndexManager m = manager(new ArrayList<>(sampleRows()), "ID");
        // Append a row — uses appendIndexedRow (assigns rowId 5, position 5)
        m.appendIndexedRow(row(6L, "Frank", 20), 5);
        assertEquals(List.of(5), m.searchByPrimaryKey(6L));
        // Delete row at position 1 (ID=2) — shifts later positions down by 1
        m.deleteRow(1);
        assertTrue(m.searchByPrimaryKey(2L).isEmpty());
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        // After delete, row with ID=6 shifted from pos 5 to pos 4
        assertEquals(List.of(4), m.searchByPrimaryKey(6L));
    }

    @Test
    void deleteThenReindexReflectsRowShift() {
        CsvIndexManager m = manager(sampleRows(), "ID");
        List<Map<String, Object>> current = new ArrayList<>();
        for (int i = 1; i < sampleRows().size(); i++) {
            current.add(sampleRows().get(i));
        }
        m.buildIndexes(current, "ID");
        assertEquals(List.of(0), m.searchByPrimaryKey(2L));
        assertEquals(List.of(3), m.searchByPrimaryKey(5L));
        assertEquals(4, m.getRowCount());
    }

    // ─── Block cache ─────────────────────────────────────────────────

    @Test
    void blockSlicingStillWorks() {
        // Force a small block size via system property so 5 rows => 3 blocks of 2.
        try {
            setCsvConfig("2", "10000");
            CsvIndexManager cm = manager(sampleRows(), "ID");
            assertEquals(3, cm.getNumBlocks());
            assertEquals(2, cm.getBlockSize());
            assertEquals(2, cm.getBlock(0).getRows().size());
            assertEquals(1, cm.getBlock(2).getRows().size());
            assertEquals("Alice", cm.getBlock(0).getRows().get(0).get("NAME"));
            assertThrows(IndexOutOfBoundsException.class, () -> cm.getBlock(3));
            // Deprecation stubs (prompt 33): blocks slice rows on demand, no LRU cache.
            assertEquals(0, cm.getCacheMissCount());
            cm.getBlock(0);
            cm.getBlock(0);
            assertEquals(0, cm.getCacheHitCount());
        } finally {
            clearCsvConfig();
        }
    }

    @Test
    void loadAllBlocksParallelMatchesSequentialBlockScan() {
        try {
            setCsvConfig("2", "4");
            CsvIndexManager m = manager(sampleRows(), "ID");
            List<DelimitedIndexManager.Block> blocks = m.loadAllBlocksParallel();
            assertEquals(3, blocks.size());
            int i = 0;
            for (DelimitedIndexManager.Block block : blocks) {
                assertEquals(i, block.getBlockIndex());
                List<Map<String, Object>> expected = i == 2 ? List.of(sampleRows().get(4))
                        : sampleRows().subList(i * 2, i * 2 + 2);
                assertEquals(expected.size(), block.getRows().size());
                i++;
            }
        } finally {
            clearCsvConfig();
        }
    }

    // ─── Parallel file reading ───────────────────────────────────────

    @Test
    void parallelFileLoadMatchesSequential() throws Exception {
        List<Map<String, Object>> source = new ArrayList<>();
        for (long id = 1; id <= 60; id++) {
            source.add(row(id, "User" + id, (int) (id % 47)));
        }
        File csv = writeCsv("parallel.csv", source);

        try {
            setCsvConfig("10", "20");
            CsvIndexManager m = new CsvIndexManager("T", COLS, types());
            List<Map<String, Object>> parallel = m.loadFromFileParallel(csv.getPath());
            List<Map<String, Object>> sequential = m.loadFromFileSequential(csv.getPath());
            assertEquals(parallel.size(), sequential.size());
            for (int i = 0; i < parallel.size(); i++) {
                assertEquals(sequential.get(i).get("ID"), parallel.get(i).get("ID"));
                assertEquals(sequential.get(i).get("NAME"), parallel.get(i).get("NAME"));
            }
        } finally {
            clearCsvConfig();
        }
    }

    @Test
    void multiLineQuotedFieldForcesSequentialFallback() throws Exception {
        List<Map<String, Object>> source = new ArrayList<>();
        source.add(row(1L, "plain", 20));
        Map<String, Object> multi = row(2L, "line1\nline2", 21);
        source.add(multi);
        source.add(row(3L, "tail", 22));
        File csv = writeCsv("multiline.csv", source);

        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> parallel = m.loadFromFileParallel(csv.getPath());
        List<Map<String, Object>> sequential = m.loadFromFileSequential(csv.getPath());
        assertEquals(3, parallel.size());
        assertEquals(3, sequential.size());
        for (int i = 0; i < parallel.size(); i++) {
            assertEquals(sequential.get(i).get("NAME"), parallel.get(i).get("NAME"));
        }
        assertEquals("line1\nline2", parallel.get(1).get("NAME"));
    }

    @Test
    void loadAndIndexBuildsPrimaryKeyIndex() throws Exception {
        File csv = writeCsv("loadandindex.csv", sampleRows());
        CsvIndexManager m = new CsvIndexManager("T", COLS, types());
        List<Map<String, Object>> loaded = m.loadAndIndex(csv.getPath(), "ID", true);
        assertEquals(5, loaded.size());
        assertEquals(List.of(3), m.searchByPrimaryKey(4L));
    }

    // ─── Configuration ───────────────────────────────────────────────

    @Test
    void configKeysHonorSystemProperties() {
        try {
            setCsvConfig("5", "999");
            CsvIndexManager m = new CsvIndexManager("T", COLS, types());
            assertEquals(5, m.getBlockSize());
        } finally {
            clearCsvConfig();
        }
    }

    // ─── CsvRowStorage integration ───────────────────────────────────

    @Test
    void storagePrimaryKeySearchAfterInsert() {
        CsvRowStorage s = new CsvRowStorage("T", COLS, types());
        s.open();
        s.setPrimaryKeyColumn("ID");
        s.insert(row(1L, "Alice", 25));
        s.insert(row(2L, "Bob", 30));
        s.insert(row(3L, "Carol", 35));
        assertTrue(s.isIndexed());
        List<Integer> idx = s.searchByPrimaryKey(2L);
        assertEquals(List.of(1), idx);
        assertEquals("Bob", s.scan().get(idx.get(0)).get("NAME"));
        assertEquals(List.of(0, 1, 2), s.rangeSearch("ID", 1L, 3L));
    }

    @Test
    void storageUpdateAndDeleteMaintainIndex() {
        CsvRowStorage s = new CsvRowStorage("T", COLS, types());
        s.open();
        s.setPrimaryKeyColumn("ID");
        s.insert(row(1L, "Alice", 25));
        s.insert(row(2L, "Bob", 30));
        s.insert(row(3L, "Carol", 35));

        s.update(1, row(2L, "ROBERT", 31));
        assertEquals(List.of(1), s.searchByPrimaryKey(2L));
        assertEquals("ROBERT", s.scan().get(1).get("NAME"));

        // Physical delete of row 0 shifts later indexes down by one.
        s.delete(0);
        assertEquals(List.of(0), s.searchByPrimaryKey(2L));
        assertEquals(List.of(1), s.searchByPrimaryKey(3L));
        assertEquals(2, s.scan().size());
    }

    @Test
    void storageSaveLoadRebuildsIndex() {
        CsvRowStorage s = new CsvRowStorage("T", COLS, types());
        s.setDataDir(tempDir.toString());
        s.open();
        s.setPrimaryKeyColumn("ID");
        s.insert(row(1L, "Alice", 25));
        s.insert(row(2L, "Bob", 30));
        s.insert(row(3L, "Carol", 35));
        s.saveToFile("T");
        assertTrue(new File(tempDir.toString(), "T.csv").exists());

        CsvRowStorage loaded = new CsvRowStorage("T", COLS, types());
        loaded.setDataDir(tempDir.toString());
        loaded.open();
        loaded.setPrimaryKeyColumn("ID");
        loaded.loadFromFile("T");
        assertEquals(3, loaded.scan().size());
        assertEquals(List.of(2), loaded.searchByPrimaryKey(3L));
        assertEquals("Carol", loaded.scan().get(2).get("NAME"));
    }

    @Test
    void storageParallelLoadFromFile() throws Exception {
        List<Map<String, Object>> source = new ArrayList<>();
        for (long id = 1; id <= 60; id++) {
            source.add(row(id, "User" + id, (int) (id % 47)));
        }
        writeCsv("BIG.csv", source);

        try {
            setCsvConfig("10", "20");
            CsvRowStorage s = new CsvRowStorage("BIG", COLS, types());
            s.setDataDir(tempDir.toString());
            s.open();
            s.setPrimaryKeyColumn("ID");
            s.loadFromFile("BIG", true);
            assertEquals(60, s.scan().size());
            assertEquals(List.of(37), s.searchByPrimaryKey(38L));

            CsvRowStorage sequential = new CsvRowStorage("BIG", COLS, types());
            sequential.setDataDir(tempDir.toString());
            sequential.open();
            sequential.setPrimaryKeyColumn("ID");
            sequential.loadFromFile("BIG", false);
            assertEquals(s.scan().size(), sequential.scan().size());
            for (int i = 0; i < s.scan().size(); i++) {
                assertEquals(sequential.scan().get(i).get("ID"), s.scan().get(i).get("ID"));
            }
        } finally {
            clearCsvConfig();
        }
    }

    @Test
    void inMemoryStorageHasNoIndexManager() {
        diesel.storage.InMemoryRowStorage s = new diesel.storage.InMemoryRowStorage("T", COLS, types());
        s.open();
        s.setPrimaryKeyColumn("ID");
        assertFalse(s.isIndexed());
    }

    // ─── Prompt 25: stable row-id tests ──────────────────────────────

    @Test
    void clusteredInsertAtMiddleThenSearchCorrect() {
        CsvIndexManager m = manager(new ArrayList<>(sampleRows()), "ID");
        // Insert row with ID=10 at position 2 (between Bob and Carol)
        Map<String, Object> inserted = row(10L, "Zara", 28);
        m.insertAt(2, inserted);

        // Existing rows shifted: Alice=0, Bob=1, Zara=2, Carol=3, Dave=4, Eve=5
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(1), m.searchByPrimaryKey(2L));
        assertEquals(List.of(2), m.searchByPrimaryKey(10L));
        assertEquals(List.of(3), m.searchByPrimaryKey(3L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(List.of(5), m.searchByPrimaryKey(5L));
        assertEquals(6, m.getRowCount());
    }

    @Test
    void secondaryIndexCorrectAfterInsertAt() {
        CsvIndexManager m = manager(new ArrayList<>(sampleRows()), "ID");
        m.createIndex("AGE");
        // Before insert: AGE=30 at positions [1, 4]
        assertEquals(List.of(1, 4), m.search("AGE", 30));

        // Insert at position 2: Bob shifts to 1 (unchanged), new row at 2, Carol shifts to 3, etc.
        m.insertAt(2, row(10L, "Zara", 30));
        // AGE=30 should now be at positions [1, 2, 5] (Bob, Zara, Eve)
        assertEquals(List.of(1, 2, 5), m.search("AGE", 30));
    }

    @Test
    void massInsertAtDoesNotDegradeToQuadratic() {
        CsvIndexManager m = manager(new ArrayList<>(), "ID");
        // Start with one row
        m.appendIndexedRow(row(1L, "First", 10), 0);

        long start = System.nanoTime();
        // Insert 1000 rows at position 0 (always shifting everything)
        for (int i = 2; i <= 1000; i++) {
            m.insertAt(0, row(i, "Row" + i, 10));
        }
        long elapsed = System.nanoTime() - start;

        assertEquals(1000, m.getRowCount());
        // Verify first and last are findable
        assertEquals(List.of(999), m.searchByPrimaryKey(1L));
        assertEquals(List.of(0), m.searchByPrimaryKey(1000L));
        // Should complete in under 2 seconds (O(n) per insert, 1000 inserts)
        assertTrue(elapsed < 2_000_000_000L, "Mass insertAt took " + (elapsed / 1_000_000) + "ms, expected < 2000ms");
    }

    @Test
    void deleteThenInsertAtMaintainsIndexCorrectness() {
        CsvIndexManager m = manager(new ArrayList<>(sampleRows()), "ID");
        // Delete Bob (position 1)
        m.deleteRow(1);
        assertEquals(4, m.getRowCount());
        assertTrue(m.searchByPrimaryKey(2L).isEmpty());
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(1), m.searchByPrimaryKey(3L));

        // Now insert at position 1 (between Alice and Carol)
        m.insertAt(1, row(2L, "Bobby", 32));
        assertEquals(5, m.getRowCount());
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(1), m.searchByPrimaryKey(2L));
        assertEquals(List.of(2), m.searchByPrimaryKey(3L));
    }

    @Test
    void storageInsertAtThenSearchCorrect() {
        CsvRowStorage s = new CsvRowStorage("T", COLS, types());
        s.open();
        s.setPrimaryKeyColumn("ID");
        s.insert(row(1L, "Alice", 25));
        s.insert(row(2L, "Bob", 30));
        s.insert(row(3L, "Carol", 35));

        // InsertAt position 1 (between Alice and Bob)
        s.insertAt(1, row(10L, "Zara", 28));

        // Storage: Alice=0, Zara=1, Bob=2, Carol=3
        assertEquals(List.of(0), s.searchByPrimaryKey(1L));
        assertEquals(List.of(1), s.searchByPrimaryKey(10L));
        assertEquals(List.of(2), s.searchByPrimaryKey(2L));
        assertEquals(List.of(3), s.searchByPrimaryKey(3L));
        assertEquals(4, s.scan().size());
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private File writeCsv(String fileName, List<Map<String, Object>> rows) throws Exception {
        File csvFile = new File(tempDir.toString(), fileName);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
             CsvRowWriter w = new CsvRowWriter(bw, COLS)) {
            w.writeHeader();
            for (Map<String, Object> r : rows) {
                w.writeRow(r);
            }
        }
        return csvFile;
    }
}