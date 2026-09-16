package diesel;

import diesel.storage.JsonlIndexManager;
import diesel.storage.JsonlRowStorage;
import diesel.storage.json.JsonParserConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 53 - JSONL indexing: stable monotonic row-ids and the
 * {@link JsonlIndexManager}. Covers the prompt-25 architecture (key -&gt;
 * stable rowId, rowId -> current position, {@code O(log n)} lookups and range
 * scans, tombstone compaction), secondary and nested dot-path indexes (prompt
 * 45), the crash-safe {@code <table>.idx} sidecar (adopt-when-fresh, rebuild
 * otherwise), append-mode semantics (prompt 49) and deferred bulk updates
 * (prompt 35).
 */
@Tag("storage")
class JsonlIndexTest {

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

    private static Object[] row(Object id, String name, Integer age) {
        return new Object[]{id, name, age};
    }

    private static List<Object[]> sampleRows() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(row(1L, "Alice", 25));
        rows.add(row(2L, "Bob", 30));
        rows.add(row(3L, "Carol", 35));
        rows.add(row(4L, "Dave", 40));
        rows.add(row(5L, "Eve", 30));
        return rows;
    }

    private static JsonlIndexManager manager(List<Object[]> rows, String pk) {
        JsonlIndexManager m = new JsonlIndexManager("T", COLS, JsonParserConfig.defaults());
        m.markDirty(rows, pk);
        return m;
    }

    private static JsonParserConfig strict() {
        return JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT).build();
    }

    private static JsonParserConfig strictAppend() {
        return JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .writeMode(JsonParserConfig.WriteMode.APPEND).build();
    }

    private static JsonParserConfig jsonColumn() {
        return JsonParserConfig.builder()
                .schemaMode(JsonParserConfig.SchemaMode.STRICT)
                .nestedMode(JsonParserConfig.NestedMode.JSON_COLUMN).build();
    }

    // ─── Primary-key index (stable rowIds) ────────────────────────────

    @Test
    void pkExactSearchReturnsRowIndex() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertEquals(List.of(4), m.searchByPrimaryKey(5L));
        assertTrue(m.searchByPrimaryKey(99L).isEmpty());
        assertTrue(m.getIndexColumns().contains("ID"));
        assertEquals(5, m.getRowCount());
    }

    @Test
    void pkSearchWithoutConfiguredPkReturnsEmpty() {
        JsonlIndexManager m = manager(sampleRows(), null);
        assertTrue(m.searchByPrimaryKey(1L).isEmpty());
        assertTrue(m.getIndexColumns().isEmpty());
    }

    @Test
    void pkRangeSearchIsInclusiveAndRespectsBounds() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertEquals(List.of(0, 1, 2), m.rangeSearch("ID", 1L, 3L));
        assertEquals(List.of(2, 3, 4), m.rangeSearch("ID", 3L, null));
        assertEquals(List.of(0, 1), m.rangeSearch("id", null, 2L));
        assertEquals(List.of(0), m.rangeSearch("ID", 1L, 1L));
        assertTrue(m.rangeSearch("ID", 5L, 1L).isEmpty(), "inverted bounds yield nothing");
    }

    // ─── Secondary indexes ───────────────────────────────────────────

    @Test
    void secondaryIndexEqualityAndColumns() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("NAME"));
        assertTrue(m.createIndex("NAME"), "re-creating an existing index is idempotent");
        assertFalse(m.createIndex("UNKNOWN"));
        assertTrue(m.getIndexColumns().contains("NAME"));
        assertEquals(List.of(3), m.search("NAME", "Dave"));
        assertTrue(m.search("NAME", "Nobody").isEmpty());
        // Searching by the primary-key column routes to the PK index.
        assertEquals(List.of(1), m.search("ID", 2L));
    }

    @Test
    void secondaryRangeSearch() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("AGE"));
        // 30 appears at rows 1 and 4.
        assertEquals(List.of(1, 4), m.search("AGE", 30));
        assertEquals(List.of(2, 3), m.rangeSearch("AGE", 31, 40));
        assertEquals(List.of(0, 1, 4), m.rangeSearch("age", 25, 30));
        assertTrue(m.rangeSearch("AGE", null, 24).isEmpty());
    }

    // ─── Stable rowId across clustered insert / deletion ──────────────

    @Test
    void insertInMiddleShiftsPositionsWithoutReKeying() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("NAME"));
        // A clustered insert lands at the "sorted" position between ids 2 and 3.
        m.insertAt(2, row(99L, "Zed", 50));
        assertEquals(List.of(2), m.searchByPrimaryKey(99L));
        // Existing rows keep their keys and only shift position.
        assertEquals(List.of(3), m.searchByPrimaryKey(3L));
        assertEquals(List.of(4), m.searchByPrimaryKey(4L));
        assertEquals(List.of(5), m.searchByPrimaryKey(5L));
        assertEquals(6, m.getRowCount());
        assertEquals(List.of(2), m.search("NAME", "Zed"));
    }

    @Test
    void appendRowKeepsRowsAtIndexIncrementally() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        m.appendRow(row(6L, "Frank", 45), 5);
        assertEquals(List.of(5), m.searchByPrimaryKey(6L));
        assertEquals(List.of(4), m.searchByPrimaryKey(5L));
        assertEquals(6, m.getRowCount());
    }

    @Test
    void deleteMarksTombstoneAndAutoCompacts() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        m.deleteRow(0);
        assertEquals(4, m.getRowCount());
        assertEquals(1, m.getDeletedCount(), "1 tombstone vs 5 rows is under the auto threshold");
        assertEquals(List.of(0), m.searchByPrimaryKey(2L), "positions shift down after delete");
        // The second delete pushes tombstones past 25% of live rows (2/3), which
        // triggers the automatic compaction: tombstones are cleared and rowIds
        // reassigned in the surviving rows' order.
        m.deleteRow(0);
        assertEquals(0, m.getDeletedCount(), "auto-compaction cleared the tombstones");
        assertEquals(3, m.getRowCount());
        assertEquals(List.of(0), m.searchByPrimaryKey(3L));
        assertEquals(List.of(1), m.searchByPrimaryKey(4L));
        assertEquals(List.of(2), m.searchByPrimaryKey(5L));
        assertEquals(3, m.getNextRowId(), "compact renumbered the surviving rows");
    }

    @Test
    void updateReKeysWithoutChangingStableRowId() {
        JsonlIndexManager m = manager(sampleRows(), "ID");
        assertTrue(m.createIndex("NAME"));
        Object[] oldRow = sampleRows().get(1);
        Object[] newRow = row(2L, "Bobby", 31);
        m.updateRow(oldRow, 1, newRow);
        assertEquals(List.of(1), m.search("NAME", "Bobby"));
        assertTrue(m.search("NAME", "Bob").isEmpty());
        // RowId consumption is not affected by an update.
        assertEquals(5L, m.getNextRowId());
    }

    // ─── Nested dot-path indexes (prompt 45) ──────────────────────────

    @Test
    void nestedJsonColumnPathIndexAndRange() {
        JsonlIndexManager m = new JsonlIndexManager("N",
                List.of("ID", "PROFL"), jsonColumn());
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1L, "{\"user\":{\"address\":{\"city\":\"Москва\"}}}"});
        rows.add(new Object[]{2L, "{\"user\":{\"address\":{\"city\":\"Berlin\"}}}"});
        rows.add(new Object[]{3L, "{\"user\":{\"address\":{\"city\":\"Москва\"}}}"});
        m.markDirty(rows, "ID");
        assertTrue(m.createIndex("PROFL.user.address.city"));
        assertTrue(m.getIndexColumns().contains("PROFL.user.address.city"));
        assertEquals(List.of(0, 2), m.search("PROFL.user.address.city", "Москва"));
        assertEquals(List.of(1), m.search("PROFL.user.address.city", "Berlin"));
        assertEquals(List.of(0, 2), m.rangeSearch("PROFL.user.address.city", "Москва", null));
    }

    @Test
    void unknownPathsAndNestedNullValuesStayConsistent() {
        JsonlIndexManager m = new JsonlIndexManager("N",
                List.of("ID", "PROFL"), jsonColumn());
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1L, "{\"user\":{\"name\":\"Alice\"}}"});
        m.markDirty(rows, "ID");
        assertFalse(m.createIndex("NOTACOLUMN"), "a path with no schema prefix is rejected");
        assertTrue(m.createIndex("PROFL.user.name"));
        assertEquals(List.of(0), m.search("PROFL.user.name", "Alice"));
        // A path that merely prefixes a column resolves (to an empty nested index).
        assertTrue(m.createIndex("PROFL.does.not.exist"));
        assertTrue(m.search("PROFL.does.not.exist", "x").isEmpty());
        // A null nested value indexes to nothing, not a key; the existing
        // nested index keeps the mapping under the shifted row.
        rows.add(new Object[]{2L, "{\"user\":null}"});
        m.insertAt(1, rows.get(1));
        assertEquals(List.of(0), m.search("PROFL.user.name", "Alice"));
        assertEquals(2, m.getRowCount());
    }

    @Test
    void flattenDottedPathIsIndexedAsLiteralColumn() {
        JsonlIndexManager m = new JsonlIndexManager("F",
                List.of("ID", "user.address.city"), JsonParserConfig.defaults());
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1L, "Москва"});
        rows.add(new Object[]{2L, "Berlin"});
        rows.add(new Object[]{3L, "Москва"});
        m.markDirty(rows, "ID");
        assertTrue(m.createIndex("user.address.city"));
        assertEquals(List.of(0, 2), m.search("user.address.city", "Москва"));
        assertEquals(List.of(1), m.search("user.address.city", "Berlin"));
        assertEquals(List.of(0, 2), m.rangeSearch("user.address.city", "Москва", null));
    }

    // ─── Deferred bulk updates (prompt 35) ────────────────────────────

    @Test
    void bulkDeleteWindowDeferredRebuildStaysLinear() {
        JsonParserConfig cfg = strict();
        JsonlRowStorage storage = new JsonlRowStorage("BULK", COLS, types(), cfg);
        storage.setDataDir(tempDir.toString());
        storage.setPrimaryKeyColumn("ID");
        storage.open();
        int count = 10_000;
        for (int id = 1; id <= count; id++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("ID", (long) id);
            r.put("NAME", "n" + id);
            r.put("AGE", id % 100);
            storage.insert(r);
        }
        assertEquals(count, storage.getJsonlIndexManager().getRowCount());
        long start = System.nanoTime();
        storage.beginBulkUpdate();
        try {
            for (int i = count; i > 0; i--) {
                storage.delete(i - 1);
            }
        } finally {
            storage.endBulkUpdate();
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, storage.getJsonlIndexManager().getRowCount());
        assertTrue(elapsedMs < 5_000, "bulk delete must stay near-linear, took " + elapsedMs + " ms");
        storage.close();
    }

    @Test
    void bulkInsertDefersPerOperationRebuild() {
        JsonlIndexManager m = new JsonlIndexManager("T", COLS, JsonParserConfig.defaults());
        List<Object[]> rows = new ArrayList<>();
        rows.add(row(1L, "Alice", 25));
        m.markDirty(rows, "ID");
        m.beginBulkUpdate();
        try {
            m.insertAt(1, row(2L, "Bob", 30));
            m.insertAt(2, row(3L, "Carol", 35));
            assertTrue(m.isBulkUpdating());
        } finally {
            m.endBulkUpdate();
        }
        assertFalse(m.isBulkUpdating());
        assertEquals(List.of(1), m.searchByPrimaryKey(2L));
        assertEquals(List.of(2), m.searchByPrimaryKey(3L));
        assertEquals(3, m.getRowCount());
    }

    // ─── Sidecar persistence and reload (prompt 53) ───────────────────

    private void insertThree(JsonlRowStorage storage) {
        Map<String, Object> r1 = new LinkedHashMap<>();
        r1.put("ID", 1L);
        r1.put("NAME", "Alice");
        r1.put("AGE", 25);
        Map<String, Object> r2 = new LinkedHashMap<>();
        r2.put("ID", 2L);
        r2.put("NAME", "Bob");
        r2.put("AGE", 30);
        Map<String, Object> r3 = new LinkedHashMap<>();
        r3.put("ID", 3L);
        r3.put("NAME", "Carol");
        r3.put("AGE", 35);
        storage.insert(r1);
        storage.insert(r2);
        storage.insert(r3);
    }

    @Test
    void sidecarPersistAndReloadAdoptsFastPath() throws Exception {
        String name = "SIDE";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strict());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        a.saveToFile(name);
        assertTrue(new File(tempDir.toString(), name + ".idx").exists(),
                "index sidecar is persisted next to the data");
        a.close();

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strict());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertTrue(m.isLoadedFromSidecar(), "matching mtime/size/schema adopts the sidecar");
        assertEquals(List.of(1), m.searchByPrimaryKey(2L));
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        assertTrue(m.getRowCount() >= 3);
        b.close();
    }

    @Test
    void staleSidecarRebuildsFromData() throws Exception {
        String name = "STALE";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strict());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        a.saveToFile(name);
        a.close();
        // Tamper with the data file: the stamp no longer matches the sidecar.
        Path data = tempDir.resolve(name + ".jsonl");
        Files.write(data, "{\"ID\":4,\"NAME\":\"Dave\",\"AGE\":40}\n".getBytes(StandardCharsets.UTF_8),
                java.nio.file.StandardOpenOption.APPEND);

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strict());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertFalse(m.isLoadedFromSidecar(), "changed data stamp must trigger a rebuild");
        assertEquals(List.of(3), m.searchByPrimaryKey(4L));
        assertEquals(List.of(1), m.searchByPrimaryKey(2L));
        assertEquals(4, m.getRowCount());
        b.close();
    }

    @Test
    void corruptOrTruncatedSidecarDoesNotBreakLoad() throws Exception {
        String name = "CORR";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strict());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        a.saveToFile(name);
        a.close();
        Path idx = tempDir.resolve(name + ".idx");
        Files.write(idx, "not a java serialized object".getBytes(StandardCharsets.UTF_8));
        // An orphaned temp from an interrupted sidecar write must be harmless too.
        Files.write(tempDir.resolve(name + ".idx.tmp"), new byte[]{1, 2, 3});

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strict());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertFalse(m.isLoadedFromSidecar());
        assertEquals(3, m.getRowCount());
        assertEquals(List.of(2), m.searchByPrimaryKey(3L));
        assertEquals(3, b.scan().size(), "data survives a bad sidecar");
        b.close();
    }

    @Test
    void secondaryIndexPersistsAndAdopts() throws Exception {
        String name = "SIDE2";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strict());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        assertTrue(a.getJsonlIndexManager().createIndex("NAME"));
        a.saveToFile(name);
        a.close();

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strict());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertTrue(m.isLoadedFromSidecar());
        assertTrue(m.getIndexColumns().contains("NAME"));
        assertEquals(List.of(1), m.search("NAME", "Bob"));
        assertEquals(List.of(2), m.search("NAME", "Carol"));
        b.close();
    }

    // ─── Append mode (prompt 49) ──────────────────────────────────────

    @Test
    void appendModeRowIdsIncrementAcrossSavesAndReload() throws Exception {
        String name = "APPEND";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strictAppend());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        a.saveToFile(name);
        // Insert two more after the first save - they land in the base file on the next save.
        Map<String, Object> r4 = new LinkedHashMap<>();
        r4.put("ID", 4L);
        r4.put("NAME", "Dave");
        r4.put("AGE", 40);
        Map<String, Object> r5 = new LinkedHashMap<>();
        r5.put("ID", 5L);
        r5.put("NAME", "Eve");
        r5.put("AGE", 45);
        a.insert(r4);
        a.insert(r5);
        a.saveToFile(name);
        a.close();

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strictAppend());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertTrue(m.isLoadedFromSidecar());
        assertEquals(5, m.getRowCount());
        assertEquals(List.of(4), m.searchByPrimaryKey(5L));
        // Fresh rowIds keep growing - never reused.
        long next = m.getNextRowId();
        Map<String, Object> r6 = new LinkedHashMap<>();
        r6.put("ID", 6L);
        r6.put("NAME", "Frank");
        r6.put("AGE", 50);
        b.insert(r6);
        assertEquals(next, m.getNextRowId() - 1, "the appended row consumed the next fresh rowId");
        b.close();
    }

    @Test
    void appendModeDeleteDeltaReloadStaysConsistent() throws Exception {
        String name = "APPENDDEL";
        JsonlRowStorage a = new JsonlRowStorage(name, COLS, types(), strictAppend());
        a.setDataDir(tempDir.toString());
        a.setPrimaryKeyColumn("ID");
        a.open();
        insertThree(a);
        Map<String, Object> r4 = new LinkedHashMap<>();
        r4.put("ID", 4L);
        r4.put("NAME", "Dave");
        r4.put("AGE", 40);
        Map<String, Object> r5 = new LinkedHashMap<>();
        r5.put("ID", 5L);
        r5.put("NAME", "Eve");
        r5.put("AGE", 45);
        a.insert(r4);
        a.insert(r5);
        a.saveToFile(name);
        // Delete the row at position 2 (id 3) - recorded as a delta.
        a.delete(2);
        a.saveToFile(name);
        assertEquals(4, a.getJsonlIndexManager().getRowCount());
        a.close();

        JsonlRowStorage b = new JsonlRowStorage(name, COLS, types(), strictAppend());
        b.setDataDir(tempDir.toString());
        b.setPrimaryKeyColumn("ID");
        b.open();
        b.loadFromFile(name);
        JsonlIndexManager m = b.getJsonlIndexManager();
        assertEquals(4, b.scan().size(), "delta deletion is applied to the reloaded base");
        assertEquals(4, m.getRowCount());
        assertTrue(m.searchByPrimaryKey(3L).isEmpty());
        assertEquals(List.of(2), m.searchByPrimaryKey(4L));
        assertEquals(List.of(0), m.searchByPrimaryKey(1L));
        b.close();
    }

    // ─── Engine integration ───────────────────────────────────────────

    @Test
    void engineCreatesAndIndexesJsonlTable() throws Exception {
        String prev = System.getProperty("diesel.storage.type");
        try {
            System.setProperty("diesel.storage.type", "jsonl");
            Database db = new Database(tempDir.toString());
            db.executeQuery("CREATE TABLE EQ (ID LONG PRIMARY KEY, NAME STRING)", null);
            db.executeQuery("INSERT INTO EQ (ID, NAME) VALUES (1, 'Alice')", null);
            db.executeQuery("INSERT INTO EQ (ID, NAME) VALUES (2, 'Bob')", null);
            db.executeQuery("INSERT INTO EQ (ID, NAME) VALUES (3, 'Carol')", null);

            Table table = db.getTable("EQ");
            assertTrue(table.getStorage() instanceof JsonlRowStorage,
                    "engine tables are JSONL-backed under diesel.storage.type=jsonl");
            JsonlIndexManager m = ((JsonlRowStorage) table.getStorage()).getJsonlIndexManager();
            assertEquals(List.of(0), m.searchByPrimaryKey(1L));
            assertEquals(List.of(1), m.searchByPrimaryKey(2L));
            assertEquals(List.of(2), m.searchByPrimaryKey(3L));
            assertTrue(m.getIndexColumns().contains("ID"));

            table.saveToFile("EQ");
            assertTrue(new File(tempDir.toString(), "EQ.idx").exists(),
                    "engine save writes the index sidecar");

            // DROP TABLE removes the data and the index sidecar.
            db.dropTable("EQ");
            assertFalse(new File(tempDir.toString(), "EQ.jsonl").exists());
            assertFalse(new File(tempDir.toString(), "EQ.idx").exists());
            assertFalse(new File(tempDir.toString(), "EQ.idx.tmp").exists());
        } finally {
            if (prev == null) {
                System.clearProperty("diesel.storage.type");
            } else {
                System.setProperty("diesel.storage.type", prev);
            }
        }
    }
}