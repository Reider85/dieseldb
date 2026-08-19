package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BulkInsertTest {

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try { database.dropTable("BULK"); } catch (TableNotFoundException ignored) { }
    }

    // ── helpers ──────────────────────────────────────────────────────

    private void createTable(String ddl) {
        database.executeQuery(ddl, null);
    }

    private Table table(String name) {
        return database.getTable(name);
    }

    private Map<String, Object> row(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    // ── bulkInsert ───────────────────────────────────────────────────

    @Test
    void bulkInsertAddsAllRows() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING, AGE INTEGER)");
        List<Map<String, Object>> batch = List.of(
                row("ID", 1, "NAME", "Alice", "AGE", 25),
                row("ID", 2, "NAME", "Bob",   "AGE", 30),
                row("ID", 3, "NAME", "Carol", "AGE", 35));
        table("BULK").bulkInsert(batch);

        Object r = database.executeQuery("SELECT ID, NAME FROM BULK ORDER BY ID", null);
        List<?> rows = (List<?>) r;
        assertEquals(3, rows.size());
        assertEquals("Alice", ((Map<?, ?>) rows.get(0)).get("NAME"));
        assertEquals("Carol", ((Map<?, ?>) rows.get(2)).get("NAME"));
    }

    @Test
    void bulkInsertRespectsClusteredOrder() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        List<Map<String, Object>> batch = List.of(
                row("ID", 3, "NAME", "C"),
                row("ID", 1, "NAME", "A"),
                row("ID", 2, "NAME", "B"));
        table("BULK").bulkInsert(batch);

        Object r = database.executeQuery("SELECT ID FROM BULK", null);
        List<?> rows = (List<?>) r;
        assertEquals(1, ((Map<?, ?>) rows.get(0)).get("ID"));
        assertEquals(2, ((Map<?, ?>) rows.get(1)).get("ID"));
        assertEquals(3, ((Map<?, ?>) rows.get(2)).get("ID"));
    }

    @Test
    void bulkInsertWithSecondaryIndex() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        table("BULK").createBTreeIndex("NAME");

        List<Map<String, Object>> batch = List.of(
                row("ID", 1, "NAME", "Alice"),
                row("ID", 2, "NAME", "Bob"),
                row("ID", 3, "NAME", "Alice"));
        table("BULK").bulkInsert(batch);

        Index idx = table("BULK").getIndex("NAME");
        assertNotNull(idx);
        List<Integer> hits = idx.search("Alice");
        assertEquals(2, hits.size());
    }

    @Test
    void bulkInsertDuplicateClusteredKeyThrows() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        List<Map<String, Object>> batch = List.of(
                row("ID", 1, "NAME", "A"),
                row("ID", 1, "NAME", "B"));
        assertThrows(IllegalStateException.class,
                () -> table("BULK").bulkInsert(batch));
    }

    @Test
    void bulkInsertDuplicateUniqueKeyThrows() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, CODE STRING)");
        table("BULK").createUniqueIndex("CODE");

        List<Map<String, Object>> batch = List.of(
                row("ID", 1, "CODE", "X"),
                row("ID", 2, "CODE", "X"));
        assertThrows(IllegalStateException.class,
                () -> table("BULK").bulkInsert(batch));
    }

    @Test
    void bulkInsertEmptyListIsNoop() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY)");
        table("BULK").bulkInsert(List.of());
        assertEquals(0, table("BULK").rowCount());
    }

    @Test
    void bulkInsertThenSingleInsertWorks() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        table("BULK").bulkInsert(List.of(
                row("ID", 1, "NAME", "A"),
                row("ID", 3, "NAME", "C")));

        // single insert interleaved
        table("BULK").addRow(row("ID", 2, "NAME", "B"));

        Object r = database.executeQuery("SELECT ID FROM BULK ORDER BY ID", null);
        List<?> rows = (List<?>) r;
        assertEquals(3, rows.size());
        assertEquals(2, ((Map<?, ?>) rows.get(1)).get("ID"));
    }

    // ── disableIndices / enableAndRebuildIndices ──────────────────────

    @Test
    void bulkLoadModeSkipsIndexUpdates() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        table("BULK").createBTreeIndex("NAME");

        table("BULK").disableIndices();
        assertTrue(table("BULK").isIndicesDisabled());

        table("BULK").addRow(row("ID", 1, "NAME", "Alice"));
        table("BULK").addRow(row("ID", 2, "NAME", "Bob"));

        // Index is empty while disabled
        Index idx = table("BULK").getIndex("NAME");
        assertTrue(idx.search("Alice").isEmpty());

        // Rebuild
        table("BULK").enableAndRebuildIndices();
        assertFalse(table("BULK").isIndicesDisabled());
        // Re-fetch index after rebuild (rebuild replaces the index object)
        idx = table("BULK").getIndex("NAME");
        assertEquals(2, idx.search("Alice").size() + idx.search("Bob").size());
        assertEquals(1, idx.search("Alice").size());
        assertEquals(1, idx.search("Bob").size());
    }

    @Test
    void bulkLoadModeInsertsManyRowsThenRebuilds() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        table("BULK").createBTreeIndex("NAME");

        table("BULK").disableIndices();
        for (int i = 1; i <= 100; i++) {
            table("BULK").addRow(row("ID", i, "NAME", "User" + i));
        }
        assertEquals(100, table("BULK").rowCount());
        table("BULK").enableAndRebuildIndices();

        Index idx = table("BULK").getIndex("NAME");
        assertEquals(1, idx.search("User42").size());
        assertEquals(41, idx.search("User42").get(0).intValue());
    }

    // ── deferIndexUpdates / flushDeferredIndexUpdates ─────────────────

    @Test
    void deferredIndexModeQueuesAndFlushes() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING)");
        table("BULK").createBTreeIndex("NAME");

        table("BULK").deferIndexUpdates();
        table("BULK").addRow(row("ID", 1, "NAME", "Alice"));
        table("BULK").addRow(row("ID", 2, "NAME", "Bob"));

        table("BULK").flushDeferredIndexUpdates();

        Index idx = table("BULK").getIndex("NAME");
        assertEquals(1, idx.search("Alice").size());
        assertEquals(1, idx.search("Bob").size());
    }

    // ── stress / correctness ─────────────────────────────────────────

    @Test
    void bulkInsertLargeBatchMaintainsIndexConsistency() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING, AGE INTEGER)");
        table("BULK").createBTreeIndex("NAME");
        table("BULK").createBTreeIndex("AGE");

        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            batch.add(row("ID", i, "NAME", "User" + i, "AGE", 20 + (i % 50)));
        }
        table("BULK").bulkInsert(batch);

        assertEquals(500, table("BULK").rowCount());

        // Clustered index correct
        Object r = database.executeQuery("SELECT ID FROM BULK ORDER BY ID", null);
        List<?> rows = (List<?>) r;
        assertEquals(500, rows.size());
        assertEquals(1, ((Map<?, ?>) rows.get(0)).get("ID"));
        assertEquals(500, ((Map<?, ?>) rows.get(499)).get("ID"));

        // Secondary indexes correct
        Index nameIdx = table("BULK").getIndex("NAME");
        assertNotNull(nameIdx);
        assertEquals(1, nameIdx.search("User1").size());
        assertEquals(1, nameIdx.search("User500").size());

        Index ageIdx = table("BULK").getIndex("AGE");
        assertNotNull(ageIdx);
        // AGE = 20 appears for IDs 1, 51, 101, 151, 201, 251, 301, 351, 401, 451 = 10 times
        assertEquals(10, ageIdx.search(20).size());
    }

    @Test
    void bulkInsertThenQueryWhereWorks() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING, AGE INTEGER)");
        table("BULK").bulkInsert(List.of(
                row("ID", 1, "NAME", "Alice", "AGE", 25),
                row("ID", 2, "NAME", "Bob",   "AGE", 30),
                row("ID", 3, "NAME", "Carol", "AGE", 25)));

        Object r = database.executeQuery("SELECT NAME FROM BULK WHERE AGE = 25 ORDER BY ID", null);
        List<?> rows = (List<?>) r;
        assertEquals(2, rows.size());
        assertEquals("Alice", ((Map<?, ?>) rows.get(0)).get("NAME"));
        assertEquals("Carol", ((Map<?, ?>) rows.get(1)).get("NAME"));
    }

    @Test
    void bulkInsertMixedTypes() {
        createTable("CREATE TABLE BULK (ID INTEGER PRIMARY KEY, NAME STRING, SCORE DOUBLE, ACTIVE BOOLEAN)");
        table("BULK").bulkInsert(List.of(
                row("ID", 1, "NAME", "Alice", "SCORE", 9.5, "ACTIVE", true),
                row("ID", 2, "NAME", "Bob",   "SCORE", 7.0, "ACTIVE", false)));

        Object r = database.executeQuery("SELECT NAME, SCORE FROM BULK WHERE ACTIVE = TRUE", null);
        List<?> rows = (List<?>) r;
        assertEquals(1, rows.size());
        assertEquals("Alice", ((Map<?, ?>) rows.get(0)).get("NAME"));
    }
}
