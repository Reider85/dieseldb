package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutoJoinIndexTest {

    private Database database;
    private List<LogRecord> captured;
    private Handler handler;

    @BeforeEach
    void setUp() {
        database = new Database();
        dropTables();
        Logger logger = Logger.getLogger("diesel.SelectQuery");
        captured = new ArrayList<>();
        handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                captured.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(handler);
    }

    private void reattachHandler() {
        Logger logger = Logger.getLogger("diesel.SelectQuery");
        if (java.util.Arrays.asList(logger.getHandlers()).contains(handler)) {
            return;
        }
        logger.addHandler(handler);
    }

    @AfterEach
    void tearDown() {
        Logger.getLogger("diesel.SelectQuery").removeHandler(handler);
        dropTables();
    }

    private void dropTables() {
        for (String name : new String[]{"A", "B", "C"}) {
            try {
                database.dropTable(name);
            } catch (IllegalArgumentException ignored) {
            }
        }
    }

    private void createJoinTables() {
        database.executeQuery("CREATE TABLE A (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery("CREATE TABLE B (B_ID LONG PRIMARY KEY, A_ID LONG)", null);
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> aRow = new HashMap<>();
            aRow.put("ID", (long) i);
            aRow.put("NAME", "Name" + i);
            database.getTable("A").addRow(aRow);
            Map<String, Object> bRow = new HashMap<>();
            bRow.put("B_ID", (long) i);
            bRow.put("A_ID", (long) i);
            database.getTable("B").addRow(bRow);
        }
    }

    private List<String> warnings() {
        List<String> warnings = new ArrayList<>();
        for (LogRecord record : captured) {
            String message = record.getMessage();
            if (message != null && message.contains("Consider creating index on")) {
                warnings.add(message);
            }
        }
        return warnings;
    }

    @Test
    void testAutoCreatesIndexOnJoinColumnWithWarning() {
        createJoinTables();
        List<Map<String, Object>> first = (List<Map<String, Object>>) database.executeQuery(
                "SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        assertEquals(10, first.size(), "Join produces one row per matching pair");

        Table b = database.getTable("B");
        assertNotNull(b.getIndex("A_ID"), "Auto-created index on the unindexed join column B.A_ID");
        List<String> warnings = warnings();
        assertEquals(1, warnings.size(), "Exactly one advisory warning for the unindexed join column");
        assertTrue(warnings.get(0).contains("Consider creating index on B.A_ID for faster JOIN"),
                "Warning names the unindexed join column");
        assertFalse(warnings.get(0).contains("A.ID"), "Clustered primary key column is not warned");

        List<Map<String, Object>> second = (List<Map<String, Object>>) database.executeQuery(
                "SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        assertEquals(second, first, "Repeated join returns identical results");
        assertEquals(1, warnings().size(), "No re-warning when the index already exists");
    }

    @Test
    void testNoAutoIndexWhenJoinColumnAlreadyIndexed() {
        createJoinTables();
        database.executeQuery("CREATE INDEX ON B (A_ID)", null);
        database.executeQuery("SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        assertTrue(warnings().isEmpty(), "No warning when the join column already has an index");
        assertNotNull(database.getTable("B").getIndex("A_ID"), "Pre-created index is untouched");
    }

    @Test
    void testClusteredPrimaryKeyColumnNotAutoIndexed() {
        createJoinTables();
        database.executeQuery("SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        Table a = database.getTable("A");
        assertNull(a.getIndex("ID"), "No secondary index is auto-created over the clustered PK column");
        assertTrue(a.hasClusteredIndex() && "ID".equals(a.getClusteredIndexColumn()), "PK stays clustered");
        for (String warning : warnings()) {
            assertFalse(warning.contains("A.ID"), "Clustered PK column is never warned");
        }
    }

    @Test
    void testForeignKeyLikeJoinColumnAutoIndexed() {
        createJoinTables();
        database.executeQuery("SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        Table b = database.getTable("B");
        assertNotNull(b.getIndex("A_ID"),
                "FK-like join column (B.A_ID referencing A.ID) is auto-indexed like PostgreSQL");
        assertEquals(1, warnings().size(), "Single warning for the FK-like column");
    }

    @Test
    void testChainJoinWarnsPerUnindexedColumn() {
        createJoinTables();
        database.executeQuery("CREATE TABLE C (C_ID LONG PRIMARY KEY, B_ID LONG)", null);
        for (int i = 1; i <= 10; i++) {
            Map<String, Object> cRow = new HashMap<>();
            cRow.put("C_ID", (long) i);
            cRow.put("B_ID", (long) i);
            database.getTable("C").addRow(cRow);
        }
        database.executeQuery("SELECT A.ID, B.A_ID, C.B_ID FROM A JOIN B ON A.ID = B.A_ID FULL JOIN C ON B.B_ID = C.B_ID", null);
        List<String> warnings = warnings();
        assertEquals(2, warnings.size(), "One warning per unindexed join column (B.A_ID and C.B_ID)");
        assertTrue(warnings.stream().anyMatch(w -> w.contains("B.A_ID")), "Warns for B.A_ID");
        assertTrue(warnings.stream().anyMatch(w -> w.contains("C.B_ID")), "Warns for C.B_ID");
        assertNotNull(database.getTable("B").getIndex("A_ID"), "B.A_ID auto-indexed by first join");
        assertNotNull(database.getTable("C").getIndex("B_ID"), "C.B_ID auto-indexed by second join");
    }

    @Test
    void testJoinBenchmarkIndexedFasterThanUnindexed() {
        database.executeQuery("CREATE TABLE A (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery("CREATE TABLE B (B_ID LONG PRIMARY KEY, A_ID LONG)", null);
        for (int i = 1; i <= 200; i++) {
            Map<String, Object> aRow = new HashMap<>();
            aRow.put("ID", (long) i);
            aRow.put("NAME", "Name" + i);
            database.getTable("A").addRow(aRow);
        }
        for (int i = 1; i <= 10000; i++) {
            Map<String, Object> bRow = new HashMap<>();
            bRow.put("B_ID", (long) i);
            bRow.put("A_ID", (long) (i % 100));
            database.getTable("B").addRow(bRow);
        }

        String literalJoin = "SELECT COUNT(*) FROM A JOIN B ON B.A_ID = 5";
        long unindexedMillis = measureLiteralJoinMillis(literalJoin);

        reattachHandler();
        database.executeQuery("SELECT A.ID, B.A_ID FROM A JOIN B ON A.ID = B.A_ID", null);
        assertNotNull(database.getTable("B").getIndex("A_ID"), "Equi-join auto-created the index");
        List<String> warnings = warnings();
        assertEquals(1, warnings.size(), "Warning emitted for the auto-created index");
        assertTrue(warnings.get(0).contains("Consider creating index on B.A_ID for faster JOIN"),
                "Warning names B.A_ID");

        long indexedMillis = measureLiteralJoinMillis(literalJoin);
        assertEquals(Long.valueOf(20000L), countOf(literalJoin),
                "Both runs return the correct row count (200 A rows x 100 matching B rows)");
        assertTrue(indexedMillis < unindexedMillis,
                "Indexed join must be faster than unindexed (indexed=" + indexedMillis
                        + " ms, unindexed=" + unindexedMillis + " ms)");
    }

    private long measureLiteralJoinMillis(String query) {
        int warmup = 3;
        int runs = 30;
        for (int i = 0; i < warmup; i++) {
            database.executeQuery(query, null);
        }
        long start = System.nanoTime();
        for (int i = 0; i < runs; i++) {
            database.executeQuery(query, null);
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private Long countOf(String query) {
        List<Map<String, Object>> result = (List<Map<String, Object>>) database.executeQuery(query, null);
        return (Long) result.get(0).get("COUNT(*)");
    }
}
