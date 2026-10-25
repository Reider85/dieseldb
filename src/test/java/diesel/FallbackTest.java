package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fallback acceptance for prompt4 #39 task 5: ALGORITHM=INPLACE requested for
 * an unsupported operation (dropping an indexed column - INPLACE never
 * rewrites rows, so the index could not survive) falls back to the background
 * COPY with a warning, and the COPY removes the column's index inside the
 * swap window while leaving other indexes and all data intact.
 */
@Tag("smoke")
@ResourceLock("diesel.alter.copy.config")
public class FallbackTest {

    private static final String TABLE = "FALLBACK_T";
    private static final int ROWS = 3_000;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
        // Small batches so the background copy is observable/awaitable.
        System.setProperty("alter.copy.batch.size", "200");
        System.setProperty("alter.copy.batch.sleep.ms", "2");
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("alter.copy.batch.size");
        System.clearProperty("alter.copy.batch.sleep.ms");
        if (database != null) {
            database.close();
        }
    }

    @Test
    void inplaceOnIndexedColumnFallsBackToCopyWithWarning() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "NAME", "AGE"),
                Map.of("ID", Integer.class, "NAME", String.class, "AGE", Integer.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("NAME", "n-" + i);
            row.put("AGE", i % 80);
            table.addRow(row);
        }
        database.executeQuery("CREATE INDEX ON " + TABLE + " (AGE)", null);
        database.executeQuery("CREATE INDEX ON " + TABLE + " (NAME)", null);
        assertTrue(table.getIndexes().containsKey("AGE"));
        assertTrue(table.getIndexes().containsKey("NAME"));

        // Default algorithm for DROP is INPLACE; the indexed column forces the
        // fallback - the result must carry the warning and the started COPY.
        Object result = database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN AGE", null);
        String message = String.valueOf(result);
        assertTrue(message.contains("Warning"), "fallback warning missing: " + message);
        assertTrue(message.contains("fell back to ALGORITHM=COPY"),
                "fallback detail missing: " + message);

        Map<String, Object> completed = awaitAlterState("COMPLETED", 120_000);
        assertEquals(100, completed.get("PROGRESS_PERCENT"));

        // Column gone, its index gone with it, the other index untouched.
        assertFalse(table.getColumns().contains("AGE"));
        assertFalse(table.getIndexes().containsKey("AGE"),
                "index on the dropped column must be removed at swap time");
        assertTrue(table.getIndexes().containsKey("NAME"),
                "unrelated index must survive: " + table.getIndexes().keySet());

        // Data intact: every row kept its remaining columns.
        assertEquals(ROWS, table.rowCount());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> star = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        assertEquals(ROWS, star.size());
        for (Map<String, Object> row : star) {
            assertFalse(row.containsKey("AGE"), "dropped column leaked: " + row);
            assertNotNull(row.get("NAME"));
            assertNotNull(row.get("ID"));
        }

        // The surviving index still serves queries.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> byName = (List<Map<String, Object>>) database.executeQuery(
                "SELECT ID FROM " + TABLE + " WHERE NAME = 'n-7'", null);
        assertEquals(1, byName.size());
        assertEquals(7, byName.get(0).get("ID"));
    }

    @Test
    void explicitCopyOnIndexedColumnStartsWithoutWarning() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "AGE"),
                Map.of("ID", Integer.class, "AGE", Integer.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < 1_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("AGE", i % 60);
            table.addRow(row);
        }
        database.executeQuery("CREATE INDEX ON " + TABLE + " (AGE)", null);

        Object result = database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN AGE ALGORITHM=COPY", null);
        String message = String.valueOf(result);
        assertTrue(message.contains("started"), "COPY start message missing: " + message);
        assertFalse(message.contains("Warning"), "explicit COPY must not warn: " + message);

        awaitAlterState("COMPLETED", 120_000);
        assertFalse(table.getColumns().contains("AGE"));
        assertFalse(table.getIndexes().containsKey("AGE"));
        assertEquals(1_000, table.rowCount());
    }

    // ─── helpers ───────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> alterStatus() {
        return (List<Map<String, Object>>) database.executeQuery("ALTER STATUS", null);
    }

    private Map<String, Object> awaitAlterState(String state, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Map<String, Object> row : alterStatus()) {
                if (TABLE.equals(row.get("TABLE_NAME")) && state.equals(row.get("STATE"))) {
                    return row;
                }
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for ALTER state " + state + "; status=" + alterStatus());
        return null;
    }
}
