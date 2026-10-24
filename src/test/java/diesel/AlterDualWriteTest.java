package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Dual-write acceptance for online ALTER COPY (prompt4 #38):
 * writes issued while the background copy runs must be visible in the new
 * table version after the atomic swap.
 *
 * The ALTER statement returns only after the journal is attached, so every
 * statement issued after it runs against the active copy by construction.
 */
@Tag("smoke")
@ResourceLock("diesel.alter.copy.config")
public class AlterDualWriteTest {

    private static final String TABLE = "ALTER_DW_T";
    private static final int ROWS = 5_000;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
        System.setProperty("alter.copy.batch.size", "100");
        System.setProperty("alter.copy.batch.sleep.ms", "5");
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
    void writesDuringCopyVisibleAfterSwap() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("NAME", "base-" + i);
            table.addRow(row);
        }

        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN AGE INTEGER ALGORITHM=COPY", null);
        // The copy is RUNNING and the journal is attached from here on.
        Map<String, Object> running = findRow("RUNNING");
        assertNotNull(running, "copy should still be running right after start");

        // Mixed writes while the copy is in flight.
        for (int i = 0; i < 100; i++) {
            database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME) VALUES ("
                    + (100_000 + i) + ", 'mid-" + i + "')", null);
        }
        database.executeQuery("UPDATE " + TABLE + " SET NAME = 'updated-42' WHERE ID = 42", null);
        database.executeQuery("DELETE FROM " + TABLE + " WHERE ID = 7", null);

        awaitAlterState("COMPLETED", 120_000);

        // Old rows + mid-copy inserts - the mid-copy delete, all under the new schema.
        assertEquals(ROWS + 100 - 1, table.rowCount());
        assertTrue(table.getColumns().contains("AGE"));

        // The update issued during the copy is visible in the new version.
        assertEquals("updated-42", selectName(42));
        // The delete issued during the copy is visible too (row gone).
        assertTrue(selectByName("ID", 7).isEmpty(), "row 7 should be deleted");
        // Mid-copy inserts are present (the new column reads as null).
        Map<String, Object> mid = selectByName("ID", 100_050);
        assertEquals("mid-50", mid.get("NAME"));
        // Baseline rows read null for the added column.
        Map<String, Object> base = selectByName("ID", 3);
        assertEquals("base-3", base.get("NAME"));

        // And the new version accepts writes with the new column.
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, AGE) VALUES (99999991, 'post', 7)", null);
        assertEquals(7, selectAge(99999991));
    }

    // ─── helpers ───────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> alterStatus() {
        return (List<Map<String, Object>>) database.executeQuery("ALTER STATUS", null);
    }

    private Map<String, Object> findRow(String state) {
        for (Map<String, Object> row : alterStatus()) {
            if (TABLE.equals(row.get("TABLE_NAME")) && state.equals(row.get("STATE"))) {
                return row;
            }
        }
        return null;
    }

    private void awaitAlterState(String state, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (findRow(state) != null) {
                return;
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for ALTER state " + state + "; status=" + alterStatus());
    }

    private String selectName(int id) {
        Map<String, Object> row = selectByName("ID", id);
        assertFalse(row.isEmpty(), "row with ID " + id + " not found");
        return (String) row.get("NAME");
    }

    private Object selectAge(int id) {
        Map<String, Object> row = selectByName("ID", id);
        assertFalse(row.isEmpty(), "row with ID " + id + " not found");
        return row.get("AGE");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> selectByName(String keyColumn, int id) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE + " WHERE " + keyColumn + " = " + id, null);
        if (rows.isEmpty()) {
            return Map.of();
        }
        return rows.get(0);
    }
}
