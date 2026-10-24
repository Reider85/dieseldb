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
 * Rollback acceptance for online ALTER COPY (prompt4 #38): an interrupted
 * copy leaves the old version fully intact - schema, version counter, data -
 * and the table keeps serving DML. A later COPY on the same table succeeds,
 * proving the journal was completely detached.
 */
@Tag("smoke")
@ResourceLock("diesel.alter.copy.config")
public class AlterRollbackTest {

    private static final String TABLE = "ALTER_ROLL_T";
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
        // Slow the copy down so cancel() reliably lands mid-copy.
        System.setProperty("alter.copy.batch.size", "100");
        System.setProperty("alter.copy.batch.sleep.ms", "10");
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
    void interruptedCopyKeepsOldVersionAndTableUsable() throws Exception {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("NAME", "r-" + i);
            table.addRow(row);
        }
        long versionBefore = table.getSchemaVersions().currentVersion();

        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN AGE INTEGER ALGORITHM=COPY", null);
        assertNotNull(findRow("RUNNING"), "copy should be running before the cancel");

        assertTrue(database.cancelAlter(TABLE), "cancel should find the running copy");
        awaitAlterState("ROLLED_BACK", 60_000);

        // Old version is preserved: no schema change, no version bump, all rows.
        assertEquals(List.of("ID", "NAME"), table.getColumns());
        assertEquals(versionBefore, table.getSchemaVersions().currentVersion());
        assertEquals(ROWS, table.rowCount());

        // The table keeps serving DML on the old schema.
        database.executeQuery("INSERT INTO " + TABLE + " (ID, NAME) VALUES (999999, 'after-rollback')", null);
        database.executeQuery("UPDATE " + TABLE + " SET NAME = 'touched' WHERE ID = 1", null);
        assertEquals(ROWS + 1, table.rowCount());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                "SELECT NAME FROM " + TABLE + " WHERE ID = 1", null);
        assertEquals("touched", rows.get(0).get("NAME"));

        // Cancel of a finished copy reports false.
        assertFalse(database.cancelAlter(TABLE));

        // The old version was not poisoned: a fresh COPY completes and installs.
        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN AGE INTEGER ALGORITHM=COPY", null);
        awaitAlterState("COMPLETED", 120_000);
        assertTrue(table.getColumns().contains("AGE"));
        assertEquals(versionBefore + 1, table.getSchemaVersions().currentVersion());
        assertEquals(ROWS + 1, table.rowCount());
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
}
