package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ALTER STATUS progress acceptance for online ALTER COPY (prompt4 #38):
 * a percentage between 0 and 100 is observable while the background copy
 * runs, 100 when it completes, plus the parser contract for the statement
 * itself (ALGORITHM=INPLACE became executable in prompt4 #39).
 */
@Tag("smoke")
@ResourceLock("diesel.alter.copy.config")
public class AlterProgressTest {

    private static final String TABLE = "ALTER_PROG_T";
    private static final int ROWS = 3_000;

    private Database database;
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
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
    void statusEmptyOnFreshDatabase() {
        assertTrue(alterStatus().isEmpty(), "no ALTERs expected on a fresh database");
    }

    @Test
    void progressGoesFromPartialToHundredPercent() throws Exception {
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

        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN TAG STRING ALGORITHM=COPY", null);

        // The running row exposes the total size immediately...
        Map<String, Object> running = findRow("RUNNING");
        assertNotNull(running, "expected a RUNNING row after start");
        assertEquals(Long.valueOf(ROWS), running.get("ROWS_TOTAL"));

        // ...and a partial percentage while batches are being copied.
        boolean sawPartial = false;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && !sawPartial) {
            Map<String, Object> current = findRow("RUNNING");
            if (current != null) {
                int percent = (int) current.get("PROGRESS_PERCENT");
                sawPartial = percent > 0 && percent < 100;
            }
            if (!sawPartial) {
                Thread.sleep(5);
            }
        }
        assertTrue(sawPartial, "never observed a partial (0<percent<100) progress value; status="
                + alterStatus());

        Map<String, Object> completed = awaitAlterState("COMPLETED", 120_000);
        assertEquals(100, completed.get("PROGRESS_PERCENT"));
        assertEquals(Long.valueOf(ROWS), completed.get("ROWS_TOTAL"));
        assertEquals(Long.valueOf(ROWS), completed.get("ROWS_COPIED"));
        assertEquals("copy completed: " + TABLE, completed.get("MESSAGE"));
    }

    @Test
    void parserContractForAlgorithmAndStatus() {
        // ALTER STATUS is a statement of its own.
        assertThrows(RuntimeException.class, () -> database.executeQuery("ALTER STATUS NOW", null));

        // INPLACE is executable since prompt4 #39 (R3-010 step 3): metadata-only
        // path, no background job, so it registers no ALTER STATUS row.
        database.createTable(TABLE,
                List.of("ID"), Map.of("ID", Integer.class), "ID");
        database.executeQuery(
                "ALTER TABLE " + TABLE + " ADD COLUMN C1 INTEGER ALGORITHM=INPLACE", null);
        assertTrue(database.getTable(TABLE).getColumns().contains("C1"));

        // Unknown algorithm tokens are rejected outright.
        RuntimeException bogus = assertThrows(RuntimeException.class, () ->
                database.executeQuery(
                        "ALTER TABLE " + TABLE + " ADD COLUMN C3 INTEGER ALGORITHM=BOGUS", null));
        assertTrue(String.valueOf(bogus.getMessage()).contains("Unsupported ALGORITHM"),
                "unexpected message: " + bogus.getMessage());

        // Unknown LOCK tokens are rejected too (prompt4 #39).
        RuntimeException badLock = assertThrows(RuntimeException.class, () ->
                database.executeQuery(
                        "ALTER TABLE " + TABLE + " ADD COLUMN C4 INTEGER LOCK=WORLD", null));
        assertTrue(String.valueOf(badLock.getMessage()).contains("Unsupported LOCK mode"),
                "unexpected message: " + badLock.getMessage());

        // The default (no clause) stays the instant metadata-only path - it has
        // no background job, so ALTER STATUS registers nothing for it.
        database.executeQuery("ALTER TABLE " + TABLE + " ADD COLUMN C2 INTEGER", null);
        assertTrue(database.getTable(TABLE).getColumns().contains("C2"));
        assertEquals(0L, alterStatus().stream()
                .filter(r -> TABLE.equals(r.get("TABLE_NAME"))).count());
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

    private Map<String, Object> awaitAlterState(String state, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> row = findRow(state);
            if (row != null) {
                return row;
            }
            Thread.sleep(10);
        }
        fail("timed out waiting for ALTER state " + state + "; status=" + alterStatus());
        return null;
    }
}
