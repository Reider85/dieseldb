package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DROP COLUMN tombstone acceptance (prompt4 #39, R3-010 step 3): the column
 * leaves the schema immediately, its keys stay in row maps until the
 * background strip, SELECT * hides them the whole time, and the hidden state
 * survives serialization (format v4).
 */
@Tag("smoke")
public class TombstoneTest {

    private static final String TABLE = "TOMBSTONE_T";
    private static final int ROWS = 500;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        try {
            database.dropTable(TABLE);
        } catch (RuntimeException ignored) {
            // not present
        }
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
    }

    @Test
    void dropColumnHidesFromAllReadPathsWhileStripIsPending() {
        createWithRows();
        Table table = database.getTable(TABLE);

        database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN NAME ALGORITHM=INPLACE", null);

        // Schema: the column is gone.
        assertFalse(table.getColumns().contains("NAME"));
        assertFalse(table.getColumnTypes().containsKey("NAME"));
        // Tombstone state: the key may still be materialized (the physical
        // strip runs on the background executor) or already stripped - both
        // are valid at this point; the hidden set is what hides it either way.
        assertTrue(table.isDroppedColumn("NAME"), "hidden set must contain the dropped column");

        // SELECT * hides it on every row.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> star = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        assertEquals(ROWS, star.size());
        assertTrue(star.stream().noneMatch(r -> r.containsKey("NAME")),
                "SELECT * leaked the dropped column");

        // Explicit projection resolves like an absent column (no value).
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> explicit = (List<Map<String, Object>>) database.executeQuery(
                "SELECT NAME FROM " + TABLE + " WHERE ID = 1", null);
        assertEquals(1, explicit.size());
        assertFalse(explicit.get(0).containsKey("NAME"),
                "explicit projection returned the dropped column: " + explicit.get(0));

        // WHERE on the dropped column is rejected at parse time: the schema no
        // longer knows the column (parse-time validation, SqlParsingUtils).
        awaitPhysicallyStripped();
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class, () ->
                database.executeQuery(
                        "SELECT * FROM " + TABLE + " WHERE NAME = 'n-1'", null));
        assertTrue(String.valueOf(rejected.getMessage()).contains("Unknown column"),
                "unexpected message: " + rejected.getMessage());

        // The rest of the data survived the drop.
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> remaining = (List<Map<String, Object>>) database.executeQuery(
                "SELECT ID FROM " + TABLE, null);
        assertEquals(ROWS, remaining.size());
        assertEquals(0, remaining.get(0).get("ID"));
    }

    @Test
    void droppedColumnStateSurvivesSerialization() {
        createWithRows();

        database.executeQuery(
                "ALTER TABLE " + TABLE + " DROP COLUMN NAME ALGORITHM=INPLACE", null);
        // Close serializes every table (Database.close -> saveTablesToDisk);
        // saveToSerializedFile compacts first, so rows are written stripped and
        // the format-v4 hidden set travels with them.
        database.close();
        database = new Database();
        database.loadTablesFromDisk();

        Table reopened = database.getTable(TABLE);
        assertFalse(reopened.getColumns().contains("NAME"),
                "dropped column resurrected after reopen");
        assertEquals(ROWS, reopened.rowCount());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> star = (List<Map<String, Object>>) database.executeQuery(
                "SELECT * FROM " + TABLE, null);
        assertEquals(ROWS, star.size());
        assertTrue(star.stream().noneMatch(r -> r.containsKey("NAME")),
                "SELECT * leaked the dropped column after reopen");
        // Row maps must be physically clean too (compacted before serialization).
        assertTrue(reopened.getLiveRows().stream().noneMatch(r -> r.containsKey("NAME")),
                "serialized row maps still carry the dropped column");
    }

    @Test
    void dropRejectsUnknownAndPrimaryKeyColumns() {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");

        assertThrows(ColumnNotFoundException.class, () ->
                database.executeQuery("ALTER TABLE " + TABLE + " DROP COLUMN NOPE", null));
        assertThrows(DieselException.class, () ->
                database.executeQuery("ALTER TABLE " + TABLE + " DROP COLUMN ID", null));

        List<String> columns = database.getTable(TABLE).getColumns();
        assertTrue(columns.contains("ID") && columns.contains("NAME"),
                "failed DROP attempts must leave the schema untouched: " + columns);
    }

    // ─── helpers ───────────────────────────────────────────────────────

    private void createWithRows() {
        database.createTable(TABLE,
                List.of("ID", "NAME"),
                Map.of("ID", Integer.class, "NAME", String.class),
                "ID");
        Table table = database.getTable(TABLE);
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("ID", i);
            row.put("NAME", "n-" + i);
            table.addRow(row);
        }
    }

    private void awaitPhysicallyStripped() {
        Table table = database.getTable(TABLE);
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            // In-memory mirror (what compact rewrites); getRows() may rescan
            // the CSV file, whose rewrite lags on the background flusher.
            if (table.getLiveRows().stream().noneMatch(r -> r.containsKey("NAME"))) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for physical strip");
            }
        }
        fail("dropped column keys still materialized after 15s (background strip)");
    }
}
