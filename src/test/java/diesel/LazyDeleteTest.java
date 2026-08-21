package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class LazyDeleteTest {

    private static final int RECORD_COUNT = 200;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);
        insertRecords();
    }

    private void insertRecords() {
        Table table = database.getTable("USERS");
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82), new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP)
            );
            database.executeQuery(query, null);
        }
        table.saveToFile("USERS");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    // ─── Basic delete tests ────────────────────────────────────────────

    @Test
    void deleteSingleRow() {
        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE ID = 1");
        assertEquals(1, result.size());

        database.executeQuery("DELETE FROM USERS WHERE ID = 1", null);

        result = runSelect("SELECT * FROM USERS WHERE ID = 1");
        assertEquals(0, result.size());

        Table table = database.getTable("USERS");
        assertEquals(RECORD_COUNT - 1, table.rowCount());
    }

    @Test
    void deleteMultipleRowsWithIN() {
        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 2, 3, 4, 5)", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS");
        assertEquals(RECORD_COUNT - 5, result.size());

        Table table = database.getTable("USERS");
        assertEquals(RECORD_COUNT - 5, table.rowCount());
    }

    @Test
    void deleteAllRows() {
        database.executeQuery("DELETE FROM USERS", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS");
        assertEquals(0, result.size());

        Table table = database.getTable("USERS");
        assertEquals(0, table.rowCount());
    }

    // ─── Index consistency tests ───────────────────────────────────────

    @Test
    void deleteWithBTreeIndex() {
        // AGE is a BTree index, delete rows with specific age
        List<Map<String, Object>> before = runSelect("SELECT * FROM USERS WHERE AGE = 18");
        int countBefore = before.size();
        assertTrue(countBefore > 0);

        database.executeQuery("DELETE FROM USERS WHERE AGE = 18", null);

        List<Map<String, Object>> after = runSelect("SELECT * FROM USERS WHERE AGE = 18");
        assertEquals(0, after.size());

        // Verify remaining rows are intact
        List<Map<String, Object>> all = runSelect("SELECT * FROM USERS");
        assertEquals(RECORD_COUNT - countBefore, all.size());
    }

    @Test
    void deleteWithHashIndex() {
        // NAME is a hash index
        database.executeQuery("DELETE FROM USERS WHERE NAME = 'User1'", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE NAME = 'User1'");
        assertEquals(0, result.size());
    }

    @Test
    void deleteWithUniqueIndex() {
        // USER_CODE is a unique index
        database.executeQuery("DELETE FROM USERS WHERE USER_CODE = 'CODE50'", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE USER_CODE = 'CODE50'");
        assertEquals(0, result.size());
    }

    @Test
    void deleteWithClusteredIndex() {
        // ID is the primary key with clustered index
        database.executeQuery("DELETE FROM USERS WHERE ID = 100", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE ID = 100");
        assertEquals(0, result.size());

        // Verify nearby rows are intact
        result = runSelect("SELECT * FROM USERS WHERE ID = 99");
        assertEquals(1, result.size());
        result = runSelect("SELECT * FROM USERS WHERE ID = 101");
        assertEquals(1, result.size());
    }

    @Test
    void deleteWithMultipleIndexes() {
        // Delete with condition on BTree-indexed column
        database.executeQuery("DELETE FROM USERS WHERE AGE = 20", null);

        // Verify via hash index lookup
        List<Map<String, Object>> all = runSelect("SELECT * FROM USERS");
        for (Map<String, Object> row : all) {
            assertNotEquals(20, ((Number) row.get("AGE")).intValue());
        }
    }

    // ─── SELECT filtering tests ────────────────────────────────────────

    @Test
    void selectAfterDeleteFiltersTombstoned() {
        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 50, 100, 150, 200)", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS");
        assertEquals(RECORD_COUNT - 5, result.size());

        // Verify deleted IDs are not in results
        for (Map<String, Object> row : result) {
            long id = ((Number) row.get("ID")).longValue();
            assertTrue(id != 1 && id != 50 && id != 100 && id != 150 && id != 200);
        }
    }

    @Test
    void selectWithIndexAfterDelete() {
        database.executeQuery("DELETE FROM USERS WHERE ID = 10", null);

        // Index-based lookup should skip the deleted row
        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE NAME = 'User10'");
        assertEquals(0, result.size());
    }

    // ─── INSERT after DELETE tests ─────────────────────────────────────

    @Test
    void insertAfterDeleteWorks() {
        database.executeQuery("DELETE FROM USERS WHERE ID = 1", null);

        // Insert a new row — should append at the end
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('NEW1', 'NewUser', 30, 500.00)", null);

        Table table = database.getTable("USERS");
        assertEquals(RECORD_COUNT, table.rowCount()); // deleted 1, inserted 1

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE NAME = 'NewUser'");
        assertEquals(1, result.size());
    }

    // ─── UPDATE after DELETE tests ─────────────────────────────────────

    @Test
    void updateAfterDeleteSkipsTombstoned() {
        database.executeQuery("DELETE FROM USERS WHERE ID = 5", null);

        // Update should skip the tombstoned row
        database.executeQuery("UPDATE USERS SET AGE = 99 WHERE ID = 5", null);

        // The deleted row should still not exist
        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE ID = 5");
        assertEquals(0, result.size());
    }

    // ─── Row count tests ──────────────────────────────────────────────

    @Test
    void rowCountAfterDelete() {
        Table table = database.getTable("USERS");
        assertEquals(RECORD_COUNT, table.rowCount());

        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 2, 3)", null);
        assertEquals(RECORD_COUNT - 3, table.rowCount());
        assertEquals(3, table.getDeletedCount());
    }

    // ─── Compaction tests ─────────────────────────────────────────────

    @Test
    void manualCompactRebuildsIndexes() {
        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 2, 3, 4, 5)", null);

        Table table = database.getTable("USERS");
        assertEquals(5, table.getDeletedCount());

        table.compact();

        assertEquals(0, table.getDeletedCount());
        assertEquals(RECORD_COUNT - 5, table.rowCount());
        assertEquals(RECORD_COUNT - 5, table.getRawRowCount());

        // Verify indexes work after compaction
        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE AGE = 18");
        for (Map<String, Object> row : result) {
            long id = ((Number) row.get("ID")).longValue();
            assertTrue(id > 5); // IDs 1-5 were deleted
        }
    }

    @Test
    void autoCompactAtThreshold() {
        Table table = database.getTable("USERS");

        // Delete 30% of rows to trigger auto-compact (threshold is 0.3)
        int toDelete = (int) (RECORD_COUNT * 0.3) + 1;
        StringBuilder ids = new StringBuilder();
        for (int i = 1; i <= toDelete; i++) {
            if (i > 1) ids.append(", ");
            ids.append(i);
        }
        database.executeQuery("DELETE FROM USERS WHERE ID IN (" + ids + ")", null);

        // Auto-compact should have triggered
        assertEquals(0, table.getDeletedCount());
        assertEquals(RECORD_COUNT - toDelete, table.rowCount());
    }

    // ─── Complex condition tests ──────────────────────────────────────

    @Test
    void deleteWithComplexConditions() {
        // Delete with AND condition
        database.executeQuery("DELETE FROM USERS WHERE AGE > 50 AND BALANCE > 5000.00", null);

        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS");
        for (Map<String, Object> row : result) {
            int age = ((Number) row.get("AGE")).intValue();
            BigDecimal balance = (BigDecimal) row.get("BALANCE");
            assertFalse(age > 50 && balance.compareTo(new BigDecimal("5000.00")) > 0,
                    "Row with age=" + age + " and balance=" + balance + " should have been deleted");
        }
    }

    // ─── Statistics tests ─────────────────────────────────────────────

    @Test
    void statisticsAfterDelete() {
        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 2, 3)", null);

        Table table = database.getTable("USERS");
        Table.TableStatistics stats = table.getStatistics();
        assertEquals(RECORD_COUNT - 3, stats.getRowCount());
    }

    // ─── Delete + re-insert + delete cycle ────────────────────────────

    @Test
    void deleteInsertDeleteCycle() {
        // Delete some rows
        database.executeQuery("DELETE FROM USERS WHERE ID IN (1, 2, 3)", null);

        // Insert new rows
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('NEW1', 'New1', 25, 100.00)", null);
        database.executeQuery("INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('NEW2', 'New2', 30, 200.00)", null);

        Table table = database.getTable("USERS");
        assertEquals(RECORD_COUNT - 1, table.rowCount()); // -3 +2 = -1

        // Delete the new rows
        List<Map<String, Object>> newRows = runSelect("SELECT * FROM USERS WHERE NAME IN ('New1', 'New2')");
        assertEquals(2, newRows.size());

        database.executeQuery("DELETE FROM USERS WHERE NAME IN ('New1', 'New2')", null);
        assertEquals(RECORD_COUNT - 3, table.rowCount());
    }
}
