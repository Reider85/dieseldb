package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Tag("query-full")
public class UpdateTest {

    private static final int RECORD_COUNT = 10;

    @TempDir
    static Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), "
                        + "USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        database.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);
        insertRecords();
    }

    private void insertRecords() {
        for (int i = 1; i <= RECORD_COUNT; i++) {
            String query = String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82),
                    new BigDecimal(100 + i * 10).setScale(2, RoundingMode.HALF_UP));
            database.executeQuery(query, null);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    // ── Index-assisted WHERE tests ──────────────────────────────────

    @Test
    void updateWithHashIndexWhere() {
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 9999 WHERE NAME = 'User5'", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE NAME = 'User5'");
        assertEquals(1, result.size());
        assertEquals(0, new BigDecimal("9999.00").compareTo((BigDecimal) result.get(0).get("BALANCE")));
    }

    @Test
    void updateWithBTreeIndexWhere() {
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 8888 WHERE AGE = 23", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE AGE = 23");
        for (Map<String, Object> row : result) {
            assertEquals(0, new BigDecimal("8888.00").compareTo((BigDecimal) row.get("BALANCE")));
        }
    }

    @Test
    void updateWithUniqueIndexWhere() {
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 7777 WHERE ID = 3", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE ID = 3");
        assertEquals(1, result.size());
        assertEquals(0, new BigDecimal("7777.00").compareTo((BigDecimal) result.get(0).get("BALANCE")));
    }

    @Test
    void updateWithInCondition() {
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 5555 WHERE NAME IN ('User1','User2','User3')", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE NAME IN ('User1','User2','User3')");
        assertEquals(3, result.size());
        for (Map<String, Object> row : result) {
            assertEquals(0, new BigDecimal("5555.00").compareTo((BigDecimal) row.get("BALANCE")));
        }
    }

    @Test
    void updateWithBTreeRangeGreaterThan() {
        // AGE > 25 should match users with AGE in {26, 27, 28} (from 10 records with AGE = 18 + i%82)
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 1111 WHERE AGE > 25", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE AGE > 25");
        assertFalse(result.isEmpty(), "should match rows with AGE > 25");
        for (Map<String, Object> row : result) {
            assertEquals(0, new BigDecimal("1111.00").compareTo((BigDecimal) row.get("BALANCE")));
        }
    }

    @Test
    void updateWithBTreeRangeLessThanOrEquals() {
        database.executeQuery(
                "UPDATE USERS SET BALANCE = 2222 WHERE AGE <= 20", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT BALANCE FROM USERS WHERE AGE <= 20");
        assertFalse(result.isEmpty(), "should match rows with AGE <= 20");
        for (Map<String, Object> row : result) {
            assertEquals(0, new BigDecimal("2222.00").compareTo((BigDecimal) row.get("BALANCE")));
        }
    }

    // ── Index maintenance correctness tests ─────────────────────────

    @Test
    void updateIndexedColumnMaintainsIndex() {
        database.executeQuery(
                "UPDATE USERS SET NAME = 'UpdatedUser5' WHERE NAME = 'User5'", null);
        List<Map<String, Object>> oldResult = runSelect(
                "SELECT * FROM USERS WHERE NAME = 'User5'");
        assertEquals(0, oldResult.size(), "old name must not be findable via index");
        List<Map<String, Object>> newResult = runSelect(
                "SELECT * FROM USERS WHERE NAME = 'UpdatedUser5'");
        assertEquals(1, newResult.size(), "new name must be findable via index");
    }

    @Test
    void updateBTreeIndexedColumnMaintainsIndex() {
        database.executeQuery(
                "UPDATE USERS SET AGE = 99 WHERE NAME = 'User3'", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT * FROM USERS WHERE AGE = 99");
        assertEquals(1, result.size());
        assertEquals("User3", result.get(0).get("NAME"));
    }

    // ── Full table scan fallback tests ──────────────────────────────

    @Test
    void updateWithNoIndexCondition() {
        // BALANCE has no index — must fall back to full scan
        assertDoesNotThrow(() ->
                database.executeQuery(
                        "UPDATE USERS SET BALANCE = 1111 WHERE BALANCE > 500", null));
    }

    @Test
    void updateWithOrCondition() {
        // OR conditions cannot use a single index — full scan fallback
        assertDoesNotThrow(() ->
                database.executeQuery(
                        "UPDATE USERS SET BALANCE = 2222 WHERE AGE = 23 OR NAME = 'User7'", null));
    }

    @Test
    void updateAllRows() {
        // No WHERE clause — update every row
        database.executeQuery("UPDATE USERS SET BALANCE = 0", null);
        List<Map<String, Object>> result = runSelect("SELECT BALANCE FROM USERS");
        assertEquals(RECORD_COUNT, result.size());
        for (Map<String, Object> row : result) {
            assertEquals(0, new BigDecimal("0.00").compareTo((BigDecimal) row.get("BALANCE")));
        }
    }

    // ── Bulk update mode tests ──────────────────────────────────────

    @Test
    void bulkUpdateManyRows() {
        // Insert enough rows to trigger bulk mode (threshold = 100)
        for (int i = RECORD_COUNT + 1; i <= 200; i++) {
            database.executeQuery(String.format(
                    "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE) VALUES ('CODE%d', 'User%d', %d, %s)",
                    i, i, 18 + (i % 82),
                    new BigDecimal(100).setScale(2, RoundingMode.HALF_UP)), null);
        }
        // Update all rows with AGE > 10 (should be > 100 rows)
        database.executeQuery("UPDATE USERS SET BALANCE = 77777 WHERE AGE > 10", null);
        // Verify index consistency after bulk rebuild
        List<Map<String, Object>> result = runSelect("SELECT * FROM USERS WHERE NAME = 'User50'");
        assertEquals(1, result.size(), "index must be consistent after bulk rebuild");
    }

    // ── Edge cases ──────────────────────────────────────────────────

    @Test
    void updateWithNullValue() {
        assertDoesNotThrow(() ->
                database.executeQuery(
                        "UPDATE USERS SET BALANCE = NULL WHERE NAME = 'User1'", null));
    }

    @Test
    void updateNoMatchingRows() {
        assertDoesNotThrow(() ->
                database.executeQuery(
                        "UPDATE USERS SET BALANCE = 9999 WHERE NAME = 'NonExistent'", null));
    }

    @Test
    void updateUniqueIndexedColumn() {
        // Update USER_CODE (unique-indexed) — index must reflect the new value
        database.executeQuery(
                "UPDATE USERS SET USER_CODE = 'NEWCODE5' WHERE USER_CODE = 'CODE5'", null);
        List<Map<String, Object>> result = runSelect(
                "SELECT * FROM USERS WHERE USER_CODE = 'NEWCODE5'");
        assertEquals(1, result.size());
        List<Map<String, Object>> oldResult = runSelect(
                "SELECT * FROM USERS WHERE USER_CODE = 'CODE5'");
        assertEquals(0, oldResult.size(), "old value must not be findable via index");
    }

    // ── MVCC transaction tests (prompt4.md #4, acceptance) ──────────

    private java.util.UUID beginTransaction() {
        String result = (String) database.executeQuery("BEGIN TRANSACTION", null);
        return java.util.UUID.fromString(result.substring("Transaction started: ".length()));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql, java.util.UUID tx) {
        return (List<Map<String, Object>>) database.executeQuery(sql, tx);
    }

    @Test
    void updateInsideTransactionIsVisibleToTheWriterUntilRollback() {
        java.util.UUID tx = beginTransaction();
        database.executeQuery("UPDATE USERS SET NAME = 'Renamed' WHERE ID = 1", tx);

        List<Map<String, Object>> inTx =
                runSelect("SELECT NAME FROM USERS WHERE ID = 1", tx);
        assertEquals(1, inTx.size());
        assertEquals("Renamed", inTx.get(0).get("NAME"),
                "the writer must read its own update");

        database.executeQuery("ROLLBACK", tx);
        List<Map<String, Object>> after =
                runSelect("SELECT NAME FROM USERS WHERE ID = 1", null);
        assertEquals(1, after.size());
        assertEquals("User1", after.get(0).get("NAME"),
                "ROLLBACK must restore the old value");
    }

    @Test
    void updateInsideTransactionPersistsAfterCommit() {
        java.util.UUID tx = beginTransaction();
        database.executeQuery("UPDATE USERS SET NAME = 'Committed' WHERE ID = 1", tx);
        database.executeQuery("COMMIT", tx);

        List<Map<String, Object>> after =
                runSelect("SELECT NAME FROM USERS WHERE ID = 1", null);
        assertEquals(1, after.size());
        assertEquals("Committed", after.get(0).get("NAME"),
                "the committed value must be durable");
    }
}
