package diesel;

import diesel.storage.avro.AvroRowStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 93 AVRO integration tests: exercises the full SQL engine path
 * (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE) against AVRO-backed tables
 * via the {@link Database} class, plus persistence round-trips, multi-table
 * isolation, index integration, and error handling. The {@code diesel.storage.type}
 * system property is set to {@code avro} in {@code setUp} so that
 * {@code StorageFactory.create} in the {@code Table} constructor resolves to
 * {@link AvroRowStorage}.
 *
 * <p>This is the first test suite that drives AVRO storage exclusively through
 * the SQL parser and executor rather than calling {@code AvroRowStorage} APIs
 * directly — closing the integration gap identified in Prompt 93.
 */
@Tag("storage")
@StorageType("avro")
class AvroStorageTest {

    @TempDir
    static Path tempDir;

    private Database database;
    private String prevStorageType;

    @BeforeEach
    void setUp() {
        prevStorageType = System.getProperty("diesel.storage.type");
        System.setProperty("diesel.storage.type", "avro");
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(user_seq 1 1), "
                        + "USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, ACTIVE BOOLEAN)", null);
    }

    @AfterEach
    void tearDown() {
        if (prevStorageType != null) {
            System.setProperty("diesel.storage.type", prevStorageType);
        } else {
            System.clearProperty("diesel.storage.type");
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelect(String sql) {
        return (List<Map<String, Object>>) database.executeQuery(sql, null);
    }

    private void insertRow(int i) {
        database.executeQuery(String.format(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('CODE%d', 'User%d', %d, %s, %s)",
                i, i, 18 + (i % 82),
                new BigDecimal(100 + (i % 9000)).setScale(2),
                (i % 2 == 0) ? "TRUE" : "FALSE"), null);
    }

    private void insertRows(int count) {
        for (int i = 1; i <= count; i++) {
            insertRow(i);
        }
        database.getTable("USERS").saveToFile("USERS");
    }

    // ─── CREATE TABLE via SQL ──────────────────────────────────────

    @Test
    void createTableProducesAvroStorage() {
        Table table = database.getTable("USERS");
        assertNotNull(table);
        assertTrue(table.getStorage() instanceof AvroRowStorage,
                "Expected AvroRowStorage but got " + table.getStorage().getClass().getName());
    }

    @Test
    void createSecondTableAlsoAvro() {
        database.executeQuery(
                "CREATE TABLE ORDERS (ID LONG PRIMARY KEY SEQUENCE(order_seq 1 1), "
                        + "USER_ID LONG, AMOUNT BIGDECIMAL)", null);
        Table orders = database.getTable("ORDERS");
        assertTrue(orders.getStorage() instanceof AvroRowStorage);
    }

    @Test
    void dropTableRemovesFromRegistry() {
        database.executeQuery(
                "CREATE TABLE TEMP_TABLE (ID LONG PRIMARY KEY, NAME STRING)", null);
        assertNotNull(database.getTable("TEMP_TABLE"));
        database.dropTable("TEMP_TABLE");
        assertThrows(TableNotFoundException.class, () -> database.getTable("TEMP_TABLE"));
    }

    // ─── INSERT via SQL ────────────────────────────────────────────

    @Test
    void insertSingleRow() {
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('C1', 'Alice', 30, 100.50, TRUE)", null);
        Table table = database.getTable("USERS");
        assertEquals(1, table.rowCount());
    }

    @Test
    void insertMultipleRows() {
        insertRows(100);
        Table table = database.getTable("USERS");
        assertEquals(100, table.rowCount());
    }

    @Test
    void insertAutoGeneratesSequenceId() {
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('X1', 'Bob', 25, 10.00, TRUE)", null);
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('X2', 'Carol', 35, 20.00, FALSE)", null);
        List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS ORDER BY ID");
        assertEquals(2, rows.size());
        Long id1 = (Long) rows.get(0).get("ID");
        Long id2 = (Long) rows.get(1).get("ID");
        assertEquals(id1 + 1, id2, "Sequence should auto-increment");
    }

    // ─── SELECT via SQL ────────────────────────────────────────────

    @Test
    void selectAllRows() {
        insertRows(50);
        List<Map<String, Object>> rows = runSelect("SELECT * FROM USERS");
        assertEquals(50, rows.size());
    }

    @Test
    void selectWithWhereEquals() {
        insertRows(100);
        List<Map<String, Object>> rows = runSelect(
                "SELECT ID, NAME FROM USERS WHERE USER_CODE = 'CODE42'");
        assertEquals(1, rows.size());
        assertEquals("User42", rows.get(0).get("NAME"));
    }

    @Test
    void selectWithWhereGreaterThan() {
        insertRows(100);
        List<Map<String, Object>> rows = runSelect(
                "SELECT ID, AGE FROM USERS WHERE AGE > 80");
        // AGE = 18 + (i % 82), i in 1..100 → AGE in [18,99], >80 means AGE 81..99
        assertTrue(rows.size() > 0, "Expected rows with AGE > 80");
        for (Map<String, Object> row : rows) {
            // AGE is Integer in the map
            assertTrue(((Number) row.get("AGE")).intValue() > 80);
        }
    }

    @Test
    void selectWithWhereIn() {
        insertRows(100);
        List<Map<String, Object>> rows = runSelect(
                "SELECT ID FROM USERS WHERE AGE IN (50, 51, 52)");
        assertTrue(rows.size() > 0, "Expected rows with AGE IN (50,51,52)");
    }

    @Test
    void selectWithOrderBy() {
        insertRows(20);
        List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS ORDER BY ID DESC");
        assertEquals(20, rows.size());
        Long first = (Long) rows.get(0).get("ID");
        Long last = (Long) rows.get(rows.size() - 1).get("ID");
        assertTrue(first > last, "ORDER BY DESC should give descending IDs");
    }

    @Test
    void selectWithLimit() {
        insertRows(100);
        List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS LIMIT 10");
        assertEquals(10, rows.size());
    }

    @Test
    void selectWithColumnProjection() {
        insertRows(10);
        List<Map<String, Object>> rows = runSelect("SELECT NAME FROM USERS");
        assertEquals(10, rows.size());
        for (Map<String, Object> row : rows) {
            assertTrue(row.containsKey("NAME"));
            assertTrue(!row.containsKey("BALANCE"), "Projection should not include BALANCE");
        }
    }

    @Test
    void selectAggregates() {
        insertRows(10);
        List<Map<String, Object>> rows = runSelect("SELECT COUNT(*) FROM USERS");
        assertEquals(1, rows.size());
    }

    // ─── UPDATE via SQL ────────────────────────────────────────────

    @Test
    void updateRow() {
        insertRows(10);
        database.executeQuery(
                "UPDATE USERS SET NAME = 'Updated' WHERE USER_CODE = 'CODE5'", null);
        List<Map<String, Object>> rows = runSelect(
                "SELECT NAME FROM USERS WHERE USER_CODE = 'CODE5'");
        assertEquals(1, rows.size());
        assertEquals("Updated", rows.get(0).get("NAME"));
    }

    @Test
    void updateMultipleRows() {
        insertRows(100);
        database.executeQuery("UPDATE USERS SET AGE = 99 WHERE AGE > 90", null);
        List<Map<String, Object>> rows = runSelect("SELECT ID FROM USERS WHERE AGE = 99");
        assertTrue(rows.size() > 0, "Expected rows updated to AGE 99");
    }

    // ─── DELETE via SQL ────────────────────────────────────────────

    @Test
    void deleteRow() {
        insertRows(10);
        database.executeQuery("DELETE FROM USERS WHERE USER_CODE = 'CODE3'", null);
        database.getTable("USERS").saveToFile("USERS");
        List<Map<String, Object>> rows = runSelect(
                "SELECT ID FROM USERS WHERE USER_CODE = 'CODE3'");
        assertEquals(0, rows.size(), "Deleted row should not be found");
    }

    @Test
    void deleteMultipleRows() {
        insertRows(100);
        database.executeQuery("DELETE FROM USERS WHERE AGE > 50", null);
        database.getTable("USERS").saveToFile("USERS");
        List<Map<String, Object>> remaining = runSelect("SELECT ID FROM USERS");
        assertTrue(remaining.size() < 100, "Some rows should have been deleted");
    }

    // ─── Persistence round-trip via SQL ────────────────────────────

    @Test
    void persistenceRoundTripThroughDatabase() {
        insertRows(50);

        // Create a new Database instance pointing at the same data dir
        Database db2 = new Database();
        db2.setDataDir(tempDir.toString());
        db2.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(user_seq 1 1), "
                        + "USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, ACTIVE BOOLEAN)", null);
        // loadFromFile reads from the .avro file into the fresh Avro storage
        AvroRowStorage storage = (AvroRowStorage) db2.getTable("USERS").getStorage();
        storage.loadFromFile("USERS");

        List<Map<String, Object>> rows = runSelectOn(db2, "SELECT * FROM USERS");
        assertEquals(50, rows.size());
        assertEquals("User1", rows.get(0).get("NAME"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> runSelectOn(Database db, String sql) {
        return (List<Map<String, Object>>) db.executeQuery(sql, null);
    }

    @Test
    void avroFileExistsAfterSave() {
        insertRows(10);
        File avroFile = new File(tempDir.toString(), "USERS.avro");
        assertTrue(avroFile.exists(), "AVRO file should exist after saveToFile");
        assertTrue(avroFile.length() > 0, "AVRO file should not be empty");
    }

    // ─── Multi-table isolation ─────────────────────────────────────

    @Test
    void twoTablesDoNotInterfere() {
        database.executeQuery(
                "CREATE TABLE ORDERS (ID LONG PRIMARY KEY SEQUENCE(order_seq 1 1), "
                        + "USER_ID LONG, AMOUNT BIGDECIMAL)", null);
        insertRows(10);
        database.executeQuery(
                "INSERT INTO ORDERS (USER_ID, AMOUNT) VALUES (1, 50.00)", null);
        database.getTable("USERS").saveToFile("USERS");
        database.getTable("ORDERS").saveToFile("ORDERS");

        assertEquals(10, runSelect("SELECT ID FROM USERS").size());
        assertEquals(1, runSelect("SELECT ID FROM ORDERS").size());

        // Verify separate .avro files exist
        assertTrue(new File(tempDir.toString(), "USERS.avro").exists());
        assertTrue(new File(tempDir.toString(), "ORDERS.avro").exists());
    }

    // ─── Index integration ─────────────────────────────────────────

    @Test
    void createIndexOnAvroTable() {
        insertRows(50);
        assertDoesNotThrow(() ->
                database.executeQuery("CREATE INDEX ON USERS (AGE)", null),
                "Creating index on AVRO table should succeed");
    }

    @Test
    void createUniqueIndexOnAvroTable() {
        insertRows(50);
        assertDoesNotThrow(() ->
                database.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null),
                "Creating unique index on AVRO table should succeed");
    }

    @Test
    void indexAcceleratedSelectWorks() {
        insertRows(100);
        database.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        List<Map<String, Object>> rows = runSelect(
                "SELECT ID FROM USERS WHERE AGE IN (50, 51, 52)");
        assertTrue(rows.size() > 0, "Index-backed SELECT should return rows");
    }

    // ─── Error cases ───────────────────────────────────────────────

    @Test
    void duplicatePrimaryKeyRejected() {
        // Sequence-based PKs auto-generate IDs, so a plain PK table is needed
        // to test explicit duplicate rejection.
        database.executeQuery(
                "CREATE TABLE PK_TEST (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery(
                "INSERT INTO PK_TEST (ID, NAME) VALUES (1, 'A')", null);
        assertThrows(Exception.class, () ->
                database.executeQuery(
                        "INSERT INTO PK_TEST (ID, NAME) VALUES (1, 'B')", null),
                "Duplicate primary key should be rejected");
    }

    @Test
    void selectFromNonexistentTableThrows() {
        assertThrows(TableNotFoundException.class, () ->
                runSelect("SELECT * FROM NO_SUCH_TABLE"));
    }

    @Test
    void invalidSqlSyntaxThrows() {
        assertThrows(Exception.class, () ->
                database.executeQuery("THIS IS NOT SQL", null));
    }

    // ─── Large dataset stress test ─────────────────────────────────

    @Test
    void largeDatasetThroughSql() {
        int rows = 2000;
        insertRows(rows);
        Table table = database.getTable("USERS");
        assertEquals(rows, table.rowCount());

        // Verify round-trip persistence
        List<Map<String, Object>> selected = runSelect("SELECT * FROM USERS");
        assertEquals(rows, selected.size());

        // Verify a filtered query on the large set
        List<Map<String, Object>> filtered = runSelect(
                "SELECT ID FROM USERS WHERE AGE > 95");
        assertTrue(filtered.size() > 0, "Filtered query on large set should return rows");
    }

    // ─── Transaction integration ───────────────────────────────────

    @Test
    void insertWithExplicitTransaction() {
        Object beginResult = database.executeQuery("BEGIN TRANSACTION", null);
        assertNotNull(beginResult);
        UUID txId = UUID.fromString(beginResult.toString().split(": ")[1]);
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('TX1', 'TxUser', 50, 5.00, TRUE)", txId);
        database.executeQuery("COMMIT", txId);

        List<Map<String, Object>> rows = runSelect(
                "SELECT ID FROM USERS WHERE USER_CODE = 'TX1'");
        assertEquals(1, rows.size());
    }

    @Test
    void rollbackDiscardsChanges() {
        insertRows(5);
        Object beginResult = database.executeQuery("BEGIN TRANSACTION", null);
        assertNotNull(beginResult);
        UUID txId = UUID.fromString(beginResult.toString().split(": ")[1]);
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('ROLL', 'RollUser', 60, 6.00, FALSE)", txId);
        database.executeQuery("ROLLBACK", txId);

        List<Map<String, Object>> rows = runSelect(
                "SELECT ID FROM USERS WHERE USER_CODE = 'ROLL'");
        assertEquals(0, rows.size(), "Rolled-back row should not be visible");
    }

    // ─── All scalar types through SQL ──────────────────────────────

    @Test
    void allScalarTypesRoundTrip() {
        database.executeQuery(
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, ACTIVE) VALUES "
                        + "('FULL', 'AllTypes', 42, 9999.99, TRUE)", null);
        database.getTable("USERS").saveToFile("USERS");

        Database db2 = new Database();
        db2.setDataDir(tempDir.toString());
        db2.executeQuery(
                "CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(user_seq 1 1), "
                        + "USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, ACTIVE BOOLEAN)", null);
        AvroRowStorage storage = (AvroRowStorage) db2.getTable("USERS").getStorage();
        storage.loadFromFile("USERS");

        List<Map<String, Object>> rows = storage.scan();
        assertEquals(1, rows.size());
        Map<String, Object> row = rows.get(0);
        assertEquals("AllTypes", row.get("NAME"));
        assertEquals(42, ((Number) row.get("AGE")).intValue());
        assertTrue(row.get("BALANCE") instanceof BigDecimal,
                "BALANCE should decode as BigDecimal, got " + row.get("BALANCE").getClass());
        assertEquals(0, new BigDecimal("9999.99").setScale(18)
                .compareTo((BigDecimal) row.get("BALANCE")));
        assertEquals(Boolean.TRUE, row.get("ACTIVE"));
    }

    // ─── Storage factory verification ──────────────────────────────

    @Test
    void storageFactoryCreatesAvroForAvroType() {
        var storage = diesel.storage.StorageFactory.create("avro", "factory_check",
                List.of("X"), Map.of("X", String.class));
        assertTrue(storage instanceof AvroRowStorage,
                "Expected AvroRowStorage but got " + storage.getClass().getName());
    }
}
