package diesel;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Level;

public class PersistenceTest {
    private static final Logger LOGGER = Logger.getLogger(PersistenceTest.class.getName());
    private static final String TABLE = "PERSIST_TEST";
    private int passed = 0;
    private int failed = 0;

    public void runTests() {
        try {
            cleanup();
            testSaveAndLoadRoundTrip();
            testSchemaAndTypesPreserved();
            testSequenceContinuesAfterLoad();
            testIndexesFunctionalAfterLoad();
            testLoadTablesFromDisk();
            testDropTableDeletesFiles();
        } catch (Exception e) {
            failed++;
            LOGGER.log(Level.SEVERE, "PersistenceTest FAILED: {0}", e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
        LOGGER.log(Level.INFO, "==========================================");
        LOGGER.log(Level.INFO, "PersistenceTest results: {0} passed, {1} failed", new Object[]{passed, failed});
        if (failed > 0) {
            throw new RuntimeException("PersistenceTest failed: " + failed + " tests");
        }
    }

    private void check(boolean condition, String message) {
        if (condition) {
            passed++;
            LOGGER.log(Level.INFO, "PASS: {0}", message);
        } else {
            failed++;
            LOGGER.log(Level.SEVERE, "FAIL: {0}", message);
        }
    }

    private void cleanup() {
        new File(TABLE + ".csv").delete();
        new File(TABLE + ".table").delete();
        new File(TABLE + "2.csv").delete();
        new File(TABLE + "2.table").delete();
    }

    private void testSaveAndLoadRoundTrip() {
        LOGGER.log(Level.INFO, "Starting test: testSaveAndLoadRoundTrip");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        check(new File(TABLE + ".table").exists(), "Serialized .table file created on disk");
        check(new File(TABLE + ".csv").exists(), "CSV file created on disk");
        check(db.getTable(TABLE).isFileInitialized(), "Table marked as initialized after save");

        Database reloaded = new Database();
        Table loaded = Table.loadFromFile(reloaded, TABLE);
        check(loaded != null, "Table loaded from file via loadFromFile");
        if (loaded == null) {
            return;
        }
        check(loaded.getName().equals(TABLE), "Loaded table name preserved");
        check(loaded.getRows().size() == 2, "Loaded table row count preserved (2 rows)");
        check(loaded.getPrimaryKeyColumn().equals("ID"), "Primary key column preserved");
        check(loaded.hasClusteredIndex() && "ID".equals(loaded.getClusteredIndexColumn()),
                "Clustered index on primary key preserved");
        check(loaded.getSequences().containsKey("ID"), "Sequence registered under its column name");
        check("ID_SEQ".equals(loaded.getSequences().get("ID").getName()), "Sequence name preserved");
        check(loaded.getDatabase() == reloaded, "Loaded table bound to new Database instance");
        LOGGER.log(Level.INFO, "Test testSaveAndLoadRoundTrip: DONE");
    }

    private void testSchemaAndTypesPreserved() {
        LOGGER.log(Level.INFO, "Starting test: testSchemaAndTypesPreserved");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);

        List<String> expectedColumns = List.of("ID", "NAME", "AGE", "ACTIVE", "BIRTHDATE", "LAST_LOGIN", "USER_SCORE", "BALANCE", "SCORE", "PRECISION", "INITIAL", "SESSION_ID");
        check(table.getColumns().equals(expectedColumns), "All column names preserved in order");

        Map<String, Class<?>> types = table.getColumnTypes();
        check(types.get("ID") == Long.class, "Type ID preserved as LONG");
        check(types.get("NAME") == String.class, "Type NAME preserved as STRING");
        check(types.get("AGE") == Integer.class, "Type AGE preserved as INTEGER");
        check(types.get("ACTIVE") == Boolean.class, "Type ACTIVE preserved as BOOLEAN");
        check(types.get("BIRTHDATE") == LocalDate.class, "Type BIRTHDATE preserved as DATE");
        check(types.get("LAST_LOGIN") == LocalDateTime.class, "Type LAST_LOGIN preserved as DATETIME");
        check(types.get("USER_SCORE") == Long.class, "Type USER_SCORE preserved as LONG");
        check(types.get("BALANCE") == BigDecimal.class, "Type BALANCE preserved as BIGDECIMAL");
        check(types.get("SCORE") == Float.class, "Type SCORE preserved as FLOAT");
        check(types.get("PRECISION") == Double.class, "Type PRECISION preserved as DOUBLE");
        check(types.get("INITIAL") == Character.class, "Type INITIAL preserved as CHAR");
        check(types.get("SESSION_ID") == UUID.class, "Type SESSION_ID preserved as UUID");

        Map<String, Object> row = table.getRows().get(0);
        check(row.get("ID").equals(1L), "Value ID round-tripped (1)");
        check("Alice".equals(row.get("NAME")), "Value NAME round-tripped preserving string literal case");
        check(Integer.valueOf(25).equals(row.get("AGE")), "Value AGE round-tripped (25)");
        check(Boolean.TRUE.equals(row.get("ACTIVE")), "Value ACTIVE round-tripped (TRUE)");
        check(LocalDate.of(1998, 5, 20).equals(row.get("BIRTHDATE")), "Value BIRTHDATE round-tripped");
        check(LocalDateTime.of(2023, 10, 15, 14, 30, 0).equals(row.get("LAST_LOGIN")), "Value LAST_LOGIN round-tripped");
        check(Long.valueOf(1000000L).equals(row.get("USER_SCORE")), "Value USER_SCORE round-tripped");
        check(new BigDecimal("123.45").equals(row.get("BALANCE")), "Value BALANCE round-tripped");
        check(Float.valueOf(99.75f).equals(row.get("SCORE")), "Value SCORE round-tripped");
        check(Double.valueOf(123456.789012).equals(row.get("PRECISION")), "Value PRECISION round-tripped");
        check(Character.valueOf('A').equals(row.get("INITIAL")), "Value INITIAL round-tripped");
        check(UUID.fromString("123e4567-e89b-12d3-a456-426614174000").equals(row.get("SESSION_ID")), "Value SESSION_ID round-tripped");

        Object result = reloaded.executeQuery("SELECT NAME, AGE FROM " + TABLE + " WHERE AGE = 25", null);
        check(result instanceof List && ((List<?>) result).size() == 1, "SELECT works on loaded table");
        LOGGER.log(Level.INFO, "Test testSchemaAndTypesPreserved: DONE");
    }

    private void testSequenceContinuesAfterLoad() {
        LOGGER.log(Level.INFO, "Starting test: testSequenceContinuesAfterLoad");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Bob')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);
        check(table.getRows().size() == 2, "Rows preserved before sequence continuation");

        reloaded.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Carol')", null);
        List<Map<String, Object>> rows = table.getRows();
        check(rows.size() == 3, "INSERT works on loaded table");
        check(Long.valueOf(3L).equals(rows.get(2).get("ID")), "Sequence continued after load (next ID = 3)");
        LOGGER.log(Level.INFO, "Test testSequenceContinuesAfterLoad: DONE");
    }

    private void testIndexesFunctionalAfterLoad() {
        LOGGER.log(Level.INFO, "Starting test: testIndexesFunctionalAfterLoad");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID STRING PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        db.executeQuery("CREATE INDEX ON " + TABLE + " (AGE)", null);
        db.executeQuery("CREATE HASH INDEX ON " + TABLE + " (NAME)", null);
        for (int i = 1; i <= 50; i++) {
            db.executeQuery("INSERT INTO " + TABLE + " (ID, NAME, AGE) VALUES ('ID" + i + "', 'Name" + i + "', " + (20 + i) + ")", null);
        }
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);

        check(table.getIndexes().containsKey("AGE"), "B-tree index rebuilt after load");
        check(table.getIndexes().containsKey("NAME"), "Hash index rebuilt after load");
        check(table.hasClusteredIndex() && "ID".equals(table.getClusteredIndexColumn()), "Clustered index rebuilt after load");

        List<Map<String, Object>> result = (List<Map<String, Object>>) reloaded.executeQuery(
                "SELECT ID, NAME FROM " + TABLE + " WHERE AGE = 60", null);
        check(result.size() == 1 && "ID40".equals(result.get(0).get("ID")), "B-tree index lookup works after load");
        result = (List<Map<String, Object>>) reloaded.executeQuery(
                "SELECT ID, NAME FROM " + TABLE + " WHERE NAME = 'Name30'", null);
        check(result.size() == 1 && "ID30".equals(result.get(0).get("ID")), "Hash index lookup works after load");
        int expectedRow = -1;
        List<Map<String, Object>> rows = table.getRows();
        for (int i = 0; i < rows.size(); i++) {
            if ("ID30".equals(rows.get(i).get("ID"))) {
                expectedRow = i;
                break;
            }
        }
        check(expectedRow >= 0, "Located row index for ID30 after clustered load");
        check(table.getIndex("NAME").search("Name30").equals(List.of(expectedRow)), "Rebuilt hash index search returns correct row index");
        int ageRow = -1;
        for (int i = 0; i < rows.size(); i++) {
            if ("ID40".equals(rows.get(i).get("ID"))) {
                ageRow = i;
                break;
            }
        }
        check(table.getIndex("AGE").search(60).equals(List.of(ageRow)), "Rebuilt B-tree index search returns correct row index");
        LOGGER.log(Level.INFO, "Test testIndexesFunctionalAfterLoad: DONE");
    }

    private void testLoadTablesFromDisk() {
        LOGGER.log(Level.INFO, "Starting test: testLoadTablesFromDisk");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + "2 (ID STRING PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + "2 (ID, NAME, AGE) VALUES ('A1', 'Alpha', 10)", null);
        db.executeQuery("INSERT INTO " + TABLE + "2 (ID, NAME, AGE) VALUES ('A2', 'Beta', 20)", null);
        db.saveTablesToDisk();

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE + "2");
        check(table.getRows().size() == 2, "Database.loadTablesFromDisk loads tables into new Database");
        Object result = reloaded.executeQuery("SELECT NAME FROM " + TABLE + "2 WHERE AGE = 20", null);
        check(result instanceof List && ((List<?>) result).size() == 1, "Queries work after Database.loadTablesFromDisk");
        LOGGER.log(Level.INFO, "Test testLoadTablesFromDisk: DONE");
    }

    private void testDropTableDeletesFiles() {
        LOGGER.log(Level.INFO, "Starting test: testDropTableDeletesFiles");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID STRING PRIMARY KEY, NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (ID, NAME) VALUES ('X1', 'Xray')", null);
        check(new File(TABLE + ".table").exists(), "Serialized file exists before drop");
        check(new File(TABLE + ".csv").exists(), "CSV file exists before drop");
        db.dropTable(TABLE);
        check(!new File(TABLE + ".table").exists(), "Serialized file deleted after drop");
        check(!new File(TABLE + ".csv").exists(), "CSV file deleted after drop");
        LOGGER.log(Level.INFO, "Test testDropTableDeletesFiles: DONE");
    }

    public static void main(String[] args) {
        PersistenceTest test = new PersistenceTest();
        test.runTests();
    }
}
