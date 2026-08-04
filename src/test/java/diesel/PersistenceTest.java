package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistenceTest {
    private static final Logger LOGGER = Logger.getLogger(PersistenceTest.class.getName());
    private static final String TABLE = "PERSIST_TEST";

    @BeforeEach
    void setUp() {
        cleanup();
    }

    @AfterEach
    void tearDown() {
        cleanup();
    }

    private void cleanup() {
        new File(TABLE + ".csv").delete();
        new File(TABLE + ".table").delete();
        new File(TABLE + "2.csv").delete();
        new File(TABLE + "2.table").delete();
    }

    @Test
    void testSaveAndLoadRoundTrip() {
        LOGGER.log(Level.INFO, "Starting test: testSaveAndLoadRoundTrip");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Bob', 30, FALSE, '1993-08-15', '2023-10-16 09:00:00', 2000000, 678.90, 88.50, 987654.321098, 'B', '550e8400-e29b-41d4-a716-446655440000')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        assertTrue(new File(TABLE + ".table").exists(), "Serialized .table file created on disk");
        assertTrue(new File(TABLE + ".csv").exists(), "CSV file created on disk");
        assertTrue(db.getTable(TABLE).isFileInitialized(), "Table marked as initialized after save");

        Database reloaded = new Database();
        Table loaded = Table.loadFromFile(reloaded, TABLE);
        assertNotNull(loaded, "Table loaded from file via loadFromFile");
        if (loaded == null) {
            return;
        }
        assertEquals(TABLE, loaded.getName(), "Loaded table name preserved");
        assertEquals(2, loaded.getRows().size(), "Loaded table row count preserved (2 rows)");
        assertEquals("ID", loaded.getPrimaryKeyColumn(), "Primary key column preserved");
        assertTrue(loaded.hasClusteredIndex() && "ID".equals(loaded.getClusteredIndexColumn()),
                "Clustered index on primary key preserved");
        assertTrue(loaded.getSequences().containsKey("ID"), "Sequence registered under its column name");
        assertEquals("ID_SEQ", loaded.getSequences().get("ID").getName(), "Sequence name preserved");
        assertTrue(loaded.getDatabase() == reloaded, "Loaded table bound to new Database instance");
        LOGGER.log(Level.INFO, "Test testSaveAndLoadRoundTrip: DONE");
    }

    @Test
    void testSchemaAndTypesPreserved() {
        LOGGER.log(Level.INFO, "Starting test: testSchemaAndTypesPreserved");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, LAST_LOGIN DATETIME, USER_SCORE LONG, BALANCE BIGDECIMAL, SCORE FLOAT, PRECISION DOUBLE, INITIAL CHAR, SESSION_ID UUID)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, LAST_LOGIN, USER_SCORE, BALANCE, SCORE, PRECISION, INITIAL, SESSION_ID) VALUES ('Alice', 25, TRUE, '1998-05-20', '2023-10-15 14:30:00', 1000000, 123.45, 99.75, 123456.789012, 'A', '123e4567-e89b-12d3-a456-426614174000')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);

        List<String> expectedColumns = List.of("ID", "NAME", "AGE", "ACTIVE", "BIRTHDATE", "LAST_LOGIN", "USER_SCORE", "BALANCE", "SCORE", "PRECISION", "INITIAL", "SESSION_ID");
        assertEquals(expectedColumns, table.getColumns(), "All column names preserved in order");

        Map<String, Class<?>> types = table.getColumnTypes();
        assertEquals(Long.class, types.get("ID"), "Type ID preserved as LONG");
        assertEquals(String.class, types.get("NAME"), "Type NAME preserved as STRING");
        assertEquals(Integer.class, types.get("AGE"), "Type AGE preserved as INTEGER");
        assertEquals(Boolean.class, types.get("ACTIVE"), "Type ACTIVE preserved as BOOLEAN");
        assertEquals(LocalDate.class, types.get("BIRTHDATE"), "Type BIRTHDATE preserved as DATE");
        assertEquals(LocalDateTime.class, types.get("LAST_LOGIN"), "Type LAST_LOGIN preserved as DATETIME");
        assertEquals(Long.class, types.get("USER_SCORE"), "Type USER_SCORE preserved as LONG");
        assertEquals(BigDecimal.class, types.get("BALANCE"), "Type BALANCE preserved as BIGDECIMAL");
        assertEquals(Float.class, types.get("SCORE"), "Type SCORE preserved as FLOAT");
        assertEquals(Double.class, types.get("PRECISION"), "Type PRECISION preserved as DOUBLE");
        assertEquals(Character.class, types.get("INITIAL"), "Type INITIAL preserved as CHAR");
        assertEquals(UUID.class, types.get("SESSION_ID"), "Type SESSION_ID preserved as UUID");

        Map<String, Object> row = table.getRows().get(0);
        assertEquals(1L, row.get("ID"), "Value ID round-tripped (1)");
        assertEquals("Alice", row.get("NAME"), "Value NAME round-tripped preserving string literal case");
        assertEquals(Integer.valueOf(25), row.get("AGE"), "Value AGE round-tripped (25)");
        assertEquals(Boolean.TRUE, row.get("ACTIVE"), "Value ACTIVE round-tripped (TRUE)");
        assertEquals(LocalDate.of(1998, 5, 20), row.get("BIRTHDATE"), "Value BIRTHDATE round-tripped");
        assertEquals(LocalDateTime.of(2023, 10, 15, 14, 30, 0), row.get("LAST_LOGIN"), "Value LAST_LOGIN round-tripped");
        assertEquals(Long.valueOf(1000000L), row.get("USER_SCORE"), "Value USER_SCORE round-tripped");
        assertEquals(new BigDecimal("123.45"), row.get("BALANCE"), "Value BALANCE round-tripped");
        assertEquals(Float.valueOf(99.75f), row.get("SCORE"), "Value SCORE round-tripped");
        assertEquals(Double.valueOf(123456.789012), row.get("PRECISION"), "Value PRECISION round-tripped");
        assertEquals(Character.valueOf('A'), row.get("INITIAL"), "Value INITIAL round-tripped");
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), row.get("SESSION_ID"), "Value SESSION_ID round-tripped");

        Object result = reloaded.executeQuery("SELECT NAME, AGE FROM " + TABLE + " WHERE AGE = 25", null);
        assertTrue(result instanceof List && ((List<?>) result).size() == 1, "SELECT works on loaded table");
        LOGGER.log(Level.INFO, "Test testSchemaAndTypesPreserved: DONE");
    }

    @Test
    void testSequenceContinuesAfterLoad() {
        LOGGER.log(Level.INFO, "Starting test: testSequenceContinuesAfterLoad");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Bob')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);
        assertEquals(2, table.getRows().size(), "Rows preserved before sequence continuation");

        reloaded.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Carol')", null);
        List<Map<String, Object>> rows = table.getRows();
        assertEquals(3, rows.size(), "INSERT works on loaded table");
        assertEquals(Long.valueOf(3L), rows.get(2).get("ID"), "Sequence continued after load (next ID = 3)");
        LOGGER.log(Level.INFO, "Test testSequenceContinuesAfterLoad: DONE");
    }

    @Test
    void testIndexesFunctionalAfterLoad() {
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

        assertTrue(table.getIndexes().containsKey("AGE"), "B-tree index rebuilt after load");
        assertTrue(table.getIndexes().containsKey("NAME"), "Hash index rebuilt after load");
        assertTrue(table.hasClusteredIndex() && "ID".equals(table.getClusteredIndexColumn()), "Clustered index rebuilt after load");

        List<Map<String, Object>> result = (List<Map<String, Object>>) reloaded.executeQuery(
                "SELECT ID, NAME FROM " + TABLE + " WHERE AGE = 60", null);
        assertTrue(result.size() == 1 && "ID40".equals(result.get(0).get("ID")), "B-tree index lookup works after load");
        result = (List<Map<String, Object>>) reloaded.executeQuery(
                "SELECT ID, NAME FROM " + TABLE + " WHERE NAME = 'Name30'", null);
        assertTrue(result.size() == 1 && "ID30".equals(result.get(0).get("ID")), "Hash index lookup works after load");
        int expectedRow = -1;
        List<Map<String, Object>> rows = table.getRows();
        for (int i = 0; i < rows.size(); i++) {
            if ("ID30".equals(rows.get(i).get("ID"))) {
                expectedRow = i;
                break;
            }
        }
        assertTrue(expectedRow >= 0, "Located row index for ID30 after clustered load");
        assertEquals(List.of(expectedRow), table.getIndex("NAME").search("Name30"), "Rebuilt hash index search returns correct row index");
        int ageRow = -1;
        for (int i = 0; i < rows.size(); i++) {
            if ("ID40".equals(rows.get(i).get("ID"))) {
                ageRow = i;
                break;
            }
        }
        assertEquals(List.of(ageRow), table.getIndex("AGE").search(60), "Rebuilt B-tree index search returns correct row index");
        LOGGER.log(Level.INFO, "Test testIndexesFunctionalAfterLoad: DONE");
    }

    @Test
    void testLoadTablesFromDisk() {
        LOGGER.log(Level.INFO, "Starting test: testLoadTablesFromDisk");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + "2 (ID STRING PRIMARY KEY, NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + "2 (ID, NAME, AGE) VALUES ('A1', 'Alpha', 10)", null);
        db.executeQuery("INSERT INTO " + TABLE + "2 (ID, NAME, AGE) VALUES ('A2', 'Beta', 20)", null);
        db.saveTablesToDisk();

        Database reloaded = new Database();
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE + "2");
        assertEquals(2, table.getRows().size(), "Database.loadTablesFromDisk loads tables into new Database");
        Object result = reloaded.executeQuery("SELECT NAME FROM " + TABLE + "2 WHERE AGE = 20", null);
        assertTrue(result instanceof List && ((List<?>) result).size() == 1, "Queries work after Database.loadTablesFromDisk");
        LOGGER.log(Level.INFO, "Test testLoadTablesFromDisk: DONE");
    }

    @Test
    void testDropTableDeletesFiles() {
        LOGGER.log(Level.INFO, "Starting test: testDropTableDeletesFiles");
        Database db = new Database();
        db.executeQuery("CREATE TABLE " + TABLE + " (ID STRING PRIMARY KEY, NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (ID, NAME) VALUES ('X1', 'Xray')", null);
        db.getTable(TABLE).saveToSerializedFile(TABLE);
        assertTrue(new File(TABLE + ".table").exists(), "Serialized file exists before drop");
        assertTrue(new File(TABLE + ".csv").exists(), "CSV file exists before drop");
        db.dropTable(TABLE);
        assertTrue(!new File(TABLE + ".table").exists(), "Serialized file deleted after drop");
        assertTrue(!new File(TABLE + ".csv").exists(), "CSV file deleted after drop");
        LOGGER.log(Level.INFO, "Test testDropTableDeletesFiles: DONE");
    }
}
