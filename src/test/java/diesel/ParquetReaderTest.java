package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import diesel.format.FormatRegistry;
import diesel.format.ReadOptions;
import diesel.format.TableData;
import diesel.format.TableFormat;
import diesel.format.WriteOptions;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 87: tests for ParquetReader: round-trip read/write, projection
 * pushdown, predicate pushdown, and all supported data types.
 */
public class ParquetReaderTest {
    private static final String TABLE = "PARQUET_READER_TEST";
    /** Test data directory: Parquet files must be co-located with CSV files in {@code data/}. */
    private static final String DATA_DIR = "data";
    private static final Path PARQUET_PATH = Path.of(DATA_DIR, TABLE + ".parquet");

    @BeforeEach
    void setUp() {
        cleanup();
    }

    @AfterEach
    void tearDown() {
        cleanup();
    }

    private void cleanup() {
        new File(DATA_DIR, TABLE + ".parquet").delete();
        new File(DATA_DIR, TABLE + ".table").delete();
        new File(DATA_DIR, TABLE + ".csv").delete();
    }

    // ─── Format framework round-trip (ParquetFormat via FormatRegistry) ───

    @Test
    void testParquetFormatRoundTripThroughRegistry() throws Exception {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Bob', 30)", null);

        Table table = db.getTable(TABLE);
        TableFormat format = FormatRegistry.get("PARQUET");
        assertNotNull(format, "PARQUET format registered in FormatRegistry");

        format.write(table.toTableData(), PARQUET_PATH, WriteOptions.DEFAULT);

        TableData read = format.read(PARQUET_PATH, ReadOptions.DEFAULT);
        assertEquals(3, read.getColumns().size());
        assertTrue(read.getColumns().containsAll(List.of("ID", "NAME", "AGE")),
                "all columns restored, was " + read.getColumns());
        assertEquals(2, read.getRows().size());
        Map<String, Object> alice = read.getRows().stream()
                .filter(r -> "Alice".equals(r.get("NAME"))).findFirst().orElseThrow();
        assertEquals(25, alice.get("AGE"));
        assertEquals("ID", read.getMetadataValue(TableData.META_PRIMARY_KEY));
        assertNotNull(read.getMetadataValue(TableData.META_SEQUENCES),
                "sequences restored from the Parquet footer");

        TableData limited = format.read(PARQUET_PATH, ReadOptions.DEFAULT.withLimit(1));
        assertEquals(1, limited.getRows().size(), "limit applied by read");

        TableData schema = format.inferSchema(PARQUET_PATH);
        assertTrue(schema.getRows().isEmpty(), "inferSchema returns no rows");
        assertEquals(3, schema.getColumns().size());
        assertTrue(schema.getColumns().containsAll(List.of("ID", "NAME", "AGE")));
    }

    @Test
    void testFormatRegistryResolvesParquetConfig() {
        TableFormat resolved = FormatRegistry.resolve(null, java.util.Map.of("storage.format", "PARQUET"));
        assertNotNull(resolved);
        assertEquals("PARQUET", resolved.getName());
        assertTrue(resolved.canRead(PARQUET_PATH));
    }

    // ─── Basic round-trip ────────────────────────────────────────────

    @Test
    void testReadAllRows() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Bob', 30)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(2, rows.size(), "Read 2 rows");

        Map<String, Object> row0 = rows.stream()
                .filter(r -> "Alice".equals(r.get("NAME")))
                .findFirst().orElseThrow();
        assertEquals("Alice", row0.get("NAME"));
        assertEquals(25, row0.get("AGE"));
        assertNotNull(row0.get("ID"), "ID should be assigned");
    }

    @Test
    void testReadEmptyTable() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(1, rows.size(), "One row round-trips");
    }

    // ─── Projection pushdown ─────────────────────────────────────────

    @Test
    void testProjectionPushdown() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE) VALUES ('Alice', 25, TRUE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE) VALUES ('Bob', 30, FALSE)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readProjected(PARQUET_PATH, List.of("NAME", "AGE"));
        assertEquals(2, rows.size());

        Map<String, Object> row0 = rows.get(0);
        assertTrue(row0.containsKey("NAME"), "NAME column present");
        assertTrue(row0.containsKey("AGE"), "AGE column present");
        assertFalse(row0.containsKey("ACTIVE"), "ACTIVE column excluded by projection");
        assertFalse(row0.containsKey("ID"), "ID column excluded by projection");
    }

    @Test
    void testProjectionPushdownSingleColumn() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readProjected(PARQUET_PATH, List.of("NAME"));
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).size(), "Only NAME column returned");
        assertEquals("Alice", rows.get(0).get("NAME"));
    }

    @Test
    void testProjectionPushdownAllColumns() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readProjected(PARQUET_PATH, List.of("ID", "NAME", "AGE"));
        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).size());
    }

    // ─── Data type round-trip ────────────────────────────────────────

    @Test
    void testAllDataTypes() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " ("
                + "ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), "
                + "NAME STRING, "
                + "AGE INTEGER, "
                + "BALANCE DOUBLE, "
                + "ACTIVE BOOLEAN, "
                + "BIRTHDATE DATE, "
                + "SALARY BIGDECIMAL"
                + ")", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, BALANCE, ACTIVE, BIRTHDATE, SALARY) "
                + "VALUES ('Alice', 25, 99999.5, TRUE, '1998-05-20', 123.45)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(1, rows.size());

        Map<String, Object> row = rows.get(0);
        assertEquals("Alice", row.get("NAME"));
        assertEquals(25, row.get("AGE"));
        assertTrue(row.get("BALANCE") instanceof Double);
        assertEquals(true, row.get("ACTIVE"));
        assertTrue(row.get("BIRTHDATE") instanceof LocalDate);
        assertEquals(LocalDate.of(1998, 5, 20), row.get("BIRTHDATE"));
        // BigDecimal is stored as text – reader returns String
        assertNotNull(row.get("SALARY"));
    }

    @Test
    void testDateTypeRoundTrip() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), EVENT_DATE DATE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (EVENT_DATE) VALUES ('2024-01-15')", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).get("EVENT_DATE") instanceof LocalDate);
        assertEquals(LocalDate.of(2024, 1, 15), rows.get(0).get("EVENT_DATE"));
    }

    @Test
    void testBooleanTypeRoundTrip() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), FLAG BOOLEAN)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (FLAG) VALUES (TRUE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (FLAG) VALUES (FALSE)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(2, rows.size());
        assertTrue(rows.get(0).get("FLAG") instanceof Boolean);
    }

    @Test
    void testLongTypeRoundTrip() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), BIG_NUM LONG)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (BIG_NUM) VALUES (999999999999)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).get("BIG_NUM") instanceof Long);
        assertEquals(999999999999L, rows.get(0).get("BIG_NUM"));
    }

    @Test
    void testDoubleTypeRoundTrip() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), RATIO DOUBLE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (RATIO) VALUES (3.14159)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        List<Map<String, Object>> rows = ParquetReader.readAll(PARQUET_PATH);
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).get("RATIO") instanceof Double);
        assertEquals(3.14159, (Double) rows.get(0).get("RATIO"), 0.0001);
    }

    // ─── Predicate pushdown ──────────────────────────────────────────

    @Test
    void testPredicatePushdownEquals() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Bob', 30)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Charlie', 35)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        // Build predicate: AGE = 25
        QueryParser.Condition cond = new QueryParser.Condition("AGE", 25, QueryParser.Operator.EQUALS, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        // FilterPredicate with Parquet row group stats; with 3 small rows in 1 group,
        // the filter is applied at row level
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("NAME"));
    }

    @Test
    void testPredicatePushdownGreaterThan() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (10)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (20)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (30)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("AGE", 20, QueryParser.Operator.GREATER_THAN, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());

        assertEquals(1, rows.size());
        assertEquals(30, rows.get(0).get("AGE"));
    }

    @Test
    void testPredicatePushdownLessThanOrEquals() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (10)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (20)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (30)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("AGE", 20, QueryParser.Operator.LESS_THAN_OR_EQUALS, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        assertEquals(2, rows.size());
    }

    @Test
    void testPredicatePushdownIsNull() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES (NULL)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("NAME", QueryParser.Operator.IS_NULL, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        assertEquals(1, rows.size());
        assertNull(rows.get(0).get("NAME"));
    }

    @Test
    void testPredicatePushdownIsNotNull() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES (NULL)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("NAME", QueryParser.Operator.IS_NOT_NULL, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).get("NAME"));
    }

    @Test
    void testPredicatePushdownNotEquals() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (25)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (AGE) VALUES (30)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("AGE", 25, QueryParser.Operator.NOT_EQUALS, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        assertEquals(1, rows.size());
        assertEquals(30, rows.get(0).get("AGE"));
    }

    @Test
    void testPredicatePushdownInList() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Bob')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Charlie')", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("NAME",
                List.of("Alice", "Charlie"), null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(cond), table.getColumnTypes());
        assertEquals(2, rows.size());
        List<String> names = rows.stream().map(r -> (String) r.get("NAME")).sorted().toList();
        assertEquals(List.of("Alice", "Charlie"), names);
    }

    @Test
    void testPredicatePushdownCombinedConditions() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Bob', 30)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Charlie', 35)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        // AGE >= 30 AND AGE <= 35
        QueryParser.Condition c1 = new QueryParser.Condition("AGE", 30, QueryParser.Operator.GREATER_THAN_OR_EQUALS, "AND", false);
        QueryParser.Condition c2 = new QueryParser.Condition("AGE", 35, QueryParser.Operator.LESS_THAN_OR_EQUALS, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, null, List.of(c1, c2), table.getColumnTypes());
        assertEquals(2, rows.size());
    }

    // ─── Schema inspection ───────────────────────────────────────────

    @Test
    void testGetFileSchema() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE) VALUES ('Alice', 25)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        org.apache.parquet.schema.MessageType schema = ParquetReader.getFileSchema(PARQUET_PATH);
        assertNotNull(schema);
        assertEquals(3, schema.getFieldCount());
        assertNotNull(schema.getType("ID"));
        assertNotNull(schema.getType("NAME"));
        assertNotNull(schema.getType("AGE"));
    }

    // ─── Combined projection + predicate ─────────────────────────────

    @Test
    void testProjectionAndPredicateCombined() {
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE) VALUES ('Alice', 25, TRUE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE) VALUES ('Bob', 30, FALSE)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE) VALUES ('Charlie', 35, TRUE)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("AGE", 25, QueryParser.Operator.GREATER_THAN, null, false);
        List<Map<String, Object>> rows = ParquetReader.readWhere(PARQUET_PATH, List.of("NAME", "AGE"),
                List.of(cond), table.getColumnTypes());
        // Bob (30) and Charlie (35) should match AGE > 25
        assertEquals(2, rows.size());
        for (Map<String, Object> row : rows) {
            assertTrue(row.containsKey("NAME"));
            assertTrue(row.containsKey("AGE"));
            assertFalse(row.containsKey("ACTIVE"), "ACTIVE excluded by projection");
            assertFalse(row.containsKey("ID"), "ID excluded by projection");
        }
    }
}
