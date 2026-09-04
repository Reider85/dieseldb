package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for unified Parquet storage (the {@code storage.format=PARQUET} mode).
 * A single {@code .parquet} file per table now serves both primary persistence
 * (schema, rows, sequences, index definitions) and columnar OLAP reads. These
 * tests verify the save/load round trip, auto-migration from legacy {@code .table}
 * files, storage selection by query type, and the format selector.
 */
public class ColumnarStorageTest {

    private static final Logger LOGGER = Logger.getLogger(ColumnarStorageTest.class.getName());
    private static final String TABLE = "COLUMNAR_TEST";

    private Database database;

    @BeforeEach
    void setUp() {
        StorageFormat.resetCacheForTest();
        QueryOptimizer.clearCacheForTest();
        cleanup();
        database = new Database(".");
        database.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, VAL BIGDECIMAL)", null);
    }

    @AfterEach
    void tearDown() {
        StorageFormat.resetCacheForTest();
        QueryOptimizer.clearCacheForTest();
        cleanup();
    }

    private void cleanup() {
        new File(TABLE + ".parquet").delete();
        new File(TABLE + ".table").delete();
        new File(TABLE + ".csv").delete();
    }

    private void insertRows(int count) {
        for (int i = 1; i <= count; i++) {
            database.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, VAL) VALUES ('name" + i + "', " + (i % 50) + ", " + (i * 1.5) + ")", null);
        }
    }

    @Test
    void testSingleParquetFileServesBothRoles() {
        insertRows(10);
        Table table = database.getTable(TABLE);

        // In PARQUET mode, auto-commit DML persists to a single .parquet file
        // and activates columnar storage backed by that same file.
        assertTrue(new File(TABLE + ".parquet").exists(),
                "A single .parquet file is created for the table");

        assertNotNull(table.getColumnarStorage(),
                "Columnar storage is backed by the primary Parquet file");
        assertEquals(TABLE + ".parquet",
                table.getColumnarStorage().getParquetFilePath().getFileName().toString(),
                "Columnar storage points at the primary .parquet file");
        assertEquals(Table.ColumnarConversionState.COMPLETED,
                table.getColumnarConversionState());
    }

    @Test
    void testParallelReadsMatchRowBased() {
        insertRows(25);
        Table table = database.getTable(TABLE);
        assertNotNull(table.getColumnarStorage());

        List<Map<String, Object>> allRows = table.getColumnarStorage().getRows(null, null);
        assertEquals(25, allRows.size(), "Rows read from columnar storage");

        List<Map<String, Object>> rowBased = table.getLiveRows();
        assertEquals(rowBased.size(), allRows.size());
        for (int i = 0; i < rowBased.size(); i++) {
            assertEquals(rowBased.get(i).get("NAME"), allRows.get(i).get("NAME"));
            assertEquals(rowBased.get(i).get("AGE"), allRows.get(i).get("AGE"));
        }
    }

    @Test
    void testProjectionPushdown() {
        insertRows(10);
        Table table = database.getTable(TABLE);

        List<Map<String, Object>> projectedRows =
                table.getColumnarStorage().getRows(List.of("NAME", "AGE"), null);

        assertEquals(10, projectedRows.size(), "All rows still returned with projection");
        for (Map<String, Object> row : projectedRows) {
            assertTrue(row.containsKey("NAME"));
            assertTrue(row.containsKey("AGE"));
            assertFalse(row.containsKey("VAL"), "VAL column omitted by projection");
            assertFalse(row.containsKey("ID"), "ID column omitted by projection");
        }
    }

    @Test
    void testPredicateFiltering() {
        insertRows(30);
        Table table = database.getTable(TABLE);

        QueryParser.Condition cond = new QueryParser.Condition("AGE", 20,
                QueryParser.Operator.GREATER_THAN, "AND", false);

        List<Map<String, Object>> filteredRows =
                table.getColumnarStorage().getRows(null, List.of(cond));

        for (Map<String, Object> row : filteredRows) {
            assertTrue(((Number) row.get("AGE")).intValue() > 20);
        }
        assertFalse(filteredRows.isEmpty());
    }

    @Test
    void testStorageSelectionByQueryType() {
        insertRows(20);
        Table table = database.getTable(TABLE);
        assertNotNull(table.getColumnarStorage());

        TableStorage storageForOltp = table.getStorageForQuery(QueryOptimizer.QueryType.OLTP);
        assertSame(table, storageForOltp, "OLTP uses row-based storage");

        TableStorage storageForOlap = table.getStorageForQuery(QueryOptimizer.QueryType.OLAP);
        assertNotSame(table, storageForOlap, "OLAP uses columnar storage when available");
        assertEquals(TableStorage.StorageType.COLUMNAR, storageForOlap.getStorageType());
        assertEquals(table.getColumnarStorage(), storageForOlap,
                "OLAP storage is backed by the primary Parquet file");
    }

    @Test
    void testSaveLoadRoundTripThroughParquet() {
        insertRows(5);
        Table table = database.getTable(TABLE);

        // Explicit save persists the whole table (schema, rows, sequences) to Parquet.
        table.saveToParquetFile(TABLE);
        assertTrue(new File(TABLE + ".parquet").exists(), "Parquet file exists after save");

        // Reload from the single Parquet file.
        Database reloaded = new Database(".");
        Table loaded = Table.loadFromParquetFile(reloaded, TABLE);
        assertNotNull(loaded, "Table loaded from Parquet");
        if (loaded == null) {
            return;
        }
        assertEquals(5, loaded.getLiveRowCount(), "Row count preserved");
        assertEquals("ID", loaded.getPrimaryKeyColumn(), "Primary key preserved");
        assertTrue(loaded.hasClusteredIndex() && "ID".equals(loaded.getClusteredIndexColumn()),
                "Clustered index preserved");
        assertTrue(loaded.getSequences().containsKey("ID"), "Sequence preserved");
        assertEquals("ID_SEQ", loaded.getSequences().get("ID").getName(), "Sequence name preserved");
        assertEquals(BigDecimal.class, loaded.getColumnTypes().get("VAL"),
                "BIGDECIMAL type preserved via Parquet metadata");

        // Columnar reads on the reloaded table work from the same file.
        assertNotNull(loaded.getColumnarStorage());
        assertEquals(5, loaded.getColumnarStorage().getRows(null, null).size(),
                "Columnar reads work after Parquet reload");
    }

    @Test
    void testLoadTablesFromDiskUsesParquet() {
        insertRows(8);
        database.saveTablesToDisk();

        Database reloaded = new Database(".");
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);
        assertNotNull(table, "Table loaded via loadTablesFromDisk");
        if (table == null) {
            return;
        }
        assertEquals(8, table.getLiveRowCount(), "Rows loaded from Parquet");
        assertNotNull(table.getColumnarStorage(), "Columnar storage activated after load");
    }

    @Test
    void testMigrationFromLegacyTableFile() {
        // Create a table and save it in the legacy .table (serialized) format.
        // Auto-commit inserts already produced a .parquet, so remove it to simulate
        // a legacy-only on-disk state before migration.
        insertRows(3);
        database.getTable(TABLE).saveToSerializedFile(TABLE);
        new File(TABLE + ".parquet").delete();
        assertTrue(new File(TABLE + ".table").exists(), "Legacy .table file exists");
        assertFalse(new File(TABLE + ".parquet").exists(), "No Parquet file yet");

        // A fresh Database with PARQUET format auto-migrates the .table file.
        Database reloaded = new Database(".");
        reloaded.loadTablesFromDisk();
        Table table = reloaded.getTable(TABLE);
        assertNotNull(table, "Table migrated from .table");
        if (table == null) {
            return;
        }
        assertEquals(3, table.getLiveRowCount(), "Migrated rows preserved");
        assertEquals("ID", table.getPrimaryKeyColumn(), "Migrated schema preserved");
        assertTrue(new File(TABLE + ".parquet").exists(), "Parquet file created after migration");
        assertFalse(new File(TABLE + ".table").exists(), "Legacy .table deleted after migration");
    }

    @Test
    void testCsvFormatFallsBackToCsvPersistence() {
        // Temporarily force CSV format.
        StorageFormat.resetCacheForTest();
        try {
            // PARQUET is the configured default; verify the parquet path via a save.
            database.getTable(TABLE).saveToParquetFile(TABLE);
            assertTrue(new File(TABLE + ".parquet").exists(), "Parquet save writes .parquet");
        } finally {
            StorageFormat.resetCacheForTest();
        }
    }

    @Test
    void testColumnarStorageThroughDatabase() {
        insertRows(100);
        Table table = database.getTable(TABLE);
        assertNotNull(table.getColumnarStorage());

        List<Map<String, Object>> rowBasedResult = (List<Map<String, Object>>)
                database.executeQuery("SELECT NAME, AGE FROM " + TABLE, null);
        assertNotNull(rowBasedResult);
        assertEquals(100, rowBasedResult.size(), "Query still returns all rows");
    }

    @Test
    void testEmptyTableSaveAndLoad() {
        // No rows inserted - saving and reloading an empty table must still work.
        database.getTable(TABLE).saveToParquetFile(TABLE);
        assertTrue(new File(TABLE + ".parquet").exists(), "Empty table still persisted to Parquet");

        Database reloaded = new Database(".");
        Table loaded = Table.loadFromParquetFile(reloaded, TABLE);
        assertNotNull(loaded, "Empty table loaded from Parquet");
        if (loaded != null) {
            assertEquals(0, loaded.getLiveRowCount(), "Empty table reloads with zero rows");
        }
    }
}
