package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;
import diesel.Database;
import diesel.Table;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for partition pruning functionality.
 * Tests various pruning scenarios for RANGE, LIST, and HASH partitioning strategies.
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
public class PartitionPruningTest {
    
    private static Database database;
    private static final String RANGE_TABLE = "orders_pruning_range";
    private static final String LIST_TABLE = "orders_pruning_list";
    private static final String HASH_TABLE = "orders_pruning_hash";
    private static final String PARTITION_COLUMN = "order_date";
    
    @TempDir
    static Path tempDir;
    
    @BeforeAll
    static void setUpDatabase() {
        database = new Database(tempDir.toString());
    }
    
    @AfterAll
    static void tearDownDatabase() {
        if (database != null) {
            database.close();
        }
    }
    
    @BeforeEach
    void setup() {
        // Clean up before each test to ensure isolation
        cleanup();
    }
    
    @AfterEach
    void cleanup() {
        dropIfPresent(RANGE_TABLE);
        dropIfPresent(LIST_TABLE);
        dropIfPresent(HASH_TABLE);
        dropIfPresent("customers");
        dropIfPresent("users");
    }
    
    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE IF EXISTS " + table, null);
        } catch (Exception e) {
            // Ignore if table doesn't exist
        }
    }
    
    // ─── Helper methods for creating test data ────────────────────────────────────
    
    private void createRangePartitionedTable() {
        String sql = String.format(
            "CREATE TABLE %s (" +
            "id INTEGER PRIMARY KEY," +
            "order_date DATE," +
            "customer_id INTEGER," +
            "amount DECIMAL(10,2)" +
            ") PARTITION BY RANGE (%s) (" +
            "PARTITION q1_2024 VALUES LESS THAN ('2024-04-01')," +
            "PARTITION q2_2024 VALUES LESS THAN ('2024-07-01')," +
            "PARTITION q3_2024 VALUES LESS THAN ('2024-10-01')," +
            "PARTITION q4_2024 VALUES LESS THAN ('2025-01-01')," +
            "PARTITION future VALUES LESS THAN MAXVALUE" +
            ")",
            RANGE_TABLE, PARTITION_COLUMN
        );
        database.executeQuery(sql, null);
    }
    
    private void createListPartitionedTable() {
        String sql = String.format(
            "CREATE TABLE %s (" +
            "id INTEGER PRIMARY KEY," +
            "customer_id INTEGER," +
            "country STRING," +
            "amount DECIMAL(10,2)" +
            ") PARTITION BY LIST (%s) (" +
            "PARTITION us VALUES IN ('US')," +
            "PARTITION eu VALUES IN ('UK', 'DE', 'FR')," +
            "PARTITION asia VALUES IN ('JP', 'CN', 'KR')," +
            "PARTITION other VALUES IN (NULL)" +
            ")",
            LIST_TABLE, "country"
        );
        database.executeQuery(sql, null);
    }
    
    private void createHashPartitionedTable() {
        String sql = String.format(
            "CREATE TABLE %s (" +
            "id INTEGER PRIMARY KEY," +
            "customer_id INTEGER," +
            "order_date DATE," +
            "amount DECIMAL(10,2)" +
            ") PARTITION BY HASH (id) PARTITIONS 4",
            HASH_TABLE
        );
        database.executeQuery(sql, null);
    }
    
    private void insertTestData() {
        // Insert test data based on table schema
        String tableSchema = getTableSchema();
        
        if (tableSchema.contains("range")) {
            // Range partitioning with order_date
            database.executeQuery(String.format(
                "INSERT INTO %s (id, order_date, customer_id, amount) VALUES " +
                "(1, '2024-01-15', 101, 100.00), (2, '2024-02-20', 102, 150.50), " +
                "(3, '2024-04-15', 103, 200.00), (4, '2024-05-10', 104, 175.25), " +
                "(5, '2024-07-05', 105, 300.00), (6, '2024-08-20', 106, 225.75), " +
                "(7, '2024-10-10', 107, 400.00), (8, '2024-12-01', 108, 350.50)",
                RANGE_TABLE
            ), null);
        } else if (tableSchema.contains("list")) {
            // List partitioning with country
            database.executeQuery(String.format(
                "INSERT INTO %s (id, customer_id, country, amount) VALUES " +
                "(1, 101, 'US', 100.00), (2, 102, 'UK', 150.50), " +
                "(3, 103, 'DE', 200.00), (4, 104, 'FR', 175.25), " +
                "(5, 105, 'JP', 300.00), (6, 106, 'CN', 225.75), " +
                "(7, 107, 'KR', 400.00), (8, 108, null, 350.50)",
                LIST_TABLE
            ), null);
        } else if (tableSchema.contains("hash")) {
            // Hash partitioning with order_date
            database.executeQuery(String.format(
                "INSERT INTO %s (id, order_date, customer_id, amount) VALUES " +
                "(1, '2024-01-15', 101, 100.00), (2, '2024-02-20', 102, 150.50), " +
                "(3, '2024-04-15', 103, 200.00), (4, '2024-05-10', 104, 175.25), " +
                "(5, '2024-07-05', 105, 300.00), (6, '2024-08-20', 106, 225.75), " +
                "(7, '2024-10-10', 107, 400.00), (8, '2024-12-01', 108, 350.50)",
                HASH_TABLE
            ), null);
        }
    }
    
    private String getTableSchema() {
        try {
            // Try to determine which table type we're working with
            // by checking if specific tables exist
            try {
                database.executeQuery("SELECT * FROM " + RANGE_TABLE + " LIMIT 1", null);
                return "range";
            } catch (Exception e1) {
                try {
                    database.executeQuery("SELECT * FROM " + LIST_TABLE + " LIMIT 1", null);
                    return "list";
                } catch (Exception e2) {
                    try {
                        database.executeQuery("SELECT * FROM " + HASH_TABLE + " LIMIT 1", null);
                        return "hash";
                    } catch (Exception e3) {
                        return "unknown";
                    }
                }
            }
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    // ─── Range Partitioning Tests ────────────────────────────────────────────────
    
    @Test
    void testRangePruningLessThan() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test WHERE order_date < '2024-04-01' - should only scan q1_2024
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put(PARTITION_COLUMN + "_lt", LocalDate.of(2024, 4, 1));
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(1, result.getPartitionsToScan().size(), "Should only scan 1 partition");
        assertEquals(4, result.getPartitionsSkipped(), "Should skip 4 partitions");
        assertEquals(0.8, result.getPruningRatio(), 0.01, "80% pruning ratio");
        assertTrue(result.getReason().contains("Range pruning"));
        
        // Verify the partition that should be scanned
        String scannedPartition = result.getPartitionsToScan().get(0).getName();
        assertEquals("q1_2024", scannedPartition);
    }
    
    @Test
    void testRangePruningLessThanOrEqual() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test WHERE order_date <= '2024-04-01' - should only scan q1_2024 and q2_2024 (if inclusive)
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put(PARTITION_COLUMN + "_lte", LocalDate.of(2024, 4, 1));
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        // Should scan q1_2024 (contains values < '2024-04-01') and possibly q2_2024 if boundary is inclusive
        assertTrue(result.getPartitionsToScan().size() >= 1, "Should scan at least 1 partition");
        assertTrue(result.getPartitionsSkipped() >= 3, "Should skip at least 3 partitions");
    }
    
    @Test
    void testRangePruningGreaterThan() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test WHERE order_date > '2024-07-01' - should scan q3_2024, q4_2024, future
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put(PARTITION_COLUMN + "_gt", LocalDate.of(2024, 7, 1));
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(3, result.getPartitionsToScan().size(), "Should scan 3 partitions");
        assertEquals(2, result.getPartitionsSkipped(), "Should skip 2 partitions");
        assertEquals(0.4, result.getPruningRatio(), 0.01, "40% pruning ratio");
    }
    
    @Test
    void testRangePruningBetween() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test WHERE order_date BETWEEN '2024-04-01' AND '2024-07-01' - should scan q2_2024
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put(PARTITION_COLUMN + "_between_start", LocalDate.of(2024, 4, 1));
        filterValues.put(PARTITION_COLUMN + "_between_end", LocalDate.of(2024, 7, 1));
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(1, result.getPartitionsToScan().size(), "Should only scan 1 partition");
        assertEquals(4, result.getPartitionsSkipped(), "Should skip 4 partitions");
        assertEquals(0.8, result.getPruningRatio(), 0.01, "80% pruning ratio");
    }
    
    @Test
    void testRangePruningNoFilters() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test no filters - should scan all partitions
        Map<String, Object> filterValues = new HashMap<>();
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(5, result.getPartitionsToScan().size(), "Should scan all 5 partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
        assertTrue(result.getReason().contains("No filters"));
    }
    
    @Test
    void testRangePruningNonPartitionColumn() {
        createRangePartitionedTable();
        insertTestData();
        
        // Test filter on non-partition column - should scan all partitions
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("customer_id", 101);
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(5, result.getPartitionsToScan().size(), "Should scan all partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
        assertTrue(result.getReason().contains("Partition column not in filter"));
    }
    
    // ─── List Partitioning Tests ───────────────────────────────────────────────
    
    @Test
    void testListPruningEquality() {
        createListPartitionedTable();
        insertTestData();
        
        // Test WHERE country = 'US' - should only scan us partition
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("country", "US");
        
        PartitionManager partitionManager = getPartitionManager(LIST_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(1, result.getPartitionsToScan().size(), "Should only scan 1 partition");
        assertEquals(3, result.getPartitionsSkipped(), "Should skip 3 partitions");
        assertEquals(0.75, result.getPruningRatio(), 0.01, "75% pruning ratio");
        assertTrue(result.getReason().contains("List pruning"));
        
        // Verify the partition that should be scanned
        String scannedPartition = result.getPartitionsToScan().get(0).getName();
        assertEquals("us", scannedPartition);
    }
    
    @Test
    void testListPruningInClause() {
        createListPartitionedTable();
        insertTestData();
        
        // Test WHERE country IN ('US', 'UK') - should scan us and eu partitions
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("country_in", Arrays.asList("US", "UK"));
        
        PartitionManager partitionManager = getPartitionManager(LIST_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(2, result.getPartitionsToScan().size(), "Should scan 2 partitions");
        assertEquals(2, result.getPartitionsSkipped(), "Should skip 2 partitions");
        assertEquals(0.5, result.getPruningRatio(), 0.01, "50% pruning ratio");
    }
    
    @Test
    void testListPruningNullValue() {
        createListPartitionedTable();
        insertTestData();
        
        // Test WHERE country IS NULL - should scan other partition
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("country", null);
        
        PartitionManager partitionManager = getPartitionManager(LIST_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(1, result.getPartitionsToScan().size(), "Should only scan 1 partition");
        assertEquals(3, result.getPartitionsSkipped(), "Should skip 3 partitions");
        assertEquals(0.75, result.getPruningRatio(), 0.01, "75% pruning ratio");
        
        // Verify the partition that should be scanned
        String scannedPartition = result.getPartitionsToScan().get(0).getName();
        assertEquals("other", scannedPartition);
    }
    
    @Test
    void testListPruningNoMatch() {
        createListPartitionedTable();
        insertTestData();
        
        // Test WHERE country = 'CA' - no partition matches, should only scan default if exists
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("country", "CA");
        
        PartitionManager partitionManager = getPartitionManager(LIST_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        // Should only scan the default partition (other) for values outside every list
        assertEquals(1, result.getPartitionsToScan().size(), "Should scan 1 partition");
        assertEquals(3, result.getPartitionsSkipped(), "Should skip 3 partitions");
        assertEquals(0.75, result.getPruningRatio(), 0.01, "75% pruning ratio");
    }
    
    // ─── Hash Partitioning Tests ────────────────────────────────────────────────
    
    @Test
    void testHashPruning() {
        createHashPartitionedTable();
        insertTestData();
        
        // Test WHERE id = 1 - hash partitioning cannot be pruned
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put("id", 1);
        
        PartitionManager partitionManager = getPartitionManager(HASH_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(4, result.getPartitionsToScan().size(), "Should scan all 4 partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
        assertTrue(result.getReason().contains("Hash partitioning"));
    }
    
    @Test
    void testHashPruningNoFilters() {
        createHashPartitionedTable();
        insertTestData();
        
        // Test no filters - should scan all partitions
        Map<String, Object> filterValues = new HashMap<>();
        
        PartitionManager partitionManager = getPartitionManager(HASH_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(4, result.getPartitionsToScan().size(), "Should scan all 4 partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
    }
    
    // ─── Edge Cases and Error Handling ──────────────────────────────────────────
    
    @Test
    void testPruningWithNullPartitionManager() {
        assertThrows(IllegalArgumentException.class, () -> {
            PartitionPruner.prunePartitions(null, new HashMap<>());
        });
    }
    
    @Test
    void testPruningWithNullFilters() {
        createRangePartitionedTable();
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(null);
        
        assertEquals(5, result.getPartitionsToScan().size(), "Should scan all partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
    }
    
    @Test
    void testPruningWithEmptyFilters() {
        createRangePartitionedTable();
        
        Map<String, Object> filterValues = new HashMap<>();
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        assertEquals(5, result.getPartitionsToScan().size(), "Should scan all partitions");
        assertEquals(0, result.getPartitionsSkipped(), "Should skip 0 partitions");
        assertEquals(0.0, result.getPruningRatio(), 0.01, "0% pruning ratio");
        assertTrue(result.getReason().contains("No filters"));
    }
    
    // ─── Helper Methods ────────────────────────────────────────────────────────
    
    private PartitionManager getPartitionManager(String tableName) {
        Table table = database.getTable(tableName.toUpperCase());
        if (table == null) {
            throw new IllegalArgumentException("Table " + tableName + " not found");
        }
        
        PartitionManager partitionManager = table.getPartitionManager();
        if (partitionManager == null) {
            throw new IllegalArgumentException("Table " + tableName + " is not partitioned");
        }
        
        return partitionManager;
    }
    

    
    @Test
    void testPruningSummaryFormat() {
        createRangePartitionedTable();
        
        Map<String, Object> filterValues = new HashMap<>();
        filterValues.put(PARTITION_COLUMN + "_lt", LocalDate.of(2024, 4, 1));
        
        PartitionManager partitionManager = getPartitionManager(RANGE_TABLE);
        PartitionManager.PartitionPruningResult result = partitionManager.routeSelectWithPruning(filterValues);
        
        String summary = result.getPruningSummary();
        assertTrue(summary.contains("Scan: 1"));
        assertTrue(summary.contains("Skip: 4"));
        assertTrue(summary.contains("Total: 5"));
        assertTrue(summary.contains("Ratio:"));
        assertTrue(summary.contains("Reason:"));
    }
}