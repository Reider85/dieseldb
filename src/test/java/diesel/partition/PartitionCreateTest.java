package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import diesel.Database;
import diesel.Table;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Tag;

/**
 * Tests for partition creation functionality.
 * Tests all three partition types: RANGE, LIST, and HASH.
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PartitionCreateTest {
    
    @TempDir
    static Path tempDir;
    
    private static Database database;
    
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
    void setUp() {
        // Use the same database instance for all test methods
    }
    
    @AfterEach
    void tearDown() {
        dropIfPresent("orders");
        dropIfPresent("customers");
        dropIfPresent("users");
        dropIfPresent("test_table");
        dropIfPresent("regular_table");
        dropIfPresent("test_range");
        dropIfPresent("test_list");
        dropIfPresent("test_hash");
    }

    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE " + table, null);
        } catch (Exception ignored) {
        }
    }
    
    @Test
    void testRangePartitionCreation() {
        // Create a partitioned table
        String createTableSql = "CREATE TABLE orders (" +
                               "id LONG PRIMARY KEY, " +
                               "order_date DATE, " +
                               "amount DECIMAL(10,2), " +
                               "customer_id LONG" +
                               ") PARTITION BY RANGE (order_date)";
        
        database.executeQuery(createTableSql, null);
        
        // Create range partitions
        database.createPartition("ORDERS", "q1_2024", null, 
                               java.time.LocalDate.of(2024, 1, 1), 
                               java.time.LocalDate.of(2024, 4, 1), null, false);
        
        database.createPartition("ORDERS", "q2_2024", null, 
                               java.time.LocalDate.of(2024, 4, 1), 
                               java.time.LocalDate.of(2024, 7, 1), null, false);
        
        database.createPartition("ORDERS", "q3_2024", null, 
                               java.time.LocalDate.of(2024, 7, 1), 
                               java.time.LocalDate.of(2024, 10, 1), null, false);
        
        database.createPartition("ORDERS", "q4_2024", null, 
                               java.time.LocalDate.of(2024, 10, 1), 
                               java.time.LocalDate.of(2025, 1, 1), null, false);
        
        // Verify partitioned table was created
        Table ordersTable = database.getTable("ORDERS");
        assertNotNull(ordersTable);
        assertTrue(ordersTable.isPartitioned());
        assertEquals("RANGE", ordersTable.getPartitionType());
        assertEquals("order_date", ordersTable.getPartitionColumn());
        
        PartitionManager partitionManager = ordersTable.getPartitionManager();
        assertNotNull(partitionManager);
        assertEquals(5, partitionManager.getPartitions().size()); // 4 range partitions + 1 default
        
        // Verify partitions exist in the partition manager (not as separate tables)
        // The actual partition names will be different since we create them automatically
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("q1_2024")));
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("q2_2024")));
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("q3_2024")));
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("q4_2024")));
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("default")));
    }
    
    @Test
    void testListPartitionCreation() {
        // Create a partitioned table
        String createTableSql = "CREATE TABLE customers (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "country STRING, " +
                               "registration_date DATE" +
                               ") PARTITION BY LIST (country)";
        
        database.executeQuery(createTableSql, null);
        
        // Verify partitioned table was created automatically with partitions
        Table customersTable = database.getTable("CUSTOMERS");
        assertNotNull(customersTable);
        assertTrue(customersTable.isPartitioned());
        assertEquals("LIST", customersTable.getPartitionType());
        assertEquals("country", customersTable.getPartitionColumn());
        
        PartitionManager partitionManager = customersTable.getPartitionManager();
        assertNotNull(partitionManager);
        assertEquals(1, partitionManager.getPartitions().size()); // Only default partition created automatically
        
        // Verify default partition exists in the partition manager
        assertTrue(partitionManager.getPartitions().stream().anyMatch(p -> p.getName().contains("default")));
    }
    
    @Test
    void testHashPartitionCreation() {
        // Create a partitioned table
        String createTableSql = "CREATE TABLE users (" +
                               "id LONG PRIMARY KEY, " +
                               "username STRING, " +
                               "email STRING, " +
                               "created_at DATETIME" +
                               ") PARTITION BY HASH (id)";
        
        database.executeQuery(createTableSql, null);
        
        // Verify partitioned table was created
        Table usersTable = database.getTable("USERS");
        assertNotNull(usersTable);
        assertTrue(usersTable.isPartitioned());
        assertEquals("HASH", usersTable.getPartitionType());
        assertEquals("id", usersTable.getPartitionColumn());
        
        PartitionManager partitionManager = usersTable.getPartitionManager();
        assertNotNull(partitionManager);
        assertEquals(4, partitionManager.getPartitions().size());
        
        // Verify partitions exist in the partition manager (not as separate tables)
        for (int i = 0; i < 4; i++) {
            final int index = i;
            assertTrue(partitionManager.getPartitions().stream()
                    .anyMatch(p -> p.getName().contains("partition_" + index)));
        }
    }
    
    @Test
    void testMultipleDefaultPartitions() {
        // Create a partitioned table
        String createTableSql = "CREATE TABLE test_table (" +
                               "id LONG PRIMARY KEY, " +
                               "value STRING" +
                               ") PARTITION BY LIST (value)";
        
        database.executeQuery(createTableSql, null);
        
        // Verify partitioned table was created automatically with default partition
        Table testTable = database.getTable("TEST_TABLE");
        PartitionManager partitionManager = testTable.getPartitionManager();
        
        // Should have one default partition created automatically
        assertEquals(1, partitionManager.getPartitions().size());
        
        // Should have a default partition
        assertNotNull(partitionManager.getDefaultPartition());
        assertTrue(partitionManager.getDefaultPartition().getName().contains("default"));
    }
    
    @Test
    void testPartitionCreationValidation() {
        // Test creating partition on non-existent table
        assertThrows(IllegalArgumentException.class, () -> {
            database.createPartition("NONEXISTENT_TABLE", "test_partition", null, null, null, 0, false);
        });
        
        // Create regular table (not partitioned)
        database.executeQuery("CREATE TABLE regular_table (id LONG PRIMARY KEY, name STRING)", null);
        
        // Test creating partition on non-partitioned table
        assertThrows(IllegalArgumentException.class, () -> {
            database.createPartition("REGULAR_TABLE", "test_partition", null, null, null, 0, false);
        });
        
        // Test range partition without min/max values
        database.executeQuery("CREATE TABLE test_range (id LONG PRIMARY KEY, value DATE) PARTITION BY RANGE (value)", null);
        assertThrows(IllegalArgumentException.class, () -> {
            database.createPartition("TEST_RANGE", "bad_range", null, null, null, 0, false);
        });
        
        // Test list partition without values
        database.executeQuery("CREATE TABLE test_list (id LONG PRIMARY KEY, value STRING) PARTITION BY LIST (value)", null);
        assertThrows(IllegalArgumentException.class, () -> {
            database.createPartition("TEST_LIST", "bad_list", null, null, null, 0, false);
        });
        
        // Test hash partition without partition number
        database.executeQuery("CREATE TABLE test_hash (id LONG PRIMARY KEY, value LONG) PARTITION BY HASH (value)", null);
        assertThrows(IllegalArgumentException.class, () -> {
            database.createPartition("TEST_HASH", "bad_hash", null, null, null, null, false);
        });
    }
    
    
}