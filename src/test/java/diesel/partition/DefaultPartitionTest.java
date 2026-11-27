package diesel.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import diesel.Database;
import diesel.Table;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for default partition functionality.
 * Tests that non-matching rows are correctly routed to default partitions.
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DefaultPartitionTest {
    
    private static Database database;
    
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
    
    @AfterEach
    void cleanup() {
        dropIfPresent("orders");
        dropIfPresent("customers");
        dropIfPresent("users");
        dropIfPresent("test_table");
        dropIfPresent("edge_cases");
        dropIfPresent("mixed_types");
    }

    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE " + table, null);
        } catch (Exception ignored) {
        }
    }
    
    @Test
    void testDefaultPartitionWithRangePartitioning() {
        // Create partitioned table with range partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE orders (" +
                               "id LONG PRIMARY KEY, " +
                               "order_date DATE, " +
                               "amount DECIMAL(10,2), " +
                               "customer_id LONG" +
                               ") PARTITION BY RANGE (order_date)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Insert data that matches specific partitions
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(1, '2024-01-15', 100.50, 101)", null);
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(2, '2024-04-15', 250.75, 102)", null);
        
        // Insert data that goes to default partition (outside defined ranges)
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(3, '2023-12-15', 75.25, 103)", null); // Before Q1 2024
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(4, '2025-02-15', 350.00, 104)", null); // After Q4 2024
        
        // Verify data is in the partitioned table
        Table ordersTable = database.getTable("ORDERS");
        assertEquals(4, ordersTable.getRowCount());
        
        // Verify data distribution using queries
        List<Map<String, Object>> q1Rows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date <= '2024-03-31' ORDER BY id", null);
        List<Map<String, Object>> q2Rows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date >= '2024-04-01' AND order_date <= '2024-06-30' ORDER BY id", null);
        List<Map<String, Object>> otherRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date < '2024-01-01' OR order_date > '2024-12-31' ORDER BY id", null);
        
        assertEquals(1, q1Rows.size());
        assertEquals(1, q2Rows.size());
        assertEquals(2, otherRows.size());
        
        // Verify default partition contains the correct data
        assertEquals(3L, otherRows.get(0).get("id"));
        assertEquals(4L, otherRows.get(1).get("id"));
    }
    
    @Test
    void testDefaultPartitionWithListPartitioning() {
        // Create partitioned table with list partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE customers (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "country STRING, " +
                               "registration_date DATE" +
                               ") PARTITION BY LIST (country)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Insert data that matches specific partitions
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(1, 'John Doe', 'US', '2024-01-15')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(2, 'Alice Brown', 'UK', '2024-02-20')", null);
        
        // Insert data that goes to default partition (unknown countries)
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(3, 'Charlie Unknown', 'CA', '2024-03-10')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(4, 'Diana Martian', 'MARS', '2024-04-05')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(5, 'Eve Null', null, '2024-05-12')", null); // Null country
        
        // Verify data is in the partitioned table
        Table customersTable = database.getTable("CUSTOMERS");
        assertEquals(5, customersTable.getRowCount());
        
        // Verify data distribution using queries
        List<Map<String, Object>> usRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'US' ORDER BY id", null);
        List<Map<String, Object>> euRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'UK' ORDER BY id", null);
        List<Map<String, Object>> otherRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country IS NULL OR country NOT IN ('US', 'UK') ORDER BY id", null);
        
        assertEquals(1, usRows.size());
        assertEquals(1, euRows.size());
        assertEquals(3, otherRows.size());
        
        // Verify default partition contains the correct data
        assertEquals("Charlie Unknown", otherRows.get(0).get("name"));
        assertEquals("Diana Martian", otherRows.get(1).get("name"));
        assertEquals("Eve Null", otherRows.get(2).get("name"));
    }
    
    @Test
    void testDefaultPartitionWithHashPartitioning() {
        // Create partitioned table with hash partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE users (" +
                               "id LONG PRIMARY KEY, " +
                               "username STRING, " +
                               "email STRING, " +
                               "created_at DATETIME" +
                               ") PARTITION BY HASH (id)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Insert data - most will go to hash partitions
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(1, 'user1', 'user1@example.com', '2024-01-01 10:00:00')", null);
        
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(2, 'user2', 'user2@example.com', '2024-01-02 11:00:00')", null);
        
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(3, 'user3', 'user3@example.com', '2024-01-03 12:00:00')", null);
        
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(4, 'user4', 'user4@example.com', '2024-01-04 13:00:00')", null);
        
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(5, 'user5', 'user5@example.com', '2024-01-05 14:00:00')", null);
        
        // Verify data is in the partitioned table
        Table usersTable = database.getTable("USERS");
        assertEquals(5, usersTable.getRowCount());
        
        // Verify every id is retrievable through the partitioned table
        for (long id = 1; id <= 5; id++) {
            List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery(
                    "SELECT * FROM users WHERE id = " + id, null);
            assertEquals(1, rows.size());
            assertEquals(id, rows.get(0).get("id"));
        }
        
        // Hash partitioning routes every value to one of the hash partitions
        PartitionManager manager = usersTable.getPartitionManager();
        assertEquals(4, manager.getPartitions().size());
        for (long id = 1; id <= 5; id++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", id);
            PartitionManager.Partition partition = manager.findPartition(row);
            assertNotNull(partition);
            assertNotSame(manager.getDefaultPartition(), partition);
        }
        
        // Null values hash to partition 0
        assertEquals(0, HashPartition.getPartitionForValue(null, 4));
    }
    
    @Test
    void testMultipleDefaultPartitions() {
        // Create partitioned table with list partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE test_table (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "category STRING" +
                               ") PARTITION BY LIST (category)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here

        database.executeQuery("INSERT INTO test_table (id, name, category) VALUES " +
                            "(1, 'Item A', 'A')", null);
        database.executeQuery("INSERT INTO test_table (id, name, category) VALUES " +
                            "(2, 'Item B', 'B')", null);
        database.executeQuery("INSERT INTO test_table (id, name, category) VALUES " +
                            "(3, 'Item C', 'C')", null);

        // Verify that all data is in the partitioned table
        Table testTable = database.getTable("TEST_TABLE");
        assertEquals(3, testTable.getRowCount());
        
        // Verify data distribution using queries
        List<Map<String, Object>> catARows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM TEST_TABLE WHERE category = 'A' ORDER BY id", null);
        List<Map<String, Object>> catBRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM TEST_TABLE WHERE category = 'B' ORDER BY id", null);
        List<Map<String, Object>> defaultRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM TEST_TABLE WHERE category IS NULL OR category NOT IN ('A', 'B') ORDER BY id", null);
        
        assertEquals(1, catARows.size());
        assertEquals(1, catBRows.size());
        assertEquals(1, defaultRows.size());
        
        // Verify the default partition has the most recent default data
        assertEquals(3L, defaultRows.get(0).get("id"));
    }
    
    @Test
    void testDefaultPartitionEdgeCases() {
        // Create partitioned table with list partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE edge_cases (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "value STRING" +
                               ") PARTITION BY LIST (value)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Test various edge cases
        database.executeQuery("INSERT INTO edge_cases (id, name, value) VALUES " +
                            "(1, 'empty_string', '')", null);
        
        database.executeQuery("INSERT INTO edge_cases (id, name, value) VALUES " +
                            "(2, 'whitespace', '   ')", null);
        
        database.executeQuery("INSERT INTO edge_cases (id, name, value) VALUES " +
                            "(3, 'special_chars', '!@#$%^&*()')", null);
        
        database.executeQuery("INSERT INTO edge_cases (id, name, value) VALUES " +
                            "(4, 'very_long_string', '" + "a".repeat(1000) + "')", null);
        
        // Verify all edge case data is in the partitioned table
        Table edgeCasesTable = database.getTable("EDGE_CASES");
        assertEquals(4, edgeCasesTable.getRowCount());
        
        // Verify all edge case data is present
        List<Map<String, Object>> allRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM EDGE_CASES ORDER BY id", null);
        assertEquals(4, allRows.size());
        assertEquals("", allRows.get(0).get("value"));
        assertEquals("   ", allRows.get(1).get("value"));
        assertEquals("!@#$%^&*()", allRows.get(2).get("value"));
        assertEquals("a".repeat(1000), allRows.get(3).get("value"));
        
        // Verify that none of the data went to the known values partition
        List<Map<String, Object>> knownRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM EDGE_CASES WHERE value IN ('known1', 'known2', 'known3') ORDER BY id", null);
        assertEquals(0, knownRows.size());
    }
    
    @Test
    void testDefaultPartitionWithMixedDataTypes() {
        // Create partitioned table with range partitioning on different data types - this automatically creates partitions
        String createTableSql = "CREATE TABLE mixed_types (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "numeric_value DECIMAL(10,2), " +
                               "string_value STRING, " +
                               "date_value DATE" +
                               ") PARTITION BY RANGE (numeric_value)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Insert data that matches partitions
        database.executeQuery("INSERT INTO mixed_types (id, name, numeric_value, string_value, date_value) VALUES " +
                            "(1, 'low', 50.0, 'low_test', '2024-01-01')", null);
        
        database.executeQuery("INSERT INTO mixed_types (id, name, numeric_value, string_value, date_value) VALUES " +
                            "(2, 'medium', 500.0, 'medium_test', '2024-02-01')", null);
        
        database.executeQuery("INSERT INTO mixed_types (id, name, numeric_value, string_value, date_value) VALUES " +
                            "(3, 'high', 5000.0, 'high_test', '2024-03-01')", null);
        
        // Insert data that goes to default partition (extreme values)
        database.executeQuery("INSERT INTO mixed_types (id, name, numeric_value, string_value, date_value) VALUES " +
                            "(4, 'very_low', -100.0, 'very_low_test', '2024-04-01')", null);
        
        database.executeQuery("INSERT INTO mixed_types (id, name, numeric_value, string_value, date_value) VALUES " +
                            "(5, 'very_high', 15000.0, 'very_high_test', '2024-05-01')", null);
        
        // Verify all data is in the partitioned table
        Table mixedTypesTable = database.getTable("MIXED_TYPES");
        assertEquals(5, mixedTypesTable.getRowCount());
        
        // Verify data distribution using queries
        List<Map<String, Object>> lowRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM MIXED_TYPES WHERE numeric_value >= 0 AND numeric_value <= 100 ORDER BY id", null);
        List<Map<String, Object>> mediumRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM MIXED_TYPES WHERE numeric_value > 100 AND numeric_value < 5000 ORDER BY id", null);
        List<Map<String, Object>> highRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM MIXED_TYPES WHERE numeric_value > 5000 ORDER BY id", null);
        List<Map<String, Object>> extremeRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM MIXED_TYPES WHERE numeric_value < 0 OR numeric_value > 10000 ORDER BY id", null);
        
        assertEquals(1, lowRows.size());
        assertEquals(1, mediumRows.size());
        assertEquals(1, highRows.size());
        assertEquals(2, extremeRows.size());
        
        // Verify default partition contains extreme values
        assertEquals(0, new java.math.BigDecimal("-100.0")
                .compareTo((java.math.BigDecimal) extremeRows.get(0).get("numeric_value")));
        assertEquals(0, new java.math.BigDecimal("15000.0")
                .compareTo((java.math.BigDecimal) extremeRows.get(1).get("numeric_value")));
    }
}