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
 * Tests for insert routing functionality.
 * Tests that inserts are correctly routed to the appropriate partition.
 */
@Tag("storage")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PartitionInsertTest {
    
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
    }

    private static void dropIfPresent(String table) {
        try {
            database.executeQuery("DROP TABLE " + table, null);
        } catch (Exception ignored) {
        }
    }
    
    @Test
    void testRangePartitionInsertRouting() {
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
        
        // Insert data into Q1 2024
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(1, '2024-01-15', 100.50, 101)", null);
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(2, '2024-02-20', 250.75, 102)", null);
        
        // Insert data into Q2 2024
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(3, '2024-04-15', 175.25, 103)", null);
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(4, '2024-05-10', 300.00, 104)", null);
        
        // Insert data into Q3 2024
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(5, '2024-07-05', 125.50, 105)", null);
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(6, '2024-08-22', 450.80, 106)", null);
        
        // Insert data into Q4 2024
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(7, '2024-10-01', 200.00, 107)", null);
        
        database.executeQuery("INSERT INTO orders (id, order_date, amount, customer_id) VALUES " +
                            "(8, '2024-12-25', 350.25, 108)", null);
        
        // Verify data is in the partitioned table
        Table ordersTable = database.getTable("ORDERS");
        assertNotNull(ordersTable);
        
        // Check total row count
        assertEquals(8, ordersTable.getRowCount());
        
        // Verify specific data using queries with WHERE clauses to test partition routing
        List<Map<String, Object>> q1Rows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date <= '2024-03-31' ORDER BY id", null);
        assertEquals(2, q1Rows.size());
        assertEquals(1L, q1Rows.get(0).get("id"));
        assertEquals(2L, q1Rows.get(1).get("id"));
        
        List<Map<String, Object>> q2Rows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date >= '2024-04-01' AND order_date <= '2024-06-30' ORDER BY id", null);
        assertEquals(2, q2Rows.size());
        assertEquals(3L, q2Rows.get(0).get("id"));
        assertEquals(4L, q2Rows.get(1).get("id"));
    }
    
    @Test
    void testListPartitionInsertRouting() {
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
        
        // Insert data into US partition
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(1, 'John Doe', 'US', '2024-01-15')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(2, 'Jane Smith', 'US', '2024-02-20')", null);
        
        // Insert data into EU partition
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(3, 'Alice Brown', 'UK', '2024-03-10')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(4, 'Bob Johnson', 'DE', '2024-04-05')", null);
        
        // Insert data into Asia partition
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(5, 'Charlie Lee', 'CN', '2024-05-12')", null);
        
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(6, 'Diana Wang', 'SG', '2024-06-18')", null);
        
        // Insert data into default partition (unknown country)
        database.executeQuery("INSERT INTO customers (id, name, country, registration_date) VALUES " +
                            "(7, 'Eve Unknown', 'CA', '2024-07-01')", null);
        
        // Verify data is in the partitioned table
        Table customersTable = database.getTable("CUSTOMERS");
        assertNotNull(customersTable);
        
        // Check total row count
        assertEquals(7, customersTable.getRowCount());
        
        // Verify specific data using queries with WHERE clauses to test partition routing
        List<Map<String, Object>> usRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'US' ORDER BY id", null);
        assertEquals(2, usRows.size());
        assertEquals("John Doe", usRows.get(0).get("name"));
        assertEquals("Jane Smith", usRows.get(1).get("name"));
        
        List<Map<String, Object>> euRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'UK' OR country = 'DE' ORDER BY id", null);
        assertEquals(2, euRows.size());
        assertEquals("Alice Brown", euRows.get(0).get("name"));
        assertEquals("Bob Johnson", euRows.get(1).get("name"));
        
        List<Map<String, Object>> asiaRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'CN' OR country = 'SG' ORDER BY id", null);
        assertEquals(2, asiaRows.size());
        assertEquals("Charlie Lee", asiaRows.get(0).get("name"));
        assertEquals("Diana Wang", asiaRows.get(1).get("name"));
        
        List<Map<String, Object>> otherRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM customers WHERE country = 'CA' ORDER BY id", null);
        assertEquals(1, otherRows.size());
        assertEquals("Eve Unknown", otherRows.get(0).get("name"));
    }
    
    @Test
    void testHashPartitionInsertRouting() {
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
        
        // Insert data - users will be distributed based on hash of id
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
        
        database.executeQuery("INSERT INTO users (id, username, email, created_at) VALUES " +
                            "(6, 'user6', 'user6@example.com', '2024-01-06 15:00:00')", null);
        
        // Verify data is in the partitioned table
        Table usersTable = database.getTable("USERS");
        assertNotNull(usersTable);
        
        // Check total row count
        assertEquals(6, usersTable.getRowCount());
        
        // Verify all users exist in the database
        List<Map<String, Object>> allUsers = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM users ORDER BY id", null);
        assertEquals(6, allUsers.size());
        
        // Test hash distribution by checking users in different ranges
        List<Map<String, Object>> users1_2 = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM users WHERE id >= 1 AND id <= 2 ORDER BY id", null);
        List<Map<String, Object>> users3_4 = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM users WHERE id >= 3 AND id <= 4 ORDER BY id", null);
        List<Map<String, Object>> users5_6 = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM users WHERE id >= 5 AND id <= 6 ORDER BY id", null);
        
        // Each group should have some data (distribution may vary based on hash function)
        int totalRows = users1_2.size() + users3_4.size() + users5_6.size();
        assertEquals(6, totalRows);
    }
    
    @Test
    void testInsertWithNullPartitionKey() {
        // Create partitioned table with list partitioning - this automatically creates partitions
        String createTableSql = "CREATE TABLE test_table (" +
                               "id LONG PRIMARY KEY, " +
                               "name STRING, " +
                               "category STRING" +
                               ") PARTITION BY LIST (category)";
        
        database.executeQuery(createTableSql, null);
        
        // The partitioned table creation automatically creates the partitions
        // So we don't need to manually create them here
        
        // Insert data with null partition key
        database.executeQuery("INSERT INTO test_table (id, name, category) VALUES " +
                            "(1, 'Item 1', 'A')", null);
        
        database.executeQuery("INSERT INTO test_table (id, name, category) VALUES " +
                            "(2, 'Item 2', null)", null); // Null category
        
        // Verify data is in the partitioned table
        Table testTable = database.getTable("TEST_TABLE");
        assertEquals(2, testTable.getRowCount());
        
        // Verify data distribution
        List<Map<String, Object>> categoryARows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM TEST_TABLE WHERE category = 'A' ORDER BY id", null);
        List<Map<String, Object>> nullRows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM TEST_TABLE WHERE category IS NULL ORDER BY id", null);
        
        assertEquals(1, categoryARows.size());
        assertEquals(1, nullRows.size());
        assertEquals("Item 1", categoryARows.get(0).get("name"));
        assertEquals("Item 2", nullRows.get(0).get("name"));
    }
    
    @Test
    void testInsertMultipleRowsSamePartition() {
        database.executeQuery("CREATE TABLE orders (" +
                            "id LONG PRIMARY KEY, " +
                            "order_date DATE, " +
                            "amount DECIMAL(10,2)" +
                            ") PARTITION BY RANGE (order_date)", null);

        // Insert multiple rows in same partition
        database.executeQuery("INSERT INTO orders (id, order_date, amount) VALUES " +
                            "(1, '2024-01-01', 100.00)", null);
        database.executeQuery("INSERT INTO orders (id, order_date, amount) VALUES " +
                            "(2, '2024-02-01', 200.00)", null);
        database.executeQuery("INSERT INTO orders (id, order_date, amount) VALUES " +
                            "(3, '2024-03-01', 300.00)", null);
        
        // Verify all rows are in the partitioned table
        Table ordersTable = database.getTable("ORDERS");
        assertEquals(3, ordersTable.getRowCount());
        
        List<Map<String, Object>> rows = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date <= '2024-03-31' ORDER BY id", null);
        assertEquals(3, rows.size());
        assertEquals(1L, rows.get(0).get("id"));
        assertEquals(2L, rows.get(1).get("id"));
        assertEquals(3L, rows.get(2).get("id"));
    }
}