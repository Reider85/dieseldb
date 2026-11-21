package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for production catalog restart scenarios (WP3).
 * 
 * This test class verifies:
 * 1. Schema version restoration on database restart
 * 2. Catalog consistency after restart
 * 3. Multi-page catalog format preservation
 * 4. Production-like workloads with restarts
 */
public class ProductionCatalogRestartTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductionCatalogRestartTest.class);
    
    @TempDir
    protected Path tempDir;
    
    protected Database database;
    
    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
    }
    
    private int getTableCount(Database db) {
        Object result = db.executeQuery("SELECT COUNT(*) FROM information_schema.tables", null);
        return Integer.parseInt(result.toString());
    }
    
    @Test
    void testSchemaVersionRestorationOnRestart() throws Exception {
        // Create tables with schema versions
        createSchemaVersionedTables();
        
        // Verify schema versions exist before restart
        verifySchemaVersionsExist();
        
        // Close database (simulating restart)
        database.close();
        
        // Reopen database
        database = new Database();
        database.setDataDir(tempDir.toString());
        
        // Verify schema versions are restored
        verifySchemaVersionsRestored();
        
        LOGGER.info("Schema version restoration on restart working correctly");
    }
    
    @Test
    void testCatalogConsistencyAfterMultipleRestarts() throws Exception {
        // Create initial catalog
        createProductionTables();
        populateProductionData();
        
        // Perform multiple restart cycles
        for (int cycle = 1; cycle <= 3; cycle++) {
            // Close and reopen database
            database.close();
            database = new Database();
            database.setDataDir(tempDir.toString());
            
            // Verify data consistency
            verifyProductionDataConsistency();
            
            // Perform some operations
            performProductionOperations();
            
            LOGGER.info("Restart cycle {} completed successfully", cycle);
        }
        
        LOGGER.info("Catalog consistency after multiple restarts verified");
    }
    
    @Test
    void testLargeCatalogRestart() throws Exception {
        // Create a large catalog (should trigger v2 format)
        createLargeCatalog(60);
        
        // Verify it's working before restart
        assertEquals(60, getTableCount(database));
        
        // Close and restart
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        
        // Verify large catalog survives restart
        assertEquals(60, getTableCount(database));
        
        // Test operations still work
        for (int i = 0; i < 60; i++) {
            String tableName = "production_table_" + i;
            Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertTrue(result.toString().contains("100"), "Table " + tableName + " should have 100 rows");
        }
        
        LOGGER.info("Large catalog restart working correctly");
    }
    
    @Test
    void testMixedSchemaVersionsRestart() throws Exception {
        // Create tables with mixed schema versions
        createMixedSchemaTables();
        
        // Perform operations that trigger schema changes
        performSchemaOperations();
        
        // Close and restart
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        
        // Verify mixed schema versions are preserved
        verifyMixedSchemaPreservation();
        
        // Test that queries still work with different schema versions
        testQueriesWithMixedSchemas();
        
        LOGGER.info("Mixed schema versions restart working correctly");
    }
    
    @Test
    void testCatalogWithWALRestart() throws Exception {
        // Enable WAL for production-like behavior
        System.setProperty("diesel.wal.enabled", "true");
        System.setProperty("diesel.wal.dir", tempDir.resolve("wal").toString());
        
        try {
            // Create database with WAL
            database = new Database(tempDir.toString());
            
            // Create tables and data
            createProductionTables();
            populateProductionData();
            
            // Perform transactions
            performTransactions();
            
            // Close database (flushes WAL)
            database.close();
            
            // Reopen with WAL recovery
            database = new Database(tempDir.toString());
            
            // Verify data is consistent after WAL recovery
            verifyWALRecovery();
            
            LOGGER.info("Catalog with WAL restart working correctly");
        } finally {
            // Reset WAL properties
            System.setProperty("diesel.wal.enabled", "");
            System.setProperty("diesel.wal.dir", "");
        }
    }
    
    @Test
    void testCatalogDirectoryChangeRestart() throws Exception {
        // Create catalog in original directory
        createProductionTables();
        populateProductionData();
        
        // Change directory
        Path newDir = tempDir.resolve("relocated_catalog");
        database.setDataDir(newDir.toString());
        
        // Close and restart in new location
        database.close();
        database = new Database();
        database.setDataDir(newDir.toString());
        
        // Verify catalog works in new location
        verifyRelocatedCatalog();
        
        LOGGER.info("Catalog directory change restart working correctly");
    }
    
    private void createSchemaVersionedTables() throws Exception {
        for (int i = 0; i < 10; i++) {
            String tableName = "versioned_table_" + i;
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, version INTEGER, created DATE)",
                tableName
            );
            database.executeQuery(createQuery, null);
            
            // Add some data
            for (int j = 1; j <= 5; j++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'item_%d_%d', %d, DATE '2023-01-01')",
                    tableName, j, i, j, j
                );
                database.executeQuery(insertQuery, null);
            }
            
            // Create schema version for some tables
            if (i % 2 == 0) {
                Table table = database.getTable(tableName);
                if (table != null) {
                    table.getSchemaVersions().createNewVersion(
                        table.getColumns(), 
                        table.getColumnTypes(), 
                        "Version 1 for table " + i
                    );
                }
            }
        }
    }
    
    private void verifySchemaVersionsExist() {
        for (int i = 0; i < 10; i++) {
            String tableName = "versioned_table_" + i;
            Table table = database.getTable(tableName);
            if (table != null) {
                if (i % 2 == 0) {
                    assertTrue(table.getSchemaVersions().getVersionCount() > 1, 
                        "Table " + tableName + " should have schema versions");
                } else {
                    assertEquals(1, table.getSchemaVersions().getVersionCount(), 
                        "Table " + tableName + " should have default version");
                }
            }
        }
    }
    
    private void verifySchemaVersionsRestored() {
        for (int i = 0; i < 10; i++) {
            String tableName = "versioned_table_" + i;
            Table table = database.getTable(tableName);
            if (table != null) {
                if (i % 2 == 0) {
                    assertTrue(table.getSchemaVersions().getVersionCount() > 1, 
                        "Table " + tableName + " schema versions should be restored");
                }
                // Verify data is still there
                Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
                assertTrue(result.toString().contains("5"), "Table " + tableName + " should have 5 rows");
            }
        }
    }
    
    private void createProductionTables() throws Exception {
        String[] tableNames = {
            "users", "orders", "products", "customers", "suppliers",
            "inventory", "transactions", "payments", "shipments", "returns"
        };
        
        for (String tableName : tableNames) {
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, status STRING, created DATE, updated TIMESTAMP)",
                tableName
            );
            database.executeQuery(createQuery, null);
        }
    }
    
    private void populateProductionData() throws Exception {
        String[] tableNames = {
            "users", "orders", "products", "customers", "suppliers",
            "inventory", "transactions", "payments", "shipments", "returns"
        };
        
        for (String tableName : tableNames) {
            for (int i = 1; i <= 100; i++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'item_%d', 'active', DATE '2023-01-01', TIMESTAMP '2023-01-01 00:00:00')",
                    tableName, i, i
                );
                database.executeQuery(insertQuery, null);
            }
        }
    }
    
    private void verifyProductionDataConsistency() {
        String[] tableNames = {
            "users", "orders", "products", "customers", "suppliers",
            "inventory", "transactions", "payments", "shipments", "returns"
        };
        
        for (String tableName : tableNames) {
            Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertTrue(result.toString().contains("100"), "Table " + tableName + " should have 100 rows");
        }
    }
    
    private void performProductionOperations() throws Exception {
        // Update some records
        database.executeQuery("UPDATE users SET status = 'inactive' WHERE ID BETWEEN 10 AND 20", null);
        
        // Insert new records
        for (int i = 101; i <= 110; i++) {
            database.executeQuery("INSERT INTO users VALUES (" + i + ", 'new_user_" + i + "', 'active', DATE '2023-01-01', TIMESTAMP '2023-01-01 00:00:00')", null);
        }
        
        // Delete some records
        database.executeQuery("DELETE FROM products WHERE ID BETWEEN 50 AND 60", null);
    }
    
    private void createLargeCatalog(int tableCount) throws Exception {
        for (int i = 0; i < tableCount; i++) {
            String tableName = "production_table_" + i;
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, data STRING, index INTEGER, created DATE)",
                tableName
            );
            database.executeQuery(createQuery, null);
            
            // Populate with data
            for (int j = 1; j <= 100; j++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'data_%d_%d', %d, DATE '2023-01-01')",
                    tableName, j, i, j, j
                );
                database.executeQuery(insertQuery, null);
            }
        }
    }
    
    private void createMixedSchemaTables() throws Exception {
        // Create tables with different schema evolution patterns
        for (int i = 0; i < 15; i++) {
            String tableName = "mixed_schema_table_" + i;
            
            if (i < 5) {
                // Tables that will get schema version 1
                String createQuery = String.format(
                    "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, version INTEGER)",
                    tableName
                );
                database.executeQuery(createQuery, null);
            } else if (i < 10) {
                // Tables that will get schema version 2 (add column)
                String createQuery = String.format(
                    "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, status STRING, version INTEGER)",
                    tableName
                );
                database.executeQuery(createQuery, null);
            } else {
                // Tables that will get schema version 3 (add multiple columns)
                String createQuery = String.format(
                    "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, status STRING, priority INTEGER, created DATE, version INTEGER)",
                    tableName
                );
                database.executeQuery(createQuery, null);
            }
            
            // Add some data
            for (int j = 1; j <= 20; j++) {
                String insertQuery;
                if (i < 5) {
                    insertQuery = String.format(
                        "INSERT INTO %s VALUES (%d, 'item_%d_%d', 1)",
                        tableName, j, i, j
                    );
                } else if (i < 10) {
                    insertQuery = String.format(
                        "INSERT INTO %s VALUES (%d, 'item_%d_%d', 'active', 1)",
                        tableName, j, i, j
                    );
                } else {
                    insertQuery = String.format(
                        "INSERT INTO %s VALUES (%d, 'item_%d_%d', 'active', 1, DATE '2023-01-01', 1)",
                        tableName, j, i, j
                    );
                }
                database.executeQuery(insertQuery, null);
            }
        }
    }
    
    private void performSchemaOperations() throws Exception {
        // Evolve schemas for some tables
        for (int i = 0; i < 15; i++) {
            String tableName = "mixed_schema_table_" + i;
            
            if (i < 5) {
                // Add a status column
                database.executeQuery("ALTER TABLE " + tableName + " ADD COLUMN status STRING", null);
                database.executeQuery("UPDATE " + tableName + " SET status = 'active'", null);
                
                Table table = database.getTable(tableName);
                if (table != null) {
                    table.getSchemaVersions().createNewVersion(
                        table.getColumns(), 
                        table.getColumnTypes(), 
                        "Added status column"
                    );
                }
            } else if (i < 10) {
                // Add priority column
                database.executeQuery("ALTER TABLE " + tableName + " ADD COLUMN priority INTEGER", null);
                database.executeQuery("UPDATE " + tableName + " SET priority = 1", null);
                
                Table table = database.getTable(tableName);
                if (table != null) {
                    table.getSchemaVersions().createNewVersion(
                        table.getColumns(), 
                        table.getColumnTypes(), 
                        "Added priority column"
                    );
                }
            }
        }
    }
    
    private void verifyMixedSchemaPreservation() {
        for (int i = 0; i < 15; i++) {
            String tableName = "mixed_schema_table_" + i;
            Table table = database.getTable(tableName);
            if (table != null) {
                assertTrue(table.getSchemaVersions().getVersionCount() >= 1, 
                    "Table " + tableName + " should have at least one schema version");
                
                // Verify data is intact
                Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
                assertTrue(result.toString().contains("20"), "Table " + tableName + " should have 20 rows");
            }
        }
    }
    
    private void testQueriesWithMixedSchemas() throws Exception {
        // Test queries work with different schema versions
        for (int i = 0; i < 15; i++) {
            String tableName = "mixed_schema_table_" + i;
            
            if (i < 5) {
                // Tables with name, status, version
                Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE status = 'active'", null);
                assertTrue(result.toString().contains("20"), "Should find 20 active records");
            } else if (i < 10) {
                // Tables with name, status, priority, version
                Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE priority = 1", null);
                assertTrue(result.toString().contains("20"), "Should find 20 records with priority 1");
            } else {
                // Tables with all columns
                Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE created = DATE '2023-01-01'", null);
                assertTrue(result.toString().contains("20"), "Should find 20 records from 2023-01-01");
            }
        }
    }
    
    private void performTransactions() throws Exception {
        // Perform some transactional operations
        database.beginTransaction(IsolationLevel.READ_COMMITTED);
        try {
            database.executeQuery("INSERT INTO users VALUES (1001, 'transaction_user', 'active', DATE '2023-01-01', TIMESTAMP '2023-01-01 00:00:00')", null);
            database.executeQuery("UPDATE orders SET status = 'shipped' WHERE ID = 1", null);
            database.executeQuery("COMMIT", null);
        } catch (Exception e) {
            database.executeQuery("ROLLBACK", null);
            throw e;
        }
    }
    
    private void verifyWALRecovery() {
        // Verify that committed transactions are preserved
        Object userCount = database.executeQuery("SELECT COUNT(*) FROM users", null);
        assertTrue(userCount.toString().contains("1001"), "Should have transaction_user from WAL recovery");
        
        Object shippedCount = database.executeQuery("SELECT COUNT(*) FROM orders WHERE status = 'shipped'", null);
        assertTrue(shippedCount.toString().contains("1"), "Should have shipped order from WAL recovery");
    }
    
    private void verifyRelocatedCatalog() {
        // Verify all tables are accessible in new location
        String[] tableNames = {
            "users", "orders", "products", "customers", "suppliers",
            "inventory", "transactions", "payments", "shipments", "returns"
        };
        
        for (String tableName : tableNames) {
            Object result = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertTrue(result.toString().contains("100"), "Table " + tableName + " should have 100 rows");
        }
        
        // Verify new operations work
        database.executeQuery("INSERT INTO users VALUES (2001, 'relocated_user', 'active', DATE '2023-01-01', TIMESTAMP '2023-01-01 00:00:00')", null);
        Object newUserCount = database.executeQuery("SELECT COUNT(*) FROM users", null);
        assertTrue(newUserCount.toString().contains("101"), "Should have 101 users after relocation");
    }
}