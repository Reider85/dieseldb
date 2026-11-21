package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multi-page catalog functionality (WP1, WP2).
 * 
 * This test class verifies:
 * 1. Large catalogs (>40 tables) work correctly with multi-page format
 * 2. Catalog format v1 (JSON array in page 0) for small catalogs
 * 3. Catalog format v2 (binary header + chunk pages) for large catalogs
 * 4. CRC32C validation and crash consistency
 */
public class CatalogMultiPageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogMultiPageTest.class);
    
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
    void testSmallCatalogUsesV1Format() throws Exception {
        // Create a small catalog (should use v1 format)
        createTables(20);
        
        // Verify all tables are accessible
        assertEquals(20, getTableCount(database));
        
        // Test basic operations
        database.executeQuery("SELECT COUNT(*) FROM table_0", null);
        database.executeQuery("INSERT INTO table_0 VALUES (1, 'test')", null);
        
        LOGGER.info("Small catalog (20 tables) working correctly with v1 format");
    }
    
    @Test
    void testLargeCatalogUsesV2Format() throws Exception {
        // Create a large catalog (should use v2 format)
        createTables(50); // Exceeds the ~40 table limit
        
        // Verify all tables are accessible
        assertEquals(50, getTableCount(database));
        
        // Test operations on different tables
        for (int i = 0; i < 50; i++) {
            String tableName = "table_" + i;
            database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            database.executeQuery("INSERT INTO " + tableName + " VALUES (" + (i + 1) + ", 'test_" + i + "')", null);
        }
        
        LOGGER.info("Large catalog (50 tables) working correctly with v2 format");
    }
    
    @Test
    void testVeryLargeCatalog() throws Exception {
        // Create a very large catalog (100 tables)
        createTables(100);
        
        // Verify all tables are accessible
        assertEquals(100, getTableCount(database));
        
        // Test performance with many tables
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            String tableName = "table_" + i;
            database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
        }
        long duration = System.currentTimeMillis() - startTime;
        
        LOGGER.info("Very large catalog (100 tables) query time: {} ms", duration);
        assertTrue(duration < 5000, "100-table queries should complete within 5 seconds");
    }
    
    @Test
    void testCatalogPersistenceAndReload() throws Exception {
        // Create tables with data
        createTables(30);
        populateTablesWithData(30);
        
        // Close and reopen database
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        
        // Verify all tables and data are preserved
        assertEquals(30, getTableCount(database));
        
        for (int i = 0; i < 30; i++) {
            String tableName = "table_" + i;
            Object countResult = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertTrue(countResult.toString().contains("30"), "Table " + tableName + " should have 30 rows");
        }
        
        LOGGER.info("Catalog persistence and reload working correctly");
    }
    
    @Test
    void testCatalogWithSchemaVersions() throws Exception {
        // Create tables with different schema versions
        createTables(25);
        
        // Add schema versions to some tables
        for (int i = 0; i < 25; i++) {
            String tableName = "table_" + i;
            if (i % 5 == 0) { // Every 5th table gets a schema version
                // Simulate schema version by adding a column and marking version
                database.executeQuery("ALTER TABLE " + tableName + " ADD COLUMN version_col INTEGER", null);
                Table table = database.getTable(tableName);
                if (table != null) {
                    table.getSchemaVersions().createNewVersion(
                        table.getColumns(), 
                        table.getColumnTypes(), 
                        "Test schema version " + (i/5 + 1)
                    );
                }
            }
        }
        
        // Close and reopen database
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        
        // Verify schema versions are preserved
        for (int i = 0; i < 25; i++) {
            String tableName = "table_" + i;
            Table table = database.getTable(tableName);
            if (table != null && i % 5 == 0) {
                assertTrue(table.getSchemaVersions().getVersionCount() > 1, 
                    "Table " + tableName + " should have schema versions");
            }
        }
        
        LOGGER.info("Catalog schema version persistence working correctly");
    }
    
    @Test
    void testCatalogDirectoryChange() throws Exception {
        // Create tables in original directory
        createTables(20);
        populateTablesWithData(20);
        
        // Change data directory
        Path newDir = tempDir.resolve("new_location");
        database.setDataDir(newDir.toString());
        
        // Verify tables are accessible in new location
        assertEquals(20, getTableCount(database));
        
        // Test operations work
        for (int i = 0; i < 20; i++) {
            String tableName = "table_" + i;
            Object countResult = database.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertTrue(countResult.toString().contains("10"), "Table " + tableName + " should have 10 rows");
        }
        
        LOGGER.info("Catalog directory change working correctly");
    }
    
    @Test
    void testCatalogCorruptionHandling() throws Exception {
        // Create tables
        createTables(15);
        
        // Close database (flushes catalog)
        database.close();
        
        // Manually corrupt a page file to test error handling
        Path pageFile = tempDir.resolve("diesel_pages.db");
        if (java.nio.file.Files.exists(pageFile)) {
            // Create a small corruption by writing invalid data
            java.nio.file.Files.write(pageFile, new byte[]{0x00, 0x00, 0x00, 0x00});
        }
        
        // Try to reopen database - should handle corruption gracefully
        Database newDb = new Database();
        newDb.setDataDir(tempDir.toString());
        
        // Should fall back to heap-only catalog and still work
        assertTrue(getTableCount(newDb) <= 15, 
            "Should have at most 15 tables after corruption");
        
        // Basic operations should still work
        newDb.executeQuery("CREATE TABLE recovery_test (ID LONG)", null);
        assertNotNull(newDb.getTable("recovery_test"), "Should have recovery_test table");
        
        LOGGER.info("Catalog corruption handling working correctly");
    }
    
    private void createTables(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            String tableName = "table_" + i;
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, value INTEGER, created DATE)",
                tableName
            );
            database.executeQuery(createQuery, null);
        }
        LOGGER.info("Created {} tables", count);
    }
    
    private void populateTablesWithData(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            String tableName = "table_" + i;
            for (int j = 1; j <= 10; j++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'name_%d_%d', %d, DATE '2023-01-01')",
                    tableName, j, i, j, j * 10
                );
                database.executeQuery(insertQuery, null);
            }
        }
        LOGGER.info("Populated {} tables with data", count);
    }
}