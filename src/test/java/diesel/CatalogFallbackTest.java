package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for catalog fallback mechanism (WP1, WP2, WP3).
 * 
 * This test class verifies:
 * 1. Graceful fallback from page-backed to heap-only catalog
 * 2. Error handling when page files are corrupted or missing
 * 3. Mixed scenarios where some tables use page-backed and others use heap-only
 * 4. Recovery from catalog failures
 */
public class CatalogFallbackTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogFallbackTest.class);
    
    @TempDir
    protected Path tempDir;
    
    protected Database database;
    
    @BeforeEach
    void setUp() {
        database = new Database(tempDir.toString());
    }
    
    private int getTableCount(Database db) {
        Object result = db.executeQuery("SELECT COUNT(*) FROM information_schema.tables", null);
        return Integer.parseInt(result.toString());
    }
    
    @Test
    void testHeapOnlyFallbackOnMissingPages() throws Exception {
        // Create database with missing page files (should trigger fallback)
        // First create some tables to ensure page files would normally be created
        createTestTables(10);
        
        // Delete page files to simulate missing pages
        deletePageFiles();
        
        // Create new database - should fall back to heap-only
        Database fallbackDb = new Database(tempDir.toString());
        
        // Verify fallback worked
        assertNotNull(fallbackDb, "Database should be created successfully");
        assertTrue(getTableCount(fallbackDb) <= 10, "Should have at most 10 tables");
        
        // Verify basic operations work
        fallbackDb.executeQuery("CREATE TABLE fallback_test (ID LONG)", null);
        assertNotNull(fallbackDb.getTable("fallback_test"), "Should have fallback_test table");
        
        LOGGER.info("Heap-only fallback on missing pages working correctly");
    }
    
    @Test
    void testFallbackOnCorruptedPages() throws Exception {
        // Create tables with valid page files
        createTestTables(15);
        populateTestData(15);
        
        // Corrupt page files
        corruptPageFiles();
        
        // Try to create new database - should fall back to heap-only
        Database corruptedDb = new Database(tempDir.toString());
        
        // Verify fallback worked
        assertNotNull(corruptedDb, "Database should be created successfully despite corruption");
        
        // Verify basic operations still work
        corruptedDb.executeQuery("CREATE TABLE recovery_table (ID LONG)", null);
        assertNotNull(corruptedDb.getTable("recovery_table"), "Should have recovery_table table");
        
        LOGGER.info("Fallback on corrupted pages working correctly");
    }
    
    @Test
    void testMixedFallbackScenario() throws Exception {
        // Create some tables normally
        createTestTables(5);
        
        // Manually corrupt page files for some tables
        partiallyCorruptPages();
        
        // Create database - should handle mixed scenario
        Database mixedDb = new Database(tempDir.toString());
        
        // Verify some tables are accessible, others fall back
        assertTrue(getTableCount(mixedDb) > 0, "Should have some accessible tables");
        
        // Test operations work on both accessible and new tables
        mixedDb.executeQuery("CREATE TABLE mixed_test (ID LONG)", null);
        assertNotNull(mixedDb.getTable("mixed_test"), "Should have mixed_test table");
        
        LOGGER.info("Mixed fallback scenario working correctly");
    }
    
    @Test
    void testFallbackWithSchemaVersions() throws Exception {
        // Create tables with schema versions
        createSchemaVersionedTables(8);
        
        // Corrupt page files
        corruptPageFiles();
        
        // Create new database - should fall back but preserve some functionality
        Database schemaFallbackDb = new Database(tempDir.toString());
        
        // Verify basic operations work even with fallback
        schemaFallbackDb.executeQuery("CREATE TABLE schema_fallback_test (ID LONG)", null);
        assertNotNull(schemaFallbackDb.getTable("schema_fallback_test"), "Should have schema_fallback_test table");
        
        // Verify new schema versions can still be created
        Table newTable = schemaFallbackDb.getTable("schema_fallback_test");
        if (newTable != null) {
            newTable.getSchemaVersions().createNewVersion(
                newTable.getColumns(), 
                newTable.getColumnTypes(), 
                "Fallback schema version"
            );
            assertTrue(newTable.getSchemaVersions().getVersionCount() > 1, 
                "Should be able to create schema versions in fallback mode");
        }
        
        LOGGER.info("Fallback with schema versions working correctly");
    }
    
    @Test
    void testFallbackAfterDirectoryChange() throws Exception {
        // Create tables in original directory
        createTestTables(10);
        populateTestData(10);
        
        // Change to new directory (should trigger page file creation there)
        Path newDir = tempDir.resolve("fallback_test_dir");
        database.setDataDir(newDir.toString());
        
        // Create new tables in new directory
        createTestTables(5, newDir.toString());
        
        // Now corrupt the original page files but keep new ones intact
        deletePageFiles(); // This deletes from original tempDir, not newDir
        
        // Create database - should work with new directory pages but fallback for old
        Database dirFallbackDb = new Database(newDir.toString());
        
        // Verify new directory tables work
        assertTrue(getTableCount(dirFallbackDb) >= 5, "Should have tables from new directory");
        
        // Test operations work
        dirFallbackDb.executeQuery("CREATE TABLE dir_fallback_test (ID LONG)", null);
        assertNotNull(dirFallbackDb.getTable("dir_fallback_test"), "Should have dir_fallback_test table");
        
        LOGGER.info("Fallback after directory change working correctly");
    }
    
    @Test
    void testFallbackConsistency() throws Exception {
        // Create database and tables
        createTestTables(12);
        populateTestData(12);
        
        // Close database (flushes to disk)
        database.close();
        
        // Corrupt page files
        corruptPageFiles();
        
        // Create new database with fallback
        Database consistentDb = new Database(tempDir.toString());
        
        // Verify consistency - no data loss, just fallback
        assertEquals(12, getTableCount(consistentDb), "Should have 12 tables");
        
        // Test that all tables can be queried (even if data is from fallback)
        for (int i = 0; i < 12; i++) {
            String tableName = "test_table_" + i;
            Table table = consistentDb.getTable(tableName);
            assertNotNull(table, "Table " + tableName + " should be accessible");
            
            // Test basic query
            Object result = consistentDb.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
            assertNotNull(result, "Query should work on " + tableName);
        }
        
        LOGGER.info("Fallback consistency working correctly");
    }
    
    @Test
    void testFallbackPerformance() throws Exception {
        // Create a large number of tables to test performance with fallback
        createTestTables(50);
        
        // Corrupt page files
        corruptPageFiles();
        
        // Create new database with fallback
        long startTime = System.currentTimeMillis();
        Database perfFallbackDb = new Database(tempDir.toString());
        long creationTime = System.currentTimeMillis() - startTime;
        
        // Verify performance is reasonable
        assertTrue(creationTime < 5000, "Database creation with fallback should be under 5 seconds");
        
        // Test performance of operations
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            String tableName = "test_table_" + i;
            perfFallbackDb.executeQuery("SELECT COUNT(*) FROM " + tableName, null);
        }
        long queryTime = System.currentTimeMillis() - startTime;
        
        assertTrue(queryTime < 3000, "50 table queries with fallback should be under 3 seconds");
        
        LOGGER.info("Fallback performance - creation: {}ms, queries: {}ms", 
            creationTime, queryTime);
    }
    
    @Test
    void testFallbackRecovery() throws Exception {
        // Create scenario where fallback database can recover when page files are restored
        createTestTables(8);
        
        // Corrupt pages
        corruptPageFiles();
        
        // Create fallback database
        Database recoveryDb = new Database(tempDir.toString());
        
        // Verify fallback works
        assertNotNull(recoveryDb);
        
        // Now restore page files (simulate fixing the corruption)
        restorePageFiles();
        
        // Create new database - should now use page-backed catalog
        Database recoveredDb = new Database(tempDir.toString());
        
        // Verify it now uses page-backed catalog
        assertEquals(8, getTableCount(recoveredDb), "Should have all 8 tables");
        
        // Test operations work with page-backed catalog
        recoveredDb.executeQuery("INSERT INTO test_table_0 VALUES (1000, 'recovered_data')", null);
        
        LOGGER.info("Fallback recovery working correctly");
    }
    
    private void createTestTables(int count) throws Exception {
        createTestTables(count, tempDir.toString());
    }
    
    private void createTestTables(int count, String dataDir) throws Exception {
        Database db = new Database(dataDir);
        
        for (int i = 0; i < count; i++) {
            String tableName = "test_table_" + i;
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, value INTEGER, created DATE)",
                tableName
            );
            db.executeQuery(createQuery, null);
        }
        
        db.close();
    }
    
    private void populateTestData(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            String tableName = "test_table_" + i;
            for (int j = 1; j <= 10; j++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'item_%d_%d', %d, DATE '2023-01-01')",
                    tableName, j, i, j, j * 10
                );
                database.executeQuery(insertQuery, null);
            }
        }
    }
    
    private void deletePageFiles() throws Exception {
        Path pageFile = tempDir.resolve("diesel_pages.db");
        if (Files.exists(pageFile)) {
            Files.delete(pageFile);
        }
        
        // Also delete any other page-related files
        Files.walk(tempDir)
            .filter(path -> path.toString().endsWith(".db") || 
                          path.toString().endsWith(".pages") || 
                          path.toString().endsWith(".catalog"))
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (Exception e) {
                    // Ignore delete errors
                }
            });
    }
    
    private void corruptPageFiles() throws Exception {
        Path pageFile = tempDir.resolve("diesel_pages.db");
        if (Files.exists(pageFile)) {
            // Write invalid header to corrupt the file
            byte[] corruption = new byte[1024];
            for (int i = 0; i < corruption.length; i++) {
                corruption[i] = (byte) (i % 256);
            }
            Files.write(pageFile, corruption);
        }
    }
    
    private void partiallyCorruptPages() throws Exception {
        Path pageFile = tempDir.resolve("diesel_pages.db");
        if (Files.exists(pageFile)) {
            // Corrupt only part of the file
            byte[] content = Files.readAllBytes(pageFile);
            if (content.length > 100) {
                // Corrupt the middle section
                for (int i = 50; i < 100; i++) {
                    content[i] = (byte) 0xFF;
                }
                Files.write(pageFile, content);
            }
        }
    }
    
    private void restorePageFiles() throws Exception {
        // This would normally restore from backup, but for testing we'll just
        // recreate the page files by creating a new database
        createTestTables(8);
    }
    
    private void createSchemaVersionedTables(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            String tableName = "schema_table_" + i;
            String createQuery = String.format(
                "CREATE TABLE %s (ID LONG PRIMARY KEY, name STRING, version INTEGER)",
                tableName
            );
            database.executeQuery(createQuery, null);
            
            // Add some data
            for (int j = 1; j <= 5; j++) {
                String insertQuery = String.format(
                    "INSERT INTO %s VALUES (%d, 'item_%d_%d', %d)",
                    tableName, j, i, j, j
                );
                database.executeQuery(insertQuery, null);
            }
            
            // Create schema version
            Table table = database.getTable(tableName);
            if (table != null) {
                table.getSchemaVersions().createNewVersion(
                    table.getColumns(), 
                    table.getColumnTypes(), 
                    "Schema version 1 for table " + i
                );
            }
        }
    }
}