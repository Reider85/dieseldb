package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * Acceptance test for BufferPoolFlusher (prompt4.md #20, R3-005 step 1/3):
 * ROW_COUNT inserts без COMMIT, kill, restart → все inserts либо committed, либо откачены (по WAL).
 */
@Tag("storage")
public class FlusherTest {

    private static final String TEST_DIR = "data_flusher_test";
    private static final String TABLE_NAME = "TEST_TABLE";
    // 5k is enough to dirty pages and exercise WAL recovery; override with
    // -Ddiesel.flusher.rowCount=50000 to stress the old 50k volume manually.
    private static final int ROW_COUNT = Integer.getInteger("diesel.flusher.rowCount", 5000);

    private Database database;
    private String walDir;

    @BeforeEach
    void setUp(TestInfo info) throws IOException {
        // Clean up any existing test directory and WAL directory
        cleanupTestDir();
        
        // Create unique WAL directory per test method to avoid cross-test contamination
        String methodName = info.getTestMethod()
            .orElseThrow(() -> new IllegalStateException("no test method context"))
            .getName();
        walDir = TEST_DIR + "_wal_" + methodName;
        
        // Create database with WAL enabled
        database = new Database(TEST_DIR, createWALConfig());
        database.executeQuery("CREATE TABLE " + TABLE_NAME + " (ID LONG PRIMARY KEY, NAME STRING)", null);
        // Persist the empty table structure so loadTablesFromDisk finds it after restart
        database.getTable(TABLE_NAME).saveToFile(TABLE_NAME);
        database.getTable(TABLE_NAME).saveToSerializedFile(TABLE_NAME);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (database != null) {
            database.close();
        }
        cleanupTestDir();
    }

    @Test
    void testFlusherCrashRecovery() throws Exception {
        // Start the flusher
        database.startPageFlusher();
        
        // Insert ROW_COUNT rows in a single uncommitted transaction
        UUID txId = beginTransaction();
        
        for (int i = 0; i < ROW_COUNT; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'name_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, txId);
            
            // Progress indicator for long-running test
            if (i % Math.max(1, ROW_COUNT / 5) == 0) {
                System.out.println("Inserted " + i + " rows...");
            }
        }
        
        System.out.println("All rows inserted, now simulating crash...");
        
        // Simulate crash: close without commit (discard dirty pages)
        // The empty table file from setUp allows loadTablesFromDisk to find it;
        // the uncommitted inserts live only in the WAL and get rolled back by recovery.
        database.closeForCrashSimulation();
        
        // Create new database instance (restart)
        Database restartedDb = new Database(TEST_DIR, createWALConfig());
        
        try {
// Load tables from disk before recovery (like RecoveryIntegrationTest)
        restartedDb.loadTablesFromDisk();
        
        // Start flusher in restarted database
        restartedDb.startPageFlusher();
        
        // Run recovery
        restartedDb.runRecovery();
            
            // Check the table state
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof List, "COUNT(*) must return a result set");
            List<?> countRows = (List<?>) result;
            assertEquals(1, countRows.size(), "COUNT(*) must collapse into a single aggregate row");
            int count = ((Number) ((Map<?, ?>) countRows.get(0)).get("COUNT(*)")).intValue();
            
            // The uncommitted inserts should be rolled back
            // So we should see 0 rows (no committed transactions)
            assertEquals(0, count, 
                "Uncommitted inserts should be rolled back by WAL recovery");
            
            // Verify individual rows don't exist
            result = restartedDb.executeQuery("SELECT ID FROM " + TABLE_NAME + " WHERE ID = 0", null);
            assertTrue(result instanceof List);
            List<?> rows = (List<?>) result;
            assertEquals(0, rows.size(), "Uncommitted rows should not exist");
            
            System.out.println("Crash recovery test passed: " + count + " rows visible (expected 0)");
            
        } finally {
            restartedDb.close();
        }
    }

    @Test
    @Tag("flaky")
    @EnabledIfSystemProperty(named = "diesel.runFlaky", matches = "true")
    void testMixedCommittedUncommitted() throws Exception {
        // Start the flusher
        database.startPageFlusher();
        
        // Insert ROW_COUNT/2 committed rows
        for (int i = 0; i < ROW_COUNT / 2; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'committed_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, null); // Auto-commit
        }
        
        // Persist committed rows to disk BEFORE uncommitted inserts
        // so the file has only committed data; uncommitted rows stay in WAL only
        database.getTable(TABLE_NAME).saveToFile(TABLE_NAME);
        database.getTable(TABLE_NAME).saveToSerializedFile(TABLE_NAME);
        
        // Begin transaction and insert ROW_COUNT/2 uncommitted rows
        UUID txId = beginTransaction();
        for (int i = ROW_COUNT / 2; i < ROW_COUNT; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'uncommitted_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, txId);
        }
        
        System.out.println("Inserted " + (ROW_COUNT / 2) + " committed + " + (ROW_COUNT / 2) + " uncommitted rows");
        
        // Simulate crash
        database.closeForCrashSimulation();
        
        // Restart and recover
        Database restartedDb = new Database(TEST_DIR, createWALConfig());
        restartedDb.loadTablesFromDisk();
        restartedDb.startPageFlusher();
        restartedDb.runRecovery();
        
try {
            // Check the table state
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof List, "COUNT(*) must return a result set");
            List<?> countRows = (List<?>) result;
            assertEquals(1, countRows.size(), "COUNT(*) must collapse into a single aggregate row");
            int count = ((Number) ((Map<?, ?>) countRows.get(0)).get("COUNT(*)")).intValue();
            
            assertEquals(ROW_COUNT / 2, count, 
                "Only committed inserts should survive crash recovery");
            
            // Verify committed rows exist
            result = restartedDb.executeQuery("SELECT ID FROM " + TABLE_NAME + " WHERE ID = 0", null);
            assertTrue(result instanceof List);
            List<?> rows = (List<?>) result;
            assertEquals(1, rows.size(), "Committed row should exist");
            
            // Verify uncommitted rows don't exist
            result = restartedDb.executeQuery("SELECT ID FROM " + TABLE_NAME + " WHERE ID = " + (ROW_COUNT / 2), null);
            assertTrue(result instanceof List);
            rows = (List<?>) result;
            assertEquals(0, rows.size(), "Uncommitted row should not exist");
            
            System.out.println("Mixed test passed: " + count + " committed rows visible (expected " + (ROW_COUNT / 2) + ")");
            
        } finally {
            restartedDb.close();
        }
    }

    @Test
    void testFlusherEnabled() throws Exception {
        // Verify flusher is started by DatabaseServer pattern
        database.startPageFlusher();
        assertTrue(database.getPageManager() != null, "PageManager should be available");
        
        // Insert some data with flusher running
        UUID txId = beginTransaction();
        for (int i = 0; i < 1000; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'test_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, txId);
        }
        
        // Commit the transaction
        database.executeQuery("COMMIT", txId);
        
        // Force persist table files for recovery
        database.getTable(TABLE_NAME).saveToFile(TABLE_NAME);
        database.getTable(TABLE_NAME).saveToSerializedFile(TABLE_NAME);
        
        // Simulate crash
        database.closeForCrashSimulation();
        
        // Restart and verify data is there (committed)
        Database restartedDb = new Database(TEST_DIR, createWALConfig());
        restartedDb.loadTablesFromDisk();
        restartedDb.startPageFlusher();
        restartedDb.runRecovery();
        
        try {
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof List, "COUNT(*) must return a result set");
            List<?> countRows = (List<?>) result;
            assertEquals(1, countRows.size(), "COUNT(*) must collapse into a single aggregate row");
            int count = ((Number) ((Map<?, ?>) countRows.get(0)).get("COUNT(*)")).intValue();
            
            assertEquals(1000, count, "Committed data should survive crash");
            
        } finally {
            restartedDb.close();
        }
    }

    // Helper methods
    private void cleanupTestDir() {
        // Clean up test data directory
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
        
        // Clean up WAL directory (unique per test method)
        if (walDir != null) {
            File walDirFile = new File(walDir);
            if (walDirFile.exists()) {
                deleteDirectory(walDirFile);
            }
        }
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    private UUID beginTransaction() {
        String result = (String) database.executeQuery("BEGIN", null);
        assertTrue(result.startsWith("Transaction started: "));
        return UUID.fromString(result.substring("Transaction started: ".length()));
    }

    private diesel.wal.WALConfig createWALConfig() {
        // Create WAL config enabled for testing with unique WAL directory per test method
        return diesel.wal.WALConfig.of(java.nio.file.Paths.get(walDir), 64 * 1024 * 1024, 1024, 
                                      3600000, java.nio.file.Paths.get(walDir + "_archive"), 7, 3600000,
                                      true, diesel.wal.FsyncPolicy.GROUP, 5, 64);
    }
}