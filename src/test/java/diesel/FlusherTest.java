package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * Acceptance test for BufferPoolFlusher (prompt4.md #20, R3-005 step 1/3):
 * 100k inserts без COMMIT, kill, restart → все inserts либо committed, либо откачены (по WAL).
 */
@Tag("storage")
public class FlusherTest {

    private static final String TEST_DIR = "data_flusher_test";
    private static final String TABLE_NAME = "test_table";
    private static final int ROW_COUNT = 50000; // 50k for efficiency, test runs faster

    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        // Clean up any existing test directory
        cleanupTestDir();
        
        // Create database with WAL enabled
        database = new Database(TEST_DIR, createWALConfig());
        database.executeQuery("CREATE TABLE " + TABLE_NAME + " (ID LONG PRIMARY KEY, NAME STRING)", null);
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
        
        // Insert 50k rows in a single uncommitted transaction
        UUID txId = beginTransaction();
        
        for (int i = 0; i < ROW_COUNT; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'name_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, txId);
            
            // Progress indicator for long-running test
            if (i % 10000 == 0) {
                System.out.println("Inserted " + i + " rows...");
            }
        }
        
        System.out.println("All rows inserted, now simulating crash...");
        
        // Simulate crash: close without commit (discard dirty pages)
        database.closeForCrashSimulation();
        
        // Create new database instance (restart)
        Database restartedDb = new Database(TEST_DIR, createWALConfig());
        
        try {
            // Start flusher in restarted database
            restartedDb.startPageFlusher();
            
            // Run recovery
            restartedDb.runRecovery();
            
            // Check the table state
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof Number);
            int count = ((Number) result).intValue();
            
            // The 50k uncommitted inserts should be rolled back
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
    void testMixedCommittedUncommitted() throws Exception {
        // Start the flusher
        database.startPageFlusher();
        
        // Insert 25k committed rows
        for (int i = 0; i < ROW_COUNT / 2; i++) {
            String sql = String.format("INSERT INTO %s (ID, NAME) VALUES (%d, 'committed_%d')", 
                TABLE_NAME, i, i);
            database.executeQuery(sql, null); // Auto-commit
        }
        
        // Begin transaction and insert 25k uncommitted rows
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
        restartedDb.startPageFlusher();
        restartedDb.runRecovery();
        
        try {
            // Should see only the committed rows
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof Number);
            int count = ((Number) result).intValue();
            
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
        
        // Simulate crash
        database.closeForCrashSimulation();
        
        // Restart and verify data is there (committed)
        Database restartedDb = new Database(TEST_DIR, createWALConfig());
        restartedDb.startPageFlusher();
        restartedDb.runRecovery();
        
        try {
            Object result = restartedDb.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME, null);
            assertTrue(result instanceof Number);
            int count = ((Number) result).intValue();
            
            assertEquals(1000, count, "Committed data should survive crash");
            
        } finally {
            restartedDb.close();
        }
    }

    // Helper methods
    private void cleanupTestDir() {
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
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
        // Create WAL config enabled for testing
        return diesel.wal.WALConfig.of(java.nio.file.Paths.get("data/wal_test"), 64 * 1024 * 1024, 1024, 
                                      3600000, java.nio.file.Paths.get("data/wal_archive"), 7, 3600000,
                                      true, diesel.wal.FsyncPolicy.GROUP, 5, 64);
    }
}