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
 * Performance test for BufferPoolFlusher (prompt4.md #20, R3-005 step 1/3):
 * Writer throughput should not drop more than 10% with flusher enabled.
 */
@Tag("perf")
public class FlusherThroughputTest {

    private static final String TEST_DIR = "data_flusher_throughput_test";
    private static final String TABLE_NAME = "throughput_table";
    private static final int INSERT_COUNT = 10000;
    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASUREMENT_ITERATIONS = 5;

    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        // Clean up any existing test directory
        cleanupTestDir();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (database != null) {
            database.close();
        }
        cleanupTestDir();
    }

    @Test
    void testThroughputWithFlusher() throws Exception {
        // Test with flusher enabled
        double withFlusher = measureThroughput(true);
        
        // Test with flusher disabled
        double withoutFlusher = measureThroughput(false);
        
        // Calculate performance impact
        double ratio = withFlusher / withoutFlusher;
        double percentageDrop = (1.0 - ratio) * 100.0;
        
        System.out.printf("Throughput with flusher: %.2f ops/ms%n", withFlusher);
        System.out.printf("Throughput without flusher: %.2f ops/ms%n", withoutFlusher);
        System.out.printf("Performance drop: %.2f%%%n", percentageDrop);
        
        // Assert that throughput doesn't drop more than 10%
        assertTrue(ratio >= 0.90, 
            String.format("Throughput dropped %.2f%% with flusher enabled (max allowed 10%%)", 
            percentageDrop));
    }

    private double measureThroughput(boolean withFlusher) throws Exception {
        // Create fresh database for each measurement
        database = new Database(TEST_DIR, createWALConfig());
        database.executeQuery("CREATE TABLE " + TABLE_NAME + " (ID LONG PRIMARY KEY, NAME STRING, VALUE LONG)", null);
        
        try {
            if (withFlusher) {
                database.startPageFlusher();
                System.out.println("Testing WITH flusher enabled...");
            } else {
                System.out.println("Testing WITHOUT flusher...");
            }
            
            // Warmup iterations
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                performInserts(false);
            }
            
            // Measurement iterations
            List<Long> measurements = new ArrayList<>();
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                long duration = performInserts(true);
                measurements.add(duration);
                System.out.printf("Iteration %d: %d ms%n", i + 1, duration);
            }
            
            // Calculate average throughput
            double avgDurationMs = measurements.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);
            
            return INSERT_COUNT / avgDurationMs; // ops/ms
            
        } finally {
            database.close();
        }
    }

    private long performInserts(boolean measure) throws Exception {
        String txId = (String) database.executeQuery("BEGIN", null);
        UUID transactionId = UUID.fromString(txId.substring("Transaction started: ".length()));
        
        long startTime = measure ? System.nanoTime() : 0;
        
        try {
            for (int i = 0; i < INSERT_COUNT; i++) {
                String sql = String.format("INSERT INTO %s (ID, NAME, VALUE) VALUES (%d, 'name_%d', %d)", 
                    TABLE_NAME, i, i, i * 2);
                database.executeQuery(sql, transactionId);
            }
            
            database.executeQuery("COMMIT", transactionId);
            
        } catch (Exception e) {
            database.executeQuery("ROLLBACK", transactionId);
            throw e;
        }
        
        if (measure) {
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            return durationMs;
        }
        
        return 0;
    }

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

    private diesel.wal.WALConfig createWALConfig() {
        // Create WAL config enabled for testing
        return diesel.wal.WALConfig.of(java.nio.file.Paths.get("data/wal_throughput_test"), 64 * 1024 * 1024, 1024, 
                                      3600000, java.nio.file.Paths.get("data/wal_archive"), 7, 3600000,
                                      true, diesel.wal.FsyncPolicy.GROUP, 5, 64);
    }
}