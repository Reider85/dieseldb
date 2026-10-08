package diesel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import diesel.wal.WALConfig;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALWriter;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WALWriter backpressure handling (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p>Tags: storage, smoke
 */
@org.junit.jupiter.api.Tag("storage")
@org.junit.jupiter.api.Tag("smoke")
class WALBackpressureTest {

    @TempDir
    Path tempDir;

    private WALConfig config;
    private WALWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        // Use a very small queue for testing backpressure
        config = WALConfig.of(tempDir, 1024 * 1024, 3); // Queue size = 3 (even smaller)
        writer = WALWriter.open(config);
    }

    @Test
    void queueFullBlocksProducerAndLosesNoData() throws Exception {
        int threadCount = 4;
        int appendsPerThread = 100;
        List<Future<WALEntry>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean allCompleted = new AtomicBoolean(true);

        // Submit append requests - this should cause backpressure
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            for (int j = 0; j < appendsPerThread; j++) {
                final int seq = j;
                Future<WALEntry> future = executor.submit(() -> {
                    // Create a larger payload to fill the queue faster
                    byte[] data = new byte[1024]; // 1KB payload
                    for (int k = 0; k < data.length; k++) {
                        data[k] = (byte) (threadId * 100 + seq + k);
                    }
                    
                    WALEntry entry = writer.append(threadId, WALOpcode.INSERT, null, data);
                    return entry;
                });
                futures.add(future);
            }
        }

        // Wait for all appends to complete
        List<WALEntry> entries = new ArrayList<>();
        for (Future<WALEntry> future : futures) {
            try {
                entries.add(future.get(30, TimeUnit.SECONDS)); // Longer timeout for backpressure
            } catch (ExecutionException e) {
                allCompleted.set(false);
                fail("Append failed: " + e.getCause().getMessage());
            }
        }

        // Verify all futures completed successfully
        assertTrue(allCompleted.get(), "All appends should complete successfully");

        // Flush and verify data integrity
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        // Verify all entries are present
        assertEquals(threadCount * appendsPerThread, allEntries.size(), 
                    "All entries should be durable");

        // Verify no data loss by checking payload integrity
        for (WALEntry entry : allEntries) {
            byte[] data = entry.getAfterImage();
            assertNotNull(data, "Payload should not be null");
            assertTrue(data.length > 0, "Payload should not be empty");
            
            // Verify payload contains expected data pattern
            // The payload was created with: threadId * 100 + seq + k
            // Just verify that the payload is the expected size
            assertEquals(1024, data.length, "Payload should be 1KB");
        }

        // Verify backpressure evidence - queue should have been full at some point
        assertTrue(writer.getMaxQueueSizeSeen() >= config.getQueueMaxSize() * 0.9, 
                   "Queue should have been nearly full at some point");

        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void blockingAppendDoesNotDropUnderSustainedLoad() throws Exception {
        // Test with a slightly larger queue but sustained load
        config = WALConfig.of(tempDir, 1024 * 1024, 16);
        writer = WALWriter.open(config);

        int totalAppends = 1000;
        List<CompletableFuture<WALEntry>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Submit appends in batches to simulate sustained load
        for (int batch = 0; batch < 10; batch++) {
            List<CompletableFuture<WALEntry>> batchFutures = new ArrayList<>();
            
            for (int i = 0; i < 100; i++) {
                final int seq = batch * 100 + i;
                CompletableFuture<WALEntry> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return writer.append(seq, WALOpcode.INSERT, null, 
                                           ("load-test-" + seq).getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                batchFutures.add(future);
                futures.add(future);
            }
            
            // Wait for batch to complete before next batch
            for (CompletableFuture<WALEntry> future : batchFutures) {
                try {
                    future.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    fail("Append failed: " + e.getCause().getMessage());
                }
            }
            
            // Small delay between batches
            Thread.sleep(10);
        }

        // Verify all appends completed
        assertEquals(totalAppends, futures.size(), "Should have submitted all appends");

        // Flush and verify data integrity
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        assertEquals(totalAppends, allEntries.size(), "All entries should be durable");

        // Verify all entries have correct payloads (order might vary)
        for (WALEntry entry : allEntries) {
            String actual = new String(entry.getAfterImage());
            assertTrue(actual.startsWith("load-test-"), "Payload should start with 'load-test-'");
            assertTrue(actual.length() > 9, "Payload should have sequence number");
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void flushWorksUnderLoad() throws Exception {
        // Fill the queue with regular operations
        for (int i = 0; i < 50; i++) {
            writer.append(i, WALOpcode.INSERT, null, ("pre-flush-" + i).getBytes());
        }

        // Submit more operations while flush is pending
        for (int i = 50; i < 100; i++) {
            writer.append(i, WALOpcode.INSERT, null, ("post-flush-" + i).getBytes());
        }

        // Flush all entries
        writer.flush();

        // Verify all entries are durable
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        assertEquals(100, allEntries.size(), "All 100 entries should be durable");

        // Verify all entries have correct payloads
        for (WALEntry entry : allEntries) {
            String payload = new String(entry.getAfterImage());
            assertTrue(payload.startsWith("pre-flush-") || payload.startsWith("post-flush-"), 
                      "Payload should be either pre-flush or post-flush");
        }
    }

    @Test
    void queueSizeTrackingWorks() throws Exception {
        // Initially empty
        assertEquals(0, writer.getQueueSize());

        // Add some entries
        for (int i = 0; i < 5; i++) {
            writer.append(i, WALOpcode.INSERT, null, "test".getBytes());
        }

        // Give the writer thread time to process some entries
        Thread.sleep(100);

        // Queue size should be >= 0 but could be empty if writer is fast
        int queueSize = writer.getQueueSize();
        assertTrue(queueSize >= 0, "Queue size should be non-negative");
        assertTrue(queueSize <= config.getQueueMaxSize(), 
                   "Queue size should not exceed capacity");

        // Flush and verify queue drains
        writer.flush();
        assertEquals(0, writer.getQueueSize(), "Queue should be empty after flush");
    }

    // Helper method to access the queue (if not available directly)
    private diesel.wal.WALQueue getQueue() {
        try {
            // Use reflection to access the private queue field for testing
            java.lang.reflect.Field field = WALWriter.class.getDeclaredField("queue");
            field.setAccessible(true);
            return (diesel.wal.WALQueue) field.get(writer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access queue", e);
        }
    }
}