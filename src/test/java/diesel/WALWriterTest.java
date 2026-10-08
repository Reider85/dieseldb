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
import java.util.concurrent.atomic.AtomicLong;
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
 * Tests for WALWriter (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p>Tags: storage, smoke
 */
@org.junit.jupiter.api.Tag("storage")
@org.junit.jupiter.api.Tag("smoke")
class WALWriterTest {

    @TempDir
    Path tempDir;

    private WALConfig config;
    private WALWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        config = WALConfig.of(tempDir, 1024 * 1024, 10_000); // Small queue for testing
        writer = WALWriter.open(config);
    }

    @Test
    void concurrentAppendsAreAllDurableAndLsnsStrictlyMonotonic() throws Exception {
        int threadCount = 100;
        int appendsPerThread = 1000;
        List<Future<WALEntry>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Submit all append requests
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            for (int j = 0; j < appendsPerThread; j++) {
                final int seq = j;
                Future<WALEntry> future = executor.submit(() -> {
                    byte[] data = ("thread-" + threadId + "-seq-" + seq).getBytes();
                    return writer.append(threadId, WALOpcode.INSERT, null, data);
                });
                futures.add(future);
            }
        }

        // Wait for all appends to complete
        List<WALEntry> entries = new ArrayList<>();
        for (Future<WALEntry> future : futures) {
            try {
                entries.add(future.get());
            } catch (ExecutionException e) {
                fail("Append failed: " + e.getCause().getMessage());
            }
        }

        // Flush and verify
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        // Verify all entries are present
        assertEquals(threadCount * appendsPerThread, allEntries.size(), 
                    "Total number of entries should match");

        // Verify LSNs are strictly monotonic
        long expectedLsn = 1;
        for (WALEntry entry : allEntries) {
            assertEquals(expectedLsn, entry.getLsn(), "LSNs should be strictly monotonic");
            expectedLsn++;
        }

        // Verify payload integrity (spot check a few entries)
        // Since entries might be reordered by the writer, we need to parse the actual payload
        for (int i = 0; i < Math.min(10, allEntries.size()); i++) {
            WALEntry entry = allEntries.get(i);
            String actual = new String(entry.getAfterImage());
            // Payload format: "thread-{threadId}-seq-{seq}"
            assertTrue(actual.startsWith("thread-"), "Payload should start with 'thread-'");
            assertTrue(actual.contains("-seq-"), "Payload should contain '-seq-'");
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void flushMakesEverythingDurable() throws IOException {
        // Append some entries
        for (int i = 0; i < 100; i++) {
            writer.append(i, WALOpcode.INSERT, null, ("data-" + i).getBytes());
        }

        // Flush
        writer.flush();

        // Verify all entries are durable
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        assertEquals(100, allEntries.size(), "All entries should be durable after flush");
    }

    @Test
    void metricsAreExposed() throws IOException {
        // Append some entries
        for (int i = 0; i < 50; i++) {
            writer.append(i, WALOpcode.INSERT, null, ("data-" + i).getBytes());
        }

        // Flush to ensure all entries are processed
        writer.flush();

        // Verify metrics
        assertTrue(writer.getAppendCount() >= 50, "Append count should be >= 50");
        assertEquals(0, writer.getQueueSize(), "Queue should be empty after flush");
        assertTrue(writer.getAppendLatencyP99() >= 0, "Latency p99 should be >= 0");
    }

    @Test
    void closeIsIdempotentAndRejectsAfterClose() throws IOException {
        // Close once
        writer.close();

        // Verify close is idempotent
        writer.close();

        // Verify append after close fails
        assertThrows(IllegalStateException.class, () -> {
            writer.append(1, WALOpcode.INSERT, null, "test".getBytes());
        }, "Append should fail after close");
    }

    @Test
    void closePreventsFurtherAppends() throws IOException {
        // Close the writer
        writer.close();

        // Try to append - should fail
        assertThrows(IllegalStateException.class, () -> {
            writer.append(1, WALOpcode.INSERT, null, "test".getBytes());
        }, "Append should fail after close");
    }

    @Test
    void configDefaultsAndSyspropOverride() {
        // Test default
        WALConfig defaultConfig = WALConfig.of(tempDir, 1024 * 1024);
        assertEquals(100_000, defaultConfig.getQueueMaxSize(), 
                    "Default queue max size should be 100,000");

        // Test system property override
        try {
            System.setProperty("wal.queue.max.size", "50000");
            // Use fromConfig() to test system property resolution
            WALConfig overrideConfig = WALConfig.fromConfig();
            assertEquals(50_000, overrideConfig.getQueueMaxSize(), 
                        "Queue max size should be overridden by system property");
        } finally {
            System.clearProperty("wal.queue.max.size");
        }
    }

    @Test
    void appendAsyncCompletesSuccessfully() throws Exception {
        CompletableFuture<WALEntry> future = writer.appendAsync(
            1, WALOpcode.INSERT, null, "async-test".getBytes());

        WALEntry entry = future.get(5, TimeUnit.SECONDS);
        assertNotNull(entry, "Future should complete with a valid entry");
        assertEquals(1, entry.getTxid(), "Transaction ID should match");
        assertEquals(WALOpcode.INSERT, entry.getOp(), "Opcode should match");
        assertEquals("async-test", new String(entry.getAfterImage()), "Payload should match");
    }

    @Test
    void appendAsyncFailsWhenClosed() throws IOException {
        writer.close();

        CompletableFuture<WALEntry> future = writer.appendAsync(
            1, WALOpcode.INSERT, null, "test".getBytes());

        assertThrows(ExecutionException.class, future::get, 
                    "Future should fail with exception after close");
    }

    // Helper method to get the manager (if not available directly)
    private WALManager getManager() throws IOException {
        return new WALManager(config);
    }
}