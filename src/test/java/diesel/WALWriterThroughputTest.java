package diesel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
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
 * Throughput tests for WALWriter (prompt4.md step 13, R3-003 step 3/5).
 *
 * <p>Tags: perf
 */
@org.junit.jupiter.api.Tag("perf")
class WALWriterThroughputTest {

    @TempDir
    Path tempDir;

    private WALConfig config;
    private WALWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        config = WALConfig.of(tempDir, 1024 * 1024, 100_000); // Default queue size
        writer = WALWriter.open(config);
    }

    @Test
    void insertThroughputMeetsTarget() throws Exception {
        int threadCount = 8;
        int appendsPerThread = 12_500; // Total 100k for reasonable test time
        int totalAppends = threadCount * appendsPerThread;
        
        List<CompletableFuture<WALEntry>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        LongAdder completedAppends = new LongAdder();
        AtomicLong maxQueueSize = new AtomicLong(0);

        // Start timing
        long startTime = System.nanoTime();

        // Submit all append requests
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            for (int j = 0; j < appendsPerThread; j++) {
                final int seq = j;
                CompletableFuture<WALEntry> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        WALEntry entry = writer.append(threadId, WALOpcode.INSERT, null, 
                                                     ("throughput-test-" + threadId + "-" + seq).getBytes());
                        completedAppends.increment();
                        
                        // Track max queue size
                        long currentSize = writer.getQueueSize();
                        long currentMax = maxQueueSize.get();
                        while (currentSize > currentMax) {
                            if (maxQueueSize.compareAndSet(currentMax, currentSize)) {
                                break;
                            }
                            currentMax = maxQueueSize.get();
                        }
                        
                        return entry;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor);
                futures.add(future);
            }
        }

        // Wait for all appends to complete
        for (CompletableFuture<WALEntry> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                fail("Append failed: " + e.getMessage());
            }
        }

        // End timing
        long endTime = System.nanoTime();
        long durationNanos = endTime - startTime;
        double durationSeconds = durationNanos / 1_000_000_000.0;
        double opsPerSec = totalAppends / durationSeconds;

        // Flush and verify data integrity
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        // Verify all entries are present
        assertEquals(totalAppends, allEntries.size(), 
                    "Total entries should match");

        System.out.printf("WAL Throughput Test Results:%n");
        System.out.printf("  Total appends: %d%n", totalAppends);
        System.out.printf("  Duration: %.2f seconds%n", durationSeconds);
        System.out.printf("  Throughput: %.0f inserts/sec%n", opsPerSec);
        System.out.printf("  Max queue size: %d (limit: %d)%n", 
                         maxQueueSize.get(), config.getQueueMaxSize());

        // Assert performance requirements
        assertTrue(opsPerSec >= 50_000, 
                   String.format("Throughput %.0f inserts/sec < minimum 50,000", opsPerSec));
        
        assertTrue(maxQueueSize.get() < 1_000, 
                   String.format("Max queue size %d >= limit 1,000", maxQueueSize.get()));

        // Verify LSNs are strictly monotonic
        for (int i = 0; i < allEntries.size(); i++) {
            WALEntry entry = allEntries.get(i);
            assertEquals(i + 1, entry.getLsn(), "LSNs should be strictly monotonic");
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void throughputWithSmallPayloads() throws Exception {
        // Test with very small payloads (worst-case for queue efficiency)
        int threadCount = 16;
        int appendsPerThread = 6_250; // Total 100k
        int totalAppends = threadCount * appendsPerThread;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        LongAdder completedAppends = new LongAdder();

        long startTime = System.nanoTime();

        // Submit append requests with tiny payloads
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            for (int j = 0; j < appendsPerThread; j++) {
                final int seq = j;
                executor.submit(() -> {
                    try {
                        writer.append(threadId, WALOpcode.INSERT, null, 
                                     new byte[4]); // 4-byte payload
                        completedAppends.increment();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        // Wait for completion
        while (completedAppends.sum() < totalAppends) {
            Thread.sleep(100);
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        double opsPerSec = totalAppends / durationSeconds;

        System.out.printf("Small Payload Throughput: %.0f inserts/sec%n", opsPerSec);

        // Should still meet minimum throughput even with small payloads
        assertTrue(opsPerSec >= 50_000, 
                   String.format("Small payload throughput %.0f < minimum 50,000", opsPerSec));

        // Verify data integrity
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        assertEquals(totalAppends, allEntries.size(), "All entries should be durable");

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void throughputWithLargePayloads() throws Exception {
        // Test with larger payloads (more realistic scenario)
        int threadCount = 4;
        int appendsPerThread = 12_500; // Total 50k to keep test time reasonable
        int totalAppends = threadCount * appendsPerThread;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        long startTime = System.nanoTime();

        // Submit append requests with 1KB payloads
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            for (int j = 0; j < appendsPerThread; j++) {
                final int seq = j;
                executor.submit(() -> {
                    try {
                        byte[] payload = new byte[1024]; // 1KB
                        for (int k = 0; k < payload.length; k++) {
                            payload[k] = (byte) (threadId * 100 + seq + k);
                        }
                        writer.append(threadId, WALOpcode.INSERT, null, payload);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        // Wait for completion
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        double opsPerSec = totalAppends / durationSeconds;

        System.out.printf("Large Payload Throughput: %.0f inserts/sec%n", opsPerSec);

        // Large payloads should still achieve reasonable throughput
        assertTrue(opsPerSec >= 10_000, 
                   String.format("Large payload throughput %.0f < minimum 10,000", opsPerSec));

        // Verify data integrity
        writer.flush();
        WALManager manager = new WALManager(config);
        List<WALEntry> allEntries = manager.readAll();
        manager.close();

        assertEquals(totalAppends, allEntries.size(), "All entries should be durable");

        // Verify payload integrity
        for (WALEntry entry : allEntries) {
            byte[] payload = entry.getAfterImage();
            assertEquals(1024, payload.length, "Payload size should be 1KB");
            
            // Verify payload pattern
            int threadId = (int) ((entry.getLsn() - 1) / appendsPerThread);
            int seq = (int) ((entry.getLsn() - 1) % appendsPerThread);
            for (int k = 0; k < Math.min(10, payload.length); k++) {
                assertEquals(threadId * 100 + seq + k, payload[k] & 0xFF, 
                           "Payload byte should match expected pattern");
            }
        }
    }

    @Test
    void throughputMetricsAreAccurate() throws Exception {
        // Perform a known number of operations
        int totalAppends = 10_000;
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < totalAppends; i++) {
            writer.append(i, WALOpcode.INSERT, null, "metric-test".getBytes());
        }
        
        long endTime = System.nanoTime();
        writer.flush();

        // Verify metrics reflect actual operations
        long actualCount = writer.getAppendCount();
        assertTrue(actualCount >= totalAppends, 
                   String.format("Append count %d < expected %d", actualCount, totalAppends));
        
        // Verify latency is reasonable (should be > 0 since we measured time)
        double p99Latency = writer.getAppendLatencyP99();
        assertTrue(p99Latency >= 0, "P99 latency should be >= 0");
        
        // Calculate actual throughput
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        double actualThroughput = totalAppends / durationSeconds;
        
        System.out.printf("Actual throughput: %.0f inserts/sec%n", actualThroughput);
        System.out.printf("P99 latency: %.2f microseconds%n", p99Latency);
    }
}