package diesel.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Performance test for GroupCommitCoordinator throughput (prompt4.md #15, R3-003 step 5/5).
 */
@Tag("perf")
@Tag("storage")
public class GroupCommitThroughputTest {

    private WALWriter writer;
    private WALConfig config;
    private ScheduledExecutorService scheduler;
    private GroupCommitCoordinator coordinator;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("group-commit-test");
        config = WALConfig.of(tempDir, 1024 * 1024, 1000);
        writer = WALWriter.open(config);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.GROUP, 10, 64, scheduler);
    }

    @Test
    void testThroughputWithSmallGroups() throws Exception {
        int numCommits = 1000;
        List<CompletableFuture<WALEntry>> futures = new ArrayList<>();
        long startTime = System.nanoTime();
        
        // Submit commits in small batches
        for (int i = 0; i < numCommits; i++) {
            CompletableFuture<WALEntry> future = coordinator.submitCommit(i, WALOpcode.COMMIT, null, null);
            futures.add(future);
            
            // Process in batches of 10
            if (i % 10 == 9) {
                for (int j = 0; j < futures.size(); j++) {
                    futures.get(j).get(1, TimeUnit.SECONDS);
                }
                futures.clear();
            }
        }
        
        // Process remaining
        for (CompletableFuture<WALEntry> future : futures) {
            future.get(1, TimeUnit.SECONDS);
        }
        
        long endTime = System.nanoTime();
        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        double throughput = numCommits / durationSec;
        
        System.out.printf("Small groups throughput: %.2f commits/sec (fsyncs: %d)%n", 
            throughput, coordinator.getFsyncCount());
        
        assertTrue(throughput > 100); // Should handle >100 commits/sec
        assertTrue(coordinator.getFsyncCount() < numCommits); // Should batch fsyncs
    }

    @Test
    void testThroughputWithLargeGroups() throws Exception {
        int numCommits = 1000;
        List<CompletableFuture<WALEntry>> futures = new ArrayList<>();
        long startTime = System.nanoTime();
        
        // Pipeline: submit everything, group flushes trigger at 64 commits
        // (or by the 10ms window for the trailing partial group)
        for (int i = 0; i < numCommits; i++) {
            futures.add(coordinator.submitCommit((long)i, WALOpcode.COMMIT, null, null));
        }
        
        // Wait for all to complete
        for (CompletableFuture<WALEntry> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        
        long endTime = System.nanoTime();
        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        double throughput = numCommits / durationSec;
        
        System.out.printf("Large groups throughput: %.2f commits/sec (fsyncs: %d)%n", 
            throughput, coordinator.getFsyncCount());
        
        assertTrue(throughput > 500); // Pipelined groups must beat sequential waits
        assertTrue(coordinator.getFsyncCount() > 0 && coordinator.getFsyncCount() < numCommits); // Should batch but not all
    }

    @Test
    void testConcurrentCommits() throws Exception {
        int numThreads = 10;
        int commitsPerThread = 100;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        long startTime = System.nanoTime();
        
        // Submit commits concurrently from multiple threads
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < commitsPerThread; i++) {
                        int txid = threadId * commitsPerThread + i;
                        CompletableFuture<WALEntry> commitFuture = coordinator.submitCommit(
                            txid, WALOpcode.COMMIT, null, null);
                        commitFuture.get(1, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            futures.add(future);
        }
        
        // Wait for all threads to complete
        for (CompletableFuture<Void> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        
        long endTime = System.nanoTime();
        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        double totalCommits = numThreads * commitsPerThread;
        double throughput = totalCommits / durationSec;
        
        System.out.printf("Concurrent throughput: %.2f commits/sec (fsyncs: %d, threads: %d)%n", 
            throughput, coordinator.getFsyncCount(), numThreads);
        
        assertTrue(throughput > 50); // Should handle concurrent commits
        assertTrue(coordinator.getFsyncCount() > 0 && coordinator.getFsyncCount() < totalCommits);
    }

    @Test
    void testMixedOperationsThroughput() throws Exception {
        int numOperations = 1000;
        List<CompletableFuture<WALEntry>> futures = new ArrayList<>();
        long startTime = System.nanoTime();
        
        // Mix of regular WAL entries and commits
        for (int i = 0; i < numOperations; i++) {
            WALOpcode op = (i % 10 == 0) ? WALOpcode.COMMIT : WALOpcode.INSERT;
            CompletableFuture<WALEntry> future = coordinator.submitCommit(i, op, null, null);
            futures.add(future);
        }
        
        // Wait for all to complete
        for (CompletableFuture<WALEntry> future : futures) {
            future.get(1, TimeUnit.SECONDS);
        }
        
        long endTime = System.nanoTime();
        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        double throughput = numOperations / durationSec;
        
        System.out.printf("Mixed operations throughput: %.2f ops/sec (fsyncs: %d)%n", 
            throughput, coordinator.getFsyncCount());
        
        assertTrue(throughput > 100); // Should handle mixed operations
        assertTrue(coordinator.getFsyncCount() > 0); // Should have some fsyncs for commits
    }

    @Test
    void testPolicyComparison() throws Exception {
        int numCommits = 1024; // 16 full groups of 64
        int batchSize = 64;

        // ALWAYS policy: one fsync per commit (baseline)
        coordinator.close();
        writer.close();
        writer = WALWriter.open(config);
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.ALWAYS, 10, batchSize, scheduler);
        double alwaysThroughput = runPipelined(numCommits, batchSize);
        long alwaysFsyncs = coordinator.getFsyncCount();

        // GROUP policy: one fsync per group
        coordinator.close();
        writer.close();
        writer = WALWriter.open(config);
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.GROUP, 10, batchSize, scheduler);
        double groupThroughput = runPipelined(numCommits, batchSize);
        long groupFsyncs = coordinator.getFsyncCount();

        System.out.printf("ALWAYS: %.0f commits/sec, %d fsyncs%n", alwaysThroughput, alwaysFsyncs);
        System.out.printf("GROUP:  %.0f commits/sec, %d fsyncs%n", groupThroughput, groupFsyncs);

        // GROUP batches fsyncs: one per full group instead of one per commit
        assertTrue(groupFsyncs <= numCommits / batchSize + 1,
            "GROUP must fsync once per group, not per commit: " + groupFsyncs);
        assertTrue(alwaysFsyncs >= numCommits, "ALWAYS must fsync every commit");
        // GROUP must be at least 20% faster under pipelined load
        assertTrue(groupThroughput > alwaysThroughput * 1.2,
            "GROUP (" + groupThroughput + "/s) must beat ALWAYS (" + alwaysThroughput + "/s) by >20%");
    }

    /** Submits {@code numCommits} commits in batches of {@code batchSize} and returns throughput. */
    private double runPipelined(int numCommits, int batchSize) throws Exception {
        long start = System.nanoTime();
        for (int batchStart = 0; batchStart < numCommits; batchStart += batchSize) {
            int end = Math.min(batchStart + batchSize, numCommits);
            List<CompletableFuture<WALEntry>> batch = new ArrayList<>(end - batchStart);
            for (int i = batchStart; i < end; i++) {
                batch.add(coordinator.submitCommit(i, WALOpcode.COMMIT, null, null));
            }
            for (CompletableFuture<WALEntry> f : batch) {
                f.get(5, TimeUnit.SECONDS);
            }
        }
        double elapsedSec = (System.nanoTime() - start) / 1_000_000_000.0;
        return numCommits / elapsedSec;
    }

    /**
     * Prompt4.md #15 acceptance: COMMIT throughput > 5000/sec and
     * p99 commit latency < 20 ms in GROUP mode.
     */
    @Test
    void testAcceptanceThroughputAndP99() throws Exception {
        int numCommits = 10_000;
        int batchSize = 64; // group max size: the 64th submit flushes synchronously
        List<Long> latenciesNs = new ArrayList<>(numCommits);

        // Warm up: JIT compile the flush path and absorb the first-fsync spike
        // so the measured p99 reflects steady state (prompt criterion is
        // steady-state p99, not cold-start).
        for (int w = 0; w < 4; w++) {
            List<CompletableFuture<WALEntry>> warm = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                warm.add(coordinator.submitCommit(-1 - w * batchSize - i, WALOpcode.COMMIT, null, null));
            }
            for (CompletableFuture<WALEntry> f : warm) {
                f.get(5, TimeUnit.SECONDS);
            }
        }

        long startAll = System.nanoTime();
        for (int start = 0; start < numCommits; start += batchSize) {
            int end = Math.min(start + batchSize, numCommits);
            int count = end - start;
            List<CompletableFuture<WALEntry>> futures = new ArrayList<>(count);
            List<Long> submitNs = new ArrayList<>(count);
            for (int i = start; i < end; i++) {
                long t0 = System.nanoTime();
                futures.add(coordinator.submitCommit(i, WALOpcode.COMMIT, null, null));
                submitNs.add(t0);
            }
            for (int j = 0; j < count; j++) {
                futures.get(j).get(5, TimeUnit.SECONDS);
                latenciesNs.add(System.nanoTime() - submitNs.get(j));
            }
        }
        long elapsedNs = System.nanoTime() - startAll;
        double throughput = numCommits / (elapsedNs / 1_000_000_000.0);

        Collections.sort(latenciesNs);
        long p99Ns = latenciesNs.get((int) Math.ceil(0.99 * latenciesNs.size()) - 1);

        System.out.printf("GROUP acceptance: %.0f commits/sec, p99=%.3f ms, fsyncs=%d (batched from %d commits)%n",
            throughput, p99Ns / 1_000_000.0, coordinator.getFsyncCount(), numCommits);

        assertTrue(throughput > 5000,
            "COMMIT throughput must exceed 5000/sec, was " + String.format("%.0f", throughput));
        assertTrue(p99Ns < 20_000_000L,
            "p99 commit latency must be < 20 ms, was " + (p99Ns / 1_000_000.0) + " ms");
        assertTrue(coordinator.getFsyncCount() < numCommits, "fsyncs must be batched");
    }

    /**
     * Prompt4.md #15 acceptance: p99 commit latency < 1 ms in NONE mode (dev).
     */
    @Test
    void testAcceptanceNonePolicyP99() throws Exception {
        coordinator.close();
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.NONE, 10, 64, scheduler);

        // Warm up: first appends create the segment file and hit JIT/IO paths
        for (int i = 0; i < 100; i++) {
            coordinator.submitCommit(-1 - i, WALOpcode.COMMIT, null, null).get(1, TimeUnit.SECONDS);
        }

        int numCommits = 1_000;
        List<Long> latenciesNs = new ArrayList<>(numCommits);
        for (int i = 0; i < numCommits; i++) {
            long t0 = System.nanoTime();
            CompletableFuture<WALEntry> future = coordinator.submitCommit(i, WALOpcode.COMMIT, null, null);
            future.get(1, TimeUnit.SECONDS);
            latenciesNs.add(System.nanoTime() - t0);
        }

        Collections.sort(latenciesNs);
        long p99Ns = latenciesNs.get((int) Math.ceil(0.99 * latenciesNs.size()) - 1);

        System.out.printf("NONE acceptance: p99=%.6f ms over %d commits, fsyncs=%d%n",
            p99Ns / 1_000_000.0, numCommits, coordinator.getFsyncCount());

        assertTrue(p99Ns < 1_000_000L,
            "p99 commit latency in NONE mode must be < 1 ms, was " + (p99Ns / 1_000_000.0) + " ms");
        assertEquals(0, coordinator.getFsyncCount(), "NONE mode must never fsync");
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            coordinator.close();
        } catch (Exception ignored) {
            // already closed by the test
        }
        try {
            writer.close();
        } catch (Exception ignored) {
            // already closed by the test
        }
        scheduler.shutdownNow();
        deleteRecursively(tempDir);
    }

    private static void deleteRecursively(Path dir) {
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // best-effort cleanup of the temp WAL directory
        }
    }
}
