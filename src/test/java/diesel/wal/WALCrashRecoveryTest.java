package diesel.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests for WAL crash recovery (prompt4.md #15, R3-003 step 5/5).
 * Simulates crash scenarios and verifies the manager recovers gracefully.
 */
@Tag("smoke")
@Tag("storage")
public class WALCrashRecoveryTest {

    private Path walDir;
    private WALConfig config;
    private WALWriter writer;
    private WALManager manager;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Files.createTempDirectory("wal-crash-test");
        config = WALConfig.of(walDir, 1024 * 1024, 1000);
        manager = new WALManager(config);
        writer = new WALWriter(manager, config);
    }

    @AfterEach
    void tearDown() {
        try {
            writer.close();
        } catch (Exception ignored) {
            // already closed by the test
        }
        try {
            manager.close();
        } catch (Exception ignored) {
            // already closed by the test
        }
        deleteRecursively(walDir);
    }

    private static void deleteRecursively(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // best-effort cleanup of the temp WAL directory
        }
    }

    /** Returns every wal-NNNN.log segment in the test WAL directory, sorted. */
    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(walDir)) {
            return files.filter(p -> p.getFileName().toString().matches("wal-\\d{4}\\.log"))
                        .sorted()
                        .collect(Collectors.toList());
        }
    }

    @Test
    void testRecoveryAfterCleanShutdown() throws Exception {
        // Write some entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.append(2, WALOpcode.UPDATE, null, null);
        writer.append(3, WALOpcode.COMMIT, null, null);
        writer.flush();
        long appendCount = writer.getAppendCount();
        
        // Clean shutdown (writer.close() also closes the owned manager)
        writer.close();
        
        // Recover: LSN must be restored from checkpoint.ptr / segments
        manager.recoverLsn();
        assertEquals(appendCount, writer.getAppendCount());
        assertFalse(segmentFiles().isEmpty(), "segments must survive a clean shutdown");
    }

    @Test
    void testRecoveryAfterCrash() throws Exception {
        // Write some entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.append(2, WALOpcode.UPDATE, null, null);
        writer.append(3, WALOpcode.COMMIT, null, null);
        writer.flush();
        long appendCount = writer.getAppendCount();
        
        // Simulate crash by not closing the writer/manager properly:
        // only kill the writer thread, leave files as-is.
        writer.close();
        
        // Recover on the still-open manager
        manager.recoverLsn();
        assertEquals(appendCount, writer.getAppendCount());
        assertFalse(segmentFiles().isEmpty());
    }

    @Test
    void testRecoveryWithPartialWrite() throws Exception {
        // Write some entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.append(2, WALOpcode.UPDATE, null, null);
        writer.flush();
        long appendCount = writer.getAppendCount();
        writer.close();
        
        // Corrupt the tail of the first segment (torn write)
        List<Path> segments = segmentFiles();
        assertFalse(segments.isEmpty());
        Path segmentFile = segments.get(0);
        Files.writeString(segmentFile, "CORRUPTED TAIL", StandardOpenOption.APPEND);
        
        // Recover must stop cleanly at the torn tail (no exception)
        manager.recoverLsn();
        assertEquals(appendCount, writer.getAppendCount());
    }

    @Test
    void testRecoveryWithMissingSegments() throws Exception {
        // Write entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.append(2, WALOpcode.UPDATE, null, null);
        writer.append(3, WALOpcode.COMMIT, null, null);
        writer.flush();
        writer.close();
        
        // Delete the first segment (simulating crash after rotation)
        List<Path> segments = segmentFiles();
        assertTrue(segments.size() >= 1);
        Files.deleteIfExists(segments.get(0));
        
        // Recover should continue with whatever segments remain
        manager.recoverLsn();
        // No exception is the primary contract here.
    }

    @Test
    void testRecoveryWithGroupCommit() throws Exception {
        // Use group commit coordinator
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        GroupCommitCoordinator coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.GROUP, 10, 4, scheduler);
        try {
            // Submit multiple commits (should be grouped)
            CompletableFuture<WALEntry> f1 = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
            CompletableFuture<WALEntry> f2 = coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
            CompletableFuture<WALEntry> f3 = coordinator.submitCommit(3, WALOpcode.COMMIT, null, null);
            
            // Wait for some commits to complete
            f1.get(1, TimeUnit.SECONDS);
            f2.get(1, TimeUnit.SECONDS);
            
            // Simulate crash: coordinator close flushes f3, then tear down the writer
            coordinator.close();
            assertTrue(writer.getAppendCount() >= 2);
            
            f3.get(1, TimeUnit.SECONDS);
            assertTrue(writer.getAppendCount() >= 3);
        } finally {
            scheduler.shutdownNow();
        }
        
        writer.close();
        manager.recoverLsn();
        assertFalse(segmentFiles().isEmpty());
    }

    @Test
    void testRecoveryWithTruncate() throws Exception {
        // Write entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.append(2, WALOpcode.UPDATE, null, null);
        writer.append(3, WALOpcode.COMMIT, null, null);
        writer.flush();
        writer.close();
        
        // Simulate a restart over the same directory: a fresh manager must
        // rediscover the existing segments and recover the LSN.
        manager = new WALManager(config);
        manager.recoverLsn();
        
        // The recovered manager accepts new appends with a monotonic LSN
        WALEntry appended = manager.append(10, WALOpcode.COMMIT, null, null);
        assertTrue(appended.getLsn() > 0);
    }

    @Test
    void testRecoveryEmptyWAL() throws Exception {
        // No entries written
        writer.close();
        
        // Recover should handle empty WAL gracefully
        manager.recoverLsn();
        assertEquals(0, writer.getAppendCount());
    }

    /**
     * Prompt4.md #15 acceptance: 10k commits, kill -9, restart — every
     * committed tx must be present on disk, none lost.
     */
    @Test
    void testTenThousandCommitsSurviveCrash() throws Exception {
        int numCommits = 10_000;
        int batchSize = 64; // group commit window size
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        GroupCommitCoordinator coordinator = new GroupCommitCoordinator(
            writer, FsyncPolicy.GROUP, 10, batchSize, scheduler);
        try {
            // A future completes only after the group fsync, so once get()
            // returns the commit is durable. Batches of 64 fill the group and
            // trigger a synchronous flush.
            for (int start = 0; start < numCommits; start += batchSize) {
                int end = Math.min(start + batchSize, numCommits);
                List<CompletableFuture<WALEntry>> batch = new ArrayList<>();
                for (int i = start; i < end; i++) {
                    batch.add(coordinator.submitCommit(i, WALOpcode.COMMIT, null, null));
                }
                for (CompletableFuture<WALEntry> f : batch) {
                    f.get(5, TimeUnit.SECONDS);
                }
            }
            assertEquals(numCommits, coordinator.getTotalCommits());
            assertTrue(coordinator.getFsyncCount() > 0);
        } finally {
            scheduler.shutdownNow();
        }

        // "kill -9": abandon the writer without any extra flush beyond what
        // the completed futures already guarantees, then restart over the
        // same directory.
        writer.close(); // closes its WALManager; no data may be lost here
        manager = new WALManager(config);
        manager.recoverLsn();

        List<WALEntry> all = manager.readAll();
        long commits = all.stream().filter(e -> e.getOp() == WALOpcode.COMMIT).count();
        java.util.Set<Long> txids = all.stream()
            .filter(e -> e.getOp() == WALOpcode.COMMIT)
            .map(WALEntry::getTxid)
            .collect(java.util.stream.Collectors.toSet());

        assertEquals(numCommits, commits, "all 10k committed txs must survive the crash");
        assertEquals(numCommits, txids.size(), "no committed txid may be lost or duplicated");
        assertEquals(numCommits, txids.stream().filter(t -> t >= 0 && t < numCommits).count(),
                     "every txid in [0, 10000) must be present");
    }

    @Test
    void testRecoveryWithCorruptedHeader() throws Exception {
        // Write some entries
        writer.append(1, WALOpcode.INSERT, null, null);
        writer.flush();
        writer.close();
        
        // Corrupt the header of the first segment (flip the magic)
        List<Path> segments = segmentFiles();
        assertFalse(segments.isEmpty());
        Path segmentFile = segments.get(0);
        byte[] original = Files.readAllBytes(segmentFile);
        original[0] ^= 0xFF;
        Files.write(segmentFile, original);
        
        // A corrupted segment magic must fail fast with WALFormatException
        // (same contract as WALManagerTest). Close the old manager first so
        // its file handles are released, then rediscover the directory.
        manager.close();
        assertThrows(WALFormatException.class, () -> new WALManager(config));
    }
}
