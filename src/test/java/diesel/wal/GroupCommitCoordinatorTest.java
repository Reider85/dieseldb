package diesel.wal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Tests for GroupCommitCoordinator (prompt4.md #15, R3-003 step 5/5).
 */
@Tag("smoke")
@Tag("storage")
public class GroupCommitCoordinatorTest {

    private Path walDir;
    private WALWriter writer;
    private ScheduledExecutorService scheduler;
    private GroupCommitCoordinator coordinator;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Files.createTempDirectory("group-commit-test");
        WALConfig config = WALConfig.of(walDir, 1024 * 1024, 1000);
        writer = WALWriter.open(config);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.GROUP, 10, 4, scheduler);
    }

    @AfterEach
    void tearDown() {
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
        deleteRecursively(walDir);
    }

    private static void deleteRecursively(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // best-effort cleanup of the temp WAL directory
        }
    }

    @Test
    void testSingleCommit() throws Exception {
        CompletableFuture<WALEntry> future = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        WALEntry entry = future.get(1, TimeUnit.SECONDS);
        
        assertNotNull(entry);
        assertEquals(1, entry.getTxid());
        assertEquals(WALOpcode.COMMIT, entry.getOp());
        assertEquals(1, coordinator.getFsyncCount());
    }

    @Test
    void testGroupCommitBySize() throws Exception {
        // Submit 4 commits (max group size)
        CompletableFuture<WALEntry> f1 = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        CompletableFuture<WALEntry> f2 = coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        CompletableFuture<WALEntry> f3 = coordinator.submitCommit(3, WALOpcode.COMMIT, null, null);
        CompletableFuture<WALEntry> f4 = coordinator.submitCommit(4, WALOpcode.COMMIT, null, null);
        
        // Should flush immediately due to size
        WALEntry e1 = f1.get(1, TimeUnit.SECONDS);
        WALEntry e2 = f2.get(1, TimeUnit.SECONDS);
        WALEntry e3 = f3.get(1, TimeUnit.SECONDS);
        WALEntry e4 = f4.get(1, TimeUnit.SECONDS);
        
        assertEquals(1, coordinator.getFsyncCount());
        assertEquals(0, coordinator.getPendingCommits());
    }

    @Test
    void testGroupCommitByTimeout() throws Exception {
        // Submit 2 commits (less than max size)
        CompletableFuture<WALEntry> f1 = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        CompletableFuture<WALEntry> f2 = coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        // Wait for timeout flush
        Thread.sleep(50); // Wait longer than 10ms window
        
        WALEntry e1 = f1.get(1, TimeUnit.SECONDS);
        WALEntry e2 = f2.get(1, TimeUnit.SECONDS);
        
        assertEquals(1, coordinator.getFsyncCount());
        assertEquals(0, coordinator.getPendingCommits());
    }

    @Test
    void testMixedOperations() throws Exception {
        // Submit regular WAL entry (batched with the commit below)
        CompletableFuture<WALEntry> fRegular = coordinator.submitCommit(1, WALOpcode.INSERT, null, null);
        // Submit commit
        CompletableFuture<WALEntry> fCommit = coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        WALEntry regular = fRegular.get(1, TimeUnit.SECONDS);
        WALEntry commit = fCommit.get(1, TimeUnit.SECONDS);
        
        assertEquals(1, coordinator.getFsyncCount()); // one fsync covers the whole group
        assertEquals(WALOpcode.INSERT, regular.getOp());
        assertEquals(WALOpcode.COMMIT, commit.getOp());
    }

    @Test
    void testAlwaysPolicy() throws Exception {
        coordinator.close();
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.ALWAYS, 10, 4, scheduler);
        
        CompletableFuture<WALEntry> f1 = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        CompletableFuture<WALEntry> f2 = coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        WALEntry e1 = f1.get(1, TimeUnit.SECONDS);
        WALEntry e2 = f2.get(1, TimeUnit.SECONDS);
        
        assertEquals(2, coordinator.getFsyncCount()); // Each commit gets its own fsync
    }

    @Test
    void testNonePolicy() throws Exception {
        coordinator.close();
        coordinator = new GroupCommitCoordinator(writer, FsyncPolicy.NONE, 10, 4, scheduler);
        
        CompletableFuture<WALEntry> f1 = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        WALEntry e1 = f1.get(1, TimeUnit.SECONDS);
        
        assertEquals(0, coordinator.getFsyncCount()); // No fsync
    }

    @Test
    void testErrorPropagation() throws Exception {
        // Closing the writer makes every subsequent append fail: the group
        // flush must complete all futures exceptionally.
        writer.close();
        
        CompletableFuture<WALEntry> future = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void testFlush() throws Exception {
        // Submit some commits without waiting
        coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        assertEquals(2, coordinator.getPendingCommits());
        
        // Flush should complete all futures
        coordinator.flush();
        
        assertEquals(0, coordinator.getPendingCommits());
        assertEquals(1, coordinator.getFsyncCount());
    }

    @Test
    void testMetrics() throws Exception {
        // Submit some commits
        coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        assertEquals(2, coordinator.getPendingCommits());
        assertEquals(2, coordinator.getTotalCommits());
        
        // Wait for flush
        Thread.sleep(50);
        
        assertEquals(0, coordinator.getPendingCommits());
        assertTrue(coordinator.getGroupThroughput() > 0);
    }

    @Test
    void testClose() throws Exception {
        // Submit some commits
        coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
        
        // Close should flush remaining
        coordinator.close();
        
        assertEquals(0, coordinator.getPendingCommits());
        assertTrue(coordinator.getFsyncCount() > 0);
    }

    @Test
    void testClosedCoordinator() throws Exception {
        coordinator.close();
        
        CompletableFuture<WALEntry> future = coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
        
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(future.isCompletedExceptionally());
    }
}
