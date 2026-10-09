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
 * Tests for AsyncWALWriter (prompt4.md #15, R3-003 step 5/5).
 */
@Tag("smoke")
@Tag("storage")
public class AsyncWALWriterTest {

    private Path walDir;
    private WALWriter writer;
    private ScheduledExecutorService scheduler;
    private AsyncWALWriter asyncWriter;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Files.createTempDirectory("wal-async-test");
        WALConfig config = WALConfig.of(walDir, 1024 * 1024, 1000);
        writer = WALWriter.open(config);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        asyncWriter = new AsyncWALWriter(writer, FsyncPolicy.GROUP, 10, 4, scheduler);
    }

    @AfterEach
    void tearDown() {
        try {
            asyncWriter.close();
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
    void testAppendAsync() throws Exception {
        CompletableFuture<WALEntry> future = asyncWriter.appendAsync(1, WALOpcode.INSERT, null, null);
        WALEntry entry = future.get(1, TimeUnit.SECONDS);
        
        assertNotNull(entry);
        assertEquals(1, entry.getTxid());
        assertEquals(WALOpcode.INSERT, entry.getOp());
    }

    @Test
    void testCommitAsync() throws Exception {
        CompletableFuture<WALEntry> future = asyncWriter.commitAsync(1, null, null);
        WALEntry entry = future.get(1, TimeUnit.SECONDS);
        
        assertNotNull(entry);
        assertEquals(1, entry.getTxid());
        assertEquals(WALOpcode.COMMIT, entry.getOp());
    }

    @Test
    void testFlushAsync() throws Exception {
        // Submit some operations
        asyncWriter.appendAsync(1, WALOpcode.INSERT, null, null);
        asyncWriter.appendAsync(2, WALOpcode.UPDATE, null, null);
        
        // Flush should complete all pending
        CompletableFuture<Void> flushFuture = asyncWriter.flushAsync();
        flushFuture.get(1, TimeUnit.SECONDS);
        
        assertTrue(flushFuture.isDone());
        assertFalse(flushFuture.isCompletedExceptionally());
    }

    @Test
    void testPolicyResolution() {
        assertEquals(FsyncPolicy.GROUP, asyncWriter.getPolicy());
        assertNotNull(asyncWriter.getCoordinator());
        assertEquals(10, asyncWriter.getCoordinator().getGroupWindowMs());
        assertEquals(4, asyncWriter.getCoordinator().getGroupMaxSize());
    }

    @Test
    void testAlwaysPolicy() throws Exception {
        asyncWriter.close();
        asyncWriter = new AsyncWALWriter(writer, FsyncPolicy.ALWAYS, 10, 4, scheduler);
        
        CompletableFuture<WALEntry> future = asyncWriter.commitAsync(1, null, null);
        WALEntry entry = future.get(1, TimeUnit.SECONDS);
        
        assertNotNull(entry);
        assertEquals(WALOpcode.COMMIT, entry.getOp());
        assertEquals(1, asyncWriter.getCoordinatorFsyncCount());
    }

    @Test
    void testNonePolicy() throws Exception {
        asyncWriter.close();
        asyncWriter = new AsyncWALWriter(writer, FsyncPolicy.NONE, 10, 4, scheduler);
        
        CompletableFuture<WALEntry> future = asyncWriter.commitAsync(1, null, null);
        WALEntry entry = future.get(1, TimeUnit.SECONDS);
        
        assertNotNull(entry);
        assertEquals(WALOpcode.COMMIT, entry.getOp());
        assertEquals(0, asyncWriter.getCoordinatorFsyncCount()); // No fsync with NONE
    }

    @Test
    void testMetrics() throws Exception {
        // Submit some operations
        asyncWriter.appendAsync(1, WALOpcode.INSERT, null, null);
        asyncWriter.appendAsync(2, WALOpcode.UPDATE, null, null);
        asyncWriter.commitAsync(3, null, null);
        
        Thread.sleep(50); // Wait for batching
        
        assertTrue(asyncWriter.getAsyncThroughput() > 0);
        assertTrue(asyncWriter.getPendingAsync() >= 0);
        assertTrue(asyncWriter.getCoordinatorPending() >= 0);
        assertTrue(asyncWriter.getCoordinatorFsyncCount() >= 0);
    }

    @Test
    void testClose() throws Exception {
        // Submit some operations and wait for them (pendingAsync is decremented
        // before the future completes, so after get() the counter is settled).
        CompletableFuture<WALEntry> f1 = asyncWriter.appendAsync(1, WALOpcode.INSERT, null, null);
        CompletableFuture<WALEntry> f2 = asyncWriter.appendAsync(2, WALOpcode.UPDATE, null, null);
        f1.get(1, TimeUnit.SECONDS);
        f2.get(1, TimeUnit.SECONDS);
        
        // Close should flush and clean up
        asyncWriter.close();
        
        assertEquals(0, asyncWriter.getPendingAsync());
        assertTrue(asyncWriter.getAsyncThroughput() >= 0);
    }

    @Test
    void testClosedWriter() {
        assertDoesNotThrow(() -> asyncWriter.close());
        
        CompletableFuture<WALEntry> future = asyncWriter.appendAsync(1, WALOpcode.INSERT, null, null);
        
        assertThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testJmxAttributes() throws Exception {
        // Test that JMX attributes are readable
        assertNotNull(asyncWriter.getAttribute("async.pending.operations"));
        assertNotNull(asyncWriter.getAttribute("async.throughput.ops.per.sec"));
        assertNotNull(asyncWriter.getAttribute("fsync.policy"));
        assertNotNull(asyncWriter.getAttribute("coordinator.pending.commits"));
        assertNotNull(asyncWriter.getAttribute("coordinator.fsync.count"));
    }
}
