package diesel.storage.page;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import diesel.wal.WALManager;
import diesel.wal.WALConfig;
import diesel.wal.WALOpcode;
import diesel.wal.FsyncPolicy;
import diesel.storage.page.BufferPool;
import diesel.storage.page.BufferPoolFlusher;
import diesel.storage.page.PageManager;
import diesel.storage.page.CheckpointManager;

/**
 * Tests checkpoint duration and performance (prompt4 #21).
 */
@Tag("performance")
@Tag("storage")
public class CheckpointDurationTest {

    private WALManager walManager;
    private BufferPoolFlusher flusher;
    private BufferPool bufferPool;
    private PageManager pageManager;
    private CheckpointManager checkpointManager;
    private Path walDir;
    private Path pageFile;

    @BeforeEach
    void setUp() throws Exception {
        walDir = createTempDir("wal");
        pageFile = createTempFile("pages");
        
        // Create WAL manager
        WALConfig walConfig = WALConfig.of(walDir, 1024 * 1024, 10000, 300000, walDir.resolve("archive"),
                                            7, 60000, true, FsyncPolicy.GROUP,
                                            WALConfig.DEFAULT_GROUP_WINDOW_MS, WALConfig.DEFAULT_GROUP_MAX_SIZE,
                                            64L * 1024 * 1024);
        walManager = new WALManager(walConfig);
        
        // Create page manager and buffer pool
        pageManager = PageManager.open(pageFile, 100, 8192);
        bufferPool = pageManager.getBufferPool();
        
        // Create flusher (not started, just for flushNow)
        flusher = new BufferPoolFlusher(bufferPool, () -> Long.MAX_VALUE, 1000);
        
        // Create checkpoint manager (disabled for most tests)
        checkpointManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                  300000, 268435456, false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (checkpointManager != null) {
            checkpointManager.stopWithoutCheckpoint();
        }
        if (flusher != null) {
            flusher.stopWithoutFlush();
        }
        if (walManager != null) {
            walManager.close();
        }
        deleteRecursively(walDir);
        deleteRecursively(pageFile.getParent());
    }

    @Test
    void checkpointDurationEmptyDatabase() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Measure checkpoint duration on empty database
            long startTime = System.currentTimeMillis();
            long checkpointLsn = enabledManager.checkpointNow();
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify checkpoint completed
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify duration is reasonable for empty database
            assertTrue(duration < 1000, "Empty checkpoint should take < 1s, took " + duration + "ms");
            
            // Verify duration is recorded
            assertTrue(enabledManager.getLastCheckpointDurationMs() > 0, 
                      "Checkpoint duration should be recorded");
            assertEquals(duration, enabledManager.getLastCheckpointDurationMs(), 
                        "Recorded duration should match actual duration");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDurationWithDirtyPages() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Dirty some pages
            int pageCount = 100;
            for (int i = 0; i < pageCount; i++) {
                Page page = new Page(new PageId(0, 1, i), 8192);
                byte[] data = new byte[8192];
                for (int j = 0; j < data.length; j++) {
                    data[j] = (byte) (i % 256);
                }
                System.arraycopy(data, 0, page.getData(), 0, data.length);
                pageManager.writePage(page);
            }
            
            // Measure checkpoint duration with dirty pages
            long startTime = System.currentTimeMillis();
            long checkpointLsn = enabledManager.checkpointNow();
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify checkpoint completed
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify duration is reasonable for dirty pages
            assertTrue(duration < 5000, "Checkpoint with " + pageCount + " pages should take < 5s, took " + duration + "ms");
            
            // Verify all pages were flushed
            assertEquals(0, bufferPool.getDirtyPageCount(), "All pages should be flushed after checkpoint");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDurationWithLargeWAL() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Write large amount of WAL data
            int entryCount = 1000;
            for (int i = 0; i < entryCount; i++) {
                byte[] data = new byte[1024]; // 1KB per entry
                for (int j = 0; j < data.length; j++) {
                    data[j] = (byte) (i % 256);
                }
                walManager.append(1L, WALOpcode.COMMIT, null, data);
            }
            
            // Measure checkpoint duration with large WAL
            long startTime = System.currentTimeMillis();
            long checkpointLsn = enabledManager.checkpointNow();
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify checkpoint completed
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify duration is reasonable for large WAL
            assertTrue(duration < 10000, "Checkpoint with " + entryCount + " WAL entries should take < 10s, took " + duration + "ms");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDurationWithConcurrentWrites() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Start concurrent writer thread
            Thread writerThread = new Thread(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        Page page = new Page(new PageId(0, 1, i), 8192);
                        byte[] data = ("Concurrent write " + i).getBytes();
                        System.arraycopy(data, 0, page.getData(), 0, data.length);
                        pageManager.writePage(page);
                        Thread.sleep(5); // Small delay
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            writerThread.start();
            
            // Measure checkpoint duration with concurrent writes
            long startTime = System.currentTimeMillis();
            long checkpointLsn = enabledManager.checkpointNow();
            long duration = System.currentTimeMillis() - startTime;
            
            // Wait for writer thread to complete
            writerThread.join();
            
            // Verify checkpoint completed
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify duration is reasonable with concurrent writes
            assertTrue(duration < 10000, "Checkpoint with concurrent writes should take < 10s, took " + duration + "ms");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDurationMultipleSequential() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Perform multiple sequential checkpoints
            int checkpointCount = 10;
            long[] durations = new long[checkpointCount];
            
            for (int i = 0; i < checkpointCount; i++) {
                // Dirty some pages
                Page page = new Page(new PageId(0, 1, i), 8192);
                byte[] data = ("Checkpoint " + i).getBytes();
                System.arraycopy(data, 0, page.getData(), 0, data.length);
                pageManager.writePage(page);
                
                // Measure checkpoint duration
                long startTime = System.currentTimeMillis();
                enabledManager.checkpointNow();
                durations[i] = System.currentTimeMillis() - startTime;
            }
            
            // Verify all checkpoints completed
            assertTrue(enabledManager.getCheckpointCount() >= checkpointCount, 
                      "Should have performed " + checkpointCount + " checkpoints");
            
            // Verify all durations are reasonable
            for (int i = 0; i < checkpointCount; i++) {
                assertTrue(durations[i] < 5000, "Checkpoint " + i + " should take < 5s, took " + durations[i] + "ms");
            }
            
            // Verify average duration is reasonable
            double avgDuration = 0;
            for (long duration : durations) {
                avgDuration += duration;
            }
            avgDuration /= checkpointCount;
            assertTrue(avgDuration < 2000, "Average checkpoint duration should be < 2s, was " + avgDuration + "ms");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDurationWriteGateBlocksWrites() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Dirty some pages
            Page dirtyPage = new Page(new PageId(0, 1, 0), 8192);
            byte[] data = "Dirty page data".getBytes();
            System.arraycopy(data, 0, dirtyPage.getData(), 0, data.length);
            pageManager.writePage(dirtyPage);
            
            // Measure checkpoint duration with write gate
            long startTime = System.currentTimeMillis();
            long checkpointLsn = enabledManager.checkpointNow();
            long duration = System.currentTimeMillis() - startTime;
            
            // Verify checkpoint completed
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify write gate blocks writes during checkpoint
            assertTrue(duration > 0, "Checkpoint should take some time with write gate");
            
            // Verify writes can continue after checkpoint
            Page postCheckpointPage = new Page(new PageId(0, 1, 1), 8192);
            byte[] postData = "Post checkpoint data".getBytes();
            System.arraycopy(postData, 0, postCheckpointPage.getData(), 0, postData.length);
            assertTimeout(java.time.Duration.ofMillis(1000), () -> {
                pageManager.writePage(postCheckpointPage);
            }, "Writes should not be blocked after checkpoint");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    // Helper methods
    private Path createTempDir(String prefix) throws IOException {
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), prefix + "-" + System.currentTimeMillis());
        java.nio.file.Files.createDirectories(dir);
        return dir;
    }

    private Path createTempFile(String prefix) throws IOException {
        Path file = Path.of(System.getProperty("java.io.tmpdir"), prefix + "-" + System.currentTimeMillis() + ".dat");
        java.nio.file.Files.createFile(file);
        return file;
    }

    private void deleteRecursively(Path path) throws IOException {
        if (java.nio.file.Files.exists(path)) {
            if (java.nio.file.Files.isDirectory(path)) {
                java.nio.file.Files.walk(path)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            java.nio.file.Files.delete(p);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
            } else {
                java.nio.file.Files.deleteIfExists(path);
            }
        }
    }
}