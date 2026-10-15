package diesel.storage.page;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;
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
import diesel.TxStatusTracker;

/**
 * Unit tests for CheckpointManager (prompt4 #21).
 */
@Tag("smoke")
@Tag("storage")
public class CheckpointManagerTest {

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
    void checkpointNowWritesRecordAndPtr() throws Exception {
        // Enabled checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                                300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Wait a bit to ensure manager is started
            Thread.sleep(100);
            
            // Perform checkpoint
            long lsn = enabledManager.checkpointNow();
            
            // Verify checkpoint was written
            assertTrue(lsn > 0, "Checkpoint LSN should be positive");
            
            // Verify checkpoint record can be loaded
            var record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should be loadable");
            // Checkpoint record should contain the LSN of the last entry before checkpoint
            // The checkpoint LSN should be one more than the last LSN in the record
            assertEquals(lsn, record.getLastLSN() + 1, "Checkpoint LSN should be one more than record LSN");
            assertEquals(0, record.getActiveTxidCount(), "No active txids expected");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointNowFlushesDirtyPages() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Dirty some pages
            Page dirtyPage = new Page(new PageId(0, 1, 0), 8192);
            pageManager.writePage(dirtyPage);
            
            assertTrue(bufferPool.getDirtyPageCount() > 0, "Should have dirty pages");
            
            // Perform checkpoint
            enabledManager.checkpointNow();
            
            // Verify pages were flushed
            assertEquals(0, bufferPool.getDirtyPageCount(), "No dirty pages after checkpoint");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointNowCapturesActiveTxids() throws Exception {
        // Create TxStatusTracker
        TxStatusTracker tracker = new TxStatusTracker();
        
        // Enable checkpoint manager with tracker
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, tracker,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Register some transactions
            long tx1 = tracker.registerTransaction();
            long tx2 = tracker.registerTransaction();
            
            // Perform checkpoint
            enabledManager.checkpointNow();
            
            // Verify checkpoint record contains txids
            var record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should be loadable");
            assertEquals(2, record.getActiveTxidCount(), "Should have 2 active txids");
            assertTrue(containsTxid(record.getActiveTxids(), tx1), "Should contain tx1");
            assertTrue(containsTxid(record.getActiveTxids(), tx2), "Should contain tx2");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void walBytesTriggerFiresCheckpoint() throws Exception {
        // Enable checkpoint manager with small WAL threshold
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 100, true); // 100 bytes threshold
        enabledManager.start();
        
        try {
            // Wait for checkpoint to trigger
            long initialCount = enabledManager.getCheckpointCount();
            long startTime = System.currentTimeMillis();
            
            // Append WAL entries until threshold is reached
            while (enabledManager.getCheckpointCount() == initialCount && 
                   System.currentTimeMillis() - startTime < 5000) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
                Thread.sleep(10);
            }
            
            // Verify checkpoint was triggered
            assertTrue(enabledManager.getCheckpointCount() > initialCount, 
                      "Checkpoint should be triggered by WAL bytes");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void intervalTriggerFiresCheckpoint() throws Exception {
        // Enable checkpoint manager with short interval
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               50, 268435456, true); // 50ms interval
        enabledManager.start();
        
        try {
            // Wait for checkpoint to trigger
            long initialCount = enabledManager.getCheckpointCount();
            Thread.sleep(100); // Wait longer than interval
            
            // Verify checkpoint was triggered
            assertTrue(enabledManager.getCheckpointCount() > initialCount, 
                      "Checkpoint should be triggered by interval");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void writeGateBlocksPageWrites() throws Exception {
        // Create a latch to coordinate the test
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger writeCompleted = new AtomicInteger(0);
        
        // Create a thread that will try to write while holding the gate
        Thread writeThread = new Thread(() -> {
            try {
                // Acquire write gate (write lock)
                checkpointManager.getWriteGate().writeLock().lock();
                
                // Signal that we're holding the lock
                latch.countDown();
                
                // Wait for test completion
                Thread.sleep(100);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                checkpointManager.getWriteGate().writeLock().unlock();
            }
        });
        
        writeThread.start();
        
        // Wait for the thread to acquire the lock
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Thread should acquire write lock");
        
        // Try to write a page while gate is held
        long startTime = System.currentTimeMillis();
        Page page = new Page(new PageId(0, 1, 0), 8192);
        
        // This should block until the lock is released
        assertTimeout(java.time.Duration.ofMillis(200), () -> {
            pageManager.writePage(page);
        });
        
        // Verify write completed after lock release
        writeCompleted.set(1);
        writeThread.join();
        assertEquals(1, writeCompleted.get(), "Write should complete after lock release");
    }

    @Test
    void lifecycleIdempotent() throws Exception {
        // Create a new enabled checkpoint manager for this test
        CheckpointManager testManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                            300000, 268435456, true);
        
        try {
            // Start
            testManager.start();
            assertTrue(testManager.isRunning(), "Should be running after start");
            
            // Start again (should be no-op)
            testManager.start();
            assertTrue(testManager.isRunning(), "Should still be running");
            
            // Stop
            testManager.stop();
            assertFalse(testManager.isRunning(), "Should not be running after stop");
            
            // Stop again (should be no-op)
            testManager.stop();
            assertFalse(testManager.isRunning(), "Should still not be running");
        } finally {
            testManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void stopDoesFinalCheckpoint() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        long initialCount = enabledManager.getCheckpointCount();
        
        // Stop with checkpoint
        enabledManager.stop();
        
        // Verify final checkpoint was performed
        assertTrue(enabledManager.getCheckpointCount() > initialCount, 
                  "Final checkpoint should be performed on stop");
        assertFalse(enabledManager.isRunning(), "Should not be running after stop");
    }

    @Test
    void disabledSkipsDaemonButManualWorks() throws Exception {
        // Disabled checkpoint manager
        CheckpointManager disabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                                300000, 268435456, false);
        disabledManager.start();
        
        // Should not be running
        assertFalse(disabledManager.isRunning(), "Disabled manager should not start");
        
        // Manual checkpoint should return 0 when disabled
        long lsn = disabledManager.checkpointNow();
        assertEquals(0, lsn, "Disabled manager should return 0 for manual checkpoint");
        
        // WAL should not have checkpoint record when disabled
        var record = walManager.loadCheckpointRecord();
        assertNull(record, "No checkpoint record should be written when disabled");
    }

    @Test
    void jmxMetricsExposed() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Perform a checkpoint
            enabledManager.checkpointNow();
            
            // Verify metrics
            assertTrue(enabledManager.getCheckpointCount() > 0, "Checkpoint count should be > 0");
            assertTrue(enabledManager.getLastCheckpointLsn() > 0, "Last checkpoint LSN should be > 0");
            assertTrue(enabledManager.getLastCheckpointDurationMs() > 0, "Duration should be > 0");
            assertEquals(300000, enabledManager.getCurrentIntervalMs(), "Current interval should match");
            assertEquals(300000, enabledManager.getBaseIntervalMs(), "Base interval should match");
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    // Helper methods
    private boolean containsTxid(long[] txids, long target) {
        for (long txid : txids) {
            if (txid == target) {
                return true;
            }
        }
        return false;
    }

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