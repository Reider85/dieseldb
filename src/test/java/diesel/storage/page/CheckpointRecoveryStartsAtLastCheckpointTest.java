package diesel.storage.page;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
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
import diesel.TxStatusTracker;

/**
 * Tests checkpoint functionality related to recovery (prompt4 #21).
 * Focuses on checkpoint record creation without full recovery.
 */
@Tag("storage")
public class CheckpointRecoveryStartsAtLastCheckpointTest {

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
    void checkpointRecordContainsCorrectLsn() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Write some data
            Page page1 = new Page(new PageId(0, 1, 0), 8192);
            byte[] data1 = "Hello World 1".getBytes();
            System.arraycopy(data1, 0, page1.getData(), 0, data1.length);
            pageManager.writePage(page1);
            
            // Perform checkpoint
            long checkpointLsn = enabledManager.checkpointNow();
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify checkpoint record can be loaded
            var record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should be loadable");
            assertEquals(checkpointLsn, record.getLastLSN() + 1, "Checkpoint LSN should be one more than record LSN");
            assertEquals(0, record.getActiveTxidCount(), "No active txids expected");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void multipleCheckpointsIncrementLastLsn() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // First checkpoint
            long checkpoint1Lsn = enabledManager.checkpointNow();
            assertTrue(checkpoint1Lsn > 0, "First checkpoint should have positive LSN");
            
            // Write some data
            Page page1 = new Page(new PageId(0, 1, 0), 8192);
            byte[] data1 = "Hello World 1".getBytes();
            System.arraycopy(data1, 0, page1.getData(), 0, data1.length);
            pageManager.writePage(page1);
            
            // Second checkpoint
            long checkpoint2Lsn = enabledManager.checkpointNow();
            assertTrue(checkpoint2Lsn > 0, "Second checkpoint should have positive LSN");
            assertTrue(checkpoint2Lsn > checkpoint1Lsn, "Second checkpoint should have higher LSN");
            
            // Verify both checkpoints exist and have correct LSNs
            var record1 = walManager.loadCheckpointRecord();
            assertNotNull(record1, "Checkpoint record should exist");
            assertEquals(checkpoint2Lsn, record1.getLastLSN() + 1, "Last checkpoint should have higher LSN");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointWithoutWritesStillCreatesRecord() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Perform checkpoint without any writes
            long checkpointLsn = enabledManager.checkpointNow();
            assertTrue(checkpointLsn > 0, "Checkpoint should create record even without writes");
            
            // Verify checkpoint record exists
            var record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should exist");
            assertEquals(checkpointLsn, record.getLastLSN() + 1, "Checkpoint LSN should be one more than record LSN");
            assertEquals(0, record.getActiveTxidCount(), "No active txids expected");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointCapturesActiveTransactions() throws Exception {
        // Create TxStatusTracker
        TxStatusTracker tracker = new TxStatusTracker();
        
        // Enable checkpoint manager with tracker
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, tracker,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Register a transaction
            long tx1 = tracker.registerTransaction();
            
            // Perform checkpoint (should capture active transaction)
            long checkpointLsn = enabledManager.checkpointNow();
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // Verify checkpoint record contains active transaction
            var record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should exist");
            assertEquals(1, record.getActiveTxidCount(), "Should have 1 active transaction");
            assertTrue(containsTxid(record.getActiveTxids(), tx1), "Should contain tx1");
            
            // Register another transaction and checkpoint again
            long tx2 = tracker.registerTransaction();
            enabledManager.checkpointNow();
            
            // Verify new checkpoint record contains both transactions
            record = walManager.loadCheckpointRecord();
            assertNotNull(record, "Checkpoint record should exist");
            assertEquals(2, record.getActiveTxidCount(), "Should have 2 active transactions");
            assertTrue(containsTxid(record.getActiveTxids(), tx1), "Should contain tx1");
            assertTrue(containsTxid(record.getActiveTxids(), tx2), "Should contain tx2");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointFlushesDirtyPages() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Dirty some pages
            Page page1 = new Page(new PageId(0, 1, 0), 8192);
            byte[] data1 = "Hello World 1".getBytes();
            System.arraycopy(data1, 0, page1.getData(), 0, data1.length);
            pageManager.writePage(page1);
            
            assertTrue(bufferPool.getDirtyPageCount() > 0, "Should have dirty pages");
            
            // Perform checkpoint
            enabledManager.checkpointNow();
            
            // Verify pages were flushed
            assertEquals(0, bufferPool.getDirtyPageCount(), "No dirty pages after checkpoint");
            
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
        Path dir = Path.of(System.getProperty("java.io.tmpdir"), prefix + "-" + java.util.UUID.randomUUID());
        java.nio.file.Files.createDirectories(dir);
        return dir;
    }

    private Path createTempFile(String prefix) throws IOException {
        // Place the file in its own unique subdirectory so pageFile.getParent()
        // is never the system temp dir (tearDown deletes it recursively).
        Path dir = createTempDir(prefix + "-pages");
        Path file = dir.resolve("pages.dat");
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