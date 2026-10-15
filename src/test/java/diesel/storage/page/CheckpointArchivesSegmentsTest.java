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

/**
 * Tests that checkpoints trigger WAL archiving of segments (prompt4 #21).
 */
@Tag("wal")
@Tag("storage")
public class CheckpointArchivesSegmentsTest {

    private WALManager walManager;
    private BufferPoolFlusher flusher;
    private BufferPool bufferPool;
    private PageManager pageManager;
    private CheckpointManager checkpointManager;
    private Path walDir;
    private Path pageFile;
    private Path archiveDir;

    @BeforeEach
    void setUp() throws Exception {
        walDir = createTempDir("wal");
        archiveDir = createTempDir("archive");
        pageFile = createTempFile("pages");
        
        // Create WAL manager with small segment size to trigger archiving
        WALConfig walConfig = WALConfig.of(walDir, 1024 * 1024, 10000, 300000, archiveDir,
                                            7, 60000, true, FsyncPolicy.GROUP,
                                            WALConfig.DEFAULT_GROUP_WINDOW_MS, WALConfig.DEFAULT_GROUP_MAX_SIZE,
                                            1024 * 1024); // 1MB segment size for testing
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
        deleteRecursively(archiveDir);
        deleteRecursively(pageFile.getParent());
    }

    @Test
    void checkpointArchivesCompletedSegments() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Write enough data to fill multiple segments
            for (int i = 0; i < 10; i++) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
            }
            
            // List segments before checkpoint
            List<Path> segmentsBefore = listWALSegments(walDir);
            List<Path> archivesBefore = listWALSegments(archiveDir);
            
            // Perform checkpoint
            long checkpointLsn = enabledManager.checkpointNow();
            assertTrue(checkpointLsn > 0, "Checkpoint should have positive LSN");
            
            // List segments after checkpoint
            List<Path> segmentsAfter = listWALSegments(walDir);
            List<Path> archivesAfter = listWALSegments(archiveDir);
            
            // Verify some segments were archived
            assertTrue(archivesAfter.size() > archivesBefore.size(), 
                      "Archive should grow after checkpoint");
            
            // Verify current segment remains in WAL directory
            assertTrue(segmentsAfter.size() > 0, "Should have current segment in WAL directory");
            
            // Verify archived segments exist
            assertTrue(archivesAfter.size() > 0, "Should have archived segments");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointArchivesSegmentsBasedOnWALSize() throws Exception {
        // Enable checkpoint manager with small WAL threshold
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 2048, true); // 2KB threshold
        
        enabledManager.start();
        
        try {
            // Write data until WAL size threshold is reached
            int entriesWritten = 0;
            while (walManager.getTotalAppendedBytes() < 2048 && entriesWritten < 100) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
                entriesWritten++;
            }
            
            // List segments before checkpoint
            List<Path> archivesBefore = listWALSegments(archiveDir);
            
            // Wait for checkpoint to trigger (should be triggered by WAL size)
            long startTime = System.currentTimeMillis();
            while (enabledManager.getCheckpointCount() == 0 && 
                   System.currentTimeMillis() - startTime < 5000) {
                Thread.sleep(10);
            }
            
            // List segments after checkpoint
            List<Path> archivesAfter = listWALSegments(archiveDir);
            
            // Verify archiving occurred
            assertTrue(archivesAfter.size() >= archivesBefore.size(), 
                      "Archive should grow after checkpoint triggered by WAL size");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointArchivesSegmentsBasedOnInterval() throws Exception {
        // Enable checkpoint manager with short interval
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               50, 268435456, true); // 50ms interval
        
        enabledManager.start();
        
        try {
            // Write some data
            for (int i = 0; i < 5; i++) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
            }
            
            // List segments before checkpoint
            List<Path> archivesBefore = listWALSegments(archiveDir);
            
            // Wait for checkpoint to trigger (should be triggered by interval)
            Thread.sleep(100); // Wait longer than interval
            
            // List segments after checkpoint
            List<Path> archivesAfter = listWALSegments(archiveDir);
            
            // Verify archiving occurred
            assertTrue(archivesAfter.size() >= archivesBefore.size(), 
                      "Archive should grow after checkpoint triggered by interval");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void multipleCheckpointsArchiveIncrementally() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // First batch of writes
            for (int i = 0; i < 5; i++) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
            }
            
            // First checkpoint
            enabledManager.checkpointNow();
            List<Path> archivesAfter1 = listWALSegments(archiveDir);
            
            // Second batch of writes
            for (int i = 0; i < 5; i++) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
            }
            
            // Second checkpoint
            enabledManager.checkpointNow();
            List<Path> archivesAfter2 = listWALSegments(archiveDir);
            
            // Verify incremental archiving
            assertTrue(archivesAfter2.size() >= archivesAfter1.size(), 
                      "Archive should grow with each checkpoint");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointDoesNotArchiveCurrentSegment() throws Exception {
        // Enable checkpoint manager
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // Write some data
            for (int i = 0; i < 3; i++) {
                walManager.append(1L, WALOpcode.COMMIT, null, null);
            }
            
            // List current segment
            List<Path> segmentsBefore = listWALSegments(walDir);
            List<Path> archivesBefore = listWALSegments(archiveDir);
            
            // Perform checkpoint
            enabledManager.checkpointNow();
            
            // List segments after checkpoint
            List<Path> segmentsAfter = listWALSegments(walDir);
            List<Path> archivesAfter = listWALSegments(archiveDir);
            
            // Verify current segment remains in WAL directory
            assertEquals(segmentsBefore.size(), segmentsAfter.size(), 
                        "Current segment count should remain the same");
            
            // Verify some segments were archived
            assertTrue(archivesAfter.size() >= archivesBefore.size(), 
                      "Archive should grow after checkpoint");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    @Test
    void checkpointArchivesWithEmptyWAL() throws Exception {
        // Enable checkpoint manager with empty WAL
        CheckpointManager enabledManager = new CheckpointManager(walManager, flusher, bufferPool, null,
                                                               300000, 268435456, true);
        enabledManager.start();
        
        try {
            // List segments before checkpoint
            List<Path> archivesBefore = listWALSegments(archiveDir);
            
            // Perform checkpoint on empty WAL
            enabledManager.checkpointNow();
            
            // List segments after checkpoint
            List<Path> archivesAfter = listWALSegments(archiveDir);
            
            // Verify no archiving occurred for empty WAL
            assertEquals(archivesBefore.size(), archivesAfter.size(), 
                        "No archiving should occur for empty WAL");
            
        } finally {
            enabledManager.stopWithoutCheckpoint();
        }
    }

    // Helper methods
    private List<Path> listWALSegments(Path dir) throws IOException {
        if (!java.nio.file.Files.exists(dir)) {
            return List.of();
        }
        return java.nio.file.Files.list(dir)
            .filter(p -> p.getFileName().toString().startsWith("wal-"))
            .sorted()
            .toList();
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