package diesel.recovery;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for ARIES checkpoint functionality.
 * Tests that checkpoints can be written and loaded correctly.
 */
@Tag("smoke")
@Tag("storage")
public class CheckpointIntegrationTest {

    private Path walDir;
    private WALManager walManager;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Path.of(System.getProperty("java.io.tmpdir"), "checkpoint-test-" + System.currentTimeMillis());
        WALConfig config = WALConfig.of(walDir, 1024 * 1024); // 1MB segment, default queue size
        walManager = new WALManager(config);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (walManager != null) {
            walManager.close();
        }
        // Clean up test directory
        java.nio.file.Files.walk(walDir)
            .sorted((a, b) -> -a.compareTo(b))
            .forEach(path -> {
                try {
                    java.nio.file.Files.delete(path);
                } catch (IOException e) {
                    // Ignore cleanup errors
                }
            });
    }

    @Test
    void testWriteAndLoadCheckpoint() throws IOException {
        // Write some WAL entries first
        walManager.append(1L, diesel.wal.WALOpcode.COMMIT, null, null);
        walManager.append(2L, diesel.wal.WALOpcode.COMMIT, null, null);
        
        // Create checkpoint with active txids
        List<Long> activeTxids = Arrays.asList(3L, 4L, 5L);
        walManager.writeCheckpoint(activeTxids);
        
        // Verify checkpoint was written by loading it
        CheckpointRecord loadedCheckpoint = walManager.loadCheckpointRecord();
        assertNotNull(loadedCheckpoint, "Checkpoint should be loadable");
        assertEquals(2L, loadedCheckpoint.getLastLSN(), "Last LSN should match");
        assertEquals(3, loadedCheckpoint.getActiveTxids().length, "Should have 3 active txids");
        assertArrayEquals(new long[]{3L, 4L, 5L}, loadedCheckpoint.getActiveTxids(), "Active txids should match");
        assertTrue(loadedCheckpoint.getTimestampEpochMs() > 0, "Timestamp should be positive");
        
        // Close and reopen WAL manager to test persistence
        walManager.close();
        walManager = new WALManager(WALConfig.of(walDir, 1024 * 1024));
        
        // Verify checkpoint persists after reopen
        CheckpointRecord persistedCheckpoint = walManager.loadCheckpointRecord();
        assertNotNull(persistedCheckpoint, "Checkpoint should persist after reopen");
        assertEquals(2L, persistedCheckpoint.getLastLSN(), "Last LSN should match after reopen");
        assertEquals(3, persistedCheckpoint.getActiveTxids().length, "Should have 3 active txids after reopen");
        assertArrayEquals(new long[]{3L, 4L, 5L}, persistedCheckpoint.getActiveTxids(), "Active txids should match after reopen");
    }

    @Test
    void testCheckpointWithNoActiveTxids() throws IOException {
        // Write checkpoint with no active transactions
        walManager.writeCheckpoint(List.of());
        
        // Verify checkpoint was written
        CheckpointRecord checkpoint = walManager.loadCheckpointRecord();
        assertNotNull(checkpoint, "Checkpoint should be loadable");
        assertEquals(0L, checkpoint.getLastLSN(), "Last LSN should be 0 when no entries written");
        assertEquals(0, checkpoint.getActiveTxids().length, "Should have 0 active txids");
    }

    @Test
    void testMultipleCheckpoints() throws IOException {
        // Write first checkpoint
        walManager.append(1L, diesel.wal.WALOpcode.COMMIT, null, null);
        walManager.writeCheckpoint(Arrays.asList(2L));
        
        // Write more entries and second checkpoint
        walManager.append(3L, diesel.wal.WALOpcode.COMMIT, null, null);
        walManager.append(4L, diesel.wal.WALOpcode.COMMIT, null, null);
        walManager.writeCheckpoint(Arrays.asList(5L, 6L));
        
        // Verify second checkpoint is the one that persists
        CheckpointRecord checkpoint = walManager.loadCheckpointRecord();
        assertNotNull(checkpoint, "Checkpoint should be loadable");
        assertEquals(4L, checkpoint.getLastLSN(), "Last LSN should match second checkpoint");
        assertEquals(2, checkpoint.getActiveTxids().length, "Should have 2 active txids");
        assertArrayEquals(new long[]{5L, 6L}, checkpoint.getActiveTxids(), "Active txids should match second checkpoint");
    }
}