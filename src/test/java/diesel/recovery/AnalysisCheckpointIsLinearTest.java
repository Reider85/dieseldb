package diesel.recovery;

import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WAL-9: swap-reference behavior in AnalysisPhase.apply CHECKPOINT branch.
 * 
 * Verifies that CHECKPOINT entries replace the active set wholesale (O(C) time)
 * rather than clearing and re-adding (O(A+C) time), preserving the clear() contract.
 */
@Tag("smoke")
@Tag("storage")
public class AnalysisCheckpointIsLinearTest {

    @TempDir
    Path walDir;

    private WALManager wal;

    @BeforeEach
    void setUp() throws IOException {
        wal = new WALManager(WALConfig.of(walDir, 2_000_000));
    }

    @Test
    void analysisOverThousandCheckpointsMatchesLastCheckpoint() throws IOException {
        // Build 1000 CHECKPOINT entries, each embedding 1000 active txids (distinct ranges)
        int checkpointCount = 1000;
        int txidsPerCheckpoint = 1000;
        
        for (int i = 0; i < checkpointCount; i++) {
            long[] activeTxids = new long[txidsPerCheckpoint];
            for (int j = 0; j < txidsPerCheckpoint; j++) {
                activeTxids[j] = (long) i * txidsPerCheckpoint + j + 1;
            }
            CheckpointRecord record = new CheckpointRecord(0L, activeTxids, 0L);
            wal.append(0L, WALOpcode.CHECKPOINT, null, record.toBytes());
        }

        // Analyze the full WAL
        AnalysisResult result = AnalysisPhase.analyze(wal, null);
        
        // Should match the last checkpoint's active txids
        Set<Long> expectedLastCheckpoint = new HashSet<>();
        for (int j = 0; j < txidsPerCheckpoint; j++) {
            expectedLastCheckpoint.add((long) (checkpointCount - 1) * txidsPerCheckpoint + j + 1);
        }
        
        assertEquals(expectedLastCheckpoint, result.getActive());
        assertTrue(result.getCommitted().isEmpty());
        assertEquals((long) checkpointCount, result.getLastLSN());
    }

    @Test
    void reseedReplacesActiveSetWholesale() throws IOException {
        // 200 checkpoints with 2000 txids each
        int checkpointCount = 200;
        int txidsPerCheckpoint = 2000;
        
        for (int i = 0; i < checkpointCount; i++) {
            long[] activeTxids = new long[txidsPerCheckpoint];
            for (int j = 0; j < txidsPerCheckpoint; j++) {
                activeTxids[j] = (long) i * txidsPerCheckpoint + j + 1;
            }
            CheckpointRecord record = new CheckpointRecord(0L, activeTxids, 0L);
            wal.append(0L, WALOpcode.CHECKPOINT, null, record.toBytes());
        }
        
        // Final checkpoint with only 5 txids
        long[] finalActiveTxids = {10001L, 10002L, 10003L, 10004L, 10005L};
        CheckpointRecord finalRecord = new CheckpointRecord(0L, finalActiveTxids, 0L);
        wal.append(0L, WALOpcode.CHECKPOINT, null, finalRecord.toBytes());
        
        // Analyze the full WAL
        AnalysisResult result = AnalysisPhase.analyze(wal, null);
        
        // Should exactly match the final checkpoint's txids (wholesale replacement, not merge)
        Set<Long> expectedFinal = Arrays.stream(finalActiveTxids).boxed().collect(Collectors.toSet());
        assertEquals(expectedFinal, result.getActive());
        assertTrue(result.getCommitted().isEmpty());
    }

    @Test
    void corruptCheckpointDoesNotSwapActiveSet() throws IOException {
        // Valid checkpoint with txids {1,2}
        long[] validTxids = {1L, 2L};
        CheckpointRecord validRecord = new CheckpointRecord(0L, validTxids, 0L);
        wal.append(0L, WALOpcode.CHECKPOINT, null, validRecord.toBytes());
        
        // Corrupt checkpoint with garbage bytes
        wal.append(0L, WALOpcode.CHECKPOINT, null, new byte[]{0x01, 0x02, 0x03, 0x04});
        
        // INSERT(3) - should become active
        wal.append(3L, WALOpcode.INSERT, null, new byte[]{0x00});
        
        // Analyze the WAL
        AnalysisResult result = AnalysisPhase.analyze(wal, null);
        
        // Should be {1,2,3} (corrupt checkpoint skipped, active set unchanged)
        Set<Long> expected = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        assertEquals(expected, result.getActive());
        assertTrue(result.getCommitted().isEmpty());
    }

    @AfterEach
    void tearDown() throws IOException {
        wal.close();
    }
}