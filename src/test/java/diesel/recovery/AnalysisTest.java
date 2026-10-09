package diesel.recovery;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance test for the ARIES analysis phase (prompt 4 #17).
 *
 * <p>Core criterion: a WAL with 50 committed and 50 uncommitted transactions
 * is separated into the correct {@code committed} / {@code active} sets.
 */
@Tag("smoke")
@Tag("storage")
public class AnalysisTest {

    private Path walDir;
    private WALManager walManager;

    @BeforeEach
    void setUp() throws IOException {
        walDir = Path.of(System.getProperty("java.io.tmpdir"),
                "analysis-test-" + UUID.randomUUID());
        walManager = new WALManager(WALConfig.of(walDir, 1024 * 1024));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (walManager != null) {
            walManager.close();
        }
        Files.walk(walDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
    }

    private void begin(long txid) throws IOException {
        walManager.append(txid, WALOpcode.BEGIN, null, null);
    }

    private void commit(long txid) throws IOException {
        walManager.append(txid, WALOpcode.COMMIT, null, null);
    }

    private void abort(long txid) throws IOException {
        walManager.append(txid, WALOpcode.ABORT, null, null);
    }

    private void insert(long txid) throws IOException {
        walManager.append(txid, WALOpcode.INSERT, null, new byte[]{1});
    }

    @Test
    void fiftyCommittedAndFiftyActiveAreSeparated() throws IOException {
        for (long txid = 1; txid <= 100; txid++) {
            begin(txid);
        }
        for (long txid = 1; txid <= 50; txid++) {
            commit(txid);
        }

        AnalysisResult result = AnalysisPhase.analyze(walManager);

        assertEquals(50, result.getCommittedTxidCount(),
                "Exactly 50 transactions committed");
        assertEquals(50, result.getActiveTxidCount(),
                "Exactly 50 transactions still active");
        for (long txid = 1; txid <= 50; txid++) {
            assertTrue(result.isCommitted(txid), "txid " + txid + " committed");
            assertFalse(result.isActive(txid), "txid " + txid + " not active");
        }
        for (long txid = 51; txid <= 100; txid++) {
            assertTrue(result.isActive(txid), "txid " + txid + " active");
            assertFalse(result.isCommitted(txid), "txid " + txid + " not committed");
        }
        assertEquals(Set.of(), result.getCommitted().stream()
                .filter(t -> t > 50).collect(java.util.stream.Collectors.toSet()),
                "No committed txid above 50");
        assertEquals(walManager.getLastLsn(), result.getLastLSN(),
                "lastLSN must be the end of the log");
    }

    @Test
    void emptyWalYieldsEmptySets() throws IOException {
        AnalysisResult result = AnalysisPhase.analyze(walManager);

        assertTrue(result.getCommitted().isEmpty(), "No committed txids on empty WAL");
        assertTrue(result.getActive().isEmpty(), "No active txids on empty WAL");
        assertEquals(0L, result.getLastLSN(), "lastLSN is 0 on empty WAL");
    }

    @Test
    void abortRemovesFromActiveWithoutCommitting() throws IOException {
        begin(10);
        abort(10);
        begin(11);
        insert(11);

        AnalysisResult result = AnalysisPhase.analyze(walManager);

        assertFalse(result.isActive(10), "Aborted txid not active");
        assertFalse(result.isCommitted(10), "Aborted txid not committed");
        assertEquals(Set.of(11L), result.getActive(), "Only txid 11 active");
        assertEquals(Set.of(), result.getCommitted(), "No commits");
    }

    @Test
    void checkpointSeedsActiveSetAndBoundsScanWindow() throws IOException {
        begin(1);                                    // LSN 1: active before checkpoint
        commit(2);                                   // LSN 2: committed before checkpoint
        insert(5);                                   // LSN 3: active before checkpoint, not seeded
        walManager.writeCheckpoint(List.of(1L, 3L)); // LSN 4: seeds active {1, 3}
        commit(1);                                   // LSN 5: after checkpoint
        insert(4);                                   // LSN 6: after checkpoint

        AnalysisResult result = AnalysisPhase.analyze(walManager);

        // Scan window starts at the checkpoint: only post-checkpoint commits count.
        assertEquals(Set.of(1L), result.getCommitted(),
                "Only post-checkpoint COMMIT lands in committed set");
        assertFalse(result.isCommitted(2L),
                "COMMIT before the checkpoint is outside the scan window");
        // Seed {1,3} -> COMMIT(1) -> {3} -> INSERT(4) -> {3,4}.
        assertEquals(Set.of(3L, 4L), result.getActive(),
                "Active set is seeded by the checkpoint then updated by the scan");
        assertFalse(result.isActive(5L),
                "Pre-checkpoint txid absent from the checkpoint active list is not active");
        assertEquals(walManager.getLastLsn(), result.getLastLSN());
    }

    @Test
    void midScanCheckpointReseedsActiveSet() throws IOException {
        insert(1);                                   // LSN 1
        long checkpointALastLsn = walManager.getLastLsn();
        walManager.writeCheckpoint(List.of(9L));     // CHECKPOINT A: active {9}
        commit(1);                                   // after A
        insert(2);                                   // after A
        walManager.writeCheckpoint(List.of(5L));     // CHECKPOINT B: active {5}
        insert(3);                                   // after B

        CheckpointRecord checkpointA = new CheckpointRecord(
                checkpointALastLsn, new long[]{9L}, System.currentTimeMillis());

        // Scan from checkpoint A: the later CHECKPOINT B must re-seed the active set,
        // clearing the intermediate activity (txids 9 and 2).
        AnalysisResult fromA = AnalysisPhase.analyze(walManager, checkpointA);
        assertEquals(Set.of(1L), fromA.getCommitted(), "COMMIT(1) seen after A");
        assertEquals(Set.of(5L, 3L), fromA.getActive(),
                "CHECKPOINT B re-seeds active to {5}, INSERT(3) adds 3");

        // Scan from the latest checkpoint (B): everything before B is out of window.
        AnalysisResult fromLatest = AnalysisPhase.analyze(walManager);
        assertEquals(Set.of(), fromLatest.getCommitted(),
                "COMMIT(1) happened before checkpoint B");
        assertEquals(Set.of(5L, 3L), fromLatest.getActive());
        assertEquals(walManager.getLastLsn(), fromLatest.getLastLSN());
    }

    @Test
    void analysisSurvivesRestart() throws IOException {
        begin(1);
        commit(1);
        begin(2);
        walManager.writeCheckpoint(List.of(2L));
        begin(3);

        walManager.close();
        walManager = new WALManager(WALConfig.of(walDir, 1024 * 1024));

        AnalysisResult result = AnalysisPhase.analyze(walManager);

        assertEquals(Set.of(), result.getCommitted(),
                "Pre-checkpoint COMMIT(1) is outside the scan window");
        assertEquals(Set.of(2L, 3L), result.getActive(),
                "Checkpoint-seeded txid 2 plus post-checkpoint BEGIN(3)");
        assertEquals(walManager.getLastLsn(), result.getLastLSN());
    }

    @Test
    void dmlWithoutBeginKeepsTransactionActive() throws IOException {
        insert(20);
        walManager.append(20, WALOpcode.UPDATE, null, new byte[]{2});
        walManager.append(20, WALOpcode.DELETE, null, new byte[]{3});

        AnalysisResult result = AnalysisPhase.analyze(walManager);

        assertEquals(Set.of(20L), result.getActive(),
                "DML-only transaction (no BEGIN emitted yet) stays active");
        assertEquals(Set.of(), result.getCommitted());
    }
}
