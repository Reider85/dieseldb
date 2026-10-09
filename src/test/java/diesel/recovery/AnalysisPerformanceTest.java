package diesel.recovery;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance acceptance test for the ARIES analysis phase (prompt 4 #17):
 * analyzing a 1 GB WAL must complete in under 5 seconds.
 *
 * <p>Builds a ~1 GiB WAL ({@code BEGIN + INSERT [+ COMMIT]} per transaction,
 * 128 KiB payloads), then times {@link AnalysisPhase#analyze(WALManager)}.
 * The target size can be overridden with {@code -Ddiesel.analysis.perf.bytes=N}
 * for quick smoke runs.
 */
@Tag("perf")
public class AnalysisPerformanceTest {

    private static final long DEFAULT_TARGET_BYTES = 1L << 30; // 1 GiB
    private static final long MAX_ANALYSIS_MS = 5_000;
    private static final int PAYLOAD_SIZE = 128 * 1024;
    private static final long SEGMENT_SIZE = 64L * 1024 * 1024; // 64 MiB segments

    @Test
    void analysisOnOneGigabyteWalCompletesUnderFiveSeconds() throws IOException {
        long targetBytes = Long.getLong("diesel.analysis.perf.bytes", DEFAULT_TARGET_BYTES);
        Path walDir = Path.of(System.getProperty("java.io.tmpdir"),
                "analysis-perf-" + UUID.randomUUID());
        WALManager walManager = new WALManager(WALConfig.of(walDir, SEGMENT_SIZE));
        try {
            byte[] payload = new byte[PAYLOAD_SIZE];
            long written = 0;
            long expectedCommitted = 0;
            long expectedActive = 0;
            long txid = 1;

            while (written < targetBytes) {
                walManager.append(txid, WALOpcode.BEGIN, null, null);
                written += 32; // fixed-size BEGIN entry (aligned)
                walManager.append(txid, WALOpcode.INSERT, null, payload);
                written += 28 + PAYLOAD_SIZE + 4; // header + image + CRC (aligned)
                if (txid % 2 == 0) {
                    walManager.append(txid, WALOpcode.COMMIT, null, null);
                    written += 32;
                    expectedCommitted++;
                } else {
                    expectedActive++;
                }
                txid++;
            }

            assertTrue(walManager.getLastLsn() > 0, "WAL has entries");

            long startNanos = System.nanoTime();
            AnalysisResult result = AnalysisPhase.analyze(walManager);
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            assertEquals(expectedCommitted, result.getCommittedTxidCount(),
                    "Committed set must match the WAL");
            assertEquals(expectedActive, result.getActiveTxidCount(),
                    "Active set must match the WAL");
            assertEquals(walManager.getLastLsn(), result.getLastLSN(),
                    "lastLSN must be the end of the log");
            assertTrue(elapsedMs < MAX_ANALYSIS_MS,
                    "analysis of " + written + " WAL bytes took " + elapsedMs
                            + " ms (limit " + MAX_ANALYSIS_MS + " ms)");
        } finally {
            walManager.close();
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
    }
}
