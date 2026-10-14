package diesel.recovery;

import diesel.storage.page.PageManager;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import diesel.wal.DmlPayload;
import diesel.wal.WALConfig;
import diesel.wal.FsyncPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Recovery single-pass I/O test: verifies that during ARIES recovery,
 * each WAL segment is read from disk at most once across all phases
 * (analysis, redo, undo) thanks to the cross-phase decoded segment cache.
 */
@Tag("smoke")
@Tag("storage")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RecoverySinglePassIOTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private WALManager walManager;
    private PageManager pageManager;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-singlepass-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        Files.createDirectories(dataDir);
        Files.createDirectories(walDir);

        // Cache (2 MB) far exceeds the WAL written by these tests, so every
        // segment stays cached: analysis does one disk pass, redo is served
        // entirely from cache.
        walManager = new WALManager(WALConfig.of(walDir, 1024 * 1024, 1000, 0,
                walDir.resolve("archive"), 1, 0, true, FsyncPolicy.GROUP, 5, 64,
                2 * 1024 * 1024));
        Path pageFile = dataDir.resolve("test-pages.bin");
        pageManager = PageManager.open(pageFile, 10, 8192);
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            walManager.close();
        } catch (IOException e) {
            // Ignore cleanup errors
        }
        try {
            pageManager.close();
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        try {
            Files.walk(root)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    @Order(1)
    @DisplayName("Recovery reads each WAL segment at most once from disk")
    void recoveryReadsEachSegmentAtMostOnce() throws IOException {
        // Deterministic segments: small batches + forceRotate() (size-based
        // rotation alone would never trigger for tiny entries).
        int numSegments = 8;
        int entriesPerSegment = 10;

        for (int seg = 1; seg <= numSegments; seg++) {
            for (int i = 0; i < entriesPerSegment; i++) {
                long txid = (long) (seg - 1) * entriesPerSegment + i + 1;
                walManager.append(txid, WALOpcode.INSERT, null,
                        DmlPayload.serialize("test_table", i % 100, createDmlPayloadValues(i % 100)));
            }
            walManager.forceRotate();
        }

        walManager.resetRecoveryCacheStats();
        ARIESAlgorithm.recover(walManager, pageManager, null, null);

        int totalSegments = walManager.getSegments().size();
        long diskReads = walManager.recoveryCacheDiskReadCount();
        long cacheHits = walManager.recoveryCacheHitCount();

        assertTrue(diskReads <= totalSegments,
                String.format("Each segment read from disk at most once: reads=%d, segments=%d",
                        diskReads, totalSegments));
        assertTrue(cacheHits > 0,
                "Redo phase should be served from the cache populated by analysis");
        assertEquals(0, walManager.recoveryCacheEntryCount(),
                "Cache should be cleared after recovery to free memory");
    }

    @Test
    @Order(2)
    @DisplayName("Recovery correctness with cached segments")
    void recoveryCorrectnessWithCachedSegments() throws IOException {
        int numSegments = 8;
        int entriesPerSegment = 5;
        List<Long> committedTxids = new ArrayList<>();
        List<Long> activeTxids = new ArrayList<>();

        for (int seg = 1; seg <= numSegments; seg++) {
            for (int i = 0; i < entriesPerSegment; i++) {
                long txid = (long) (seg - 1) * entriesPerSegment + i + 1;
                WALOpcode op = (i % 3 == 0) ? WALOpcode.COMMIT :
                               (i % 3 == 1) ? WALOpcode.ABORT : WALOpcode.INSERT;

                if (op == WALOpcode.COMMIT) {
                    committedTxids.add(txid);
                } else if (op == WALOpcode.INSERT) {
                    activeTxids.add(txid);
                }

                walManager.append(txid, op, null,
                        op == WALOpcode.INSERT ? DmlPayload.serialize("test_table", i, createDmlPayloadValues(i)) : null);
            }
            walManager.forceRotate();
        }

        walManager.resetRecoveryCacheStats();
        RecoveryResult result = ARIESAlgorithm.recover(walManager, pageManager, null, null);

        assertEquals(committedTxids.size(), result.getAnalysis().getCommitted().size(),
                "Committed transactions count should match");
        assertEquals(activeTxids.size(), result.getAnalysis().getActive().size(),
                "Active transactions count should match");

        long diskReads = walManager.recoveryCacheDiskReadCount();
        int totalSegments = walManager.getSegments().size();
        assertTrue(diskReads <= totalSegments,
                String.format("Disk reads (%d) should not exceed total segments (%d)",
                        diskReads, totalSegments));
    }

    /**
     * Creates a DmlPayload values map for testing.
     */
    private Map<String, Object> createDmlPayloadValues(int rowIndex) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            values.put("col_" + i, "value_" + rowIndex + "_" + i);
        }
        return values;
    }
}
