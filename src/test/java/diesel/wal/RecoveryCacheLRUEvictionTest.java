package diesel.wal;

import diesel.recovery.ARIESAlgorithm;
import diesel.recovery.RecoveryResult;
import diesel.storage.page.PageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
 * Recovery cache LRU eviction test: verifies that the recovery cache
 * evicts least recently used segments when the configured size limit is
 * exceeded, that a disabled cache (size = 0) always reads from disk, and
 * that recovery stays correct regardless of cache behaviour.
 */
@Tag("smoke")
@Tag("storage")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RecoveryCacheLRUEvictionTest {

    private Path root;
    private Path dataDir;
    private Path walDir;
    private WALManager walManager;
    private PageManager pageManager;

    @BeforeEach
    void setUp() throws IOException {
        root = Path.of(System.getProperty("java.io.tmpdir"),
                "recovery-lru-" + UUID.randomUUID());
        dataDir = root.resolve("data");
        walDir = root.resolve("wal");
        Files.createDirectories(dataDir);
        Files.createDirectories(walDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            if (pageManager != null) {
                pageManager.close();
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        try {
            if (walManager != null) {
                walManager.close();
            }
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

    private WALManager newManager(long recoveryCacheSizeBytes) throws IOException {
        return new WALManager(WALConfig.of(walDir, 1024 * 1024, 1000, 0,
                walDir.resolve("archive"), 1, 0, true, FsyncPolicy.GROUP, 5, 64,
                recoveryCacheSizeBytes));
    }

    private void writeSegments(int numSegments, int entriesPerSegment) throws IOException {
        for (int seg = 1; seg <= numSegments; seg++) {
            for (int i = 0; i < entriesPerSegment; i++) {
                long txid = (long) (seg - 1) * entriesPerSegment + i + 1;
                walManager.append(txid, WALOpcode.INSERT, null,
                        DmlPayload.serialize("test_table", i, createDmlPayloadValues(i)));
            }
            walManager.forceRotate();
        }
    }

    /**
     * Returns the encoded entry bytes of an already-written segment.
     */
    private long segmentBytes(int segmentNumber) throws IOException {
        return walManager.getSegments().get(segmentNumber).readAll().stream()
                .mapToLong(WALEntry::encodedSize)
                .sum();
    }

    @Test
    @Order(1)
    @DisplayName("Cache evicts least recently used segment when size limit exceeded")
    void cacheEvictsSegmentsWhenSizeLimitExceeded() throws IOException {
        // Phase A: write 4 segments with a large cache.
        walManager = newManager(2 * 1024 * 1024);
        writeSegments(4, 3);

        // Exact byte footprint of segments 1 and 2 — the reopened cache limit
        // will hold exactly those two segments.
        long limit = segmentBytes(1) + segmentBytes(2);
        walManager.close();

        // Phase B: reopen the same WAL directory with cache = size(1) + size(2).
        walManager = newManager(limit);
        walManager.resetRecoveryCacheStats();

        // Read order 1,2,3,2:
        //   put(1) fits, put(2) fits (total == limit, not >), put(3) exceeds
        //   the limit and evicts segment 1 (LRU), re-read of 2 hits the cache.
        for (int segNum : List.of(1, 2, 3, 2)) {
            List<WALEntry> entries = walManager.readAllCached(segNum);
            assertFalse(entries.isEmpty(), "Segment " + segNum + " should not be empty");
        }

        assertEquals(3, walManager.recoveryCacheDiskReadCount(),
                "Segments 1,2,3 read once each; re-read of 2 served from cache");
        assertEquals(1, walManager.recoveryCacheHitCount(),
                "Second read of segment 2 should hit the cache");
        assertTrue(walManager.recoveryCacheEntryCount() <= 2,
                "LRU should have evicted the oldest segment (cache holds at most 2 segments)");
    }

    @Test
    @Order(2)
    @DisplayName("Cache disabled when size = 0")
    void cacheDisabledWhenSizeZero() throws IOException {
        walManager = newManager(0);
        writeSegments(3, 5);

        walManager.resetRecoveryCacheStats();

        for (int segNum = 1; segNum <= 3; segNum++) {
            List<WALEntry> entries = walManager.readAllCached(segNum);
            assertFalse(entries.isEmpty(), "Segment " + segNum + " should not be empty");
        }

        assertEquals(3, walManager.recoveryCacheDiskReadCount(),
                "Should have one disk read per segment when cache is disabled");
        assertEquals(0, walManager.recoveryCacheHitCount(),
                "Should have no cache hits when cache is disabled");
        assertEquals(0, walManager.recoveryCacheEntryCount(),
                "Should have no cache entries when cache is disabled");
    }

    @Test
    @Order(3)
    @DisplayName("Recovery remains correct with cache eviction")
    void recoveryRemainsCorrectWithCacheEviction() throws IOException {
        // A 1-byte cache limit evicts every segment immediately after it is
        // inserted, forcing analysis and redo to each re-read from disk —
        // recovery must still produce the correct analysis result.
        walManager = newManager(1);

        int numSegments = 6;
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

        Path pageFile = dataDir.resolve("test-pages.bin");
        pageManager = PageManager.open(pageFile, 10, 8192);

        walManager.resetRecoveryCacheStats();
        RecoveryResult result = ARIESAlgorithm.recover(walManager, pageManager, null, null);

        assertEquals(committedTxids.size(), result.getAnalysis().getCommitted().size(),
                "Committed transactions count should match even with a tiny cache");
        assertEquals(activeTxids.size(), result.getAnalysis().getActive().size(),
                "Active transactions count should match even with a tiny cache");

        long diskReads = walManager.recoveryCacheDiskReadCount();
        int totalSegments = walManager.getSegments().size();
        assertTrue(diskReads >= totalSegments,
                "Eviction forces re-reads: analysis and redo each read segments");
        assertTrue(diskReads <= 2L * totalSegments + 1,
                String.format("At most one extra pass over the WAL: reads=%d, segments=%d",
                        diskReads, totalSegments));
        assertEquals(0, walManager.recoveryCacheEntryCount(),
                "Cache should be empty after recovery (cleared and everything evicted)");
    }

    private Map<String, Object> createDmlPayloadValues(int rowIndex) {
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            values.put("col_" + i, "value_" + rowIndex + "_" + i);
        }
        return values;
    }
}
