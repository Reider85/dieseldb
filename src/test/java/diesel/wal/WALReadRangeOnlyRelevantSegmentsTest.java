package diesel.wal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WAL-4 verification test: readRange skips non-overlapping segments using the
 * lsnToSegment index. With 100 segments of 1000 entries each, a narrow LSN
 * window must scan at most 2 segments (the one containing the window start
 * and the one containing the window end).
 *
 * <p>Segment-scan counting uses the package-private
 * {@code WALManager.readRangeSegmentsScanned()} hook rather than a FileChannel
 * wrapper: WALSegment/WALManager are final and open channels internally, so a
 * channel delegate would require a heavier production seam.
 */
@Tag("storage")
@Tag("smoke")
class WALReadRangeOnlyRelevantSegmentsTest {

    private static final int SEGMENT_COUNT = 100;
    private static final int ENTRIES_PER_SEGMENT = 1000;
    private static final int TOTAL_ENTRIES = SEGMENT_COUNT * ENTRIES_PER_SEGMENT;

    @TempDir
    Path tempDir;

    /**
     * Creates a WAL with {@link #SEGMENT_COUNT} segments, each holding
     * {@link #ENTRIES_PER_SEGMENT} entries, so LSNs run 1..{@link #TOTAL_ENTRIES}.
     */
    private WALManager buildManySegments() throws Exception {
        Path walDir = tempDir.resolve("wal");
        Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager manager = new WALManager(config);

        for (int s = 0; s < SEGMENT_COUNT; s++) {
            List<WALEntry> batch = new ArrayList<>(ENTRIES_PER_SEGMENT);
            for (int i = 0; i < ENTRIES_PER_SEGMENT; i++) {
                long lsn = (long) s * ENTRIES_PER_SEGMENT + i + 1;
                batch.add(new WALEntry(lsn, 1L, WALOpcode.INSERT, null, ("data-" + lsn).getBytes()));
            }
            manager.appendBatch(batch);
            manager.forceRotate();
        }
        return manager;
    }

    @Test
    void readRangeScansAtMostTwoSegments() throws Exception {
        WALManager manager = buildManySegments();
        try {
            assertEquals((long) TOTAL_ENTRIES, manager.getLastLsn());

            // A narrow 11-LSN window straddling a segment boundary:
            // LSN 50_000 ends segment 49 (49_001..50_000); 50_001..50_010 start segment 50.
            long from = 50_000;
            long to = 50_010;

            // Correctness first: exactly the 11 entries, ascending, right payloads.
            List<WALEntry> range = manager.readRange(from, to);
            assertEquals(11, range.size(), "window must contain exactly 11 entries");
            for (int i = 0; i < range.size(); i++) {
                long expectedLsn = from + i;
                assertEquals(expectedLsn, range.get(i).getLsn());
                assertArrayEquals(("data-" + expectedLsn).getBytes(), range.get(i).getAfterImage());
            }

            // WAL-4 core claim: the index-based scan touches at most 2 segments.
            manager.resetReadRangeSegmentsScanned();
            manager.readRange(from, to);
            assertTrue(manager.readRangeSegmentsScanned() <= 2,
                    "readRange(" + from + ", " + to + ") scanned "
                            + manager.readRangeSegmentsScanned() + " segments, expected <= 2");
        } finally {
            manager.close();
        }
    }

    @Test
    void targetedReadRangeIsMuchFasterThanFullScan() throws Exception {
        WALManager manager = buildManySegments();
        try {
            // Warm up (JIT, lazy caches).
            manager.readRange(50_000, 50_010);
            manager.readRange(1, TOTAL_ENTRIES);

            // Full-range call scans every indexed segment (same as the old
            // behaviour); targeted call scans 2. Compare averages so machine
            // speed cancels out.
            int fullIterations = 3;
            long fullTotal = 0;
            for (int i = 0; i < fullIterations; i++) {
                long start = System.nanoTime();
                manager.readRange(1, TOTAL_ENTRIES);
                fullTotal += System.nanoTime() - start;
            }
            double fullAvgNanos = (double) fullTotal / fullIterations;

            int targetedIterations = 50;
            long targetedTotal = 0;
            for (int i = 0; i < targetedIterations; i++) {
                long start = System.nanoTime();
                manager.readRange(50_000, 50_010);
                targetedTotal += System.nanoTime() - start;
            }
            double targetedAvgNanos = (double) targetedTotal / targetedIterations;

            // Expected ratio is ~2/100; require < 1/4 to tolerate noise.
            assertTrue(targetedAvgNanos * 4 < fullAvgNanos,
                    String.format("targeted readRange avg %.0fns should be < 1/4 of full-scan avg %.0fns",
                            targetedAvgNanos, fullAvgNanos));
        } finally {
            manager.close();
        }
    }

    @Test
    void fullRangeScanStillReturnsEverything() throws Exception {
        WALManager manager = buildManySegments();
        try {
            // Regression guard: the subMap path must not lose entries for a
            // window covering every segment.
            List<WALEntry> all = manager.readRange(1, TOTAL_ENTRIES);
            assertEquals(TOTAL_ENTRIES, all.size());
            assertEquals(1L, all.get(0).getLsn());
            assertEquals((long) TOTAL_ENTRIES, all.get(TOTAL_ENTRIES - 1).getLsn());
        } finally {
            manager.close();
        }
    }
}
