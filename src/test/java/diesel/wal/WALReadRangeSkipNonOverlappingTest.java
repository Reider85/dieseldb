package diesel.wal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * WAL-4 edge-case test: readRange correctness and segment-skip behaviour for
 * ranges that do not overlap (or only partially overlap) the WAL's LSN span.
 *
 * <p>Uses the package-private {@code WALManager.readRangeSegmentsScanned()}
 * hook to assert exactly how many segments each call touches.
 */
@Tag("storage")
@Tag("smoke")
class WALReadRangeSkipNonOverlappingTest {

    private static final int SEGMENT_COUNT = 10;
    private static final int ENTRIES_PER_SEGMENT = 100;
    private static final int TOTAL_ENTRIES = SEGMENT_COUNT * ENTRIES_PER_SEGMENT; // 1000

    @TempDir
    Path tempDir;

    /**
     * Creates a WAL with 10 segments of 100 entries each: LSNs 1..1000,
     * segment k (1-based) holds LSNs (k-1)*100+1 .. k*100.
     */
    private WALManager buildWal() throws Exception {
        Path walDir = tempDir.resolve("wal");
        Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager manager = new WALManager(config);
        for (int i = 1; i <= TOTAL_ENTRIES; i++) {
            manager.append(1L, WALOpcode.INSERT, null, ("data-" + i).getBytes());
            if (i % ENTRIES_PER_SEGMENT == 0) {
                manager.forceRotate();
            }
        }
        return manager;
    }

    @Test
    void rangeBeforeAllEntriesReturnsEmptyAndScansNothing() throws Exception {
        WALManager manager = buildWal();
        try {
            manager.resetReadRangeSegmentsScanned();
            // Entirely before every indexed segment (entries start at LSN 1).
            List<WALEntry> range = manager.readRange(0, 0);
            assertTrue(range.isEmpty(), "range before all entries must be empty");
            assertEquals(0, manager.readRangeSegmentsScanned(),
                    "no segment should be scanned for a range below the first LSN");

            // Partially overlapping from below: [0, 50] must yield LSNs 1..50.
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> overlap = manager.readRange(0, 50);
            assertEquals(50, overlap.size());
            assertEquals(1L, overlap.get(0).getLsn());
            assertEquals(50L, overlap.get(49).getLsn());
            assertEquals(1, manager.readRangeSegmentsScanned(),
                    "only the first segment overlaps [0, 50]");
        } finally {
            manager.close();
        }
    }

    @Test
    void rangeAfterAllEntriesReturnsEmptyAndScansNothing() throws Exception {
        WALManager manager = buildWal();
        try {
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(5_000, 6_000);
            assertTrue(range.isEmpty(), "range after all entries must be empty");
            assertEquals(0, manager.readRangeSegmentsScanned(),
                    "the last segment's lastLsn (1000) is below the range, so it must be skipped");
        } finally {
            manager.close();
        }
    }

    @Test
    void invertedRangeReturnsEmpty() throws Exception {
        WALManager manager = buildWal();
        try {
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(500, 400);
            assertTrue(range.isEmpty(), "fromLsn > toLsn must yield an empty range");
            assertEquals(0, manager.readRangeSegmentsScanned());
        } finally {
            manager.close();
        }
    }

    @Test
    void rangeInsideSingleSegmentScansOneSegment() throws Exception {
        WALManager manager = buildWal();
        try {
            // LSNs 250..260 live entirely inside segment 3 (201..300).
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(250, 260);
            assertEquals(11, range.size());
            assertEquals(250L, range.get(0).getLsn());
            assertEquals(260L, range.get(10).getLsn());
            assertEquals(1, manager.readRangeSegmentsScanned(),
                    "a range inside one segment must scan exactly one segment");
        } finally {
            manager.close();
        }
    }

    @Test
    void rangeSpanningSegmentBoundaryScansTwoSegments() throws Exception {
        WALManager manager = buildWal();
        try {
            // [295, 305] straddles the segment 3/4 boundary (segment 3: 201..300,
            // segment 4: 301..400).
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(295, 305);
            assertEquals(11, range.size());
            assertEquals(295L, range.get(0).getLsn());
            assertEquals(305L, range.get(10).getLsn());
            assertEquals(2, manager.readRangeSegmentsScanned(),
                    "a boundary-spanning range must scan exactly two segments");
        } finally {
            manager.close();
        }
    }

    @Test
    void wideRangeScansAllOverlappingSegments() throws Exception {
        WALManager manager = buildWal();
        try {
            // [150, 850] overlaps segments 2..9 (8 segments).
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(150, 850);
            assertEquals(701, range.size());
            assertEquals(150L, range.get(0).getLsn());
            assertEquals(850L, range.get(700).getLsn());
            assertEquals(8, manager.readRangeSegmentsScanned(),
                    "a wide range must scan exactly the overlapping segments");
        } finally {
            manager.close();
        }
    }

    @Test
    void singleLsnRangeScansOneSegment() throws Exception {
        WALManager manager = buildWal();
        try {
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(42, 42);
            assertEquals(1, range.size());
            assertEquals(42L, range.get(0).getLsn());
            assertEquals(1, manager.readRangeSegmentsScanned());
        } finally {
            manager.close();
        }
    }

    @Test
    void emptyWalYieldsEmptyRange() throws Exception {
        Path walDir = tempDir.resolve("empty-wal");
        Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager manager = new WALManager(config);
        try {
            manager.resetReadRangeSegmentsScanned();
            List<WALEntry> range = manager.readRange(1, 100);
            assertTrue(range.isEmpty());
            assertEquals(0, manager.readRangeSegmentsScanned(),
                    "an empty WAL has no indexed segments to scan");
        } finally {
            manager.close();
        }
    }
}
