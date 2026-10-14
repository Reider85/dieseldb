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
 * O2 WAL-5 verification test: WALManager.readAll() returns entries that are
 * already LSN-ascending due to TreeMap segment order and strictly increasing
 * LSNs within segments. The previous O(N log N) sort was redundant.
 */
@Tag("storage")
@Tag("smoke")
class WALReadAllIsAlreadySortedTest {

    @TempDir
    Path tempDir;

    /**
     * Creates a WAL with the given number of segments, each holding the given
     * number of entries (appended via appendBatch to ensure rotation).
     */
    private WALManager buildWal(int segmentCount, int entriesPerSegment) throws Exception {
        Path walDir = tempDir.resolve("wal");
        Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager manager = new WALManager(config);

        for (int s = 0; s < segmentCount; s++) {
            List<WALEntry> batch = new ArrayList<>(entriesPerSegment);
            for (int i = 0; i < entriesPerSegment; i++) {
                long lsn = (long) s * entriesPerSegment + i + 1;
                batch.add(new WALEntry(lsn, 1L, WALOpcode.INSERT, null, ("data-" + lsn).getBytes()));
            }
            manager.appendBatch(batch);
            if (s < segmentCount - 1) {
                manager.forceRotate();
            }
        }
        return manager;
    }

    @Test
    void readAllIsStrictlyAscendingAcrossSegments() throws Exception {
        WALManager manager = buildWal(10, 100);
        try {
            List<WALEntry> all = manager.readAll();
            assertEquals(1000, all.size(), "10 segments × 100 entries = 1000 total");

            // Verify strictly ascending LSNs and correct payloads
            for (int i = 0; i < all.size(); i++) {
                long expectedLsn = i + 1L;
                WALEntry entry = all.get(i);
                assertEquals(expectedLsn, entry.getLsn(), "Entry " + i + " LSN");
                assertArrayEquals(("data-" + expectedLsn).getBytes(), entry.getAfterImage(),
                        "Entry " + i + " payload");
            }
        } finally {
            manager.close();
        }
    }

    @Test
    void readAllEmptyWalReturnsEmptyList() throws Exception {
        Path walDir = tempDir.resolve("empty-wal");
        Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager manager = new WALManager(config);
        try {
            List<WALEntry> all = manager.readAll();
            assertTrue(all.isEmpty(), "Empty WAL should return empty list");
        } finally {
            manager.close();
        }
    }

    @Test
    void readAllStaysSortedAfterRestart() throws Exception {
        WALManager manager = buildWal(5, 50);
        List<WALEntry> original;
        long lastLsn;

        // Read all before close
        original = manager.readAll();
        assertEquals(250, original.size(), "5 segments × 50 entries");
        lastLsn = manager.getLastLsn();

        // Close and reopen to exercise discoverExistingSegments
        manager.close();

        WALConfig config = WALConfig.of(tempDir.resolve("wal"), WALConfig.MIN_SEGMENT_SIZE_BYTES);
        WALManager reopenedManager = new WALManager(config);
        try {
            List<WALEntry> reopened = reopenedManager.readAll();
            assertEquals(original.size(), reopened.size(), "Same number of entries after reopen");
            assertEquals(lastLsn, reopenedManager.getLastLsn(), "Last LSN preserved");

            // Verify order is preserved
            for (int i = 0; i < original.size(); i++) {
                assertEquals(original.get(i).getLsn(), reopened.get(i).getLsn(),
                        "Entry " + i + " LSN matches after reopen");
                assertArrayEquals(original.get(i).getAfterImage(), reopened.get(i).getAfterImage(),
                        "Entry " + i + " payload matches after reopen");
            }
        } finally {
            reopenedManager.close();
        }
    }

    @Test
    void readAllIsAtLeastAsFastAsTheOldSortPath() throws Exception {
        WALManager manager = buildWal(10, 100);
        try {
            // Warm up (JIT, lazy caches)
            manager.readAll();
            manager.readAll();

            // Measure new path (current implementation)
            int iterations = 10;
            long newTotalNanos = 0;
            for (int i = 0; i < iterations; i++) {
                long start = System.nanoTime();
                List<WALEntry> result = manager.readAll();
                long end = System.nanoTime();
                newTotalNanos += (end - start);
                // Correctness check: assert in readAll ensures result is sorted
            }
            double newAvgNanos = (double) newTotalNanos / iterations;

            // Measure old path simulation: same loop + sort
            long oldTotalNanos = 0;
            for (int i = 0; i < iterations; i++) {
                long start = System.nanoTime();
                List<WALEntry> allEntries = new ArrayList<>();
                for (WALSegment segment : manager.getSegments().values()) {
                    List<WALEntry> entries = segment.readAll();
                    allEntries.addAll(entries);
                }
                // Old sort (redundant, as proved)
                allEntries.sort((e1, e2) -> Long.compare(e1.getLsn(), e2.getLsn()));
                long end = System.nanoTime();
                oldTotalNanos += (end - start);
            }
            double oldAvgNanos = (double) oldTotalNanos / iterations;

            // Assert new path is not slower than old path (with small slack for JIT noise)
            assertTrue(newAvgNanos <= oldAvgNanos * 1.1,
                    String.format("New path (%.1f ns) should be <= old path (%.1f ns) * 1.1",
                            newAvgNanos, oldAvgNanos));

            // Log observed performance for reporting
            System.out.printf("WAL-5 benchmark: new avg=%.1f ns, old avg=%.1f ns, ratio=%.2f%n",
                    newAvgNanos, oldAvgNanos, oldAvgNanos / newAvgNanos);
        } finally {
            manager.close();
        }
    }
}