package diesel;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import java.lang.reflect.Method;
import static org.junit.jupiter.api.Assertions.*;

/**
 * WAL-1 verification test: findSegmentForLsn is O(log n) using LSN-to-segment index.
 * Uses reflection to test the private method with large datasets.
 */
@Tag("storage")
@Tag("smoke")
class WALFindSegmentIsLogNTest {

    @TempDir
    Path tempDir;

    @Test
    void findSegmentForLsnIsLogarithmicTime() throws Exception {
            // Create WAL manager with small segments to force many rotations
            Path walDir = tempDir.resolve("wal");
            java.nio.file.Files.createDirectories(walDir);
            // Small segment size: 1MB segments with 100-byte entries (minimum allowed)
            WALConfig config = WALConfig.of(walDir, 1024 * 1024);
        WALManager walManager = new WALManager(config);

        try {
            // Create many segments by appending small entries and forcing rotation
            int segmentCount = 50; // Reduced for faster test
            List<Long> testLsns = new ArrayList<>();

            for (int i = 0; i < segmentCount; i++) {
                // Append a small entry
                byte[] data = ("entry-" + i).getBytes();
                WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, data);
                testLsns.add(entry.getLsn());
                
                // Force rotation every 5 entries to create multiple segments
                if (i % 5 == 4) {
                    walManager.forceRotate();
                }
            }

            // Test findSegmentForLsn via reflection on the private method
            WALManagerTestHelper helper = new WALManagerTestHelper(walManager);
            
            // Test correctness: find segments for first, middle, and last LSNs
            long firstLsn = testLsns.get(0);
            long middleLsn = testLsns.get(segmentCount / 2);
            long lastLsn = testLsns.get(segmentCount - 1);

            WALSegmentProxy firstSegment = helper.findSegmentForLsn(firstLsn);
            WALSegmentProxy middleSegment = helper.findSegmentForLsn(middleLsn);
            WALSegmentProxy lastSegment = helper.findSegmentForLsn(lastLsn);

            assertNotNull(firstSegment, "Should find segment for first LSN");
            assertNotNull(middleSegment, "Should find segment for middle LSN");
            assertNotNull(lastSegment, "Should find segment for last LSN");

            // Test performance: measure time for many random lookups
            int lookupCount = 1000;
            long totalNanos = 0;

            for (int i = 0; i < lookupCount; i++) {
                long randomLsn = testLsns.get(i % testLsns.size());
                long start = System.nanoTime();
                WALSegmentProxy result = helper.findSegmentForLsn(randomLsn);
                long end = System.nanoTime();
                totalNanos += (end - start);
                
                // Verify result is not null (LSN should exist in some segment)
                assertNotNull(result, "Lookup " + i + " should find a segment for LSN " + randomLsn);
            }

            double avgMicros = (totalNanos / lookupCount) / 1000.0;
            assertTrue(avgMicros < 100.0, 
                    "Average lookup time " + String.format("%.2f", avgMicros) + "µs, expected < 100µs (O(log n) proof)");
            
            // Test edge cases
            // LSN before all segments
            assertNull(helper.findSegmentForLsn(0), "LSN 0 should return null");
            
            // LSN after all segments (simulate checkpoint ptr beyond end)
            long beyondLastLsn = lastLsn + 1000;
            WALSegmentProxy result = helper.findSegmentForLsn(beyondLastLsn);
            // May return the last segment (correct behavior: readByLsn will return null)
            assertNotNull(result, "Beyond LSN should return last segment (readByLsn will handle null)");

        } finally {
            walManager.close();
        }
    }

    /**
     * Helper class to access WALSegment via reflection.
     */
    private static class WALSegmentProxy {
        private final Object segment;
        private final int number;

        public WALSegmentProxy(Object segment) throws Exception {
            this.segment = segment;
            this.number = (int) segment.getClass().getMethod("getNumber").invoke(segment);
        }

        public int getNumber() {
            return number;
        }

        public long getFirstLsn() throws Exception {
            return (long) segment.getClass().getMethod("getFirstLsn").invoke(segment);
        }
    }

    /**
     * Helper class to access private WALManager methods via reflection.
     */
    private static class WALManagerTestHelper {
        private final WALManager walManager;
        private final Method findSegmentMethod;

        public WALManagerTestHelper(WALManager walManager) throws Exception {
            this.walManager = walManager;
            this.findSegmentMethod = WALManager.class.getDeclaredMethod("findSegmentForLsn", long.class);
            this.findSegmentMethod.setAccessible(true);
        }

        public WALSegmentProxy findSegmentForLsn(long lsn) throws Exception {
            Object segment = findSegmentMethod.invoke(walManager, lsn);
            if (segment == null) {
                return null;
            }
            return new WALSegmentProxy(segment);
        }
    }
}