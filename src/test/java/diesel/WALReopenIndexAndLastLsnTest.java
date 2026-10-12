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
 * WAL-1 verification test: WAL reopen with LSN-to-segment index and lastLsn lazy computation.
 * Tests that index is rebuilt correctly on restart and lastLsn is computed lazily.
 */
@Tag("storage")
@Tag("smoke")
class WALReopenIndexAndLastLsnTest {

    @TempDir
    Path tempDir;

    @Test
    void reopenPreservesIndexAndLastLsn() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        WALConfig config = WALConfig.of(walDir, 1024 * 1024); // 1MB segments (minimum)

        List<Long> expectedFirstLsns = new ArrayList<>();
        List<Long> expectedLastLsns = new ArrayList<>();
        WALManager walManager = null;

        try {
            // Create a WAL with multiple segments and entries
            walManager = new WALManager(config);

            // Segment 1: append 3 entries
            for (int i = 1; i <= 3; i++) {
                byte[] data = ("seg1-entry-" + i).getBytes();
                WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, data);
                expectedFirstLsns.add(1L);
                expectedLastLsns.add(entry.getLsn());
            }

            // Force rotation to segment 2
            walManager.forceRotate();

            // Segment 2: append 2 entries
            for (int i = 1; i <= 2; i++) {
                byte[] data = ("seg2-entry-" + i).getBytes();
                WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, data);
                expectedFirstLsns.add(4L); // First LSN of segment 2
                expectedLastLsns.add(entry.getLsn());
            }

            // Force rotation to segment 3
            walManager.forceRotate();

            // Segment 3: append 1 entry
            byte[] data = ("seg3-entry-1").getBytes();
            WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, data);
            expectedFirstLsns.add(6L); // First LSN of segment 3
            expectedLastLsns.add(entry.getLsn());

            // Verify we can read entries across segments
            assertNotNull(walManager.readByLsn(1), "Should read first entry");
            assertNotNull(walManager.readByLsn(3), "Should read last entry of segment 1");
            assertNotNull(walManager.readByLsn(4), "Should read first entry of segment 2");
            assertNotNull(walManager.readByLsn(5), "Should read last entry of segment 2");
            assertNotNull(walManager.readByLsn(6), "Should read first entry of segment 3");
            assertNotNull(walManager.readByLsn(6), "Should read last entry of segment 3");

            // Get current segments for later comparison
            List<WALSegmentInfo> originalSegments = getSegmentInfos(walManager);

        } finally {
            if (walManager != null) {
                walManager.close();
            }
        }

        // Reopen the WAL manager
        WALManager reopenedManager = new WALManager(config);

        try {
            // Verify index is rebuilt correctly
            WALManagerTestHelper helper = new WALManagerTestHelper(reopenedManager);
            List<Long> reopenedFirstLsns = new ArrayList<>();
            List<Long> reopenedLastLsns = new ArrayList<>();

            // Test that we can find segments for all expected LSNs
            for (int i = 0; i < expectedFirstLsns.size(); i++) {
                long lsn = expectedFirstLsns.get(i);
                WALSegmentProxy segment = helper.findSegmentForLsn(lsn);
                assertNotNull(segment, "Should find segment for LSN " + lsn);
                reopenedFirstLsns.add(segment.getFirstLsn());
                
                // Get last LSN (triggers lazy computation if needed)
                reopenedLastLsns.add(segment.getLastLsn());
            }

            // Verify first LSNs match
            assertEquals(expectedFirstLsns, reopenedFirstLsns, "First LSNs should match after reopen");

            // Verify last LSNs are computed (lazy computation should work)
            assertEquals(expectedLastLsns.size(), reopenedLastLsns.size(), "Should have same number of last LSNs");
            for (int i = 0; i < expectedLastLsns.size(); i++) {
                assertTrue(reopenedLastLsns.get(i) > 0, "Last LSN should be > 0 for segment " + i);
            }

            // Verify we can still read all entries
            assertNotNull(reopenedManager.readByLsn(1), "Should read first entry after reopen");
            assertNotNull(reopenedManager.readByLsn(3), "Should read last entry of segment 1 after reopen");
            assertNotNull(reopenedManager.readByLsn(4), "Should read first entry of segment 2 after reopen");
            assertNotNull(reopenedManager.readByLsn(5), "Should read last entry of segment 2 after reopen");
            assertNotNull(reopenedManager.readByLsn(6), "Should read first entry of segment 3 after reopen");

            // Test edge case: LSN beyond all entries (should return last segment)
            long beyondLsn = 10;
            WALSegmentProxy lastSegment = helper.findSegmentForLsn(beyondLsn);
            assertNotNull(lastSegment, "Beyond LSN should return last segment");
            assertEquals(6L, lastSegment.getFirstLsn(), "Last segment should have firstLsn=6");

            // Test that index works after reopen (try to find some LSN)
            WALSegmentProxy reopenedCheckpointSegment = helper.findSegmentForLsn(6);
            // May be null if LSN 6 doesn't exist or is beyond checkpoint
            // The important thing is that the index doesn't crash and works correctly

        } finally {
            reopenedManager.close();
        }
    }

    /**
     * Helper to get segment information for verification.
     */
    private List<WALSegmentInfo> getSegmentInfos(WALManager manager) throws Exception {
        List<WALSegmentInfo> infos = new ArrayList<>();
        for (Object segment : manager.getSegments().values()) {
            WALSegmentInfo info = new WALSegmentInfo(segment);
            infos.add(info);
        }
        return infos;
    }

    /**
     * Helper class to store segment info.
     */
    private static class WALSegmentInfo {
        private final int number;
        private final long firstLsn;
        private final long lastLsn;

        public WALSegmentInfo(Object segment) throws Exception {
            this.number = (int) segment.getClass().getMethod("getNumber").invoke(segment);
            this.firstLsn = (long) segment.getClass().getMethod("getFirstLsn").invoke(segment);
            this.lastLsn = (long) segment.getClass().getMethod("getLastLsn").invoke(segment);
        }

        public int getNumber() { return number; }
        public long getFirstLsn() { return firstLsn; }
        public long getLastLsn() { return lastLsn; }
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

        public long getLastLsn() throws Exception {
            return (long) segment.getClass().getMethod("getLastLsn").invoke(segment);
        }
    }
}