package diesel;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import diesel.wal.WALFormatException;
import diesel.wal.InvalidCRCException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WALManager (prompt4.md step 12, R3-003 step 2/5).
 *
 * <p>Tests segment management, LSN allocation, checkpoint persistence,
 * and restart recovery.
 */
@Tag("storage")
@Tag("smoke")
class WALManagerTest {

    @TempDir
    Path tempDir;

    private WALManager walManager;
    private WALConfig config;

    @BeforeEach
    void setUp() throws Exception {
        // Ensure the WAL directory exists and is clean
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        config = WALConfig.of(walDir, 2 * 1024 * 1024); // 2MB segment for rotation tests
        walManager = new WALManager(config);
    }

    @Test
    void appendAndReadBackByLsn() throws Exception {
        // Append 10k entries
        List<WALEntry> entries = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            // Payload is named by LSN: the n-th append gets LSN n, so data = "test-data-n"
            byte[] data = ("test-data-" + (i + 1)).getBytes();
            WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, data);
            entries.add(entry);
            
            // Verify LSN is monotonic
            if (i > 0) {
                assertTrue(entry.getLsn() > entries.get(i - 1).getLsn());
            }
        }

        // Verify last LSN
        assertEquals(10000L, walManager.getLastLsn());

        // Read back some entries by LSN
        WALEntry entry500 = walManager.readByLsn(500);
        assertNotNull(entry500);
        assertEquals(500L, entry500.getLsn());
        assertEquals(WALOpcode.INSERT, entry500.getOp());
        assertEquals("test-data-500", new String(entry500.getAfterImage()));

        // Read first and last entries
        WALEntry first = walManager.readByLsn(1);
        assertNotNull(first);
        assertEquals(1L, first.getLsn());

        WALEntry last = walManager.readByLsn(10000);
        assertNotNull(last);
        assertEquals(10000L, last.getLsn());

        // Read all entries
        List<WALEntry> all = walManager.readAll();
        assertEquals(10000, all.size());
        assertEquals(1L, all.get(0).getLsn());
        assertEquals(10000L, all.get(9999).getLsn());
    }

    @Test
    void segmentRotationBySize() throws Exception {
        // Separate WAL directory so this manager does not share segments with setUp's manager
        Path rotationDir = tempDir.resolve("rotation");
        Files.createDirectories(rotationDir);
        // Smallest allowed segment size (MIN_SEGMENT_SIZE_BYTES = 1MB); 4KB payloads fill it fast
        WALConfig smallConfig = WALConfig.of(rotationDir, diesel.wal.WALConfig.MIN_SEGMENT_SIZE_BYTES);
        try (WALManager smallManager = new WALManager(smallConfig)) {
            byte[] payload = new byte[4096];
            int maxEntries = (int) (WALConfig.MIN_SEGMENT_SIZE_BYTES / 4096) * 4 + 1000;

            List<WALEntry> entries = new ArrayList<>();
            while (smallManager.getCurrent().getNumber() == 1 && entries.size() < maxEntries) {
                WALEntry entry = smallManager.append(1L, WALOpcode.INSERT, null, payload);
                entries.add(entry);
            }

            // Verify rotation occurred
            assertEquals(2, smallManager.getCurrent().getNumber(),
                    "Segment should rotate once the size limit is reached");
            assertTrue(entries.size() > 0, "Rotation should have occurred after at least one entry");

            // Verify we can read entries across segments
            assertNotNull(smallManager.readByLsn(1));
            assertNotNull(smallManager.readByLsn(entries.size()));

            // Read range across segments
            List<WALEntry> range = smallManager.readRange(1, entries.size());
            assertEquals(entries.size(), range.size());
        }
    }

    @Test
    void restartRecoversCurrentSegmentAndLsn() throws Exception {
        // Append some entries
        for (int i = 1; i <= 1000; i++) {
            walManager.append(i, WALOpcode.COMMIT, null, null);
        }
        long lastLsn = walManager.getLastLsn();

        // Close and reopen
        walManager.close();
        walManager = new WALManager(config);

        // Verify recovery
        assertEquals(lastLsn, walManager.getLastLsn());
        assertEquals(1, walManager.getCurrent().getNumber()); // Should be back to first segment

        // Verify we can read entries
        WALEntry entry500 = walManager.readByLsn(500);
        assertNotNull(entry500);
        assertEquals(500L, entry500.getLsn());
        assertEquals(WALOpcode.COMMIT, entry500.getOp());

        // Verify we can append more
        WALEntry newEntry = walManager.append(1001L, WALOpcode.INSERT, null, "new-data".getBytes());
        assertEquals(lastLsn + 1, newEntry.getLsn());
        assertEquals(lastLsn + 1, walManager.getLastLsn());
    }

    @Test
    void checkpointPtrPersistedOnClose() throws Exception {
        // Append some entries
        walManager.append(1L, WALOpcode.COMMIT, null, null);
        walManager.append(2L, WALOpcode.COMMIT, null, null);
        long lastLsn = walManager.getLastLsn();

        // Close (should persist checkpoint.ptr)
        walManager.close();

        // Verify checkpoint.ptr exists
        Path checkpointFile = config.getWalDir().resolve("checkpoint.ptr");
        assertTrue(Files.exists(checkpointFile));

        // Reopen and verify recovery
        walManager = new WALManager(config);
        assertEquals(lastLsn, walManager.getLastLsn());
    }

    @Test
    void checkpointPtrMissingRecovery() throws Exception {
        // Append entries and manually delete checkpoint.ptr
        walManager.append(1L, WALOpcode.COMMIT, null, null);
        walManager.append(2L, WALOpcode.COMMIT, null, null);
        long lastLsn = walManager.getLastLsn();

        Path checkpointFile = config.getWalDir().resolve("checkpoint.ptr");
        Files.deleteIfExists(checkpointFile);

        // Reopen - should recover from segments
        walManager.close();
        walManager = new WALManager(config);

        assertEquals(lastLsn, walManager.getLastLsn());
    }

    @Test
    void segmentHeaderRejectsBadMagic() throws Exception {
        // Append an entry to create a valid segment
        walManager.append(1L, WALOpcode.COMMIT, null, null);

        // Corrupt the segment header magic
        Path segmentFile = config.getWalDir().resolve("wal-0001.log");
        byte[] content = Files.readAllBytes(segmentFile);
        content[0] = (byte) 0xFF; // Corrupt magic
        Files.write(segmentFile, content);

        // Try to reopen - should fail
        assertThrows(WALFormatException.class, () -> {
            WALManager newManager = new WALManager(config);
            newManager.close();
        });
    }

    @Test
    void tornTailStopsCleanly() throws Exception {
        // Append entries
        for (int i = 0; i < 100; i++) {
            walManager.append(1L, WALOpcode.COMMIT, null, null);
        }

        // Truncate the file to simulate torn write
        Path segmentFile = config.getWalDir().resolve("wal-0001.log");
        long validSize = walManager.getCurrent().getSize();
        Files.write(segmentFile, Arrays.copyOf(Files.readAllBytes(segmentFile), (int) (validSize - 50)));

        // Reopen and read - should stop at torn tail
        walManager.close();
        walManager = new WALManager(config);

        List<WALEntry> entries = walManager.readAll();
        assertTrue(entries.size() < 100, "Should stop at torn tail");
    }

    @Test
    void allocateLsnMonotonic() throws Exception {
        // Test direct allocation
        long lsn1 = walManager.allocateLsn();
        long lsn2 = walManager.allocateLsn();
        long lsn3 = walManager.allocateLsn();

        assertEquals(1L, lsn1);
        assertEquals(2L, lsn2);
        assertEquals(3L, lsn3);

        // Test append with allocated LSN
        WALEntry entry = new WALEntry(lsn3, 1L, WALOpcode.COMMIT, null, null);
        walManager.append(entry);
        assertEquals(lsn3, walManager.getLastLsn());
    }

    @Test
    void readRangeAcrossSegments() throws Exception {
        // Append enough entries to potentially trigger rotation
        for (int i = 1; i <= 5000; i++) {
            walManager.append(i, WALOpcode.COMMIT, null, null);
        }

        // Read range across potential segment boundaries
        List<WALEntry> range = walManager.readRange(1000, 4000);
        assertEquals(3001, range.size());
        assertEquals(1000L, range.get(0).getLsn());
        assertEquals(4000L, range.get(3000).getLsn());
    }

    @Test
    void configDefaultsAndSyspropOverride() throws Exception {
        // Test defaults
        WALConfig defaultConfig = WALConfig.fromConfig();
        assertEquals(64 * 1024 * 1024, defaultConfig.getMaxSegmentSizeBytes());
        assertTrue(defaultConfig.getWalDir().toString().contains("wal"));

        // Test system property override (keys must match WALConfig.DIR_KEY / SEGMENT_MAX_SIZE_MB_KEY)
        Path customDirPath = tempDir.resolve("custom-wal");
        String customDir = customDirPath.toString();
        System.setProperty(WALConfig.DIR_KEY, customDir);
        System.setProperty(WALConfig.SEGMENT_MAX_SIZE_MB_KEY, "16");
        try {
            WALConfig overrideConfig = WALConfig.fromConfig();
            assertEquals(16 * 1024 * 1024, overrideConfig.getMaxSegmentSizeBytes());
            assertEquals(customDirPath.toAbsolutePath().normalize(), overrideConfig.getWalDir());
        } finally {
            // Clean up even if an assert fails, so other tests are unaffected
            System.clearProperty(WALConfig.DIR_KEY);
            System.clearProperty(WALConfig.SEGMENT_MAX_SIZE_MB_KEY);
        }
    }

    @Test
    void appendWithPreAssignedLsn() throws Exception {
        // Append normally
        walManager.append(1L, WALOpcode.COMMIT, null, null);

        // Append with pre-assigned LSN (must be > last)
        long customLsn = walManager.getLastLsn() + 10;
        WALEntry customEntry = new WALEntry(customLsn, 2L, WALOpcode.INSERT, null, "custom".getBytes());
        walManager.append(customEntry);

        assertEquals(customLsn, walManager.getLastLsn());

        // Verify we can read it
        WALEntry readBack = walManager.readByLsn(customLsn);
        assertNotNull(readBack);
        assertEquals(customLsn, readBack.getLsn());
    }

    @Test
    void appendRejectsNonIncreasingLsn() throws Exception {
        // Append first entry
        walManager.append(1L, WALOpcode.COMMIT, null, null);

        // Try to append with LSN <= last
        WALEntry badEntry = new WALEntry(1L, 2L, WALOpcode.INSERT, null, "bad".getBytes());
        assertThrows(IllegalArgumentException.class, () -> walManager.append(badEntry));
    }
}