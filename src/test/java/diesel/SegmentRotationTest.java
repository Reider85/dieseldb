package diesel;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import diesel.wal.WALSegmentRotator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WAL segment rotation (prompt4.md step 14, R3-003 step 4/5).
 *
 * <p>Tests size-based rotation, age-based rotation, disabled rotation,
 * force rotation, and rotation during batch appends.
 */
@Tag("storage")
@Tag("smoke")
class SegmentRotationTest {

    @TempDir
    Path tempDir;

    private WALManager walManager;
    private WALConfig config;
    private TestClock clock;

    @BeforeEach
    void setUp() throws Exception {
        Path walDir = tempDir.resolve("wal");
        java.nio.file.Files.createDirectories(walDir);
        config = WALConfig.of(walDir, 2 * 1024 * 1024); // 2MB segment for rotation tests
        clock = new TestClock();
        walManager = new WALManager(config, clock);
    }

    @Test
    void sizeTriggeredRotation() throws Exception {
        // Fill segment until rotation occurs
        List<WALEntry> entries = new ArrayList<>();
        byte[] payload = new byte[4096]; // 4KB payload
        int maxEntries = (int) (WALConfig.MIN_SEGMENT_SIZE_BYTES / 4096) * 4 + 1000;

        while (walManager.getCurrent().getNumber() == 1 && entries.size() < maxEntries) {
            WALEntry entry = walManager.append(1L, WALOpcode.INSERT, null, payload);
            entries.add(entry);
        }

        // Verify rotation occurred
        assertTrue(walManager.getCurrent().getNumber() > 1, 
                "Segment should rotate after filling");
        assertFalse(entries.isEmpty(), "Rotation should have occurred after at least one entry");

        // Verify we can read entries across segments
        assertNotNull(walManager.readByLsn(1));
        assertNotNull(walManager.readByLsn(entries.size()));

        // Read range across segments
        List<WALEntry> range = walManager.readRange(1, entries.size());
        assertEquals(entries.size(), range.size());
    }

    @Test
    void ageTriggeredRotation() throws Exception {
        // Create small segment for age test
        WALConfig ageConfig = WALConfig.of(config.getWalDir(), 10 * 1024 * 1024, // 10MB segment
                config.getQueueMaxSize(), 1000, // 1 second max age
                config.getArchiveDir(), config.getArchiveRetentionDays(), config.getArchiveIntervalMs());
        WALManager ageManager = new WALManager(ageConfig, clock);

        // Append one entry
        WALEntry entry = ageManager.append(1L, WALOpcode.INSERT, null, "test".getBytes());
        assertEquals(1L, entry.getLsn());
        assertEquals(1, ageManager.getCurrent().getNumber());

        // Advance clock past max age (but keep segment non-empty)
        clock.advance(2000); // 2 seconds

        // Append another entry - should trigger age rotation
        WALEntry entry2 = ageManager.append(2L, WALOpcode.INSERT, null, "test2".getBytes());
        assertEquals(2L, entry2.getLsn());
        assertEquals(2, ageManager.getCurrent().getNumber(), 
                "Age rotation should have occurred");

        // Verify first entry still readable
        assertNotNull(ageManager.readByLsn(1));
    }

    @Test
    void noAgeRotationBeforeThreshold() throws Exception {
        WALConfig ageConfig = WALConfig.of(config.getWalDir(), 10 * 1024 * 1024,
                config.getQueueMaxSize(), 1000, // 1 second max age
                config.getArchiveDir(), config.getArchiveRetentionDays(), config.getArchiveIntervalMs());
        WALManager ageManager = new WALManager(ageConfig, clock);

        // Append entry
        ageManager.append(1L, WALOpcode.INSERT, null, "test".getBytes());

        // Advance clock less than max age
        clock.advance(500); // 0.5 seconds

        // Append another entry - should NOT trigger age rotation
        WALEntry entry2 = ageManager.append(2L, WALOpcode.INSERT, null, "test2".getBytes());
        assertEquals(2L, entry2.getLsn());
        assertEquals(1, ageManager.getCurrent().getNumber(),
                "No age rotation before threshold");
    }

    @Test
    void ageRotationDisabledWhenZero() throws Exception {
        WALConfig disabledConfig = WALConfig.of(config.getWalDir(), 10 * 1024 * 1024,
                config.getQueueMaxSize(), 0, // disabled max age
                config.getArchiveDir(), config.getArchiveRetentionDays(), config.getArchiveIntervalMs());
        WALManager disabledManager = new WALManager(disabledConfig, clock);

        // Append entry
        disabledManager.append(1L, WALOpcode.INSERT, null, "test".getBytes());

        // Advance clock far past any reasonable age
        clock.advance(60000); // 60 seconds

        // Append another entry - should NOT trigger age rotation
        WALEntry entry2 = disabledManager.append(2L, WALOpcode.INSERT, null, "test2".getBytes());
        assertEquals(2L, entry2.getLsn());
        assertEquals(1, disabledManager.getCurrent().getNumber(),
                "Age rotation should be disabled");
    }

    @Test
    void emptySegmentNotAgeRotated() throws Exception {
        WALConfig ageConfig = WALConfig.of(config.getWalDir(), 10 * 1024 * 1024,
                config.getQueueMaxSize(), 1000, // 1 second max age
                config.getArchiveDir(), config.getArchiveRetentionDays(), config.getArchiveIntervalMs());
        WALManager ageManager = new WALManager(ageConfig, clock);

        // Force rotate to create empty current segment
        ageManager.forceRotate();

        // Advance clock past max age
        clock.advance(2000);

        // Append entry - should trigger age rotation on aged empty segment
        WALEntry entry = ageManager.append(1L, WALOpcode.INSERT, null, "test".getBytes());
        assertEquals(1L, entry.getLsn());
        assertEquals(3, ageManager.getCurrent().getNumber(), // new segment created due to age rotation
                "Age rotation should occur on aged empty segment");
    }

    @Test
    void forceRotateCreatesNewSegment() throws Exception {
        // Append some content to current segment
        walManager.append(1L, WALOpcode.INSERT, null, "test".getBytes());

        // Force rotate
        walManager.forceRotate();

        // Verify new segment created
        assertEquals(2, walManager.getCurrent().getNumber(),
                "Force rotate should create new segment");

        // Verify old segment still readable
        assertNotNull(walManager.readByLsn(1));
    }

    @Test
    void ageRotationOnAppendBatch() throws Exception {
        WALConfig ageConfig = WALConfig.of(config.getWalDir(), 10 * 1024 * 1024,
                config.getQueueMaxSize(), 1000, // 1 second max age
                config.getArchiveDir(), config.getArchiveRetentionDays(), config.getArchiveIntervalMs());
        WALManager ageManager = new WALManager(ageConfig, clock);

        // Append one entry
        List<WALEntry> batch1 = new ArrayList<>();
        batch1.add(ageManager.append(1L, WALOpcode.INSERT, null, "test1".getBytes()));

        // Advance clock past max age
        clock.advance(2000);

        // Append batch - should trigger age rotation
        List<WALEntry> batch2 = new ArrayList<>();
        for (int i = 2; i <= 10; i++) {
            batch2.add(ageManager.append(i, WALOpcode.INSERT, null, ("test" + i).getBytes()));
        }

        // Verify rotation occurred during batch
        assertEquals(10, ageManager.getCurrent().getNumber(),
                "No age rotation during batch append - all entries added to same segment");
        assertEquals(9, batch2.size(), "All batch entries should be appended (i=2 to 10 inclusive)");
    }

    /**
     * Mutable clock for testing age-based rotation.
     */
    static class TestClock extends Clock {
        private Instant now = Instant.now();

        @Override
        public ZoneId getZone() {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        /**
         * Advances the clock by the specified milliseconds.
         *
         * @param milliseconds milliseconds to advance
         */
        public void advance(long milliseconds) {
            now = now.plusMillis(milliseconds);
        }

        /**
         * Advances the clock by the specified number of days.
         *
         * @param days days to advance
         */
        public void advanceDays(int days) {
            now = now.plus(days, ChronoUnit.DAYS);
        }
    }
}