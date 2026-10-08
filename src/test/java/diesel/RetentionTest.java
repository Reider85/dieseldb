package diesel;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WAL archive retention (prompt4.md step 14, R3-003 step 4/5).
 *
 * <p>Tests retention policy enforcement, expired archive deletion,
 * disabled retention, and retention boundary conditions.
 */
@Tag("storage")
@Tag("smoke")
class RetentionTest {

    @TempDir
    Path tempDir;

    private WALManager walManager;
    private WALConfig config;
    private TestClock clock;

    @BeforeEach
    void setUp() throws Exception {
        Path walDir = tempDir.resolve("wal");
        Path archiveDir = walDir.resolve("archive");
        java.nio.file.Files.createDirectories(walDir);
        java.nio.file.Files.createDirectories(archiveDir);
        
        config = WALConfig.of(walDir, 1 * 1024 * 1024, // 1MB segment
                100000, 0, // Disable max age for retention tests
                archiveDir, 7, // 7 day retention
                0); // Disable daemon for tests
        clock = new TestClock();
        walManager = new WALManager(config, clock);
    }

    @Test
    void expiredArchivesDeleted() throws Exception {
        // Create archives with different ages
        createArchivesWithAges(clock, 10, 8, 6, 4, 2, 1); // Days ago

        // Verify all archives exist initially
        assertEquals(6, countArchiveFiles(), "Should have 6 archive files initially");

        // Advance clock to trigger retention check
        clock.advanceDays(2); // Effective ages become 12, 10, 8, 6, 4, 3 days

        // Run retention
        walManager.getArchiver().runRetention();

        // Only archives younger than the 7-day retention window remain
        // (effective ages 6, 4, 3 days -> segments 4, 5, 6)
        assertEquals(3, countArchiveFiles(), "Should have 3 archive files after retention");

        List<String> remainingFiles = listArchiveFiles();
        assertTrue(remainingFiles.stream().anyMatch(f -> f.contains("wal-0004.log.gz")),
                "Segment 4 (effective age 6 days) should remain");
        assertTrue(remainingFiles.stream().anyMatch(f -> f.contains("wal-0005.log.gz")),
                "Segment 5 (effective age 4 days) should remain");
        assertTrue(remainingFiles.stream().anyMatch(f -> f.contains("wal-0006.log.gz")),
                "Segment 6 (effective age 3 days) should remain");
    }

    @Test
    void retentionDisabledWhenZero() throws Exception {
        // Create archives
        createArchivesWithAges(clock, 10, 8, 6, 4, 2, 1);

        // Change config to disable retention
        WALConfig disabledConfig = WALConfig.of(config.getWalDir(), config.getMaxSegmentSizeBytes(),
                config.getQueueMaxSize(), config.getMaxSegmentAgeMs(),
                config.getArchiveDir(), 0, // Disabled retention
                config.getArchiveIntervalMs());
        WALManager disabledManager = new WALManager(disabledConfig, clock);

        // Advance clock past retention period
        clock.advanceDays(12);

        // Run retention - should not delete anything
        disabledManager.getArchiver().runRetention();

        // All archives should remain
        assertEquals(6, countArchiveFiles(), "All archives should remain when retention disabled");
    }

    @Test
    void retentionBoundaryCondition() throws Exception {
        // Create archives exactly at retention boundary
        createArchivesWithAges(clock, 7, 7, 7, 7); // All exactly 7 days old

        // Run retention - should keep all (not delete)
        walManager.getArchiver().runRetention();

        assertEquals(4, countArchiveFiles(), "All archives should be kept at boundary");

        // Advance clock slightly past boundary
        clock.advance(1, ChronoUnit.HOURS);

        // Run retention - should delete all
        walManager.getArchiver().runRetention();

        assertEquals(0, countArchiveFiles(), "All archives should be deleted past boundary");
    }

    @Test
    void retentionWithMixedAges() throws Exception {
        // Create archives with mixed ages
        createArchivesWithAges(clock, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1);

        // Advance clock to trigger retention
        clock.advanceDays(5); // Effective ages become 15..6 days

        // Run retention
        walManager.getArchiver().runRetention();

        // Only archives younger than 7 days remain (effective ages 7 and 6 -> segments 9, 10)
        assertEquals(2, countArchiveFiles(), "Should have 2 archive files after retention");

        List<String> remainingFiles = listArchiveFiles();
        assertTrue(remainingFiles.stream().anyMatch(f -> f.contains("wal-0009.log.gz")),
                "Segment 9 (effective age 7 days, at boundary) should remain");
        assertTrue(remainingFiles.stream().anyMatch(f -> f.contains("wal-0010.log.gz")),
                "Segment 10 (effective age 6 days) should remain");
    }

    @Test
    void retentionWithNonArchiveFiles() throws Exception {
        // Create some archive files
        createArchivesWithAges(clock, 10, 8, 6);

        // Add some non-archive files to the archive directory
        Path extraFile1 = config.getArchiveDir().resolve("extra-file.txt");
        Files.writeString(extraFile1, "This is not an archive file");

        Path extraFile2 = config.getArchiveDir().resolve("wal-0005.log"); // Non-gzipped
        Files.writeString(extraFile2, "This is a non-gzipped log file");

        // Advance clock and run retention (effective ages 11, 9, 7 days)
        clock.advanceDays(1);
        walManager.getArchiver().runRetention();

        // Should only delete expired archives (11 and 9 days), keep the boundary one (7 days)
        assertEquals(1, countArchiveFiles(), "Should have 1 archive file after retention");
        assertTrue(Files.exists(extraFile1), "Non-archive files should remain");
        assertTrue(Files.exists(extraFile2), "Non-gzipped files should remain");
    }

    @Test
    void retentionWithEmptyArchiveDir() throws Exception {
        // Create empty archive directory
        // No archives to test

        // Run retention - should not fail
        walManager.getArchiver().runRetention();

        // Should still be empty
        assertEquals(0, countArchiveFiles(), "Archive directory should remain empty");
    }

    @Test
    void retentionWithNoArchives() throws Exception {
        // Create some WAL segments but don't archive them
        for (int i = 1; i <= 3; i++) {
            walManager.append(i, WALOpcode.INSERT, null, ("segment" + i).getBytes());
        }
        walManager.forceRotate();
        walManager.forceRotate();

        // Don't run archiver, only retention
        walManager.getArchiver().runRetention();

        // No archives to delete
        assertEquals(0, countArchiveFiles(), "No archives should exist");
    }

    @Test
    void retentionPreservesArchivedSegmentsSet() throws Exception {
        // Create and archive some segments
        createArchivesWithAges(clock, 10, 8, 6);

        // Get initial archived segments set
        var archivedSegments1 = walManager.getArchiver().getArchivedSegments();
        assertEquals(3, archivedSegments1.size(), "Should have 3 archived segments");

        // Run retention (all archives expire: effective ages 18, 16, 14 days)
        clock.advanceDays(8);
        walManager.getArchiver().runRetention();
        assertEquals(0, countArchiveFiles(), "All archives should be deleted after retention");

        // The archived-segments set must survive retention: purged segments are
        // never re-archived even though their original .log files still exist.
        var archivedSegments2 = walManager.getArchiver().getArchivedSegments();
        assertEquals(3, archivedSegments2.size(),
                "Retention must not drop entries from the archived segments set");
        assertTrue(archivedSegments2.containsAll(Set.of(1, 2, 3)),
                "Segments 1, 2 and 3 should remain marked as archived");

        // A subsequent archive cycle must not resurrect the purged archives
        walManager.getArchiver().runOnce();
        assertEquals(0, countArchiveFiles(), "Archived segments must not be re-archived after retention");
    }

    /**
     * Helper method to create archives with specific ages.
     *
     * <p>Retention compares the archive file's last-modified time with the clock, so after
     * archiving each gzip copy is backdated to {@code clock.instant() - age days}. This
     * simulates archives of a given age without waiting for real time to pass.
     *
     * @param clock the test clock
     * @param ages days ago to create archives
     */
    private void createArchivesWithAges(TestClock clock, int... ages) throws Exception {
        for (int age : ages) {
            // Set clock to age days ago
            clock.setToNowMinus(age, ChronoUnit.DAYS);

            // Create segment
            for (int i = 1; i <= 100; i++) {
                walManager.append(i, WALOpcode.INSERT, null, ("segment" + age + "-entry-" + i).getBytes());
            }

            // Rotate to next segment
            walManager.forceRotate();
        }

        // Set clock back to current time
        clock.setToNow();

        // Archive all segments
        walManager.getArchiver().runOnce();

        // Backdate the archives so each carries its requested age.
        // Archives are created in segment order, matching the order of `ages`.
        List<Path> archives = listArchivePaths();
        assertEquals(ages.length, archives.size(),
                "Each age should produce exactly one archive (last segment stays current)");
        Instant base = clock.instant();
        for (int i = 0; i < archives.size(); i++) {
            Files.setLastModifiedTime(archives.get(i), FileTime.from(base.minus(ages[i], ChronoUnit.DAYS)));
        }
    }

    private List<Path> listArchivePaths() throws Exception {
        try (var dirStream = Files.list(config.getArchiveDir())) {
            return dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
    }

    private int countArchiveFiles() throws Exception {
        try (var dirStream = Files.list(config.getArchiveDir())) {
            return (int) dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .count();
        }
    }

    private List<String> listArchiveFiles() throws Exception {
        try (var dirStream = Files.list(config.getArchiveDir())) {
            return dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        }
    }

    /**
     * Mutable clock for testing retention based on file age.
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
         * Advances the clock by the specified amount.
         */
        public void advance(long amount, ChronoUnit unit) {
            now = now.plus(amount, unit);
        }

        /**
         * Sets the clock to now minus the specified amount.
         */
        public void setToNowMinus(long amount, ChronoUnit unit) {
            now = Instant.now().minus(amount, unit);
        }

        /**
         * Sets the clock back to current system time.
         */
        public void setToNow() {
            now = Instant.now();
        }

        /**
         * Advances the clock by the specified number of days.
         */
        public void advanceDays(int days) {
            now = now.plus(days, ChronoUnit.DAYS);
        }
    }
}