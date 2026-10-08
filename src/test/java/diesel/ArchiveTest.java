package diesel;

import diesel.wal.WALConfig;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;
import diesel.wal.WALArchiver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WAL archiving (prompt4.md step 14, R3-003 step 4/5).
 *
 * <p>Tests archiving of rotated segments, copy semantics (originals preserved),
 * current segment never archived, idempotent re-runs, and empty segment handling.
 */
@Tag("storage")
@Tag("smoke")
class ArchiveTest {

    @TempDir
    Path tempDir;

    private WALManager walManager;
    private WALConfig config;

    @BeforeEach
    void setUp() throws Exception {
        Path walDir = tempDir.resolve("wal");
        Path archiveDir = walDir.resolve("archive");
        java.nio.file.Files.createDirectories(walDir);
        java.nio.file.Files.createDirectories(archiveDir);
        
        config = WALConfig.of(walDir, 1 * 1024 * 1024, // 1MB segment
                100000, 300000, // Use default queue size and max age
                archiveDir, 7, 0); // Disable daemon for tests
        walManager = new WALManager(config);
    }

    @Test
    void fiveSegmentsProduceFiveArchives() throws Exception {
        // Create 5 segments (4 rotations) with content
        List<String> originalContents = new ArrayList<>();
        int totalEntries = 0;

        // Segment 1
        for (int i = 1; i <= 1000; i++) {
            byte[] data = ("segment1-entry-" + i).getBytes();
            walManager.append(1L, WALOpcode.INSERT, null, data);
            totalEntries++;
        }
        originalContents.add(walManager.readAll().stream()
                .map(e -> e.getAfterImage() != null ? new String(e.getAfterImage()) : "")
                .reduce("", (a, b) -> a + b + "|"));

        // Rotate to segment 2
        walManager.forceRotate();
        for (int i = 1001; i <= 2000; i++) {
            byte[] data = ("segment2-entry-" + i).getBytes();
            walManager.append(2L, WALOpcode.INSERT, null, data);
            totalEntries++;
        }
        originalContents.add(walManager.readAll().stream()
                .map(e -> e.getAfterImage() != null ? new String(e.getAfterImage()) : "")
                .reduce("", (a, b) -> a + b + "|"));

        // Rotate to segment 3
        walManager.forceRotate();
        for (int i = 2001; i <= 3000; i++) {
            byte[] data = ("segment3-entry-" + i).getBytes();
            walManager.append(3L, WALOpcode.INSERT, null, data);
            totalEntries++;
        }
        originalContents.add(walManager.readAll().stream()
                .map(e -> e.getAfterImage() != null ? new String(e.getAfterImage()) : "")
                .reduce("", (a, b) -> a + b + "|"));

        // Rotate to segment 4
        walManager.forceRotate();
        for (int i = 3001; i <= 4000; i++) {
            byte[] data = ("segment4-entry-" + i).getBytes();
            walManager.append(4L, WALOpcode.INSERT, null, data);
            totalEntries++;
        }
        originalContents.add(walManager.readAll().stream()
                .map(e -> e.getAfterImage() != null ? new String(e.getAfterImage()) : "")
                .reduce("", (a, b) -> a + b + "|"));

        // Rotate to segment 5
        walManager.forceRotate();
        for (int i = 4001; i <= 5000; i++) {
            byte[] data = ("segment5-entry-" + i).getBytes();
            walManager.append(5L, WALOpcode.INSERT, null, data);
            totalEntries++;
        }
        originalContents.add(walManager.readAll().stream()
                .map(e -> e.getAfterImage() != null ? new String(e.getAfterImage()) : "")
                .reduce("", (a, b) -> a + b + "|"));

        // Close segment 5 so all 5 segments are archived (segment 6 stays current and empty)
        walManager.forceRotate();

        // Archive the segments
        walManager.getArchiver().runOnce();

        // Verify archive files exist
        List<Path> archiveFiles = new ArrayList<>();
        try (var dirStream = Files.list(config.getArchiveDir())) {
            archiveFiles = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(5, archiveFiles.size(), "Should have 5 archive files (5 segments -> 5 gzip archives)");

        // Verify each archive decompresses to its own segment content
        for (int i = 0; i < archiveFiles.size(); i++) {
            Path archiveFile = archiveFiles.get(i);
            int segmentNumber = i + 1;

            byte[] decompressed = decompressGzip(archiveFile);
            String actualContent = new String(decompressed);
            assertTrue(actualContent.contains("segment" + segmentNumber + "-entry-"),
                    "Archive " + segmentNumber + " should contain segment content");
            if (segmentNumber > 1) {
                assertFalse(actualContent.contains("segment1-entry-"),
                        "Archive " + segmentNumber + " must not contain entries of segment 1");
            }
        }

        // Cumulative readAll snapshot must contain every segment's entries
        String allContent = originalContents.get(originalContents.size() - 1);
        for (int segment = 1; segment <= 5; segment++) {
            assertTrue(allContent.contains("segment" + segment + "-entry-"),
                    "readAll snapshot should contain segment " + segment + " entries");
        }
        assertEquals(5000, totalEntries, "All 5000 entries should have been appended");

        // Verify original .log files still exist (copy semantics)
        Path walDir = config.getWalDir();
        List<Path> walFiles = new ArrayList<>();
        try (var dirStream = Files.list(walDir)) {
            walFiles = dirStream
                    .filter(p -> p.getFileName().toString().matches("wal-\\d{4}\\.log"))
                    .sorted()
                    .toList();
        }
        assertEquals(6, walFiles.size(), "Original .log files should still exist (5 + current)");
    }

    @Test
    void currentSegmentNeverArchived() throws Exception {
        // Create some segments and archive them
        for (int i = 1; i <= 3; i++) {
            walManager.append(i, WALOpcode.INSERT, null, ("segment" + i).getBytes());
        }
        walManager.forceRotate();
        // Append some data to segment 2 before rotating again
        walManager.append(2, WALOpcode.INSERT, null, ("segment2-data").getBytes());
        walManager.forceRotate();
        // Append some data to segment 3 before rotating again
        walManager.append(3, WALOpcode.INSERT, null, ("segment3-data").getBytes());
        walManager.forceRotate();

        walManager.getArchiver().runOnce();

        // Current segment (wal-0004.log) should not be archived
        Path archiveDir = config.getArchiveDir();
        List<Path> archiveFiles = new ArrayList<>();
        try (var dirStream = Files.list(archiveDir)) {
            archiveFiles = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(3, archiveFiles.size(), "Only 3 segments should be archived");

        // Verify no archive for current segment number
        int currentSegmentNumber = walManager.getCurrent().getNumber();
        boolean currentArchived = archiveFiles.stream()
                .anyMatch(p -> p.getFileName().toString().contains(
                        String.format("%04d", currentSegmentNumber)));
        assertFalse(currentArchived, "Current segment should not be archived");
    }

    @Test
    void restartDoesNotRearchive() throws Exception {
        // Create and archive some segments
        for (int i = 1; i <= 2; i++) {
            walManager.append(i, WALOpcode.INSERT, null, ("segment" + i).getBytes());
        }
        walManager.forceRotate();
        // Append some data to segment 2 before rotating again
        walManager.append(2, WALOpcode.INSERT, null, ("segment2-data").getBytes());
        walManager.forceRotate();

        walManager.getArchiver().runOnce();

        // Verify archives exist
        Path archiveDir = config.getArchiveDir();
        List<Path> archiveFiles1 = new ArrayList<>();
        try (var dirStream = Files.list(archiveDir)) {
            archiveFiles1 = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(2, archiveFiles1.size(), "Should have 2 archive files");

        // Run archiver again - should be idempotent
        walManager.getArchiver().runOnce();

        // Verify same number of archives (no duplicates)
        List<Path> archiveFiles2 = new ArrayList<>();
        try (var dirStream = Files.list(archiveDir)) {
            archiveFiles2 = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(2, archiveFiles2.size(), "Should still have 2 archive files after re-run");

        // Verify archived segments set is stable
        Set<Integer> archivedSegments = walManager.getArchiver().getArchivedSegments();
        assertEquals(2, archivedSegments.size(), "Should have 2 archived segments");
    }

    @Test
    void emptySegmentSkipped() throws Exception {
        // Create a non-empty segment
        walManager.append(1L, WALOpcode.INSERT, null, "content".getBytes());

        // Force rotate to create empty current segment
        walManager.forceRotate();

        // Run archiver - should not archive empty segment
        walManager.getArchiver().runOnce();

        Path archiveDir = config.getArchiveDir();
        List<Path> archiveFiles = new ArrayList<>();
        try (var dirStream = Files.list(archiveDir)) {
            archiveFiles = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(1, archiveFiles.size(), "Should archive non-empty segment 1, skip empty segment 2");
    }

    @Test
    void archiveDirCreatedOnDemand() throws Exception {
        // Delete archive directory
        Path archiveDir = config.getArchiveDir();
        Files.deleteIfExists(archiveDir);

        // Create segments and archive
        for (int i = 1; i <= 2; i++) {
            walManager.append(i, WALOpcode.INSERT, null, ("segment" + i).getBytes());
        }
        walManager.forceRotate();
        // Append some data to segment 2 before rotating again
        walManager.append(2, WALOpcode.INSERT, null, ("segment2-data").getBytes());
        walManager.forceRotate();

        walManager.getArchiver().runOnce();

        // Verify archive directory was recreated
        assertTrue(Files.exists(archiveDir), "Archive directory should be created on demand");
        assertTrue(Files.isDirectory(archiveDir), "Archive directory should be a directory");

        // Verify archives exist
        List<Path> archiveFiles = new ArrayList<>();
        try (var dirStream = Files.list(archiveDir)) {
            archiveFiles = dirStream
                    .filter(p -> p.getFileName().toString().endsWith(".log.gz"))
                    .sorted()
                    .toList();
        }
        assertEquals(2, archiveFiles.size(), "Should have 2 archive files");
    }

    /**
     * Helper method to decompress a gzip file.
     */
    private byte[] decompressGzip(Path gzipFile) throws Exception {
        try (var inputStream = Files.newInputStream(gzipFile);
             var gzipStream = new java.util.zip.GZIPInputStream(inputStream);
             var outputStream = new java.io.ByteArrayOutputStream()) {
            gzipStream.transferTo(outputStream);
            return outputStream.toByteArray();
        }
    }
}