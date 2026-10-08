package diesel.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WAL archiver with retention policy (prompt4.md step 14, R3-003 step 4/5).
 *
 * <p>Archives closed WAL segments to gzip files in the archive directory and cleans up
 * old archives based on retention policy. Runs on a background daemon thread with
 * configurable interval. Copy semantics: original .log files are preserved.
 *
 * <p>Thread-safety: thread-safe (daemon thread + safe file operations).
 */
public final class WALArchiver implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALArchiver.class);

    /** Daemon thread name. */
    static final String THREAD_NAME = "diesel-wal-archiver";
    /** GZIP file extension. */
    static final String GZIP_EXTENSION = ".gz";
    /** Temporary file extension for atomic writes. */
    static final String TEMP_EXTENSION = ".tmp";

    private final WALConfig config;
    private final WALManager manager;
    private final Clock clock;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Set<Integer> archivedSegments = Collections.synchronizedSet(new TreeSet<>());

    /**
     * Creates a new WAL archiver.
     *
     * @param config the WAL configuration
     * @param manager the WAL manager for current segment number queries
     * @param clock the clock for retention calculations (for tests: injectable)
     */
    public WALArchiver(WALConfig config, WALManager manager, Clock clock) {
        this.config = config;
        this.manager = manager;
        this.clock = clock;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Creates a WAL archiver with system clock.
     *
     * @param config the WAL configuration
     * @param manager the WAL manager
     * @return the archiver
     */
    public static WALArchiver create(WALConfig config, WALManager manager) {
        return new WALArchiver(config, manager, Clock.systemUTC());
    }

    /**
     * Creates a WAL archiver with the given clock (for testing).
     *
     * @param config the WAL configuration
     * @param manager the WAL manager
     * @param clock the clock for retention calculations
     * @return the archiver
     */
    public static WALArchiver create(WALConfig config, WALManager manager, Clock clock) {
        return new WALArchiver(config, manager, clock);
    }

    /**
     * Starts the background archiving daemon if enabled.
     */
    public void start() {
        if (config.getArchiveIntervalMs() <= 0) {
            LOGGER.info("Archive daemon disabled (interval = {}ms)", config.getArchiveIntervalMs());
            return;
        }

        executor.scheduleAtFixedRate(
                this::runOnce,
                config.getArchiveIntervalMs(),
                config.getArchiveIntervalMs(),
                TimeUnit.MILLISECONDS);
        LOGGER.info("Archive daemon started, interval = {}ms", config.getArchiveIntervalMs());
    }

    /**
     * Runs one archiving cycle: archive new segments and clean up old archives.
     * This method is safe to call manually (e.g., in tests).
     */
    public void runOnce() {
        if (!running.get()) {
            return;
        }

        try {
            LOGGER.debug("Running archive cycle");
            archiveNewSegments();
            runRetention();
        } catch (Exception e) {
            LOGGER.error("Archive cycle failed", e);
        }
    }

    /**
     * Archives all new closed segments that haven't been archived yet.
     */
    private void archiveNewSegments() throws IOException {
        Path walDir = config.getWalDir();
        Path archiveDir = config.getArchiveDir();
        int currentSegmentNumber = manager.getCurrent().getNumber();

        // Ensure archive directory exists
        Files.createDirectories(archiveDir);

        // Find all WAL segment files
        List<Path> walFiles;
        try (var dirStream = Files.list(walDir)) {
            walFiles = dirStream
                    .filter(p -> p.getFileName().toString().matches("wal-\\d{4}\\.log"))
                    .sorted()
                    .toList();
        }

        for (Path walFile : walFiles) {
            String fileName = walFile.getFileName().toString();
            int segmentNumber = Integer.parseInt(fileName.substring(4, 8)); // Extract from wal-NNNN.log

            // Skip current segment and already archived segments
            if (segmentNumber == currentSegmentNumber || archivedSegments.contains(segmentNumber)) {
                LOGGER.debug("Skipping segment {} (current={} or already archived)", segmentNumber, currentSegmentNumber);
                continue;
            }
            
            LOGGER.debug("Considering segment {} for archiving", segmentNumber);

            // Skip empty segments (0 bytes or just header)
            long fileSize = Files.size(walFile);
            if (fileSize <= WALFormat.SEGMENT_HEADER_SIZE) {
                LOGGER.debug("Skipping empty segment {} ({} bytes)", segmentNumber, fileSize);
                continue;
            }
            
            LOGGER.debug("Archiving segment {} ({} bytes)", segmentNumber, fileSize);

            // Archive the segment
            archiveSegment(walFile, segmentNumber);
        }
    }

    /**
     * Archives a single WAL segment to gzip format.
     *
     * @param walFile the WAL segment file to archive
     * @param segmentNumber the segment number
     * @throws IOException if archiving fails
     */
    private void archiveSegment(Path walFile, int segmentNumber) throws IOException {
        Path archiveFile = config.getArchiveDir().resolve("wal-" + 
                String.format("%04d", segmentNumber) + ".log" + GZIP_EXTENSION);
        Path tempFile = archiveFile.resolveSibling(archiveFile.getFileName() + TEMP_EXTENSION);

        try {
            // Create gzip compressed copy
            try (var input = Files.newInputStream(walFile);
                 var gzipOutput = new java.util.zip.GZIPOutputStream(Files.newOutputStream(tempFile))) {
                input.transferTo(gzipOutput);
            }

            // Verify by decompressing and comparing size
            try (var gzipInput = new GZIPInputStream(Files.newInputStream(tempFile));
                 var verifyOutput = new java.io.ByteArrayOutputStream()) {
                gzipInput.transferTo(verifyOutput);
                
                byte[] originalBytes = Files.readAllBytes(walFile);
                byte[] decompressedBytes = verifyOutput.toByteArray();
                
                if (originalBytes.length != decompressedBytes.length) {
                    throw new IOException("Size mismatch after gzip: original=" + originalBytes.length + 
                            ", decompressed=" + decompressedBytes.length);
                }
            }

            // Atomic rename to final location
            Files.move(tempFile, archiveFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Preserve source segment age: retention is based on archive mtime,
            // which should reflect when the segment was last written, not when
            // the gzip copy was produced.
            Files.setLastModifiedTime(archiveFile, Files.getLastModifiedTime(walFile));

            synchronized (this) {
                archivedSegments.add(segmentNumber);
            }
            
            LOGGER.debug("Archived segment {} to {}", segmentNumber, archiveFile);
        } catch (IOException e) {
            // Clean up temp file on failure
            Files.deleteIfExists(tempFile);
            throw e;
        }
    }

    /**
     * Stops the background daemon and shuts down the executor.
     */
    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOGGER.debug("Archive daemon stopped");
        }
    }

    /**
     * Runs the retention policy: deletes archive files whose last-modified time
     * is older than {@code wal.archive.retention.days}. Called after every archive
     * cycle and safe to call manually (e.g., in tests).
     *
     * <p>Retention deletes only the gzip copies; the in-memory archived-segments set is
     * intentionally left untouched so purged segments are never re-archived.
     * When retention is disabled ({@code retentionDays <= 0}) nothing is deleted.
     */
    public synchronized void runRetention() {
        if (config.getArchiveRetentionDays() <= 0) {
            LOGGER.debug("Retention disabled (retentionDays <= 0)");
            return;
        }

        long retentionMs = config.getArchiveRetentionDays() * 24L * 60L * 60L * 1000L;
        long cutoffTime = clock.instant().toEpochMilli() - retentionMs;

        try {
            List<Path> archivesToDelete = new ArrayList<>();
            try (var dirStream = Files.list(config.getArchiveDir())) {
                dirStream
                    .filter(p -> p.getFileName().toString().endsWith(GZIP_EXTENSION))
                    .forEach(archiveFile -> {
                        try {
                            long fileTime = Files.getLastModifiedTime(archiveFile).toMillis();
                            if (fileTime < cutoffTime) {
                                archivesToDelete.add(archiveFile);
                            }
                        } catch (IOException e) {
                            LOGGER.warn("Failed to get modification time for {}: {}", archiveFile, e.getMessage());
                        }
                    });
            }

            // Delete expired archives
            for (Path archiveFile : archivesToDelete) {
                try {
                    Files.deleteIfExists(archiveFile);
                    LOGGER.debug("Deleted expired archive: {}", archiveFile.getFileName());
                } catch (IOException e) {
                    LOGGER.warn("Failed to delete expired archive {}: {}", archiveFile, e.getMessage());
                }
            }

            LOGGER.debug("Retention completed: deleted {} expired archives", archivesToDelete.size());
        } catch (IOException e) {
            LOGGER.warn("Retention failed: {}", e.getMessage());
        }
    }

    /**
     * Returns the set of archived segment numbers (for testing).
     *
     * @return immutable copy of archived segment numbers
     */
    public synchronized Set<Integer> getArchivedSegments() {
        return Set.copyOf(archivedSegments);
    }

    /**
     * Clears the archived segments set (for testing).
     */
    public synchronized void clearArchivedSegments() {
        archivedSegments.clear();
    }
}