package diesel.wal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WAL Manager (prompt4.md step 12, R3-003 step 2/5).
 *
 * <p>Manages WAL segment files, LSN allocation, and persistence.
 * Creates/opens segments named {@code wal-NNNN.log} in the WAL directory.
 * Maintains a monotonic LSN allocator and checkpoint persistence.
 *
 * <p>Thread-safety: not thread-safe (single-writer assumption for step 12).
 */
public final class WALManager implements AutoCloseable {

    private final WALConfig config;
    private final Clock clock;
    private final WALSegmentRotator rotator;
    private final WALArchiver archiver;
    private final AtomicLong lastAppendedLsn = new AtomicLong(0);
    private final AtomicLong lastAllocatedLsn = new AtomicLong(0);
    private final NavigableMap<Integer, WALSegment> segments = new TreeMap<>();
    private WALSegment currentSegment;
    private final AtomicInteger currentSegmentNumber = new AtomicInteger(0);
    private int nextSegmentNumber = 1;
    private int persistCounter = 0;

    /**
     * Creates a new WALManager with the given configuration.
     *
     * @param config the WAL configuration
     * @throws IOException if the WAL directory cannot be created or opened
     */
    public WALManager(WALConfig config) throws IOException {
        this(config, Clock.systemUTC());
    }

    /**
     * Creates a WALManager with the given configuration and clock (for testing).
     *
     * @param config the WAL configuration
     * @param clock the clock for age-based rotation and retention
     * @throws IOException if the WAL directory cannot be created or opened
     */
    public WALManager(WALConfig config, Clock clock) throws IOException {
        this.config = config;
        this.clock = clock;
        this.rotator = WALSegmentRotator.create(config, clock);
        this.archiver = WALArchiver.create(config, this, clock);
        
        // Ensure WAL directory exists
        Path walDir = config.getWalDir();
        if (Files.exists(walDir) && !Files.isDirectory(walDir)) {
            throw new IOException("WAL directory path exists but is not a directory: " + walDir);
        }
        Files.createDirectories(walDir);
        
        // Discover existing segments
        discoverExistingSegments();
        
        // Determine next segment number and current segment
        if (!segments.isEmpty()) {
            nextSegmentNumber = segments.lastKey() + 1;
            currentSegment = segments.lastEntry().getValue();
        } else {
            // Create first segment
            currentSegment = WALSegment.create(config.getWalDir(), nextSegmentNumber);
            segments.put(nextSegmentNumber, currentSegment);
            nextSegmentNumber++;
        }
        
        // Update current segment number for archiver
        currentSegmentNumber.set(currentSegment.getNumber());
        
        // Recover LSN from checkpoint.ptr and segments
        recoverLsn();
        
        // Start archiver daemon
        archiver.start();
    }

    /**
     * Creates a WALManager with default configuration.
     *
     * @return the WAL manager
     * @throws IOException if initialization fails
     */
    public static WALManager openDefault() throws IOException {
        return new WALManager(WALConfig.fromConfig());
    }

    /**
     * Discovers existing WAL segment files in the WAL directory.
     *
     * @throws IOException if segments cannot be opened
     */
    private void discoverExistingSegments() throws IOException {
        List<Path> walFiles;
        try (var dirStream = Files.list(config.getWalDir())) {
            walFiles = dirStream
                .filter(p -> p.getFileName().toString().matches("wal-\\d{4}\\.log"))
                .sorted()
                .collect(Collectors.toList());
        }

        int lastSegmentNumber = walFiles.isEmpty() ? -1
                : Integer.parseInt(walFiles.get(walFiles.size() - 1).getFileName().toString().substring(4, 8));
        for (Path filePath : walFiles) {
            String fileName = filePath.getFileName().toString();
            int segmentNumber = Integer.parseInt(fileName.substring(4, 8)); // Extract from wal-NNNN.log
            try {
                // The newest segment becomes currentSegment and must accept appends after restart.
                boolean readOnly = segmentNumber != lastSegmentNumber;
                WALSegment segment = WALSegment.open(config.getWalDir(), segmentNumber, readOnly);
                segments.put(segmentNumber, segment);
            } catch (IOException e) {
                LOGGER.warn("Failed to open segment {}: {}", filePath, e.getMessage());
            }
        }
    }

    /**
     * Recovers the last LSN from checkpoint.ptr and segments.
     */
    public void recoverLsn() throws IOException {
        long checkpointLsn = readCheckpointPtr();
        long maxSegmentLsn = findMaxLsnInSegments();
        
        long recoveredLsn = Math.max(checkpointLsn, maxSegmentLsn);
        lastAppendedLsn.set(recoveredLsn);
        lastAllocatedLsn.set(recoveredLsn);
    }

    /**
     * Finds the maximum LSN in all segments.
     *
     * @return the maximum LSN found, or 0 if no segments
     */
    private long findMaxLsnInSegments() throws IOException {
        long maxLsn = 0;
        
        for (WALSegment segment : segments.values()) {
            List<WALEntry> entries = segment.readAll();
            if (!entries.isEmpty()) {
                long segmentMax = entries.stream()
                    .mapToLong(WALEntry::getLsn)
                    .max()
                    .orElse(0);
                maxLsn = Math.max(maxLsn, segmentMax);
            }
        }
        
        return maxLsn;
    }

    /**
     * Allocates the next LSN.
     *
     * @return the next LSN
     */
    public long allocateLsn() {
        return lastAllocatedLsn.incrementAndGet();
    }

    /**
     * Appends a WAL entry with the given parameters.
     * Allocates an LSN and constructs the entry.
     *
     * @param txid the transaction ID
     * @param op the operation code
     * @param beforeImage the before-image, or null if absent
     * @param afterImage the after-image, or null if absent
     * @return the appended entry
     * @throws IOException if the entry cannot be written
     */
    public WALEntry append(long txid, WALOpcode op, byte[] beforeImage, byte[] afterImage) throws IOException {
        long lsn = allocateLsn();
        WALEntry entry = new WALEntry(lsn, txid, op, beforeImage, afterImage);
        append(entry);
        return entry;
    }

    /**
     * Appends a WAL entry.
     * Validates that the entry's LSN is strictly greater than the last appended LSN.
     *
     * @param entry the entry to append
     * @throws IOException if the entry cannot be written
     * @throws IllegalArgumentException if the entry's LSN is not strictly increasing
     */
    public void append(WALEntry entry) throws IOException {
        long entryLsn = entry.getLsn();
        long lastLsn = lastAppendedLsn.get();
        
        if (entryLsn <= lastLsn) {
            throw new IllegalArgumentException("Entry LSN " + entryLsn + 
                    " must be > last appended LSN " + lastLsn);
        }

        // Check if we need to rotate the segment (size or age)
        int entrySize = entry.encodedSize();
        if (rotator.shouldRotate(currentSegment, entrySize)) {
            rotateSegment();
        }

        // Append to current segment
        currentSegment.append(entry);
        lastAppendedLsn.set(entryLsn);
        lastAllocatedLsn.set(entryLsn);

        // Persist checkpoint periodically
        persistCounter++;
        if (persistCounter >= 4096 || currentSegment.getPosition() > 1024 * 1024) {
            persistCheckpointPtr();
            persistCounter = 0;
        }
    }

    /**
     * Appends a batch of WAL entries in strictly increasing LSN order with as
     * few segment writes as possible (prompt4.md step 13).
     *
     * <p>The batch is grouped into runs that fit the current segment; each run
     * is encoded and written with one channel write. Segment rotation and
     * checkpoint persistence behave exactly like repeated {@link #append(WALEntry)}.
     *
     * @param entries the entries to append, in strictly increasing LSN order
     * @throws IOException if an entry cannot be written
     * @throws IllegalArgumentException if the LSNs are not strictly increasing
     */
    public void appendBatch(List<WALEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        long previousLsn = lastAppendedLsn.get();
        for (WALEntry entry : entries) {
            if (entry.getLsn() <= previousLsn) {
                throw new IllegalArgumentException("Entry LSN " + entry.getLsn() +
                        " must be > last appended LSN " + previousLsn);
            }
            previousLsn = entry.getLsn();
        }

        List<WALEntry> run = new ArrayList<>(entries.size());
        for (WALEntry entry : entries) {
            if (currentSegment.getPosition() + entry.encodedSize() > config.getMaxSegmentSizeBytes()) {
                appendRun(run);
                run.clear();
                rotateSegment();
            }
            run.add(entry);
        }
        appendRun(run);
    }

    /**
     * Writes one run of consecutive entries as a single segment write and
     * advances the appended/allocated LSN cursors plus the checkpoint counter.
     *
     * @param run the entries to write; no-op when empty
     * @throws IOException if the run cannot be written
     */
    private void appendRun(List<WALEntry> run) throws IOException {
        if (run.isEmpty()) {
            return;
        }
        currentSegment.appendBatch(run);
        long lastLsn = run.get(run.size() - 1).getLsn();
        lastAppendedLsn.set(lastLsn);
        lastAllocatedLsn.set(lastLsn);

        persistCounter += run.size();
        if (persistCounter >= 4096 || currentSegment.getPosition() > 1024 * 1024) {
            persistCheckpointPtr();
            persistCounter = 0;
        }
    }

    /**
     * Rotates to the next segment.
     *
     * @throws IOException if the new segment cannot be created
     */
    private void rotateSegment() throws IOException {
        // Durability: persist the outgoing segment before switching.
        // The segment stays open (readable) so later readByLsn/readAll across
        // segments keeps working; close() releases every channel.
        currentSegment.force();

        currentSegment = WALSegment.create(config.getWalDir(), nextSegmentNumber);
        segments.put(nextSegmentNumber, currentSegment);
        nextSegmentNumber++;
        
        // Update current segment number for archiver
        currentSegmentNumber.set(currentSegment.getNumber());
        
        LOGGER.debug("Rotated to segment {}", currentSegment.getNumber());
    }

    /**
     * Reads a WAL entry by LSN.
     *
     * @param lsn the LSN to read
     * @return the entry, or null if not found
     * @throws IOException if reading fails
     */
    public WALEntry readByLsn(long lsn) throws IOException {
        // Find the segment containing this LSN
        WALSegment segment = findSegmentForLsn(lsn);
        if (segment == null) {
            return null;
        }

        // Scan the segment for the entry
        List<WALEntry> entries = segment.readAll();
        for (WALEntry entry : entries) {
            if (entry.getLsn() == lsn) {
                return entry;
            }
            if (entry.getLsn() > lsn) {
                // LSN not found in this segment
                break;
            }
        }

        return null;
    }

    /**
     * Finds the segment containing the given LSN.
     *
     * @param lsn the LSN to find
     * @return the segment, or null if not found
     */
    private WALSegment findSegmentForLsn(long lsn) {
        // For now, scan all segments (simple, works for small WALs)
        // Future optimization: maintain per-segment LSN ranges
        for (WALSegment segment : segments.descendingMap().values()) {
            List<WALEntry> entries;
            try {
                entries = segment.readAll();
            } catch (IOException e) {
                LOGGER.warn("Failed to read segment {}: {}", segment.getNumber(), e.getMessage());
                continue;
            }
            
            if (!entries.isEmpty()) {
                long firstLsn = entries.get(0).getLsn();
                long lastLsn = entries.get(entries.size() - 1).getLsn();
                if (lsn >= firstLsn && lsn <= lastLsn) {
                    return segment;
                }
            }
        }
        
        return null;
    }

    /**
     * Reads all WAL entries.
     *
     * @return list of all entries (in LSN order)
     * @throws IOException if reading fails
     */
    public List<WALEntry> readAll() throws IOException {
        List<WALEntry> allEntries = new ArrayList<>();
        
        for (WALSegment segment : segments.values()) {
            List<WALEntry> entries = segment.readAll();
            allEntries.addAll(entries);
        }
        
        // Sort by LSN
        allEntries.sort((e1, e2) -> Long.compare(e1.getLsn(), e2.getLsn()));
        
        return Collections.unmodifiableList(allEntries);
    }

    /**
     * Reads entries in the given LSN range (inclusive).
     *
     * @param fromLsnInclusive the starting LSN (inclusive)
     * @param toLsnInclusive the ending LSN (inclusive)
     * @return list of entries in the range
     * @throws IOException if reading fails
     */
    public List<WALEntry> readRange(long fromLsnInclusive, long toLsnInclusive) throws IOException {
        List<WALEntry> rangeEntries = new ArrayList<>();
        
        for (WALSegment segment : segments.values()) {
            List<WALEntry> entries = segment.readAll();
            
            for (WALEntry entry : entries) {
                long lsn = entry.getLsn();
                if (lsn >= fromLsnInclusive && lsn <= toLsnInclusive) {
                    rangeEntries.add(entry);
                } else if (lsn > toLsnInclusive) {
                    break; // No more entries in this segment
                }
            }
        }
        
        return Collections.unmodifiableList(rangeEntries);
    }

    /**
     * Returns the current WAL segment.
     *
     * @return the current segment
     */
    public WALSegment getCurrent() {
        return currentSegment;
    }

    /**
     * Returns all segments.
     *
     * @return unmodifiable map of segment number to segment
     */
    public java.util.Map<Integer, WALSegment> getSegments() {
        return Collections.unmodifiableMap(segments);
    }

    /**
     * Returns the last appended LSN.
     *
     * @return the last LSN
     */
    public long getLastLsn() {
        return lastAppendedLsn.get();
    }

    /**
     * Returns the WAL directory path.
     *
     * @return the WAL directory
     */
    public Path getWalDir() {
        return config.getWalDir();
    }

    /**
     * Forces all buffered writes to disk and persists checkpoint.
     *
     * @throws IOException if the force fails
     */
    public void flush() throws IOException {
        for (WALSegment segment : segments.values()) {
            segment.force();
        }
        persistCheckpointPtr();
    }

    /**
     * Persists the last appended LSN to checkpoint.ptr.
     *
     * @throws IOException if the write fails
     */
    private void persistCheckpointPtr() throws IOException {
        Path checkpointFile = config.getWalDir().resolve("checkpoint.ptr");
        Path tempFile = config.getWalDir().resolve("checkpoint.ptr.tmp");
        
        try {
            // Write to temp file
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(lastAppendedLsn.get());
            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            Files.write(tempFile, bytes);
            
            // Atomic move
            Files.move(tempFile, checkpointFile, StandardCopyOption.REPLACE_EXISTING, 
                    StandardCopyOption.ATOMIC_MOVE);
            
            LOGGER.debug("Persisted checkpoint.ptr: {}", lastAppendedLsn.get());
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /**
     * Reads the checkpoint.ptr file.
     *
     * @return the persisted LSN, or 0 if the file doesn't exist
     * @throws IOException if the read fails
     */
    private long readCheckpointPtr() throws IOException {
        Path checkpointFile = config.getWalDir().resolve("checkpoint.ptr");
        if (!Files.exists(checkpointFile)) {
            return 0;
        }
        
        byte[] bytes = Files.readAllBytes(checkpointFile);
        if (bytes.length != 8) {
            LOGGER.warn("checkpoint.ptr has invalid size: {} bytes", bytes.length);
            return 0;
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }

    /**
     * Closes this WAL manager.
     * Stops the archiver daemon, flushes all segments, and closes all file channels.
     *
     * @throws IOException if the close fails
     */
    @Override
    public void close() throws IOException {
        // Stop archiver daemon first (no final cycle to preserve files for restart)
        archiver.close();
        
        flush();
        for (WALSegment segment : segments.values()) {
            segment.close();
        }
        segments.clear();
        LOGGER.debug("WALManager closed");
    }

    /**
     * Returns the current segment number (for archiver use).
     *
     * @return the current segment number
     */
    public int getCurrentSegmentNumber() {
        return currentSegmentNumber.get();
    }

    /**
     * Returns the WAL archiver (for testing and manual archiving).
     *
     * @return the archiver
     */
    public WALArchiver getArchiver() {
        return archiver;
    }

    /**
     * Forces rotation of the current segment if it has content.
     * Useful for manual rotation or testing.
     *
     * @throws IOException if rotation fails
     */
    public void forceRotate() throws IOException {
        if (rotator.forceRotate(currentSegment)) {
            rotateSegment();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WALManager.class);
}