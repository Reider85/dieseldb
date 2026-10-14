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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diesel.recovery.CheckpointRecord;
import diesel.recovery.CheckpointPointerFile;
import diesel.recovery.CheckpointFormatException;

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
    private final NavigableMap<Long, WALSegment> lsnToSegment = new TreeMap<>();
    private WALSegment currentSegment;
    private final AtomicInteger currentSegmentNumber = new AtomicInteger(0);
    private final CheckpointPointerFile checkpointPointerFile;
    private final RecoverySegmentCache recoveryCache;
    private final long recoveryCacheSizeBytes;
    private long recoveryCacheDiskReads = 0;
    private long recoveryCacheHits = 0;
    /**
     * LSN of the last CHECKPOINT entry written (0 = none). Persisted to
     * checkpoint.ptr on close so a clean restart can reload the checkpoint
     * (ARIES semantics: the pointer references a CHECKPOINT entry, not the
     * end-of-log LSN — segment scan covers LSN recovery in recoverLsn()).
     */
    private long lastCheckpointLsn;
    private final AtomicLong lastFlushedLsn = new AtomicLong(0);
    private int nextSegmentNumber = 1;
    private int persistCounter = 0;
    /**
     * Number of segments whose data was scanned by readRange since the last
     * reset. Package-private test hook (WAL-4) proving the lsnToSegment
     * subMap skips non-overlapping segments.
     */
    private final AtomicInteger readRangeSegmentsScanned = new AtomicInteger();

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
        this.checkpointPointerFile = new CheckpointPointerFile(config.getWalDir());
        // Preserve an existing checkpoint pointer across close() cycles.
        this.lastCheckpointLsn = this.checkpointPointerFile.read();
        
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
        
        // Initialize recovery cache
        this.recoveryCacheSizeBytes = config.getRecoveryCacheSizeBytes();
        this.recoveryCache = recoveryCacheSizeBytes > 0 ? new RecoverySegmentCache() : null;
        
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
     * Indexes a segment in the LSN-to-segment map if it has a first LSN.
     *
     * @param segment the segment to index
     */
    private void indexSegment(WALSegment segment) {
        long firstLsn = segment.getFirstLsn();
        if (firstLsn > 0) {
            lsnToSegment.put(firstLsn, segment);
        }
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
                // Index the segment if it has entries
                indexSegment(segment);
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
        // Walk segments in descending order to find the first one with a non-zero last LSN
        // This handles trailing empty segments correctly (lastEntry() would return 0 incorrectly)
        for (WALSegment segment : segments.descendingMap().values()) {
            long lastLsn = segment.getLastLsn();
            if (lastLsn > 0) {
                return lastLsn;
            }
        }
        return 0; // All segments are empty
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
        
        // Index the segment if it's the first entry
        indexSegment(currentSegment);
        
        // Note: Periodic checkpoint persistence removed for ARIES.
        // Checkpoints are written explicitly via writeCheckpoint().
    }

    /**
     * Writes a checkpoint record to WAL and updates checkpoint.ptr.
     * This is the ARIES checkpoint mechanism: writes the record as a CHECKPOINT
     * WAL entry and atomically updates the checkpoint pointer to point to it.
     *
     * @param activeTxids the list of active transaction IDs at checkpoint time
     * @throws IOException if the write fails
     */
    public void writeCheckpoint(List<Long> activeTxids) throws IOException {
        if (activeTxids == null) {
            throw new IllegalArgumentException("activeTxids cannot be null");
        }

        // Create checkpoint record with current last LSN and active txids
        long currentLsn = lastAppendedLsn.get();
        long timestamp = clock.millis();
        CheckpointRecord checkpointRecord = new CheckpointRecord(currentLsn, 
                activeTxids.stream().mapToLong(Long::longValue).toArray(), timestamp);

        // Serialize the record and create a CHECKPOINT WAL entry (txid=0 for system)
        byte[] recordBytes = checkpointRecord.toBytes();
        WALEntry checkpointEntry = new WALEntry(currentLsn + 1, 0L, WALOpcode.CHECKPOINT, null, recordBytes);

        // Append the checkpoint entry to WAL (this allocates a new LSN)
        currentSegment.append(checkpointEntry);
        long checkpointLsn = checkpointEntry.getLsn();
        lastAppendedLsn.set(checkpointLsn);
        lastAllocatedLsn.set(checkpointLsn);
        
        // Index the segment if it's the first entries
        indexSegment(currentSegment);

        // Force the segment to ensure the checkpoint record is durable
        currentSegment.force();

        // Update last flushed LSN to include the checkpoint record
        lastFlushedLsn.set(checkpointLsn);

        // Atomically update checkpoint.ptr to point to the checkpoint record
        checkpointPointerFile.write(checkpointLsn);
        lastCheckpointLsn = checkpointLsn;

        LOGGER.info("Checkpoint written at LSN {}, {} active txids", checkpointLsn, activeTxids.size());
    }

    /**
     * Loads the last checkpoint record from WAL using checkpoint.ptr.
     * Returns null if no checkpoint exists or the checkpoint record is corrupt.
     *
     * @return the checkpoint record, or null if none/invalid
     * @throws IOException if the read fails
     */
    public CheckpointRecord loadCheckpointRecord() throws IOException {
        long checkpointLsn = checkpointPointerFile.read();
        if (checkpointLsn == 0) {
            LOGGER.debug("No checkpoint found (checkpoint.ptr is 0)");
            return null;
        }

        try {
            WALEntry entry = readByLsn(checkpointLsn);
            if (entry == null) {
                LOGGER.warn("Checkpoint ptr points to non-existent LSN: {}", checkpointLsn);
                return null;
            }

            if (entry.getOp() != WALOpcode.CHECKPOINT) {
                LOGGER.warn("Checkpoint ptr points to non-checkpoint entry: {} at LSN {}", 
                        entry.getOp(), checkpointLsn);
                return null;
            }

            // Deserialize the checkpoint record from the after-image
            return CheckpointRecord.fromBytes(entry.getAfterImage());
        } catch (CheckpointFormatException e) {
            LOGGER.warn("Invalid checkpoint record at LSN {}: {}", checkpointLsn, e.getMessage());
            return null;
        } catch (Exception e) {
            LOGGER.warn("Failed to load checkpoint at LSN {}: {}", checkpointLsn, e.getMessage());
            return null;
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
        
        // Index the segment if it's the first entries
        indexSegment(currentSegment);
        
        // Note: Periodic checkpoint persistence removed for ARIES.
        // Checkpoints are written explicitly via writeCheckpoint().
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
        
        // New segments are initially empty (firstLsn=0), so no indexing yet
        
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

        // Use the new streaming readByLsn method for early exit
        return segment.readByLsn(lsn);
    }

    /**
     * Finds the segment containing the given LSN.
     *
     * @param lsn the LSN to find
     * @return the segment, or null if not found
     */
    private WALSegment findSegmentForLsn(long lsn) {
        // Use the LSN-to-segment map for O(log n) lookup
        Map.Entry<Long, WALSegment> entry = lsnToSegment.floorEntry(lsn);
        if (entry == null) {
            return null;
        }
        
        WALSegment segment = entry.getValue();
        // LSNs below the first indexed entry belong to no segment
        if (lsn < segment.getFirstLsn()) {
            return null;
        }
        
        return segment;
    }

    /**
     * Reads all WAL entries.
     *
     * <p>Post-condition: the returned list is strictly LSN-ascending. This relies
     * on two invariants (WAL-5 removed the previous defensive O(N log N) sort):
     * segments are held in a TreeMap keyed by monotonically assigned segment
     * numbers, and {@link #append(WALEntry)} enforces strictly increasing LSNs,
     * so concatenating per-segment reads in segment order yields global LSN order.
     *
     * @return unmodifiable list of all entries in ascending LSN order
     * @throws IOException if reading fails
     */
    public List<WALEntry> readAll() throws IOException {
        List<WALEntry> allEntries = new ArrayList<>();
        
        for (WALSegment segment : segments.values()) {
            List<WALEntry> entries = segment.readAll();
            allEntries.addAll(entries);
        }
        
        assert isLsnAscending(allEntries) : "WAL entries out of order";
        
        return Collections.unmodifiableList(allEntries);
    }

    /** O(N) check that entry LSNs are strictly increasing. Used by the assert in readAll. */
    private static boolean isLsnAscending(List<WALEntry> entries) {
        long prev = Long.MIN_VALUE;
        for (WALEntry e : entries) {
            long lsn = e.getLsn();
            if (lsn <= prev) return false;
            prev = lsn;
        }
        return true;
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
        if (fromLsnInclusive > toLsnInclusive || lsnToSegment.isEmpty()) {
            return Collections.unmodifiableList(rangeEntries);
        }

        // The segment with the greatest firstLsn <= fromLsn is the only one that
        // may begin before the requested window yet still contain fromLsn.
        Map.Entry<Long, WALSegment> floor = lsnToSegment.floorEntry(fromLsnInclusive);
        long startKey = (floor != null) ? floor.getKey() : lsnToSegment.firstKey();
        if (startKey > toLsnInclusive) {
            // Range lies entirely before every indexed segment; subMap would throw.
            return Collections.unmodifiableList(rangeEntries);
        }

        for (WALSegment segment : lsnToSegment.subMap(startKey, true, toLsnInclusive, true).values()) {
            // Defensive short-circuits mirroring AnalysisPhase/RedoPhase: skip
            // segments whose LSN range falls entirely outside the window.
            if (segment.getFirstLsn() > toLsnInclusive && segment.getFirstLsn() != 0) {
                break;
            }
            if (segment.getLastLsn() != 0 && segment.getLastLsn() < fromLsnInclusive) {
                continue;
            }
            readRangeSegmentsScanned.incrementAndGet();

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
     * Returns the number of segments scanned by readRange since the last reset.
     * Package-private test hook for WAL-4 (skip non-overlapping segments).
     *
     * @return segments scanned count
     */
    int readRangeSegmentsScanned() {
        return readRangeSegmentsScanned.get();
    }

    /**
     * Resets the readRange segment-scan counter. Package-private test hook for WAL-4.
     */
    void resetReadRangeSegmentsScanned() {
        readRangeSegmentsScanned.set(0);
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
     * @return unmodifiable navigable map of segment number to segment
     */
    public java.util.NavigableMap<Integer, WALSegment> getSegments() {
        return Collections.unmodifiableNavigableMap(segments);
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
     * Returns the LSN of the last WAL segment that was forced to disk.
     * Used by BufferPoolFlusher to implement the WAL-before-page rule.
     *
     * @return the last flushed LSN, or 0 if never flushed
     */
    public long getLastFlushedLsn() {
        return lastFlushedLsn.get();
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
        // Update last flushed LSN to the last appended LSN after force
        lastFlushedLsn.set(lastAppendedLsn.get());
        // Note: Removed periodic persistCheckpointPtr() for ARIES.
        // The checkpoint pointer is updated by writeCheckpoint() and close().
    }

    /**
     * Persists the last checkpoint entry LSN to checkpoint.ptr.
     * Called from close() so a clean restart can reload the checkpoint record;
     * writes 0 when no checkpoint was ever written (LSN recovery uses the
     * segment scan in recoverLsn(), which takes max(ptr, segment LSNs)).
     *
     * @throws IOException if the write fails
     */
    private void persistCheckpointPtr() throws IOException {
        checkpointPointerFile.write(lastCheckpointLsn);
        LOGGER.debug("Persisted checkpoint.ptr: {}", lastCheckpointLsn);
    }

    /**
     * Reads the checkpoint.ptr file.
     * This is a legacy method for LSN recovery.
     *
     * @return the persisted LSN, or 0 if the file doesn't exist
     * @throws IOException if the read fails
     */
    private long readCheckpointPtr() throws IOException {
        return checkpointPointerFile.read();
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
        // Persist checkpoint ptr for recovery (ARIES requires this for restart)
        persistCheckpointPtr();
        clearRecoveryCache(); // Free memory
        for (WALSegment segment : segments.values()) {
            segment.close();
        }
        segments.clear();
        lsnToSegment.clear();
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

    /**
     * LRU cache for decoded WAL segment bodies during recovery.
     * Evicts least recently accessed segments when size exceeds configured limit.
     */
    private final class RecoverySegmentCache extends LinkedHashMap<Integer, List<WALEntry>> {
        private long totalBytes = 0;

        private RecoverySegmentCache() {
            super(16, 0.75f, true); // access-order LRU
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, List<WALEntry>> eldest) {
            if (totalBytes > recoveryCacheSizeBytes) {
                // LinkedHashMap evicts via an internal removeNode call that does
                // NOT dispatch to our remove() override — decrement here.
                totalBytes -= calculateSize(eldest.getValue());
                return true;
            }
            return false;
        }

        @Override
        public List<WALEntry> put(Integer key, List<WALEntry> value) {
            // Remove existing entry if present to update LRU and subtract size
            List<WALEntry> existing = super.get(key);
            if (existing != null) {
                totalBytes -= calculateSize(existing);
            }

            // Update size BEFORE super.put so removeEldestEntry sees the new size
            totalBytes += calculateSize(value);
            return super.put(key, value);
        }

        @Override
        public List<WALEntry> remove(Object key) {
            List<WALEntry> removed = super.remove(key);
            if (removed != null) {
                totalBytes -= calculateSize(removed);
            }
            return removed;
        }

        @Override
        public void clear() {
            super.clear();
            totalBytes = 0;
        }

        private long calculateSize(List<WALEntry> entries) {
            long size = 0;
            for (WALEntry entry : entries) {
                size += entry.encodedSize();
            }
            return size;
        }
    }

    /**
     * Reads all entries from a segment using the recovery cache.
     * Package-private for testing.
     *
     * @param segmentNumber the segment number to read
     * @return unmodifiable list of entries from the segment
     * @throws IOException if reading fails
     */
    public List<WALEntry> readAllCached(int segmentNumber) throws IOException {
        if (recoveryCache == null) {
            // Cache disabled - read directly from segment
            WALSegment segment = segments.get(segmentNumber);
            if (segment == null) {
                return List.of();
            }
            recoveryCacheDiskReads++;
            return Collections.unmodifiableList(segment.readAll());
        }

        // Check cache first
        List<WALEntry> cached = recoveryCache.get(segmentNumber);
        if (cached != null) {
            recoveryCacheHits++;
            return cached;
        }

        // Cache miss - read from disk
        WALSegment segment = segments.get(segmentNumber);
        if (segment == null) {
            return List.of();
        }
        List<WALEntry> entries = Collections.unmodifiableList(segment.readAll());
        recoveryCacheDiskReads++;

        // Put in cache (eviction handled by RecoverySegmentCache)
        recoveryCache.put(segmentNumber, entries);

        return entries;
    }

    /**
     * Clears the recovery cache to free memory.
     * Called at the end of recovery phases. Statistics are preserved;
     * use {@link #resetRecoveryCacheStats()} to reset them explicitly.
     */
    public void clearRecoveryCache() {
        if (recoveryCache != null) {
            recoveryCache.clear();
        }
    }

    /**
     * Returns the number of disk reads made by the recovery cache.
     * Test hook for verifying single-pass I/O.
     *
     * @return disk read count
     */
    public long recoveryCacheDiskReadCount() {
        return recoveryCacheDiskReads;
    }

    /**
     * Returns the number of cache hits made by the recovery cache.
     * Test hook for verifying cache effectiveness.
     *
     * @return cache hit count
     */
    public long recoveryCacheHitCount() {
        return recoveryCacheHits;
    }

    /**
     * Returns the current number of entries in the recovery cache.
     * Test hook for verifying cache lifecycle (cleared after recovery).
     *
     * @return entry count (0 when cache is disabled or empty)
     */
    public int recoveryCacheEntryCount() {
        return recoveryCache == null ? 0 : recoveryCache.size();
    }

    /**
     * Resets the recovery cache statistics.
     * Test hook for test isolation.
     */
    public void resetRecoveryCacheStats() {
        recoveryCacheDiskReads = 0;
        recoveryCacheHits = 0;
    }
}