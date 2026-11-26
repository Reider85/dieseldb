package diesel.replication.raft;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persistent Raft log for leadership and configuration changes (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Stores Raft log entries with term-based consistency guarantees. Provides
 * persistence, recovery, and log compaction capabilities.
 * 
 * <p>Log entry format:
 * <ul>
 *   <li>Index: Monotonically increasing sequence number</li>
 *   <li>Term: Term in which the entry was created</li>
 *   <li>Command: The actual command/data being replicated</li>
 *   <li>PreviousTerm: Term of the previous entry (for consistency checking)</li>
 * </ul>
 */
public class RaftLog implements AutoCloseable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftLog.class);
    
    // File names
    private static final String STATE_FILE = "raft-state.dat";
    private static final String LOG_PREFIX = "raft-log-";
    private static final String SNAPSHOT_PREFIX = "raft-snapshot-";
    
    // Configuration
    private final RaftConfig config;
    private final Path raftDir;
    
    /**
     * Get the Raft configuration.
     */
    public RaftConfig getConfig() {
        return config;
    }
    
    // In-memory state
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong votedFor = new AtomicLong(-1);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    
    // Log segments
    private final Map<Long, LogSegment> segments = new TreeMap<>();
    private long firstIndex = 0;
    private long lastIndex = -1;
    
    public RaftLog(RaftConfig config) {
        this.config = config;
        this.raftDir = config.getRaftDir();
        
        // Ensure raft directory exists
        try {
            Files.createDirectories(raftDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create raft directory: " + raftDir, e);
        }
    }
    
    /**
     * Start the Raft log and load existing state.
     */
    public void start() throws IOException {
        loadPersistedState();
        loadLogSegments();
        
        LOGGER.info("Raft log started with {} entries from index {} to {}", 
            segments.size(), firstIndex, lastIndex);
    }
    
    /**
     * Stop the Raft log.
     */
    @Override
    public void close() {
        // Flush any pending writes
        flushAllSegments();
    }
    
    /**
     * Append a new entry to the log.
     */
    public synchronized void appendEntry(RaftLogEntry entry) {
        long index = entry.getIndex();
        
        // Validate entry
        if (index != lastIndex + 1) {
            throw new IllegalArgumentException("Entry index " + index + " does not follow last index " + lastIndex);
        }
        
        if (entry.getTerm() < currentTerm.get()) {
            throw new IllegalArgumentException("Entry term " + entry.getTerm() + " is less than current term " + currentTerm.get());
        }
        
        // Create or get current segment
        LogSegment segment = getCurrentSegment();
        
        // Add entry to segment
        segment.addEntry(entry);
        lastIndex = index;
        
        // Check if we need to rotate segment
        if (segment.getSize() > config.getMaxSegmentSize()) {
            rotateSegment();
        }
        
        LOGGER.debug("Appended entry {} to segment {}", index, segment.getSegmentId());
    }
    
    /**
     * Get an entry by index.
     */
    public synchronized RaftLogEntry getEntry(long index) {
        if (index < firstIndex || index > lastIndex) {
            return null;
        }
        
        // Find the segment containing this index
        LogSegment segment = findSegment(index);
        if (segment != null) {
            return segment.getEntry(index);
        }
        
        return null;
    }
    
    /**
     * Get entries in a range.
     */
    public synchronized List<RaftLogEntry> getEntries(long fromIndex, int maxCount) {
        if (fromIndex < firstIndex || fromIndex > lastIndex) {
            return Collections.emptyList();
        }
        
        List<RaftLogEntry> entries = new ArrayList<>();
        long currentIndex = fromIndex;
        int count = 0;
        
        while (currentIndex <= lastIndex && count < maxCount) {
            RaftLogEntry entry = getEntry(currentIndex);
            if (entry != null) {
                entries.add(entry);
                count++;
            }
            currentIndex++;
        }
        
        return entries;
    }
    
    /**
     * Get the last entry in the log.
     */
    public synchronized RaftLogEntry getLastEntry() {
        if (lastIndex < firstIndex) {
            return null;
        }
        return getEntry(lastIndex);
    }
    
    /**
     * Get the last index in the log.
     */
    public synchronized long getLastIndex() {
        return lastIndex;
    }
    
    /**
     * Get the current term.
     */
    public synchronized long getCurrentTerm() {
        return currentTerm.get();
    }
    
    /**
     * Get the voted-for candidate.
     */
    public synchronized long getVotedFor() {
        return votedFor.get();
    }
    
    /**
     * Set the current term.
     */
    public synchronized void setCurrentTerm(long term) {
        currentTerm.set(term);
    }
    
    /**
     * Set the voted-for candidate.
     */
    public synchronized void setVotedFor(long candidateId) {
        votedFor.set(candidateId);
    }
    
    /**
     * Set commit index.
     */
    public synchronized void setCommitIndex(long index) {
        commitIndex.set(index);
    }
    
    /**
     * Get commit index.
     */
    public synchronized long getCommitIndex() {
        return commitIndex.get();
    }
    
    /**
     * Set last applied index.
     */
    public synchronized void setLastApplied(long index) {
        lastApplied.set(index);
    }
    
    /**
     * Get last applied index.
     */
    public synchronized long getLastApplied() {
        return lastApplied.get();
    }
    
    /**
     * Compact the log by removing entries before the given index.
     */
    public synchronized void compact(long upToIndex) {
        if (upToIndex <= firstIndex) {
            return; // Nothing to compact
        }
        
        LOGGER.info("Compacting log up to index {}", upToIndex);
        
        // Remove segments that are completely before the compact index
        Iterator<Map.Entry<Long, LogSegment>> it = segments.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, LogSegment> entry = it.next();
            LogSegment segment = entry.getValue();
            
            if (segment.getLastIndex() < upToIndex) {
                // Remove this segment
                segment.delete();
                it.remove();
            }
        }
        
        // Update first index
        firstIndex = upToIndex;
        
        // Update current segment if needed
        LogSegment currentSegment = getCurrentSegment();
        if (currentSegment != null && currentSegment.getFirstIndex() < upToIndex) {
            // Create new segment starting from compact index
            rotateSegment();
        }
        
        LOGGER.info("Log compacted, new first index: {}", firstIndex);
    }
    
    /**
     * Create a snapshot at the current commit index.
     */
    public synchronized void createSnapshot() {
        long snapshotIndex = commitIndex.get();
        if (snapshotIndex <= 0) {
            return; // Nothing to snapshot
        }
        
        Path snapshotFile = raftDir.resolve(SNAPSHOT_PREFIX + snapshotIndex + ".dat");
        
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(snapshotFile)))) {
            // Write snapshot header
            dos.writeUTF("RAFT-SNAPSHOT");
            dos.writeLong(snapshotIndex);
            dos.writeLong(currentTerm.get());
            
            // Write snapshot data (implementation depends on state machine)
            // For now, just write a placeholder
            dos.writeUTF("SNAPSHOT-DATA");
            
            LOGGER.info("Created snapshot at index {}", snapshotIndex);
            
        } catch (IOException e) {
            LOGGER.error("Failed to create snapshot at index {}", snapshotIndex, e);
            throw new RuntimeException("Failed to create snapshot", e);
        }
    }
    
    /**
     * Load persisted state from disk.
     */
    public synchronized void loadPersistedState() throws IOException {
        Path stateFile = raftDir.resolve(STATE_FILE);
        
        if (Files.exists(stateFile)) {
            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(stateFile)))) {
                String magic = dis.readUTF();
                if (!"RAFT-STATE".equals(magic)) {
                    throw new IOException("Invalid state file format");
                }
                
                currentTerm.set(dis.readLong());
                votedFor.set(dis.readLong());
                commitIndex.set(dis.readLong());
                lastApplied.set(dis.readLong());
                
                LOGGER.info("Loaded persisted state: term={}, votedFor={}, commitIndex={}, lastApplied={}", 
                    currentTerm.get(), votedFor.get(), commitIndex.get(), lastApplied.get());
            }
        } else {
            LOGGER.info("No existing state file found, starting with fresh state");
        }
    }
    
    /**
     * Persist current state to disk.
     */
    public synchronized void persistState(long term, long votedFor) throws IOException {
        Path stateFile = raftDir.resolve(STATE_FILE);
        
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(stateFile)))) {
            dos.writeUTF("RAFT-STATE");
            dos.writeLong(term);
            dos.writeLong(votedFor);
            dos.writeLong(commitIndex.get());
            dos.writeLong(lastApplied.get());
        }
    }
    
    /**
     * Load existing log segments from disk.
     */
    private synchronized void loadLogSegments() throws IOException {
        // Clear existing segments
        segments.clear();
        firstIndex = 0;
        lastIndex = -1;
        
        // Find all log segment files
        DirectoryStream<Path> stream = Files.newDirectoryStream(raftDir, LOG_PREFIX + "*.dat");
        List<Path> segmentFiles = new ArrayList<>();
        for (Path file : stream) {
            segmentFiles.add(file);
        }
        stream.close();
        
        // Sort files by name (which contains segment ID)
        segmentFiles.sort(Comparator.comparing(Path::getFileName));
        
        // Load each segment
        for (Path segmentFile : segmentFiles) {
            LogSegment segment = new LogSegment(segmentFile, this);
            segment.load();
            
            segments.put(segment.getSegmentId(), segment);
            
            // Update log bounds
            if (firstIndex == 0) {
                firstIndex = segment.getFirstIndex();
            }
            lastIndex = Math.max(lastIndex, segment.getLastIndex());
        }
        
        LOGGER.info("Loaded {} log segments, first index: {}, last index: {}", 
            segments.size(), firstIndex, lastIndex);
    }
    
    /**
     * Get the current segment for writing.
     */
    private synchronized LogSegment getCurrentSegment() {
        if (segments.isEmpty()) {
            // Create first segment
            return createSegment(0);
        }
        
        LogSegment lastSegment = segments.get(segments.keySet().stream().max(Long::compare).orElse(0L));
        if (lastSegment.getSize() < config.getMaxSegmentSize()) {
            return lastSegment;
        } else {
            // Create new segment
            return createSegment(lastSegment.getLastIndex() + 1);
        }
    }
    
    /**
     * Create a new log segment.
     */
    private synchronized LogSegment createSegment(long startIndex) {
        long segmentId = segments.isEmpty() ? 0 : segments.keySet().stream().max(Long::compare).orElse(0L) + 1;
        LogSegment segment = new LogSegment(segmentId, startIndex, raftDir, this);
        segments.put(segmentId, segment);
        return segment;
    }
    
    /**
     * Rotate to a new segment.
     */
    private synchronized void rotateSegment() {
        LogSegment currentSegment = getCurrentSegment();
        if (currentSegment != null && currentSegment.getSize() > 0) {
            currentSegment.finish();
        }
    }
    
    /**
     * Find the segment containing the given index.
     */
    private synchronized LogSegment findSegment(long index) {
        for (LogSegment segment : segments.values()) {
            if (index >= segment.getFirstIndex() && index <= segment.getLastIndex()) {
                return segment;
            }
        }
        return null;
    }
    
    /**
     * Flush all segments to disk.
     */
    private synchronized void flushAllSegments() {
        for (LogSegment segment : segments.values()) {
            segment.flush();
        }
    }
    
    /**
     * Delete old segments beyond the retention limit.
     */
    public synchronized void cleanupOldSegments() {
        if (segments.size() <= config.getMaxSegments()) {
            return;
        }
        
        // Remove oldest segments
        Iterator<Map.Entry<Long, LogSegment>> it = segments.entrySet().iterator();
        while (it.hasNext() && segments.size() > config.getMaxSegments()) {
            Map.Entry<Long, LogSegment> entry = it.next();
            LogSegment segment = entry.getValue();
            segment.delete();
            it.remove();
        }
    }
    
    /**
     * Get log statistics.
     */
    public synchronized LogStats getStats() {
        return new LogStats(
            firstIndex,
            lastIndex,
            segments.size(),
            currentTerm.get(),
            votedFor.get(),
            commitIndex.get(),
            lastApplied.get()
        );
    }
    
    /**
     * Log statistics.
     */
    public static class LogStats {
        private final long firstIndex;
        private final long lastIndex;
        private final int segmentCount;
        private final long currentTerm;
        private final long votedFor;
        private final long commitIndex;
        private final long lastApplied;
        
        public LogStats(long firstIndex, long lastIndex, int segmentCount, long currentTerm, 
                       long votedFor, long commitIndex, long lastApplied) {
            this.firstIndex = firstIndex;
            this.lastIndex = lastIndex;
            this.segmentCount = segmentCount;
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.commitIndex = commitIndex;
            this.lastApplied = lastApplied;
        }
        
        // Getters
        public long getFirstIndex() { return firstIndex; }
        public long getLastIndex() { return lastIndex; }
        public int getSegmentCount() { return segmentCount; }
        public long getCurrentTerm() { return currentTerm; }
        public long getVotedFor() { return votedFor; }
        public long getCommitIndex() { return commitIndex; }
        public long getLastApplied() { return lastApplied; }
    }
}