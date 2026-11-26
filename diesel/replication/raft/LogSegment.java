package diesel.replication.raft;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log segment for Raft log storage (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Manages a single segment of the Raft log with efficient file-based storage.
 * Handles reading, writing, and managing log entries within a segment boundary.
 */
class LogSegment implements AutoCloseable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSegment.class);
    
    // Segment file format
    private static final String SEGMENT_MAGIC = "RAFT-SEGMENT";
    private static final int SEGMENT_VERSION = 1;
    
    // Configuration
    private final RaftLog raftLog;
    private final Path segmentFile;
    private final long segmentId;
    private long startIndex;
    
    // Segment state
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Long, RaftLogEntry> entries = new TreeMap<>();
    private long lastIndex = -1;
    private long currentSize = 0;
    private boolean closed = false;
    
    /**
     * Create a new log segment.
     */
    public LogSegment(long segmentId, long startIndex, Path raftDir, RaftLog raftLog) {
        this.raftLog = raftLog;
        this.segmentId = segmentId;
        this.startIndex = startIndex;
        this.segmentFile = raftDir.resolve("raft-log-" + segmentId + ".dat");
        
        LOGGER.debug("Created new log segment {} starting at index {}", segmentId, startIndex);
    }
    
    /**
     * Load an existing log segment from file.
     */
    public LogSegment(Path segmentFile, RaftLog raftLog) {
        this.raftLog = raftLog;
        this.segmentFile = segmentFile;
        this.segmentId = extractSegmentId(segmentFile.getFileName().toString());
        this.startIndex = -1; // Will be loaded from file
        
        LOGGER.debug("Loading existing log segment {} from {}", segmentId, segmentFile);
    }
    
    /**
     * Load segment data from file.
     */
    public void load() throws IOException {
        lock.lock();
        try {
            if (Files.exists(segmentFile)) {
                try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(segmentFile)))) {
                    // Read header
                    String magic = dis.readUTF();
                    if (!SEGMENT_MAGIC.equals(magic)) {
                        throw new IOException("Invalid segment file format: " + magic);
                    }
                    
                    int version = dis.readInt();
                    if (version != SEGMENT_VERSION) {
                        throw new IOException("Unsupported segment version: " + version);
                    }
                    
                    // Read segment metadata
                    startIndex = dis.readLong();
                    int entryCount = dis.readInt();
                    
                    // Read entries
                    entries.clear();
                    lastIndex = startIndex - 1;
                    currentSize = 0;
                    
                    for (int i = 0; i < entryCount; i++) {
                        long index = dis.readLong();
                        long term = dis.readLong();
                        int commandLength = dis.readInt();
                        byte[] commandBytes = new byte[commandLength];
                        dis.readFully(commandBytes);
                        String command = new String(commandBytes);
                        
                        RaftLogEntry entry = new RaftLogEntry(term, index, command);
                        entries.put(index, entry);
                        lastIndex = index;
                        currentSize += calculateEntrySize(entry);
                    }
                    
                    LOGGER.info("Loaded segment {} with {} entries from index {} to {}", 
                        segmentId, entryCount, startIndex, lastIndex);
                }
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Add an entry to this segment.
     */
    public void addEntry(RaftLogEntry entry) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Segment is closed");
            }
            
            long index = entry.getIndex();
            if (index < startIndex) {
                throw new IllegalArgumentException("Entry index " + index + " is before segment start index " + startIndex);
            }
            
            // Check if this entry fits in the segment
            if (index > startIndex + raftLog.getConfig().getMaxSegmentSize() - 1) {
                throw new IllegalStateException("Entry index " + index + " exceeds segment capacity");
            }
            
            // Add entry
            entries.put(index, entry);
            lastIndex = index;
            currentSize += calculateEntrySize(entry);
            
            LOGGER.debug("Added entry {} to segment {}", index, segmentId);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get an entry by index.
     */
    public RaftLogEntry getEntry(long index) {
        lock.lock();
        try {
            if (index < startIndex || index > lastIndex) {
                return null;
            }
            return entries.get(index);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get entries in a range.
     */
    public List<RaftLogEntry> getEntries(long fromIndex, int maxCount) {
        lock.lock();
        try {
            List<RaftLogEntry> result = new ArrayList<>();
            long currentIndex = fromIndex;
            int count = 0;
            
            while (currentIndex <= lastIndex && count < maxCount) {
                RaftLogEntry entry = entries.get(currentIndex);
                if (entry != null) {
                    result.add(entry);
                    count++;
                }
                currentIndex++;
            }
            
            return result;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get the first index in this segment.
     */
    public long getFirstIndex() {
        return startIndex;
    }
    
    /**
     * Get the last index in this segment.
     */
    public long getLastIndex() {
        return lastIndex;
    }
    
    /**
     * Get the segment ID.
     */
    public long getSegmentId() {
        return segmentId;
    }
    
    /**
     * Get the size of this segment in bytes.
     */
    public long getSize() {
        lock.lock();
        try {
            return currentSize;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get the number of entries in this segment.
     */
    public int getEntryCount() {
        lock.lock();
        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Flush the segment to disk.
     */
    public void flush() {
        lock.lock();
        try {
            if (!entries.isEmpty()) {
                persistToDisk();
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Finish this segment (no more entries can be added).
     */
    public void finish() {
        lock.lock();
        try {
            closed = true;
            persistToDisk();
            LOGGER.debug("Finished segment {}", segmentId);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Delete this segment from disk.
     */
    public void delete() {
        lock.lock();
        try {
            try {
                Files.deleteIfExists(segmentFile);
                LOGGER.debug("Deleted segment {} file {}", segmentId, segmentFile);
            } catch (IOException e) {
                LOGGER.error("Failed to delete segment {} file {}", segmentId, segmentFile, e);
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Persist segment data to disk.
     */
    private void persistToDisk() {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(segmentFile)))) {
            // Write header
            dos.writeUTF(SEGMENT_MAGIC);
            dos.writeInt(SEGMENT_VERSION);
            
            // Write segment metadata
            dos.writeLong(startIndex);
            dos.writeInt(entries.size());
            
            // Write entries
            for (RaftLogEntry entry : entries.values()) {
                dos.writeLong(entry.getIndex());
                dos.writeLong(entry.getTerm());
                byte[] commandBytes = entry.getCommand().getBytes();
                dos.writeInt(commandBytes.length);
                dos.write(commandBytes);
            }
            
            LOGGER.debug("Persisted segment {} with {} entries", segmentId, entries.size());
            
        } catch (IOException e) {
            LOGGER.error("Failed to persist segment {} to disk", segmentId, e);
            throw new RuntimeException("Failed to persist segment", e);
        }
    }
    
    /**
     * Extract segment ID from filename.
     */
    private long extractSegmentId(String filename) {
        try {
            return Long.parseLong(filename.substring("raft-log-".length(), filename.lastIndexOf(".dat")));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid segment filename: " + filename);
        }
    }
    
    /**
     * Calculate the size of an entry in bytes.
     */
    private int calculateEntrySize(RaftLogEntry entry) {
        // Approximate size: index (8) + term (8) + command length (4) + command bytes
        return 8 + 8 + 4 + entry.getCommand().getBytes().length;
    }
    
    /**
     * Check if this segment is full.
     */
    public boolean isFull() {
        lock.lock();
        try {
            return currentSize >= raftLog.getConfig().getMaxSegmentSize();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Check if this segment is closed.
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Get segment statistics.
     */
    public SegmentStats getStats() {
        lock.lock();
        try {
            return new SegmentStats(segmentId, startIndex, lastIndex, entries.size(), currentSize, closed);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Segment statistics.
     */
    public static class SegmentStats {
        private final long segmentId;
        private final long startIndex;
        private final long lastIndex;
        private final int entryCount;
        private final long size;
        private final boolean closed;
        
        public SegmentStats(long segmentId, long startIndex, long lastIndex, 
                           int entryCount, long size, boolean closed) {
            this.segmentId = segmentId;
            this.startIndex = startIndex;
            this.lastIndex = lastIndex;
            this.entryCount = entryCount;
            this.size = size;
            this.closed = closed;
        }
        
        // Getters
        public long getSegmentId() { return segmentId; }
        public long getStartIndex() { return startIndex; }
        public long getLastIndex() { return lastIndex; }
        public int getEntryCount() { return entryCount; }
        public long getSize() { return size; }
        public boolean isClosed() { return closed; }
    }
    
    @Override
    public void close() throws IOException {
        // Close any open resources
        LOGGER.debug("Closing log segment {}", segmentId);
    }
}