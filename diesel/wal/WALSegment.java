package diesel.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single WAL segment file (prompt4.md step 12, R3-003 step 2/5).
 *
 * <p>Each segment file contains a 24-byte header (version 1) or 32-byte header (version 2) 
 * followed by zero or more WAL entries. Files are named {@code wal-NNNN.log} (4-digit zero-padded,
 * 1-based numbering). Segment headers are written lazily on the first append
 * to an empty segment (0-byte files are valid empty segments).
 *
 * <p>Thread-safety: not thread-safe (single-writer assumption for step 12).
 */
public final class WALSegment implements AutoCloseable {

    private final int segmentNumber;
    private final Path filePath;
    private final boolean readOnly;
    private final long createdAtEpochMs;
    private FileChannel channel;
    private long writePosition;
    private boolean headerWritten;
    private long firstLsn;
    private long lastLsn;
    private long persistedLastLsn;
    private int headerSize;
    private short headerVersion;

    /**
     * Creates a new WAL segment file.
     *
     * @param dir the WAL directory
     * @param segmentNumber the segment number (1-based)
     * @return the created segment
     * @throws IOException if the file cannot be created
     */
    public static WALSegment create(Path dir, int segmentNumber) throws IOException {
        Path filePath = dir.resolve("wal-" + String.format("%04d", segmentNumber) + ".log");
        Files.createFile(filePath);
        return openInternal(filePath, segmentNumber, false, System.currentTimeMillis());
    }

    /**
     * Opens an existing WAL segment file.
     *
     * @param dir the WAL directory
     * @param segmentNumber the segment number (1-based)
     * @param readOnly whether to open in read-only mode
     * @return the opened segment
     * @throws IOException if the file cannot be opened
     */
    public static WALSegment open(Path dir, int segmentNumber, boolean readOnly) throws IOException {
        Path filePath = dir.resolve("wal-" + String.format("%04d", segmentNumber) + ".log");
        long createdAtEpochMs = System.currentTimeMillis(); // Fallback
        try {
            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);
            createdAtEpochMs = attrs.creationTime().toMillis();
        } catch (IOException e) {
            // Fallback to last modified time if creation time is not available
            try {
                createdAtEpochMs = Files.getLastModifiedTime(filePath).toMillis();
            } catch (IOException ignored) {
                // Keep fallback to current time
            }
        }
        return openInternal(filePath, segmentNumber, readOnly, createdAtEpochMs);
    }

    private static WALSegment openInternal(Path filePath, int segmentNumber, boolean readOnly) throws IOException {
        return openInternal(filePath, segmentNumber, readOnly, System.currentTimeMillis());
    }

    private static WALSegment openInternal(Path filePath, int segmentNumber, boolean readOnly, long createdAtEpochMs) throws IOException {
        WALSegment segment = new WALSegment(segmentNumber, filePath, readOnly, createdAtEpochMs);
        segment.channel = readOnly
                ? FileChannel.open(filePath, StandardOpenOption.READ)
                : FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
        segment.writePosition = segment.channel.size();
        segment.readHeader();
        return segment;
    }

    private WALSegment(int segmentNumber, Path filePath, boolean readOnly, long createdAtEpochMs) {
        this.segmentNumber = segmentNumber;
        this.filePath = filePath;
        this.readOnly = readOnly;
        this.createdAtEpochMs = createdAtEpochMs;
    }

    /**
     * Reads the segment header if present and valid.
     *
     * @throws IOException if the header is corrupt
     */
    private void readHeader() throws IOException {
        if (writePosition < WALFormat.SEGMENT_HEADER_V1_SIZE) {
            // Empty or incomplete file: header not written yet
            headerWritten = false;
            headerSize = 0;
            firstLsn = 0;
            lastLsn = 0;
            return;
        }

        // Read magic and version first to determine header size
        ByteBuffer probeBuffer = ByteBuffer.allocate(6);
        channel.read(probeBuffer, 0);
        probeBuffer.flip();

        byte[] magicBytes = new byte[4];
        probeBuffer.get(magicBytes);
        String magic = new String(magicBytes);
        if (!magic.equals(WALFormat.SEGMENT_MAGIC)) {
            throw new WALFormatException("Invalid segment magic: expected " + WALFormat.SEGMENT_MAGIC + 
                    ", got " + magic);
        }

        short version = probeBuffer.getShort();
        if (version == WALFormat.SEGMENT_FORMAT_VERSION_V1) {
            // Legacy version 1: 24-byte header
            headerSize = WALFormat.SEGMENT_HEADER_V1_SIZE;
            headerVersion = WALFormat.SEGMENT_FORMAT_VERSION_V1;
        } else if (version == WALFormat.SEGMENT_FORMAT_VERSION) {
            // Current version 2: 32-byte header
            if (writePosition < WALFormat.SEGMENT_HEADER_SIZE) {
                throw new WALFormatException("Segment file too small for version 2 header: " + writePosition + " bytes");
            }
            headerSize = WALFormat.SEGMENT_HEADER_SIZE;
            headerVersion = WALFormat.SEGMENT_FORMAT_VERSION;
        } else {
            throw new WALFormatException("Unsupported segment version: " + version);
        }

        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
        channel.read(headerBuffer, 0);
        headerBuffer.flip();

        // Skip magic (4) + version (2) + reserved (2)
        headerBuffer.position(WALFormat.SEGMENT_OFFSET_RESERVED);
        headerBuffer.getShort();

        // Read segment number
        int readSegmentNumber = headerBuffer.getInt();
        if (readSegmentNumber != segmentNumber) {
            throw new WALFormatException("Segment number mismatch: file has " + readSegmentNumber + 
                    ", expected " + segmentNumber);
        }

        // Read first LSN
        firstLsn = headerBuffer.getLong();

        // Read last LSN (version 2 only)
        if (headerVersion == WALFormat.SEGMENT_FORMAT_VERSION) {
            lastLsn = headerBuffer.getLong();
            persistedLastLsn = lastLsn;
        } else {
            lastLsn = 0; // Legacy version 1: lastLsn computed lazily
            persistedLastLsn = 0;
        }

        // Validate CRC
        int storedCrc = headerBuffer.getInt();
        headerBuffer.rewind();
        headerBuffer.limit(headerVersion == WALFormat.SEGMENT_FORMAT_VERSION ? 
                          WALFormat.SEGMENT_OFFSET_CRC : WALFormat.SEGMENT_OFFSET_CRC_V1);
        int computedCrc = computeCrc(headerBuffer);
        if (computedCrc != storedCrc) {
            throw new WALFormatException("Segment header CRC32C mismatch: expected " + 
                    storedCrc + ", got " + computedCrc);
        }

        headerWritten = true;
    }

    /**
     * Writes the segment header if not already written.
     *
     * @throws IOException if the header cannot be written
     */
    private void ensureHeaderWritten() throws IOException {
        if (!headerWritten) {
            // Initialize header for version 2 (new segments always use version 2)
            headerSize = WALFormat.SEGMENT_HEADER_SIZE;
            headerVersion = WALFormat.SEGMENT_FORMAT_VERSION;
            
            ByteBuffer header = ByteBuffer.allocate(headerSize);
            
            // Magic
            header.put(WALFormat.SEGMENT_MAGIC.getBytes());
            
            // Version
            header.putShort((short) WALFormat.SEGMENT_FORMAT_VERSION);
            
            // Reserved
            header.putShort((short) 0);
            
            // Segment number
            header.putInt(segmentNumber);
            
            // First LSN (will be updated when first entry is written)
            firstLsn = 0;
            lastLsn = 0; // Will be set on first append
            persistedLastLsn = 0;
            header.putLong(firstLsn);
            header.putLong(lastLsn); // lastLsn field in version 2
            
            // CRC (computed over first 28 bytes for version 2)
            header.flip();
            header.limit(WALFormat.SEGMENT_OFFSET_CRC);
            int crc = computeCrc(header);
            header.limit(headerSize);
            header.putInt(crc);
            
            // Write header
            header.rewind();
            channel.write(header, 0);
            writePosition = headerSize;
            headerWritten = true;
        }
    }

    /**
     * Appends a WAL entry to this segment.
     *
     * @param entry the entry to append
     * @throws IOException if the entry cannot be written
     */
    public void append(WALEntry entry) throws IOException {
        if (readOnly) {
            throw new IllegalStateException("Cannot append to read-only segment");
        }

        ensureHeaderWritten();

        int entrySize = entry.encodedSize();
        if (writePosition + entrySize > Integer.MAX_VALUE) {
            throw new IOException("Segment too large: position=" + writePosition + 
                    ", entrySize=" + entrySize);
        }

        ByteBuffer buffer = ByteBuffer.allocate(entrySize);
        entry.writeTo(buffer);
        buffer.flip();
        channel.write(buffer, writePosition);
        writePosition += entrySize;

        // Update first LSN if this is the first entry
        if (firstLsn == 0) {
            firstLsn = entry.getLsn();
            lastLsn = entry.getLsn(); // First entry = last entry
            persistedLastLsn = lastLsn;
            rewriteHeader();
        }
        
        // Update last LSN to this entry
        lastLsn = entry.getLsn();
    }

    /**
     * Appends a batch of WAL entries with a single channel write.
     *
     * <p>The batch is encoded into one buffer and written with as few
     * {@code channel.write} calls as possible: a per-entry positional write
     * costs ~12us on Windows, which caps single-entry throughput at ~85k/s;
     * one write per batch removes that fixed cost (prompt4.md step 13).
     *
     * @param entries the entries to append, in LSN order; must not be empty
     * @throws IOException if the entries cannot be written
     */
    public void appendBatch(List<WALEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        if (readOnly) {
            throw new IllegalStateException("Cannot append to read-only segment");
        }

        ensureHeaderWritten();

        long totalSize = 0;
        for (WALEntry entry : entries) {
            totalSize += entry.encodedSize();
        }
        if (writePosition + totalSize > Integer.MAX_VALUE) {
            throw new IOException("Segment too large: position=" + writePosition +
                    ", batchSize=" + totalSize);
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) totalSize);
        for (WALEntry entry : entries) {
            entry.writeTo(buffer);
        }
        buffer.flip();

        long filePos = writePosition;
        while (buffer.hasRemaining()) {
            filePos += channel.write(buffer, filePos);
        }
        writePosition += totalSize;

        // Update first LSN if these are the first entries
        if (firstLsn == 0) {
            firstLsn = entries.get(0).getLsn();
            lastLsn = entries.get(entries.size() - 1).getLsn(); // First batch: first=last
            persistedLastLsn = lastLsn;
            rewriteHeader();
        }
        
        // Update last LSN to the last entry in the batch
        lastLsn = entries.get(entries.size() - 1).getLsn();
    }

    /**
     * Rewrites the segment header with updated first LSN.
     *
     * @throws IOException if the header cannot be rewritten
     */
    private void rewriteHeader() throws IOException {
        if (headerSize != WALFormat.SEGMENT_HEADER_SIZE) {
            // Only rewrite version 2 headers (legacy v1 cannot be upgraded in-place)
            return;
        }
        
        ByteBuffer header = ByteBuffer.allocate(headerSize);
        
        // Magic
        header.put(WALFormat.SEGMENT_MAGIC.getBytes());
        
        // Version
        header.putShort((short) WALFormat.SEGMENT_FORMAT_VERSION);
        
        // Reserved
        header.putShort((short) 0);
        
        // Segment number
        header.putInt(segmentNumber);
        
        // First LSN and last LSN (version 2)
        header.putLong(firstLsn);
        header.putLong(lastLsn);
        
        // CRC (computed over first 28 bytes for version 2)
        header.flip();
        header.limit(WALFormat.SEGMENT_OFFSET_CRC);
        int crc = computeCrc(header);
        header.limit(headerSize);
        header.putInt(crc);
        
        // Rewrite header
        header.rewind();
        channel.write(header, 0);
    }

    /**
     * Reads all complete entries from this segment.
     * Stops at the first incomplete entry (torn write).
     *
     * @return list of entries (empty if none)
     * @throws IOException if the segment cannot be read
     */
    public java.util.List<WALEntry> readAll() throws IOException {
        java.util.List<WALEntry> entries = new java.util.ArrayList<>();
        if (channel == null) {
            throw new IllegalStateException("Segment " + segmentNumber + " is closed");
        }
        if (writePosition < headerSize) {
            return entries; // Empty segment
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) (writePosition - headerSize));
        channel.read(buffer, headerSize);
        buffer.flip();

        while (buffer.remaining() >= WALFormat.MIN_ENTRY_SIZE) {
            try {
                int position = buffer.position();
                WALEntry entry = WALEntry.readFrom(buffer);
                entries.add(entry);
            } catch (WALFormatException e) {
                // Incomplete entry: stop reading
                LOGGER.debug("Stopped reading segment {} at position {}: {}", 
                        segmentNumber, buffer.position(), e.getMessage());
                break;
            }
        }

        return entries;
    }

    /**
     * Reads a WAL entry by LSN with early exit.
     * Streams the segment file with a ByteBuffer, decoding entries one at a time
     * and returning on first match. This is O(position-of-LSN-within-segment)
     * and avoids the full readAll().
     *
     * @param lsn the LSN to read
     * @return the entry, or null if not found
     * @throws IOException if reading fails
     */
    public WALEntry readByLsn(long lsn) throws IOException {
        if (channel == null) {
            throw new IllegalStateException("Segment " + segmentNumber + " is closed");
        }
        if (writePosition < headerSize) {
            return null; // Empty segment
        }
        if (firstLsn > 0 && lsn < firstLsn) {
            return null; // LSN before first entry in segment
        }

        final int CHUNK_SIZE = 64 * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(CHUNK_SIZE);
        long filePos = headerSize;
        int n = channel.read(buffer, filePos);
        if (n > 0) {
            filePos += n;
        }
        buffer.flip();

        while (true) {
            // Decode as many complete entries as possible from current buffer
            while (buffer.remaining() >= WALFormat.MIN_ENTRY_SIZE) {
                int mark = buffer.position();
                
                // Peek fixed header to get entry size
                long entryLsn;
                int beforeLen;
                int afterLen;
                try {
                    entryLsn = buffer.getLong(mark);
                    // Skip txid, op, flags, reserved
                    buffer.position(mark + 20);
                    beforeLen = buffer.getInt();
                    afterLen = buffer.getInt();
                    buffer.position(mark); // Restore position
                } catch (Exception e) {
                    // Should not happen due to MIN_ENTRY_SIZE check, but defensive
                    break;
                }

                // Validate image lengths
                if (beforeLen < 0 || afterLen < 0) {
                    return null; // Corrupt header - mirror readAll stop semantics
                }

                int fullSize;
                try {
                    fullSize = WALFormat.entrySize(beforeLen, afterLen);
                } catch (IllegalArgumentException e) {
                    return null; // Absurd lengths - mirror readAll stop semantics
                }

                if (entryLsn > lsn) {
                    // LSN not found - passed the target
                    return null;
                }

                if (buffer.remaining() < fullSize) {
                    // Incomplete entry - need more data
                    break;
                }

                if (entryLsn == lsn) {
                    // Found the target entry - decode it
                    try {
                        return WALEntry.readFrom(buffer);
                    } catch (WALFormatException e) {
                        // Corrupt entry - mirror readAll stop semantics
                        return null;
                    }
                }

                // Skip this entry
                buffer.position(mark + fullSize);
            }

            // Check for EOF or need for more data
            if (n <= 0) {
                return null; // EOF - entry not found or torn tail
            }

            // Prepare next chunk
            buffer.compact();
            n = channel.read(buffer, filePos);
            if (n > 0) {
                filePos += n;
            }
            buffer.flip();
        }
    }

    /**
     * Forces any buffered writes to disk.
     *
     * @throws IOException if the force fails
     */
    public synchronized void force() throws IOException {
        if (channel == null) {
            return;
        }
        
        // Rewrite header if version 2 and lastLsn changed since last persist
        if (!readOnly && headerSize == WALFormat.SEGMENT_HEADER_SIZE && lastLsn != persistedLastLsn) {
            rewriteHeader();
            persistedLastLsn = lastLsn;
        }
        
        channel.force(true);
    }

    /**
     * Closes this segment.
     *
     * @throws IOException if the close fails
     */
    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    /**
     * Returns the segment number.
     *
     * @return the segment number
     */
    public int getNumber() {
        return segmentNumber;
    }

    /**
     * Returns the segment file path.
     *
     * @return the file path
     */
    public Path getFilePath() {
        return filePath;
    }

    /**
     * Returns the current write position.
     *
     * @return write position in bytes
     */
    public long getPosition() {
        return writePosition;
    }

    /**
     * Returns the size of this segment in bytes.
     *
     * @return segment size
     */
    public long getSize() {
        return writePosition;
    }

    /**
     * Returns the first LSN in this segment (0 if empty).
     *
     * @return first LSN
     */
    public long getFirstLsn() {
        return firstLsn;
    }

    /**
     * Returns the last LSN in this segment (0 if empty).
     * Computed lazily for segments opened from disk.
     *
     * @return last LSN
     */
    public synchronized long getLastLsn() {
        if (lastLsn == 0 && writePosition > headerSize) {
            // Lazy computation: try tail scan first, fallback to readAll
            try {
                WALEntry lastEntry = readLastEntryFromTail();
                if (lastEntry != null) {
                    lastLsn = lastEntry.getLsn();
                } else {
                    // Fallback: full readAll (for very large entries or torn tail)
                    List<WALEntry> entries = readAll();
                    if (!entries.isEmpty()) {
                        lastLsn = entries.get(entries.size() - 1).getLsn();
                    }
                }
            } catch (IOException e) {
                // If read fails, lastLsn remains 0 (empty segment)
                LOGGER.debug("Failed to compute lastLsn for segment {}: {}", segmentNumber, e.getMessage());
            }
        }
        return lastLsn;
    }

    /**
     * Returns whether this segment is read-only.
     *
     * @return true if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Returns the creation time of this segment in milliseconds since epoch.
     * For opened segments, this is the file creation time (or last modified time as fallback).
     * For newly created segments, this is the current time.
     *
     * @return creation time in milliseconds since epoch
     */
    public long getCreatedAtEpochMs() {
        return createdAtEpochMs;
    }

    /**
     * Computes CRC32C of the given buffer.
     *
     * @param buffer the buffer to checksum
     * @return the CRC32C value
     */
    private static int computeCrc(ByteBuffer buffer) {
        CRC32C crc = new CRC32C();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }

    @Override
    public String toString() {
        return String.format("WALSegment{number=%d, path=%s, size=%dB, firstLsn=%d, readOnly=%b}", 
                segmentNumber, filePath, writePosition, firstLsn, readOnly);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WALSegment.class);

    /**
     * Reads the last entry from the segment tail with backward scanning.
     * Reads the last 64KB chunk and scans backwards for the last complete entry.
     * Returns null if no valid entry is found (torn tail or empty).
     *
     * @return the last entry, or null if not found
     * @throws IOException if reading fails
     */
    private WALEntry readLastEntryFromTail() throws IOException {
        long end = writePosition;
        if (end <= headerSize) {
            return null; // Empty or no header
        }

        final int CHUNK_SIZE = 64 * 1024;
        long start = Math.max(headerSize, end - CHUNK_SIZE);
        int len = (int) (end - start);
        ByteBuffer buffer = ByteBuffer.allocate(len);
        channel.read(buffer, start);
        buffer.flip();

        // Scan backwards from the end of the buffer
        // Entry starts are 8-aligned: positions p where p >= headerSize, p % 8 == 0
        long lastValidPos = -1;
        int lastRawSize = 0;
        int lastFullSize = 0;

        for (long p = alignDown(end - WALFormat.MIN_ENTRY_SIZE); p >= start && p >= headerSize; p -= 8) {
            int bufferPos = (int) (p - start);
            if (bufferPos + 24 > buffer.remaining()) {
                continue; // Not enough data for fixed header
            }

            // Peek entry size info
            buffer.position(bufferPos);
            long entryLsn = buffer.getLong();
            // Skip txid, op, flags, reserved
            buffer.position(bufferPos + 20);
            int beforeLen = buffer.getInt();
            int afterLen = buffer.getInt();
            buffer.position(bufferPos); // Reset

            // Validate image lengths
            if (beforeLen < 0 || afterLen < 0) {
                continue; // Invalid header
            }

            try {
                int rawSize = WALFormat.rawEntrySize(beforeLen, afterLen);
                int fullSize = WALFormat.alignedSize(rawSize);
                
                // Check if this entry fits and ends exactly at or before end
                if (p + fullSize > end) {
                    continue; // Entry would extend beyond file end
                }

                // Check if this entry is valid (CRC optional here for performance)
                // For now, accept the first candidate that fits (should be the last entry)
                // If multiple candidates exist, the first found (largest p) is the true last
                lastValidPos = p;
                lastRawSize = rawSize;
                lastFullSize = fullSize;

                // Found a valid candidate - try to decode it
                buffer.position(bufferPos);
                try {
                    return WALEntry.readFrom(buffer);
                } catch (WALFormatException e) {
                    // Entry corrupt - continue scanning backwards
                    LOGGER.debug("Failed to decode entry at position {}: {}", p, e.getMessage());
                    continue;
                }
            } catch (IllegalArgumentException e) {
                // Invalid entry size - continue scanning
                continue;
            }
        }

        return null; // No valid entry found in the tail
    }

    /**
     * Aligns the given size down to the nearest multiple of 8.
     *
     * @param size the size to align
     * @return the aligned size
     */
    private static long alignDown(long size) {
        return size / WALFormat.ALIGNMENT * WALFormat.ALIGNMENT;
    }
}