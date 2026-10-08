package diesel.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single WAL segment file (prompt4.md step 12, R3-003 step 2/5).
 *
 * <p>Each segment file contains a 24-byte header followed by zero or more
 * WAL entries. Files are named {@code wal-NNNN.log} (4-digit zero-padded,
 * 1-based numbering). Segment headers are written lazily on the first append
 * to an empty segment (0-byte files are valid empty segments).
 *
 * <p>Thread-safety: not thread-safe (single-writer assumption for step 12).
 */
public final class WALSegment implements AutoCloseable {

    private final int segmentNumber;
    private final Path filePath;
    private final boolean readOnly;
    private FileChannel channel;
    private long writePosition;
    private boolean headerWritten;
    private long firstLsn;

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
        return openInternal(filePath, segmentNumber, false);
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
        return openInternal(filePath, segmentNumber, readOnly);
    }

    private static WALSegment openInternal(Path filePath, int segmentNumber, boolean readOnly) throws IOException {
        WALSegment segment = new WALSegment(segmentNumber, filePath, readOnly);
        segment.channel = readOnly
                ? FileChannel.open(filePath, StandardOpenOption.READ)
                : FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
        segment.writePosition = segment.channel.size();
        segment.readHeader();
        return segment;
    }

    private WALSegment(int segmentNumber, Path filePath, boolean readOnly) {
        this.segmentNumber = segmentNumber;
        this.filePath = filePath;
        this.readOnly = readOnly;
    }

    /**
     * Reads the segment header if present and valid.
     *
     * @throws IOException if the header is corrupt
     */
    private void readHeader() throws IOException {
        if (writePosition < WALFormat.SEGMENT_HEADER_SIZE) {
            // Empty or incomplete file: header not written yet
            headerWritten = false;
            firstLsn = 0;
            return;
        }

        ByteBuffer headerBuffer = ByteBuffer.allocate(WALFormat.SEGMENT_HEADER_SIZE);
        channel.read(headerBuffer, 0);
        headerBuffer.flip();

        // Read and validate magic
        byte[] magicBytes = new byte[4];
        headerBuffer.get(magicBytes);
        String magic = new String(magicBytes);
        if (!magic.equals(WALFormat.SEGMENT_MAGIC)) {
            throw new WALFormatException("Invalid segment magic: expected " + WALFormat.SEGMENT_MAGIC + 
                    ", got " + magic);
        }

        // Read and validate version
        short version = headerBuffer.getShort();
        if (version != WALFormat.SEGMENT_FORMAT_VERSION) {
            throw new WALFormatException("Unsupported segment version: " + version);
        }

        // Skip reserved
        headerBuffer.getShort();

        // Read segment number
        int readSegmentNumber = headerBuffer.getInt();
        if (readSegmentNumber != segmentNumber) {
            throw new WALFormatException("Segment number mismatch: file has " + readSegmentNumber + 
                    ", expected " + segmentNumber);
        }

        // Read first LSN
        firstLsn = headerBuffer.getLong();

        // Validate CRC
        int storedCrc = headerBuffer.getInt();
        headerBuffer.rewind();
        headerBuffer.limit(WALFormat.SEGMENT_OFFSET_CRC);
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
            ByteBuffer header = ByteBuffer.allocate(WALFormat.SEGMENT_HEADER_SIZE);
            
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
            header.putLong(firstLsn);
            
            // CRC (computed over first 20 bytes)
            header.flip();
            header.limit(WALFormat.SEGMENT_OFFSET_CRC);
            int crc = computeCrc(header);
            header.limit(WALFormat.SEGMENT_HEADER_SIZE);
            header.putInt(crc);
            
            // Write header
            header.rewind();
            channel.write(header, 0);
            writePosition = WALFormat.SEGMENT_HEADER_SIZE;
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
            rewriteHeader();
        }
    }

    /**
     * Rewrites the segment header with updated first LSN.
     *
     * @throws IOException if the header cannot be rewritten
     */
    private void rewriteHeader() throws IOException {
        ByteBuffer header = ByteBuffer.allocate(WALFormat.SEGMENT_HEADER_SIZE);
        
        // Magic
        header.put(WALFormat.SEGMENT_MAGIC.getBytes());
        
        // Version
        header.putShort((short) WALFormat.SEGMENT_FORMAT_VERSION);
        
        // Reserved
        header.putShort((short) 0);
        
        // Segment number
        header.putInt(segmentNumber);
        
        // First LSN
        header.putLong(firstLsn);
        
        // CRC
        header.flip();
        header.limit(WALFormat.SEGMENT_OFFSET_CRC);
        int crc = computeCrc(header);
        header.limit(WALFormat.SEGMENT_HEADER_SIZE);
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
        if (writePosition < WALFormat.SEGMENT_HEADER_SIZE) {
            return entries; // Empty segment
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) (writePosition - WALFormat.SEGMENT_HEADER_SIZE));
        channel.read(buffer, WALFormat.SEGMENT_HEADER_SIZE);
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
     * Forces any buffered writes to disk.
     *
     * @throws IOException if the force fails
     */
    public void force() throws IOException {
        if (channel != null) {
            channel.force(true);
        }
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
     * Returns whether this segment is read-only.
     *
     * @return true if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
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
}