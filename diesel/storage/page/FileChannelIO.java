package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Low-level page file I/O using FileChannel with configurable read modes.
 * <p>
 * Supports three I/O modes:
 * - <b>positional</b>: Heap ByteBuffer + FileChannel.read/write (default, safest)
 * - <b>mapped</b>: FileChannel.map(READ_ONLY, offset, pageSize) for zero-copy read
 * - <b>odirect</b>: Reserved for future Linux O_DIRECT (JNI not implemented, WARN+fallback)
 * <p>
 * All writes are followed by channel.force(true) for durability.
 */
final class FileChannelIO implements AutoCloseable {

    public static final String MODE_KEY = "page.io.mode";
    public static final String MODE_DEFAULT = "positional";
    public static final String MODE_POSITIONAL = "positional";
    public static final String MODE_MAPPED = "mapped";
    public static final String MODE_ODIRECT = "odirect";

    private final FileChannel channel;
    private final int pageSize;
    private final String mode;
    private final boolean useMapped;

    /**
     * Opens or creates a page file for random access.
     *
     * @param file path to the page file
     * @param pageSize page size in bytes (must be > 0)
     * @throws IOException if the file cannot be opened
     */
    FileChannelIO(Path file, int pageSize) throws IOException {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be positive: " + pageSize);
        }
        this.pageSize = pageSize;

        // Resolve I/O mode from system property → config → default
        String rawMode = System.getProperty(MODE_KEY, MODE_DEFAULT);
        String resolvedMode = rawMode.toLowerCase();
        boolean resolvedMapped = MODE_MAPPED.equals(resolvedMode);

        // O_DIRECT is reserved for future JNI implementation (not supported yet)
        if (MODE_ODIRECT.equals(resolvedMode)) {
            System.err.println("WARN: O_DIRECT mode for page I/O is not implemented (no JNI support); falling back to positional mode");
            resolvedMode = MODE_POSITIONAL;
            resolvedMapped = false;
        }

        this.mode = resolvedMode;
        this.useMapped = resolvedMapped;

        // Open channel with read/write, create if missing, truncate if empty
        this.channel = FileChannel.open(
            file,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);
    }

    /**
     * Reads a page at the given PageId's offset.
     * Uses FileChannel.read(ByteBuffer, offset) by default,
     * or FileChannel.map for zero-copy in mapped mode.
     *
     * @param pageId identifies the page (fileOffset = pageNum * pageSize)
     * @return page data as ByteBuffer (position=0, limit=pageSize)
     * @throws IOException on read failure or short read
     */
    ByteBuffer readPage(PageId pageId) throws IOException {
        long offset = pageId.fileOffset(pageSize);
        ByteBuffer buffer;

        if (useMapped) {
            // Zero-copy mapped read (fallback to positional if map fails)
            try {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, pageSize);
            } catch (IOException | UnsupportedOperationException e) {
                System.err.println("WARN: FileChannel.map failed for page " + pageId + ", falling back to positional read: " + e.getMessage());
                buffer = ByteBuffer.allocate(pageSize);
                readPositional(buffer, offset);
            }
        } else {
            // Standard positional read into heap buffer
            buffer = ByteBuffer.allocate(pageSize);
            readPositional(buffer, offset);
        }

        buffer.flip(); // position=0, limit=pageSize
        return buffer;
    }

    /**
     * Writes a page buffer at the given PageId's offset.
     * Always uses positional write followed by force.
     *
     * @param pageId identifies the page (fileOffset = pageNum * pageSize)
     * @param buffer page data (position=0, limit=pageSize)
     * @throws IOException on write failure
     */
    void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        if (buffer.remaining() != pageSize) {
            throw new IllegalArgumentException("buffer size must match page size: expected " + pageSize + ", got " + buffer.remaining());
        }

        long offset = pageId.fileOffset(pageSize);
        writePositional(buffer, offset);
        channel.force(true); // ensure durability
    }

    /**
     * Extends the file to accommodate at least the given offset + pageSize.
     * Fills any new space with zeros.
     *
     * @param offsetPlusSize required file size (offset + pageSize)
     * @throws IOException on file extension failure
     */
    void extendTo(long offsetPlusSize) throws IOException {
        long currentSize = channel.size();
        if (offsetPlusSize <= currentSize) {
            return; // no extension needed
        }

        // Write zeros to extend the file
        ByteBuffer zeros = ByteBuffer.allocate((int) (offsetPlusSize - currentSize));
        while (zeros.hasRemaining()) {
            zeros.put((byte) 0);
        }
        zeros.flip();
        writePositional(zeros, currentSize);
        channel.force(true);
    }

    /**
     * Returns the current file size in bytes.
     *
     * @throws IOException if size cannot be determined
     */
    long size() throws IOException {
        return channel.size();
    }

    /**
     * Forces all buffered writes to disk (sync).
     *
     * @throws IOException on force failure
     */
    void force() throws IOException {
        channel.force(true);
    }

    /**
     * Closes the underlying FileChannel.
     * Idempotent and safe to call multiple times.
     */
    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    // --- Private helpers ---

    /**
     * Reads data at the given offset into the buffer using positional read.
     * Ensures the full buffer is filled or throws IOException.
     */
    private void readPositional(ByteBuffer buffer, long offset) throws IOException {
        buffer.clear();
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, offset + totalRead);
            if (n < 0) {
                throw new IOException("short read: expected " + pageSize + " bytes, got " + totalRead + " at offset " + offset);
            }
            totalRead += n;
        }
    }

    /**
     * Writes the buffer at the given offset using positional write.
     * Ensures the full buffer is written.
     */
    private void writePositional(ByteBuffer buffer, long offset) throws IOException {
        int totalWritten = 0;
        while (buffer.hasRemaining()) {
            int n = channel.write(buffer, offset + totalWritten);
            if (n <= 0) {
                throw new IOException("write failed: wrote " + totalWritten + " of " + buffer.capacity() + " bytes at offset " + offset);
            }
            totalWritten += n;
        }
    }
}