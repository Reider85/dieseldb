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
 * - <b>odirect</b>: O_DIRECT I/O using native library for optimal performance
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
    private final boolean useO_DIRECT;
    private final O_DIRECT_IO o_directIO;

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

        this.mode = resolvedMode;
        this.useMapped = resolvedMapped;
        boolean useO_DIRECT_temp = MODE_ODIRECT.equals(resolvedMode);
        O_DIRECT_IO o_directIO_temp = null;
        FileChannel channel_temp = null;

        if (useO_DIRECT_temp) {
            // Initialize O_DIRECT I/O
            try {
                o_directIO_temp = new O_DIRECT_IO(file, pageSize);
                channel_temp = null; // Not used in O_DIRECT mode
            } catch (IOException e) {
                System.err.println("WARN: O_DIRECT mode failed, falling back to positional mode: " + e.getMessage());
                useO_DIRECT_temp = false;
                o_directIO_temp = null;
                channel_temp = FileChannel.open(
                    file,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            }
        } else {
            o_directIO_temp = null;
            // Open channel with read/write, create if missing, truncate if empty
            channel_temp = FileChannel.open(
                file,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
        }
        
        this.useO_DIRECT = useO_DIRECT_temp;
        this.o_directIO = o_directIO_temp;
        this.channel = channel_temp;
    }

    /**
     * Reads a page at the given PageId's offset.
     * Uses O_DIRECT I/O when enabled, otherwise uses FileChannel.read(ByteBuffer, offset) by default,
     * or FileChannel.map for zero-copy in mapped mode.
     *
     * @param pageId identifies the page (fileOffset = pageNum * pageSize)
     * @return page data as ByteBuffer (position=0, limit=pageSize)
     * @throws IOException on read failure or short read
     */
    ByteBuffer readPage(PageId pageId) throws IOException {
        if (useO_DIRECT) {
            return o_directIO.readPage(pageId);
        }
        
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
     * Uses O_DIRECT I/O when enabled, otherwise uses positional write followed by force.
     *
     * @param pageId identifies the page (fileOffset = pageNum * pageSize)
     * @param buffer page data (position=0, limit=pageSize)
     * @throws IOException on write failure
     */
    void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        if (buffer.remaining() != pageSize) {
            throw new IllegalArgumentException("buffer size must match page size: expected " + pageSize + ", got " + buffer.remaining());
        }

        if (useO_DIRECT) {
            o_directIO.writePage(pageId, buffer);
            o_directIO.force(); // ensure durability
            return;
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
        if (useO_DIRECT) {
            o_directIO.extendTo(offsetPlusSize);
            return;
        }

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
        if (useO_DIRECT) {
            return o_directIO.size();
        }
        return channel.size();
    }

    /**
     * Forces all buffered writes to disk (sync).
     *
     * @throws IOException on force failure
     */
    void force() throws IOException {
        if (useO_DIRECT) {
            o_directIO.force();
            return;
        }
        channel.force(true);
    }

    /**
     * Closes the underlying FileChannel or O_DIRECT I/O.
     * Idempotent and safe to call multiple times.
     */
    @Override
    public void close() throws IOException {
        if (useO_DIRECT) {
            if (o_directIO != null) {
                o_directIO.close();
            }
        } else {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    /**
     * Returns the current I/O mode for testing.
     *
     * @return the current I/O mode string
     */
    public String getIoMode() {
        if (useO_DIRECT) {
            return MODE_ODIRECT;
        } else if (useMapped) {
            return MODE_MAPPED;
        } else {
            return MODE_POSITIONAL;
        }
    }
    
    /**
     * Checks if O_DIRECT mode is enabled.
     *
     * @return true if O_DIRECT mode is enabled, false otherwise
     */
    public boolean isUsingO_DIRECT() {
        return useO_DIRECT;
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