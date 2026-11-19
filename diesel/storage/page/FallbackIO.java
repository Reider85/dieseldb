package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Fallback I/O implementation using regular FileChannel when O_DIRECT is not available.
 */
public class FallbackIO implements AutoCloseable, PageLoader, PageFlusher {
    
    private final FileChannel channel;
    private final int pageSize;
    private boolean isOpen;
    
    /**
     * Creates a new fallback I/O instance.
     * 
     * @param file path to the file to open
     * @param pageSize page size in bytes
     * @throws IOException if the file cannot be opened
     */
    public FallbackIO(Path file, int pageSize) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be positive: " + pageSize);
        }
        
        this.pageSize = pageSize;
        
        // Open channel with read/write, create if missing
        this.channel = FileChannel.open(
            file,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);
        
        this.isOpen = true;
    }
    
    /**
     * Reads a page from the file at the specified PageId offset.
     * 
     * @param pageId identifies the page to read
     * @return the page data as a ByteBuffer (position=0, limit=pageSize)
     * @throws IOException if read fails
     */
    public ByteBuffer readPage(PageId pageId) throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        long offset = (long) pageId.pageNum() * pageSize;
        
        int bytesRead = channel.read(buffer, offset);
        if (bytesRead != pageSize) {
            throw new IOException("Failed to read complete page: " + bytesRead + " bytes read");
        }
        
        buffer.flip();
        return buffer;
    }
    
    /**
     * Writes a page to the file at the specified PageId offset.
     * 
     * @param page the page to write
     * @throws IOException if write fails
     */
    public void writePage(Page page) throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        
        ByteBuffer buffer = page.getBuffer();
        buffer.rewind(); // Reset position to 0
        long offset = (long) page.getPageId().pageNum() * pageSize;
        
        int bytesWritten = channel.write(buffer, offset);
        if (bytesWritten != pageSize) {
            throw new IOException("Failed to write complete page: " + bytesWritten + " bytes written");
        }
    }
    
    /**
     * Writes a page to the file at the specified PageId offset.
     * 
     * @param pageId identifies the page to write
     * @param buffer the page data to write
     * @throws IOException if write fails
     */
    public void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        
        buffer.rewind(); // Reset position to 0
        long offset = (long) pageId.pageNum() * pageSize;
        
        int bytesWritten = channel.write(buffer, offset);
        if (bytesWritten != pageSize) {
            throw new IOException("Failed to write complete page: " + bytesWritten + " bytes written");
        }
    }
    
    /**
     * Loads a page from disk.
     * 
     * @param pageId identifies the page to load
     * @return the loaded page
     * @throws IOException if read fails
     */
    @Override
    public Page load(PageId pageId) throws IOException {
        ByteBuffer buffer = readPage(pageId);
        byte[] data = new byte[pageSize];
        buffer.get(data);
        Page page = new Page(pageId, pageSize);
        page.setData(data);
        return page;
    }
    
    /**
     * Flushes a page to disk.
     * 
     * @param page the page to flush
     * @throws IOException if write fails
     */
    @Override
    public void flush(Page page) throws IOException {
        writePage(page.getPageId(), page.getBuffer());
    }
    
    /**
     * Extends the file to the specified size.
     * 
     * @param size required file size in bytes
     * @throws IOException if file extension fails
     */
    public void extendTo(long size) throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size must be non-negative: " + size);
        }
        
        channel.truncate(size);
    }
    
    /**
     * Returns the current file size in bytes.
     * 
     * @return the file size in bytes
     * @throws IOException if size cannot be determined
     */
    public long size() throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        
        return channel.size();
    }
    
    /**
     * Forces all buffered writes to disk (sync).
     * 
     * @throws IOException if sync fails
     */
    public void force() throws IOException {
        if (!isOpen) {
            throw new IOException("FallbackIO is closed");
        }
        
        channel.force(true);
    }
    
    /**
     * Closes the fallback I/O instance.
     * 
     * @throws IOException if close fails
     */
    @Override
    public void close() throws IOException {
        if (isOpen) {
            try {
                channel.close();
            } finally {
                this.isOpen = false;
            }
        }
    }
    
    /**
     * Checks if this I/O instance is open.
     * 
     * @return true if open, false if closed
     */
    public boolean isOpen() {
        return isOpen;
    }
    
    /**
     * Gets the page size used by this I/O instance.
     * 
     * @return the page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }
    
    /**
     * Checks if this I/O instance is using O_DIRECT.
     * 
     * @return false (this is a fallback implementation)
     */
    public boolean isUsingO_DIRECT() {
        return false;
    }
}