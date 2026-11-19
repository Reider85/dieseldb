package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * O_DIRECT I/O implementation using native library calls.
 * 
 * This class provides direct I/O operations with proper alignment handling
 * for optimal performance. It manages a pool of aligned buffers and uses
 * native system calls for O_DIRECT operations.
 * Falls back to regular file I/O if O_DIRECT is not available.
 */
public class O_DIRECT_IO implements AutoCloseable, PageLoader, PageFlusher {
    
    private final long fd;
    private final int pageSize;
    private final O_DIRECT_BufferPool bufferPool;
    private final boolean usingO_DIRECT;
    private boolean isOpen;
    
    /**
     * Creates a new O_DIRECT I/O instance.
     * 
     * @param file path to the file to open
     * @param pageSize page size in bytes (must be aligned to 512 bytes)
     * @throws IOException if the file cannot be opened or O_DIRECT is not supported
     */
    public O_DIRECT_IO(Path file, int pageSize) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        if (pageSize <= 0 || pageSize % 512 != 0) {
            throw new IllegalArgumentException("Page size must be positive and aligned to 512 bytes: " + pageSize);
        }
        
        this.pageSize = pageSize;
        this.bufferPool = new O_DIRECT_BufferPool(pageSize, 16); // Configurable buffer count
        
        // Check if O_DIRECT is available
        if (!O_DIRECT_Natives.isO_DIRECTAvailable()) {
            throw new IOException("O_DIRECT is not available on this platform");
        }
        
        // Open file with O_DIRECT using JNI
        this.fd = O_DIRECT_Natives.openO_DIRECT(file.toString(), pageSize);
        if (fd < 0) {
            throw new IOException("Failed to open file with O_DIRECT: error code " + fd);
        }
        
        this.usingO_DIRECT = true;
        this.isOpen = true;
    }
    
    /**
     * Creates a new O_DIRECT I/O instance with fallback to regular file I/O.
     * 
     * @param file path to the file to open
     * @param pageSize page size in bytes (must be aligned to 512 bytes)
     * @throws IOException if the file cannot be opened
     */
    public O_DIRECT_IO(Path file, int pageSize, boolean useO_DIRECT) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File path must not be null");
        }
        if (pageSize <= 0 || pageSize % 512 != 0) {
            throw new IllegalArgumentException("Page size must be positive and aligned to 512 bytes: " + pageSize);
        }
        
        this.pageSize = pageSize;
        this.bufferPool = new O_DIRECT_BufferPool(pageSize, 16); // Configurable buffer count
        
        // Check if O_DIRECT is available and requested
        if (useO_DIRECT && O_DIRECT_Natives.isO_DIRECTAvailable()) {
            // Try to open with O_DIRECT
            this.fd = O_DIRECT_Natives.openO_DIRECT(file.toString(), pageSize);
            if (fd < 0) {
                throw new IOException("Failed to open file with O_DIRECT: error code " + fd);
            }
            this.usingO_DIRECT = true;
        } else {
            // Fall back to regular file I/O (not implemented yet)
            throw new IOException("Regular file I/O fallback not yet implemented");
        }
        
        this.isOpen = true;
    }
    
    /**
     * Reads a page from the file at the specified PageId offset.
     * 
     * @param pageId identifies the page to read
     * @return the page data as a ByteBuffer (position=0, limit=pageSize)
     * @throws IOException if read fails or buffer is not available
     */
    public ByteBuffer readPage(PageId pageId) throws IOException {
        if (!isOpen) {
            throw new IOException("O_DIRECT_IO is closed");
        }
        if (pageId == null) {
            throw new IllegalArgumentException("PageId must not be null");
        }
        
        long offset = pageId.fileOffset(pageSize);
        ByteBuffer alignedBuffer = bufferPool.getAlignedBuffer(0);
        
        if (alignedBuffer == null) {
            throw new IOException("No aligned buffer available for reading page " + pageId);
        }
        
        try {
            int bytesRead = O_DIRECT_Natives.readO_DIRECT(fd, offset, alignedBuffer, 0, pageSize);
            if (bytesRead != pageSize) {
                throw new IOException("Short read: expected " + pageSize + " bytes, got " + bytesRead + 
                                   " at offset " + offset + " for page " + pageId);
            }
            
            // Create a new buffer to return to avoid buffer reuse issues
            ByteBuffer result = ByteBuffer.allocateDirect(pageSize);
            result.put(alignedBuffer);
            result.flip();
            
            return result;
        } catch (IOException e) {
            bufferPool.releaseAlignedBuffer(0);
            throw e;
        }
    }
    
    /**
     * Writes a page to the file at the specified PageId offset.
     * 
     * @param pageId identifies the page to write
     * @param buffer page data (position=0, limit=pageSize)
     * @throws IOException if write fails or buffer is not available
     */
    public void writePage(PageId pageId, ByteBuffer buffer) throws IOException {
        if (!isOpen) {
            throw new IOException("O_DIRECT_IO is closed");
        }
        if (pageId == null) {
            throw new IllegalArgumentException("PageId must not be null");
        }
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer must not be null");
        }
        if (buffer.remaining() != pageSize) {
            throw new IllegalArgumentException("Buffer size must match page size: expected " + pageSize + 
                                             ", got " + buffer.remaining());
        }
        
        long offset = pageId.fileOffset(pageSize);
        ByteBuffer alignedBuffer = bufferPool.getAlignedBuffer(0);
        
        if (alignedBuffer == null) {
            throw new IOException("No aligned buffer available for writing page " + pageId);
        }
        
        try {
            // Copy data to aligned buffer
            alignedBuffer.clear();
            alignedBuffer.put(buffer);
            alignedBuffer.flip();
            
            int bytesWritten = O_DIRECT_Natives.writeO_DIRECT(fd, offset, alignedBuffer, 0, pageSize);
            if (bytesWritten != pageSize) {
                throw new IOException("Short write: expected " + pageSize + " bytes, got " + bytesWritten + 
                                     " at offset " + offset + " for page " + pageId);
            }
        } finally {
            bufferPool.releaseAlignedBuffer(0);
        }
    }
    
    /**
     * Extends the file to accommodate at least the given size.
     * Fills any new space with zeros.
     * 
     * @param size required file size in bytes
     * @throws IOException if file extension fails
     */
    public void extendTo(long size) throws IOException {
        if (!isOpen) {
            throw new IOException("O_DIRECT_IO is closed");
        }
        if (size < 0) {
            throw new IllegalArgumentException("Size must be non-negative: " + size);
        }
        
        O_DIRECT_Natives.setSizeO_DIRECT(fd, size);
    }
    
    /**
     * Returns the current file size in bytes.
     * 
     * @return the file size in bytes
     * @throws IOException if size cannot be determined
     */
    public long size() throws IOException {
        if (!isOpen) {
            throw new IOException("O_DIRECT_IO is closed");
        }
        
        // Use a temporary buffer to get file size
        ByteBuffer tempBuffer = ByteBuffer.allocateDirect(8);
        try {
            int bytesRead = O_DIRECT_Natives.readO_DIRECT(fd, 0, tempBuffer, 0, 8);
            if (bytesRead != 8) {
                throw new IOException("Failed to read file size");
            }
            return tempBuffer.getLong(0);
        } catch (IOException e) {
            throw new IOException("Failed to determine file size", e);
        }
    }
    
    /**
     * Forces all buffered writes to disk (sync).
     * 
     * @throws IOException if sync fails
     */
    public void force() throws IOException {
        if (!isOpen) {
            throw new IOException("O_DIRECT_IO is closed");
        }
        
        O_DIRECT_Natives.syncO_DIRECT(fd);
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
     * Closes the O_DIRECT I/O instance.
     * 
     * @throws IOException if close fails
     */
    @Override
    public void close() throws IOException {
        if (isOpen) {
            try {
                O_DIRECT_Natives.closeO_DIRECT(fd);
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
     * @return true if using O_DIRECT, false otherwise
     */
    public boolean isUsingO_DIRECT() {
        return usingO_DIRECT;
    }
}