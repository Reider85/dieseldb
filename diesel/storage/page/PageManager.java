package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PageManager: manages page storage, BufferPool integration, and file I/O.
 * <p>
 * Owns a BufferPool and wires itself as PageFlusher/PageLoader for disk operations.
 * Provides:
 * - readPage(PageId) → pins page from pool (loads from disk on miss)
 * - writePage(Page) → ensures residency and marks dirty
 * - allocatePage(long) → extends file, returns new PageId
 * - flush() → flushes dirty pages and forces file sync
 * <p>
 * Thread-safe: external synchronization required for multi-page operations.
 */
public final class PageManager implements PageFlusher, PageLoader, AutoCloseable {
    
    /** Default page size (8KB) */
    public static final int PAGE_SIZE = PageConfig.PAGE_SIZE_8K;

    private final Path file;
    private final int pageSize;
    private final BufferPool pool;
    private final FileChannelIO io;
    private final AtomicLong nextFilePageNum = new AtomicLong(0);

    /**
     * Creates a PageManager that owns its BufferPool.
     *
     * @param file path to the page file
     * @param capacityPages maximum number of pages to cache in memory
     * @param pageSize page size in bytes
     * @throws IOException if file cannot be opened or extended
     */
    public PageManager(Path file, int capacityPages, int pageSize) throws IOException {
        this.file = file;
        this.pageSize = pageSize;
        this.io = new FileChannelIO(file, pageSize);
        this.pool = new BufferPool(capacityPages, pageSize, this, this);
        
        // Initialize next page number from current file size
        long fileSize = io.size();
        if (fileSize > 0) {
            this.nextFilePageNum.set(fileSize / pageSize);
        }
    }

    /**
     * Creates a PageManager with default page size from config.
     *
     * @param file path to the page file
     * @param capacityPages maximum number of pages to cache in memory
     * @throws IOException if file cannot be opened or page size is invalid
     */
    public PageManager(Path file, int capacityPages) throws IOException {
        this(file, capacityPages, PageConfig.getPageSize());
    }

    /**
     * Factory method that creates a PageManager and its BufferPool.
     *
     * @param file path to the page file
     * @param capacityPages maximum number of pages to cache in memory
     * @param pageSize page size in bytes
     * @return new PageManager instance
     * @throws IOException on file or initialization failure
     */
    public static PageManager open(Path file, int capacityPages, int pageSize) throws IOException {
        return new PageManager(file, capacityPages, pageSize);
    }

    /**
     * Factory method with default page size.
     *
     * @param file path to the page file
     * @param capacityPages maximum number of pages to cache in memory
     * @return new PageManager instance
     * @throws IOException on file or initialization failure
     */
    public static PageManager open(Path file, int capacityPages) throws IOException {
        return new PageManager(file, capacityPages, PageConfig.getPageSize());
    }

    /**
     * Returns the owned BufferPool for JMX or advanced operations.
     *
     * @return the BufferPool instance
     */
    public BufferPool getBufferPool() {
        return pool;
    }

    /**
     * Returns the page size in bytes.
     *
     * @return the page size
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Reads a page by pinning it from the pool.
     * If not resident, loads from disk via PageLoader.
     *
     * @param pageId identifies the page to read
     * @return pinned page handle (use try-with-resources)
     * @throws IOException if page cannot be loaded or pinned
     */
    public PinnedPage readPage(PageId pageId) throws IOException {
        if (pageId == null) {
            throw new IllegalArgumentException("pageId must not be null");
        }
        return pool.pin(pageId, this);
    }

    /**
     * Writes a page by ensuring it's resident in the pool and marking it dirty.
     * Physical write occurs on flush or eviction.
     *
     * @param page the page to write
     * @throws IOException if page cannot be made resident
     */
    public void writePage(Page page) throws IOException {
        if (page == null) {
            throw new IllegalArgumentException("page must not be null");
        }
        if (page.getPageSize() != pageSize) {
            throw new IllegalArgumentException("page size mismatch: expected " + pageSize + ", got " + page.getPageSize());
        }

        // Ensure page is resident (insert returns pinned handle)
        try (PinnedPage pinned = pool.insert(page)) {
            // Mark page dirty - physical write will happen via flusher callback
            page.setDirty(true);
            // Unpin immediately - the page is now resident in the pool
        }
    }

    /**
     * Allocates a new page in the file and returns its PageId.
     * Extends the file if needed and inserts the empty page into the pool.
     *
     * @param tablespaceId tablespace identifier (currently unused, reserved for future multi-tablespace)
     * @return PageId of the newly allocated page
     * @throws IOException if file cannot be extended or page inserted
     */
    public PageId allocatePage(long tablespaceId) throws IOException {
        long pageNum = nextFilePageNum.getAndIncrement();
        PageId pageId = new PageId(tablespaceId, 1, pageNum); // fileId=1 for now (prompt 10 stub)

        // Extend file to accommodate the new page
        long requiredSize = (pageNum + 1L) * pageSize;
        io.extendTo(requiredSize);

        // Create empty page and insert into pool (will be pinned)
        Page newPage = new Page(pageId, pageSize);
        try (PinnedPage pinned = pool.insert(newPage)) {
            // Mark dirty so it gets written on flush
            newPage.setDirty(true);
            return pageId;
        }
    }

    /**
     * Flushes all dirty pages to disk and forces file sync.
     *
     * @throws IOException on flush or sync failure
     */
    public void flush() throws IOException {
        pool.flushDirty();
        io.force();
    }

    /**
     * Closes the PageManager: flushes dirty pages, closes file channel, and closes pool.
     * Idempotent and safe to call multiple times.
     *
     * @throws IOException on flush or close failure
     */
    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            // Clean up leftover temporary files
            cleanupTempFiles();
            io.close();
            pool.close();
        }
    }

    // --- PageFlusher implementation (called by BufferPool under lock) ---

    /**
     * Flushes a dirty page to disk at its PageId's offset.
     * Called by BufferPool during eviction or flushDirty().
     *
     * @param page the dirty page to flush
     * @throws IOException on write failure
     */
    @Override
    public void flush(Page page) throws IOException {
        if (page == null) {
            throw new IllegalArgumentException("page must not be null");
        }
        if (page.getPageSize() != pageSize) {
            throw new IllegalArgumentException("page size mismatch: expected " + pageSize + ", got " + page.getPageSize());
        }

        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        page.writeTo(buffer); // syncs header into data
        buffer.flip();
        io.writePage(page.getPageId(), buffer);
    }

    // --- PageLoader implementation (called by BufferPool under lock) ---

    /**
     * Loads a page from disk when it's not resident in the pool.
     * Called by BufferPool during pin miss.
     *
     * @param pageId identifies the page to load
     * @return the loaded page (must be clean), or null if page doesn't exist
     * @throws IOException on read failure or page format error
     */
    @Override
    public Page load(PageId pageId) throws IOException {
        if (pageId == null) {
            throw new IllegalArgumentException("pageId must not be null");
        }

        // Check if page exists in file (offset + pageSize <= file size)
        long offset = pageId.fileOffset(pageSize);
        if (offset + pageSize > io.size()) {
            return null; // page doesn't exist yet
        }

        try {
            ByteBuffer buffer = io.readPage(pageId);
            Page page = Page.readFrom(buffer);
            page.setDirty(false); // loaded from disk - content is clean
            return page;
        } catch (PageFormatException e) {
            throw new IOException("corrupt page " + pageId + " at offset " + offset, e);
        }
    }

    // --- Package-private helpers for tests ---

    /**
     * Returns the current file size in bytes.
     *
     * @throws IOException if size cannot be determined
     */
    public long getFileSize() throws IOException {
        return io.size();
    }

    /**
     * Returns the next allocated page number (for testing).
     *
     * @return the page number that will be allocated next
     */
    public long getNextPageNum() {
        return nextFilePageNum.get();
    }

    /**
     * Returns the I/O mode for testing.
     *
     * @return the current I/O mode string
     */
    public String getIoMode() {
        return FileChannelIO.MODE_DEFAULT; // Could expose via FileChannelIO if needed
    }

    /**
     * Cleans up leftover temporary files from interrupted atomic writes.
     * Called during close() to ensure no temp files remain after shutdown.
     */
    private void cleanupTempFiles() throws IOException {
        Path tempFile = AtomicFileWriter.tmpPath(file);
        if (Files.exists(tempFile)) {
            try {
                Files.delete(tempFile);
            } catch (IOException e) {
                // Log but don't fail the close operation
                System.err.println("Warning: failed to delete temp file: " + tempFile + " - " + e.getMessage());
            }
        }
    }
}