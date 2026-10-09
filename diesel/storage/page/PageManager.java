package diesel.storage.page;

import diesel.wal.WALEntry;
import diesel.wal.WALOpcode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean closed = new AtomicBoolean(false);

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
            // insert() keeps the existing frame instance when the PageId is
            // already resident (e.g. after allocatePage) — copy the argument
            // page's content (data + header) into the resident frame so the
            // data is not lost.
            Page resident = pinned.getPage();
            if (resident != page) {
                resident.copyContentFrom(page);
            } else {
                resident.setDirty(true);
            }
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
     * Applies a physical page-image WAL entry idempotently (ARIES redo,
     * prompt 4 #18).
     *
     * <p>The entry's after-image is a full serialized page; it replaces the
     * current page content and stamps the page LSN with the record's LSN.
     * If the on-disk page already carries an LSN greater than or equal to the
     * record's LSN the record was applied by an earlier recovery pass and the
     * call is a no-op ({@code false}) — redo can therefore be re-run safely.
     *
     * <p>A page beyond the current end of file is created by the redo pass
     * (allocation redo); the allocator watermark is advanced so a later
     * {@link #allocatePage(long)} cannot overwrite it. Physical durability of
     * the applied image is the caller's responsibility ({@link #flush()} is
     * invoked by {@code RedoPhase} after the pass).
     *
     * @param entry the WAL entry; must be a {@link WALOpcode#PAGE_IMAGE} record
     * @return {@code true} if the after-image was applied, {@code false} if the
     *         page already satisfied the LSN check (already redone)
     * @throws IOException if reading the current page or installing the image fails
     * @throws IllegalArgumentException if the entry is not a page-image record,
     *         has no after-image, or the image page size differs from this manager
     */
    public boolean applyRedo(WALEntry entry) throws IOException {
        if (entry == null) {
            throw new IllegalArgumentException("entry must not be null");
        }
        if (entry.getOp() != WALOpcode.PAGE_IMAGE) {
            throw new IllegalArgumentException("Not a page-image record: " + entry.getOp());
        }
        if (!entry.hasAfterImage()) {
            throw new IllegalArgumentException("Page-image record lsn=" + entry.getLsn() + " has no after-image");
        }

        Page image = Page.readFrom(ByteBuffer.wrap(entry.getAfterImage()));
        if (image.getPageSize() != pageSize) {
            throw new IllegalArgumentException("Page size mismatch in record lsn=" + entry.getLsn()
                    + ": expected " + pageSize + ", got " + image.getPageSize());
        }
        PageId pageId = image.getPageId();

        // LSN check: compare against the persisted page state. A page not yet
        // covered by the file has never been written, so it cannot satisfy the
        // check and must be created from the after-image.
        long currentLsn = -1L;
        long offset = pageId.fileOffset(pageSize);
        if (offset >= 0 && offset + pageSize <= io.size()) {
            try (PinnedPage pinned = readPage(pageId)) {
                currentLsn = pinned.getPage().getLsn();
            }
        }
        if (currentLsn >= entry.getLsn()) {
            return false; // already redone — idempotent skip
        }

        image.setLsn(entry.getLsn()); // marks the image dirty
        writePage(image);
        // Redo may create a page past the allocator's watermark; advance it so
        // allocatePage cannot hand out an address that already holds data.
        nextFilePageNum.accumulateAndGet(pageId.pageNum() + 1, Math::max);
        return true;
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
        if (!closed.compareAndSet(false, true)) {
            return; // already closed — idempotent
        }
        try {
            flush();
        } finally {
            // Clean up leftover temporary files
            cleanupTempFiles();
            io.close();
            pool.close();
        }
    }

    /**
     * Simulates a crash: closes the file and discards all dirty pages without
     * flushing them to disk. Used by crash-recovery tests to verify that
     * unflushed data is lost on a crash.
     *
     * @throws IOException on close failure
     */
    public void closeDiscardingDirty() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return; // already closed — idempotent
        }
        try {
            pool.abandon();
        } finally {
            io.close();
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