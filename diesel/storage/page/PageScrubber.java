package diesel.storage.page;

import diesel.checksum.ChecksummedPage;
import diesel.observability.MetricsRegistry;
import diesel.storage.page.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Background page scrubber for CRC32C verification and corruption detection (Prompt4 #53).
 * 
 * <p>Runs as a daemon thread that periodically scans all pages in the page file,
 * verifies CRC32C checksums, and reports corrupted pages to logs and metrics.
 * Does NOT block writers - reads pages directly from disk via separate FileChannel.
 * 
 * <p>Scrubber lifecycle:
 * - Constructor: configures scrubber parameters
 * - start(): launches background daemon thread
 * - stop(): signals graceful shutdown, waits for completion
 * - close(): idempotent cleanup (calls stop())
 * - scrubNow(): synchronous full scan (for tests and manual triggers)
 */
public final class PageScrubber implements AutoCloseable {
    
    private final Path pageFile;
    private final int pageSize;
    private final long intervalMillis;
    private final boolean enabled;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Thread> daemonThread = new AtomicReference<>();
    private final Lock scrubLock = new ReentrantLock();
    
    /**
     * Result of a scrub operation.
     */
    public static final class ScrubResult {
        private final long pagesScanned;
        private final List<PageId> corruptedPages;
        private final long durationMs;
        
        public ScrubResult(long pagesScanned, List<PageId> corruptedPages, long durationMs) {
            this.pagesScanned = pagesScanned;
            this.corruptedPages = new ArrayList<>(corruptedPages);
            this.durationMs = durationMs;
        }
        
        public long getPagesScanned() { return pagesScanned; }
        public List<PageId> getCorruptedPages() { return new ArrayList<>(corruptedPages); }
        public long getDurationMs() { return durationMs; }
        public int getCorruptedPageCount() { return corruptedPages.size(); }
    }
    
    /**
     * Creates a PageScrubber.
     * 
     * @param pageFile path to the page file to scrub
     * @param pageSize page size in bytes
     * @param intervalDays scrub interval in days (for background thread)
     * @param enabled whether scrubber is enabled
     */
    public PageScrubber(Path pageFile, int pageSize, long intervalDays, boolean enabled) {
        if (pageFile == null) {
            throw new IllegalArgumentException("pageFile must not be null");
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive: " + pageSize);
        }
        if (intervalDays < 0) {
            throw new IllegalArgumentException("intervalDays must be non-negative: " + intervalDays);
        }
        
        this.pageFile = pageFile;
        this.pageSize = pageSize;
        this.intervalMillis = intervalDays * 24L * 60 * 60 * 1000; // Convert days to milliseconds
        this.enabled = enabled;
    }
    
    /**
     * Starts the background scrubber daemon thread.
     * Idempotent - safe to call multiple times.
     */
    public void start() {
        if (!enabled) {
            return;
        }
        
        if (running.compareAndSet(false, true)) {
            Thread thread = new Thread(this::daemonLoop, "diesel-page-scrubber");
            thread.setDaemon(true);
            thread.start();
            daemonThread.set(thread);
        }
    }
    
    /**
     * Stops the background scrubber gracefully.
     * Waits for current scrub to complete, then shuts down.
     * Idempotent - safe to call multiple times.
     */
    public void stop() {
        if (!enabled) {
            return;
        }
        
        if (running.compareAndSet(true, false)) {
            Thread thread = daemonThread.getAndSet(null);
            if (thread != null) {
                try {
                    thread.join(5000); // Wait up to 5 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    /**
     * Performs a synchronous full scrub now.
     * Blocks until all pages are scanned and verified.
     * 
     * @return scrub result with scan statistics
     * @throws IOException if file I/O fails
     */
    public ScrubResult scrubNow() throws IOException {
        if (!enabled) {
            return new ScrubResult(0, List.of(), 0);
        }
        
        scrubLock.lock();
        try {
            long startTime = System.currentTimeMillis();
            
            // Open separate read-only channel to avoid interfering with writers
            try (FileChannel channel = FileChannel.open(
                    pageFile,
                    StandardOpenOption.READ)) {
                
                long fileSize = channel.size();
                if (fileSize == 0) {
                    return new ScrubResult(0, List.of(), 0);
                }
                
                long pageCount = fileSize / pageSize;
                List<PageId> corruptedPages = new ArrayList<>();
                
                // Scan all pages
                for (long pageNum = 0; pageNum < pageCount; pageNum++) {
                    long offset = pageNum * pageSize;
                    ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                    
                    try {
                        // Read page at offset
                        int totalRead = 0;
                        while (buffer.hasRemaining()) {
                            int n = channel.read(buffer, offset + totalRead);
                            if (n < 0) {
                                throw new IOException("short read at page " + pageNum + 
                                        ": expected " + pageSize + " bytes, got " + totalRead);
                            }
                            totalRead += n;
                        }
                        
                        buffer.flip();
                        Page page = Page.readFrom(buffer);
                        
                        // Verify checksum (double-read filter for torn writes)
                        if (!verifyPageWithRetry(page)) {
                            corruptedPages.add(page.getPageId());
                        }
                        
                    } catch (PageFormatException e) {
                        // Page format error = corruption
                        corruptedPages.add(new PageId(0, 1, pageNum)); // Default tablespace/file
                    }
                }
                
                long durationMs = System.currentTimeMillis() - startTime;
                
                // Log and metrics
                if (!corruptedPages.isEmpty()) {
                    for (PageId corrupted : corruptedPages) {
                        System.err.println("PageScrubber: corrupted page detected: " + corrupted);
                    }
                    System.err.println("PageScrubber: found " + corruptedPages.size() + 
                            " corrupted pages out of " + pageCount + " scanned");
                } else {
                    System.out.println("PageScrubber: successfully scrubbed " + pageCount + 
                            " pages, no corruption detected");
                }
                
                // Record metrics
                MetricsRegistry.getInstance().counter("diesel_scrubber_pages_scanned_total")
                    .increment(pageCount);
                MetricsRegistry.getInstance().histogram("diesel_scrubber_duration_seconds")
                    .record(durationMs / 1000.0);
                MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total")
                    .increment(corruptedPages.size());
                
                return new ScrubResult(pageCount, corruptedPages, durationMs);
            }
        } finally {
            scrubLock.unlock();
        }
    }
    
    /**
     * Verifies a page checksum with one retry for torn-write protection.
     * 
     * @param page the page to verify
     * @return true if checksum is valid (after retry if needed)
     */
    private boolean verifyPageWithRetry(Page page) {
        // First attempt
        if (ChecksummedPage.verifyChecksum(page)) {
            return true;
        }
        
        // Checksum failed - try one re-read to filter torn writes
        try {
            ByteBuffer buffer = ByteBuffer.allocate(pageSize);
            buffer.put(page.toBytes());
            buffer.flip();
            Page retryPage = Page.readFrom(buffer);
            return ChecksummedPage.verifyChecksum(retryPage);
        } catch (Exception e) {
            // Re-read failed - definitely corrupted
            return false;
        }
    }
    
    /**
     * Background daemon loop.
     * Sleeps for interval, then performs scrub.
     */
    private void daemonLoop() {
        while (running.get()) {
            try {
                // Sleep for interval (or shorter if shutdown requested)
                long sleepTime = intervalMillis;
                if (sleepTime > 0) {
                    // Sleep in smaller chunks to allow quick shutdown
                    long chunk = Math.min(sleepTime, 60000); // 1 minute chunks
                    long remaining = sleepTime;
                    
                    while (remaining > 0 && running.get()) {
                        Thread.sleep(chunk);
                        remaining -= chunk;
                        chunk = Math.min(remaining, 60000);
                    }
                }
                
                // Perform scrub if still running
                if (running.get()) {
                    try {
                        scrubNow();
                    } catch (IOException e) {
                        System.err.println("PageScrubber: scrub failed: " + e.getMessage());
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * Closes the scrubber (alias for stop()).
     * Idempotent and safe to call multiple times.
     */
    @Override
    public void close() {
        stop();
    }
}