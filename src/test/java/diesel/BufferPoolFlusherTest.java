package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import diesel.storage.page.BufferPool;
import diesel.storage.page.BufferPoolFlusher;
import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageLoader;
import diesel.storage.page.PageFlusher;
import diesel.storage.page.PinnedPage;

/**
 * Unit tests for BufferPoolFlusher (prompt4.md #20, R3-005 step 1/3).
 */
@Tag("smoke")
public class BufferPoolFlusherTest {

    private BufferPool pool;
    private TestPageLoader loader;
    private TestPageFlusher flusher;
    private BufferPoolFlusher flusherDaemon;
    private final LongSupplier testWalLsnSupplier = () -> 100;

    @BeforeEach
    void setUp() throws IOException {
        // Create a small buffer pool for testing (order: capacity, pageSize, flusher, loader)
        loader = new TestPageLoader();
        flusher = new TestPageFlusher();
        pool = new BufferPool(10, 8192, flusher, loader);
        
        // Create flusher with short interval for testing
        flusherDaemon = new BufferPoolFlusher(pool, testWalLsnSupplier, 50);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (flusherDaemon != null) {
            flusherDaemon.stop();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void testInitialState() {
        assertFalse(flusherDaemon.isRunning());
        assertEquals(50, flusherDaemon.getCurrentIntervalMs());
        assertEquals(50, flusherDaemon.getBaseIntervalMs());
        assertEquals(0, flusherDaemon.getDirtyPageCount());
        assertEquals(10, flusherDaemon.getCapacityPages());
    }



    @Test
    void testFlushEligiblePages() throws IOException {
        // Start flusher
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());

        // Create pages with different LSNs
        Page page1 = new Page(new PageId(1, 1, 1), 8192); // LSN = 0 (always eligible)
        page1.setDirty(true);
        
        Page page2 = new Page(new PageId(1, 1, 2), 8192); // LSN = 0 (always eligible)
        page2.setLsn(50); // <= WAL watermark (100)
        page2.setDirty(true);
        
        Page page3 = new Page(new PageId(1, 1, 3), 8192); // LSN = 150 (> watermark)
        page3.setLsn(150);
        page3.setDirty(true);

        // Insert all pages (close handles so pages are unpinned and flushable)
        try (PinnedPage p1 = pool.insert(page1);
             PinnedPage p2 = pool.insert(page2);
             PinnedPage p3 = pool.insert(page3)) {
            // handles closed at end of try-with-resources
        }

        // Wait for flush cycle
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Only eligible pages should be flushed
        assertFalse(page1.isDirty()); // LSN 0, eligible
        assertFalse(page2.isDirty()); // LSN 50, eligible
        assertTrue(page3.isDirty());  // LSN 150, not eligible
        
        flusherDaemon.stop();
    }

    @Test
    void testSkipPinnedPages() throws IOException {
        // Start flusher
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());

        // Create and pin a page
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.setDirty(true);
        PinnedPage pinned = pool.insert(page);
        assertTrue(page.isDirty());

        // Wait for flush cycle
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Pinned page should remain dirty
        assertTrue(page.isDirty());

        // Unpin and wait for next cycle
        pinned.close();
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Now page should be flushed
        assertFalse(page.isDirty());
        
        flusherDaemon.stop();
    }

    @Test
    void testStopWithoutFlush() throws IOException {
        // Create dirty page
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.setDirty(true);
        pool.insert(page);
        assertTrue(page.isDirty());

        // Start and stop flusher without flush
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());
        
        flusherDaemon.stopWithoutFlush();
        assertFalse(flusherDaemon.isRunning());

        // Page should remain dirty
        assertTrue(page.isDirty());
    }

    @Test
    void testStopWithFlush() throws IOException {
        // Create dirty page
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.setDirty(true);
        try (PinnedPage pinned = pool.insert(page)) {
            assertTrue(pinned.getPage().isDirty());
        }
        assertTrue(page.isDirty());

        // Start and stop flusher with flush
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());
        
        flusherDaemon.stop();
        assertFalse(flusherDaemon.isRunning());

        // Page should be flushed
        assertFalse(page.isDirty());
    }

    @Test
    void testIdempotentStart() throws IOException {
        // Start first time
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());

        // Start again (should be no-op)
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());

        flusherDaemon.stop();
    }

    @Test
    void testIdempotentStop() throws IOException {
        // Stop without starting (should be no-op)
        flusherDaemon.stop();
        assertFalse(flusherDaemon.isRunning());

        // Start then stop
        flusherDaemon.start();
        assertTrue(flusherDaemon.isRunning());
        
        flusherDaemon.stop();
        assertFalse(flusherDaemon.isRunning());

        // Stop again (should be no-op)
        flusherDaemon.stop();
        assertFalse(flusherDaemon.isRunning());
    }

    @Test
    void testMetrics() throws IOException, InterruptedException {
        flusherDaemon.start();
        
        // Create some dirty pages (close handles: background flush skips pinned frames)
        for (int i = 0; i < 3; i++) {
            Page page = new Page(new PageId(1, 1, i + 1), 8192);
            page.setDirty(true);
            try (PinnedPage pinned = pool.insert(page)) {
                assertTrue(pinned.getPage().isDirty());
            }
        }

        // Wait for flush cycles
        Thread.sleep(150);
        
        // Check metrics
        assertTrue(flusherDaemon.getFlushCount() > 0);
        assertTrue(flusherDaemon.getTotalFlushedPages() > 0);
        assertTrue(flusherDaemon.getLastFlushDurationMs() >= 0);
        assertTrue(flusherDaemon.getTotalFlushDurationMs() >= 0);
        
        flusherDaemon.stop();
    }

    @Test
    void testWaldDisabled() throws IOException, InterruptedException {
        // Create flusher with disabled WAL (Long.MAX_VALUE supplier)
        LongSupplier noWalSupplier = () -> Long.MAX_VALUE;
        BufferPoolFlusher noWalFlusher = new BufferPoolFlusher(pool, noWalSupplier, 50);
        
        try {
            // Create dirty page
            Page page = new Page(new PageId(1, 1, 1), 8192);
            page.setLsn(200); // Any LSN
            page.setDirty(true);
            try (PinnedPage pinned = pool.insert(page)) {
                assertTrue(pinned.getPage().isDirty());
            }
            assertTrue(page.isDirty());

            // Start flusher
            noWalFlusher.start();
            assertTrue(noWalFlusher.isRunning());

            // Wait for flush cycle
            Thread.sleep(100);

            // With WAL disabled, all pages should be flushed
            assertFalse(page.isDirty());
            
        } finally {
            noWalFlusher.stop();
        }
    }

    // Helper classes for testing
    private static class TestPageLoader implements PageLoader {
        @Override
        public Page load(PageId pageId) throws IOException {
            return new Page(pageId, 8192);
        }
    }

    private static class TestPageFlusher implements PageFlusher {
        @Override
        public void flush(Page page) throws IOException {
            page.setDirty(false);
        }
    }
}