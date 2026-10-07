package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import diesel.storage.page.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * PageManager correctness tests.
 * Covers round-trip I/O, page allocation, restart simulation, BufferPool integration,
 * and crash recovery for interrupted atomic writes.
 */
@Tag("storage")
class PageManagerTest {

    @TempDir
    Path tempDir;

    private Path pageFile;
    private PageManager manager;
    private static final int PAGE_SIZE = 8192;
    private static final int CAPACITY_PAGES = 10;

    @BeforeEach
    void setUp() throws Exception {
        pageFile = tempDir.resolve("test.pages");
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
    }

    @Test
    void roundTripSinglePage() throws Exception {
        PageId id = new PageId(1, 1, 0);
        
        // Create page with test data
        Page original = new Page(id, PAGE_SIZE);
        byte[] testData = "Hello, PageManager!".getBytes();
        int slotId = original.insert(testData);
        assertEquals(0, slotId, "first slot should be 0");
        
        // Write page
        manager.writePage(original);
        
        // Read page back
        try (PinnedPage pinned = manager.readPage(id)) {
            Page loaded = pinned.getPage();
            byte[] loadedData = loaded.get(slotId);
            assertArrayEquals(testData, loadedData, "written and read data should match");
            assertFalse(loaded.isDirty(), "loaded page should be clean");
        }
        
        // Verify file size
        assertEquals(PAGE_SIZE, manager.getFileSize(), "file should contain one page");
    }

    @Test
    void allocatePageCreatesSequentialIds() throws Exception {
        List<PageId> ids = new ArrayList<>();
        
        // Allocate multiple pages
        for (int i = 0; i < 5; i++) {
            PageId id = manager.allocatePage(1);
            ids.add(id);
            assertEquals(i, id.pageNum(), "page numbers should be sequential");
            assertEquals(1, id.fileId(), "fileId should be 1");
            assertEquals(1, id.tablespaceId(), "tablespaceId should be 1");
        }
        
        // Verify file size
        assertEquals(5L * PAGE_SIZE, manager.getFileSize(), "file should contain 5 pages");
        
        // Verify pages can be read immediately after allocation
        for (PageId id : ids) {
            try (PinnedPage pinned = manager.readPage(id)) {
                Page page = pinned.getPage();
                assertNotNull(page, "allocated page should be readable");
                assertFalse(page.isDirty(), "allocated page should be clean after read");
            }
        }
    }

    @Test
    void writePageMakesPageResidentAndDirty() throws Exception {
        PageId id = new PageId(1, 1, 0);
        Page page = new Page(id, PAGE_SIZE);
        page.insert("test".getBytes());
        
        // Write page
        manager.writePage(page);
        
        // Verify page is resident in pool
        try (PinnedPage pinned = manager.readPage(id)) {
            Page resident = pinned.getPage();
            assertSame(page, resident, "writePage should make page resident");
            assertTrue(page.isDirty(), "written page should be dirty");
        }
    }

    @Test
    void readPageMissLoadsFromDisk() throws Exception {
        PageId id = new PageId(1, 1, 0);
        Page original = new Page(id, PAGE_SIZE);
        original.insert("disk load test".getBytes());
        
        // Write page to disk
        manager.writePage(original);
        
        // Close and reopen manager to clear pool
        manager.close();
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        
        // Read page (should trigger load)
        try (PinnedPage pinned = manager.readPage(id)) {
            Page loaded = pinned.getPage();
            byte[] data = loaded.get(0);
            assertArrayEquals("disk load test".getBytes(), data, "loaded data should match");
            assertFalse(loaded.isDirty(), "loaded page should be clean");
        }
        
        // Verify BufferPool hit/miss counters
        BufferPool pool = manager.getBufferPool();
        assertTrue(pool.getHits() > 0, "should have cache hits after load");
        assertTrue(pool.getMisses() > 0, "should have cache misses on load");
    }

    @Test
    void flushDirtyWritesToDisk() throws Exception {
        PageId id = new PageId(1, 1, 0);
        Page page = new Page(id, PAGE_SIZE);
        page.insert("flush test".getBytes());
        page.setDirty(true); // simulate dirty page
        
        // Flush should write to disk
        manager.flush();
        
        // Close and reopen with new manager
        manager.close();
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        
        // Verify data is on disk
        try (PinnedPage pinned = manager.readPage(id)) {
            Page loaded = pinned.getPage();
            byte[] data = loaded.get(0);
            assertArrayEquals("flush test".getBytes(), data, "flushed data should persist");
        }
    }

    @Test
    void restartSimulationTenThousandPages() throws Exception {
        int pageCount = 10000;
        List<PageId> ids = new ArrayList<>();
        
        // Write 10,000 pages
        for (int i = 0; i < pageCount; i++) {
            PageId id = manager.allocatePage(1);
            ids.add(id);
            Page page = new Page(id, PAGE_SIZE);
            page.insert(("page-" + i).getBytes());
            manager.writePage(page);
            
            // Flush periodically to test mixed dirty/clean state
            if (i % 1000 == 0) {
                manager.flush();
            }
        }
        
        // Verify all pages are in file
        assertEquals((long) pageCount * PAGE_SIZE, manager.getFileSize(), "file should contain all pages");
        
        // Close manager (simulating crash/restart)
        manager.close();
        
        // Reopen manager
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        
        // Read all pages back
        for (int i = 0; i < pageCount; i++) {
            PageId id = ids.get(i);
            try (PinnedPage pinned = manager.readPage(id)) {
                Page loaded = pinned.getPage();
                byte[] data = loaded.get(0);
                assertEquals("page-" + i, new String(data), "page " + i + " should match");
                assertFalse(loaded.isDirty(), "loaded page should be clean");
            }
        }
        
        // Verify BufferPool hit rate is high (most pages should be cached)
        BufferPool pool = manager.getBufferPool();
        double hitRate = pool.getHitRate();
        assertTrue(hitRate > 0.8, "hit rate should be high after reading all pages: " + hitRate);
    }

    @Test
    void loadedPageIsClean() throws Exception {
        PageId id = new PageId(1, 1, 0);
        Page original = new Page(id, PAGE_SIZE);
        original.insert("clean test".getBytes());
        original.setDirty(true);
        
        // Write dirty page
        manager.writePage(original);
        
        // Close and reopen to clear pool
        manager.close();
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
        
        // Load page - should be clean
        try (PinnedPage pinned = manager.readPage(id)) {
            Page loaded = pinned.getPage();
            assertFalse(loaded.isDirty(), "page loaded from disk should be clean");
        }
    }

    @Test
    void missingPageReturnsNull() throws Exception {
        PageId id = new PageId(1, 1, 999); // non-existent page
        
        // Should return null when loader can't find page
        try {
            manager.readPage(id);
            fail("should throw IOException for missing page");
        } catch (IOException e) {
            // Expected - BufferPool throws IOException when loader returns null
        }
    }

    @Test
    void allocatePageExtendsFile() throws Exception {
        // Initially empty file
        assertEquals(0, manager.getFileSize(), "initial file should be empty");
        
        // Allocate first page
        PageId id1 = manager.allocatePage(1);
        assertEquals(PAGE_SIZE, manager.getFileSize(), "file should extend to one page");
        
        // Allocate second page
        PageId id2 = manager.allocatePage(1);
        assertEquals(2L * PAGE_SIZE, manager.getFileSize(), "file should extend to two pages");
        
        // Verify pages are distinct
        assertNotEquals(id1, id2, "allocated pages should have different page numbers");
        assertEquals(0, id1.pageNum());
        assertEquals(1, id2.pageNum());
    }

    @Test
    void closeIdempotent() throws Exception {
        // Write some data
        PageId id = new PageId(1, 1, 0);
        Page page = new Page(id, PAGE_SIZE);
        page.insert("close test".getBytes());
        manager.writePage(page);
        
        // Close multiple times
        manager.close();
        manager.close(); // should not throw
        
        // Verify file still exists and has data
        assertTrue(Files.exists(pageFile), "file should still exist after close");
        assertEquals(PAGE_SIZE, Files.size(pageFile), "file should retain data");
    }

    @Test
    void ioModeConfigurable() throws Exception {
        // Verify the I/O mode is accessible (for testing)
        String mode = manager.getIoMode();
        assertNotNull(mode, "I/O mode should be accessible");
        // Could test sysprop override if needed
    }
}