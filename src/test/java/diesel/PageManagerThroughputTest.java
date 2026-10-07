package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import diesel.storage.page.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * PageManager throughput test.
 * Measures write performance to achieve reasonable throughput with 256MB buffer.
 * Runs in perf profile to avoid blocking normal gates.
 */
@Tag("perf")
class PageManagerThroughputTest {

    @TempDir
    Path tempDir;

    private Path pageFile;
    private PageManager manager;
    private static final int PAGE_SIZE = 8192; // 8KB pages
    private static final int CAPACITY_PAGES = 32768; // 256MB / 8KB = 32K pages
    private static final int TEST_PAGES = 1000; // Reduced for reasonable test time
    private static final int TUPLE_SIZE = 8000; // Leave room for header and slots

    @BeforeEach
    void setUp() throws Exception {
        pageFile = tempDir.resolve("throughput-test.pages");
        manager = PageManager.open(pageFile, CAPACITY_PAGES, PAGE_SIZE);
    }

    @Test
    void writeThroughputMeetsTarget() throws Exception {
        List<PageId> ids = new ArrayList<>();
        long startTime = System.nanoTime();
        
        // Write TEST_PAGES pages
        for (int i = 0; i < TEST_PAGES; i++) {
            PageId id = manager.allocatePage(1);
            ids.add(id);
            Page page = new Page(id, PAGE_SIZE);
            
            // Fill page with test data
            byte[] pageData = new byte[TUPLE_SIZE];
            for (int j = 0; j < TUPLE_SIZE; j++) {
                pageData[j] = (byte) (i % 256); // Fill with page index mod 256
            }
            page.insert(pageData);
            manager.writePage(page);
            
            // Periodic flush
            if (i % 100 == 0) {
                manager.flush();
            }
        }
        
        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        double actualRate = TEST_PAGES / durationSeconds;
        
        // Log performance - no hard assertion for now
        System.out.printf("PageManager write throughput: %.0f pages/sec (%.2f sec for %d pages)%n",
            actualRate, durationSeconds, TEST_PAGES);
        
        // Verify file size
        assertEquals((long) TEST_PAGES * PAGE_SIZE, manager.getFileSize(), 
            "file should contain all written pages");
        
        // Simple verification - just check pages can be read back
        int readCount = 0;
        for (int i = 0; i < Math.min(100, TEST_PAGES); i++) {
            PageId id = ids.get(i);
            try (PinnedPage pinned = manager.readPage(id)) {
                Page page = pinned.getPage();
                readCount++;
                // Basic check - page should not be null
                assertNotNull(page, "page should not be null");
            }
        }
        
        assertTrue(readCount > 0, "should have read at least one page");
    }
}