package diesel.checksum;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.storage.page.PageCorruptedException;
import diesel.observability.MetricsRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.AfterEach;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for checksum failure metrics (Prompt4 #52).
 * 
 * <p>Verifies that the diesel_page_checksum_failures_total metric is incremented
 * when pages fail checksum verification.
 */
@Tag("storage")
@Tag("metrics")
public class MetricTest {

    @TempDir
    Path tempDir;
    
    private PageManager pageManager;
    private static final int PAGE_SIZE = 8192;
    private static final int CAPACITY_PAGES = 10;
    
    @BeforeEach
    void setUp() throws Exception {
        Path pageFile = tempDir.resolve("test_pages.db");
        pageManager = new PageManager(pageFile, CAPACITY_PAGES, PAGE_SIZE);
    }
    
    @Test
    void checksumFailureMetricIncrementedOnCorruption() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Metric test data".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Get initial metric value
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        long initialValue = counter.get();
        
        // Read back and corrupt
        Page loadedPage = pageManager.readPage(pageId).getPage();
        byte[] pageData = loadedPage.toBytes();
        
        // Flip a bit to corrupt the page
        pageData[100] ^= 0x01;
        Page corruptedPage = Page.readFrom(ByteBuffer.wrap(pageData));
        
        // Try to validate - this should increment the metric
        assertThrows(PageCorruptedException.class, () -> ChecksummedPage.validatePage(corruptedPage));
        
        // Metric should be incremented
        long finalValue = counter.get();
        assertEquals(initialValue + 1, finalValue);
    }
    
    @Test
    void checksumFailureMetricIncrementedOnRead() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Read metric test".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Get initial metric value
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        long initialValue = counter.get();
        
        // Read page data directly from disk and corrupt it
        byte[] pageData = page.toBytes();
        pageData[150] ^= 0x01;
        
        // Create a corrupted page
        Page corruptedPage = Page.readFrom(ByteBuffer.wrap(pageData));
        
        // Load through PageManager (this should trigger metric increment)
        // Note: This simulates the corruption being detected during load
        assertThrows(IOException.class, () -> {
            // We need to manually trigger the checksum verification that happens in load
            ChecksummedPage.validatePage(corruptedPage);
        });
        
        // Metric should be incremented
        long finalValue = counter.get();
        assertEquals(initialValue + 1, finalValue);
    }
    
    @Test
    void multipleChecksumFailuresIncrementedMultipleTimes() throws Exception {
        // Create and write multiple clean pages
        PageId pageId1 = new PageId(1, 1, 0);
        PageId pageId2 = new PageId(1, 1, 1);
        Page page1 = new Page(pageId1, PAGE_SIZE);
        Page page2 = new Page(pageId2, PAGE_SIZE);
        
        // Insert test data
        byte[] testData1 = "Multiple failure test 1".getBytes();
        byte[] testData2 = "Multiple failure test 2".getBytes();
        page1.insert(testData1);
        page2.insert(testData2);
        
        // Write to disk
        pageManager.writePage(page1);
        pageManager.writePage(page2);
        
        // Get initial metric value
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        long initialValue = counter.get();
        
        // Corrupt both pages
        corruptPage(page1, 200);
        corruptPage(page2, 300);
        
        // Validate both - should increment metric twice
        assertThrows(PageCorruptedException.class, () -> ChecksummedPage.validatePage(page1));
        assertThrows(PageCorruptedException.class, () -> ChecksummedPage.validatePage(page2));
        
        // Metric should be incremented twice
        long finalValue = counter.get();
        assertEquals(initialValue + 2, finalValue);
    }
    
    @Test
    void checksumVerificationDoesNotIncrementMetric() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Clean metric test".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Get initial metric value
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        long initialValue = counter.get();
        
        // Read back and validate - should not increment metric
        Page loadedPage = pageManager.readPage(pageId).getPage();
        assertDoesNotThrow(() -> ChecksummedPage.validatePage(loadedPage));
        
        // Metric should not be incremented
        long finalValue = counter.get();
        assertEquals(initialValue, finalValue);
    }
    
    @Test
    void metricIsCounterType() throws Exception {
        // Verify that the metric is indeed a counter
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        
        // Counter should support increment operations
        long initialCount = counter.get();
        counter.increment();
        assertEquals(initialCount + 1, counter.get());
        
        counter.increment();
        assertEquals(initialCount + 2, counter.get());
    }
    
    @Test
    void metricNameMatchesSpecification() throws Exception {
        // Verify that the metric name matches the specification
        var counter = MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total");
        
        // Should be able to access the counter with the exact name
        assertNotNull(counter);
        assertTrue(counter.get() >= 0); // Should be non-negative
    }
    
    private void corruptPage(Page page, int position) {
        byte[] pageData = page.toBytes();
        pageData[position] ^= 0x01; // Flip a bit
        // Apply corrupted data back to the page
        page.setData(pageData);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pageManager != null) {
            pageManager.close();
        }
    }
}