package diesel.checksum;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageManager;
import diesel.storage.page.PageCorruptedException;
import diesel.storage.page.SlottedPageLayout;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.AfterEach;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for corruption detection using CRC32C checksums (Prompt4 #52).
 * 
 * <p>Verifies that corrupted pages are detected through checksum verification.
 */
@Tag("storage")
public class CorruptionDetectionTest {

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
    void cleanPagePassesChecksumVerification() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert some test data
        byte[] testData = "Hello, World!".getBytes();
        int slotId = page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read back and verify checksum
        Page loadedPage = pageManager.readPage(pageId).getPage();
        assertTrue(ChecksummedPage.verifyChecksum(loadedPage));
        
        // Validation should pass
        assertDoesNotThrow(() -> ChecksummedPage.validatePage(loadedPage));
    }
    
    @Test
    void singleBitCorruptionDetected() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Test data for corruption detection".getBytes();
        int slotId = page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read page data from disk (bypassing checksum)
        byte[] pageData = page.toBytes();
        
        // Flip a single bit in the payload (not header)
        int flipPosition = PageManager.PAGE_SIZE / 2; // Middle of page
        pageData[flipPosition] ^= 0x01; // Flip the least significant bit
        
        // Create page with corrupted data
        Page corruptedPage = new Page(pageId, PAGE_SIZE);
        corruptedPage.setData(pageData);
        
        // Verify checksum should fail
        assertFalse(ChecksummedPage.verifyChecksum(corruptedPage));
        
        // Validation should throw exception
        PageCorruptedException exception = assertThrows(PageCorruptedException.class, 
            () -> ChecksummedPage.validatePage(corruptedPage));
        
        assertEquals(pageId, exception.getPageId());
        assertTrue(exception.getMessage().contains("corrupted checksum"));
    }
    
    @Test
    void headerCorruptionDetected() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Header corruption test".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read page data from disk
        byte[] pageData = page.toBytes();
        
        // Flip a bit in the checksum field (offset 40)
        int checksumOffset = 40;
        pageData[checksumOffset] ^= 0x01;
        
        // Create page from corrupted data - checksum validation should fail
        // but page should still be readable (magic number intact)
        Page corruptedPage = new Page(pageId, PAGE_SIZE);
        corruptedPage.setData(pageData);
        
        // Verify checksum should fail
        assertFalse(ChecksummedPage.verifyChecksum(corruptedPage));
        
        // Validation should throw exception
        assertThrows(PageCorruptedException.class, () -> ChecksummedPage.validatePage(corruptedPage));
    }
    
    @Test
    void multipleCorruptionsDetected() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Multiple corruption test data".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read page data from disk
        byte[] pageData = page.toBytes();
        
        // Flip multiple bits at different positions
        Random random = new Random(42); // Fixed seed for reproducibility
        for (int i = 0; i < 10; i++) {
            int position = random.nextInt(pageData.length);
            pageData[position] ^= 0x01;
        }
        
        // Create page with corrupted data
        Page corruptedPage = new Page(pageId, PAGE_SIZE);
        corruptedPage.setData(pageData);
        
        // Verify checksum should fail
        assertFalse(ChecksummedPage.verifyChecksum(corruptedPage));
        
        // Validation should throw exception
        assertThrows(PageCorruptedException.class, () -> ChecksummedPage.validatePage(corruptedPage));
    }
    
    @Test
    void pageValidatorDetectsCorruption() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Page validator test".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read back and corrupt
        Page loadedPage = pageManager.readPage(pageId).getPage();
        byte[] pageData = loadedPage.toBytes();
        
        // Flip a bit
        pageData[100] ^= 0x01;
        Page corruptedPage = new Page(pageId, PAGE_SIZE);
        corruptedPage.setData(pageData);
        
        // PageValidator should detect corruption
        assertFalse(PageValidator.isValid(corruptedPage));
        
        // ValidationResult should show corruption
        var result = PageValidator.getValidationResult(corruptedPage);
        assertFalse(result.isValid());
        assertNotNull(result.getCorruptionException());
        assertTrue(result.getDetails().contains("corrupted"));
    }
    
    @Test
    void cleanPageValidatorReportsValid() throws Exception {
        // Create and write a clean page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, PAGE_SIZE);
        
        // Insert test data
        byte[] testData = "Clean page test".getBytes();
        page.insert(testData);
        
        // Write to disk
        pageManager.writePage(page);
        
        // Read back
        try (var pinnedPage = pageManager.readPage(pageId)) {
            Page loadedPage = pinnedPage.getPage();
            
            // PageValidator should report valid
            assertTrue(PageValidator.isValid(loadedPage));
            
            // ValidationResult should show valid
            var result = PageValidator.getValidationResult(loadedPage);
            assertTrue(result.isValid());
            assertNull(result.getCorruptionException());
            assertEquals("Page checksum is valid", result.getDetails());
        }
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pageManager != null) {
            pageManager.close();
        }
    }
}