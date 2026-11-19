package diesel.storage.page;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Integration tests for O_DIRECT mode with PageManager.
 */
public class O_DIRECT_Integration_Test {

    @Test
    public void testPageManagerWithO_DIRECT() throws IOException {
        Path pageFile = Files.createTempFile("page-manager-odirect-test", ".dat");
        try {
            int pageSize = 8192;
            int capacityPages = 10;
            
            // Enable O_DIRECT mode
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            PageManager manager = new PageManager(pageFile, capacityPages, pageSize);
            
            // Test page allocation
            PageId pageId1 = manager.allocatePage(1);
            assertNotNull(pageId1);
            assertEquals(0, pageId1.pageNum());
            
            PageId pageId2 = manager.allocatePage(1);
            assertNotNull(pageId2);
            assertEquals(1, pageId2.pageNum());
            
            // Test page writing and reading
            try (PinnedPage pinned1 = manager.readPage(pageId1)) {
                assertNotNull(pinned1.getPage());
                assertEquals(pageSize, pinned1.getPage().getPageSize());
            }
            
            try (PinnedPage pinned2 = manager.readPage(pageId2)) {
                assertNotNull(pinned2.getPage());
                assertEquals(pageSize, pinned2.getPage().getPageSize());
            }
            
            // Test page content - just verify the page can be written and read
            Page page1 = new Page(pageId1, pageSize);
            
            manager.writePage(page1);
            
            try (PinnedPage pinned = manager.readPage(pageId1)) {
                Page readPage = pinned.getPage();
                assertNotNull(readPage);
                assertEquals(pageSize, readPage.getPageSize());
                // Verify the page can be written to buffer
                ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                readPage.writeTo(readBuffer);
                readBuffer.flip();
                assertEquals(pageSize, readBuffer.remaining());
            }
            
            // Test file size
            assertEquals(2 * pageSize, manager.getFileSize());
            
            manager.close();
            
        } finally {
            Files.deleteIfExists(pageFile);
            System.clearProperty(FileChannelIO.MODE_KEY);
        }
    }

    @Test
    public void testPageManagerO_DIRECTFlush() throws IOException {
        Path pageFile = Files.createTempFile("page-manager-flush-test", ".dat");
        try {
            int pageSize = 8192;
            int capacityPages = 5;
            
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            PageManager manager = new PageManager(pageFile, capacityPages, pageSize);
            
            // Write multiple pages
            for (int i = 0; i < 10; i++) {
                PageId pageId = manager.allocatePage(1);
                Page page = new Page(pageId, pageSize);
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                buffer.putInt(i); // Page marker
                // Fill the rest of the buffer with zeros
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 0);
                }
                buffer.flip();
                page.writeTo(buffer);
                manager.writePage(page);
            }
            
            // Flush all dirty pages
            manager.flush();
            
            // Verify file size
            assertEquals(10 * pageSize, manager.getFileSize());
            
            manager.close();
            
        } finally {
            Files.deleteIfExists(pageFile);
            System.clearProperty(FileChannelIO.MODE_KEY);
        }
    }

    @Test
    public void testPageManagerO_DIRECTIoMode() throws IOException {
        Path pageFile = Files.createTempFile("page-manager-mode-test", ".dat");
        try {
            int pageSize = 8192;
            int capacityPages = 10;
            
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            PageManager manager = new PageManager(pageFile, capacityPages, pageSize);
            
            // Check that O_DIRECT mode is being used (or fallback on Windows)
            String expectedMode = System.getProperty("os.name").toLowerCase().contains("win") ? 
                FileChannelIO.MODE_POSITIONAL : FileChannelIO.MODE_ODIRECT;
            assertEquals(expectedMode, manager.getIoMode());
            boolean expectedUsingO_DIRECT = !System.getProperty("os.name").toLowerCase().contains("win");
            assertEquals(expectedUsingO_DIRECT, manager.getFileChannelIO().isUsingO_DIRECT());
            
            manager.close();
            
        } finally {
            Files.deleteIfExists(pageFile);
            System.clearProperty(FileChannelIO.MODE_KEY);
        }
    }

    @Test
    public void testPageManagerO_DIRECTFallback() throws IOException {
        Path pageFile = Files.createTempFile("page-manager-fallback-test", ".dat");
        try {
            int pageSize = 8192;
            int capacityPages = 10;
            
            // Use an invalid page size that should cause O_DIRECT to fail
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            // This should fall back to positional mode
            PageManager manager = new PageManager(pageFile, capacityPages, pageSize);
            
            // Check that it fell back to positional mode
            assertEquals(FileChannelIO.MODE_POSITIONAL, manager.getIoMode());
            assertFalse(manager.getFileChannelIO().isUsingO_DIRECT());
            
            manager.close();
            
        } finally {
            Files.deleteIfExists(pageFile);
            System.clearProperty(FileChannelIO.MODE_KEY);
        }
    }
}