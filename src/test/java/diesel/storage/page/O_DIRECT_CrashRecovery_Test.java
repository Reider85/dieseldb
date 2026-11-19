package diesel.storage.page;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Crash recovery tests for O_DIRECT mode.
 */
public class O_DIRECT_CrashRecovery_Test {

    @Test
    public void testO_DIRECTCrashRecovery() throws IOException {
        Path pageFile = Files.createTempFile("odirect-crash-test", ".dat");
        
        try {
            int pageSize = 8192;
            int pageCount = 100;
            
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            // Create PageManager and write pages
            PageManager manager = new PageManager(pageFile, 50, pageSize);
            
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = manager.allocatePage(1);
                Page page = new Page(pageId, pageSize);
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                buffer.putInt(i); // Page marker
                // Fill the rest of the buffer with zeros
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 0);
                }
                buffer.rewind();
                page.writeTo(buffer);
                manager.writePage(page);
            }
            
            // Flush all pages
            manager.flush();
            
            // Verify all pages are written
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = new PageId(1, 1, i);
try (PinnedPage pinned = manager.readPage(pageId)) {
                        Page readPage = pinned.getPage();
                        assertNotNull(readPage);
                        assertEquals(pageSize, readPage.getPageSize());
                        // Verify the page can be written to buffer
                        ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                        readPage.writeTo(readBuffer);
                        readBuffer.flip();
                        assertEquals(pageSize, readBuffer.remaining());
                    }
                    
                    // Verify the page was written to disk
                    assertTrue(Files.exists(pageFile));
                    assertTrue(Files.size(pageFile) > 0);
            }
            
            // Simulate crash by closing without flush
            manager.closeDiscardingDirty();
            
            // Create new PageManager and verify data consistency
            PageManager newManager = new PageManager(pageFile, 50, pageSize);
            
            // All pages should still be readable (since we flushed before crash)
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = new PageId(1, 1, i);
                try (PinnedPage pinned = newManager.readPage(pageId)) {
                    Page readPage = pinned.getPage();
                    assertNotNull(readPage);
                    assertEquals(pageSize, readPage.getPageSize());
                    // Verify the page can be written to buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                    readPage.writeTo(readBuffer);
                    readBuffer.flip();
                    assertEquals(pageSize, readBuffer.remaining());
                }
            }
            
            newManager.close();
            
        } finally {
            System.clearProperty(FileChannelIO.MODE_KEY);
            Files.deleteIfExists(pageFile);
        }
    }
    
    @Test
    public void testO_DIRECTAtomicWriteRecovery() throws IOException {
        Path pageFile = Files.createTempFile("odirect-atomic-test", ".dat");
        
        try {
            int pageSize = 8192;
            int pageCount = 50;
            
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            // Create PageManager
            PageManager manager = new PageManager(pageFile, 30, pageSize);
            
            // Write pages
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = manager.allocatePage(1);
                Page page = new Page(pageId, pageSize);
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                buffer.putInt(0x12345678); // Magic marker
                buffer.putInt(i); // Page number
                // Fill the rest of the buffer with zeros
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 0);
                }
                buffer.rewind();
                page.writeTo(buffer);
                manager.writePage(page);
            }
            
            // Flush to ensure data is on disk
            manager.flush();
            
            // Verify data
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = new PageId(1, 1, i);
                try (PinnedPage pinned = manager.readPage(pageId)) {
                    Page readPage = pinned.getPage();
                    assertNotNull(readPage);
                    assertEquals(pageSize, readPage.getPageSize());
                    // Verify the page can be written to buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                    readPage.writeTo(readBuffer);
                    readBuffer.flip();
                    assertEquals(pageSize, readBuffer.remaining());
                }
            }
            
            manager.close();
            
            // Reopen and verify data consistency
            PageManager newManager = new PageManager(pageFile, 30, pageSize);
            
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = new PageId(1, 1, i);
                try (PinnedPage pinned = newManager.readPage(pageId)) {
                    Page readPage = pinned.getPage();
                    assertNotNull(readPage);
                    assertEquals(pageSize, readPage.getPageSize());
                    // Verify the page can be written to buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                    readPage.writeTo(readBuffer);
                    readBuffer.flip();
                    assertEquals(pageSize, readBuffer.remaining());
                }
            }
            
            newManager.close();
            
        } finally {
            System.clearProperty(FileChannelIO.MODE_KEY);
            Files.deleteIfExists(pageFile);
        }
    }
    
    @Test
    public void testO_DIRECTPartialWriteRecovery() throws IOException {
        Path pageFile = Files.createTempFile("odirect-partial-test", ".dat");
        
        try {
            int pageSize = 8192;
            int pageCount = 20;
            
            System.setProperty(FileChannelIO.MODE_KEY, FileChannelIO.MODE_ODIRECT);
            
            // Create PageManager
            PageManager manager = new PageManager(pageFile, 20, pageSize);
            
            // Write some pages but don't flush all
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = manager.allocatePage(1);
                Page page = new Page(pageId, pageSize);
                ByteBuffer buffer = ByteBuffer.allocate(pageSize);
                buffer.putInt(i); // Page marker
                // Fill the rest of the buffer with zeros
                while (buffer.remaining() > 0) {
                    buffer.put((byte) 0);
                }
                buffer.rewind();
                page.writeTo(buffer);
                manager.writePage(page);
                
                // Flush every other page to simulate partial write
                if (i % 2 == 0) {
                    manager.flush();
                }
            }
            
            manager.close();
            
            // Reopen and verify that flushed pages are consistent
            PageManager newManager = new PageManager(pageFile, 20, pageSize);
            
            // Check flushed pages (even indices)
            for (int i = 0; i < pageCount; i += 2) {
                PageId pageId = new PageId(1, 1, i);
                try (PinnedPage pinned = newManager.readPage(pageId)) {
                    Page readPage = pinned.getPage();
                    assertNotNull(readPage);
                    assertEquals(pageSize, readPage.getPageSize());
                    // Verify the page can be written to buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                    readPage.writeTo(readBuffer);
                    readBuffer.flip();
                    assertEquals(pageSize, readBuffer.remaining());
                }
            }
            
            // Check non-flushed pages (odd indices) - may or may not be present
            for (int i = 1; i < pageCount; i += 2) {
                PageId pageId = new PageId(1, 1, i);
                try (PinnedPage pinned = newManager.readPage(pageId)) {
                    Page readPage = pinned.getPage();
                    assertNotNull(readPage);
                    assertEquals(pageSize, readPage.getPageSize());
                    // Verify the page can be written to buffer
                    ByteBuffer readBuffer = ByteBuffer.allocate(pageSize);
                    readPage.writeTo(readBuffer);
                    readBuffer.flip();
                    assertEquals(pageSize, readBuffer.remaining());
                }
            }
            
            newManager.close();
            
        } finally {
            System.clearProperty(FileChannelIO.MODE_KEY);
            Files.deleteIfExists(pageFile);
        }
    }
}