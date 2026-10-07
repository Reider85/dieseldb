package diesel;

import diesel.storage.page.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Page class (R3-002 step 1).
 * 
 * <p>Tests page creation, serialization, tuple operations, and error conditions.
 * All operations are in-memory; no file I/O in this layer.
 */
@Tag("storage")
class PageTest {

    private String prevPageSize;

    @BeforeEach
    void savePageSize() {
        prevPageSize = System.getProperty(Page.PAGE_SIZE_KEY);
    }

    @AfterEach
    void restorePageSize() {
        if (prevPageSize == null) {
            System.clearProperty(Page.PAGE_SIZE_KEY);
        } else {
            System.setProperty(Page.PAGE_SIZE_KEY, prevPageSize);
        }
    }

    /**
     * Helper: create a unique byte array for testing.
     */
    private byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    @Test
    void roundTripOfHundredFiftyByteRowsOn8kPage() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Insert 100 rows of 50 bytes each
        byte[][] rows = new byte[100][50];
        for (int i = 0; i < 100; i++) {
            rows[i] = uniqueBytes(50, i);
            int slotId = page.insert(rows[i]);
            assertEquals(i, slotId, "Slot ID should match insertion order");
        }

        // Verify page state
        assertEquals(100, page.getSlotCount(), "Should have 100 slots");
        assertTrue(page.getFreeSpace() > 0, "Should have free space remaining");
        assertTrue(page.isDirty(), "Page should be dirty after inserts");

        // Serialize and deserialize
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        page.writeTo(buffer);
        buffer.rewind();
        Page page2 = Page.readFrom(buffer);

        // Verify deserialized page
        assertEquals(pageId, page2.getPageId(), "Page ID should match");
        assertEquals(pageSize, page2.getPageSize(), "Page size should match");
        assertEquals(100, page2.getSlotCount(), "Should have 100 slots");

        // Verify all rows are readable and correct
        for (int i = 0; i < 100; i++) {
            byte[] retrieved = page2.get(i);
            assertNotNull(retrieved, "Row " + i + " should not be null");
            assertEquals(50, retrieved.length, "Row " + i + " should have correct length");
            assertTrue(Arrays.equals(rows[i], retrieved), "Row " + i + " should match original");
        }

        // Verify header fields survived
        assertEquals(0, page2.getLsn(), "LSN should be 0 by default");
        assertEquals(0, page2.getChecksum(), "Checksum should be 0 by default");
    }

    @Test
    void pageHeaderFieldsSurviveSerialization() {
        PageId pageId = new PageId(42, 17, 1337);
        int pageSize = 16384;
        Page page = new Page(pageId, pageSize);

        // Set some header fields
        page.setLsn(12345);
        page.setChecksum(67890);

        // Insert a row to make page dirty
        byte[] row = uniqueBytes(100, 1);
        page.insert(row);

        // Serialize and deserialize
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        page.writeTo(buffer);
        buffer.rewind();
        Page page2 = Page.readFrom(buffer);

        // Verify all header fields
        assertEquals(42, page2.getPageId().tablespaceId(), "Tablespace ID should match");
        assertEquals(17, page2.getPageId().fileId(), "File ID should match");
        assertEquals(1337, page2.getPageId().pageNum(), "Page number should match");
        assertEquals(12345, page2.getLsn(), "LSN should match");
        assertEquals(67890, page2.getChecksum(), "Checksum should match");
    }

    @Test
    void deleteMarksSlotAndKeepsOthers() {
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, 8192);

        // Insert 10 rows
        byte[][] rows = new byte[10][30];
        for (int i = 0; i < 10; i++) {
            rows[i] = uniqueBytes(30, i);
            page.insert(rows[i]);
        }

        // Delete rows 3, 5, 7
        page.delete(3);
        page.delete(5);
        page.delete(7);

        // Verify deletions
        assertNull(page.get(3), "Deleted row 3 should be null");
        assertNull(page.get(5), "Deleted row 5 should be null");
        assertNull(page.get(7), "Deleted row 7 should be null");

        // Verify other rows are intact
        for (int i = 0; i < 10; i++) {
            if (i != 3 && i != 5 && i != 7) {
                byte[] retrieved = page.get(i);
                assertNotNull(retrieved, "Row " + i + " should not be null");
                assertEquals(30, retrieved.length, "Row " + i + " should have correct length");
                assertTrue(Arrays.equals(rows[i], retrieved), "Row " + i + " should match original");
            }
        }

        // Slot count should remain 10 (directory doesn't shrink on delete)
        assertEquals(10, page.getSlotCount(), "Should still have 10 slots");
    }

    @Test
    void insertFailsWhenPageFull() {
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, 8192);

        // Fill page with small rows until almost full
        int rowSize = 50;
        int maxRows = (8192 - PageHeader.HEADER_SIZE) / (rowSize + 8); // rough estimate
        int inserted = 0;

        try {
            while (true) {
                byte[] row = uniqueBytes(rowSize, inserted);
                page.insert(row);
                inserted++;
            }
        } catch (PageFullException e) {
            // Expected
        }

        // Verify we inserted some rows
        assertTrue(inserted > 0, "Should have inserted at least one row");
        assertTrue(inserted < maxRows * 2, "Should not have inserted too many rows"); // sanity check

        // Verify page is full - try inserting something that definitely won't fit
        try {
            page.insert(uniqueBytes(1000, 999)); // 1000 bytes definitely won't fit in remaining space
            fail("Should have thrown PageFullException");
        } catch (PageFullException e) {
            // Expected
        }
    }

    @Test
    void pageSizes8k16k64kAreConfigurable() {
        PageId pageId = new PageId(1, 1, 1);

        // Test each allowed size
        int[] sizes = {8192, 16384, 65536};
        for (int size : sizes) {
            Page page = new Page(pageId, size);
            assertEquals(size, page.getPageSize(), "Page size should match");
            
            // Insert at least one row to verify it works
            byte[] row = uniqueBytes(100, size);
            int slotId = page.insert(row);
            assertEquals(0, slotId, "Should have inserted in slot 0");
            assertNotNull(page.get(0), "Should be able to retrieve the row");
        }
    }

    @Test
    void pageSizeConfigFallbackOnInvalidValue() {
        PageId pageId = new PageId(1, 1, 1);

        // Test invalid raw value
        System.setProperty(Page.PAGE_SIZE_KEY, "99999");
        Page page = new Page(pageId); // uses default size
        assertEquals(8192, page.getPageSize(), "Should fall back to 8192");

        // Test invalid K value
        System.setProperty(Page.PAGE_SIZE_KEY, "999K");
        page = new Page(pageId);
        assertEquals(8192, page.getPageSize(), "Should fall back to 8192");

        // Test valid K values
        System.setProperty(Page.PAGE_SIZE_KEY, "16K");
        page = new Page(pageId);
        assertEquals(16384, page.getPageSize(), "Should parse 16K as 16384");

        System.setProperty(Page.PAGE_SIZE_KEY, "64KB");
        page = new Page(pageId);
        assertEquals(65536, page.getPageSize(), "Should parse 64KB as 65536");

        // Test valid raw values
        System.setProperty(Page.PAGE_SIZE_KEY, "16384");
        page = new Page(pageId);
        assertEquals(16384, page.getPageSize(), "Should parse raw 16384");

        // Clean up
        System.clearProperty(Page.PAGE_SIZE_KEY);
    }

    @Test
    void readFromRejectsCorruptMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.insert(uniqueBytes(100, 1));
        page.writeTo(buffer);

        // Corrupt magic
        buffer.putInt(0, 0x12345678); // wrong magic
        buffer.rewind();

        assertThrows(PageFormatException.class, () -> Page.readFrom(buffer), 
                "Should reject corrupt magic");
    }

    @Test
    void readFromRejectsCorruptVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.insert(uniqueBytes(100, 1));
        page.writeTo(buffer);

        // Corrupt version
        buffer.putShort(4, (short) 999); // wrong version
        buffer.rewind();

        assertThrows(PageFormatException.class, () -> Page.readFrom(buffer), 
                "Should reject corrupt version");
    }

    @Test
    void readFromRejectsInvalidPageSize() {
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        Page page = new Page(new PageId(1, 1, 1), 8192);
        page.insert(uniqueBytes(100, 1));
        page.writeTo(buffer);

        // Rewind to start before reading header
        buffer.rewind();
        byte[] headerBytes = new byte[PageHeader.HEADER_SIZE];
        buffer.get(headerBytes, 0, PageHeader.HEADER_SIZE);
        
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
        headerBuffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        headerBuffer.putInt(56, 99999); // corrupt pageSize
        
        // Reset buffer position and write corrupted header back
        buffer.rewind();
        buffer.put(headerBytes, 0, PageHeader.HEADER_SIZE);
        buffer.rewind();

        assertThrows(PageFormatException.class, () -> Page.readFrom(buffer), 
                "Should reject invalid page size");
    }

    @Test
    void equalityAndHashCode() {
        PageId pageId = new PageId(1, 1, 1);
        Page page1 = new Page(pageId, 8192);
        Page page2 = new Page(pageId, 8192);
        Page page3 = new Page(new PageId(1, 1, 2), 8192);
        Page page4 = new Page(pageId, 16384);

        // Same ID and size
        page1.insert(uniqueBytes(100, 1));
        page2.insert(uniqueBytes(100, 1));
        assertEquals(page1, page2, "Pages with same ID and size should be equal");
        assertEquals(page1.hashCode(), page2.hashCode(), "Equal pages should have same hash code");

        // Different ID
        assertNotEquals(page1, page3, "Pages with different ID should not be equal");

        // Different size
        assertNotEquals(page1, page4, "Pages with different size should not be equal");
    }
}