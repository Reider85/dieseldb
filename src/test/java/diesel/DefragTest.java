package diesel;

import diesel.storage.page.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for page defragmentation (R3-002 step 1).
 * 
 * <p>Tests SlottedPageLayout.defragment() and Page.defrag() behavior,
 * including free space reclamation and tuple preservation.
 */
@Tag("storage")
class DefragTest {

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
    void defragRestoresFreeSpaceAfterDeletes() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Insert 50 rows of 50 bytes each
        byte[][] rows = new byte[50][50];
        for (int i = 0; i < 50; i++) {
            rows[i] = uniqueBytes(50, i);
            page.insert(rows[i]);
        }

        // Record free space before deletes
        int freeBefore = page.getFreeSpace();

        // Delete 25 rows (indices 0, 2, 4, ..., 48)
        for (int i = 0; i < 50; i += 2) {
            page.delete(i);
        }

        // Free space should not change immediately (deleted tuples still occupy space)
        int freeAfterDelete = page.getFreeSpace();
        assertEquals(freeBefore, freeAfterDelete, "Free space should be unchanged after deletes");

        // Defragment
        int reclaimed = page.defrag();

        // Verify free space after defrag
        int freeAfterDefrag = page.getFreeSpace();
        assertTrue(freeAfterDefrag >= 0.30 * pageSize, 
                "Free space after defrag should be >= 30% of page: " + freeAfterDefrag + " >= " + (0.30 * pageSize));
        assertTrue(freeAfterDefrag > freeBefore, 
                "Free space should increase after defrag: " + freeAfterDefrag + " > " + freeBefore);
        assertTrue(reclaimed > 0, "Defrag should reclaim some bytes: " + reclaimed);

        // Verify all remaining 25 rows are still readable and correct
        for (int i = 0; i < 50; i++) {
            if (i % 2 == 1) { // odd indices should remain (we deleted evens)
                byte[] retrieved = page.get(i);
                assertNotNull(retrieved, "Row " + i + " should not be null");
                assertEquals(50, retrieved.length, "Row " + i + " should have correct length");
                assertTrue(Arrays.equals(rows[i], retrieved), "Row " + i + " should match original");
            } else {
                assertNull(page.get(i), "Deleted row " + i + " should be null");
            }
        }

        // Verify slot count remains 50 (directory doesn't shrink)
        assertEquals(50, page.getSlotCount(), "Should still have 50 slots");
    }

    @Test
    void defragRepacksTuplesContiguously() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Insert 10 rows of varying sizes
        byte[][] rows = {
            uniqueBytes(100, 1),
            uniqueBytes(200, 2),
            uniqueBytes(50, 3),
            uniqueBytes(300, 4),
            uniqueBytes(150, 5),
            uniqueBytes(80, 6),
            uniqueBytes(250, 7),
            uniqueBytes(120, 8),
            uniqueBytes(180, 9),
            uniqueBytes(90, 10)
        };

        for (byte[] row : rows) {
            page.insert(row);
        }

        // Delete some rows to create gaps
        page.delete(1); // delete 200-byte row
        page.delete(3); // delete 300-byte row
        page.delete(6); // delete 250-byte row

        // Record free space before defrag
        int freeBeforeDefrag = page.getFreeSpace();

        // Defragment
        int reclaimed = page.defrag();

        // Verify exact free space (proves packing)
        // Expected: total live bytes = 100+50+150+80+120+180+90 = 770
        // Expected free space = 8192 - PageHeader.HEADER_SIZE - 10*8 - 770 = 8192 - 64 - 80 - 770 = 7278
        int expectedFreeSpace = 7278;
        assertEquals(expectedFreeSpace, page.getFreeSpace(), 
                "Free space should match expected value after defrag");
        assertEquals(freeBeforeDefrag + reclaimed, page.getFreeSpace(), 
                "Reclaimed bytes added to original free space should equal new free space");

        // Verify all remaining rows are readable and in correct order
        int[] remainingIndices = {0, 2, 4, 5, 7, 8, 9};
        for (int i = 0; i < remainingIndices.length; i++) {
            int slotId = remainingIndices[i];
            byte[] retrieved = page.get(slotId);
            assertNotNull(retrieved, "Row " + slotId + " should not be null");
            assertTrue(Arrays.equals(rows[slotId], retrieved), "Row " + slotId + " should match original");
        }
    }

    @Test
    void defragOnEmptyPageIsNoOp() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Initial state should be empty
        assertEquals(0, page.getSlotCount(), "Empty page should have 0 slots");
        assertEquals(pageSize - PageHeader.HEADER_SIZE, page.getFreeSpace(), 
                "Empty page should have all space free except header");

        // Defrag should do nothing
        int reclaimed = page.defrag();
        assertEquals(0, reclaimed, "Defrag on empty page should reclaim 0 bytes");

        // State should remain unchanged
        assertEquals(0, page.getSlotCount(), "Should still have 0 slots");
        assertEquals(pageSize - PageHeader.HEADER_SIZE, page.getFreeSpace(), 
                "Should still have all space free");
    }

    @Test
    void defragAfterAllDeletesFreesAlmostWholePage() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Insert 10 rows
        byte[][] rows = new byte[10][100];
        for (int i = 0; i < 10; i++) {
            rows[i] = uniqueBytes(100, i);
            page.insert(rows[i]);
        }

        // Delete all rows
        for (int i = 0; i < 10; i++) {
            page.delete(i);
        }

        // Record free space before defrag
        int freeBeforeDefrag = page.getFreeSpace();

        // Defragment should reclaim almost all space
        int reclaimed = page.defrag();
        int expectedFreeSpace = pageSize - PageHeader.HEADER_SIZE - 10 * 8; // header + slots only
        assertEquals(expectedFreeSpace, page.getFreeSpace(), 
                "Free space should be almost entire page");
        assertEquals(freeBeforeDefrag + 1000, page.getFreeSpace(), 
                "Reclaimed 1000 bytes of deleted tuples");

        // Verify all rows are deleted
        for (int i = 0; i < 10; i++) {
            assertNull(page.get(i), "Row " + i + " should be deleted");
        }
    }

    @Test
    void defragDoesNotAffectLiveTupleContent() {
        int pageSize = 8192;
        PageId pageId = new PageId(1, 1, 1);
        Page page = new Page(pageId, pageSize);

        // Insert rows with predictable content
        byte[][] rows = {
            uniqueBytes(50, 1),
            uniqueBytes(100, 2),
            uniqueBytes(75, 3)
        };

        for (byte[] row : rows) {
            page.insert(row);
        }

        // Defragment
        page.defrag();

        // Verify all rows are identical to originals
        for (int i = 0; i < rows.length; i++) {
            byte[] retrieved = page.get(i);
            assertNotNull(retrieved, "Row " + i + " should not be null");
            assertEquals(rows[i].length, retrieved.length, "Row " + i + " should have correct length");
            assertTrue(Arrays.equals(rows[i], retrieved), "Row " + i + " should match original exactly");
        }
    }

    @Test
    void defragWithDifferentPageSizes() {
        // Test defrag on all supported page sizes
        int[] sizes = {8192, 16384, 65536};
        PageId pageId = new PageId(1, 1, 1);

        for (int pageSize : sizes) {
            Page page = new Page(pageId, pageSize);

            // Insert some rows
            for (int i = 0; i < 5; i++) {
                page.insert(uniqueBytes(100, i));
            }

            // Delete some
            page.delete(1);
            page.delete(3);

            // Defrag should work on any size
            int reclaimed = page.defrag();
            assertTrue(reclaimed > 0, "Defrag should reclaim bytes on " + pageSize + " page");

            // Verify rows are still readable
            assertNotNull(page.get(0), "Row 0 should be readable");
            assertNull(page.get(1), "Row 1 should be deleted");
            assertNotNull(page.get(2), "Row 2 should be readable");
            assertNull(page.get(3), "Row 3 should be deleted");
            assertNotNull(page.get(4), "Row 4 should be readable");
        }
    }
}