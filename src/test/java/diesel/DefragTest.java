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

        // Verify all remaining 25 rows are still readable and correct.
        // o2 Buffer-6: defrag renumbers live slots to a contiguous 0..L-1 range,
        // so the surviving odd-indexed rows now live in slots 0..24.
        for (int k = 0; k < 25; k++) {
            int originalIndex = 2 * k + 1;
            byte[] retrieved = page.get(k);
            assertNotNull(retrieved, "Renumbered slot " + k + " should not be null");
            assertEquals(50, retrieved.length, "Renumbered slot " + k + " should have correct length");
            assertTrue(Arrays.equals(rows[originalIndex], retrieved),
                    "Renumbered slot " + k + " should hold original row " + originalIndex);
        }
        // Slots beyond the compacted directory read as null
        for (int k = 25; k < 50; k++) {
            assertNull(page.get(k), "Slot " + k + " beyond compacted directory should be null");
        }

        // Verify slot count shrank to the live count (directory compacted)
        assertEquals(25, page.getSlotCount(), "Slot count should equal live tuple count after defrag");
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

        // Verify exact free space (proves packing + directory compaction)
        // Expected: total live bytes = 100+50+150+80+120+180+90 = 770
        // Expected free space = 8192 - PageHeader.HEADER_SIZE - 7*8 - 770 = 8192 - 64 - 56 - 770 = 7302
        // (o2 Buffer-6: the 3 deleted slots are reclaimed, directory shrinks 10 -> 7 entries)
        int expectedFreeSpace = 7302;
        assertEquals(expectedFreeSpace, page.getFreeSpace(), 
                "Free space should match expected value after defrag");
        assertEquals(freeBeforeDefrag + reclaimed, page.getFreeSpace(), 
                "Reclaimed bytes added to original free space should equal new free space");

        // Verify all remaining rows are readable under renumbered contiguous slot ids 0..6
        int[] remainingIndices = {0, 2, 4, 5, 7, 8, 9};
        assertEquals(remainingIndices.length, page.getSlotCount(),
                "Slot count should equal live tuple count after defrag");
        for (int newSlotId = 0; newSlotId < remainingIndices.length; newSlotId++) {
            int originalIndex = remainingIndices[newSlotId];
            byte[] retrieved = page.get(newSlotId);
            assertNotNull(retrieved, "Renumbered slot " + newSlotId + " should not be null");
            assertTrue(Arrays.equals(rows[originalIndex], retrieved),
                    "Renumbered slot " + newSlotId + " should hold original row " + originalIndex);
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

        // Defragment should reclaim almost all space, including the tombstone directory
        int reclaimed = page.defrag();
        int expectedFreeSpace = pageSize - PageHeader.HEADER_SIZE; // header only: directory compacted to 0 slots
        assertEquals(expectedFreeSpace, page.getFreeSpace(), 
                "Free space should be almost entire page");
        assertEquals(freeBeforeDefrag + 1080, page.getFreeSpace(), 
                "Reclaimed 1000 bytes of deleted tuples + 80 bytes of tombstone directory");
        assertEquals(0, page.getSlotCount(), "Slot count should be 0 after all-delete defrag");

        // Verify all rows are deleted (slots beyond compacted directory read as null)
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
            byte[][] rows = new byte[5][];
            for (int i = 0; i < 5; i++) {
                rows[i] = uniqueBytes(100, i);
                page.insert(rows[i]);
            }

            // Delete some
            page.delete(1);
            page.delete(3);

            // Defrag should work on any size
            int reclaimed = page.defrag();
            assertTrue(reclaimed > 0, "Defrag should reclaim bytes on " + pageSize + " page");

            // Verify rows are still readable under renumbered slot ids:
            // live rows {0,2,4} are compacted into slots {0,1,2}
            assertNotNull(page.get(0), "Row 0 should be readable");
            assertTrue(Arrays.equals(rows[0], page.get(0)), "Slot 0 should hold original row 0");
            assertNotNull(page.get(1), "Row 2 should be readable at renumbered slot 1");
            assertTrue(Arrays.equals(rows[2], page.get(1)), "Slot 1 should hold original row 2");
            assertNotNull(page.get(2), "Row 4 should be readable at renumbered slot 2");
            assertTrue(Arrays.equals(rows[4], page.get(2)), "Slot 2 should hold original row 4");
            assertNull(page.get(3), "Slot 3 beyond compacted directory should be null");
            assertNull(page.get(4), "Slot 4 beyond compacted directory should be null");
            assertEquals(3, page.getSlotCount(), "Slot count should equal live tuple count on " + pageSize + " page");
        }
    }
}