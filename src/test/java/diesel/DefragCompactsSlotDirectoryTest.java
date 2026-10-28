package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageHeader;
import diesel.storage.page.PageId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-6 acceptance: {@code SlottedPageLayout.defragment} compacts the
 * slot directory, renumbering live slots to a contiguous 0..L-1 range so
 * slotCount equals the live tuple count instead of historical inserts.
 */
@Tag("storage")
class DefragCompactsSlotDirectoryTest {

    private static final int PAGE_SIZE = 65536; // max slots = 8184; 1000 inserts fit comfortably

    private static byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    @Test
    void defragShrinksSlotCountToLiveCount() {
        Page page = new Page(new PageId(1, 1, 1), PAGE_SIZE);

        // Workload from the prompt: 1000 inserts + 900 deletes + 1 defrag
        byte[][] rows = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            rows[i] = uniqueBytes(8, i);
            page.insert(rows[i]);
        }
        assertEquals(1000, page.getSlotCount(), "Precondition: slotCount == inserts");

        for (int i = 0; i < 900; i++) {
            page.delete(i);
        }

        page.defrag();

        assertEquals(100, page.getSlotCount(),
                "slotCount should equal live tuple count (100), not historical inserts (1000)");
        // Directory reclaimed the 900 tombstone entries: freeSpaceStart = HEADER + 100*8
        page.validate();
        assertEquals(PAGE_SIZE - 100 * 8 - PageHeader.HEADER_SIZE - 100 * 8, page.getFreeSpace(),
                "Free space = page - header - live directory (100*8) - live tuples (100*8)");
    }

    @Test
    void defragRenumbersLiveSlotsContiguously() {
        Page page = new Page(new PageId(1, 1, 1), PAGE_SIZE);

        byte[][] rows = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            rows[i] = uniqueBytes(8, i);
            page.insert(rows[i]);
        }
        // Delete a strided pattern so tombstones are interleaved with live slots
        for (int i = 0; i < 900; i++) {
            page.delete(i);
        }
        page.defrag();

        // Surviving originals are rows 900..999, renumbered to slots 0..99
        for (int k = 0; k < 100; k++) {
            byte[] retrieved = page.get(k);
            assertNotNull(retrieved, "Renumbered slot " + k + " should be live");
            assertTrue(Arrays.equals(rows[900 + k], retrieved),
                    "Renumbered slot " + k + " should hold original row " + (900 + k));
        }
        for (int k = 100; k < 1000; k++) {
            assertNull(page.get(k), "Slot " + k + " beyond compacted directory should read as null");
        }
        page.validate();
    }

    @Test
    void repeatedDefragIsIdempotent() {
        Page page = new Page(new PageId(1, 1, 1), PAGE_SIZE);

        for (int i = 0; i < 1000; i++) {
            page.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < 900; i++) {
            page.delete(i);
        }

        int reclaimedFirst = page.defrag();
        assertTrue(reclaimedFirst > 0, "First defrag should reclaim tuple + directory bytes");
        int slotCountAfterFirst = page.getSlotCount();
        int freeAfterFirst = page.getFreeSpace();

        int reclaimedSecond = page.defrag();
        assertEquals(0, reclaimedSecond, "Second defrag on a compacted page should reclaim nothing");
        assertEquals(slotCountAfterFirst, page.getSlotCount(), "slotCount should be stable");
        assertEquals(freeAfterFirst, page.getFreeSpace(), "free space should be stable");
        page.validate();
    }

    @Test
    void compactedPageAcceptsFurtherInsertsAtContiguousSlots() {
        Page page = new Page(new PageId(1, 1, 1), PAGE_SIZE);

        for (int i = 0; i < 1000; i++) {
            page.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < 900; i++) {
            page.delete(i);
        }
        page.defrag();
        assertEquals(100, page.getSlotCount());

        // Next insert reuses the slot right after the compacted directory
        int slotId = page.insert(uniqueBytes(8, 42));
        assertEquals(100, slotId, "Insert after defrag should append at slot == compacted slotCount");
        assertEquals(101, page.getSlotCount());
        assertTrue(Arrays.equals(uniqueBytes(8, 42), page.get(slotId)));
        page.validate();
    }
}
