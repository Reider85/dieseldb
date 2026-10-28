package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-7 acceptance: {@code SlottedPageLayout.insertTuple} reuses
 * tombstoned slot ids popped from the free-slot stack, so slotCount is bounded
 * by the high-water mark of concurrent live tuples instead of historical
 * inserts.
 */
@Tag("storage")
class InsertReusesTombstoneSlotsTest {

    private static final int PAGE_SIZE = 65536; // max slots = 8184; 1500 inserts fit comfortably

    private static byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    @Test
    void insertAfterDeletesReusesTombstoneSlots() {
        Page page = new Page(new PageId(7, 7, 1), PAGE_SIZE);

        // Workload from the prompt: 1000 inserts + 500 deletes + 500 inserts
        byte[][] rows = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            rows[i] = uniqueBytes(8, i);
            assertEquals(i, page.insert(rows[i]), "no tombstones exist yet - append in order");
        }
        assertEquals(1000, page.getSlotCount(), "Precondition: slotCount == inserts");

        for (int i = 0; i < 500; i++) {
            page.delete(i);
        }

        byte[][] replacements = new byte[500][];
        Set<Integer> reusedSlotIds = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            replacements[i] = uniqueBytes(8, 10_000 + i);
            int slotId = page.insert(replacements[i]);
            assertTrue(slotId >= 0 && slotId < 500,
                    "insert must reuse a tombstoned slot from the deleted range, got " + slotId);
            reusedSlotIds.add(slotId);
        }

        assertEquals(1000, page.getSlotCount(),
                "slotCount must stay at 1000 (reuse), not grow to 1500");
        assertEquals(500, reusedSlotIds.size(),
                "the 500 re-inserts must occupy 500 distinct freed slots");

        // Survivors (slots 500..999) keep their original content
        for (int slotId = 500; slotId < 1000; slotId++) {
            assertArrayEquals(rows[slotId], page.get(slotId),
                    "survivor at slot " + slotId + " must be untouched");
        }
        // Every reused slot holds the content of exactly one replacement
        for (int i = 0; i < 500; i++) {
            int slotId = findSlotHolding(page, replacements[i]);
            assertTrue(slotId >= 0 && slotId < 500,
                    "replacement " + i + " must live in a reused slot, found at " + slotId);
        }
        page.validate();
    }

    @Test
    void tombstoneReuseSurvivesSerializationRoundTrip() {
        Page page = new Page(new PageId(7, 7, 2), PAGE_SIZE);

        byte[][] rows = new byte[10][];
        for (int i = 0; i < 10; i++) {
            rows[i] = uniqueBytes(30, i);
            page.insert(rows[i]);
        }
        page.delete(3);
        page.delete(5);
        page.delete(7);

        // Serialize + reload: the in-memory free-slot stack is NOT persisted; the
        // reloaded page must rebuild it from the directory tombstones (o2 Buffer-7)
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        page.writeTo(buffer);
        buffer.rewind();
        Page reloaded = Page.readFrom(buffer);

        assertEquals(10, reloaded.getSlotCount(), "Precondition: directory unchanged by round-trip");

        byte[] fresh = uniqueBytes(30, 99);
        int slotId = reloaded.insert(fresh);
        assertTrue(slotId == 3 || slotId == 5 || slotId == 7,
                "reloaded page must reuse a tombstone slot, got " + slotId);
        assertEquals(10, reloaded.getSlotCount(), "reuse after reload must not grow the directory");
        assertArrayEquals(fresh, reloaded.get(slotId));
        reloaded.validate();
    }

    private static int findSlotHolding(Page page, byte[] content) {
        for (int slotId = 0; slotId < page.getSlotCount(); slotId++) {
            byte[] tuple = page.get(slotId);
            if (tuple != null && Arrays.equals(content, tuple)) {
                return slotId;
            }
        }
        return -1;
    }
}
