package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-7: 100k insert/delete cycles on one page leave slotCount bounded
 * by the maximum number of concurrent live tuples, not by historical inserts.
 *
 * <p>Steady-state pattern: after warming up K live tuples, each cycle deletes
 * slot {@code c % K} and immediately re-inserts - the free-slot stack hands
 * back exactly the slot just freed, so slotCount stays K forever. Under the
 * old append-only directory it grew to ~100_000.
 *
 * <p>Tuple space is reclaimed only by defrag (delete is a pure tombstone by
 * design), so the page is defragmented every DEFrag_EVERY_CYCLES cycles to stay
 * physically insertable; with reuse each such defrag is O(K), not O(cycles).
 */
@Tag("storage")
class SlotDirectoryDoesNotGrowUnboundedlyTest {

    private static final int PAGE_SIZE = 65536;
    private static final int LIVE = 50;               // max concurrent live tuples
    private static final int CYCLES = 100_000;
    private static final int DEFRAG_EVERY_CYCLES = 10_000;
    private static final int CHECKPOINT_EVERY = 1_000;

    private static byte[] uniqueBytes(int size, long seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    @Test
    @Timeout(120)
    void slotCountStaysBoundedByMaxConcurrentLiveTuples() {
        Page page = new Page(new PageId(8, 8, 1), PAGE_SIZE);

        // Warm-up: fill to K live tuples (no tombstones exist yet - pure appends)
        for (int i = 0; i < LIVE; i++) {
            assertEquals(i, page.insert(uniqueBytes(4, i)),
                    "warm-up inserts append in order");
        }
        assertEquals(LIVE, page.getSlotCount(), "Precondition: slotCount == live after warm-up");

        for (long c = 0; c < CYCLES; c++) {
            int slot = (int) (c % LIVE);
            page.delete(slot);
            int reused = page.insert(uniqueBytes(4, LIVE + c));
            assertEquals(slot, reused,
                    "cycle " + c + ": insert must reuse the slot just freed, got " + reused);

            if ((c + 1) % CHECKPOINT_EVERY == 0) {
                assertEquals(LIVE, page.getSlotCount(),
                        "after " + (c + 1) + " insert/delete cycles slotCount must stay at the "
                                + "concurrent-live bound " + LIVE
                                + " (old append-only directory would be at ~" + (c + 1) + ")");
            }
            if ((c + 1) % DEFRAG_EVERY_CYCLES == 0) {
                page.defrag(); // reclaim dead tuple bytes; all K slots live -> ids stay 0..K-1
                assertEquals(LIVE, page.getSlotCount(),
                        "defrag of a reuse-bounded page must keep slotCount == live count");
            }
        }

        assertEquals(LIVE, page.getSlotCount(),
                CYCLES + " cycles must leave slotCount at the live bound, not " + CYCLES);
        page.validate();

        // Still functional: one more churn cycle works and stays bounded
        page.delete(0);
        assertEquals(0, page.insert(uniqueBytes(4, 999_999)), "must reuse the freed slot 0");
        assertEquals(LIVE, page.getSlotCount(), "post-check churn must not grow the directory");
        page.validate();
    }
}
