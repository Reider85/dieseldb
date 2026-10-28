package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-6: after one compaction defrag, the slot directory holds only
 * live slots (slotCount = L), so every subsequent O(slotCount) operation -
 * further defrags, validateLayout, full scans - costs O(L), not
 * O(historical inserts).
 *
 * <p>The first defrag after heavy churn is inherently O(S): it must scan the
 * whole tombstone-riddled directory once to discover the live slots. The
 * O(L) benefit applies from then on, which is exactly what this test
 * measures: repeated defrags on a churned-then-compacted page (A) cost the
 * same as defrags on a fresh page with only the live slots (B). Under the
 * old directory-never-shrinks code, A kept slotCount=3000 forever (~30x
 * B's 100 slots per defrag).
 *
 * <p>Timing gates follow the DirtyPageTrackingIsODTest recipe: structural
 * assert (slotCount == live count) is load-independent; the timing assert
 * is a relative big/small ratio with an unmeasured JIT warm-up plus a
 * generous absolute bound (min-of-3 filters scheduler pauses).
 */
@Tag("storage")
class DefragIsOLiveTest {

    private static final int PAGE_SIZE = 65536; // max slots = 8184
    private static final int HISTORICAL_INSERTS = 3_000;
    private static final int LIVE = 100;
    private static final int ITERATIONS = 5_000;
    private static final long ABSOLUTE_BOUND_MILLIS = 2_000;

    private static byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    /** Page with HISTORICAL_INSERTS slots, only the last LIVE tuples alive, compacted by one defrag. */
    private static Page churnedCompactedPage() {
        Page page = new Page(new PageId(3, 3, 1), PAGE_SIZE);
        for (int i = 0; i < HISTORICAL_INSERTS; i++) {
            page.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < HISTORICAL_INSERTS - LIVE; i++) {
            page.delete(i);
        }
        page.defrag();
        assertEquals(LIVE, page.getSlotCount(),
                "one defrag must shrink slotCount from " + HISTORICAL_INSERTS + " to the live count " + LIVE);
        return page;
    }

    /** Fresh page holding exactly LIVE tuples - the O(L) reference cost. */
    private static Page freshLivePage() {
        Page page = new Page(new PageId(3, 3, 2), PAGE_SIZE);
        for (int i = 0; i < LIVE; i++) {
            page.insert(uniqueBytes(8, i));
        }
        assertEquals(LIVE, page.getSlotCount());
        return page;
    }

    /** Best-of-3 wall time of ITERATIONS defrags; setup stays outside the timed window. */
    private static long timeDefragLoopMillis(Page page) {
        long best = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 3; attempt++) {
            long start = System.nanoTime();
            long reclaimedSum = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                reclaimedSum += page.defrag();
            }
            best = Math.min(best, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            // Dead-code guard + idempotence: a compacted page reclaims nothing, ever.
            assertEquals(0, reclaimedSum, "repeated defrag of a compacted page must reclaim 0 bytes");
        }
        return best;
    }

    @Test
    @Timeout(60)
    void defragAfterCompactionCostsOLiveNotOhistoricalInserts() {
        Page churned = churnedCompactedPage();
        Page fresh = freshLivePage();
        assertEquals(fresh.getSlotCount(), churned.getSlotCount(),
                "after compaction both pages must have identical directory sizes");

        // Unmeasured warm-up on both pages: pays JIT compilation of the shared
        // defrag path so the measured ratio reflects the algorithm, not the
        // order in which pages were first compiled (EvictionUnderPinnedHeadTest lesson).
        timeDefragLoopMillis(churned);
        timeDefragLoopMillis(fresh);

        long churnedMillis = timeDefragLoopMillis(churned);
        long freshMillis = timeDefragLoopMillis(fresh);

        assertTrue(churnedMillis < ABSOLUTE_BOUND_MILLIS,
                ITERATIONS + " defrags of a compacted " + HISTORICAL_INSERTS + "-slot page took "
                        + churnedMillis + " ms, expected < " + ABSOLUTE_BOUND_MILLIS
                        + " ms (slot directory compacted to " + LIVE + " live slots)");
        assertTrue(churnedMillis <= 3 * Math.max(freshMillis, 1),
                "defrag of churned-then-compacted page took " + churnedMillis + " ms vs " + freshMillis
                        + " ms for a fresh " + LIVE + "-slot page (min-of-3, " + ITERATIONS
                        + " defrags each) - defrag cost must track the live count, not historical inserts (bound 3x)");
    }

    @Test
    @Timeout(30)
    void compactionKeepsValidateLayoutCheapAndCorrect() {
        Page page = churnedCompactedPage();
        page.validate();

        // A second churn cycle on the compacted page stays bounded: insert/delete
        // never grow slotCount beyond live + tombstones of this cycle, and one
        // defrag compacts again.
        for (int i = 0; i < LIVE; i++) {
            page.delete(i);
        }
        for (int i = 0; i < LIVE; i++) {
            page.insert(uniqueBytes(8, 5_000 + i));
        }
        assertEquals(2 * LIVE, page.getSlotCount(),
                "re-inserts append after the compacted directory (no free-slot reuse in Buffer-6)");
        page.defrag();
        assertEquals(LIVE, page.getSlotCount(), "second churn cycle must compact back to the live count");
        page.validate();
    }
}
