package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageId;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-8: a full-page tuple iteration (the CatalogTable read pattern:
 * loop {@code 0..slotCount}, skip null tuples) touches only live slots after
 * the Buffer-6/7 fixes - slotCount tracks the high-water mark of concurrent
 * live tuples and defrag compacts the directory down to the live count, so
 * the scan costs O(live), not O(historical inserts).
 *
 * <p>Timing gates follow the DefragIsOLiveTest recipe: structural asserts are
 * load-independent; the timing assert is a relative big/small ratio with an
 * unmeasured JIT warm-up plus a generous absolute bound (min-of-3 filters
 * scheduler pauses).
 */
@Tag("storage")
class FullPageScanIsOLiveTest {

    private static final int PAGE_SIZE = 65536;
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

    /** CatalogTable.readCatalogBytes scan pattern: iterate every slot, count non-null tuples. */
    private static int scanLiveTuples(Page page) {
        int found = 0;
        for (int slot = 0; slot < page.getSlotCount(); slot++) {
            if (page.get(slot) != null) {
                found++;
            }
        }
        return found;
    }

    /** Page with HISTORICAL_INSERTS slots, only the last LIVE tuples alive, compacted by one defrag. */
    private static Page churnedCompactedPage() {
        Page page = new Page(new PageId(10, 10, 1), PAGE_SIZE);
        for (int i = 0; i < HISTORICAL_INSERTS; i++) {
            page.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < HISTORICAL_INSERTS - LIVE; i++) {
            page.delete(i);
        }
        page.defrag();
        assertEquals(LIVE, page.getSlotCount());
        return page;
    }

    /** Fresh page holding exactly LIVE tuples - the O(L) reference cost. */
    private static Page freshLivePage() {
        Page page = new Page(new PageId(10, 10, 2), PAGE_SIZE);
        for (int i = 0; i < LIVE; i++) {
            page.insert(uniqueBytes(8, i));
        }
        return page;
    }

    @Test
    @Timeout(60)
    void scanTouchesOnlyLiveSlots() {
        // Compacted page: the directory IS the live set - scan iterates exactly
        // slotCount == LIVE slots and every one of them is non-null.
        Page compacted = churnedCompactedPage();
        assertEquals(LIVE, compacted.getLiveCount());
        assertEquals(LIVE, scanLiveTuples(compacted),
                "compacted page scan must find exactly the live tuples, no nulls in the directory");

        // Churned (not yet defragged) page: the scan walks the high-water
        // directory but still finds exactly the live tuples, skipping tombstones.
        Page churned = new Page(new PageId(10, 10, 3), PAGE_SIZE);
        for (int i = 0; i < HISTORICAL_INSERTS; i++) {
            churned.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < HISTORICAL_INSERTS - LIVE; i++) {
            churned.delete(i);
        }
        assertEquals(HISTORICAL_INSERTS, churned.getSlotCount(),
                "without defrag the directory keeps the high-water mark (o2 Buffer-7 bound)");
        assertEquals(LIVE, churned.getLiveCount(), "live count tracks the surviving tuples");
        assertEquals(LIVE, scanLiveTuples(churned),
                "tombstone-skipping scan must find exactly the live tuples");
    }

    @Test
    @Timeout(60)
    void scanCostTracksLiveCount() {
        Page churned = churnedCompactedPage();
        Page fresh = freshLivePage();
        assertEquals(fresh.getSlotCount(), churned.getSlotCount(),
                "after compaction both pages have identical directory sizes");

        // Unmeasured warm-up on both pages: pays JIT compilation of the shared
        // scan path so the measured ratio reflects the algorithm, not the
        // order in which pages were first compiled.
        timeScanLoopMillis(churned);
        timeScanLoopMillis(fresh);

        long churnedMillis = timeScanLoopMillis(churned);
        long freshMillis = timeScanLoopMillis(fresh);

        assertTrue(churnedMillis < ABSOLUTE_BOUND_MILLIS,
                ITERATIONS + " full scans of a compacted " + HISTORICAL_INSERTS + "-slot page took "
                        + churnedMillis + " ms, expected < " + ABSOLUTE_BOUND_MILLIS
                        + " ms (slot directory compacted to " + LIVE + " live slots)");
        assertTrue(churnedMillis <= 3 * Math.max(freshMillis, 1),
                "scan of churned-then-compacted page took " + churnedMillis + " ms vs " + freshMillis
                        + " ms for a fresh " + LIVE + "-slot page (min-of-3, " + ITERATIONS
                        + " scans each) - scan cost must track the live count, not historical inserts (bound 3x)");
    }

    /** Best-of-3 wall time of ITERATIONS full scans; setup stays outside the timed window. */
    private static long timeScanLoopMillis(Page page) {
        long best = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 3; attempt++) {
            long start = System.nanoTime();
            int foundSum = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                foundSum += scanLiveTuples(page);
            }
            best = Math.min(best, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            assertEquals((long) LIVE * ITERATIONS, foundSum,
                    "every scan pass must see the same live tuples");
        }
        return best;
    }
}
