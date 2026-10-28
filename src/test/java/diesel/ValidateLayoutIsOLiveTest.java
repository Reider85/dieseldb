package diesel;

import diesel.storage.page.Page;
import diesel.storage.page.PageFormatException;
import diesel.storage.page.PageHeader;
import diesel.storage.page.PageId;
import diesel.storage.page.SlottedPageLayout;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * o2 Buffer-8: the live-tuple count is maintained by insert/delete/defrag and
 * cached by validateLayout, so validating a page with zero live tuples
 * short-circuits the per-slot loop instead of walking a tombstone-riddled
 * directory (a churned page with slotCount=3000 all tombstones costs O(1),
 * not O(3000), per validate).
 *
 * <p>Timing gates follow the DefragIsOLiveTest recipe: structural asserts are
 * load-independent; the timing assert is a relative big/small ratio with an
 * unmeasured JIT warm-up plus a generous absolute bound (min-of-3 filters
 * scheduler pauses).
 */
@Tag("storage")
class ValidateLayoutIsOLiveTest {

    private static final int PAGE_SIZE = 65536;
    private static final int CHURN = 3_000;
    private static final int ITERATIONS = 20_000;
    private static final long ABSOLUTE_BOUND_MILLIS = 2_000;

    private static byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    /** Page with CHURN slots that were all deleted - directory full of tombstones, zero live. */
    private static Page churnedEmptyPage(int pageIdNum) {
        Page page = new Page(new PageId(9, 9, pageIdNum), PAGE_SIZE);
        for (int i = 0; i < CHURN; i++) {
            page.insert(uniqueBytes(8, i));
        }
        for (int i = 0; i < CHURN; i++) {
            page.delete(i);
        }
        assertEquals(CHURN, page.getSlotCount(), "delete-only churn keeps the directory size");
        assertEquals(0, page.getLiveCount(), "all tuples deleted -> live count is 0");
        return page;
    }

    @Test
    @Timeout(30)
    void liveCountTracksInsertDeleteDefrag() {
        Page page = new Page(new PageId(9, 9, 10), PAGE_SIZE);

        for (int i = 0; i < 100; i++) {
            page.insert(uniqueBytes(8, i));
        }
        assertEquals(100, page.getLiveCount(), "100 inserts -> 100 live");

        for (int i = 0; i < 40; i++) {
            page.delete(i);
        }
        assertEquals(60, page.getLiveCount(), "40 deletes -> 60 live");
        assertEquals(100, page.getSlotCount(), "slotCount unchanged by delete (tombstones)");

        // Re-inserts reuse tombstoned slots: live grows back, slotCount stays put
        for (int i = 0; i < 40; i++) {
            page.insert(uniqueBytes(8, 200 + i));
        }
        assertEquals(100, page.getLiveCount(), "re-inserts restore the live count");
        assertEquals(100, page.getSlotCount(), "reuses keep slotCount at the high-water mark");

        page.defrag();
        assertEquals(100, page.getLiveCount(), "defrag renumbering preserves the live count");
        assertEquals(100, page.getSlotCount(), "compacted directory equals the live count");

        // Delete everything, then defrag: page collapses to slotCount=0
        for (int i = 0; i < 100; i++) {
            page.delete(i);
        }
        assertEquals(0, page.getLiveCount(), "delete-all -> zero live");
        page.defrag();
        assertEquals(0, page.getSlotCount(), "defrag of an empty page zeroes the directory");
        assertEquals(0, page.getLiveCount(), "defrag of an empty page zeroes the live count");
        page.validate();
    }

    @Test
    @Timeout(60)
    void validateOnChurnedEmptyPageShortCircuits() {
        Page churned = churnedEmptyPage(1);
        Page freshEmpty = new Page(new PageId(9, 9, 2), PAGE_SIZE);
        assertEquals(0, freshEmpty.getLiveCount());

        // Unmeasured warm-up on both pages: pays JIT compilation of the shared
        // validate path so the measured ratio reflects the algorithm, not the
        // order in which pages were first compiled.
        timeValidateLoopMillis(churned);
        timeValidateLoopMillis(freshEmpty);

        long churnedMillis = timeValidateLoopMillis(churned);
        long freshMillis = timeValidateLoopMillis(freshEmpty);

        assertTrue(churnedMillis < ABSOLUTE_BOUND_MILLIS,
                ITERATIONS + " validates of a churned " + CHURN + "-tombstone page took "
                        + churnedMillis + " ms, expected < " + ABSOLUTE_BOUND_MILLIS
                        + " ms (empty-page short-circuit must skip the slot loop)");
        assertTrue(churnedMillis <= 3 * Math.max(freshMillis, 1),
                "validate of churned-empty page took " + churnedMillis + " ms vs " + freshMillis
                        + " ms for a fresh empty page (min-of-3, " + ITERATIONS
                        + " validates each) - validate must short-circuit zero-live pages (bound 3x)");
    }

    @Test
    @Timeout(30)
    void validateStillRunsAccountingCheckOnShortCircuitPath() {
        // Churn a page down to zero live tuples so liveCount is valid-0 and the
        // per-slot loop is short-circuited...
        PageHeader header = new PageHeader(new PageId(9, 9, 4), PAGE_SIZE);
        byte[] data = new byte[PAGE_SIZE];
        for (int i = 0; i < CHURN; i++) {
            SlottedPageLayout.insertTuple(data, header, uniqueBytes(8, i));
        }
        for (int i = 0; i < CHURN; i++) {
            SlottedPageLayout.deleteTuple(data, header, i);
        }
        assertEquals(0, SlottedPageLayout.countLiveTuples(data, header),
                "churned page has zero live tuples");
        SlottedPageLayout.validateLayout(data, header);

        // ...then corrupt the free-space accounting (data length no longer matches
        // the header's pageSize-derived accounting): the short-circuit must skip
        // only the slot loop, never the accounting check.
        byte[] shortData = new byte[PAGE_SIZE - 8];
        System.arraycopy(data, 0, shortData, 0, shortData.length);
        assertThrows(PageFormatException.class,
                () -> SlottedPageLayout.validateLayout(shortData, header),
                "empty-page short-circuit must still run the free-space accounting check");
    }

    /** Best-of-3 wall time of ITERATIONS validates; setup stays outside the timed window. */
    private static long timeValidateLoopMillis(Page page) {
        long best = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 3; attempt++) {
            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                page.validate();
            }
            best = Math.min(best, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        return best;
    }
}
