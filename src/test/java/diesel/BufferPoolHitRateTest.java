package diesel;

import diesel.storage.page.BufferPool;
import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PinnedPage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Statistical hit-rate check for BufferPool (prompt4.md step 7): a hot set of
 * 96% of requests hits 6% of the frames, a cold set walks a huge id space —
 * with LRU the observed rate must exceed 95% (uniform-random replacement
 * would score ~6% here, so the assertion genuinely pins down the policy).
 *
 * <p>Read-only miss path: the loader hands back empty pages (clean), no
 * flusher is needed even at close.
 */
@Tag("storage")
class BufferPoolHitRateTest {

    private static final int PAGE_SIZE = 8192;
    /** 1 GB / 8 KB — big enough that the cold walk rarely repeats. */
    private static final int COLD_PAGES = 131_072;
    /** 100 MB / 8 KB — the hot set lives entirely inside the pool. */
    private static final int CAPACITY = 12_800;
    private static final int HOT_PAGES = 64;
    private static final int REQUESTS = 1_000_000;

    @Test
    void hotSetWorkingSetKeepsHitRateAbove95Percent() throws Exception {
        Random random = new Random(42);
        PageId[] hot = new PageId[HOT_PAGES];
        for (int i = 0; i < HOT_PAGES; i++) {
            hot[i] = new PageId(1, 1, i);
        }

        try (BufferPool pool = new BufferPool(CAPACITY, PAGE_SIZE, null,
                id -> new Page(id, PAGE_SIZE))) {
            // mixed workload: i % 100 < 96 selects the hot set (96% of requests)
            for (int i = 0; i < REQUESTS; i++) {
                PageId id = (i % 100) < 96
                        ? hot[random.nextInt(HOT_PAGES)]
                        : new PageId(1, 1, random.nextInt(COLD_PAGES));
                try (PinnedPage pinned = pool.pin(id)) {
                    assertNotNull(pinned.getPage());
                }
            }

            long hits = pool.getHits();
            long misses = pool.getMisses();
            assertEquals(REQUESTS, hits + misses, "every request is a hit or a miss");

            assertTrue(pool.getEvictions() > 0, "cold walk must overflow the pool");
            assertTrue(hits > 0, "hot set must produce hits");
            double rate = pool.getHitRate();
            assertTrue(rate > 0.95,
                    "expected >95% hit rate, got " + (rate * 100) + "%");
            assertEquals(CAPACITY, pool.getResidentPages(),
                    "pool must stay saturated by the cold walk");
        }
    }
}
