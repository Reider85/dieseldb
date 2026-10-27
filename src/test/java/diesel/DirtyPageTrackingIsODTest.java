package diesel;

import diesel.storage.page.BufferPool;
import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PinnedPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.*;

/**
 * O2 Buffer-4: the buffer pool must track dirty pages in a dedicated
 * insertion-ordered index instead of scanning every resident frame.
 *
 * <p>{@code getDirtyPageCount} is now O(1) and the flush paths are O(D)
 * (D = dirty pages) instead of O(F) (F = frames). Correctness of the index
 * is asserted via exact flush/eligible/pin counts, which are
 * load-independent. Timing gates are load-tolerant:
 *
 * <ul>
 *   <li>{@code getDirtyPageCount} - absolute wall bound: 20k calls over a
 *       10k-frame pool must stay under 1s; the old full-frame scan would
 *       take seconds (10k volatile reads per call).</li>
 *   <li>{@code flushDirtyMatching} - relative bound: the same flush loop
 *       over 10k frames must cost no more than 20x the loop over 4 frames.
 *       The old full-frame scan scaled ~2500x between those pools; the new
 *       index path is frame-count independent, and taking the ratio keeps
 *       the assertion immune to machine speed and parallel-suite load.</li>
 * </ul>
 *
 * <p>Consolidates the two prompt-named classes (DirtyPageCountIsO1Test,
 * FlushDirtyOnlyIteratesDirtyTest) plus a small index-lifecycle method.
 */
@Tag("storage")
class DirtyPageTrackingIsODTest {

    private static final int PAGE_SIZE = 8192;
    private static final int FRAMES = 20_000;
    private static final int DIRTY = 4;
    private static final long BOUND_MILLIS = 1_000;

    private static PageId pageId(int n) {
        return new PageId(2, 2, n);
    }

    /** Fills a pool with {@code count} clean resident pages. */
    private static List<Page> fillClean(BufferPool pool, int count) throws IOException {
        List<Page> pages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Page page = new Page(pageId(i), PAGE_SIZE);
            try (PinnedPage pinned = pool.insert(page)) {
                assertEquals(page, pinned.getPage(), "insert must host the page");
            }
            pages.add(page);
        }
        return pages;
    }

    @Test
    @Timeout(60)
    void getDirtyPageCountIsConstantTime() throws Exception {
        int dirty = 1_000;
        int calls = 20_000;
        try (BufferPool pool = new BufferPool(FRAMES, PAGE_SIZE, page -> { }, null)) {
            List<Page> pages = fillClean(pool, FRAMES);
            for (int i = 0; i < dirty; i++) {
                pages.get(i).setDirty(true);
            }
            assertEquals(dirty, pool.getDirtyPageCount(), "index must hold exactly the dirty pages");

            // Warm-up + dead-code guard: the accumulated sum forces every call to run.
            long sum = 0;
            for (int i = 0; i < 2_000; i++) {
                sum += pool.getDirtyPageCount();
            }
            assertEquals(2_000L * dirty, sum);

            long start = System.nanoTime();
            for (int i = 0; i < calls; i++) {
                sum += pool.getDirtyPageCount();
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertEquals((2_000L + calls) * dirty, sum, "every read must see the full count");
            assertTrue(elapsedMillis < BOUND_MILLIS,
                    calls + " getDirtyPageCount calls over " + FRAMES + " frames took " + elapsedMillis
                            + " ms, expected < " + BOUND_MILLIS + " ms (O(1) dirty-page index)");
        }
    }

    /**
     * Times {@code iterations} flushDirtyMatching cycles on a pool with
     * {@code frames} resident pages (DIRTY of them dirty) and returns the
     * best of three runs. Setup (page allocation, inserts) stays outside the
     * timed window so only the flush path is measured; min-of-3 filters
     * scheduler pauses.
     */
    private static long timeFlushLoopMillis(int frames, int iterations) throws Exception {
        AtomicInteger eligibleCalls = new AtomicInteger();
        List<PageId> flushedIds = new ArrayList<>();
        long best = Long.MAX_VALUE;
        try (BufferPool pool = new BufferPool(frames, PAGE_SIZE, page -> flushedIds.add(page.getPageId()), null)) {
            List<Page> pages = fillClean(pool, frames);
            for (int i = 0; i < DIRTY; i++) {
                pages.get(i).setDirty(true);
            }
            assertEquals(DIRTY, pool.getDirtyPageCount());

            for (int attempt = 0; attempt < 3; attempt++) {
                eligibleCalls.set(0);
                flushedIds.clear();
                long start = System.nanoTime();
                for (int i = 0; i < iterations; i++) {
                    // Re-dirty the flushed pages so every cycle has the same work.
                    for (int d = 0; d < DIRTY; d++) {
                        pages.get(d).setDirty(true);
                    }
                    assertEquals(DIRTY, pool.flushDirtyMatching(page -> {
                        eligibleCalls.incrementAndGet();
                        return true;
                    }), "every dirty unpinned page must flush each cycle");
                }
                best = Math.min(best, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                assertEquals((long) iterations * DIRTY, flushedIds.size(),
                        "flusher must see exactly the dirty pages, attempt " + attempt);
                assertEquals((long) iterations * DIRTY, eligibleCalls.get(),
                        "eligible must be tested once per dirty page only");
                assertEquals(0, pool.getDirtyPageCount(), "all pages flushed, index empty");
            }
        }
        return best;
    }

    @Test
    @Timeout(60)
    void flushDirtyMatchingCostDependsOnDirtyCountNotFrameCount() throws Exception {
        int iterations = 5_000;

        // Pinned dirty pages are skipped even when they sit in the index.
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, page -> { }, null)) {
            List<Page> pages = fillClean(pool, 4);
            for (int i = 0; i < DIRTY; i++) {
                pages.get(i).setDirty(true);
            }
            try (PinnedPage pin = pool.pin(pageId(0))) {
                assertEquals(DIRTY - 1, pool.flushDirtyMatching(p -> true), "pinned page must be skipped");
                assertEquals(1, pool.getDirtyPageCount(), "pinned dirty page stays indexed");
            }
        }

        long bigMillis = timeFlushLoopMillis(FRAMES, iterations);
        long smallMillis = timeFlushLoopMillis(4, iterations);

        assertTrue(bigMillis <= 20 * Math.max(smallMillis, 1),
                "flush loop over " + FRAMES + " frames took " + bigMillis
                        + " ms vs " + smallMillis + " ms over 4 frames (best of 3, " + iterations
                        + " cycles each) - flush cost must not scale with the frame count (O(D), bound 20x)");
    }

    @Test
    @Timeout(30)
    void dirtyIndexFollowsLifecycleTransitions() throws Exception {
        // (a) Pre-dirty page arriving through insert() is indexed immediately.
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, page -> { }, null)) {
            Page page = new Page(pageId(1), PAGE_SIZE);
            page.setDirty(true);
            assertEquals(0, pool.getDirtyPageCount(), "standalone page must not count");
            try (PinnedPage pinned = pool.insert(page)) {
                assertEquals(1, pool.getDirtyPageCount(), "insert of a dirty page must index it");
            }
            assertEquals(1, pool.flushDirtyMatching(p -> true), "unpinned dirty page must flush");
            assertEquals(0, pool.getDirtyPageCount(), "flush must clear the index entry");
        }

        // (b) A failed dirty eviction restores the frame and keeps the index entry.
        AtomicBoolean failNextFlush = new AtomicBoolean(true);
        try (BufferPool pool = new BufferPool(1, PAGE_SIZE, page -> {
            if (failNextFlush.compareAndSet(true, false)) {
                throw new IOException("simulated flush failure");
            }
        }, null)) {
            Page a = new Page(pageId(1), PAGE_SIZE);
            try (PinnedPage pinned = pool.insert(a)) {
                // handle released here so the page becomes evictable
            }
            a.setDirty(true);
            assertEquals(1, pool.getDirtyPageCount());

            Page b = new Page(pageId(2), PAGE_SIZE);
            IOException thrown = assertThrows(IOException.class, () -> pool.insert(b).close());
            assertTrue(thrown.getMessage().contains("simulated flush failure"));
            assertEquals(1, pool.getDirtyPageCount(), "restored dirty frame must stay indexed");
            try (PinnedPage pinned = pool.pin(pageId(1))) {
                assertSame(a, pinned.getPage(), "victim frame must be restored");
            }
            assertEquals(1, pool.flushDirtyMatching(p -> true));
            assertEquals(0, pool.getDirtyPageCount());
        }

        // (c) close() detaches listeners and clears the index.
        BufferPool pool = new BufferPool(4, PAGE_SIZE, page -> { }, null);
        Page page = new Page(pageId(3), PAGE_SIZE);
        try (PinnedPage pinned = pool.insert(page)) {
            page.setDirty(true);
        }
        assertEquals(1, pool.getDirtyPageCount());
        pool.close();
        assertEquals(0, pool.getDirtyPageCount(), "close() must clear the dirty-page index");
        page.setDirty(false);
        page.setDirty(true);
        assertEquals(0, pool.getDirtyPageCount(), "detached page must not re-index a closed pool");
    }
}
