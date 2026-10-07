package diesel;

import diesel.storage.page.BufferPool;
import diesel.storage.page.Page;
import diesel.storage.page.PageFlusher;
import diesel.storage.page.PageId;
import diesel.storage.page.PageLoader;
import diesel.storage.page.PinnedPage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-threaded stress test for BufferPool (prompt4.md step 7): many threads
 * pin/unpin through a tiny pool so every operation races against eviction,
 * and markers must survive dirty write-back / reload round trips.
 * Page itself is not thread-safe (prompt 6 contract) — mutations happen only
 * under {@code synchronized(page)}; the pool lock guarantees frame identity.
 */
@Tag("storage")
class BufferPoolStressTest {

    private static final int PAGE_SIZE = 8192;
    private static final int CAPACITY = 32;
    private static final int LOGICAL_PAGES = 200;
    private static final int THREADS = 10;
    private static final int OPS_PER_THREAD = 100_000;

    /** Backing store emulating the file layer: logical pageId → serialized bytes. */
    private static final ConcurrentHashMap<PageId, byte[]> BACKING = new ConcurrentHashMap<>();

    private static PageId pageId(int n) {
        return new PageId(1, 1, n);
    }

    private static byte markerOf(int n) {
        return (byte) (n & 0x7F);
    }

    /** Serializes the page image (writeTo writes into, not returns, the buffer). */
    private static byte[] serialize(Page page) {
        ByteBuffer buf = ByteBuffer.allocate(page.getPageSize());
        page.writeTo(buf);
        return buf.array();
    }

    /**
     * In-memory store standing in for prompt 8's PageManager: dirty flush
     * serializes the full page image, a load deserializes it back.
     */
    private static final class MemoryStore implements PageFlusher, PageLoader {
        @Override
        public void flush(Page page) {
            BACKING.put(page.getPageId(), serialize(page));
        }

        @Override
        public Page load(PageId id) {
            byte[] raw = BACKING.get(id);
            if (raw == null) {
                return null;
            }
            Page page = Page.readFrom(ByteBuffer.wrap(raw));
            page.setDirty(false);
            return page;
        }
    }

    @Test
    void concurrentPinUnpinNeverLosesMarkers() throws Exception {
        BACKING.clear();
        // seed the backing store: one marker byte per logical page
        for (int n = 0; n < LOGICAL_PAGES; n++) {
            Page seed = new Page(pageId(n), PAGE_SIZE);
            seed.insert(new byte[] {markerOf(n)});
            seed.setDirty(false);
            BACKING.put(pageId(n), serialize(seed));
        }

        MemoryStore store = new MemoryStore();
        List<Thread> workers = new ArrayList<>();
        Queue<String> errors = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(THREADS);

        try (BufferPool pool = new BufferPool(CAPACITY, PAGE_SIZE, store, store)) {
            for (int t = 0; t < THREADS; t++) {
                final int tid = t;
                Thread thread = new Thread(() -> {
                    try {
                        for (int i = 0; i < OPS_PER_THREAD && !stop.get(); i++) {
                            int n = (i + tid * 37) % LOGICAL_PAGES;
                            PageId id = pageId(n);
                            try (PinnedPage pinned = pool.pin(id)) {
                                Page page = pinned.getPage();
                                synchronized (page) {
                                    byte[] tuple = page.get(0);
                                    if (tuple == null || tuple[0] != markerOf(n)) {
                                        errors.add("page " + id + " lost marker, got "
                                                + (tuple == null ? "null" : tuple[0]));
                                        stop.set(true);
                                        return;
                                    }
                                    tuple[0] = markerOf(n); // touch without changing value
                                    page.setDirty(true);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.add("thread " + tid + ": " + e);
                        stop.set(true);
                    } finally {
                        done.countDown();
                    }
                });
                workers.add(thread);
                thread.start();
            }

            assertTrue(done.await(120, java.util.concurrent.TimeUnit.SECONDS),
                    "workers must finish within the timeout");
            for (Thread thread : workers) {
                thread.join(5_000);
            }

            assertTrue(errors.isEmpty(), "no worker errors: " + errors);

            long requests = (long) THREADS * OPS_PER_THREAD;
            assertEquals(requests, pool.getHits() + pool.getMisses(),
                    "every request is either a hit or a miss");
            assertTrue(pool.getEvictions() > 0,
                    "pool smaller than working set must evict under pressure");
            assertEquals(0, pool.getPinnedPages(), "all handles released");
            assertEquals(CAPACITY, pool.getResidentPages(),
                    "pool stays full after the churn");

            // final consistency sweep over every logical page
            for (int n = 0; n < LOGICAL_PAGES; n++) {
                try (PinnedPage pinned = pool.pin(pageId(n))) {
                    byte[] tuple = pinned.getPage().get(0);
                    assertNotNull(tuple, "page " + n + " must have a tuple");
                    assertEquals(markerOf(n), tuple[0], "page " + n + " marker");
                }
            }
        }
    }

    @Test
    void samePagePinnedFromManyThreadsServesHitsWithoutDoubleLoad() throws Exception {
        BACKING.clear();
        PageId hot = pageId(7);
        Page seed = new Page(hot, PAGE_SIZE);
        seed.insert(new byte[] {42});
        seed.setDirty(false);
        BACKING.put(hot, serialize(seed));

        MemoryStore store = new MemoryStore();
        final int threads = 8;
        final int pinsPerThread = 10_000;
        Queue<String> errors = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(threads);

        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, store, store)) {
            for (int t = 0; t < threads; t++) {
                Thread thread = new Thread(() -> {
                    try {
                        for (int i = 0; i < pinsPerThread; i++) {
                            try (PinnedPage pinned = pool.pin(hot)) {
                                byte[] tuple = pinned.getPage().get(0);
                                if (tuple == null || tuple[0] != 42) {
                                    errors.add("marker lost: "
                                            + (tuple == null ? "null" : tuple[0]));
                                    return;
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(String.valueOf(e));
                    } finally {
                        done.countDown();
                    }
                });
                thread.start();
            }

            assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS),
                    "workers must finish within the timeout");
            assertTrue(errors.isEmpty(), "no worker errors: " + errors);

            long requests = (long) threads * pinsPerThread;
            assertEquals(requests, pool.getHits() + pool.getMisses());
            assertEquals(requests - 1, pool.getHits(),
                    "first request misses, all later ones must hit");
            assertEquals(1, pool.getMisses(), "page loaded exactly once");
            assertEquals(0, pool.getPinnedPages());
            assertEquals(1, pool.getResidentPages());
        }
    }
}
