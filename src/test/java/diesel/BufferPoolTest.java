package diesel;

import diesel.storage.page.BufferPool;
import diesel.storage.page.BufferPoolFullException;
import diesel.storage.page.Page;
import diesel.storage.page.PageFlusher;
import diesel.storage.page.PageFormatException;
import diesel.storage.page.PageId;
import diesel.storage.page.PageLoader;
import diesel.storage.page.PinnedPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BufferPool (prompt4.md step 7, R3-002 step 2/5): pin/unpin,
 * LRU eviction with pinned frames, dirty-flush callback, configuration and the
 * BufferPoolMXBean JMX view. All operations are in-memory; no file I/O in
 * this layer (prompt 8 wires FileChannel through PageLoader/PageFlusher).
 */
@Tag("storage")
class BufferPoolTest {

    private static final int PAGE_SIZE = 8192;
    private static final PageId ID_A = new PageId(1, 1, 1);
    private static final PageId ID_B = new PageId(1, 1, 2);
    private static final PageId ID_C = new PageId(1, 1, 3);
    private static final PageId ID_D = new PageId(1, 1, 4);

    private String prevSizeMb;

    @BeforeEach
    void saveSizeMb() {
        prevSizeMb = System.getProperty(BufferPool.SIZE_MB_KEY);
    }

    @AfterEach
    void restoreSizeMb() {
        if (prevSizeMb == null) {
            System.clearProperty(BufferPool.SIZE_MB_KEY);
        } else {
            System.setProperty(BufferPool.SIZE_MB_KEY, prevSizeMb);
        }
    }

    private static byte[] marker(int seed) {
        byte[] bytes = new byte[40];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    private static Page pageWith(PageId id, byte[] tuple) {
        Page page = new Page(id, PAGE_SIZE);
        page.insert(tuple);
        return page;
    }

    /** No-op flusher for tests that only need eviction bookkeeping. */
    private static PageFlusher noopFlusher() {
        return page -> { };
    }

    // ─── Pin / unpin basics ─────────────────────────────────────────

    @Test
    void insertThenPinRoundTripKeepsContentAndCountsHit() throws Exception {
        byte[] tuple = marker(1);
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, noopFlusher(), null)) {
            Page page = pageWith(ID_A, tuple);
            try (PinnedPage inserted = pool.insert(page)) {
                assertSame(page, inserted.getPage());
                assertEquals(1, pool.getPinnedPages());
            }
            assertEquals(0, pool.getPinnedPages(), "close() must unpin");

            try (PinnedPage pinned = pool.pin(ID_A)) {
                assertArrayEquals(tuple, pinned.getPage().get(0));
                assertEquals(1, pool.getHits());
                assertEquals(0, pool.getMisses());
            }
            assertEquals(1, pool.getResidentPages());
        }
    }

    @Test
    void pinOfMissingPageInvokesLoaderOnceThenServesResident() throws Exception {
        byte[] tuple = marker(2);
        AtomicInteger loads = new AtomicInteger();
        PageLoader loader = id -> {
            loads.incrementAndGet();
            Page page = new Page(id, PAGE_SIZE);
            page.insert(tuple);
            page.setDirty(false); // simulates "loaded from disk": content present, clean
            return page;
        };
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, noopFlusher(), loader)) {
            try (PinnedPage first = pool.pin(ID_A)) {
                assertArrayEquals(tuple, first.getPage().get(0));
                assertEquals(1, pool.getMisses());
                assertEquals(0, pool.getHits());
            }
            try (PinnedPage second = pool.pin(ID_A)) {
                assertArrayEquals(tuple, second.getPage().get(0));
                assertEquals(1, pool.getHits());
            }
            assertEquals(1, loads.get(), "loader must run exactly once");
        }
    }

    @Test
    void pinOfMissingPageWithoutLoaderThrowsAndCountsMiss() throws Exception {
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE)) {
            IOException ex = assertThrows(IOException.class, () -> pool.pin(ID_A));
            assertTrue(ex.getMessage().contains("no PageLoader"), ex.getMessage());
            assertEquals(1, pool.getMisses());
            assertEquals(0, pool.getResidentPages());
        }
    }

    @Test
    void rawUnpinOfUnpinnedOrUnknownPageThrowsIllegalState() throws Exception {
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), null)) {
            pool.insert(pageWith(ID_A, marker(3))).close();

            IllegalStateException doubleUnpin =
                    assertThrows(IllegalStateException.class, () -> pool.unpin(ID_A));
            assertTrue(doubleUnpin.getMessage().contains("double unpin"), doubleUnpin.getMessage());

            assertThrows(IllegalStateException.class, () -> pool.unpin(ID_B));

            assertThrows(IllegalArgumentException.class, () -> pool.unpin(null));
            assertThrows(IllegalArgumentException.class, () -> pool.pin(null));
        }
    }

    @Test
    void pinnedPageCloseIsIdempotentAndBlocksGetPageAfterwards() throws Exception {
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), null)) {
            PinnedPage handle = pool.insert(pageWith(ID_A, marker(4)));
            Page page = handle.getPage();
            handle.close();
            handle.close(); // second close is a no-op, not a double unpin
            assertTrue(handle.isClosed());
            assertEquals(0, pool.getPinnedPages());
            assertThrows(IllegalStateException.class, handle::getPage);

            // frame is evictable again
            pool.insert(pageWith(ID_B, marker(5)));
            pool.insert(pageWith(ID_C, marker(6)));
            assertEquals(1, pool.getEvictions(), "A unpinned, must be the LRU victim");
            assertNotNull(page);
        }
    }

    @Test
    void insertOfResidentIdReturnsExistingFrameInstance() throws Exception {
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, noopFlusher(), null)) {
            Page first = pageWith(ID_A, marker(7));
            Page rival = pageWith(ID_A, marker(8)); // same id, different object
            try (PinnedPage h1 = pool.insert(first);
                 PinnedPage h2 = pool.insert(rival)) {
                assertSame(first, h1.getPage(), "frame must keep the first instance");
                assertSame(first, h2.getPage(), "resident insert pins the existing frame");
                assertEquals(1, pool.getResidentPages());
                assertEquals(1, pool.getPinnedPages(), "one distinct pinned frame");
            }
            assertEquals(0, pool.getPinnedPages());
        }
    }

    // ─── LRU eviction ───────────────────────────────────────────────

    @Test
    void lruEvictsLeastRecentlyUsedFrameNotInsertionOrder() throws Exception {
        List<Page> flushed = new ArrayList<>();
        AtomicInteger bLoads = new AtomicInteger();
        PageLoader bLoader = id -> {
            if (ID_B.equals(id)) {
                bLoads.incrementAndGet();
            }
            return new Page(id, PAGE_SIZE);
        };
        try (BufferPool pool = new BufferPool(3, PAGE_SIZE, flushed::add, bLoader)) {
            pool.insert(pageWith(ID_A, marker(10))).close();
            pool.insert(pageWith(ID_B, marker(11))).close();
            pool.insert(pageWith(ID_C, marker(12))).close();

            // touch A: recency order becomes B(LRU), C, A(MRU)
            pool.pin(ID_A).close();

            pool.insert(pageWith(ID_D, marker(13))).close();
            assertEquals(1, pool.getEvictions(), "only B may be evicted");
            assertEquals(1, flushed.size());
            assertEquals(ID_B, flushed.get(0).getPageId(), "B is the LRU victim");

            // C and A stayed resident (hits), B must be reloaded (miss)
            pool.pin(ID_C).close();
            pool.pin(ID_A).close();
            try (PinnedPage b = pool.pin(ID_B)) {
                assertNotNull(b.getPage());
            }
            assertEquals(3, pool.getHits(), "A touch, C and A re-pin were hits");
            assertEquals(1, pool.getMisses(), "B was the miss");
            assertEquals(1, bLoads.get());
        }
    }

    @Test
    void pinnedFramesAreSkippedByEvictionSelection() throws Exception {
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), null)) {
            PinnedPage pinnedA = pool.insert(pageWith(ID_A, marker(20)));
            pool.insert(pageWith(ID_B, marker(21))).close();

            // capacity reached and A is pinned: only B can go
            pool.insert(pageWith(ID_C, marker(22))).close();
            assertEquals(1, pool.getEvictions());

            try (PinnedPage a = pool.pin(ID_A)) {
                assertSame(pinnedA.getPage(), a.getPage(), "pinned A must survive eviction pressure");
                assertEquals(1, pool.getHits());
            }
            pinnedA.close();
        }
    }

    @Test
    void allPinnedThrowsBufferPoolFullAndKeepsFrames() throws Exception {
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), null)) {
            PinnedPage a = pool.insert(pageWith(ID_A, marker(30)));
            PinnedPage b = pool.insert(pageWith(ID_B, marker(31)));
            try {
                assertThrows(BufferPoolFullException.class,
                        () -> pool.insert(pageWith(ID_C, marker(32))));
                assertEquals(2, pool.getResidentPages(), "failed insert must not leave a frame");
                assertEquals(0, pool.getEvictions(), "nothing evictable, nothing evicted");
            } finally {
                a.close();
                b.close();
            }
        }
    }

    // ─── Dirty vs clean eviction ────────────────────────────────────

    @Test
    void dirtyEvictionWritesThroughFlusherWithContent() throws Exception {
        byte[] tuple = marker(40);
        List<Page> flushed = new ArrayList<>();
        try (BufferPool pool = new BufferPool(1, PAGE_SIZE, flushed::add, null)) {
            pool.insert(pageWith(ID_A, tuple)).close();
            pool.insert(pageWith(ID_B, marker(41))).close();

            assertEquals(1, pool.getEvictions());
            assertEquals(1, flushed.size());
            Page victim = flushed.get(0);
            assertEquals(ID_A, victim.getPageId());
            assertArrayEquals(tuple, victim.get(0), "flushed image must carry the data");
        }
    }

    @Test
    void cleanEvictionDiscardsWithoutCallingFlusher() throws Exception {
        List<Page> flushed = new ArrayList<>();
        try (BufferPool pool = new BufferPool(1, PAGE_SIZE, flushed::add, null)) {
            Page clean = pageWith(ID_A, marker(50));
            clean.setDirty(false); // loaded pages are clean
            pool.insert(clean).close();
            pool.insert(pageWith(ID_B, marker(51))).close();

            assertEquals(1, pool.getEvictions());
            assertTrue(flushed.isEmpty(), "clean page must be discarded without flush");
        }
    }

    @Test
    void dirtyEvictionWithoutFlusherThrowsAndRestoresFrame() throws Exception {
        try (BufferPool pool = new BufferPool(1, PAGE_SIZE)) {
            Page dirty = pageWith(ID_A, marker(60));
            pool.insert(dirty).close();

            IOException ex = assertThrows(IOException.class,
                    () -> pool.insert(pageWith(ID_B, marker(61))));
            assertTrue(ex.getMessage().contains("no PageFlusher"), ex.getMessage());

            assertEquals(0, pool.getEvictions(), "failed flush must not count as eviction");
            try (PinnedPage a = pool.pin(ID_A)) {
                assertSame(dirty, a.getPage(), "victim frame must be restored");
                assertEquals(1, pool.getHits());
            }
            dirty.setDirty(false); // let close() pass without a flusher
        }
    }

    @Test
    void flushDirtyPersistsAllDirtyPagesOnce() throws Exception {
        List<Page> flushed = new ArrayList<>();
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, flushed::add, null)) {
            pool.insert(pageWith(ID_A, marker(70))).close();
            pool.insert(pageWith(ID_B, marker(71))).close();
            Page clean = pageWith(ID_C, marker(72));
            clean.setDirty(false);
            pool.insert(clean).close();

            pool.flushDirty();
            assertEquals(2, flushed.size(), "only dirty A and B flushed");

            pool.flushDirty();
            assertEquals(2, flushed.size(), "already clean pages are not flushed twice");
        }
    }

    // ─── Configuration ──────────────────────────────────────────────

    @Test
    void capacityIsResolvedFromConfigSizeMb() {
        System.setProperty(BufferPool.SIZE_MB_KEY, "1"); // 1 MB / 8 KB = 128 frames
        try (BufferPool pool = new BufferPool()) {
            assertEquals(128, pool.getCapacityPages());
            assertEquals(PAGE_SIZE, pool.getPageSize());
        } catch (IOException e) {
            fail("close() must not fail on an empty pool: " + e);
        }
    }

    @Test
    void defaultConfigYields256MbSizedPool() {
        System.clearProperty(BufferPool.SIZE_MB_KEY);
        try (BufferPool pool = new BufferPool()) {
            assertEquals(BufferPool.DEFAULT_SIZE_MB * 1024 * 1024 / PAGE_SIZE,
                    pool.getCapacityPages(), "256 MB / 8 KB = 32768 frames");
        } catch (IOException e) {
            fail("close() must not fail on an empty pool: " + e);
        }
    }

    @Test
    void invalidCapacityAndPageSizeAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(0, PAGE_SIZE));
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(-1, PAGE_SIZE));
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(4, 100));
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(4, 0));
    }

    @Test
    void loaderReturningWrongPageIsRejected() {
        PageLoader wrongId = id -> new Page(new PageId(id.tablespaceId(), id.fileId(), id.pageNum() + 1), PAGE_SIZE);
        PageLoader wrongSize = id -> new Page(id, 16384);
        PageLoader nullPage = id -> null;
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), wrongId)) {
            assertThrows(PageFormatException.class, () -> pool.pin(ID_A));
        } catch (IOException e) {
            fail("close() must not fail: " + e);
        }
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), wrongSize)) {
            assertThrows(PageFormatException.class, () -> pool.pin(ID_A));
        } catch (IOException e) {
            fail("close() must not fail: " + e);
        }
        try (BufferPool pool = new BufferPool(2, PAGE_SIZE, noopFlusher(), nullPage)) {
            IOException ex = assertThrows(IOException.class, () -> pool.pin(ID_A));
            assertTrue(ex.getMessage().contains("returned null"), ex.getMessage());
        } catch (IOException e) {
            fail("close() must not fail: " + e);
        }
    }

    // ─── Lifecycle + JMX ────────────────────────────────────────────

    @Test
    void hitRateReflectsHitAndMissCounters() throws Exception {
        PageLoader loader = id -> new Page(id, PAGE_SIZE);
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, noopFlusher(), loader)) {
            assertEquals(0.0, pool.getHitRate(), 1e-9, "no requests yet");

            pool.pin(ID_A).close(); // miss
            pool.pin(ID_A).close(); // hit
            pool.pin(ID_A).close(); // hit

            assertEquals(2.0 / 3.0, pool.getHitRate(), 1e-9);
        }
    }

    @Test
    void mxBeanExposesCountersAndUnregistersOnClose() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        PageLoader loader = id -> new Page(id, PAGE_SIZE);
        ObjectName name;
        try (BufferPool pool = new BufferPool(4, PAGE_SIZE, noopFlusher(), loader)) {
            pool.pin(ID_A).close(); // miss
            pool.pin(ID_A).close(); // hit

            name = pool.getObjectName();
            assertNotNull(name, "MBean must register on construction");
            assertEquals(1L, server.getAttribute(name, "Hits"));
            assertEquals(1L, server.getAttribute(name, "Misses"));
            assertEquals(1, (int) server.getAttribute(name, "ResidentPages"));
            assertEquals(4, (int) server.getAttribute(name, "CapacityPages"));
            assertEquals(0, (int) server.getAttribute(name, "PinnedPages"));
            assertEquals(0L, server.getAttribute(name, "Evictions"));
            assertEquals(0.5, (double) server.getAttribute(name, "HitRate"), 1e-9);
        }

        assertTrue(server.queryNames(name, null).isEmpty(), "MBean must unregister on close");
        assertThrows(InstanceNotFoundException.class, () -> server.getObjectInstance(name));
    }

    @Test
    void closeFlushesDirtiesRejectsFurtherPinsAndIsIdempotent() throws Exception {
        byte[] tuple = marker(80);
        List<Page> flushed = new ArrayList<>();
        BufferPool pool = new BufferPool(2, PAGE_SIZE, flushed::add, null);
        pool.insert(pageWith(ID_A, tuple)).close();

        pool.close();
        assertEquals(1, flushed.size(), "close() must flush dirty frames");
        assertArrayEquals(tuple, flushed.get(0).get(0));
        assertNull(pool.getObjectName());

        assertThrows(IllegalStateException.class, () -> pool.pin(ID_A));
        pool.close(); // idempotent

        // a pin handle that outlives the pool closes silently (no ISE)
        BufferPool latePool = new BufferPool(2, PAGE_SIZE, noopFlusher(), null);
        PinnedPage lateHandle = latePool.insert(pageWith(ID_B, marker(81)));
        latePool.close();
        assertDoesNotThrow(lateHandle::close);
    }
}
