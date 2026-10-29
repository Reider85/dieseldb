package diesel.storage.page;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.observability.MetricsRegistry;

/**
 * Fixed-capacity pool of resident pages with LRU eviction and pinned frames
 * (prompt4.md step 7, ROADMAP3 R3-002).
 *
 * <p><b>Sizing.</b> {@code bufferpool.size.mb} (system property first, then
 * {@code config.properties}, default {@value #DEFAULT_SIZE_MB} MB) divided by
 * the page size gives the frame count; explicit constructors take the frame
 * count directly (tests).
 *
 * <p><b>Pin/unpin.</b> {@link #pin(PageId)} returns a {@link PinnedPage}
 * handle whose {@link PinnedPage#close()} unpins; {@link #unpin(PageId)} is
 * the raw counterpart (double unpin throws {@link IllegalStateException}).
 * Pinned frames are never chosen as eviction victims.
 *
 * <p><b>Eviction.</b> When a new frame would exceed capacity, the least
 * recently released <em>unpinned</em> frame is selected in O(1)
 * ({@link LruEvictionPolicy} keeps an unpinned-only queue, o2 Buffer-5): a
 * dirty page is written through the {@link PageFlusher} (on failure the
 * frame is restored and the error propagates), a clean page is discarded.
 * When every frame is pinned a {@link BufferPoolFullException} is thrown.
 *
 * <p><b>Miss path.</b> A pin of a non-resident page loads it via
 * {@link PageLoader} (per-call loader or the pool default). Loader and flusher
 * callbacks run while the pool lock is held — a deliberate prompt-7
 * simplification; prompt 8 ({@code PageManager}) wires short FileChannel
 * operations and prompt 20 (R3-005) moves slow flushing to a background
 * thread.
 *
 * <p><b>Threading.</b> The pool itself is thread-safe (single
 * {@link ReentrantLock} guarding frames, recency order and pin counts;
 * counters are lock-free {@link AtomicLong} reads for JMX). The {@link Page}
 * objects are <em>not</em> thread-safe (prompt 6): a pinned page may only be
 * touched by the pinning thread until it unpins, and concurrent writers to
 * one page must serialise externally (monitor on the {@link Page}).
 *
 * <p><b>JMX.</b> Implements {@link BufferPoolMXBean}, registered on the
 * platform MBean server as {@code diesel:type=BufferPool,id=N} on construction
 * and unregistered by {@link #close()}.
 */
public final class BufferPool implements BufferPoolMXBean, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferPool.class);

    /** Configuration key: pool size in megabytes. */
    public static final String SIZE_MB_KEY = "bufferpool.size.mb";
    /** Default pool size (256 MB). */
    public static final int DEFAULT_SIZE_MB = 256;

    static final String OBJECT_NAME_PREFIX = "diesel:type=BufferPool";
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final int capacityPages;
    private final int pageSize;
    private final PageFlusher flusher;
    private final PageLoader defaultLoader;

    /** Guards frames, lru and pinnedPages; also serialises loader/flusher callbacks. */
    private final ReentrantLock lock = new ReentrantLock();
    private final HashMap<PageId, BufferFrame> frames = new HashMap<>();
    private final LruEvictionPolicy lru = new LruEvictionPolicy();

    /**
     * Guards {@link #dirtyPageIds} only. Deliberately separate from
     * {@link #lock} so dirty transitions reported from pinning threads never
     * contend with frame-structure updates. Lock order: {@code lock} then
     * {@code dirtyLock}, never the reverse; the dirty-transition callback
     * takes {@code dirtyLock} alone.
     */
    private final Object dirtyLock = new Object();
    /**
     * Insertion-ordered index of pages whose dirty flag is set (o2 Buffer-4).
     * Maintained via {@link Page}'s dirty listener on frame creation and on
     * every dirty-flag transition; {@code LinkedHashSet} keeps flush order
     * deterministic (FIFO by first-dirty time).
     */
    private final LinkedHashSet<PageId> dirtyPageIds = new LinkedHashSet<>();

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    /** Distinct frames with pin count > 0; guarded by {@link #lock}. */
    private int pinnedPages;

    private volatile boolean closed;
    private volatile ObjectName registeredName;

    /** Per-frame pin accounting. */
    private static final class BufferFrame {
        private final Page page;
        private int pinCount;

        BufferFrame(Page page) {
            this.page = page;
        }
    }

    /**
     * Creates a pool sized from configuration: page size from
     * {@code page.size}, frame count from {@code bufferpool.size.mb}
     * (defaults, no loader/flusher — wire them via the explicit constructor
     * or per-call {@link #pin(PageId, PageLoader)}).
     */
    public BufferPool() {
        this(resolveCapacityPages(), PageConfig.getPageSize(), null, null);
    }

    /**
     * Creates a pool with an explicit frame count and page size (tests),
     * without loader/flusher.
     *
     * @param capacityPages maximum resident frames (&gt; 0)
     * @param pageSize      page size in bytes (8K / 16K / 64K)
     */
    public BufferPool(int capacityPages, int pageSize) {
        this(capacityPages, pageSize, null, null);
    }

    /**
     * Creates a pool with explicit sizing and I/O callbacks.
     *
     * @param capacityPages maximum resident frames (&gt; 0)
     * @param pageSize      page size in bytes (must be an allowed page size)
     * @param flusher       persists dirty pages on eviction/flush; {@code null}
     *                      means the pool is memory-only and dirty eviction or
     *                      {@link #flushDirty()} fails with {@link IOException}
     * @param loader        loads non-resident pages on pin misses;
     *                      {@code null} means pinning a missing page fails
     *                      with {@link IOException} (only {@link #insert(Page)}
     *                      populates such a pool)
     * @throws IllegalArgumentException on non-positive capacity or invalid
     *                                  page size
     */
    public BufferPool(int capacityPages, int pageSize, PageFlusher flusher, PageLoader loader) {
        if (capacityPages <= 0) {
            throw new IllegalArgumentException("buffer pool capacity must be positive: " + capacityPages);
        }
        if (!Page.isAllowedSize(pageSize)) {
            throw new IllegalArgumentException("Invalid page size: " + pageSize
                    + " (allowed: " + java.util.Arrays.toString(PageConfig.ALLOWED_SIZES) + ")");
        }
        this.capacityPages = capacityPages;
        this.pageSize = pageSize;
        this.flusher = flusher;
        this.defaultLoader = loader;
        registerMBean();
        // Prompt4 #44: hit-ratio gauge is supplier-backed by the existing
        // AtomicLong counters; the most recently created pool wins the
        // JVM-wide registration (tests create many pools per JVM).
        MetricsRegistry.getInstance()
                .registerGauge(MetricsRegistry.BUFFER_POOL_HIT_RATIO, this::getHitRate);
    }

    // ─── Pin / unpin ────────────────────────────────────────────────

    /**
     * Pins a page using the pool's default loader on a miss.
     *
     * @param pageId page to pin
     * @return an open pin handle
     * @throws IOException           when the page is not resident, no default
     *                               loader is configured, loading fails, or
     *                               no unpinned frame can be evicted
     * @throws IllegalStateException when the pool is closed
     */
    public PinnedPage pin(PageId pageId) throws IOException {
        return pin(pageId, defaultLoader);
    }

    /**
     * Pins a page, loading it through {@code loader} on a miss.
     *
     * @param pageId page to pin
     * @param loader loader used when the page is not resident; {@code null}
     *               requires residency
     * @return an open pin handle (increments the frame's pin count and marks
     *         the page most recently used)
     * @throws IOException           loading/eviction failure, or miss without
     *                               a loader
     * @throws IllegalStateException when the pool is closed
     */
    public PinnedPage pin(PageId pageId, PageLoader loader) throws IOException {
        if (pageId == null) {
            throw new IllegalArgumentException("pageId must not be null");
        }
        lock.lock();
        try {
            checkOpen();
            BufferFrame frame = frames.get(pageId);
            if (frame != null) {
                hits.incrementAndGet();
                pinFrameLocked(pageId, frame);
                return new PinnedPage(this, pageId, frame.page);
            }
            misses.incrementAndGet();
            if (loader == null) {
                throw new IOException("Page " + pageId + " is not resident and no PageLoader is configured");
            }
            evictForRoomLocked();
            Page page = loader.load(pageId);
            validateLoadedPage(pageId, page);
            frame = new BufferFrame(page);
            frames.put(pageId, frame);
            attachPageLocked(pageId, page);
            pinFrameLocked(pageId, frame);
            return new PinnedPage(this, pageId, page);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Makes a freshly created page resident and pins it for the caller
     * (page-allocation path of prompt 8's {@code PageManager}). When the id is
     * already resident, the existing frame is pinned instead of the argument.
     *
     * @param page the new page to host (must not be {@code null})
     * @return an open pin handle to the resident frame
     * @throws IllegalStateException when the pool is closed
     * @throws IOException           when no unpinned frame can be evicted
     */
    public PinnedPage insert(Page page) throws IOException {
        if (page == null) {
            throw new IllegalArgumentException("page must not be null");
        }
        lock.lock();
        try {
            checkOpen();
            PageId pageId = page.getPageId();
            BufferFrame frame = frames.get(pageId);
            if (frame == null) {
                evictForRoomLocked();
                frame = new BufferFrame(page);
                frames.put(pageId, frame);
                attachPageLocked(pageId, page);
            }
            pinFrameLocked(pageId, frame);
            return new PinnedPage(this, pageId, frame.page);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Raw unpin counterpart of {@link #pin(PageId)} (the handle returned by
     * pin already unpins on {@link PinnedPage#close()}).
     *
     * @param pageId page to unpin
     * @throws IllegalStateException when the pool is closed (no-op instead,
     *                               see below), the page is not resident, or
     *                               its pin count is already zero (double unpin)
     */
    public void unpin(PageId pageId) {
        if (pageId == null) {
            throw new IllegalArgumentException("pageId must not be null");
        }
        lock.lock();
        try {
            if (closed) {
                // close() released every frame; a late handle closing after the
                // pool shut down must not throw (try-with-resources ordering).
                return;
            }
            BufferFrame frame = frames.get(pageId);
            if (frame == null) {
                throw new IllegalStateException("Unpin of non-resident page " + pageId);
            }
            if (frame.pinCount <= 0) {
                throw new IllegalStateException("Page " + pageId + " is not pinned (double unpin)");
            }
            if (--frame.pinCount == 0) {
                pinnedPages--;
                lru.onUnpinned(pageId);
            }
        } finally {
            lock.unlock();
        }
    }

    // ─── Flush / close ──────────────────────────────────────────────

    /**
     * Writes every dirty resident page through the {@link PageFlusher} and
     * marks them clean. Callers must quiesce concurrent page modification
     * first (checkpoint semantics); pinned pages are included.
     *
     * @throws IOException the first flush failure (later ones are attached
     *                     as suppressed); also thrown when a page is dirty
     *                     but no flusher is configured
     */
    public void flushDirty() throws IOException {
        IOException failure = flushResidentDirty();
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Flushes all dirty pages, drops every frame and unregisters the MBean.
     * Idempotent. A late {@link PinnedPage#close()} after shutdown is a
     * no-op, so try-with-resources nesting is safe.
     *
     * @throws IOException flush failure while shutting down (frames are
     *                     released regardless)
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            lock.unlock();
        }

        IOException failure;
        try {
            failure = flushResidentDirty();
        } finally {
            lock.lock();
            try {
                releaseAllFramesLocked();
            } finally {
                lock.unlock();
            }
            unregisterMBean();
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Simulates a crash: drops every frame <em>without</em> flushing dirty
     * pages and unregisters the MBean. Idempotent. Used by crash-recovery
     * tests to verify that unflushed data is lost.
     */
    void abandon() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            releaseAllFramesLocked();
        } finally {
            lock.unlock();
        }
        unregisterMBean();
    }

    // ─── Counters / MXBean view ─────────────────────────────────────

    @Override
    public long getHits() {
        return hits.get();
    }

    @Override
    public long getMisses() {
        return misses.get();
    }

    @Override
    public long getEvictions() {
        return evictions.get();
    }

    @Override
    public int getResidentPages() {
        lock.lock();
        try {
            return frames.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getCapacityPages() {
        return capacityPages;
    }

    @Override
    public int getPinnedPages() {
        lock.lock();
        try {
            return pinnedPages;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public double getHitRate() {
        long total = hits.get() + misses.get();
        return total == 0 ? 0.0 : (double) hits.get() / total;
    }

    /**
     * Returns the configured page size of this pool's frames.
     *
     * @return page size in bytes
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns the JMX object name this pool is registered under.
     *
     * @return the registered object name, or {@code null} if registration
     *         failed or the pool is already closed
     */
    public ObjectName getObjectName() {
        return registeredName;
    }

    /**
     * Returns the current number of dirty pages in the pool (o2 Buffer-4).
     * Backed by the dirty-page index, so the cost is O(1) regardless of how
     * many clean frames are resident.
     *
     * @return number of dirty pages
     */
    public int getDirtyPageCount() {
        synchronized (dirtyLock) {
            return dirtyPageIds.size();
        }
    }

    /**
     * Flushes dirty pages matching the given predicate (o2 Buffer-4).
     * Iterates the dirty-page index — O(D) where D is the number of dirty
     * pages — instead of every resident frame. Snapshots matching dirty
     * unpinned pages under the pool lock, then flushes them outside the lock.
     * Returns the count of pages that were flushed.
     * Skips pinned pages to avoid torn writes.
     *
     * @param eligible predicate to determine if a dirty page should be flushed
     * @return number of pages flushed
     * @throws IOException if the first flush fails (later failures are suppressed)
     */
    public int flushDirtyMatching(Predicate<Page> eligible) throws IOException {
        List<Page> dirty = snapshotResidentDirty(true, eligible);

        IOException failure = null;
        int flushedCount = 0;
        for (Page page : dirty) {
            try {
                persistDirty(page);
                flushedCount++;
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        return flushedCount;
    }

    // ─── Internals ──────────────────────────────────────────────────

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("BufferPool is closed");
        }
    }

    private void pinFrameLocked(PageId pageId, BufferFrame frame) {
        if (frame.pinCount++ == 0) {
            pinnedPages++;
            lru.onPinned(pageId);
        }
    }

    private void validateLoadedPage(PageId requested, Page page) throws IOException {
        if (page == null) {
            throw new IOException("PageLoader returned null for page " + requested);
        }
        if (!requested.equals(page.getPageId())) {
            throw new PageFormatException("PageLoader returned page " + page.getPageId()
                    + " for requested " + requested);
        }
        if (page.getPageSize() != pageSize) {
            throw new PageFormatException("PageLoader returned page " + requested + " with size "
                    + page.getPageSize() + ", pool expects " + pageSize);
        }
    }

    /**
     * Evicts unpinned LRU frames until one more frame fits. Runs under the
     * pool lock; a failed dirty flush restores the victim frame and
     * propagates.
     */
    private void evictForRoomLocked() throws IOException {
        while (frames.size() >= capacityPages) {
            PageId victim = lru.pollEvictable(id -> {
                BufferFrame frame = frames.get(id);
                return frame != null && frame.pinCount == 0;
            });
            if (victim == null) {
                throw new BufferPoolFullException("All " + capacityPages
                        + " frames are pinned; cannot make room for a new page");
            }
            BufferFrame frame = frames.remove(victim);
            try {
                if (frame.page.isDirty()) {
                    persistDirty(frame.page);
                }
            } catch (IOException | RuntimeException e) {
                frames.put(victim, frame);
                // o2 Buffer-5: the victim left the queue in pollEvictable;
                // re-register it (still unpinned) or it becomes unevictable.
                lru.onUnpinned(victim);
                throw e;
            }
            evictions.incrementAndGet();
        }
    }

    private void persistDirty(Page page) throws IOException {
        if (flusher == null) {
            throw new IOException("Cannot flush dirty page " + page.getPageId()
                    + ": no PageFlusher configured");
        }
        flusher.flush(page);
        page.setDirty(false);
    }

    /**
     * Snapshots the dirty resident pages (o2 Buffer-4: iterates the
     * dirty-page index, O(D)), then flushes them outside the pool lock.
     *
     * @return the first failure, or {@code null} when everything persisted
     */
    private IOException flushResidentDirty() {
        List<Page> dirty = snapshotResidentDirty(false, null);

        IOException failure = null;
        for (Page page : dirty) {
            try {
                persistDirty(page);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        return failure;
    }

    /**
     * Collects the pages referenced by the dirty-page index that are still
     * resident (o2 Buffer-4). Ids can be transiently present for a page
     * already removed from {@code frames} mid-eviction — such entries are
     * skipped here and disappear once the flush completes.
     *
     * @param unpinnedOnly when true, skip frames with {@code pinCount > 0}
     * @param eligible     extra predicate tested under the pool lock; {@code null}
     *                     accepts everything
     * @return matching dirty pages, in first-dirty (FIFO) order
     */
    private List<Page> snapshotResidentDirty(boolean unpinnedOnly, Predicate<Page> eligible) {
        List<PageId> dirtyIds;
        synchronized (dirtyLock) {
            dirtyIds = new ArrayList<>(dirtyPageIds);
        }
        List<Page> dirty = new ArrayList<>(dirtyIds.size());
        lock.lock();
        try {
            for (PageId id : dirtyIds) {
                BufferFrame frame = frames.get(id);
                if (frame == null || !frame.page.isDirty()) {
                    continue;
                }
                if (unpinnedOnly && frame.pinCount != 0) {
                    continue;
                }
                if (eligible != null && !eligible.test(frame.page)) {
                    continue;
                }
                dirty.add(frame.page);
            }
        } finally {
            lock.unlock();
        }
        return dirty;
    }

    /**
     * Wires a freshly created frame's page into the dirty-page index
     * (o2 Buffer-4): attaches the transition listener and registers the
     * page when it arrives already dirty. Runs under the pool lock, so the
     * nested {@code dirtyLock} acquisition keeps the lock order.
     */
    private void attachPageLocked(PageId pageId, Page page) {
        page.setDirtyListener(this::onPageDirtyChanged);
        if (page.isDirty()) {
            synchronized (dirtyLock) {
                dirtyPageIds.add(pageId);
            }
        }
    }

    /**
     * Dirty-flag transition sink (o2 Buffer-4). Invoked by pinning threads
     * without the pool lock; takes {@code dirtyLock} alone.
     */
    void onPageDirtyChanged(PageId pageId, boolean dirty) {
        if (closed) {
            return;
        }
        synchronized (dirtyLock) {
            if (dirty) {
                dirtyPageIds.add(pageId);
            } else {
                dirtyPageIds.remove(pageId);
            }
        }
    }

    /**
     * Detaches dirty tracking from every resident page and drops all frame
     * state (o2 Buffer-4). Runs under the pool lock during close/abandon.
     */
    private void releaseAllFramesLocked() {
        for (BufferFrame frame : frames.values()) {
            frame.page.setDirtyListener(null);
        }
        frames.clear();
        lru.clear();
        pinnedPages = 0;
        synchronized (dirtyLock) {
            dirtyPageIds.clear();
        }
    }

    // ─── Configuration ──────────────────────────────────────────────

    /**
     * Resolves the frame count from {@code bufferpool.size.mb} (system
     * property → {@code config.properties} → default {@value #DEFAULT_SIZE_MB})
     * divided by the configured page size (at least one frame).
     *
     * @return frame count for the configured pool
     */
    static int resolveCapacityPages() {
        int pageSize = PageConfig.getPageSize();
        long sizeBytes = (long) resolveSizeMb() * 1024L * 1024L;
        return (int) Math.max(1L, sizeBytes / pageSize);
    }

    /**
     * Reads {@code bufferpool.size.mb}: system property first, then
     * {@code config.properties}, falling back to {@value #DEFAULT_SIZE_MB}
     * with a warning on unparsable or non-positive values.
     *
     * @return configured pool size in megabytes
     */
    static long resolveSizeMb() {
        String raw = System.getProperty(SIZE_MB_KEY);
        if (raw == null) {
            raw = PageConfig.loadRootProps().getProperty(SIZE_MB_KEY, String.valueOf(DEFAULT_SIZE_MB));
        }
        if (raw == null || raw.isBlank()) {
            LOGGER.warn("Empty {}, using default {}", SIZE_MB_KEY, DEFAULT_SIZE_MB);
            return DEFAULT_SIZE_MB;
        }
        try {
            long mb = Long.parseLong(raw.trim());
            if (mb <= 0) {
                LOGGER.warn("Invalid {} '{}': must be positive, using default {}", SIZE_MB_KEY, raw, DEFAULT_SIZE_MB);
                return DEFAULT_SIZE_MB;
            }
            return mb;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid {} '{}': {}, using default {}", SIZE_MB_KEY, raw, e.getMessage(), DEFAULT_SIZE_MB);
            return DEFAULT_SIZE_MB;
        }
    }

    // ─── JMX registration ───────────────────────────────────────────

    /**
     * Registers {@code diesel:type=BufferPool,id=N} on the platform MBean
     * server; the id suffix keeps multiple pool instances independent.
     * Failure is logged, never fatal.
     */
    private void registerMBean() {
        try {
            ObjectName name = new ObjectName(
                    OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, name);
            registeredName = name;
        } catch (Exception e) {
            LOGGER.warn("Failed to register BufferPool MBean: {}", e.toString());
        }
    }

    private void unregisterMBean() {
        ObjectName name = registeredName;
        registeredName = null;
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
        }
    }

    @Override
    public String toString() {
        return String.format("BufferPool{capacity=%d,resident=%d,pinned=%d,hits=%d,misses=%d,evictions=%d,pageSize=%d}",
                capacityPages, frames.size(), pinnedPages, hits.get(), misses.get(), evictions.get(), pageSize);
    }
}
