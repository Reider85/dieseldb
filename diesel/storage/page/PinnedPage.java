package diesel.storage.page;

/**
 * A pin handle for a resident {@link Page} (prompt4.md step 7, R3-002).
 *
 * <p>{@code pin(pageId)} returns a {@code PinnedPage}; while it is open the
 * frame cannot be evicted by LRU selection. {@link #close()} unpins the page
 * and is idempotent (double close is a no-op, safe with try-with-resources):
 *
 * <pre>{@code
 * try (PinnedPage pinned = pool.pin(pageId)) {
 *     pinned.getPage().insert(tuple);
 * }
 * }</pre>
 *
 * <p>Threading contract (Page itself is not thread-safe, prompt 6): the page
 * returned by {@link #getPage()} may only be accessed by the pinning thread
 * until this handle is closed, and the reference must not be used after
 * {@code close()} — {@link #getPage()} then throws
 * {@link IllegalStateException}. Concurrent writers to one page must
 * serialise outside the pool (e.g. a monitor on the {@link Page} instance).
 */
public final class PinnedPage implements AutoCloseable {

    private final BufferPool pool;
    private final PageId pageId;
    private final Page page;
    private volatile boolean closed;

    /**
     * Creates a pin handle. Package-private: instances are produced by
     * {@link BufferPool#pin(PageId)} / {@link BufferPool#insert(Page)}.
     *
     * @param pool   the owning pool (unpinned on close)
     * @param pageId the pinned page identifier
     * @param page   the pinned page instance (same object as the pool frame)
     */
    PinnedPage(BufferPool pool, PageId pageId, Page page) {
        this.pool = pool;
        this.pageId = pageId;
        this.page = page;
    }

    /**
     * Returns the pinned page identifier.
     *
     * @return the page id this handle pins
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Returns the pinned page for access while the pin is held.
     *
     * @return the resident page
     * @throws IllegalStateException if this handle was already closed
     */
    public Page getPage() {
        if (closed) {
            throw new IllegalStateException(
                    "PinnedPage for " + pageId + " is already unpinned; getPage() invalid after close()");
        }
        return page;
    }

    /**
     * Returns whether {@link #close()} has already released the pin.
     *
     * @return {@code true} once unpinned
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Unpins the page, making it evictable again. Idempotent: only the first
     * call releases the pin, later calls do nothing.
     */
    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            pool.unpin(pageId);
        }
    }
}
