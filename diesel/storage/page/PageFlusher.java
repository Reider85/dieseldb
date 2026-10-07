package diesel.storage.page;

import java.io.IOException;

/**
 * Persistence callback used by {@link BufferPool} when a dirty page must be
 * written out (prompt4.md step 7, R3-002).
 *
 * <p>Invoked from two places:
 * <ul>
 *   <li><b>eviction</b> — an unpinned LRU page that carries uncommitted
 *       in-memory changes is flushed before its frame is discarded;</li>
 *   <li><b>explicit flush</b> — {@link BufferPool#flushDirty()} and
 *       {@link BufferPool#close()} persist every dirty resident page.</li>
 * </ul>
 *
 * <p>Not invoked for clean pages (they are discarded directly). The
 * implementation must be safe to call while the buffer pool lock is held
 * (prompt 8's {@code PageManager} will wire a {@code FileChannel} write here);
 * keep the callback short or perform slow work in a background flusher
 * (prompt 20, R3-005).
 *
 * <p>On failure the page is NOT discarded: eviction restores the frame so the
 * dirty data stays resident and the {@link IOException} propagates to the
 * caller.
 */
@FunctionalInterface
public interface PageFlusher {

    /**
     * Writes the given page's current content to its backing store.
     *
     * @param page the dirty page to persist (never {@code null}); its
     *             {@link Page#getPageId()} identifies the destination
     * @throws IOException if the page cannot be written
     */
    void flush(Page page) throws IOException;
}
