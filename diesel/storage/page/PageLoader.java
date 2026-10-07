package diesel.storage.page;

import java.io.IOException;

/**
 * Load callback used by {@link BufferPool} when a requested page is not
 * resident (prompt4.md step 7, R3-002).
 *
 * <p>Invoked on a cache miss inside {@link BufferPool#pin(PageId, PageLoader)}
 * while the pool lock is held, before the loaded page becomes resident. The
 * implementation should therefore be short; prompt 8's {@code PageManager}
 * will wire a {@code FileChannel} positional read here.
 *
 * <p>The returned page must carry the requested {@link PageId}; the pool
 * rejects a mismatched or {@code null} page with an error.
 */
@FunctionalInterface
public interface PageLoader {

    /**
     * Loads the given page from the backing store.
     *
     * @param pageId identifier of the page to load (never {@code null})
     * @return the loaded page, never {@code null}, with {@code pageId} equal
     *         to the requested identifier
     * @throws IOException if the page cannot be read
     */
    Page load(PageId pageId) throws IOException;
}
