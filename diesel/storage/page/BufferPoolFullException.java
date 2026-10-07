package diesel.storage.page;

import java.io.IOException;

/**
 * Thrown when {@link BufferPool} cannot make room for a new page because
 * every frame in the pool is pinned (prompt4.md step 7, R3-002).
 *
 * <p>Extends {@link IOException} so callers handle it together with
 * {@link PageLoader}/{@link PageFlusher} I/O failures in one catch clause.
 * The condition is transient: unpinning any resident page (or closing a
 * leaked {@link PinnedPage}) makes the pool usable again.
 */
public class BufferPoolFullException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new BufferPoolFullException with the specified detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                 by the {@link #getMessage()} method)
     */
    public BufferPoolFullException(String message) {
        super(message);
    }
}
