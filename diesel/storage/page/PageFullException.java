package diesel.storage.page;

/**
 * Exception thrown when a page insertion fails due to insufficient free space (R3-002).
 * 
 * <p>Thrown by Page.insert() when the tuple cannot fit in the available free space.
 * 
 * <p>Extends RuntimeException for convenience (no checked exceptions needed in page layer).
 */
public class PageFullException extends RuntimeException {

    /**
     * Constructs a new PageFullException with the specified detail message.
     * 
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     */
    public PageFullException(String message) {
        super(message);
    }

    /**
     * Constructs a new PageFullException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public PageFullException(String message, Throwable cause) {
        super(message, cause);
    }
}