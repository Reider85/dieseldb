package diesel.storage.page;

/**
 * Exception thrown when a page format is invalid or corrupt (R3-002).
 * 
 * <p>Used by Page.readFrom() and SlottedPageLayout.validateLayout() when:
 * - Magic number doesn't match
 * - Format version is unsupported
 * - Page size is invalid
 * - Header fields are out of bounds
 * - Slot/tuple data is corrupt
 * 
 * <p>Extends RuntimeException for convenience (no checked exceptions needed in page layer).
 */
public class PageFormatException extends RuntimeException {

    /**
     * Constructs a new PageFormatException with the specified detail message.
     * 
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     */
    public PageFormatException(String message) {
        super(message);
    }

    /**
     * Constructs a new PageFormatException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public PageFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}