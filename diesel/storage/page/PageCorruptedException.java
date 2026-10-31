package diesel.storage.page;

import java.io.IOException;

/**
 * Exception thrown when a page fails checksum verification (Prompt4 #52).
 * 
 * <p>Indicates that a page has been corrupted (bit rot, hardware failure, etc.)
 * and the checksum verification failed. This exception may be thrown during
 * page reads or when validating checksums.
 */
public class PageCorruptedException extends IOException {
    
    private final PageId pageId;
    private final int storedChecksum;
    private final int computedChecksum;
    
    /**
     * Creates a page corruption exception with detailed information.
     * 
     * @param pageId the ID of the corrupted page
     * @param storedChecksum the checksum stored in the page header
     * @param computedChecksum the checksum computed from the page data
     * @param message detailed error message
     */
    public PageCorruptedException(PageId pageId, int storedChecksum, int computedChecksum, String message) {
        super(message);
        this.pageId = pageId;
        this.storedChecksum = storedChecksum;
        this.computedChecksum = computedChecksum;
    }
    
    /**
     * Creates a page corruption exception with basic information.
     * 
     * @param message error message
     */
    public PageCorruptedException(String message) {
        this(null, 0, 0, message);
    }
    
    /**
     * Creates a page corruption exception with cause.
     * 
     * @param message error message
     * @param cause the underlying cause
     */
    public PageCorruptedException(String message, Throwable cause) {
        super(message, cause);
        this.pageId = null;
        this.storedChecksum = 0;
        this.computedChecksum = 0;
    }
    
    /**
     * Gets the ID of the corrupted page.
     * 
     * @return the page ID, or null if not available
     */
    public PageId getPageId() {
        return pageId;
    }
    
    /**
     * Gets the checksum stored in the page header.
     * 
     * @return the stored checksum value
     */
    public int getStoredChecksum() {
        return storedChecksum;
    }
    
    /**
     * Gets the checksum computed from the page data.
     * 
     * @return the computed checksum value
     */
    public int getComputedChecksum() {
        return computedChecksum;
    }
    
    /**
     * Gets a detailed message including checksum information.
     * 
     * @return detailed error message
     */
    @Override
    public String getMessage() {
        if (pageId != null && storedChecksum != 0 && computedChecksum != 0) {
            return super.getMessage() + 
                   " [Page: " + pageId + 
                   ", Stored: 0x" + Integer.toHexString(storedChecksum) + 
                   ", Computed: 0x" + Integer.toHexString(computedChecksum) + "]";
        }
        return super.getMessage();
    }
}