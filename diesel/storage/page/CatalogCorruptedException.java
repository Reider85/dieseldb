package diesel.storage.page;

/**
 * Raised when the catalog page (pageId=0) cannot be parsed back into table
 * schemas (prompt4 #9). The message is deliberately self-contained so an
 * operator can act on it without reading the stack trace.
 */
public class CatalogCorruptedException extends RuntimeException {

    public CatalogCorruptedException(String message) {
        super("Catalog page 0 is corrupt: " + message);
    }

    public CatalogCorruptedException(String message, Throwable cause) {
        super("Catalog page 0 is corrupt: " + message, cause);
    }
}
