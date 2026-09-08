package diesel;

/**
 * Thrown when an index structure is corrupted or inconsistent.
 */
public class IndexCorruptionException extends DieselException {

    public IndexCorruptionException(String message) {
        super(message);
    }

    public IndexCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}