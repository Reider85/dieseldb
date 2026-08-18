package diesel;

/**
 * Base runtime exception for all DieselDB-specific errors.
 *
 * <p>All domain exceptions ({@link TableNotFoundException},
 * {@link ColumnNotFoundException}, {@link SyntaxErrorException},
 * {@link TransactionException}) extend this class so that callers
 * can catch a single type when they do not need to distinguish
 * between failure modes.</p>
 */
public class DieselException extends RuntimeException {

    public DieselException(String message) {
        super(message);
    }

    public DieselException(String message, Throwable cause) {
        super(message, cause);
    }
}
