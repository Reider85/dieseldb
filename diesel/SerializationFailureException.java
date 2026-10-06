package diesel;

/**
 * Thrown when a SERIALIZABLE transaction violates the serializable isolation contract
 * by detecting a write-write conflict or a read-write conflict at commit time.
 * Extends TransactionException for backward compatibility with existing tests
 * that catch TransactionException for transaction-related failures.
 */
public class SerializationFailureException extends TransactionException {

    public SerializationFailureException(String message) {
        super(message);
    }
}