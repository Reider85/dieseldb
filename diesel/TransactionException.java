package diesel;

/**
 * Thrown when a transaction operation is invalid (e.g. committing
 * when no transaction is active, or starting a second transaction
 * on the same client).
 */
public class TransactionException extends DieselException {

    public TransactionException(String message) {
        super(message);
    }
}
