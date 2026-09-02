package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Serializable wire message requesting the server to close (deregister) a
 * previously prepared statement, releasing its cached parsed AST (Prompt 79).
 *
 * @see DatabaseClient
 * @see DatabaseServer
 */
class ClosePreparedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String statementId;
    private final UUID transactionId;

    /**
     * Creates a close-prepared message.
     *
     * @param statementId   the prepared-statement id to close
     * @param transactionId the caller's transaction id, or null
     */
    public ClosePreparedMessage(String statementId, UUID transactionId) {
        this.statementId = statementId;
        this.transactionId = transactionId;
    }

    /**
     * Returns the prepared-statement id to close.
     *
     * @return the statement id
     */
    public String getStatementId() {
        return statementId;
    }

    /**
     * Returns the caller's transaction id, or null when not in a transaction.
     *
     * @return the transaction id, or null
     */
    public UUID getTransactionId() {
        return transactionId;
    }
}
