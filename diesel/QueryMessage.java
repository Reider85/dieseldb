package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Serializable wire message exchanged between {@link DatabaseClient} and
 * {@link DatabaseServer}: carries the SQL query and the caller's transaction
 * id.
 *
 * @see DatabaseClient
 * @see DatabaseServer
 */
class QueryMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String query;
    private final UUID transactionId;

    /**
     * Creates a query message.
     *
     * @param query         the SQL query
     * @param transactionId the caller's transaction id, or null
     */
    public QueryMessage(String query, UUID transactionId) {
        this.query = query;
        this.transactionId = transactionId;
    }

    /**
     * Returns the SQL query.
     *
     * @return the query text
     */
    public String getQuery() {
        return query;
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