package diesel;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Serializable wire message for batch query execution: carries multiple SQL queries
 * and the caller's transaction id for parallel execution of independent queries.
 *
 * @see DatabaseClient
 * @see DatabaseServer
 */
class BatchQueryMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<String> queries;
    private final UUID transactionId;

    /**
     * Creates a batch query message.
     *
     * @param queries       the list of SQL queries to execute
     * @param transactionId the caller's transaction id, or null
     */
    public BatchQueryMessage(List<String> queries, UUID transactionId) {
        this.queries = queries;
        this.transactionId = transactionId;
    }

    /**
     * Returns the list of SQL queries.
     *
     * @return the query texts
     */
    public List<String> getQueries() {
        return queries;
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