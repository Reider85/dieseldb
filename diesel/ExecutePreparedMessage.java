package diesel;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Serializable wire message requesting the server to execute a previously
 * prepared statement (Prompt 79). Carries the prepared-statement id returned
 * by {@link PrepareMessage} handling, the bound parameters and the caller's
 * transaction id.
 *
 * @see DatabaseClient
 * @see DatabaseServer
 * @see PrepareMessage
 */
class ExecutePreparedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String statementId;
    private final List<Object> params;
    private final UUID transactionId;

    /**
     * Creates an execute-prepared message.
     *
     * @param statementId   the prepared-statement id from the prepare phase
     * @param params        the bound parameter values, in placeholder order
     * @param transactionId the caller's transaction id, or null
     */
    public ExecutePreparedMessage(String statementId, List<Object> params, UUID transactionId) {
        this.statementId = statementId;
        this.params = params;
        this.transactionId = transactionId;
    }

    /**
     * Returns the prepared-statement id.
     *
     * @return the statement id
     */
    public String getStatementId() {
        return statementId;
    }

    /**
     * Returns the bound parameter values.
     *
     * @return the parameters, or null when none were bound
     */
    public List<Object> getParams() {
        return params;
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
