package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Serializable wire message requesting the server to prepare a statement
 * (Prompt 79). Carries the SQL template with {@code ?} placeholders and the
 * caller's transaction id. The server parses the template lazily and registers
 * it in the handler's prepared-statement registry, answering with a statement
 * id the client uses for {@link ExecutePreparedMessage}.
 *
 * @see DatabaseClient
 * @see DatabaseServer
 * @see ExecutePreparedMessage
 */
class PrepareMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String sqlTemplate;
    private final UUID transactionId;

    /**
     * Creates a prepare message.
     *
     * @param sqlTemplate   the SQL template with {@code ?} placeholders
     * @param transactionId the caller's transaction id, or null
     */
    public PrepareMessage(String sqlTemplate, UUID transactionId) {
        this.sqlTemplate = sqlTemplate;
        this.transactionId = transactionId;
    }

    /**
     * Returns the SQL template.
     *
     * @return the template text
     */
    public String getSqlTemplate() {
        return sqlTemplate;
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
