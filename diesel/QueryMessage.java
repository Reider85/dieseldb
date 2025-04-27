package diesel;

import java.io.Serializable;
import java.util.UUID;

class QueryMessage implements Serializable {
    private final String query;
    private final UUID transactionId;

    public QueryMessage(String query, UUID transactionId) {
        this.query = query;
        this.transactionId = transactionId;
    }

    public String getQuery() {
        return query;
    }

    public UUID getTransactionId() {
        return transactionId;
    }
}