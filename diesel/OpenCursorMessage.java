package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Client-to-server message that opens a server-side cursor (Prompt 81).
 * The server executes {@code query} and holds an iterator, so the client can
 * fetch the result in batches of {@code fetchSize} rows via
 * {@link FetchCursorMessage}.
 *
 * @see Cursor
 * @see FetchCursorMessage
 * @see CloseCursorMessage
 */
class OpenCursorMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String query;
    private final int fetchSize;
    private final UUID transactionId;

    OpenCursorMessage(String query, int fetchSize, UUID transactionId) {
        this.query = query;
        this.fetchSize = fetchSize;
        this.transactionId = transactionId;
    }

    String getQuery() {
        return query;
    }

    int getFetchSize() {
        return fetchSize;
    }

    UUID getTransactionId() {
        return transactionId;
    }
}
