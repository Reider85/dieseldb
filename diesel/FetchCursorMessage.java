package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Client-to-server message that fetches the next batch of rows from an
 * open server-side cursor (Prompt 81).
 *
 * @see Cursor
 * @see OpenCursorMessage
 * @see CloseCursorMessage
 */
class FetchCursorMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final UUID cursorId;
    private final UUID transactionId;

    FetchCursorMessage(UUID cursorId, UUID transactionId) {
        this.cursorId = cursorId;
        this.transactionId = transactionId;
    }

    UUID getCursorId() {
        return cursorId;
    }

    UUID getTransactionId() {
        return transactionId;
    }
}
