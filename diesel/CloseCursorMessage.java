package diesel;

import java.io.Serializable;
import java.util.UUID;

/**
 * Client-to-server message that closes an open server-side cursor
 * (Prompt 81), releasing the server-held iterator.
 *
 * @see Cursor
 * @see OpenCursorMessage
 * @see FetchCursorMessage
 */
class CloseCursorMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final UUID cursorId;
    private final UUID transactionId;

    CloseCursorMessage(UUID cursorId, UUID transactionId) {
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
