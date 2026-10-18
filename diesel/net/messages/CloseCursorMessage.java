package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Close an open cursor (prompt4.md #29).
 *
 * <p>Binary layout:
 * <pre>
 *   [type: u16 CLOSE_CURSOR][cursorId: 16 bytes][txid: 16 bytes]
 * </pre>
 */
public final class CloseCursorMessage implements Message {
    private final UUID cursorId;
    private final UUID transactionId;

    /**
     * Creates a close cursor message.
     *
     * @param cursorId      the server-assigned cursor id
     * @param transactionId the caller's transaction id, or null
     */
    public CloseCursorMessage(UUID cursorId, UUID transactionId) {
        this.cursorId = cursorId;
        this.transactionId = transactionId;
    }

    /**
     * Returns the cursor id.
     *
     * @return the cursor id
     */
    public UUID getCursorId() {
        return cursorId;
    }

    /**
     * Returns the caller's transaction id, or null when not in a transaction.
     *
     * @return the transaction id, or null
     */
    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public MessageType getType() {
        return MessageType.CLOSE_CURSOR;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        writeUUID(out, cursorId);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a close cursor message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded message
     * @throws IOException if an I/O error occurs
     */
    public static CloseCursorMessage readFrom(DataInput in) throws IOException {
        UUID cursorId = readUUID(in);
        UUID transactionId = readUUID(in);
        return new CloseCursorMessage(cursorId, transactionId);
    }

    private static void writeUUID(DataOutput out, UUID uuid) throws IOException {
        if (uuid == null) {
            out.writeLong(0);
            out.writeLong(0);
        } else {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }
    }

    private static UUID readUUID(DataInput in) throws IOException {
        long msb = in.readLong();
        long lsb = in.readLong();
        if (msb == 0 && lsb == 0) {
            return null;
        }
        return new UUID(msb, lsb);
    }
}
