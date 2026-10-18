package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Close cursor response (server -> client) (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 CLOSE_CURSOR_RESPONSE][txid: 16 bytes]
 * </pre>
 */
public final class CloseCursorResponseMessage implements Message {
    private final UUID transactionId;

    /**
     * Creates a close cursor response message.
     *
     * @param transactionId the transaction id, or null
     */
    public CloseCursorResponseMessage(UUID transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Returns the transaction id.
     *
     * @return the transaction id, or null
     */
    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public MessageType getType() {
        return MessageType.CLOSE_CURSOR_RESPONSE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        writeUUID(out, transactionId);
    }

    /**
     * Reads a close cursor response message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded close cursor response message
     * @throws IOException if an I/O error occurs
     */
    public static CloseCursorResponseMessage readFrom(DataInput in) throws IOException {
        UUID transactionId = readUUID(in);
        return new CloseCursorResponseMessage(transactionId);
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