package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Close a prepared statement by id (prompt4.md #29).
 *
 * <p>Binary layout:
 * <pre>
 *   [type: u16 CLOSE_PREPARED][statementId: UTF][txid: 16 bytes]
 * </pre>
 */
public final class ClosePreparedMessage implements Message {
    private final String statementId;
    private final UUID transactionId;

    /**
     * Creates a close prepared statement message.
     *
     * @param statementId   the server-assigned statement id
     * @param transactionId the caller's transaction id, or null
     */
    public ClosePreparedMessage(String statementId, UUID transactionId) {
        this.statementId = statementId;
        this.transactionId = transactionId;
    }

    /**
     * Returns the statement id.
     *
     * @return the statement id
     */
    public String getStatementId() {
        return statementId;
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
        return MessageType.CLOSE_PREPARED;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(statementId);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a close prepared statement message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded message
     * @throws IOException if an I/O error occurs
     */
    public static ClosePreparedMessage readFrom(DataInput in) throws IOException {
        String statementId = in.readUTF();
        UUID transactionId = readUUID(in);
        return new ClosePreparedMessage(statementId, transactionId);
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
