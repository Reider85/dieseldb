package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Response for prepare message (server -> client) (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 PREPARE_RESPONSE][preparedId: UTF][txid: 16 bytes]
 * </pre>
 */
public final class PrepareResponseMessage implements Message {
    private final String preparedId;
    private final UUID transactionId;

    /**
     * Creates a prepare response message.
     *
     * @param preparedId    the prepared statement id
     * @param transactionId the transaction id, or null
     */
    public PrepareResponseMessage(String preparedId, UUID transactionId) {
        this.preparedId = preparedId;
        this.transactionId = transactionId;
    }

    /**
     * Returns the prepared statement id.
     *
     * @return the prepared statement id
     */
    public String getPreparedId() {
        return preparedId;
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
        return MessageType.PREPARE_RESPONSE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(preparedId);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a prepare response message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded prepare response message
     * @throws IOException if an I/O error occurs
     */
    public static PrepareResponseMessage readFrom(DataInput in) throws IOException {
        String preparedId = in.readUTF();
        UUID transactionId = readUUID(in);
        return new PrepareResponseMessage(preparedId, transactionId);
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