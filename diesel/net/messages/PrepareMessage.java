package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Prepare a SQL template (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 PREPARE][sqlTemplate: UTF][txid: 16 bytes]
 * </pre>
 */
public final class PrepareMessage implements Message {
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

    @Override
    public MessageType getType() {
        return MessageType.PREPARE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(sqlTemplate);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a prepare message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded prepare message
     * @throws IOException if an I/O error occurs
     */
    public static PrepareMessage readFrom(DataInput in) throws IOException {
        String sqlTemplate = in.readUTF();
        UUID transactionId = readUUID(in);
        return new PrepareMessage(sqlTemplate, transactionId);
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