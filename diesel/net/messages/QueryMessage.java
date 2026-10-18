package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * SQL query with bind variables (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 QUERY][query: UTF][txid: 16 bytes]
 * </pre>
 */
public final class QueryMessage implements Message {
    private final String query;
    private final UUID transactionId;

    /**
     * Creates a query message.
     *
     * @param query         the SQL query
     * @param transactionId the caller's transaction id, or null
     */
    public QueryMessage(String query, UUID transactionId) {
        this.query = query;
        this.transactionId = transactionId;
    }

    /**
     * Returns the SQL query.
     *
     * @return the query text
     */
    public String getQuery() {
        return query;
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
        return MessageType.QUERY;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(query);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a query message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded query message
     * @throws IOException if an I/O error occurs
     */
    public static QueryMessage readFrom(DataInput in) throws IOException {
        String query = in.readUTF();
        UUID transactionId = readUUID(in);
        return new QueryMessage(query, transactionId);
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