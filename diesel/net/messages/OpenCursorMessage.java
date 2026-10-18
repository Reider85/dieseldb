package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Open a server-side cursor over a SELECT (prompt4.md #29).
 *
 * <p>Binary layout:
 * <pre>
 *   [type: u16 OPEN_CURSOR][query: UTF][fetchSize: i32][txid: 16 bytes]
 * </pre>
 */
public final class OpenCursorMessage implements Message {
    private final String query;
    private final int fetchSize;
    private final UUID transactionId;

    /**
     * Creates an open cursor message.
     *
     * @param query         the SQL SELECT to run
     * @param fetchSize     rows per fetch ({@code <= 0} lets the server pick)
     * @param transactionId the caller's transaction id, or null
     */
    public OpenCursorMessage(String query, int fetchSize, UUID transactionId) {
        this.query = query;
        this.fetchSize = fetchSize;
        this.transactionId = transactionId;
    }

    /**
     * Returns the SQL SELECT.
     *
     * @return the query text
     */
    public String getQuery() {
        return query;
    }

    /**
     * Returns the requested rows per fetch.
     *
     * @return the fetch size, or {@code <= 0} for the server default
     */
    public int getFetchSize() {
        return fetchSize;
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
        return MessageType.OPEN_CURSOR;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(query);
        out.writeInt(fetchSize);
        writeUUID(out, transactionId);
    }

    /**
     * Reads an open cursor message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded message
     * @throws IOException if an I/O error occurs
     */
    public static OpenCursorMessage readFrom(DataInput in) throws IOException {
        String query = in.readUTF();
        int fetchSize = in.readInt();
        UUID transactionId = readUUID(in);
        return new OpenCursorMessage(query, fetchSize, transactionId);
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
