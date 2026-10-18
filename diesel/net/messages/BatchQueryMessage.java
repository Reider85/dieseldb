package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Batch of N queries executed in one round-trip (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 BATCH_QUERY][queryCount: short][queries: ×N][txid: 16 bytes]
 * </pre>
 * Each query is serialized as:
 * <pre>
 *   [query: UTF]
 * </pre>
 */
public final class BatchQueryMessage implements Message {
    private final List<String> queries;
    private final UUID transactionId;

    /**
     * Creates a batch query message.
     *
     * @param queries       the list of SQL queries to execute
     * @param transactionId the caller's transaction id, or null
     */
    public BatchQueryMessage(List<String> queries, UUID transactionId) {
        this.queries = queries;
        this.transactionId = transactionId;
    }

    /**
     * Returns the list of SQL queries.
     *
     * @return list of queries
     */
    public List<String> getQueries() {
        return queries;
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
        return MessageType.BATCH_QUERY;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeShort(queries.size());
        for (String query : queries) {
            out.writeUTF(query);
        }
        
        // Write transaction id
        writeUUID(out, transactionId);
    }

    /**
     * Reads a batch query message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded batch query message
     * @throws IOException if an I/O error occurs
     */
    public static BatchQueryMessage readFrom(DataInput in) throws IOException {
        int queryCount = in.readShort();
        List<String> queries = new java.util.ArrayList<>(queryCount);
        for (int i = 0; i < queryCount; i++) {
            queries.add(in.readUTF());
        }
        
        UUID transactionId = readUUID(in);
        return new BatchQueryMessage(queries, transactionId);
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