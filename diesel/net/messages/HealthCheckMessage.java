package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Health / liveness probe (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 HEALTH_CHECK][timestamp: long][txid: 16 bytes]
 * </pre>
 */
public final class HealthCheckMessage implements Message {
    private final long timestamp;
    private final UUID transactionId;

    /**
     * Creates a health check message.
     *
     * @param timestamp     the timestamp when the check was requested
     * @param transactionId the caller's transaction id, or null
     */
    public HealthCheckMessage(long timestamp, UUID transactionId) {
        this.timestamp = timestamp;
        this.transactionId = transactionId;
    }

    /**
     * Returns the timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
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
        return MessageType.HEALTH_CHECK;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a health check message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded health check message
     * @throws IOException if an I/O error occurs
     */
    public static HealthCheckMessage readFrom(DataInput in) throws IOException {
        long timestamp = in.readLong();
        UUID transactionId = readUUID(in);
        return new HealthCheckMessage(timestamp, transactionId);
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