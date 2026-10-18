package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Health probe response (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 HEALTH_CHECK_RESPONSE][status: UTF][timestamp: long][responseTime: long][txid: 16 bytes]
 * </pre>
 */
public final class HealthCheckResponse implements Message {
    private final String status;
    private final long timestamp;
    private final long responseTime;
    private final UUID transactionId;

    /**
     * Creates a health check response message.
     *
     * @param status        the health status (e.g., "OK", "ERROR")
     * @param timestamp     the original request timestamp
     * @param responseTime  the response time in nanoseconds
     * @param transactionId the caller's transaction id, or null
     */
    public HealthCheckResponse(String status, long timestamp, long responseTime, UUID transactionId) {
        this.status = status;
        this.timestamp = timestamp;
        this.responseTime = responseTime;
        this.transactionId = transactionId;
    }

    /**
     * Returns the health status.
     *
     * @return the status string
     */
    public String getStatus() {
        return status;
    }

    /**
     * Returns the original request timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the response time in nanoseconds.
     *
     * @return response time in nanoseconds
     */
    public long getResponseTime() {
        return responseTime;
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
        return MessageType.HEALTH_CHECK_RESPONSE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(status);
        out.writeLong(timestamp);
        out.writeLong(responseTime);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a health check response message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded health check response message
     * @throws IOException if an I/O error occurs
     */
    public static HealthCheckResponse readFrom(DataInput in) throws IOException {
        String status = in.readUTF();
        long timestamp = in.readLong();
        long responseTime = in.readLong();
        UUID transactionId = readUUID(in);
        return new HealthCheckResponse(status, timestamp, responseTime, transactionId);
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