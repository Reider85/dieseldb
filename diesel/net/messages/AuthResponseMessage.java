package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Server verdict of the credentials handshake (prompt4.md #31).
 *
 * <p>On success {@code token} and {@code userId} identify the issued
 * session; on failure {@code message} carries the human-readable reason
 * ({@code token}/{@code userId} are null).
 *
 * <p>Binary layout:
 * <pre>
 *   [type: u16 AUTH_RESPONSE][success: byte][token: 16 bytes]
 *   [userId: 16 bytes][mustChangePassword: byte][message: writeUTF]
 *   [txid: 16 bytes]
 * </pre>
 */
public final class AuthResponseMessage implements Message {

    private final boolean success;
    private final UUID token;
    private final UUID userId;
    private final boolean mustChangePassword;
    private final String message;
    private final UUID transactionId;

    /**
     * Creates an auth response message.
     *
     * @param success              whether authentication succeeded
     * @param token                the session token, or null on failure
     * @param userId               the authenticated user id, or null on failure
     * @param mustChangePassword   whether the user must change the password first
     * @param message              human-readable result message
     * @param transactionId        the echoed transaction id, or null
     */
    public AuthResponseMessage(boolean success, UUID token, UUID userId,
            boolean mustChangePassword, String message, UUID transactionId) {
        this.success = success;
        this.token = token;
        this.userId = userId;
        this.mustChangePassword = mustChangePassword;
        this.message = message;
        this.transactionId = transactionId;
    }

    public boolean isSuccess() {
        return success;
    }

    public UUID getToken() {
        return token;
    }

    public UUID getUserId() {
        return userId;
    }

    public boolean isMustChangePassword() {
        return mustChangePassword;
    }

    public String getMessage() {
        return message;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public MessageType getType() {
        return MessageType.AUTH_RESPONSE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(success ? 1 : 0);
        writeUUID(out, token);
        writeUUID(out, userId);
        out.writeByte(mustChangePassword ? 1 : 0);
        out.writeUTF(message == null ? "" : message);
        writeUUID(out, transactionId);
    }

    /**
     * Reads an auth response message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded auth response message
     * @throws IOException if an I/O error occurs
     */
    public static AuthResponseMessage readFrom(DataInput in) throws IOException {
        boolean success = in.readByte() != 0;
        UUID token = readUUID(in);
        UUID userId = readUUID(in);
        boolean mustChangePassword = in.readByte() != 0;
        String message = in.readUTF();
        UUID transactionId = readUUID(in);
        return new AuthResponseMessage(success, token, userId, mustChangePassword, message, transactionId);
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
