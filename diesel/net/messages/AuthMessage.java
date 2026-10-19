package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

/**
 * Credentials handshake sent as the first v2 frame of a session
 * (prompt4.md #31, R3-008 step 2/4).
 *
 * <p>The plaintext password travels over the wire only until TLS lands
 * (prompt4.md #34); never log it.
 *
 * <p>Binary layout:
 * <pre>
 *   [type: u16 AUTH][userName: writeUTF][password: writeUTF]
 *   [databaseName: writeUTF][txid: 16 bytes]
 * </pre>
 */
public final class AuthMessage implements Message {

    private final String userName;
    private final String password;
    private final String databaseName;
    private final UUID transactionId;

    /**
     * Creates an authentication message.
     *
     * @param userName      the account name
     * @param password      the plaintext password
     * @param databaseName  the requested database, or null for the default
     * @param transactionId the caller's transaction id, or null
     */
    public AuthMessage(String userName, String password, String databaseName, UUID transactionId) {
        this.userName = userName;
        this.password = password;
        this.databaseName = databaseName;
        this.transactionId = transactionId;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    @Override
    public MessageType getType() {
        return MessageType.AUTH;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(userName == null ? "" : userName);
        out.writeUTF(password == null ? "" : password);
        out.writeUTF(databaseName == null ? "" : databaseName);
        writeUUID(out, transactionId);
    }

    /**
     * Reads an auth message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded auth message
     * @throws IOException if an I/O error occurs
     */
    public static AuthMessage readFrom(DataInput in) throws IOException {
        String userName = in.readUTF();
        String password = in.readUTF();
        String databaseName = in.readUTF();
        UUID transactionId = readUUID(in);
        return new AuthMessage(userName, password, databaseName, transactionId);
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
