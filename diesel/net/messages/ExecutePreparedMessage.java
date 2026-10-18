package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Execute a previously prepared statement with parameters (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 EXECUTE_PREPARED][preparedId: UTF][paramCount: short]
 *   [parameters: ×N][txid: 16 bytes]
 * </pre>
 * Each parameter is serialized as:
 * <pre>
 *   [type: byte][value: ...]
 * </pre>
 */
public final class ExecutePreparedMessage implements Message {
    private final String preparedId;
    private final List<Object> parameters;
    private final UUID transactionId;

    /**
     * Creates an execute prepared message.
     *
     * @param preparedId    the prepared statement id
     * @param parameters    the query parameters
     * @param transactionId the caller's transaction id, or null
     */
    public ExecutePreparedMessage(String preparedId, List<Object> parameters, UUID transactionId) {
        this.preparedId = preparedId;
        this.parameters = parameters;
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
     * Returns the query parameters.
     *
     * @return list of parameters
     */
    public List<Object> getParameters() {
        return parameters;
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
        return MessageType.EXECUTE_PREPARED;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(preparedId);
        
        // Write parameters
        out.writeShort(parameters.size());
        for (Object param : parameters) {
            writeParameter(out, param);
        }
        
        // Write transaction id
        writeUUID(out, transactionId);
    }

    /**
     * Reads an execute prepared message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded execute prepared message
     * @throws IOException if an I/O error occurs
     */
    public static ExecutePreparedMessage readFrom(DataInput in) throws IOException {
        String preparedId = in.readUTF();
        
        // Read parameters
        int paramCount = in.readShort();
        List<Object> parameters = new java.util.ArrayList<>(paramCount);
        for (int i = 0; i < paramCount; i++) {
            parameters.add(readParameter(in));
        }
        
        UUID transactionId = readUUID(in);
        return new ExecutePreparedMessage(preparedId, parameters, transactionId);
    }

    private void writeParameter(DataOutput out, Object param) throws IOException {
        if (param == null) {
            out.writeByte(0);
            return;
        }
        
        if (param instanceof String) {
            out.writeByte(1);
            out.writeUTF((String) param);
        } else if (param instanceof Integer) {
            out.writeByte(2);
            out.writeInt((Integer) param);
        } else if (param instanceof Long) {
            out.writeByte(3);
            out.writeLong((Long) param);
        } else if (param instanceof Double) {
            out.writeByte(4);
            out.writeDouble((Double) param);
        } else if (param instanceof Boolean) {
            out.writeByte(5);
            out.writeBoolean((Boolean) param);
        } else {
            throw new IOException("Unsupported parameter type: " + param.getClass());
        }
    }

    private static Object readParameter(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case 0: return null;
            case 1: return in.readUTF();
            case 2: return in.readInt();
            case 3: return in.readLong();
            case 4: return in.readDouble();
            case 5: return in.readBoolean();
            default:
                throw new IOException("Unknown parameter type: " + type);
        }
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