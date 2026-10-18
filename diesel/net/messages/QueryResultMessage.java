package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Result of a single query: columns, rows, status, duration (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 QUERY_RESULT][status: UTF][duration: long][columnCount: short]
 *   [columnNames: UTF×N][rowCount: int][rows: ×N]
 *   [txid: 16 bytes]
 * </pre>
 * Each row is serialized as:
 * <pre>
 *   [isNull: byte×columnCount][values: ×columnCount]
 * </pre>
 */
public final class QueryResultMessage implements Message {
    private final String status;
    private final long duration;
    private final List<String> columnNames;
    private final List<List<Object>> rows;
    private final UUID transactionId;

    /**
     * Creates a query result message.
     *
     * @param status        the execution status (e.g., "OK", "ERROR")
     * @param duration      execution duration in nanoseconds
     * @param columnNames   list of column names
     * @param rows          list of result rows, each row is a list of values
     * @param transactionId the transaction id, or null
     */
    public QueryResultMessage(String status, long duration, List<String> columnNames, 
                             List<List<Object>> rows, UUID transactionId) {
        this.status = status;
        this.duration = duration;
        this.columnNames = columnNames;
        this.rows = rows;
        this.transactionId = transactionId;
    }

    /**
     * Returns the execution status.
     *
     * @return the status string
     */
    public String getStatus() {
        return status;
    }

    /**
     * Returns the execution duration in nanoseconds.
     *
     * @return duration in nanoseconds
     */
    public long getDuration() {
        return duration;
    }

    /**
     * Returns the column names.
     *
     * @return list of column names
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * Returns the result rows.
     *
     * @return list of result rows
     */
    public List<List<Object>> getRows() {
        return rows;
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
        return MessageType.QUERY_RESULT;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(status);
        out.writeLong(duration);
        
        // Write column names
        out.writeShort(columnNames.size());
        for (String columnName : columnNames) {
            out.writeUTF(columnName);
        }
        
        // Write rows
        out.writeInt(rows.size());
        for (List<Object> row : rows) {
            writeRow(out, row);
        }
        
        // Write transaction id
        writeUUID(out, transactionId);
    }

    /**
     * Reads a query result message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded query result message
     * @throws IOException if an I/O error occurs
     */
    public static QueryResultMessage readFrom(DataInput in) throws IOException {
        String status = in.readUTF();
        long duration = in.readLong();
        
        // Read column names
        int columnCount = in.readShort();
        List<String> columnNames = new java.util.ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(in.readUTF());
        }
        
        // Read rows
        int rowCount = in.readInt();
        List<List<Object>> rows = new java.util.ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(readRow(in, columnCount));
        }
        
        UUID transactionId = readUUID(in);
        return new QueryResultMessage(status, duration, columnNames, rows, transactionId);
    }

    private void writeRow(DataOutput out, List<Object> row) throws IOException {
        int columnCount = columnNames.size();
        
        // Write null flags
        for (int i = 0; i < columnCount; i++) {
            out.writeByte(row.get(i) == null ? 1 : 0);
        }
        
        // Write values
        for (int i = 0; i < columnCount; i++) {
            writeValue(out, row.get(i));
        }
    }

    private static List<Object> readRow(DataInput in, int columnCount) throws IOException {
        List<Object> row = new java.util.ArrayList<>(columnCount);
        
        // Read null flags and values
        for (int i = 0; i < columnCount; i++) {
            boolean isNull = in.readByte() == 1;
            if (isNull) {
                row.add(null);
            } else {
                row.add(readValue(in));
            }
        }
        
        return row;
    }

    private void writeValue(DataOutput out, Object value) throws IOException {
        if (value == null) {
            throw new IOException("Cannot write null value in row");
        }
        
        if (value instanceof String) {
            out.writeByte(1);
            out.writeUTF((String) value);
        } else if (value instanceof Integer) {
            out.writeByte(2);
            out.writeInt((Integer) value);
        } else if (value instanceof Long) {
            out.writeByte(3);
            out.writeLong((Long) value);
        } else if (value instanceof Double) {
            out.writeByte(4);
            out.writeDouble((Double) value);
        } else if (value instanceof Boolean) {
            out.writeByte(5);
            out.writeBoolean((Boolean) value);
        } else {
            throw new IOException("Unsupported value type: " + value.getClass());
        }
    }

    private static Object readValue(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case 1: return in.readUTF();
            case 2: return in.readInt();
            case 3: return in.readLong();
            case 4: return in.readDouble();
            case 5: return in.readBoolean();
            default:
                throw new IOException("Unknown value type: " + type);
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