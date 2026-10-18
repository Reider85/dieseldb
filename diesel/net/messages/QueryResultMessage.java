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
 *   [type: u16 QUERY_RESULT][status: str][duration: long][columnCount: short]
 *   [columnNames: str×N][rowCount: int][rows: ×N]
 *   [txid: 16 bytes]
 * </pre>
 * Each row is serialized as:
 * <pre>
 *   [isNull: byte×columnCount][values: ×columnCount]
 * </pre>
 * Strings use {@code [length: i32][utf8 bytes]} rather than {@code writeUTF},
 * so a single cell may exceed the 64 KiB {@code DataOutput.writeUTF} limit.
 *
 * <p>The {@code status} field carries the shape of the original Java result so
 * the client can rebuild it exactly:
 * <ul>
 *   <li>{@link #STATUS_ROWS} — a row result (rebuilt as
 *       {@code List<Map<String,Object>>})</li>
 *   <li>{@link #STATUS_SCALAR} — a single value (rebuilt as that value)</li>
 *   <li>{@link #STATUS_ERROR} — an {@code Error: } message (rebuilt as the
 *       message String)</li>
 * </ul>
 */
public final class QueryResultMessage implements Message {

    /** Row result: rebuild as a list of rows keyed by column name. */
    public static final String STATUS_ROWS = "OK";

    /** Scalar result: rebuild as the single value in the first row. */
    public static final String STATUS_SCALAR = "SCALAR";

    /** Error result: rebuild as the error message String. */
    public static final String STATUS_ERROR = "ERROR";

    private static final int TAG_STRING = 1;
    private static final int TAG_INT = 2;
    private static final int TAG_LONG = 3;
    private static final int TAG_DOUBLE = 4;
    private static final int TAG_BOOLEAN = 5;
    private static final int TAG_FLOAT = 6;
    private static final int TAG_SHORT = 7;
    private static final int TAG_BYTE = 8;
    private static final int TAG_BIG_DECIMAL = 9;
    private static final int TAG_BIG_INTEGER = 10;
    private static final int TAG_STRING_FALLBACK = 11;

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
        writeString(out, status);
        out.writeLong(duration);
        
        // Write column names
        out.writeShort(columnNames.size());
        for (String columnName : columnNames) {
            writeString(out, columnName);
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
        String status = readString(in);
        long duration = in.readLong();
        
        // Read column names
        int columnCount = in.readShort();
        List<String> columnNames = new java.util.ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columnNames.add(readString(in));
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
        for (int i = 0; i < columnCount; i++) {
            Object value = row.get(i);
            out.writeByte(value == null ? 1 : 0);
            if (value != null) {
                writeValue(out, value);
            }
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

        if (value instanceof String s) {
            out.writeByte(TAG_STRING);
            writeString(out, s);
        } else if (value instanceof Integer i) {
            out.writeByte(TAG_INT);
            out.writeInt(i);
        } else if (value instanceof Long l) {
            out.writeByte(TAG_LONG);
            out.writeLong(l);
        } else if (value instanceof Double d) {
            out.writeByte(TAG_DOUBLE);
            out.writeDouble(d);
        } else if (value instanceof Boolean b) {
            out.writeByte(TAG_BOOLEAN);
            out.writeBoolean(b);
        } else if (value instanceof Float f) {
            out.writeByte(TAG_FLOAT);
            out.writeFloat(f);
        } else if (value instanceof Short s) {
            out.writeByte(TAG_SHORT);
            out.writeShort(s);
        } else if (value instanceof Byte b) {
            out.writeByte(TAG_BYTE);
            out.writeByte(b);
        } else if (value instanceof java.math.BigDecimal bd) {
            out.writeByte(TAG_BIG_DECIMAL);
            writeString(out, bd.toPlainString());
        } else if (value instanceof java.math.BigInteger bi) {
            out.writeByte(TAG_BIG_INTEGER);
            writeString(out, bi.toString());
        } else {
            // Last resort: keep the value visible instead of failing the whole
            // result. The client receives the textual form of the type.
            out.writeByte(TAG_STRING_FALLBACK);
            writeString(out, value.toString());
        }
    }

    private static Object readValue(DataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case TAG_STRING:
                return readString(in);
            case TAG_INT:
                return in.readInt();
            case TAG_LONG:
                return in.readLong();
            case TAG_DOUBLE:
                return in.readDouble();
            case TAG_BOOLEAN:
                return in.readBoolean();
            case TAG_FLOAT:
                return in.readFloat();
            case TAG_SHORT:
                return in.readShort();
            case TAG_BYTE:
                return in.readByte();
            case TAG_BIG_DECIMAL:
                return new java.math.BigDecimal(readString(in));
            case TAG_BIG_INTEGER:
                return new java.math.BigInteger(readString(in));
            case TAG_STRING_FALLBACK:
                return readString(in);
            default:
                throw new IOException("Unknown value type: " + type);
        }
    }

    /**
     * Writes a length-prefixed UTF-8 string. Unlike {@code DataOutput.writeUTF}
     * there is no 64 KiB limit on the value.
     */
    private static void writeString(DataOutput out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        if (length > WireProtocol.MAX_PAYLOAD_BYTES) {
            throw new IOException("String too long: " + length + " bytes");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
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