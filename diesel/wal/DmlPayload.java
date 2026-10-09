package diesel.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Payload of a WAL {@link WALOpcode#INSERT}, {@link WALOpcode#UPDATE} or
 * {@link WALOpcode#DELETE} record (prompt 4 #19).
 *
 * <p>The logical DML payload identifies the affected row by table name and
 * raw row index, and carries the row values as a type-tagged map. For
 * {@code INSERT} only the after-image slot is populated; for {@code DELETE}
 * only the before-image slot; for {@code UPDATE} both slots carry a payload.
 *
 * <pre>
 *   writeUTF  tableName
 *   int       rowIndex
 *   int       column count
 *   per column: writeUTF name, byte typeTag, value
 * </pre>
 *
 * <p>Supported type tags: {@code 0 = null}, {@code 1 = boolean}, {@code 2 = int},
 * {@code 3 = long}, {@code 4 = double}, {@code 5 = float}, {@code 6 = String},
 * {@code 7 = BigDecimal}, {@code 8 = other Serializable via writeObject}.
 *
 * <p>Like {@link CommitPayload}, malformed payloads are reported as unchecked
 * {@link IllegalArgumentException} so the undo phase can skip a corrupt record
 * without aborting the recovery pass.
 */
public final class DmlPayload {

    private static final byte TAG_NULL = 0;
    private static final byte TAG_BOOLEAN = 1;
    private static final byte TAG_INT = 2;
    private static final byte TAG_LONG = 3;
    private static final byte TAG_DOUBLE = 4;
    private static final byte TAG_FLOAT = 5;
    private static final byte TAG_STRING = 6;
    private static final byte TAG_BIG_DECIMAL = 7;
    private static final byte TAG_OBJECT = 8;

    private final String tableName;
    private final int rowIndex;
    private final Map<String, Object> values;

    /**
     * Creates a DML payload.
     *
     * @param tableName the affected table
     * @param rowIndex  the raw row index
     * @param values    the row values (before-image for DELETE, after-image
     *                  for INSERT, and separately for UPDATE)
     */
    public DmlPayload(String tableName, int rowIndex, Map<String, Object> values) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.rowIndex = rowIndex;
        this.values = values == null ? Map.of() : new LinkedHashMap<>(values);
    }

    /**
     * Serializes a DML payload.
     *
     * @param tableName the affected table
     * @param rowIndex  the raw row index
     * @param values    the row values
     * @return the encoded payload bytes
     * @throws IOException if the payload cannot be written
     */
    public static byte[] serialize(String tableName, int rowIndex, Map<String, Object> values)
            throws IOException {
        Objects.requireNonNull(tableName, "tableName");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeUTF(tableName);
        dos.writeInt(rowIndex);
        Map<String, Object> safe = values == null ? Map.of() : values;
        dos.writeInt(safe.size());
        for (Map.Entry<String, Object> entry : safe.entrySet()) {
            dos.writeUTF(entry.getKey());
            writeValue(dos, entry.getValue());
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Deserializes a DML payload written by {@link #serialize}.
     *
     * @param data the encoded payload bytes
     * @return the decoded payload
     * @throws IllegalArgumentException if the payload is truncated or malformed
     */
    public static DmlPayload deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            String tableName = dis.readUTF();
            int rowIndex = dis.readInt();
            int columnCount = dis.readInt();
            if (columnCount < 0) {
                throw new IllegalArgumentException("Negative column count: " + columnCount);
            }
            Map<String, Object> values = new LinkedHashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String column = dis.readUTF();
                Object value = readValue(dis);
                values.put(column, value);
            }
            return new DmlPayload(tableName, rowIndex, values);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Truncated DML payload (" + data.length + " bytes)", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed DML payload: " + e.getMessage(), e);
        }
    }

    private static void writeValue(DataOutputStream dos, Object value) throws IOException {
        if (value == null) {
            dos.writeByte(TAG_NULL);
        } else if (value instanceof Boolean b) {
            dos.writeByte(TAG_BOOLEAN);
            dos.writeBoolean(b);
        } else if (value instanceof Integer i) {
            dos.writeByte(TAG_INT);
            dos.writeInt(i);
        } else if (value instanceof Long l) {
            dos.writeByte(TAG_LONG);
            dos.writeLong(l);
        } else if (value instanceof Double d) {
            dos.writeByte(TAG_DOUBLE);
            dos.writeDouble(d);
        } else if (value instanceof Float f) {
            dos.writeByte(TAG_FLOAT);
            dos.writeFloat(f);
        } else if (value instanceof String s) {
            dos.writeByte(TAG_STRING);
            dos.writeUTF(s);
        } else if (value instanceof BigDecimal bd) {
            dos.writeByte(TAG_BIG_DECIMAL);
            dos.writeUTF(bd.toString());
        } else if (value instanceof Serializable ser) {
            dos.writeByte(TAG_OBJECT);
            // Serialize to a side buffer so the ObjectOutputStream close does
            // not close the shared DataOutputStream mid-record.
            ByteArrayOutputStream side = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(side)) {
                oos.writeObject(ser);
            }
            byte[] bytes = side.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value type for WAL DML payload: " + value.getClass().getName());
        }
    }

    private static Object readValue(DataInputStream dis) throws IOException {
        byte tag = dis.readByte();
        return switch (tag) {
            case TAG_NULL -> null;
            case TAG_BOOLEAN -> dis.readBoolean();
            case TAG_INT -> dis.readInt();
            case TAG_LONG -> dis.readLong();
            case TAG_DOUBLE -> dis.readDouble();
            case TAG_FLOAT -> dis.readFloat();
            case TAG_STRING -> dis.readUTF();
            case TAG_BIG_DECIMAL -> new BigDecimal(dis.readUTF());
            case TAG_OBJECT -> {
                int length = dis.readInt();
                if (length < 0) {
                    throw new IllegalArgumentException("Negative object length: " + length);
                }
                byte[] bytes = new byte[length];
                dis.readFully(bytes);
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                    yield ois.readObject();
                } catch (ClassNotFoundException e) {
                    throw new IOException("Unknown serialized class in DML payload", e);
                }
            }
            default -> throw new IllegalArgumentException("Unknown DML payload type tag: " + tag);
        };
    }

    /**
     * Returns the affected table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the raw row index.
     */
    public int getRowIndex() {
        return rowIndex;
    }

    /**
     * Returns the decoded row values.
     */
    public Map<String, Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "DmlPayload{table=" + tableName + ", rowIndex=" + rowIndex
                + ", columns=" + values.size() + '}';
    }
}
