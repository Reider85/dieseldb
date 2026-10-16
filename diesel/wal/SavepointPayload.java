package diesel.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Payload of a WAL {@link WALOpcode#SAVEPOINT} or {@link WALOpcode#RELEASE_SAVEPOINT} record
 * (prompt4.md #26).
 *
 * <p>For SAVEPOINT: carries the savepoint name and sequence number for recovery context.
 * For RELEASE_SAVEPOINT: carries only the savepoint name (informational; recovery ignores).
 *
 * <pre>
 *   writeUTF  name
 *   long      sequence (SAVEPOINT only; RELEASE omits this field)
 * </pre>
 *
 * <p>Malformed payloads are reported as unchecked
 * {@link IllegalArgumentException} so the recovery phases can skip corrupt records
 * without aborting the pass.
 */
public final class SavepointPayload {

    private final String name;
    private final long sequence;

    /**
     * Creates a savepoint payload (for SAVEPOINT opcode).
     *
     * @param name     the savepoint name
     * @param sequence the sequence number (monotonic, from SavepointStack)
     */
    public SavepointPayload(String name, long sequence) {
        this.name = Objects.requireNonNull(name, "name");
        this.sequence = sequence;
    }

    /**
     * Creates a release savepoint payload (for RELEASE_SAVEPOINT opcode).
     *
     * @param name the savepoint name
     */
    public SavepointPayload(String name) {
        this(name, -1L); // -1 indicates release (no sequence)
    }

    /**
     * Serializes a savepoint payload (SAVEPOINT opcode).
     *
     * @param name     the savepoint name
     * @param sequence the sequence number
     * @return the encoded payload bytes
     * @throws IOException if the payload cannot be written
     */
    public static byte[] serialize(String name, long sequence) throws IOException {
        Objects.requireNonNull(name, "name");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeUTF(name);
        dos.writeLong(sequence);

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Serializes a release savepoint payload (RELEASE_SAVEPOINT opcode).
     *
     * @param name the savepoint name
     * @return the encoded payload bytes
     * @throws IOException if the payload cannot be written
     */
    public static byte[] serialize(String name) throws IOException {
        Objects.requireNonNull(name, "name");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeUTF(name);

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Deserializes a savepoint payload written by {@link #serialize(String, long)}.
     *
     * @param data the encoded payload bytes
     * @return the decoded payload
     * @throws IllegalArgumentException if the payload is truncated or malformed
     */
    public static SavepointPayload deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            String name = dis.readUTF();
            long sequence = dis.readLong();
            return new SavepointPayload(name, sequence);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Truncated savepoint payload (" + data.length + " bytes)", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed savepoint payload: " + e.getMessage(), e);
        }
    }

    /**
     * Deserializes a release savepoint payload written by {@link #serialize(String)}.
     *
     * @param data the encoded payload bytes
     * @return the decoded payload
     * @throws IllegalArgumentException if the payload is truncated or malformed
     */
    public static SavepointPayload deserializeRelease(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            String name = dis.readUTF();
            return new SavepointPayload(name);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Truncated release savepoint payload (" + data.length + " bytes)", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed release savepoint payload: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the savepoint name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the sequence number (returns -1 for release payloads).
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns whether this is a release payload (sequence = -1).
     */
    public boolean isRelease() {
        return sequence == -1L;
    }

    @Override
    public String toString() {
        return isRelease() 
                ? "SavepointPayload{name='" + name + "', release}"
                : "SavepointPayload{name='" + name + "', sequence=" + sequence + '}';
    }
}