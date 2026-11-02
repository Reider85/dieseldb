package diesel.replication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Wire framing for physical streaming replication (prompt4 #58, R3-016 step 1).
 *
 * <p>Frame layout (big-endian):
 * <pre>
 *   magic   'D' 'R' 'P' 'L'   4 bytes
 *   version u16 = 1            2 bytes
 *   type    u8                 1 byte
 *   length  i32 (payload)      4 bytes
 *   payload length bytes
 * </pre>
 *
 * <p>Message types:
 * <ul>
 *   <li>{@link #TYPE_BASE_BACKUP} (standby → master): request a base backup;
 *       answered by a sequence of {@link #TYPE_FILE} frames terminated by
 *       {@link #TYPE_BACKUP_END}.</li>
 *   <li>{@link #TYPE_FILE} (master → standby): one backup file
 *       ({@code nameLen i32, name UTF-8, bytes}).</li>
 *   <li>{@link #TYPE_BACKUP_END} (master → standby): {@code startLsn i64} —
 *       the LSN the streamed WAL must resume from.</li>
 *   <li>{@link #TYPE_START} (standby → master): {@code startLsn i64} — resume
 *       streaming from this LSN (inclusive).</li>
 *   <li>{@link #TYPE_WAL_ENTRY} (master → standby): one serialized
 *       {@link diesel.wal.WALEntry}.</li>
 *   <li>{@link #TYPE_HEARTBEAT} (master → standby): {@code lastLsn i64} —
 *       sent while the master is idle so the standby can detect a dead peer.</li>
 *   <li>{@link #TYPE_ACK} (standby → master): {@code appliedLsn i64} —
 *       reserved for synchronous replication (prompt 60); accepted and
 *       ignored by the sender in this step.</li>
 *   <li>{@link #TYPE_ERROR} (master → standby): UTF-8 message.</li>
 * </ul>
 *
 * <p>One base-backup exchange may run on one connection (which the standby
 * then closes), and streaming always runs on its own connection opened with
 * {@link #TYPE_START} — so a reconnect after a network drop simply re-opens a
 * connection and sends {@code START} with the resume LSN.
 */
public final class ReplicationWire {

    /** Frame magic: {@code DRPL}. */
    public static final byte[] MAGIC = {'D', 'R', 'P', 'L'};
    /** Protocol version written into every frame header. */
    public static final int VERSION = 1;
    /** Header size in bytes: magic(4) + version(2) + type(1) + length(4). */
    public static final int HEADER_SIZE = 11;

    /** Standby → master: no payload; answered with FILE* + BACKUP_END. */
    public static final int TYPE_BASE_BACKUP = 1;
    /** Master → standby: {@code nameLen i32, name UTF-8, content bytes}. */
    public static final int TYPE_FILE = 2;
    /** Master → standby: {@code startLsn i64}. */
    public static final int TYPE_BACKUP_END = 3;
    /** Standby → master: {@code startLsn i64}. */
    public static final int TYPE_START = 4;
    /** Master → standby: serialized {@code WALEntry.toBytes()}. */
    public static final int TYPE_WAL_ENTRY = 5;
    /** Master → standby: {@code lastLsn i64}. */
    public static final int TYPE_HEARTBEAT = 6;
    /** Standby → master: {@code appliedLsn i64} (reserved for prompt 60). */
    public static final int TYPE_ACK = 7;
    /** Either direction: UTF-8 error message. */
    public static final int TYPE_ERROR = 8;

    /**
     * Maximum payload size. A single backup file or WAL entry must fit into
     * one frame; larger transfers are rejected with an IOException (file-level
     * chunking is out of scope for this step).
     */
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private ReplicationWire() {
        throw new AssertionError("Utility class, do not instantiate");
    }

    /** One decoded frame: message type plus payload bytes (possibly empty). */
    public record Frame(int type, byte[] payload) {
    }

    /**
     * Writes one frame and flushes the underlying stream.
     *
     * @param out     the destination
     * @param type    one of the {@code TYPE_*} constants
     * @param payload the payload, never null (use {@code new byte[0]} for none)
     * @throws IOException if writing or flushing fails
     * @throws IllegalArgumentException if the payload exceeds {@link #MAX_PAYLOAD_BYTES}
     */
    public static void writeFrame(DataOutputStream out, int type, byte[] payload) throws IOException {
        Objects.requireNonNull(payload, "payload");
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Replication frame payload too large: "
                    + payload.length + " > " + MAX_PAYLOAD_BYTES);
        }
        out.write(MAGIC);
        out.writeShort(VERSION);
        out.writeByte(type);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /**
     * Reads one frame.
     *
     * @param in the source
     * @return the decoded frame
     * @throws EOFException if the peer closed the connection cleanly
     * @throws IOException if the header is malformed or I/O fails
     */
    public static Frame readFrame(DataInputStream in) throws IOException {
        byte[] magic = new byte[MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IOException("Bad replication frame magic: "
                    + new String(magic, StandardCharsets.ISO_8859_1));
        }
        int version = in.readUnsignedShort();
        if (version != VERSION) {
            throw new IOException("Unsupported replication protocol version: " + version);
        }
        int type = in.readUnsignedByte();
        int length = in.readInt();
        if (length < 0 || length > MAX_PAYLOAD_BYTES) {
            throw new IOException("Invalid replication frame length: " + length);
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new Frame(type, payload);
    }

    /** Encodes a big-endian long payload. */
    public static byte[] longPayload(long value) {
        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    /** Decodes a big-endian long payload; the frame must carry exactly 8 bytes. */
    public static long readLong(Frame frame) {
        byte[] payload = frame.payload();
        if (payload.length != 8) {
            throw new IllegalArgumentException("Expected 8-byte long payload, got " + payload.length);
        }
        long value = 0;
        for (byte b : payload) {
            value = (value << 8) | (b & 0xFFL);
        }
        return value;
    }

    /**
     * Encodes one backup file as a {@link #TYPE_FILE} payload:
     * {@code nameLen i32, name UTF-8, content bytes}.
     *
     * @param name    the path relative to the data directory, using {@code '/'} separators
     * @param content the file content
     * @return the encoded payload
     */
    public static byte[] encodeFile(String name, byte[] content) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[4 + nameBytes.length + content.length];
        writeInt(payload, 0, nameBytes.length);
        System.arraycopy(nameBytes, 0, payload, 4, nameBytes.length);
        System.arraycopy(content, 0, payload, 4 + nameBytes.length, content.length);
        return payload;
    }

    /**
     * Decodes a {@link #TYPE_FILE} payload.
     *
     * @param payload the frame payload
     * @return the file record (relative name plus content)
     * @throws IOException if the payload is truncated or malformed
     */
    public static BackupFile decodeFile(byte[] payload) throws IOException {
        if (payload.length < 4) {
            throw new IOException("Truncated backup file frame");
        }
        int nameLen = readInt(payload, 0);
        if (nameLen < 0 || 4L + nameLen > payload.length) {
            throw new IOException("Invalid backup file name length: " + nameLen);
        }
        String name = new String(payload, 4, nameLen, StandardCharsets.UTF_8);
        byte[] content = Arrays.copyOfRange(payload, 4 + nameLen, payload.length);
        return new BackupFile(name, content);
    }

    /** One file delivered by the base backup protocol. */
    public record BackupFile(String name, byte[] content) {
    }

    private static void writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static int readInt(byte[] source, int offset) {
        return ((source[offset] & 0xFF) << 24)
                | ((source[offset + 1] & 0xFF) << 16)
                | ((source[offset + 2] & 0xFF) << 8)
                | (source[offset + 3] & 0xFF);
    }
}
