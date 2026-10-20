package diesel.net;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Wire-protocol v2 framing for the DieselDB network layer (prompt4.md #27,
 * R3-007 step 1/3).
 *
 * <p>Frame layout (all integers big-endian):
 * <pre>
 *   Offset  Size  Field
 *   0       4     magic 'D','S','E','L'
 *   4       2     protocol version (u16) = 2
 *   6       2     message type id (u16)
 *   8       4     payload length N (i32, 0..MAX_PAYLOAD_BYTES)
 *   12      N     payload bytes
 *   12+N    4     CRC32 (u32) over bytes [4, 12+N) — version+type+length+payload
 * </pre>
 *
 * <p>This replaces the legacy Java native serialization protocol
 * ({@code ObjectInputStream.readObject}) which is an RCE vector: a
 * {@code DSEL}-prefixed frame is the only thing this class will parse, and
 * payloads are handled exclusively by explicit-field {@link MessageCodec}
 * implementations.
 *
 * <p>Version policy: version 1 (legacy Java serialization era) and any
 * version other than {@value #VERSION} is rejected with
 * {@link InvalidMessageException}.
 *
 * @see MessageCodec
 * @see InvalidMessageException
 */
public final class WireProtocol {

    /** ASCII magic bytes {@code DSEL} opening every v2 frame. */
    public static final byte[] MAGIC = {'D', 'S', 'E', 'L'};

    /** Current protocol version written into every frame. */
    public static final int VERSION = 2;

    /** High bit flag indicating the frame payload is compressed. */
    public static final int COMPRESSED_FLAG = 0x8000;

    /** Frame header size in bytes: magic(4) + version(2) + type(2) + length(4). */
    public static final int HEADER_SIZE = 12;

    /** CRC32 footer size in bytes. */
    public static final int FOOTER_SIZE = 4;

    /** Header + footer overhead per frame. */
    public static final int FRAME_OVERHEAD = HEADER_SIZE + FOOTER_SIZE;

    /**
     * Maximum accepted payload size (64 MiB): guards the decoder against a
     * forged length field causing an out-of-memory allocation.
     */
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private WireProtocol() {
    }

    /**
     * A decoded frame: the validated message type plus the raw payload bytes
     * (without header or CRC footer).
     *
     * @param type      the message type from the frame header
     * @param payload   the raw payload bytes
     * @param compressed whether the payload is compressed
     */
    public record Frame(MessageType type, byte[] payload, boolean compressed) {
    }

    /**
     * Encodes one frame to the given output.
     *
     * @param type      the message type, never null
     * @param payload   the payload bytes, never null (may be empty)
     * @param out      the destination stream
     * @throws IOException if the payload exceeds {@link #MAX_PAYLOAD_BYTES}
     *                     or the stream cannot be written
     */
    public static void encodeFrame(MessageType type, byte[] payload, DataOutput out)
            throws IOException {
        encodeFrame(type, payload, false, out);
    }

    /**
     * Encodes one frame to the given output.
     *
     * @param type      the message type, never null
     * @param payload   the payload bytes, never null (may be empty)
     * @param compressed whether the payload is compressed
     * @param out       the destination stream
     * @throws IOException if the payload exceeds {@link #MAX_PAYLOAD_BYTES}
     *                     or the stream cannot be written
     */
    public static void encodeFrame(MessageType type, byte[] payload, boolean compressed, DataOutput out)
            throws IOException {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IOException("Payload too large: " + payload.length
                    + " > " + MAX_PAYLOAD_BYTES);
        }
        int version = compressed ? VERSION | COMPRESSED_FLAG : VERSION;
        out.write(MAGIC);
        out.writeShort(version);
        out.writeShort(type.id());
        out.writeInt(payload.length);
        out.write(payload);
        out.writeInt(crcOf(version, type.id(), payload));
    }

    /**
     * Decodes one frame from the given input, validating magic, version,
     * type id, length bounds and the CRC32 footer.
     *
     * @param in the source stream
     * @return the decoded frame
     * @throws EOFException            if the stream closes cleanly between frames
     *                                 (first magic byte unavailable)
     * @throws InvalidMessageException on any framing error: bad magic,
     *                                 unsupported version, unknown type,
     *                                 impossible length, CRC mismatch, or a
     *                                 stream truncated mid-frame
     * @throws IOException             if the underlying stream fails
     */
    public static Frame decodeFrame(DataInput in) throws IOException {
        int first;
        try {
            first = in.readUnsignedByte();
        } catch (EOFException e) {
            // Clean connection close between frames — let callers detect it.
            throw e;
        }
        if (first != (MAGIC[0] & 0xFF)) {
            throw new InvalidMessageException(
                    "Bad magic byte 0x" + Integer.toHexString(first)
                            + " (expected 'D' — not a protocol v2 frame)");
        }
        byte[] restOfMagic = new byte[3];
        try {
            in.readFully(restOfMagic);
        } catch (EOFException e) {
            throw new InvalidMessageException("Truncated frame: magic", e);
        }
        if (!Arrays.equals(restOfMagic, 0, 3, MAGIC, 1, 4)) {
            throw new InvalidMessageException(
                    "Bad magic bytes: D"
                            + new String(restOfMagic, java.nio.charset.StandardCharsets.ISO_8859_1));
        }

        int version;
        short typeId;
        int length;
        try {
            version = in.readUnsignedShort();
            typeId = in.readShort();
            length = in.readInt();
        } catch (EOFException e) {
            throw new InvalidMessageException("Truncated frame: header", e);
        }

        int actualVersion = version & ~COMPRESSED_FLAG;
        if (actualVersion != VERSION) {
            throw new InvalidMessageException("Unsupported protocol version: "
                    + actualVersion + " (supported: " + VERSION + ")");
        }
        boolean compressed = (version & COMPRESSED_FLAG) != 0;
        MessageType type = MessageType.fromId(typeId);
        if (length < 0 || length > MAX_PAYLOAD_BYTES) {
            throw new InvalidMessageException("Invalid payload length: " + length
                    + " (allowed: 0.." + MAX_PAYLOAD_BYTES + ")");
        }

        byte[] payload = new byte[length];
        try {
            in.readFully(payload);
        } catch (EOFException e) {
            throw new InvalidMessageException(
                    "Truncated frame: payload " + payload.length + " bytes expected", e);
        }

        int expectedCrc;
        try {
            expectedCrc = in.readInt();
        } catch (EOFException e) {
            throw new InvalidMessageException("Truncated frame: CRC footer", e);
        }
        int actualCrc = crcOf(actualVersion, typeId, payload);
        if (actualCrc != expectedCrc) {
            throw new InvalidMessageException("CRC32 mismatch: expected 0x"
                    + Integer.toHexString(expectedCrc) + ", actual 0x"
                    + Integer.toHexString(actualCrc));
        }
        return new Frame(type, payload, compressed);
    }

    /**
     * Encodes a message with the codec into a payload and writes it as one
     * frame.
     *
     * @param message the message, never null
     * @param codec   the field codec, never null
     * @param out     the destination stream
     * @throws IOException on codec or stream failure
     */
    public static void writeMessage(Message message, MessageCodec codec, DataOutput out)
            throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
        try (DataOutputStream payloadOut = new DataOutputStream(buffer)) {
            codec.encode(message, payloadOut);
        }
        encodeFrame(message.getType(), buffer.toByteArray(), out);
    }

    /**
     * Reads one frame and decodes its payload with the codec.
     *
     * @param codec the field codec, never null
     * @param in    the source stream
     * @return the decoded message
     * @throws EOFException            if the stream closes cleanly between frames
     * @throws InvalidMessageException on framing errors or a payload type tag
     *                                 that disagrees with the frame header
     * @throws IOException             on codec or stream failure
     */
    public static Message readMessage(MessageCodec codec, DataInput in) throws IOException {
        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }
        Frame frame = decodeFrame(in);
        DataInputStream payloadIn = new DataInputStream(
                new ByteArrayInputStream(frame.payload()));
        Message message = codec.decode(payloadIn);
        if (message.getType() != frame.type()) {
            throw new InvalidMessageException("Payload type tag "
                    + message.getType() + " does not match frame header type "
                    + frame.type());
        }
        return message;
    }

    public static int crcOf(int version, short typeId, byte[] payload) {
        CRC32 crc = new CRC32();
        byte[] headerFields = new byte[8];
        // Use version without compressed flag for CRC calculation
        int cleanVersion = version & ~COMPRESSED_FLAG;
        headerFields[0] = (byte) (cleanVersion >>> 8);
        headerFields[1] = (byte) cleanVersion;
        headerFields[2] = (byte) (typeId >>> 8);
        headerFields[3] = (byte) typeId;
        int length = payload.length;
        headerFields[4] = (byte) (length >>> 24);
        headerFields[5] = (byte) (length >>> 16);
        headerFields[6] = (byte) (length >>> 8);
        headerFields[7] = (byte) length;
        crc.update(headerFields, 0, headerFields.length);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }
}
