package diesel.net;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire-protocol v2 framing tests (prompt4.md #27, R3-007 step 1/3): frame
 * round-trip for every message type, CRC32 corruption detection, version
 * policy (v1 rejected, v2 accepted) and rejection of Java-serialized
 * payloads (the legacy RCE vector).
 */
@Tag("network")
public class WireProtocolTest {

    // ---------------------------------------------------------------- helpers

    private static byte[] encodeToBytes(MessageType type, byte[] payload) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            WireProtocol.encodeFrame(type, payload, out);
        }
        return buffer.toByteArray();
    }

    private static WireProtocol.Frame decodeFromBytes(byte[] frame) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame))) {
            return WireProtocol.decodeFrame(in);
        }
    }

    /** Builds a raw frame with explicit version/type/length and a valid CRC32. */
    private static byte[] craftFrame(int version, short typeId, byte[] payload) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.write(WireProtocol.MAGIC);
            out.writeShort(version);
            out.writeShort(typeId);
            out.writeInt(payload.length);
            out.write(payload);
            // Valid CRC over the crafted header fields + payload
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            byte[] header = new byte[8];
            header[0] = (byte) (version >>> 8);
            header[1] = (byte) version;
            header[2] = (byte) (typeId >>> 8);
            header[3] = (byte) typeId;
            header[4] = (byte) (payload.length >>> 24);
            header[5] = (byte) (payload.length >>> 16);
            header[6] = (byte) (payload.length >>> 8);
            header[7] = (byte) payload.length;
            crc.update(header, 0, header.length);
            crc.update(payload, 0, payload.length);
            out.writeInt((int) crc.getValue());
        }
        return buffer.toByteArray();
    }

    // ------------------------------------------------- round-trip: all types

    @Test
    void frameRoundTripsForEveryMessageType() throws IOException {
        for (MessageType type : MessageType.values()) {
            byte[] payload = ("payload-for-" + type).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] frame = encodeToBytes(type, payload);
            WireProtocol.Frame decoded = decodeFromBytes(frame);
            assertEquals(type, decoded.type(), "type mismatch for " + type);
            assertArrayEquals(payload, decoded.payload(), "payload mismatch for " + type);
        }
    }

    @Test
    void emptyPayloadRoundTrips() throws IOException {
        WireProtocol.Frame decoded = decodeFromBytes(
                encodeToBytes(MessageType.HEALTH_CHECK, new byte[0]));
        assertEquals(MessageType.HEALTH_CHECK, decoded.type());
        assertEquals(0, decoded.payload().length);
    }

    @Test
    void largePayloadRoundTrips() throws IOException {
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31);
        }
        WireProtocol.Frame decoded = decodeFromBytes(
                encodeToBytes(MessageType.QUERY_RESULT, payload));
        assertEquals(MessageType.QUERY_RESULT, decoded.type());
        assertArrayEquals(payload, decoded.payload());
    }

    // ------------------------------------------------------------ CRC32 gate

    @Test
    void corruptedPayloadByteRejected() throws IOException {
        byte[] frame = encodeToBytes(MessageType.QUERY, "SELECT 1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        frame[WireProtocol.HEADER_SIZE + 3] ^= 0x01; // flip one payload bit
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(frame));
        assertTrue(e.getMessage().contains("CRC32"), e.getMessage());
    }

    @Test
    void corruptedHeaderTypeByteRejected() throws IOException {
        byte[] frame = encodeToBytes(MessageType.PREPARE, new byte[4]);
        frame[6] ^= 0x01; // flip a bit in the type id field (covered by CRC)
        assertThrows(InvalidMessageException.class, () -> decodeFromBytes(frame));
    }

    @Test
    void corruptedMagicRejected() throws IOException {
        byte[] frame = encodeToBytes(MessageType.QUERY, new byte[0]);
        frame[0] ^= 0x01;
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(frame));
        assertTrue(e.getMessage().contains("magic"), e.getMessage());
    }

    // -------------------------------------------------------- version policy

    @Test
    void protocolVersion1Rejected() throws IOException {
        byte[] frame = craftFrame(1, MessageType.QUERY.id(), new byte[0]);
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(frame));
        assertTrue(e.getMessage().contains("version"), e.getMessage());
    }

    @Test
    void protocolVersion2Accepted() throws IOException {
        byte[] payload = "v2 ok".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] frame = craftFrame(2, MessageType.QUERY_RESULT.id(), payload);
        WireProtocol.Frame decoded = decodeFromBytes(frame);
        assertEquals(MessageType.QUERY_RESULT, decoded.type());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void protocolVersion3Rejected() throws IOException {
        byte[] frame = craftFrame(3, MessageType.QUERY.id(), new byte[0]);
        assertThrows(InvalidMessageException.class, () -> decodeFromBytes(frame));
    }

    // --------------------------------------------------------- header fields

    @Test
    void unknownMessageTypeIdRejected() throws IOException {
        byte[] frame = craftFrame(2, (short) 999, new byte[0]);
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(frame));
        assertTrue(e.getMessage().contains("Unknown message type"), e.getMessage());
    }

    @Test
    void negativeLengthRejected() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.write(WireProtocol.MAGIC);
            out.writeShort(2);
            out.writeShort(MessageType.QUERY.id());
            out.writeInt(-1);
        }
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(buffer.toByteArray()));
        assertTrue(e.getMessage().contains("length"), e.getMessage());
    }

    @Test
    void lengthOverMaxRejected() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.write(WireProtocol.MAGIC);
            out.writeShort(2);
            out.writeShort(MessageType.QUERY.id());
            out.writeInt(WireProtocol.MAX_PAYLOAD_BYTES + 1);
        }
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(buffer.toByteArray()));
        assertTrue(e.getMessage().contains("length"), e.getMessage());
    }

    @Test
    void truncatedPayloadRejected() throws IOException {
        byte[] frame = encodeToBytes(MessageType.QUERY_RESULT, new byte[64]);
        byte[] truncated = Arrays.copyOf(frame, frame.length - 10);
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> decodeFromBytes(truncated));
        assertTrue(e.getMessage().contains("Truncated"), e.getMessage());
    }

    @Test
    void cleanEofBetweenFramesThrowsEofException() {
        assertThrows(EOFException.class, () -> decodeFromBytes(new byte[0]));
    }

    // --------------------------------------- security: no Java serialization

    @Test
    void javaSerializedPayloadRejected() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
            oos.writeObject("gadget-payload");
        }
        byte[] legacyPayload = buffer.toByteArray();
        // ac ed (STREAM_MAGIC) can never pass the DSEL magic check
        assertThrows(InvalidMessageException.class, () -> decodeFromBytes(legacyPayload));
    }

    @Test
    void exceptionHierarchyIsIoSafe() {
        assertInstanceOf(IOException.class, new InvalidMessageException("x"));
        assertInstanceOf(WireException.class, new InvalidMessageException("x"));
    }

    // -------------------------------------------------- codec: write/read

    @Test
    void codecRoundTripViaWriteAndReadMessage() throws IOException {
        TestMessageCodec codec = new TestMessageCodec();
        TestMessage original = new TestMessage("SELECT * FROM t WHERE id = 1");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            WireProtocol.writeMessage(original, codec, out);
        }
        Message decoded;
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(buffer.toByteArray()))) {
            decoded = WireProtocol.readMessage(codec, in);
        }
        assertEquals(MessageType.QUERY, decoded.getType());
        assertEquals(original.text(), ((TestMessage) decoded).text());
    }

    @Test
    void payloadTypeTagMismatchRejected() throws IOException {
        // Payload tag claims QUERY while the frame header says QUERY_RESULT
        ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream();
        try (DataOutputStream payloadOut = new DataOutputStream(payloadBuffer)) {
            payloadOut.writeShort(MessageType.QUERY.id());
            payloadOut.writeUTF("bogus");
        }
        byte[] frame = encodeToBytes(MessageType.QUERY_RESULT, payloadBuffer.toByteArray());
        TestMessageCodec codec = new TestMessageCodec();
        InvalidMessageException e = assertThrows(InvalidMessageException.class,
                () -> {
                    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame))) {
                        WireProtocol.readMessage(codec, in);
                    }
                });
        assertTrue(e.getMessage().contains("does not match"), e.getMessage());
    }

    // ----------------------------------------------------------- test stubs

    /** Minimal explicit-field message used to exercise the codec path. */
    private record TestMessage(String text) implements Message {
        @Override
        public MessageType getType() {
            return MessageType.QUERY;
        }
    }

    /**
     * Minimal codec: payload = [type u16][UTF text]. Mirrors the layout real
     * message codecs will use in prompt4.md #28.
     */
    private static final class TestMessageCodec implements MessageCodec {
        @Override
        public void encode(Message message, DataOutput out) throws IOException {
            out.writeShort(message.getType().id());
            out.writeUTF(((TestMessage) message).text());
        }

        @Override
        public Message decode(DataInput in) throws IOException {
            MessageType type = MessageType.fromId(in.readShort());
            if (type != MessageType.QUERY) {
                throw new InvalidMessageException("Unexpected type: " + type);
            }
            return new TestMessage(in.readUTF());
        }
    }
}
