package diesel;

import diesel.net.CompressionHandshakeHandler;
import diesel.net.InvalidMessageException;
import diesel.net.Message;
import diesel.net.MessageCodec;
import diesel.net.WireProtocol;
import diesel.net.messages.CompressionHandshakeMessage;
import diesel.net.messages.CompressionHandshakeResponseMessage;
import diesel.net.messages.MessageCodecRegistry;
import diesel.net.messages.QueryMessage;
import diesel.net.messages.QueryResultMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * Raw v2 wire-protocol probe for network tests (prompt4.md #29): opens a
 * socket, runs the compression handshake and exchanges individual frames
 * without the full {@link DatabaseClient}, so a test can inspect the frame
 * header itself (compression flag, message type) or drive the server message
 * by message.
 *
 * <p>Uses {@code diesel.net.messages.*} v2 messages; unqualified message names
 * in this package resolve to the legacy {@code diesel.*} classes, so callers
 * must single-type-import or fully qualify v2 message types.
 */
final class WireProbe implements AutoCloseable {

    /** Client defaults advertised by {@link DatabaseClient}. */
    static final List<String> DEFAULT_ALGORITHMS = List.of("ZSTD", "GZIP");
    static final int DEFAULT_LEVEL = 6;
    static final int DEFAULT_THRESHOLD = 4096;

    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private CompressionHandshakeHandler handler;

    WireProbe(int port) throws IOException {
        this(port, 15000);
    }

    WireProbe(int port, int soTimeoutMs) throws IOException {
        socket = new Socket("localhost", port);
        socket.setSoTimeout(soTimeoutMs);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }

    /** Runs the default handshake (ZSTD+GZIP, level 6, threshold 4096). */
    CompressionHandshakeResponseMessage handshake() throws IOException {
        return handshake(new CompressionHandshakeMessage(
                true, DEFAULT_ALGORITHMS, DEFAULT_LEVEL, DEFAULT_THRESHOLD, null));
    }

    /**
     * Runs the handshake with an explicit client message and installs the
     * negotiated codec for frame decompression.
     *
     * @param message the handshake to send
     * @return the server's handshake response
     * @throws IOException if the stream fails or the answer is no response
     */
    CompressionHandshakeResponseMessage handshake(CompressionHandshakeMessage message) throws IOException {
        write(message);
        Message response = read();
        if (!(response instanceof CompressionHandshakeResponseMessage responseMessage)) {
            throw new InvalidMessageException("Expected a handshake response, got " + response.getType());
        }
        handler = new CompressionHandshakeHandler(
                responseMessage.getChosenAlgorithm(),
                responseMessage.getActualCompressionLevel(),
                responseMessage.getActualCompressionThresholdBytes());
        return responseMessage;
    }

    /** Sends a message as one plain (uncompressed) frame. */
    void write(Message message) throws IOException {
        byte[] payload = encode(message);
        WireProtocol.encodeFrame(message.getType(), payload, false, out);
        out.flush();
    }

    /**
     * Sends a message the way {@link DatabaseClient} does: compressed when the
     * negotiated handler asks for it and the result shrinks.
     */
    void writeCompressed(Message message) throws IOException {
        byte[] payload = encode(message);
        byte[] wire = payload;
        boolean compressed = false;
        if (handler != null && handler.shouldCompress(payload.length)) {
            byte[] candidate = handler.compress(payload);
            if (candidate.length < payload.length) {
                wire = candidate;
                compressed = true;
            }
        }
        WireProtocol.encodeFrame(message.getType(), wire, compressed, out);
        out.flush();
    }

    /** Reads the next frame exactly as it arrived (compression flag kept). */
    WireProtocol.Frame readFrame() throws IOException {
        return WireProtocol.decodeFrame(in);
    }

    /** Decompresses and decodes one frame into its message. */
    Message decode(WireProtocol.Frame frame) throws IOException {
        byte[] payload = frame.payload();
        if (frame.compressed()) {
            if (handler == null) {
                throw new InvalidMessageException("Compressed frame received before the handshake");
            }
            payload = handler.decompress(payload);
        }
        MessageCodec codec = MessageCodecRegistry.getCodec(frame.type());
        Message message = codec.decode(new DataInputStream(new ByteArrayInputStream(payload)));
        if (message.getType() != frame.type()) {
            throw new InvalidMessageException("Payload type tag " + message.getType()
                    + " does not match frame header type " + frame.type());
        }
        return message;
    }

    /** Reads and decodes the next message (decompressing when flagged). */
    Message read() throws IOException {
        return decode(readFrame());
    }

    /**
     * Decompresses and decodes one frame, rebuilding the Java result object
     * the way {@link DatabaseClient} does (row list, scalar or error String).
     */
    Object decodeValue(WireProtocol.Frame frame) throws IOException {
        Message message = decode(frame);
        if (!(message instanceof QueryResultMessage result)) {
            throw new InvalidMessageException("Expected a query result, got " + message.getType());
        }
        return DatabaseClient.toJavaResult(result);
    }

    /**
     * Reads the next query result and rebuilds the Java object exactly like
     * the client does. Server errors stay as {@code "Error: ..."} Strings —
     * this method never throws for them, so a test can assert on the message.
     */
    Object readValue() throws IOException {
        Message message = read();
        if (!(message instanceof QueryResultMessage result)) {
            throw new InvalidMessageException("Expected a query result, got " + message.getType());
        }
        return DatabaseClient.toJavaResult(result);
    }

    /** Sends a SQL query (no transaction id) and returns its result object. */
    Object query(String sql) throws IOException {
        write(new QueryMessage(sql, null));
        return readValue();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort teardown: the server closes its side anyway.
        }
    }

    private static byte[] encode(Message message) throws IOException {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream(64);
        try (DataOutputStream payloadOut = new DataOutputStream(buffer)) {
            MessageCodecRegistry.getCodec(message.getType()).encode(message, payloadOut);
        }
        return buffer.toByteArray();
    }
}
