package diesel.net;

import diesel.net.messages.CompressionHandshakeMessage;
import diesel.net.messages.CompressionHandshakeResponseMessage;
import diesel.storage.CompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression negotiation and byte-level (de)compression for the network
 * protocol (prompt4.md #29, R3-007 step 3/3).
 *
 * <p>Negotiation happens exactly once per connection: the client opens with a
 * {@link CompressionHandshakeMessage} advertising the algorithms it can decode,
 * the server answers with a {@link CompressionHandshakeResponseMessage} naming
 * the chosen codec, level and threshold. ZSTD is preferred, GZIP is the
 * fallback; {@code none} is negotiated when the intersection is empty or the
 * configured threshold is {@code <= 0}.
 *
 * <p>Whether a payload actually travels compressed is decided by the
 * {@link WireProtocol#COMPRESSED_FLAG} bit of the frame header, never by
 * re-measuring the payload: a compressed payload is typically far smaller than
 * the threshold that triggered it, so size-based detection would be wrong.
 */
public final class CompressionHandshakeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompressionHandshakeHandler.class);

    /** Server preference order: ZSTD first, GZIP as fallback. */
    private static final List<String> SUPPORTED_ALGORITHMS = Arrays.asList("ZSTD", "GZIP");

    private static final String ALGORITHM_ZSTD = "ZSTD";
    private static final String ALGORITHM_GZIP = "GZIP";
    private static final String ALGORITHM_NONE = "none";

    /** Algorithm name published in the handshake response when compression is off. */
    public static final String NONE = ALGORITHM_NONE;

    private final CompressionCodec chosenCodec;
    private final int actualLevel;
    private final int actualThreshold;

    /**
     * Creates a handler for the given client handshake.
     *
     * @param handshake        the client's compression handshake
     * @param configThreshold  configured threshold in bytes ({@code <= 0} disables compression)
     * @param configLevel      configured compression level
     */
    public CompressionHandshakeHandler(CompressionHandshakeMessage handshake,
                                       int configThreshold, int configLevel) {
        // A configured threshold <= 0 switches compression off for this server
        // regardless of what the client offers.
        String chosenAlgorithm = configThreshold <= 0
                ? ALGORITHM_NONE : negotiateAlgorithm(handshake);
        int level = negotiateLevel(chosenAlgorithm, handshake.getPreferredCompressionLevel(), configLevel);
        this.chosenCodec = createCodec(chosenAlgorithm, level);
        this.actualLevel = level;
        this.actualThreshold = negotiateThreshold(handshake.getCompressionThresholdBytes(), configThreshold);

        LOGGER.info("Compression negotiated: algorithm={}, level={}, threshold={} bytes",
                chosenCodec.name(), actualLevel, actualThreshold);
    }

    /**
     * Creates a handler with explicit settings (no handshake exchange).
     *
     * @param algorithm the algorithm name, {@code ZSTD}, {@code GZIP} or {@code none}
     * @param level     the compression level
     * @param threshold the payload threshold in bytes, {@code <= 0} disables compression
     */
    public CompressionHandshakeHandler(String algorithm, int level, int threshold) {
        String normalized = (algorithm == null || algorithm.isEmpty()) ? ALGORITHM_NONE : algorithm;
        if (threshold <= 0) {
            normalized = ALGORITHM_NONE;
        }
        this.chosenCodec = createCodec(normalized, level);
        this.actualLevel = ALGORITHM_NONE.equals(normalized) ? 0 : level;
        this.actualThreshold = threshold <= 0 ? 0 : threshold;
    }

    /**
     * Picks the first algorithm supported by both peers, in server preference
     * order, or {@code none} when the client opted out or the intersection is
     * empty.
     */
    private String negotiateAlgorithm(CompressionHandshakeMessage handshake) {
        if (handshake == null || !handshake.isClientSupportsCompression()) {
            return ALGORITHM_NONE;
        }
        List<String> clientAlgorithms = handshake.getSupportedAlgorithms();
        if (clientAlgorithms == null || clientAlgorithms.isEmpty()) {
            return ALGORITHM_NONE;
        }
        for (String candidate : SUPPORTED_ALGORITHMS) {
            for (String clientAlgorithm : clientAlgorithms) {
                if (candidate.equalsIgnoreCase(clientAlgorithm)) {
                    return candidate;
                }
            }
        }
        return ALGORITHM_NONE;
    }

    /** Clamps the negotiated level into the algorithm's valid range. */
    private int negotiateLevel(String algorithm, int clientLevel, int configLevel) {
        if (ALGORITHM_NONE.equals(algorithm)) {
            return 0;
        }
        if (ALGORITHM_GZIP.equals(algorithm)) {
            return 6;
        }
        int level = clientLevel > 0 ? clientLevel : configLevel;
        return Math.max(1, Math.min(22, level));
    }

    /**
     * Resolves the payload threshold. A non-positive configured threshold means
     * compression is switched off for this connection and overrides any client
     * preference; otherwise the client preference wins when it is positive.
     */
    private int negotiateThreshold(int clientThreshold, int configThreshold) {
        if (configThreshold <= 0) {
            return 0;
        }
        return clientThreshold > 0 ? clientThreshold : configThreshold;
    }

    private CompressionCodec createCodec(String algorithm, int level) {
        if (ALGORITHM_ZSTD.equalsIgnoreCase(algorithm)) {
            return new ZstdCompressionCodec(level);
        }
        if (ALGORITHM_GZIP.equalsIgnoreCase(algorithm)) {
            return new GzipCompressionCodec();
        }
        if (ALGORITHM_NONE.equalsIgnoreCase(algorithm)) {
            return new NoneCompressionCodec();
        }
        throw new IllegalArgumentException("Unsupported compression algorithm: " + algorithm);
    }

    /**
     * Returns {@code true} when a payload of the given size must be compressed.
     * A threshold of {@code 0} (compression negotiated off) never compresses.
     *
     * @param payloadSize the uncompressed payload size in bytes
     * @return {@code true} if the payload must be compressed
     */
    public boolean shouldCompress(int payloadSize) {
        return !chosenCodec.isNone() && actualThreshold > 0 && payloadSize > actualThreshold;
    }

    /** Returns {@code true} when a codec was negotiated (compression is on). */
    public boolean isCompressionEnabled() {
        return !chosenCodec.isNone();
    }

    /**
     * Compresses the payload with the negotiated codec. Never throws: on a
     * codec failure the original bytes are returned so the frame still
     * travels (uncompressed, without the compressed flag).
     *
     * @param payload the uncompressed payload
     * @return the compressed payload, or {@code payload} unchanged
     */
    public byte[] compress(byte[] payload) {
        if (!shouldCompress(payload.length)) {
            return payload;
        }
        long startTime = System.nanoTime();
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(Math.max(64, payload.length / 4));
            try (OutputStream compressed = chosenCodec.wrapOutputStream(buffer)) {
                compressed.write(payload);
            }
            byte[] result = buffer.toByteArray();
            long duration = System.nanoTime() - startTime;
            LOGGER.debug("Compressed {} bytes to {} bytes in {} us",
                    payload.length, result.length, duration / 1_000);
            return result;
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("Compression failed, sending payload uncompressed", e);
            return payload;
        }
    }

    /**
     * Decompresses a payload that arrived with
     * {@link WireProtocol#COMPRESSED_FLAG} set.
     *
     * @param payload the compressed payload
     * @return the original payload
     * @throws IOException if the payload cannot be decompressed
     */
    public byte[] decompress(byte[] payload) throws IOException {
        if (chosenCodec.isNone()) {
            throw new IOException("Received a compressed payload but no compression codec was negotiated");
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(Math.max(64, payload.length * 4));
        try (InputStream decompressed = chosenCodec.wrapInputStream(new ByteArrayInputStream(payload))) {
            byte[] chunk = new byte[8192];
            int read;
            while ((read = decompressed.read(chunk)) > 0) {
                buffer.write(chunk, 0, read);
            }
        }
        return buffer.toByteArray();
    }

    /** Returns the negotiated codec. */
    public CompressionCodec getChosenCodec() {
        return chosenCodec;
    }

    /** Returns the negotiated compression level. */
    public int getActualLevel() {
        return actualLevel;
    }

    /** Returns the negotiated payload threshold in bytes (0 = compression off). */
    public int getActualThreshold() {
        return actualThreshold;
    }

    /**
     * Builds the handshake response describing what was negotiated.
     *
     * @param transactionId the client transaction id to echo back, or null
     * @return the response message
     */
    public CompressionHandshakeResponseMessage createResponse(UUID transactionId) {
        return new CompressionHandshakeResponseMessage(
                true,
                chosenCodec.name(),
                actualLevel,
                actualThreshold,
                transactionId);
    }

    /** ZSTD codec backed by the zstd-jni streaming API. */
    private static final class ZstdCompressionCodec implements CompressionCodec {
        private final int level;

        ZstdCompressionCodec(int level) {
            this.level = level;
        }

        @Override
        public String name() {
            return ALGORITHM_ZSTD;
        }

        @Override
        public String suffix() {
            return ".zst";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new ZstdOutputStream(out, level);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new ZstdInputStream(in);
        }
    }

    /** GZIP codec backed by the JDK streaming API. */
    private static final class GzipCompressionCodec implements CompressionCodec {
        @Override
        public String name() {
            return ALGORITHM_GZIP;
        }

        @Override
        public String suffix() {
            return "";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) throws IOException {
            return new GZIPOutputStream(out);
        }

        @Override
        public InputStream wrapInputStream(InputStream in) throws IOException {
            return new GZIPInputStream(in);
        }
    }

    /** Identity codec negotiated when compression is switched off. */
    private static final class NoneCompressionCodec implements CompressionCodec {
        @Override
        public String name() {
            return ALGORITHM_NONE;
        }

        @Override
        public String suffix() {
            return "";
        }

        @Override
        public OutputStream wrapOutputStream(OutputStream out) {
            return out;
        }

        @Override
        public InputStream wrapInputStream(InputStream in) {
            return in;
        }

        @Override
        public boolean isNone() {
            return true;
        }
    }
}
