package diesel;

import java.io.Serializable;
import java.util.List;

/**
 * Message for compression negotiation handshake between client and server.
 * Sent immediately after connection establishment before any queries.
 */
public class CompressionHandshakeMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean clientSupportsCompression;
    private final List<String> supportedAlgorithms;
    private final int preferredCompressionLevel;
    private final int compressionThresholdBytes;

    public CompressionHandshakeMessage(
            boolean clientSupportsCompression,
            List<String> supportedAlgorithms,
            int preferredCompressionLevel,
            int compressionThresholdBytes) {
        this.clientSupportsCompression = clientSupportsCompression;
        this.supportedAlgorithms = supportedAlgorithms;
        this.preferredCompressionLevel = preferredCompressionLevel;
        this.compressionThresholdBytes = compressionThresholdBytes;
    }

    public boolean isClientSupportsCompression() {
        return clientSupportsCompression;
    }

    public List<String> getSupportedAlgorithms() {
        return supportedAlgorithms;
    }

    public int getPreferredCompressionLevel() {
        return preferredCompressionLevel;
    }

    public int getCompressionThresholdBytes() {
        return compressionThresholdBytes;
    }

    @Override
    public String toString() {
        return "CompressionHandshakeMessage{" +
                "clientSupportsCompression=" + clientSupportsCompression +
                ", supportedAlgorithms=" + supportedAlgorithms +
                ", preferredCompressionLevel=" + preferredCompressionLevel +
                ", compressionThresholdBytes=" + compressionThresholdBytes +
                '}';
    }
}