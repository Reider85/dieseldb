package diesel;

import java.io.Serializable;

/**
 * Server response to compression handshake with agreed settings.
 */
public class CompressionHandshakeResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean compressionEnabled;
    private final String agreedAlgorithm;
    private final int agreedCompressionLevel;
    private final int agreedThresholdBytes;

    public CompressionHandshakeResponse(
            boolean compressionEnabled,
            String agreedAlgorithm,
            int agreedCompressionLevel,
            int agreedThresholdBytes) {
        this.compressionEnabled = compressionEnabled;
        this.agreedAlgorithm = agreedAlgorithm;
        this.agreedCompressionLevel = agreedCompressionLevel;
        this.agreedThresholdBytes = agreedThresholdBytes;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public String getAgreedAlgorithm() {
        return agreedAlgorithm;
    }

    public int getAgreedCompressionLevel() {
        return agreedCompressionLevel;
    }

    public int getAgreedThresholdBytes() {
        return agreedThresholdBytes;
    }

    @Override
    public String toString() {
        return "CompressionHandshakeResponse{" +
                "compressionEnabled=" + compressionEnabled +
                ", agreedAlgorithm='" + agreedAlgorithm + '\'' +
                ", agreedCompressionLevel=" + agreedCompressionLevel +
                ", agreedThresholdBytes=" + agreedThresholdBytes +
                '}';
    }
}