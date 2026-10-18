package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Compression negotiation handshake, client -> server (prompt4.md #28).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 COMPRESSION_HANDSHAKE][supportsCompression: byte][supportedAlgorithms: ×N]
 *   [preferredLevel: byte][thresholdBytes: int][txid: 16 bytes]
 * </pre>
 */
public final class CompressionHandshakeMessage implements Message {
    private final boolean supportsCompression;
    private final List<String> supportedAlgorithms;
    private final int preferredCompressionLevel;
    private final int compressionThresholdBytes;
    private final UUID transactionId;

    /**
     * Creates a compression handshake message.
     *
     * @param supportsCompression     whether the client supports compression
     * @param supportedAlgorithms     list of supported compression algorithms
     * @param preferredCompressionLevel the preferred compression level (1-9)
     * @param compressionThresholdBytes the compression threshold in bytes
     * @param transactionId           the caller's transaction id, or null
     */
    public CompressionHandshakeMessage(boolean supportsCompression, 
                                    List<String> supportedAlgorithms,
                                    int preferredCompressionLevel,
                                    int compressionThresholdBytes,
                                    UUID transactionId) {
        this.supportsCompression = supportsCompression;
        this.supportedAlgorithms = supportedAlgorithms;
        this.preferredCompressionLevel = preferredCompressionLevel;
        this.compressionThresholdBytes = compressionThresholdBytes;
        this.transactionId = transactionId;
    }

    /**
     * Returns whether the client supports compression.
     *
     * @return true if compression is supported
     */
    public boolean isClientSupportsCompression() {
        return supportsCompression;
    }

    /**
     * Returns the list of supported compression algorithms.
     *
     * @return list of supported algorithms
     */
    public List<String> getSupportedAlgorithms() {
        return supportedAlgorithms;
    }

    /**
     * Returns the preferred compression level.
     *
     * @return the preferred compression level
     */
    public int getPreferredCompressionLevel() {
        return preferredCompressionLevel;
    }

    /**
     * Returns the compression threshold in bytes.
     *
     * @return the compression threshold
     */
    public int getCompressionThresholdBytes() {
        return compressionThresholdBytes;
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
        return MessageType.COMPRESSION_HANDSHAKE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(supportsCompression ? 1 : 0);
        
        // Write supported algorithms
        out.writeShort(supportedAlgorithms.size());
        for (String algorithm : supportedAlgorithms) {
            out.writeUTF(algorithm);
        }
        
        out.writeByte(preferredCompressionLevel);
        out.writeInt(compressionThresholdBytes);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a compression handshake message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded compression handshake message
     * @throws IOException if an I/O error occurs
     */
    public static CompressionHandshakeMessage readFrom(DataInput in) throws IOException {
        boolean supportsCompression = in.readByte() == 1;
        
        // Read supported algorithms
        int algorithmCount = in.readShort();
        List<String> supportedAlgorithms = new java.util.ArrayList<>(algorithmCount);
        for (int i = 0; i < algorithmCount; i++) {
            supportedAlgorithms.add(in.readUTF());
        }
        
        int preferredCompressionLevel = in.readByte() & 0xFF;
        int compressionThresholdBytes = in.readInt();
        UUID transactionId = readUUID(in);
        
        return new CompressionHandshakeMessage(supportsCompression, supportedAlgorithms,
                                            preferredCompressionLevel, compressionThresholdBytes,
                                            transactionId);
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