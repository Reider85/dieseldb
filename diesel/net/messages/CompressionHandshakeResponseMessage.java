package diesel.net.messages;

import diesel.net.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Compression negotiation handshake response, server -> client (prompt4.md #29).
 * 
 * <p>Binary layout:
 * <pre>
 *   [type: u16 COMPRESSION_HANDSHAKE_RESPONSE][success: byte][chosenAlgorithm: UTF]
 *   [actualLevel: byte][actualThreshold: int][txid: 16 bytes]
 * </pre>
 */
public final class CompressionHandshakeResponseMessage implements Message {
    private final boolean success;
    private final String chosenAlgorithm;
    private final int actualCompressionLevel;
    private final int actualCompressionThresholdBytes;
    private final UUID transactionId;

    /**
     * Creates a compression handshake response message.
     *
     * @param success                    whether the handshake succeeded
     * @param chosenAlgorithm            the chosen compression algorithm, or null if failed
     * @param actualCompressionLevel     the actual compression level to use
     * @param actualCompressionThresholdBytes the actual compression threshold in bytes
     * @param transactionId              the caller's transaction id, or null
     */
    public CompressionHandshakeResponseMessage(boolean success, 
                                            String chosenAlgorithm,
                                            int actualCompressionLevel,
                                            int actualCompressionThresholdBytes,
                                            UUID transactionId) {
        this.success = success;
        this.chosenAlgorithm = chosenAlgorithm;
        this.actualCompressionLevel = actualCompressionLevel;
        this.actualCompressionThresholdBytes = actualCompressionThresholdBytes;
        this.transactionId = transactionId;
    }

    /**
     * Returns whether the handshake succeeded.
     *
     * @return true if the handshake succeeded
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Returns the chosen compression algorithm.
     *
     * @return the chosen algorithm, or null if failed
     */
    public String getChosenAlgorithm() {
        return chosenAlgorithm;
    }

    /**
     * Returns the actual compression level to use.
     *
     * @return the actual compression level
     */
    public int getActualCompressionLevel() {
        return actualCompressionLevel;
    }

    /**
     * Returns the actual compression threshold in bytes.
     *
     * @return the actual compression threshold
     */
    public int getActualCompressionThresholdBytes() {
        return actualCompressionThresholdBytes;
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
        return MessageType.COMPRESSION_HANDSHAKE_RESPONSE;
    }

    /**
     * Writes this message to the given output stream.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(success ? 1 : 0);
        
        // Write chosen algorithm (null if failed)
        if (success && chosenAlgorithm != null) {
            out.writeUTF(chosenAlgorithm);
        } else {
            out.writeUTF("");
        }
        
        out.writeByte(actualCompressionLevel);
        out.writeInt(actualCompressionThresholdBytes);
        writeUUID(out, transactionId);
    }

    /**
     * Reads a compression handshake response message from the given input stream.
     *
     * @param in the input stream
     * @return the decoded compression handshake response message
     * @throws IOException if an I/O error occurs
     */
    public static CompressionHandshakeResponseMessage readFrom(DataInput in) throws IOException {
        boolean success = in.readByte() == 1;
        
        // Read chosen algorithm (empty string if failed or null)
        String chosenAlgorithm = null;
        if (success) {
            String algorithm = in.readUTF();
            if (!algorithm.isEmpty()) {
                chosenAlgorithm = algorithm;
            }
        }
        
        int actualCompressionLevel = in.readByte() & 0xFF;
        int actualCompressionThresholdBytes = in.readInt();
        UUID transactionId = readUUID(in);
        
        return new CompressionHandshakeResponseMessage(success, chosenAlgorithm,
                                                    actualCompressionLevel, 
                                                    actualCompressionThresholdBytes,
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