package diesel.recovery;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import diesel.wal.WALFormatException;

/**
 * Immutable checkpoint record for ARIES recovery.
 * Contains lastLSN, active transaction IDs, and timestamp.
 * Binary format: DCPT + version + lastLSN + timestamp + txidCount + txids + CRC32C.
 */
public final class CheckpointRecord {
    public static final int MAGIC = 0x44504354; // "DCPT"
    public static final short VERSION = 1;
    private static final int HEADER_SIZE = 4 + 2 + 2 + 8 + 8 + 4; // magic + version + reserved + lastLSN + timestamp + txidCount
    private static final int TXID_SIZE = 8;
    public static final int MAX_TXIDS = 1_000_000;

    private final long lastLSN;
    private final long[] activeTxids;
    private final long timestampEpochMs;

    public CheckpointRecord(long lastLSN, long[] activeTxids, long timestampEpochMs) {
        if (activeTxids == null) {
            throw new IllegalArgumentException("activeTxids cannot be null");
        }
        if (activeTxids.length > MAX_TXIDS) {
            throw new IllegalArgumentException("Too many txids: " + activeTxids.length);
        }
        this.lastLSN = lastLSN;
        this.activeTxids = activeTxids.clone(); // defensive copy
        this.timestampEpochMs = timestampEpochMs;
    }

    public long getLastLSN() {
        return lastLSN;
    }

    public long[] getActiveTxids() {
        return activeTxids.clone(); // defensive copy
    }

    public int getActiveTxidCount() {
        return activeTxids.length;
    }

    public long getTimestampEpochMs() {
        return timestampEpochMs;
    }

    public int encodedSize() {
        return HEADER_SIZE + (activeTxids.length * TXID_SIZE) + 4; // + crc32c
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(encodedSize());
        writeTo(buffer);
        return buffer.array();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(MAGIC);
        buffer.putShort(VERSION);
        buffer.putShort((short) 0); // reserved
        buffer.putLong(lastLSN);
        buffer.putLong(timestampEpochMs);
        buffer.putInt(activeTxids.length);
        for (long txid : activeTxids) {
            buffer.putLong(txid);
        }
        
        // Compute CRC over all data before the CRC field
        CRC32C crc = new CRC32C();
        int dataLength = buffer.position() - 4; // exclude CRC field
        crc.update(buffer.array(), 0, dataLength);
        buffer.putInt((int) crc.getValue());
    }

    public static CheckpointRecord fromBytes(byte[] bytes) throws CheckpointFormatException {
        if (bytes == null) {
            throw new CheckpointFormatException("bytes cannot be null");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return readFrom(buffer);
    }

    public static CheckpointRecord readFrom(ByteBuffer buffer) throws CheckpointFormatException {
        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new CheckpointFormatException("Invalid checkpoint magic: 0x" + Integer.toHexString(magic));
        }

        short version = buffer.getShort();
        if (version != VERSION) {
            throw new CheckpointFormatException("Unsupported checkpoint version: " + version);
        }

        buffer.getShort(); // reserved

        long lastLSN = buffer.getLong();
        long timestampEpochMs = buffer.getLong();
        int txidCount = buffer.getInt();

        if (txidCount < 0 || txidCount > MAX_TXIDS) {
            throw new CheckpointFormatException("Invalid txid count: " + txidCount);
        }

        long[] activeTxids = new long[txidCount];
        for (int i = 0; i < txidCount; i++) {
            activeTxids[i] = buffer.getLong();
        }

        // Verify CRC - compute over all data before the CRC field
        int dataLength = buffer.position() - 4;
        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, dataLength);
        int expectedCrc = (int) crc.getValue();
        int actualCrc = buffer.getInt();
        if (expectedCrc != actualCrc) {
            throw new CheckpointFormatException("CRC mismatch: expected=0x" + Integer.toHexString(expectedCrc) + 
                                              ", actual=0x" + Integer.toHexString(actualCrc));
        }

        return new CheckpointRecord(lastLSN, activeTxids, timestampEpochMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CheckpointRecord that = (CheckpointRecord) o;
        if (lastLSN != that.lastLSN) return false;
        if (timestampEpochMs != that.timestampEpochMs) return false;
        return java.util.Arrays.equals(activeTxids, that.activeTxids);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(lastLSN);
        result = 31 * result + java.util.Arrays.hashCode(activeTxids);
        result = 31 * result + Long.hashCode(timestampEpochMs);
        return result;
    }

    @Override
    public String toString() {
        return "CheckpointRecord{" +
               "lastLSN=" + lastLSN +
               ", activeTxids=" + java.util.Arrays.toString(activeTxids) +
               ", timestampEpochMs=" + timestampEpochMs +
               '}';
    }
}