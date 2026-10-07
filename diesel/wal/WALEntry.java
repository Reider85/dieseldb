package diesel.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * A single write-ahead log entry (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>Immutable value object with explicit binary serialization — no Java
 * object serialization. See {@link WALFormat} for the byte layout and
 * {@code doc/wal/format.md} for the full specification.
 *
 * <p>Serialization contract:
 * <ul>
 *   <li>{@link #writeTo(ByteBuffer)} — writes the entry (header + images + CRC32C +
 *       zero padding) at the buffer's current position, advancing it by
 *       {@link #encodedSize()}. Requires {@code remaining >= encodedSize()},
 *       otherwise {@link IllegalArgumentException}.</li>
 *   <li>{@link #readFrom(ByteBuffer)} — reads one entry at the current position
 *       (including padding), advancing it. Verifies CRC32C; on mismatch throws
 *       {@link InvalidCRCException}, on structural problems
 *       {@link WALFormatException}.</li>
 * </ul>
 *
 * <p>Image arrays are copied defensively on construction and on access.
 * {@code null} images are normalized to empty arrays (absent images).
 *
 * <p>Thread-safe: fully immutable.
 */
public final class WALEntry {

    private final long lsn;
    private final long txid;
    private final WALOpcode op;
    private final byte[] beforeImage;
    private final byte[] afterImage;

    /**
     * Creates an entry without before/after images (e.g. COMMIT/ABORT records).
     *
     * @param lsn monotonic log sequence number
     * @param txid transaction id
     * @param op the operation code
     * @throws IllegalArgumentException if op is null
     */
    public WALEntry(long lsn, long txid, WALOpcode op) {
        this(lsn, txid, op, null, null);
    }

    /**
     * Creates an entry with optional before/after images.
     *
     * @param lsn monotonic log sequence number
     * @param txid transaction id
     * @param op the operation code
     * @param beforeImage pre-image bytes, or {@code null} if absent
     * @param afterImage post-image bytes, or {@code null} if absent
     * @throws IllegalArgumentException if op is null
     */
    public WALEntry(long lsn, long txid, WALOpcode op, byte[] beforeImage, byte[] afterImage) {
        this.op = Objects.requireNonNull(op, "op");
        this.lsn = lsn;
        this.txid = txid;
        this.beforeImage = beforeImage == null ? new byte[0] : beforeImage.clone();
        this.afterImage = afterImage == null ? new byte[0] : afterImage.clone();
    }

    /**
     * Returns the on-disk size of this entry in bytes, including zero padding
     * to the 8-byte boundary.
     *
     * @return the encoded size (always a multiple of 8)
     */
    public int encodedSize() {
        return WALFormat.entrySize(beforeImage.length, afterImage.length);
    }

    /**
     * Writes this entry at the buffer's current position.
     *
     * @param dst the destination buffer
     * @throws IllegalArgumentException if {@code dst.remaining() < encodedSize()}
     */
    public void writeTo(ByteBuffer dst) {
        dst.order(ByteOrder.BIG_ENDIAN);
        int size = encodedSize();
        if (dst.remaining() < size) {
            throw new IllegalArgumentException("Buffer too small for WAL entry: remaining="
                    + dst.remaining() + ", needed=" + size);
        }

        byte[] header = new byte[WALFormat.ENTRY_FIXED_HEADER_SIZE];
        ByteBuffer hb = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        hb.putLong(lsn);
        hb.putLong(txid);
        hb.put(op.code());
        hb.put((byte) 0);                       // flags = 0
        hb.putShort((short) 0);                 // reserved = 0
        hb.putInt(beforeImage.length);
        hb.putInt(afterImage.length);

        int crc = computeCrc(header, beforeImage, afterImage);

        dst.put(header);
        dst.put(beforeImage);
        dst.put(afterImage);
        dst.putInt(crc);

        for (int i = WALFormat.padding(WALFormat.rawEntrySize(beforeImage.length, afterImage.length)); i > 0; i--) {
            dst.put((byte) 0);
        }
    }

    /**
     * Reads one entry (header + images + CRC32C + padding) at the buffer's
     * current position and advances the position past it.
     *
     * @param src the source buffer positioned at the start of an entry
     * @return the decoded entry
     * @throws WALFormatException if the entry is structurally invalid (truncated
     *         buffer, unknown opcode, bad lengths, non-zero reserved fields,
     *         missing/non-zero padding)
     * @throws InvalidCRCException if the stored CRC32C does not match the payload
     */
    public static WALEntry readFrom(ByteBuffer src) {
        src.order(ByteOrder.BIG_ENDIAN);

        if (src.remaining() < WALFormat.ENTRY_FIXED_HEADER_SIZE) {
            throw new WALFormatException("Buffer too short for WAL entry header: remaining="
                    + src.remaining() + ", needed=" + WALFormat.ENTRY_FIXED_HEADER_SIZE);
        }

        byte[] header = new byte[WALFormat.ENTRY_FIXED_HEADER_SIZE];
        src.get(header);
        ByteBuffer hb = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);

        long lsn = hb.getLong();
        long txid = hb.getLong();
        byte opCode = hb.get();
        byte flags = hb.get();
        short reserved = hb.getShort();
        int beforeLen = hb.getInt();
        int afterLen = hb.getInt();

        if (flags != 0) {
            throw new WALFormatException("Non-zero flags byte in WAL entry: " + flags);
        }
        if (reserved != 0) {
            throw new WALFormatException("Non-zero reserved field in WAL entry: " + reserved);
        }

        WALOpcode op;
        try {
            op = WALOpcode.fromByte(opCode);
        } catch (IllegalArgumentException e) {
            throw new WALFormatException("Unknown WAL opcode byte: " + (opCode & 0xFF), e);
        }

        if (beforeLen < 0 || afterLen < 0) {
            throw new WALFormatException("Negative image length: before=" + beforeLen + ", after=" + afterLen);
        }
        long payload = (long) beforeLen + afterLen;
        if (payload > src.remaining() - WALFormat.CRC_SIZE) {
            throw new WALFormatException("Truncated WAL entry: declared images " + payload
                    + " bytes + CRC exceed remaining " + src.remaining());
        }

        byte[] before = new byte[beforeLen];
        src.get(before);
        byte[] after = new byte[afterLen];
        src.get(after);
        int storedCrc = src.getInt();

        int computedCrc = computeCrc(header, before, after);
        if (computedCrc != storedCrc) {
            throw new InvalidCRCException("WAL entry CRC32C mismatch at lsn=" + lsn + " txid=" + txid,
                    computedCrc, storedCrc);
        }

        long raw = (long) WALFormat.ENTRY_FIXED_HEADER_SIZE + payload + WALFormat.CRC_SIZE;
        int pad = (int) ((WALFormat.ALIGNMENT - raw % WALFormat.ALIGNMENT) % WALFormat.ALIGNMENT);
        if (src.remaining() < pad) {
            throw new WALFormatException("Missing " + pad + " padding bytes after WAL entry lsn=" + lsn);
        }
        for (int i = 0; i < pad; i++) {
            byte b = src.get();
            if (b != 0) {
                throw new WALFormatException("Non-zero padding byte at index " + i + " after WAL entry lsn=" + lsn);
            }
        }

        return new WALEntry(lsn, txid, op, before, after);
    }

    /**
     * Serializes this entry into a new byte array of exactly
     * {@link #encodedSize()} bytes.
     *
     * @return the encoded entry
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(encodedSize());
        writeTo(buffer);
        return buffer.array();
    }

    /**
     * Decodes a single entry from the start of the given byte array.
     * Trailing bytes (e.g. a following entry) are ignored.
     *
     * @param data bytes containing at least one encoded entry
     * @return the decoded entry
     * @throws WALFormatException if the entry is structurally invalid
     * @throws InvalidCRCException if the stored CRC32C does not match
     */
    public static WALEntry fromBytes(byte[] data) {
        Objects.requireNonNull(data, "data");
        return readFrom(ByteBuffer.wrap(data));
    }

    private static int computeCrc(byte[] header, byte[] before, byte[] after) {
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
        if (before.length > 0) {
            crc.update(before, 0, before.length);
        }
        if (after.length > 0) {
            crc.update(after, 0, after.length);
        }
        return (int) crc.getValue();
    }

    public long getLsn() {
        return lsn;
    }

    public long getTxid() {
        return txid;
    }

    public WALOpcode getOp() {
        return op;
    }

    /** Returns a copy of the before-image; empty array if absent. */
    public byte[] getBeforeImage() {
        return beforeImage.clone();
    }

    /** Returns a copy of the after-image; empty array if absent. */
    public byte[] getAfterImage() {
        return afterImage.clone();
    }

    public boolean hasBeforeImage() {
        return beforeImage.length > 0;
    }

    public boolean hasAfterImage() {
        return afterImage.length > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WALEntry other)) {
            return false;
        }
        return lsn == other.lsn
                && txid == other.txid
                && op == other.op
                && Arrays.equals(beforeImage, other.beforeImage)
                && Arrays.equals(afterImage, other.afterImage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(lsn, txid, op);
        result = 31 * result + Arrays.hashCode(beforeImage);
        result = 31 * result + Arrays.hashCode(afterImage);
        return result;
    }

    @Override
    public String toString() {
        return String.format("WALEntry{lsn=%d, txid=%d, op=%s, before=%dB, after=%dB}",
                lsn, txid, op, beforeImage.length, afterImage.length);
    }
}
