package diesel.wal;

/**
 * Binary layout constants for WAL entries (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>Entry layout (big-endian, length-prefixed, padded to 8-byte alignment for
 * direct-IO compatibility):
 * <pre>
 * Offset  Size  Field
 * 0       8     lsn (monotonic)
 * 8       8     txid
 * 16      1     op ({@link WALOpcode} wire code)
 * 17      1     flags = 0 (reserved)
 * 18      2     reserved = 0
 * 20      4     beforeLen (0 = no before-image)
 * 24      4     afterLen (0 = no after-image)
 * 28      B     before-image (beforeLen bytes)
 * 28+B    A     after-image (afterLen bytes)
 * 28+B+A  4     crc32c (over bytes [0, 28+B+A), i.e. header + both images)
 * ...     P     zero padding to next 8-byte boundary
 * </pre>
 *
 * <p>The entry has no magic number: segment-level framing (magic, version,
 * segment LSN range) is introduced by the WALManager prompt (step 12). The
 * CRC32C scope deliberately excludes the padding.
 *
 * <p>Utility class — not instantiable.
 */
public final class WALFormat {

    /** Size of the fixed header before the variable-length images. */
    public static final int ENTRY_FIXED_HEADER_SIZE = 28;

    /** Size of the trailing CRC32C field. */
    public static final int CRC_SIZE = 4;

    /** Entries start at 8-byte boundaries (direct-IO alignment). */
    public static final int ALIGNMENT = 8;

    /** Offset of the LSN field. */
    public static final int OFFSET_LSN = 0;

    /** Offset of the txid field. */
    public static final int OFFSET_TXID = 8;

    /** Offset of the 1-byte opcode. */
    public static final int OFFSET_OP = 16;

    /** Offset of the reserved flags byte (must be 0). */
    public static final int OFFSET_FLAGS = 17;

    /** Offset of the reserved short (must be 0). */
    public static final int OFFSET_RESERVED = 18;

    /** Offset of the before-image length prefix. */
    public static final int OFFSET_BEFORE_LEN = 20;

    /** Offset of the after-image length prefix. */
    public static final int OFFSET_AFTER_LEN = 24;

    /** Offset where the variable-length images start. */
    public static final int OFFSET_IMAGES = ENTRY_FIXED_HEADER_SIZE;

    /** Smallest possible entry: empty images, header + CRC = 32 bytes (already 8-aligned). */
    public static final int MIN_ENTRY_SIZE = ENTRY_FIXED_HEADER_SIZE + CRC_SIZE;

    private WALFormat() {
        // utility class
    }

    /**
     * Returns the unpadded byte count of an entry with the given image sizes:
     * {@code ENTRY_FIXED_HEADER_SIZE + beforeLen + afterLen + CRC_SIZE}.
     *
     * @param beforeLen before-image length in bytes (>= 0)
     * @param afterLen after-image length in bytes (>= 0)
     * @return the raw entry size
     * @throws IllegalArgumentException if a length is negative or the total overflows int
     */
    public static int rawEntrySize(int beforeLen, int afterLen) {
        if (beforeLen < 0 || afterLen < 0) {
            throw new IllegalArgumentException("Negative image length: before=" + beforeLen + ", after=" + afterLen);
        }
        long raw = (long) ENTRY_FIXED_HEADER_SIZE + beforeLen + afterLen + CRC_SIZE;
        if (raw > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Entry too large: " + raw + " bytes");
        }
        return (int) raw;
    }

    /**
     * Returns the padded on-disk size of an entry with the given image sizes,
     * i.e. {@link #rawEntrySize} rounded up to the next {@link #ALIGNMENT}-byte boundary.
     *
     * @param beforeLen before-image length in bytes (>= 0)
     * @param afterLen after-image length in bytes (>= 0)
     * @return the aligned entry size (always >= 32 and a multiple of 8)
     * @throws IllegalArgumentException if a length is negative or the total overflows int
     */
    public static int entrySize(int beforeLen, int afterLen) {
        return alignedSize(rawEntrySize(beforeLen, afterLen));
    }

    /**
     * Rounds {@code size} up to the next 8-byte boundary.
     *
     * @param size the raw size (>= 0)
     * @return the aligned size
     * @throws IllegalArgumentException if size is negative or alignment would overflow int
     */
    public static int alignedSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Negative size: " + size);
        }
        long aligned = ((long) size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
        if (aligned > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Aligned size overflows int: " + aligned);
        }
        return (int) aligned;
    }

    /**
     * Returns the number of zero padding bytes appended after an entry of
     * {@code rawSize} bytes to reach 8-byte alignment.
     *
     * @param rawSize the unpadded entry size (>= 0)
     * @return padding bytes, 0..7
     * @throws IllegalArgumentException if rawSize is negative
     */
    public static int padding(int rawSize) {
        if (rawSize < 0) {
            throw new IllegalArgumentException("Negative raw size: " + rawSize);
        }
        int rem = rawSize % ALIGNMENT;
        return rem == 0 ? 0 : ALIGNMENT - rem;
    }
}
