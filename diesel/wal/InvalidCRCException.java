package diesel.wal;

/**
 * Exception thrown when a WAL entry fails CRC32C verification
 * (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>Raised by {@link WALEntry#readFrom(java.nio.ByteBuffer)} when the stored
 * checksum does not match the recomputed CRC32C of the entry payload —
 * a corrupted byte anywhere in the header or the before/after images.
 *
 * <p>A subclass of {@link WALFormatException} so callers can catch the format
 * family broadly or this specific checksum failure.
 */
public class InvalidCRCException extends WALFormatException {

    /**
     * Constructs a new InvalidCRCException with expected/actual checksum values.
     *
     * @param message the detail message (should include expected vs. actual values)
     * @param expected the CRC32C computed over the bytes actually read
     * @param actual the CRC32C stored in the entry
     */
    public InvalidCRCException(String message, int expected, int actual) {
        super(message + " (expected=0x" + Integer.toHexString(expected)
                + ", actual=0x" + Integer.toHexString(actual) + ")");
    }
}
