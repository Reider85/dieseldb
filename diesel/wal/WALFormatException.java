package diesel.wal;

/**
 * Exception thrown when a WAL entry is structurally invalid or corrupt
 * (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>Thrown by {@link WALEntry#readFrom(java.nio.ByteBuffer)} when:
 * <ul>
 *   <li>the buffer holds fewer than {@link WALFormat#ENTRY_FIXED_HEADER_SIZE} header bytes</li>
 *   <li>flags or the reserved field are non-zero</li>
 *   <li>the opcode byte is unknown</li>
 *   <li>a declared image length is negative or exceeds the remaining buffer</li>
 *   <li>the entry padding is missing or non-zero</li>
 * </ul>
 *
 * <p>CRC mismatches throw the more specific {@link InvalidCRCException}.
 * Extends RuntimeException for convenience (no checked exceptions in the WAL layer),
 * mirroring {@code diesel.storage.page.PageFormatException}.
 */
public class WALFormatException extends RuntimeException {

    /**
     * Constructs a new WALFormatException with the specified detail message.
     *
     * @param message the detail message
     */
    public WALFormatException(String message) {
        super(message);
    }

    /**
     * Constructs a new WALFormatException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public WALFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
