package diesel.net;

/**
 * Thrown when an incoming wire-protocol v2 frame is malformed: bad magic
 * bytes, unsupported protocol version, unknown message-type id, impossible
 * payload length, CRC32 mismatch, or a truncated stream (prompt4.md #27,
 * R3-007 step 1/3).
 *
 * <p>This is also the rejection path for Java-serialized payloads: a legacy
 * {@code ObjectOutputStream} stream never starts with the {@code DSEL} magic
 * and therefore fails here before any payload bytes are interpreted.
 *
 * @see WireProtocol
 */
public class InvalidMessageException extends WireException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an invalid-message exception with the given detail message.
     *
     * @param message the detail message
     */
    public InvalidMessageException(String message) {
        super(message);
    }

    /**
     * Creates an invalid-message exception with a detail message and cause.
     *
     * @param message the detail message
     * @param cause   the underlying cause (e.g. EOFException on truncation)
     */
    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
