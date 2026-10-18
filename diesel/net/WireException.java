package diesel.net;

import java.io.IOException;

/**
 * Base checked exception for all wire-protocol v2 failures (prompt4.md #27,
 * R3-007 step 1/3).
 *
 * <p>Extends {@link IOException} so transport callers that only handle IO
 * failures keep compiling, while protocol-aware callers can catch this type
 * to distinguish framing/codec errors from raw socket errors.
 *
 * @see InvalidMessageException
 */
public class WireException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a wire exception with the given detail message.
     *
     * @param message the detail message
     */
    public WireException(String message) {
        super(message);
    }

    /**
     * Creates a wire exception with a detail message and cause.
     *
     * @param message the detail message
     * @param cause   the underlying cause
     */
    public WireException(String message, Throwable cause) {
        super(message, cause);
    }
}
