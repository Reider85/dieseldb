package diesel.storage.json;

import java.io.IOException;

/**
 * Uniform parsing/streaming violation signalled by a {@code diesel.storage.json}
 * backend (depth limit, string-length limit, lenient JSON shapes such as
 * NaN/Infinity or comments). Subclasses {@link IOException} so callers in the
 * storage package wrap it with {@code file:line} coordinates automatically.
 */
public final class JsonStreamException extends IOException {

    public JsonStreamException(String message) {
        super(message);
    }

    public JsonStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    private static final long serialVersionUID = 1L;
}