package diesel.recovery;

import diesel.wal.WALFormatException;

/**
 * Exception thrown when checkpoint record or pointer is corrupt.
 */
public class CheckpointFormatException extends WALFormatException {
    public CheckpointFormatException(String message) {
        super(message);
    }

    public CheckpointFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}