package diesel;

/**
 * Thrown when an I/O operation fails (network, file system, serialization).
 */
public class DieselIOException extends DieselException {

    public DieselIOException(String message, Throwable cause) {
        super(message, cause);
    }
}
