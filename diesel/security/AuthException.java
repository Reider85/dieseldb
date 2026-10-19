package diesel.security;

import diesel.DieselException;

/**
 * Raised when a credentials handshake fails: unknown account or wrong
 * password (prompt4 #31). Deliberately gives no hint which of the two was
 * wrong, so the message cannot be used to enumerate accounts.
 */
public class AuthException extends DieselException {

    /** Message used for every failed authentication attempt. */
    public static final String INVALID_CREDENTIALS = "Invalid user or password";

    public AuthException(String message) {
        super(message);
    }

    public AuthException(String message, Throwable cause) {
        super(message, cause);
    }
}
