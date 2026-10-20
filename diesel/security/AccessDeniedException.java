package diesel.security;

import diesel.DieselException;

/**
 * Thrown when a user attempts an operation without the required privilege
 * (prompt4 #32, R3-008 step 3/4).
 */
public class AccessDeniedException extends DieselException {
    
    /**
     * Constructs an access denied exception with a detailed message.
     *
     * @param message the reason for the denial (e.g., "User 'alice' has no SELECT privilege on users")
     */
    public AccessDeniedException(String message) {
        super(message);
    }
}