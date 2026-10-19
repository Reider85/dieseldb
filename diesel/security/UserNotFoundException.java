package diesel.security;

import diesel.DieselException;

/** Thrown when a referenced user does not exist. */
public class UserNotFoundException extends DieselException {

    public UserNotFoundException(String message) {
        super(message);
    }
}
