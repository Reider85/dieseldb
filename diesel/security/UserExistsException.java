package diesel.security;

import diesel.DieselException;

/** Thrown when CREATE USER targets a name that already exists. */
public class UserExistsException extends DieselException {

    public UserExistsException(String message) {
        super(message);
    }
}
