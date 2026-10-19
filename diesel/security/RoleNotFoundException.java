package diesel.security;

import diesel.DieselException;

/** Thrown when a referenced role does not exist. */
public class RoleNotFoundException extends DieselException {

    public RoleNotFoundException(String message) {
        super(message);
    }
}
