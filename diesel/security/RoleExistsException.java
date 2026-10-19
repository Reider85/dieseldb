package diesel.security;

import diesel.DieselException;

/** Thrown when CREATE ROLE targets a name that already exists. */
public class RoleExistsException extends DieselException {

    public RoleExistsException(String message) {
        super(message);
    }
}
