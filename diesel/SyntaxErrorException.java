package diesel;

/**
 * Thrown when SQL syntax is invalid or a query cannot be parsed.
 */
public class SyntaxErrorException extends DieselException {

    public SyntaxErrorException(String message) {
        super(message);
    }
}
