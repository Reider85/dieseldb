package diesel;

/**
 * Thrown when a SQL query cannot be parsed.
 */
public class QueryParseException extends DieselException {

    public QueryParseException(String message) {
        super(message);
    }

    public QueryParseException(String message, Throwable cause) {
        super(message, cause);
    }
}