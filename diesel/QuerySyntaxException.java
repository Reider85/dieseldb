package diesel;

public class QuerySyntaxException extends DieselException {
    public QuerySyntaxException(String sql, String reason) {
        super(reason);
    }

    public QuerySyntaxException(String sql, String reason, Throwable cause) {
        super(reason, cause);
    }
}