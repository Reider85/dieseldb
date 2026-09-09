package diesel;

public class QuerySyntaxException extends DieselException {
    public QuerySyntaxException(String sql, String reason) {
        super(reason);
    }
}