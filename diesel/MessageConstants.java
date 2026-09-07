package diesel;

/**
 * Prompt 9 (java:S1192): String literals that appear 3+ times across the
 * engine, extracted into named constants to prevent spelling drift and
 * reduce code duplication.
 */
public final class MessageConstants {

    private MessageConstants() {
    }

    // Parser token / node type names (QueryParser + SubqueryParser)
    public static final String TOKEN_LOGICAL_OPERATOR = "Logical Operator";
    public static final String TOKEN_LIKE_CONDITION = "Like Condition";
    public static final String TOKEN_CLAUSE = "clause";

    // SQL formatting fragments (QueryExecutor, SelectQuery, QueryParser, SubqueryParser)
    public static final String SQL_WHERE_SPACED = " WHERE ";
    public static final String SQL_FROM_SPACED = " FROM ";

    // EXPLAIN output fragments (ExplainQuery, SelectQuery)
    public static final String SQL_INDEX_PREFIX = "  Index: ";
    public static final String SQL_INDEX_ON = " index on ";

    // Error message prefixes (QueryParser, SubqueryParser)
    public static final String ERROR_BOOLEAN_VALUE_PREFIX = "Boolean value '";
}
