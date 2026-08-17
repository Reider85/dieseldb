package diesel;

/**
 * Prompt 31 (java:S1192): SQL keyword string literals that repeat 3+ times in
 * the engine, extracted into named constants. Behaviour is identical to using
 * the inline literal - this class only centralises the spelling so a keyword
 * cannot drift between files. Regex fragments, error-message fragments and
 * single-occurrence keywords are deliberately left inline.
 */
public final class SqlKeywords {

    private SqlKeywords() {
    }

    public static final String SELECT = "SELECT";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String WHERE = "WHERE";
    public static final String GROUP_BY = "GROUP BY";
    public static final String ORDER_BY = "ORDER BY";
    public static final String LIMIT = "LIMIT";
    public static final String OFFSET = "OFFSET";
    public static final String JOIN = "JOIN";
    public static final String INNER_JOIN = "INNER JOIN";
    public static final String LEFT_JOIN = "LEFT JOIN";
    public static final String RIGHT_JOIN = "RIGHT JOIN";
    public static final String ON = "ON";
    public static final String AND = "AND";
    public static final String OR = "OR";
    public static final String NOT = "NOT";
    public static final String LIKE = "LIKE";
    public static final String NOT_LIKE = "NOT LIKE";
    public static final String AS = "AS";
    public static final String TRUE = "TRUE";
    public static final String FALSE = "FALSE";
    public static final String ASC = "ASC";
    public static final String VALUES = "VALUES";
    public static final String TABLE = "TABLE";
    public static final String HAVING = "HAVING";
    public static final String COUNT = "COUNT";
    public static final String SUM = "SUM";
    public static final String MIN = "MIN";
    public static final String MAX = "MAX";
    public static final String AVG = "AVG";
    public static final String SET = "SET";
    public static final String ANALYZE = "ANALYZE";
    public static final String EXPLAIN = "EXPLAIN";
    public static final String NULL = "NULL";
    public static final String INSERT_INTO = "INSERT INTO";
    public static final String COMMIT_TRANSACTION = "COMMIT TRANSACTION";
    public static final String ROLLBACK_TRANSACTION = "ROLLBACK TRANSACTION";
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String CREATE_INDEX = "CREATE INDEX";
    public static final String CREATE_UNIQUE_INDEX = "CREATE UNIQUE INDEX";
    public static final String CREATE_HASH_INDEX = "CREATE HASH INDEX";
    public static final String CREATE_UNIQUE_CLUSTERED_INDEX = "CREATE UNIQUE CLUSTERED INDEX";
}