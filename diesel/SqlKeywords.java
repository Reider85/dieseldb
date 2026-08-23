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
    public static final String COVERING = "COVERING";

    // Single keywords (used in SqlLexer and elsewhere)
    public static final String FROM = "FROM";
    public static final String INTO = "INTO";
    public static final String CREATE = "CREATE";
    public static final String INDEX = "INDEX";
    public static final String HASH = "HASH";
    public static final String UNIQUE = "UNIQUE";
    public static final String CLUSTERED = "CLUSTERED";
    public static final String PRIMARY = "PRIMARY";
    public static final String KEY = "KEY";
    public static final String SEQUENCE = "SEQUENCE";
    public static final String IN = "IN";
    public static final String IS = "IS";
    public static final String INNER = "INNER";
    public static final String LEFT = "LEFT";
    public static final String RIGHT = "RIGHT";
    public static final String OUTER = "OUTER";
    public static final String FULL = "FULL";
    public static final String CROSS = "CROSS";
    public static final String GROUP = "GROUP";
    public static final String BY = "BY";
    public static final String ORDER = "ORDER";
    public static final String DESC = "DESC";
    public static final String DISTINCT = "DISTINCT";
    public static final String BEGIN = "BEGIN";
    public static final String TRANSACTION = "TRANSACTION";
    public static final String COMMIT = "COMMIT";
    public static final String ROLLBACK = "ROLLBACK";
    public static final String ISOLATION = "ISOLATION";
    public static final String LEVEL = "LEVEL";
    public static final String AUTOCOMMIT = "AUTOCOMMIT";

    // Transaction commands
    public static final String DELETE_FROM = "DELETE FROM";
    public static final String BEGIN_TRANSACTION = "BEGIN TRANSACTION";
    public static final String START_TRANSACTION = "START TRANSACTION";
    public static final String BEGIN_TRANSACTION_ISOLATION_LEVEL = "BEGIN TRANSACTION ISOLATION LEVEL";
    public static final String START_TRANSACTION_ISOLATION_LEVEL = "START TRANSACTION ISOLATION LEVEL";

    // Isolation levels
    public static final String ISOLATION_LEVEL_READ_UNCOMMITTED = "ISOLATION LEVEL READ UNCOMMITTED";
    public static final String ISOLATION_LEVEL_READ_COMMITTED = "ISOLATION LEVEL READ COMMITTED";
    public static final String ISOLATION_LEVEL_REPEATABLE_READ = "ISOLATION LEVEL REPEATABLE READ";
    public static final String ISOLATION_LEVEL_SERIALIZABLE = "ISOLATION LEVEL SERIALIZABLE";
    public static final String SET_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
    public static final String SET_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    public static final String SET_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ";
    public static final String SET_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";

    // Join types
    public static final String LEFT_OUTER_JOIN = "LEFT OUTER JOIN";
    public static final String RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN";
    public static final String FULL_JOIN = "FULL JOIN";
    public static final String FULL_OUTER_JOIN = "FULL OUTER JOIN";
    public static final String LEFT_INNER_JOIN = "LEFT INNER JOIN";
    public static final String RIGHT_INNER_JOIN = "RIGHT INNER JOIN";
    public static final String CROSS_JOIN = "CROSS JOIN";

    // Column types
    public static final String TYPE_STRING = "STRING";
    public static final String TYPE_INTEGER = "INTEGER";
    public static final String TYPE_LONG = "LONG";
    public static final String TYPE_SHORT = "SHORT";
    public static final String TYPE_BYTE = "BYTE";
    public static final String TYPE_BIGDECIMAL = "BIGDECIMAL";
    public static final String TYPE_FLOAT = "FLOAT";
    public static final String TYPE_DOUBLE = "DOUBLE";
    public static final String TYPE_CHAR = "CHAR";
    public static final String TYPE_UUID = "UUID";
    public static final String TYPE_BOOLEAN = "BOOLEAN";
    public static final String TYPE_DATE = "DATE";
    public static final String TYPE_DATETIME = "DATETIME";
    public static final String TYPE_DATETIME_MS = "DATETIME_MS";

    // Constraint keywords
    public static final String PRIMARY_KEY = "PRIMARY KEY";
}