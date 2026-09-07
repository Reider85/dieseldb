package diesel;

/**
 * Prompt 45 (java:S1192): String literals that appear 3+ times across the
 * engine, extracted into named constants to prevent spelling drift and
 * reduce code duplication.
 */
public final class ErrorMessages {

    private ErrorMessages() {
    }

    public static final String TABLE_PREFIX = "Table ";
    public static final String DOES_NOT_EXIST = " does not exist";
    public static final String NOT_ATTACHED_TO_DB = " is not attached to a database";
    public static final String UNKNOWN_COLUMN_PREFIX = "Unknown column: ";
    public static final String NUMERIC_VALUE_PREFIX = "Numeric value '";
    public static final String TYPE_MISMATCH_SUFFIX = "' does not match column type: ";
    public static final String DUPLICATE_KEY_PREFIX = "Duplicate key violation: key '";
    public static final String ALREADY_EXISTS_SUFFIX = "' already exists";
    public static final String TABLE_NOT_FOUND_PREFIX = "Table not found: ";
    public static final String QUERY_NULL = "Query must not be null";
    public static final String UNSUPPORTED_OPERATOR_PREFIX = "Unsupported operator: ";

    public static final String NONE_FULL_SCAN = "none (full scan)";

    public static final String CONFIG_FILE = "config.properties";
    public static final String TABLE_EXTENSION = ".table";
    public static final String BIN_EXTENSION = ".bin";

    public static final String STAGE_JOIN = "join";
    public static final String STAGE_RESULT = "result";

    public static final String INDEX_BTREE = "BTREE";
    public static final String INDEX_HASH = "HASH";
    public static final String INDEX_UNIQUE = "UNIQUE";
    public static final String INDEX_COMPOSITE_BTREE = "COMPOSITE_BTREE";
    public static final String INDEX_COVERING_BTREE = "COVERING_BTREE";

    public static final String EXIT_COMMAND = "EXIT";

    // S1192 – regex patterns duplicated across QueryParser / SubqueryParser / Database
    public static final String CASE_INSENSITIVE_START_PATTERN = "(?i)^(";
    public static final String CASE_INSENSITIVE_GROUP_PATTERN = "(?i)(";
    public static final String SELECT_KEYWORD = "(SELECT";
    public static final String FROM_PATTERN = "(?i)FROM\\s+";

    // S1192 – error / status messages
    public static final String ERROR_NOT_CONNECTED = "Client is not connected: call connect() first";
    public static final String TRANSACTION_STARTED = "Transaction started: ";
    public static final String ERROR_PREFIX = "Error: ";
    public static final String UNBALANCED_PARENS_SUBQUERY = "Unbalanced parentheses in subquery: ";

    // S1192 – column / key message fragments (Table.java)
    public static final String COLUMN_PREFIX = "Column ";
    public static final String DUPLICATE_KEY_QUOTED = "Duplicate key '";
    public static final String FOUND_IN_COLUMN = "' found in column ";
    public static final String IN_COLUMN = " in column ";

    // S1192 – subquery prefix (SubqueryParser.java)
    public static final String SUBQUERY_PREFIX = "SUBQUERY_";

    // S1192 – parser tag names (QueryParser.java)
    public static final String TAG_QUOTED_STRING = "quotedString";
    public static final String TAG_OPEN_PAREN = "openParen";
    public static final String TAG_CLOSE_PAREN = "closeParen";

    // S1192 – performance logging (AggregateFunctions.java)
    public static final String MS_SPEEDUP = "ms, Speedup: ";
}
