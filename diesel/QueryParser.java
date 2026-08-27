package diesel;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Parser that turns a SQL string into a {@link Query} execution object.
 *
 * <p>The parser normalizes the input (uppercasing keywords while preserving
 * quoted identifiers), strips surrounding parentheses, and dispatches on the
 * leading statement keyword: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE,
 * CREATE INDEX variants, BEGIN/COMMIT/ROLLBACK TRANSACTION, SET AUTOCOMMIT and
 * SET TRANSACTION ISOLATION LEVEL. Complex SELECTs are further decomposed by
 * the lexer-based {@link #parse} path, and queries containing subqueries are
 * delegated to {@link SubqueryParser}.
 *
 * <p>Example:
 * <pre>{@code
 * Query<?> query = new QueryParser().parse("SELECT ID FROM USERS WHERE NAME = 'Alice'", database);
 * Object result = query.execute(database.getTable("USERS"));
 * }</pre>
 *
 * @see Query
 * @see SubqueryParser
 * @see Database
 */
class QueryParser {
    private static final Logger LOGGER;

    static {
        // Инициализация логгера
        LOGGER = Logger.getLogger(QueryParser.class.getName());
        // Отключение родительских обработчиков для избежания дублирования логов
        LOGGER.setUseParentHandlers(false);
        // Добавление ConsoleHandler
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL); // Устанавливаем минимальный уровень для обработчика
        LOGGER.addHandler(handler);

        // Загрузка уровня логирования из config.properties
        Properties props = new Properties();
        try (InputStream input = QueryParser.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                LOGGER.warning("config.properties not found, using default logging level INFO");
                LOGGER.setLevel(Level.INFO);
            } else {
                props.load(input);
                String logLevelStr = props.getProperty("logging.level.diesel", "INFO").toUpperCase();
                try {
                    Level logLevel = Level.parse(logLevelStr);
                    LOGGER.setLevel(logLevel);
                    handler.setLevel(logLevel);
                    LOGGER.info("Logging level set to " + logLevelStr + " from config.properties");
                } catch (IllegalArgumentException e) {
                    LOGGER.warning("Invalid logging level '" + logLevelStr + "' in config.properties, defaulting to INFO");
                    LOGGER.setLevel(Level.INFO);
                    handler.setLevel(Level.INFO);
                }
            }
        } catch (IOException e) {
            LOGGER.warning("Failed to load config.properties, defaulting to INFO: " + e.getMessage());
            LOGGER.setLevel(Level.INFO);
            handler.setLevel(Level.INFO);
        }
    }
    static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final DateTimeFormatter DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final String QUOTED_IDENTIFIER_PATTERN = "\"[^\"]*\"";
    private static final String SIMPLE_IDENTIFIER_PATTERN = "[a-zA-Z_]\\w*";
    private static final String IDENTIFIER_PATTERN = "(?:" + QUOTED_IDENTIFIER_PATTERN + "|" + SIMPLE_IDENTIFIER_PATTERN + ")";
    private static final String QUALIFIED_IDENTIFIER_PATTERN = IDENTIFIER_PATTERN + "(?:\\." + IDENTIFIER_PATTERN + ")*+";

    enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS,
        IN, LIKE, NOT_LIKE, IS_NULL, IS_NOT_NULL
    }

    enum JoinType {
        INNER, LEFT_INNER, RIGHT_INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }

    // Вспомогательный класс для токенов
    enum TokenType {
        CONDITION,
        LOGICAL_OPERATOR
    }

    static class SubQuery {
        Query<?> query;
        String alias;

        SubQuery(Query<?> query, String alias) {
            this.query = query;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return "(" + query.toString() + ")" + (alias != null ? " AS " + alias : "");
        }
    }

    static class Condition {
        String column;
        Object value;
        String rightColumn;
        List<Object> inValues;
        Set<Object> inValueSet;
        SubQuery subQuery;
        Operator operator;
        String conjunction;
        boolean not;
        List<Condition> subConditions;

        Condition(String column, Object value, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = value;
            this.rightColumn = null;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = null;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(String column, List<Object> inValues, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.rightColumn = null;
            this.inValues = inValues;
            this.inValueSet = inValues == null ? null : new HashSet<>(inValues);
            this.subQuery = null;
            this.operator = Operator.IN;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(List<Condition> subConditions, String conjunction, boolean not) {
            this.column = null;
            this.value = null;
            this.rightColumn = null;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = null;
            this.operator = null;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = subConditions;
        }

        Condition(String column, String rightColumn, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.rightColumn = rightColumn;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = null;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(String column, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.rightColumn = null;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = null;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(String column, SubQuery subQuery, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.rightColumn = null;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = subQuery;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(String column, SubQuery subQuery, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.rightColumn = null;
            this.inValues = null;
            this.inValueSet = null;
            this.subQuery = subQuery;
            this.operator = Operator.IN;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        boolean isGrouped() {
            return subConditions != null;
        }

        boolean isInOperator() {
            return operator == Operator.IN;
        }

        boolean isColumnComparison() {
            return rightColumn != null;
        }

        boolean isNullOperator() {
            return operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL;
        }

        boolean isSubQueryCondition() {
            return subQuery != null;
        }

        @Override
        public String toString() {
            if (isGrouped()) {
                String subCondStr = subConditions.stream()
                        .map(Condition::toString)
                        .collect(Collectors.joining(" "));
                return (not ? "NOT " : "") + "(" + subCondStr + ")" + (conjunction != null ? " " + conjunction : "");
            }
            if (isInOperator()) {
                if (subQuery != null) {
                    return (not ? "NOT " : "") + column + " IN " + subQuery.toString() + (conjunction != null ? " " + conjunction : "");
                }
                String valuesStr = inValues.stream()
                        .map(v -> v instanceof String ? "'" + v + "'" : v.toString())
                        .collect(Collectors.joining(", "));
                return (not ? "NOT " : "") + column + " IN (" + valuesStr + ")" + (conjunction != null ? " " + conjunction : "");
            }
            if (isColumnComparison()) {
                String operatorStr = switch (operator) {
                    case LIKE -> SqlKeywords.LIKE;
                    case NOT_LIKE -> SqlKeywords.NOT_LIKE;
                    default -> operator.toString();
                };
                return (not ? "NOT " : "") + column + " " + operatorStr + " " + rightColumn + (conjunction != null ? " " + conjunction : "");
            }
            if (isNullOperator()) {
                String operatorStr = operator == Operator.IS_NULL ? "IS NULL" : "IS NOT NULL";
                return (not ? "NOT " : "") + column + " " + operatorStr + (conjunction != null ? " " + conjunction : "");
            }
            if (isSubQueryCondition()) {
                String operatorStr = switch (operator) {
                    case LIKE -> SqlKeywords.LIKE;
                    case NOT_LIKE -> SqlKeywords.NOT_LIKE;
                    default -> operator.toString();
                };
                return (not ? "NOT " : "") + column + " " + operatorStr + " " + subQuery.toString() + (conjunction != null ? " " + conjunction : "");
            }
            String operatorStr = switch (operator) {
                case LIKE -> SqlKeywords.LIKE;
                case NOT_LIKE -> SqlKeywords.NOT_LIKE;
                default -> operator.toString();
            };
            return (not ? "NOT " : "") + column + " " + operatorStr + " " + (value instanceof String ? "'" + value + "'" : value) + (conjunction != null ? " " + conjunction : "");
        }
    }

    static class JoinInfo {
        String tableName;
        String alias;
        String leftColumn;
        String rightColumn;
        String originalTable;
        JoinType joinType;
        List<Condition> onConditions;

        JoinInfo(String originalTable, String tableName, String alias, String leftColumn, String rightColumn, JoinType joinType) {
            this(originalTable, tableName, alias, leftColumn, rightColumn, joinType, new ArrayList<>());
        }

        JoinInfo(String originalTable, String tableName, String alias, String leftColumn, String rightColumn, JoinType joinType, List<Condition> onConditions) {
            this.originalTable = originalTable;
            this.tableName = tableName;
            this.alias = alias;
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
            this.joinType = joinType;
            this.onConditions = onConditions;
        }

        @Override
        public String toString() {
            return "JoinInfo{originalTable=" + originalTable + ", table=" + tableName +
                    ", alias=" + alias + ", leftColumn=" + leftColumn + ", rightColumn=" + rightColumn +
                    ", joinType=" + joinType + ", onConditions=" + onConditions + "}";
        }
    }

    static class OrderByInfo {
        String column;
        boolean ascending;

        OrderByInfo(String column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        @Override
        public String toString() {
            return column + (ascending ? " ASC" : " DESC");
        }
    }

    static class AggregateFunction {
        String functionName;
        String column;
        SubQuery subQuery;
        String alias;

        AggregateFunction(String functionName, String column, String alias) {
            this.functionName = functionName.toUpperCase();
            this.column = column;
            this.subQuery = null;
            this.alias = alias;
        }

        AggregateFunction(String functionName, SubQuery subQuery, String alias) {
            this.functionName = functionName.toUpperCase();
            this.column = null;
            this.subQuery = subQuery;
            this.alias = alias;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            sb.append(column == null ? (subQuery != null ? subQuery.toString() : "*") : column);
            sb.append(")");
            if (alias != null) {
                sb.append(" AS ").append(alias);
            }
            return sb.toString();
        }
    }

    static class HavingCondition {
        AggregateFunction aggregate;
        Operator operator;
        Object value;
        String conjunction;
        boolean not;
        List<HavingCondition> subConditions;

        HavingCondition(AggregateFunction aggregate, Operator operator, Object value, String conjunction, boolean not) {
            this.aggregate = aggregate;
            this.operator = operator;
            this.value = value;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        HavingCondition(List<HavingCondition> subConditions, String conjunction, boolean not) {
            this.aggregate = null;
            this.operator = null;
            this.value = null;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = subConditions;
        }

        boolean isGrouped() {
            return subConditions != null;
        }

        @Override
        public String toString() {
            if (isGrouped()) {
                String subCondStr = subConditions.stream()
                        .map(HavingCondition::toString)
                        .collect(Collectors.joining(" "));
                return (not ? "NOT " : "") + "(" + subCondStr + ")" + (conjunction != null ? " " + conjunction : "");
            }
            String operatorStr = switch (operator) {
                case EQUALS -> "=";
                case NOT_EQUALS -> "!=";
                case LESS_THAN -> "<";
                case GREATER_THAN -> ">";
                case LESS_THAN_OR_EQUALS -> "<=";
                case GREATER_THAN_OR_EQUALS -> ">=";
                default -> operator.toString();
            };
            return (not ? "NOT " : "") + aggregate.toString() + " " + operatorStr + " " +
                    (value instanceof String ? "'" + value + "'" : value) +
                    (conjunction != null ? " " + conjunction : "");
        }
    }


    // Вспомогательный класс для хранения результатов парсинга элементов SELECT
    /**
     * Holds the parsed items of a SELECT clause: plain columns, aggregate
     * functions, scalar subqueries and their aliases.
     */
    public static class SelectItems {
        List<String> columns;
        List<AggregateFunction> aggregates;
        List<SubQuery> subQueries;
        Map<String, String> columnAliases;

        SelectItems(List<String> columns, List<AggregateFunction> aggregates, List<SubQuery> subQueries,
                    Map<String, String> columnAliases) {
            this.columns = columns;
            this.aggregates = aggregates;
            this.subQueries = subQueries;
            this.columnAliases = columnAliases;
        }
    }

    // Вспомогательный класс для хранения результатов парсинга таблиц и соединений
    /**
     * Holds the parsed FROM clause of a SELECT: the main table, its alias,
     * the list of joins, and the combined column types of all involved tables.
     */
    public static class TableJoins {
        String tableName;
        String tableAlias;
        List<JoinInfo> joins;
        Map<String, String> tableAliases;
        Map<String, Class<?>> combinedColumnTypes;
        Table derivedMainTable;

        TableJoins(String tableName, String tableAlias, List<JoinInfo> joins, Map<String, String> tableAliases,
                   Map<String, Class<?>> combinedColumnTypes) {
            this(tableName, tableAlias, joins, tableAliases, combinedColumnTypes, null);
        }

        TableJoins(String tableName, String tableAlias, List<JoinInfo> joins, Map<String, String> tableAliases,
                   Map<String, Class<?>> combinedColumnTypes, Table derivedMainTable) {
            this.tableName = tableName;
            this.tableAlias = tableAlias;
            this.joins = joins;
            this.tableAliases = tableAliases;
            this.combinedColumnTypes = combinedColumnTypes;
            this.derivedMainTable = derivedMainTable;
        }
    }

    // Вспомогательный класс для хранения дополнительных клауз
    /**
     * Holds the trailing clauses of a SELECT: WHERE conditions, GROUP BY
     * columns, HAVING conditions, ORDER BY list, LIMIT/OFFSET and subquery
     * GROUP BY expressions.
     */
    public static class AdditionalClauses {
        List<Condition> conditions;
        List<String> groupBy;
        List<HavingCondition> havingConditions;
        List<OrderByInfo> orderBy;
        Integer limit;
        Integer offset;
        Map<String, String> groupBySubQueries;

        AdditionalClauses(List<Condition> conditions, List<String> groupBy, List<HavingCondition> havingConditions,
                          List<OrderByInfo> orderBy, Integer limit, Integer offset) {
            this(conditions, groupBy, havingConditions, orderBy, limit, offset, new HashMap<>());
        }

        AdditionalClauses(List<Condition> conditions, List<String> groupBy, List<HavingCondition> havingConditions,
                          List<OrderByInfo> orderBy, Integer limit, Integer offset, Map<String, String> groupBySubQueries) {
            this.conditions = conditions;
            this.groupBy = groupBy;
            this.havingConditions = havingConditions;
            this.orderBy = orderBy;
            this.limit = limit;
            this.offset = offset;
            this.groupBySubQueries = groupBySubQueries != null ? groupBySubQueries : new HashMap<>();
        }
    }

    static class OperatorInfo {
        String operator;
        int index;
        int endIndex;

        OperatorInfo(String operator, int index, int endIndex) {
            this.operator = operator;
            this.index = index;
            this.endIndex = endIndex;
        }
    }

    /**
     * A single lexer token: either a {@link TokenType#CONDITION} or a
     * {@link TokenType#LOGICAL_OPERATOR} with its text value.
     */
    public static class Token {
        final TokenType type;
        final String value;

        Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Token{type=" + type + ", value='" + value + "'}'}";
        }
    }

    /**
     * Converts a SQL LIKE pattern into an anchored regular expression.
     * {@code %} becomes {@code .*} and {@code _} becomes {@code .}, while
     * every regex metacharacter in the pattern is escaped.
     *
     * @param pattern the LIKE pattern, e.g. {@code %er500}
     * @return the anchored regex, e.g. {@code ^.*er500$}
     * @throws IllegalArgumentException if the pattern is null or empty
     */
    public static String convertLikePatternToRegex(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("LIKE pattern cannot be null or empty");
        }

        StringBuilder escapedPattern = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (".^$*+?()[{\\|".indexOf(c) >= 0) {
                escapedPattern.append('\\').append(c);
            } else {
                escapedPattern.append(c);
            }
        }

        String regex = escapedPattern.toString()
                .replaceAll("%+", "%")
                .replace("%", ".*")
                .replace("_", ".");

        return "^" + regex + "$";
    }

    private static String extractSequenceDef(String input) {
        int start = input.indexOf("SEQUENCE(");
        if (start < 0) return "";
        int end = input.indexOf(')', start + 9);
        if (end < 0) return "";
        return input.substring(start + 9, end).trim();
    }

    /**
     * Strategy for recognizing and parsing one SQL statement type in the
     * {@link #parse(String, Database)} dispatch. Replaces the long if/else chain
     * (java:S3776 cognitive complexity) with one object per statement type,
     * preserving the exact precedence the legacy chain used.
     */
    private interface QueryParseStrategy {
        boolean matches(String normalized);
        Query<?> parse(String normalized, String original, Database database);
    }

    private final List<QueryParseStrategy> parseStrategies = buildParseStrategies();

    private List<QueryParseStrategy> buildParseStrategies() {
        List<QueryParseStrategy> strategies = new ArrayList<>();
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.EXPLAIN);
            }
            @Override
            public Query<?> parse(String n, String o, Database d) {
                return parseExplainQuery(o, d);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.SELECT);
            }
            @Override
            public Query<?> parse(String n, String o, Database d) {
                return parseSelectQuery(n, o, d);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.INSERT_INTO);
            }
            @Override
            public Query<?> parse(String n, String o, Database d) {
                return parseInsertQuery(n, o, d);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.UPDATE);
            }
            @Override
            public Query<?> parse(String n, String o, Database d) {
                return parseUpdateQuery(n, o, d);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.DELETE_FROM);
            }
            @Override
            public Query<?> parse(String n, String o, Database d) {
                return parseDeleteQuery(n, o, d);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.CREATE_TABLE);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseCreateTableQuery(o);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.CREATE_UNIQUE_CLUSTERED_INDEX);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseCreateUniqueDurableClusteredIndexQuery(n);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.CREATE_UNIQUE_INDEX);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseCreateUniqueIndexQuery(n);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.CREATE_HASH_INDEX);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseCreateHashIndexQuery(n);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.CREATE_INDEX);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseCreateIndexQuery(n);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.equals(SqlKeywords.BEGIN) || n.equals(SqlKeywords.BEGIN_TRANSACTION)
                        || n.equals(SqlKeywords.START_TRANSACTION)
                        || n.startsWith(SqlKeywords.BEGIN_TRANSACTION_ISOLATION_LEVEL)
                        || n.startsWith(SqlKeywords.START_TRANSACTION_ISOLATION_LEVEL);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                IsolationLevel isolationLevel = null;
                if (n.contains(SqlKeywords.ISOLATION_LEVEL_READ_UNCOMMITTED)) {
                    isolationLevel = IsolationLevel.READ_UNCOMMITTED;
                } else if (n.contains(SqlKeywords.ISOLATION_LEVEL_READ_COMMITTED)) {
                    isolationLevel = IsolationLevel.READ_COMMITTED;
                } else if (n.contains(SqlKeywords.ISOLATION_LEVEL_REPEATABLE_READ)) {
                    isolationLevel = IsolationLevel.REPEATABLE_READ;
                } else if (n.contains(SqlKeywords.ISOLATION_LEVEL_SERIALIZABLE)) {
                    isolationLevel = IsolationLevel.SERIALIZABLE;
                }
                return new BeginTransactionQuery(isolationLevel);
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.equals(SqlKeywords.COMMIT_TRANSACTION) || n.equals(SqlKeywords.COMMIT);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return new CommitTransactionQuery();
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.equals(SqlKeywords.ROLLBACK_TRANSACTION) || n.equals(SqlKeywords.ROLLBACK);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return new RollbackTransactionQuery();
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.SET);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                Query<String> setAutoCommitQuery = parseSetAutoCommitQuery(n);
                if (setAutoCommitQuery != null) {
                    return setAutoCommitQuery;
                } else if (n.equals(SqlKeywords.SET_TRANSACTION_ISOLATION_LEVEL_READ_UNCOMMITTED)) {
                    return new SetIsolationLevelQuery(IsolationLevel.READ_UNCOMMITTED);
                } else if (n.equals(SqlKeywords.SET_TRANSACTION_ISOLATION_LEVEL_READ_COMMITTED)) {
                    return new SetIsolationLevelQuery(IsolationLevel.READ_COMMITTED);
                } else if (n.equals(SqlKeywords.SET_TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ)) {
                    return new SetIsolationLevelQuery(IsolationLevel.REPEATABLE_READ);
                } else if (n.equals(SqlKeywords.SET_TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE)) {
                    return new SetIsolationLevelQuery(IsolationLevel.SERIALIZABLE);
                }
                throw new IllegalArgumentException("Unsupported query type");
            }
        });
        strategies.add(new QueryParseStrategy() {
            @Override
            public boolean matches(String n) {
                return n.startsWith(SqlKeywords.ANALYZE);
            }
            @Override
            @SuppressWarnings("unused")
            public Query<?> parse(String n, String o, Database d) {
                return parseAnalyzeTableQuery(n);
            }
        });
        return strategies;
    }

    /**
     * Parses a SQL query into an executable {@link Query} object bound to the
     * given database. The database is used to resolve table metadata while
     * parsing and when executing joins.
     *
     * @param query    the SQL query to parse
     * @param database the database the query will run against
     * @return the parsed query object
     * @throws IllegalArgumentException if the query is null, empty or unsupported
     */
    public Query<?> parse(String query, Database database) {
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        try {
            // Normalize and remove surrounding parentheses
            String normalized = toUpperCasePreservingQuotedIdentifiers(query.trim());
            while (normalized.startsWith("(") && normalized.endsWith(")")) {
                normalized = toUpperCasePreservingQuotedIdentifiers(normalized.substring(1, normalized.length() - 1).trim());
            }
            LOGGER.log(Level.INFO, "Normalized query: {0}", normalized);
            Query<?> lexerResult = parseWithLexer(query, normalized, database);
            if (lexerResult != null) {
                return lexerResult;
            }
            for (QueryParseStrategy strategy : parseStrategies) {
                if (strategy.matches(normalized)) {
                    return strategy.parse(normalized, query, database);
                }
            }
            throw new IllegalArgumentException("Unsupported query type");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    /**
     * Builds the query AST using {@link SqlLexer} for tokenization.
     * The statement keyword is recognized case-insensitively (SqlLexer uppercases
     * all keywords) and the query is dispatched to the appropriate builder for
     * SELECT, INSERT, UPDATE and DELETE. Returns {@code null} for statements that
     * are not handled here so that the caller can fall back to the legacy parser.
     */
    private Query<?> parseWithLexer(String originalQuery, String normalizedQuery, Database database) {
        try {
            SqlLexer lexer = new SqlLexer();
            List<SqlLexer.Token> tokens = lexer.tokenize(originalQuery);
            String statementType = null;
            for (SqlLexer.Token token : tokens) {
                if (token.type != SqlLexer.TokenType.PUNCTUATION || !token.value.equals("(")) {
                    statementType = token.type == SqlLexer.TokenType.KEYWORD ? token.value.toUpperCase() : null;
                    break;
                }
            }
            if (statementType == null) {
                return null;
            }
            switch (statementType) {
                case SqlKeywords.SELECT:
                    return parseSelectQuery(normalizedQuery, originalQuery, database);
                case SqlKeywords.INSERT:
                    return parseInsertQuery(normalizedQuery, originalQuery, database);
                case SqlKeywords.UPDATE:
                    return parseUpdateQuery(normalizedQuery, originalQuery, database);
                case SqlKeywords.DELETE:
                    return parseDeleteQuery(normalizedQuery, originalQuery, database);
                default:
                    return null;
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARNING, "Lexer-based parsing failed for query: {0}, Error: {1}",
                    new Object[]{originalQuery, e.getMessage()});
            return null;
        }
    }

    /**
     * Returns whether the query is an EXPLAIN statement (case-insensitive).
     * {@link Database#parse} uses this to route EXPLAIN to {@link QueryParser}
     * even when the inner statement contains subqueries, so that SubqueryParser
     * does not mistake the inner {@code (SELECT ...)} for its own input.
     *
     * @param query the SQL query to inspect
     * @return true when the query starts with the EXPLAIN keyword
     */
    static boolean isExplainQuery(String query) {
        // Prompt 22 (java:S2259): a null query has no EXPLAIN prefix.
        if (query == null) {
            return false;
        }
        return toUpperCasePreservingQuotedIdentifiers(query.trim()).startsWith(SqlKeywords.EXPLAIN);
    }

    /**
     * Parses an EXPLAIN statement: strips the EXPLAIN (and optional ANALYZE)
     * keyword, parses the inner SELECT / INSERT / UPDATE / DELETE statement
     * with the full pipeline (subqueries included) and wraps it into an
     * {@link ExplainQuery}. EXPLAIN supports only data statements, so DDL and
     * transaction commands are rejected.
     */
    private Query<?> parseExplainQuery(String original, Database database) {
        String rest = stripLeadingKeyword(original, SqlKeywords.EXPLAIN);
        boolean analyze = false;
        if (rest.toUpperCase().startsWith(SqlKeywords.ANALYZE)) {
            analyze = true;
            rest = stripLeadingKeyword(rest, SqlKeywords.ANALYZE);
        }
        String inner = rest.trim();
        String innerNormalized = toUpperCasePreservingQuotedIdentifiers(inner);
        if (!(innerNormalized.startsWith(SqlKeywords.SELECT) || innerNormalized.startsWith(SqlKeywords.INSERT)
                || innerNormalized.startsWith(SqlKeywords.UPDATE) || innerNormalized.startsWith(SqlKeywords.DELETE))) {
            throw new IllegalArgumentException("EXPLAIN supports only SELECT, INSERT, UPDATE and DELETE statements");
        }
        SubqueryParser subqueryParser = new SubqueryParser();
        Query<?> innerQuery = subqueryParser.containsSubquery(inner)
                ? subqueryParser.parse(inner, database)
                : new QueryParser().parse(inner, database);
        return new ExplainQuery(innerQuery, analyze, inner);
    }

    private String stripLeadingKeyword(String text, String keyword) {
        String trimmed = text.trim();
        if (trimmed.toUpperCase().startsWith(keyword)) {
            return trimmed.substring(keyword.length()).trim();
        }
        return trimmed;
    }

    /**
     * Parses the {@code SET AUTOCOMMIT = {ON|OFF}} command (optionally with the
     * {@code SESSION} keyword, e.g. {@code SET SESSION AUTOCOMMIT = OFF}) and
     * returns a {@link SetAutoCommitQuery} that changes the session's
     * auto-commit mode. Returns {@code null} if the query is not a SET
     * AUTOCOMMIT command.
     */
    private Query<String> parseSetAutoCommitQuery(String normalized) {
        Matcher matcher = Pattern.compile("^SET\\s+(?:SESSION\\s+)?AUTOCOMMIT\\s*(?:=\\s*|\\s+)(ON|OFF|TRUE|FALSE|1|0)\\s*;?$").matcher(normalized);
        if (!matcher.matches()) {
            return null;
        }
        String value = matcher.group(1);
        return new SetAutoCommitQuery(value.equals(SqlKeywords.ON) || value.equals(SqlKeywords.TRUE) || value.equals("1"));
    }

    /**
     * Parses an {@code ANALYZE TABLE <name>} statement (case-insensitive) into
     * an {@link AnalyzeTableQuery} that forces a synchronous statistics
     * recalculation. A trailing semicolon is tolerated; anything other than a
     * single table name is rejected with a descriptive error.
     *
     * @param normalized the uppercased query text
     * @return the parsed analyze query
     * @throws IllegalArgumentException on malformed ANALYZE TABLE input
     */
    private Query<?> parseAnalyzeTableQuery(String normalized) {
        String rest = normalized.substring(SqlKeywords.ANALYZE.length()).trim();
        if (!rest.toUpperCase().startsWith(SqlKeywords.TABLE)) {
            throw new IllegalArgumentException("Invalid ANALYZE TABLE syntax: expected 'ANALYZE TABLE <table name>'");
        }
        String tableName = rest.substring(SqlKeywords.TABLE.length()).trim();
        if (tableName.endsWith(";")) {
            tableName = tableName.substring(0, tableName.length() - 1).trim();
        }
        if (tableName.isEmpty() || CharOps.containsWhitespace(tableName) || tableName.contains("(")) {
            throw new IllegalArgumentException("Invalid ANALYZE TABLE syntax: expected 'ANALYZE TABLE <table name>'");
        }
        return new AnalyzeTableQuery(tableName.toUpperCase());
    }

    private Query<Void> parseCreateIndexQuery(String normalized) {
        String[] parts = normalized.split(SqlKeywords.ON);
        if (parts.length != 2) {
            throw new SyntaxErrorException("Invalid CREATE INDEX query format");
        }
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();

        // Check for COVERING clause: CREATE INDEX ON T(A) COVERING (B, C)
        if (tableAndColumn.toUpperCase().contains(SqlKeywords.COVERING)) {
            int coveringIdx = tableAndColumn.toUpperCase().indexOf(SqlKeywords.COVERING);
            String beforeCovering = tableAndColumn.substring(0, coveringIdx).trim();
            String afterCovering = tableAndColumn.substring(coveringIdx + SqlKeywords.COVERING.length()).trim();

            // Extract index column from before COVERING
            String indexColumn = beforeCovering.substring(
                    beforeCovering.indexOf("(") + 1, beforeCovering.indexOf(")")).trim();

            // Extract cover columns from after COVERING(...)
            String coverPart = afterCovering.substring(
                    afterCovering.indexOf("(") + 1, afterCovering.indexOf(")")).trim();
            String[] coverColNames = coverPart.split(",");
            List<String> coverColumns = new ArrayList<>();
            for (String c : coverColNames) {
                coverColumns.add(c.trim());
            }
            return new CreateCoveringIndexQuery(tableName, indexColumn, coverColumns);
        }

        String columnsPart = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        String[] columnNames = columnsPart.split(",");
        if (columnNames.length == 1) {
            return new CreateIndexQuery(tableName, columnNames[0].trim());
        } else {
            List<String> columns = new ArrayList<>();
            for (String c : columnNames) {
                columns.add(c.trim());
            }
            return new CreateCompositeIndexQuery(tableName, columns);
        }
    }

    private Query<Void> parseCreateHashIndexQuery(String normalized) {
        String[] parts = normalized.split(SqlKeywords.ON);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE HASH INDEX query format");
        }
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateHashIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateUniqueIndexQuery(String normalized) {
        String[] parts = normalized.split(SqlKeywords.ON);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE UNIQUE INDEX query format");
        }
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateUniqueIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateUniqueDurableClusteredIndexQuery(String normalized) {
        String[] parts = normalized.split(SqlKeywords.ON);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE UNIQUE CLUSTERED INDEX query format");
        }
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateUniqueClusteredIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateTableQuery(String original) {
        int firstParen = original.indexOf('(');
        int lastParen = original.lastIndexOf(')');
        if (firstParen == -1 || lastParen == -1 || lastParen < firstParen) {
            throw new IllegalArgumentException("Invalid CREATE TABLE query format: missing or mismatched parentheses");
        }

        String rawName = original.substring(0, firstParen).replace(SqlKeywords.CREATE_TABLE, "").trim();
        boolean quoted = rawName.length() >= 2 && rawName.charAt(0) == '"' && rawName.charAt(rawName.length() - 1) == '"';
        String tableName = unquoteIdentifier(rawName);
        if (!quoted) {
            tableName = tableName.toUpperCase();
        }
        String columnsPart = original.substring(firstParen + 1, lastParen).trim();

        List<String> columnDefs = splitColumnDefinitions(columnsPart);
        List<String> columns = new ArrayList<>();
        Map<String, Class<?>> columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Sequence> sequences = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String primaryKeyColumn = null;

        for (String colDef : columnDefs) {
            String[] colParts = colDef.trim().split("\\s+", 3);
            if (colParts.length < 2) {
                throw new IllegalArgumentException("Invalid column definition: " + colDef);
            }
            String colName = unquoteIdentifier(colParts[0]);
            String type = colParts[1].toUpperCase();
            String constraints = colParts.length > 2 ? colParts[2].toUpperCase() : "";
            boolean isPrimaryKey = constraints.contains(SqlKeywords.PRIMARY_KEY);
            boolean hasSequence = constraints.contains(SqlKeywords.SEQUENCE) || type.endsWith("_SEQUENCE");

            columns.add(colName);

            if (hasSequence) {
                if (!type.equals(SqlKeywords.TYPE_LONG) && !type.equals(SqlKeywords.TYPE_INTEGER) && !type.startsWith("LONG_SEQUENCE") && !type.startsWith("INTEGER_SEQUENCE")) {
                    throw new IllegalArgumentException("Sequence is only supported for LONG or INTEGER types: " + colDef);
                }
                String seqDef;
                if (type.endsWith("_SEQUENCE")) {
                    seqDef = colParts.length > 2 ? extractSequenceDef(colParts[2]) : "";
                } else {
                    seqDef = extractSequenceDef(constraints);
                }
                String[] seqParts = seqDef.split("\\s+");
                if (seqParts.length < 3) {
                    throw new IllegalArgumentException("Invalid SEQUENCE definition in column: " + colDef);
                }
                String seqName = seqParts[0];
                long start = Long.parseLong(seqParts[1]);
                long increment = Long.parseLong(seqParts[2]);
                Class<?> seqType = type.equals(SqlKeywords.TYPE_LONG) || type.startsWith("LONG_SEQUENCE") ? Long.class : Integer.class;
                sequences.put(colName, new Sequence(seqName, seqType, start, increment));
                columnTypes.put(colName, seqType);
            } else {
                switch (type) {
                    case SqlKeywords.TYPE_STRING:
                        columnTypes.put(colName, String.class);
                        break;
                    case SqlKeywords.TYPE_INTEGER:
                        columnTypes.put(colName, Integer.class);
                        break;
                    case SqlKeywords.TYPE_LONG:
                        columnTypes.put(colName, Long.class);
                        break;
                    case SqlKeywords.TYPE_SHORT:
                        columnTypes.put(colName, Short.class);
                        break;
                    case SqlKeywords.TYPE_BYTE:
                        columnTypes.put(colName, Byte.class);
                        break;
                    case SqlKeywords.TYPE_BIGDECIMAL:
                        columnTypes.put(colName, BigDecimal.class);
                        break;
                    case SqlKeywords.TYPE_FLOAT:
                        columnTypes.put(colName, Float.class);
                        break;
                    case SqlKeywords.TYPE_DOUBLE:
                        columnTypes.put(colName, Double.class);
                        break;
                    case SqlKeywords.TYPE_CHAR:
                        columnTypes.put(colName, Character.class);
                        break;
                    case SqlKeywords.TYPE_UUID:
                        columnTypes.put(colName, UUID.class);
                        break;
                    case SqlKeywords.TYPE_BOOLEAN:
                        columnTypes.put(colName, Boolean.class);
                        break;
                    case SqlKeywords.TYPE_DATE:
                        columnTypes.put(colName, LocalDate.class);
                        break;
                    case SqlKeywords.TYPE_DATETIME:
                    case SqlKeywords.TYPE_DATETIME_MS:
                        columnTypes.put(colName, LocalDateTime.class);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported column type: " + type);
                }
            }

            if (isPrimaryKey) {
                if (primaryKeyColumn != null) {
                    throw new IllegalArgumentException("Multiple primary keys defined in table " + tableName);
                }
                primaryKeyColumn = colName;
            }
        }

        LOGGER.log(Level.INFO, "Parsed CREATE TABLE query: table={0}, columns={1}, types={2}, primaryKey={3}, sequences={4}",
                new Object[]{tableName, columns, columnTypes, primaryKeyColumn, sequences.keySet()});

        return new CreateTableQuery(tableName, columns, columnTypes, primaryKeyColumn, sequences);
    }

    private List<String> splitColumnDefinitions(String columnsPart) {
        if (columnsPart == null || columnsPart.trim().isEmpty()) {
            return new ArrayList<>();
        }

        List<String> columnDefs = new ArrayList<>();
        for (String part : splitTopLevelComma(columnsPart)) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                columnDefs.add(trimmed);
            }
        }

        return columnDefs;
    }

    /**
     * Splits a string on commas that are outside single-quoted string literals
     * and outside parenthesized groups. Quoted strings may contain doubled
     * quotes ({@code ''}) and backslash escapes, neither of which ends the
     * literal. A linear single-pass scan replaces the previous catastrophic
     * {@code (?=([^']*'[^']*')*[^']*$)} lookahead split, which could backtrack
     * exponentially (and overflow the regex stack) on long inputs with quotes.
     */
    private List<String> splitTopLevelComma(String input) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (inQuotes) {
                if (c == '\\' && i + 1 < input.length()) {
                    current.append(c);
                    current.append(input.charAt(++i));
                } else if (c == '\'') {
                    if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                        current.append('\'');
                        current.append('\'');
                        i++;
                    } else {
                        inQuotes = false;
                        current.append(c);
                    }
                } else {
                    current.append(c);
                }
            } else if (c == '\'') {
                inQuotes = true;
                current.append(c);
            } else if (c == ',' && parenDepth == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')' && parenDepth > 0) {
                    parenDepth--;
                }
                current.append(c);
            }
        }

        parts.add(current.toString());
        return parts;
    }

    private int indexOfIgnoreCase(String source, String target) {
        if (source == null || target == null || target.isEmpty()) {
            return -1;
        }
        return source.toUpperCase().indexOf(target.toUpperCase());
    }

    private int findMainFromClause(String query) {
        if (query == null || query.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}", query);
            return -1;
        }

        // Регулярные выражения для разных типов токенов
        Pattern quotedStringPattern = Pattern.compile("(?i)'[^'\\\\]*+(?:\\\\.[^'\\\\]*+)*+'", Pattern.DOTALL);
        Pattern quotedIdentifierPattern = Pattern.compile("\"[^\"]*\"");
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern fromPattern = Pattern.compile("(?i)\\bFROM\\b");
        Pattern wordPattern = Pattern.compile("\\S+");

        int bracketDepth = 0;
        int fromIndex = -1;
        int currentPos = 0;

        while (currentPos < query.length()) {
            // Проверяем строки в кавычках
            Matcher quotedStringMatcher = quotedStringPattern.matcher(query).region(currentPos, query.length());
            // Проверяем quoted-идентификаторы
            Matcher quotedIdentifierMatcher = quotedIdentifierPattern.matcher(query).region(currentPos, query.length());
            // Проверяем открывающую скобку
            Matcher openParenMatcher = openParenPattern.matcher(query).region(currentPos, query.length());
            // Проверяем закрывающую скобку
            Matcher closeParenMatcher = closeParenPattern.matcher(query).region(currentPos, query.length());
            // Проверяем FROM
            Matcher fromMatcher = fromPattern.matcher(query).region(currentPos, query.length());
            // Проверяем слово
            Matcher wordMatcher = wordPattern.matcher(query).region(currentPos, query.length());

            int nextPos = query.length();
            String token = null;
            String tokenType = null;
            int start = currentPos;

            // Находим ближайший токен
            if (quotedStringMatcher.lookingAt()) {
                token = quotedStringMatcher.group();
                nextPos = quotedStringMatcher.end();
                tokenType = "quotedString";
            } else if (quotedIdentifierMatcher.lookingAt()) {
                token = quotedIdentifierMatcher.group();
                nextPos = quotedIdentifierMatcher.end();
                tokenType = "quotedIdentifier";
            } else if (openParenMatcher.lookingAt()) {
                token = openParenMatcher.group();
                nextPos = openParenMatcher.end();
                tokenType = "openParen";
            } else if (closeParenMatcher.lookingAt()) {
                token = closeParenMatcher.group();
                nextPos = closeParenMatcher.end();
                tokenType = "closeParen";
            } else if (fromMatcher.lookingAt()) {
                token = fromMatcher.group();
                nextPos = fromMatcher.end();
                tokenType = "from";
            } else if (wordMatcher.lookingAt()) {
                token = wordMatcher.group();
                nextPos = wordMatcher.end();
                tokenType = "word";
            }

            if (token == null) {
                // Пропускаем пробелы или неизвестные символы
                currentPos++;
                continue;
            }

            // Логируем токен
            //LOGGER.log(Level.FINEST, "Токен: start={0}, end={1}, type={2}, value={3}, bracketDepth={4}",
            //        new Object[]{start, nextPos, tokenType, token, bracketDepth});

            // Обрабатываем токен
            if (tokenType.equals("quotedString")) {
                // skip
            } else if (tokenType.equals("quotedIdentifier")) {
                // skip
            } else if (tokenType.equals("openParen")) {
                bracketDepth++;
            } else if (tokenType.equals("closeParen")) {
                bracketDepth--;
                if (bracketDepth < 0) {
                    LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}",
                            new Object[]{start, query});
                    return -1;
                }
            } else if (tokenType.equals("from") && bracketDepth == 0) {
                fromIndex = start;
                LOGGER.log(Level.FINEST, "Найден основной FROM на позиции {0} в запросе: {1}",
                        new Object[]{fromIndex, query});
                return fromIndex;
            } else if (tokenType.equals("word")) {
                // skip
            }

            currentPos = nextPos;
        }

        if (bracketDepth != 0) {
            LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе: bracketDepth={0}, query={1}",
                    new Object[]{bracketDepth, query});
            return -1;
        }

        LOGGER.log(Level.FINEST, "Основной FROM не найден в запросе: {0}", query);
        return -1;
    }
    private Query<List<Map<String, Object>>> parseSelectQuery(String normalized, String original, Database database) {
        // Находим индекс основного FROM
        int fromIndex = findMainFromClause(original);
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Недопустимый формат SELECT-запроса: отсутствует FROM");
        }

        // Извлекаем части запроса
        int selectIndex = indexOfIgnoreCase(original, SqlKeywords.SELECT);
        if (selectIndex == -1) {
            throw new IllegalArgumentException("Недопустимый формат SELECT-запроса: отсутствует SELECT");
        }
        String selectPartOriginal = original.substring(selectIndex + 6, fromIndex).trim();
        String tableAndJoinsOriginal = original.substring(fromIndex + 4).trim();

        // Парсим элементы SELECT
        SelectItems selectItems = parseSelectItems(selectPartOriginal, database);
        List<String> columns = selectItems.columns;
        List<AggregateFunction> aggregates = selectItems.aggregates;
        List<SubQuery> subQueries = selectItems.subQueries;
        Map<String, String> columnAliases = selectItems.columnAliases;

        // Парсим таблицы и соединения
        TableJoins tableJoins = parseTableAndJoins(tableAndJoinsOriginal, database);
        String tableName = tableJoins.tableName;
        String tableAlias = tableJoins.tableAlias;
        List<JoinInfo> joins = tableJoins.joins;
        Map<String, String> tableAliases = tableJoins.tableAliases;
        Map<String, Class<?>> combinedColumnTypes = tableJoins.combinedColumnTypes;

        // Парсим дополнительные условия и клаузы
        ParseContext ctx = new ParseContext(tableName, database, original, false, combinedColumnTypes, tableAliases, columnAliases);
        AdditionalClauses clauses = parseAdditionalClauses(tableAndJoinsOriginal, ctx, aggregates, subQueries);
        List<Condition> conditions = clauses.conditions;
        List<String> groupBy = clauses.groupBy;
        List<HavingCondition> havingConditions = clauses.havingConditions;
        List<OrderByInfo> orderBy = clauses.orderBy;
        Integer limit = clauses.limit;
        Integer offset = clauses.offset;

        LOGGER.log(Level.INFO, "Разобран SELECT-запрос: таблица={0}, столбцы={1}, агрегации={2}, соединения={3}, условия={4}",
                new Object[]{tableName, columns, aggregates, joins, conditions});

        return SelectQuery.builder()
                .tableName(tableName)
                .tableAlias(tableAlias)
                .columns(columns)
                .aggregates(aggregates)
                .joins(joins)
                .conditions(conditions)
                .groupBy(groupBy)
                .havingConditions(havingConditions)
                .orderBy(orderBy)
                .limit(limit)
                .offset(offset)
                .tableAliases(columnAliases)
                .extraTableAliases(tableAliases)
                .columnTypes(tableJoins.combinedColumnTypes)
                .build();
    }


    // Парсит элементы SELECT (столбцы, агрегации, подзапросы)
    private SelectItems parseSelectItems(String selectPartOriginal, Database database) {
        List<String> selectItems = splitSelectItems(selectPartOriginal);
        List<String> columns = new ArrayList<>();
        List<AggregateFunction> aggregates = new ArrayList<>();
        List<SubQuery> subQueries = new ArrayList<>();
        Map<String, String> columnAliases = new HashMap<>();

        // Single unified pattern: FUNC_NAME ( ARG ) [AS alias]
        // Group 1 = function name (case-insensitive), group 2 = argument, group 3 = alias
        Pattern aggPattern = Pattern.compile(
                "(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|" + QUALIFIED_IDENTIFIER_PATTERN + "|\\([^()]*+\\))\\s*\\)(?:\\s+(?:AS\\s+)?(" + IDENTIFIER_PATTERN + "))?$");
        Pattern columnPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")(?:\\s+(?:AS\\s+)?(" + IDENTIFIER_PATTERN + "))?$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+[^()]*+\\)\\s*(?:AS\\s+(" + IDENTIFIER_PATTERN + "))?\\s*$");

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher aggMatcher = aggPattern.matcher(trimmedItem);
            if (aggMatcher.matches()) {
                aggregates.add(parseAggregateArg(aggMatcher.group(1), aggMatcher.group(2),
                        unquoteIdentifier(aggMatcher.group(3)), database));
            } else {
                Matcher subQueryMatcher = subQueryPattern.matcher(trimmedItem);
                if (subQueryMatcher.matches()) {
                    parseSelectSubQuery(trimmedItem, subQueryMatcher, subQueries, columnAliases, database);
                } else {
                    Matcher columnMatcher = columnPattern.matcher(trimmedItem);
                    if (columnMatcher.matches()) {
                        parseSelectColumn(columnMatcher, columns, columnAliases);
                    } else if (trimmedItem.equals("*")) {
                        columns.add("*");
                        LOGGER.log(Level.FINE, "Разобран столбец: *");
                    } else {
                        throw new IllegalArgumentException("Недопустимый элемент SELECT: " + trimmedItem);
                    }
                }
            }
        }

        return new SelectItems(columns, aggregates, subQueries, columnAliases);
    }

    private AggregateFunction parseAggregateArg(String funcName, String arg, String alias, Database database) {
        boolean isSubQuery = arg.startsWith("(") && arg.endsWith(")");
        String subQueryStr = isSubQuery ? arg.substring(1, arg.length() - 1).trim() : null;
        String column = isSubQuery ? null
                : (arg.equals("*") ? null : unquoteQualifiedIdentifier(arg));

        if (isSubQuery) {
            Query<?> subQuery = parse(subQueryStr, database);
            LOGGER.log(Level.FINE, "Parsed {0}(subquery){1}",
                    new Object[]{funcName, alias != null ? " AS " + alias : ""});
            return new AggregateFunction(funcName, new SubQuery(subQuery, null), alias);
        } else {
            LOGGER.log(Level.FINE, "Parsed {0}({1}){2}",
                    new Object[]{funcName, column == null ? "*" : column, alias != null ? " AS " + alias : ""});
            return new AggregateFunction(funcName, column, alias);
        }
    }

    private void parseSelectSubQuery(String trimmedItem, Matcher subQueryMatcher,
                                      List<SubQuery> subQueries, Map<String, String> columnAliases, Database database) {
        int subQueryEnd = findMatchingParenthesis(trimmedItem, 0);
        if (subQueryEnd == -1) {
            throw new IllegalArgumentException("Недопустимый синтаксис подзапроса в SELECT: " + trimmedItem);
        }
        String subQueryStr = trimmedItem.substring(1, subQueryEnd).trim();
        if (subQueryStr.isEmpty()) {
            throw new IllegalArgumentException("Пустой подзапрос в SELECT: " + trimmedItem);
        }
        String alias = unquoteIdentifier(subQueryMatcher.group(1));
        Query<?> subQuery = parse(subQueryStr, database);
        SubQuery newSubQuery = new SubQuery(subQuery, alias);
        subQueries.add(newSubQuery);
        if (alias != null) {
            String subQueryKey = "SUBQUERY_" + subQueries.size();
            columnAliases.put(subQueryKey, alias);
        }
        LOGGER.log(Level.FINE, "Parsed SELECT subquery: {0}{1}",
                new Object[]{subQueryStr, alias != null ? " AS " + alias : ""});
    }

    private void parseSelectColumn(Matcher columnMatcher, List<String> columns, Map<String, String> columnAliases) {
        String column = unquoteQualifiedIdentifier(columnMatcher.group(1));
        String alias = unquoteIdentifier(columnMatcher.group(2));
        columns.add(column);
        if (alias != null) {
            columnAliases.put(column, alias);
            LOGGER.log(Level.FINE, "Разобран столбец с алиасом: {0} AS {1}", new Object[]{column, alias});
        } else {
            LOGGER.log(Level.FINE, "Разобран столбец: {0}", new Object[]{column});
        }
    }

    // Парсит таблицы и соединения
    private TableJoins parseTableAndJoins(String tableAndJoinsOriginal, Database database) {
        String tableAndJoins = normalizeQueryString(tableAndJoinsOriginal).trim();
        List<JoinInfo> joins = new ArrayList<>();
        String tableName;
        String tableAlias = null;
        Map<String, String> tableAliases = new HashMap<>();

        Pattern joinPattern = Pattern.compile("(?i)\\s*(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|LEFT INNER JOIN|RIGHT INNER JOIN|LEFT OUTER JOIN|RIGHT OUTER JOIN|FULL OUTER JOIN)\\s+");
        Matcher joinMatcher = joinPattern.matcher(tableAndJoins);
        List<String> joinParts = new ArrayList<>();
        int lastEnd = 0;
        while (joinMatcher.find()) {
            joinParts.add(tableAndJoins.substring(lastEnd, joinMatcher.start()).trim());
            joinParts.add(joinMatcher.group(1).trim());
            lastEnd = joinMatcher.end();
        }
        joinParts.add(tableAndJoins.substring(lastEnd).trim());

        String mainTablePart = joinParts.get(0).trim();
        String[] mainTableTokens = mainTablePart.split("\\s+");
        tableName = unquoteIdentifier(mainTableTokens[0].trim());
        if (mainTableTokens.length > 1) {
            if (mainTableTokens.length == 3 && mainTableTokens[1].equalsIgnoreCase(SqlKeywords.AS)) {
                tableAlias = unquoteIdentifier(mainTableTokens[2].trim());
            } else if (mainTableTokens.length == 2) {
                tableAlias = unquoteIdentifier(mainTableTokens[1].trim());
            }
        }
        if (tableAlias != null) {
            tableAliases.put(tableAlias, tableName);
            LOGGER.log(Level.FINE, "Разобран алиас главной таблицы: {0} -> {1}", new Object[]{tableAlias, tableName});
        }

        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Таблица не найдена: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
        tableAliases.put(tableName, tableName);

        String mainTableName = tableName;
        joins = parseJoins(joinParts, mainTableName, database, tableAndJoinsOriginal, tableAliases, combinedColumnTypes);

        return new TableJoins(mainTableName, tableAlias, joins, tableAliases, combinedColumnTypes);
    }

    // Парсит JOIN-части (выделено из parseTableAndJoins для снижения
    // когнитивной сложности — java:S3776). Мутирует tableAliases и
    // combinedColumnTypes, возвращает список JoinInfo.
    private List<JoinInfo> parseJoins(List<String> joinParts, String mainTableName, Database database,
            String tableAndJoinsOriginal, Map<String, String> tableAliases,
            Map<String, Class<?>> combinedColumnTypes) {
        List<JoinInfo> joins = new ArrayList<>();
        String tableName = mainTableName;
        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();

            JoinType joinType = parseJoinType(joinTypeStr);
            String joinTableName;
            String joinTableAlias = null;
            List<Condition> onConditions = new ArrayList<>();

            // Обновляем разделение joinPart
            Pattern clausePattern = Pattern.compile("(?i)\\s+(WHERE|LIMIT|OFFSET|ORDER BY|GROUP BY|$)\\b");
            Matcher clauseMatcher = clausePattern.matcher(joinPart);
            String onClausePart = joinPart;
            if (clauseMatcher.find()) {
                onClausePart = joinPart.substring(0, clauseMatcher.start()).trim();
                LOGGER.log(Level.FINE, "Обрезана часть после ON до {0}: {1}", new Object[]{clauseMatcher.group(1), onClausePart});
            }

// Обновляем разделение joinPart
            String[] joinTableTokens;
            if (joinType == JoinType.CROSS) {
                joinTableTokens = new String[]{joinPart.trim()};
            } else {
                // Ищем позицию ON вне скобок и кавычек
                int onIndex = findOnClausePosition(joinPart);
                if (onIndex == -1) {
                    throw new IllegalArgumentException("Недопустимый формат " + joinTypeStr + ": отсутствует ON");
                }
                String joinTablePart = joinPart.substring(0, onIndex).trim();
                String onClause = joinPart.substring(onIndex + 2).trim(); // Пропускаем SqlKeywords.ON
                joinTableTokens = new String[]{joinTablePart, onClause};
            }

            String joinTablePart = joinTableTokens[0].trim();
            String[] joinTableParts = joinTablePart.split("\\s+");
            joinTableName = unquoteIdentifier(joinTableParts[0].trim());
            if (joinTableParts.length > 1) {
                if (joinTableParts.length == 3 && joinTableParts[1].equalsIgnoreCase(SqlKeywords.AS)) {
                    joinTableAlias = unquoteIdentifier(joinTableParts[2].trim());
                } else if (joinTableParts.length == 2) {
                    joinTableAlias = unquoteIdentifier(joinTableParts[1].trim());
                }
            }
            if (joinTableAlias != null) {
                tableAliases.put(joinTableAlias, joinTableName);
                LOGGER.log(Level.FINE, "Разобран алиас таблицы соединения: {0} -> {1}", new Object[]{joinTableAlias, joinTableName});
            }

            if (joinType == JoinType.CROSS) {
                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Таблица соединения не найдена: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                tableAliases.put(joinTableName, joinTableName);
                if (joinTableTokens.length > 1 && !joinTableTokens[1].trim().isEmpty()) {
                    String remaining = joinTableTokens[1].trim();
                    if (remaining.toUpperCase().contains(" ON ")) {
                        throw new IllegalArgumentException("CROSS JOIN не поддерживает ON: " + joinPart);
                    }
                }
                LOGGER.log(Level.FINE, "Разобран CROSS JOIN: таблица={0}, алиас={1}", new Object[]{joinTableName, joinTableAlias});
                joins.add(new JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            } else {
                if (joinTableTokens.length != 2) {
                    throw new IllegalArgumentException("Недопустимый формат " + joinTypeStr + ": неверный ON");
                }
                String onClause = joinTableTokens[1].trim();

                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Таблица соединения не найдена: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                tableAliases.put(joinTableName, joinTableName);

                onConditions = parseConditions(onClause, new ParseContext(tableName, database, tableAndJoinsOriginal, true,
                        combinedColumnTypes, tableAliases, new HashMap<>()));

                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }

                LOGGER.log(Level.FINE, "Разобранные условия ON для {0}: {1}", new Object[]{joinTypeStr, onConditions});
                joins.add(new JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            }
            tableName = joinTableName;
        }
        return joins;
    }

    // Парсит тип соединения
    private JoinType parseJoinType(String joinTypeStr) {
        return switch (joinTypeStr.toUpperCase()) {
            case SqlKeywords.JOIN, SqlKeywords.INNER_JOIN -> JoinType.INNER;
            case SqlKeywords.LEFT_JOIN, SqlKeywords.LEFT_OUTER_JOIN -> JoinType.LEFT_OUTER;
            case SqlKeywords.RIGHT_JOIN, SqlKeywords.RIGHT_OUTER_JOIN -> JoinType.RIGHT_OUTER;
            case SqlKeywords.FULL_JOIN, SqlKeywords.FULL_OUTER_JOIN -> JoinType.FULL_OUTER;
            case SqlKeywords.LEFT_INNER_JOIN -> JoinType.LEFT_INNER;
            case SqlKeywords.RIGHT_INNER_JOIN -> JoinType.RIGHT_INNER;
            case SqlKeywords.CROSS_JOIN -> JoinType.CROSS;
            default -> throw new IllegalArgumentException("Неподдерживаемый тип соединения: " + joinTypeStr);
        };
    }

    // Парсит дополнительные клаузы (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
    private AdditionalClauses parseAdditionalClauses(String tableAndJoinsOriginal, ParseContext ctx,
                                                     List<AggregateFunction> aggregates, List<SubQuery> subQueries) {
        List<Condition> conditions = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<HavingCondition> havingConditions = new ArrayList<>();
        List<OrderByInfo> orderBy = new ArrayList<>();
        Integer limit = null;
        Integer offset = null;
        Map<String, String> groupBySubQueries = new HashMap<>();

        // Parse GROUP BY (and HAVING)
        int groupByIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.GROUP_BY);
        if (groupByIndex != -1) {
            ParsedGroupBy parsedGroupBy = extractGroupBy(tableAndJoinsOriginal, groupByIndex,
                    ctx.defaultTableName, ctx.database, ctx.combinedColumnTypes, ctx.tableAliases,
                    ctx.columnAliases, groupBySubQueries, aggregates, ctx);
            groupBy = parsedGroupBy.groupBy;
            havingConditions = parsedGroupBy.havingConditions;
            tableAndJoinsOriginal = removeClause(tableAndJoinsOriginal, groupByIndex, parsedGroupBy.endIndex);
        }

        // Parse ORDER BY
        int orderByIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.ORDER_BY);
        if (orderByIndex != -1) {
            ParsedOrderBy parsedOrderBy = extractOrderBy(tableAndJoinsOriginal, orderByIndex,
                    ctx.defaultTableName, ctx.combinedColumnTypes, ctx.tableAliases,
                    ctx.columnAliases, subQueries);
            orderBy = parsedOrderBy.orderBy;
            // ORDER BY ends where LIMIT/OFFSET starts, not at end of string
            int orderByEnd = tableAndJoinsOriginal.length();
            int limIdx = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.LIMIT);
            int offIdx = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.OFFSET);
            if (limIdx > orderByIndex) orderByEnd = Math.min(orderByEnd, limIdx);
            if (offIdx > orderByIndex) orderByEnd = Math.min(orderByEnd, offIdx);
            tableAndJoinsOriginal = removeClause(tableAndJoinsOriginal, orderByIndex, orderByEnd);
        }

        // Parse LIMIT (and optional trailing OFFSET)
        int limitIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.LIMIT);
        if (limitIndex != -1) {
            ParsedLimitOffset parsedLimit = extractLimit(tableAndJoinsOriginal, limitIndex);
            limit = parsedLimit.limit;
            offset = parsedLimit.offset;
            tableAndJoinsOriginal = removeClause(tableAndJoinsOriginal, limitIndex, tableAndJoinsOriginal.length());
        }

        // Parse standalone OFFSET without LIMIT
        if (limitIndex == -1) {
            int offsetIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.OFFSET);
            if (offsetIndex != -1) {
                offset = extractOffset(tableAndJoinsOriginal, offsetIndex);
                tableAndJoinsOriginal = removeClause(tableAndJoinsOriginal, offsetIndex, tableAndJoinsOriginal.length());
            }
        }

        // Parse WHERE
        int whereIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, SqlKeywords.WHERE);
        if (whereIndex != -1) {
            String conditionStr = tableAndJoinsOriginal.substring(whereIndex + 6).trim();
            conditions = parseConditions(conditionStr, ctx);
        }

        return new AdditionalClauses(conditions, groupBy, havingConditions, orderBy, limit, offset, groupBySubQueries);
    }

    private String removeClause(String text, int clauseIndex, int endIndex) {
        String before = text.substring(0, clauseIndex).trim();
        String after = (endIndex < text.length()) ? text.substring(endIndex).trim() : "";
        return (before + " " + after).trim();
    }

    private record ParsedGroupBy(List<String> groupBy, List<HavingCondition> havingConditions, int endIndex) {}
    private record ParsedOrderBy(List<OrderByInfo> orderBy) {}
    private record ParsedLimitOffset(Integer limit, Integer offset) {}

    private ParsedGroupBy extractGroupBy(String text, int groupByIndex, String tableName, Database database,
            Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
            Map<String, String> columnAliases, Map<String, String> groupBySubQueries,
            List<AggregateFunction> aggregates, ParseContext ctx) {
        int orderByIndex = findClauseOutsideSubquery(text, SqlKeywords.ORDER_BY);
        int limitIndex = findClauseOutsideSubquery(text, SqlKeywords.LIMIT);
        int endIndex = text.length();
        for (int idx : new int[]{orderByIndex, limitIndex}) {
            if (idx != -1 && idx > groupByIndex && idx < endIndex) {
                endIndex = idx;
            }
        }
        String groupByClause = text.substring(groupByIndex + 8, endIndex).trim();
        int havingIndex = findClauseOutsideSubquery(groupByClause, SqlKeywords.HAVING);
        String havingClause = null;
        if (havingIndex != -1) {
            havingClause = groupByClause.substring(havingIndex + 6).trim();
            groupByClause = groupByClause.substring(0, havingIndex).trim();
        }
        List<String> groupBy = parseGroupByClause(groupByClause, tableName, database,
                combinedColumnTypes, tableAliases, columnAliases, groupBySubQueries);
        List<HavingCondition> havingConditions = havingClause != null
                ? parseHavingConditions(havingClause, ctx, aggregates) : new ArrayList<>();
        return new ParsedGroupBy(groupBy, havingConditions, endIndex);
    }

    private ParsedOrderBy extractOrderBy(String text, int orderByIndex, String defaultTableName,
            Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
            Map<String, String> columnAliases, List<SubQuery> subQueries) {
        String orderByClause = text.substring(orderByIndex + 8).trim();
        Pattern orderByLimitPattern = Pattern.compile(
                "(?i)\\s*(?:LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?|OFFSET\\s+\\d+)\\s*$", Pattern.DOTALL);
        Matcher orderByLimitMatcher = orderByLimitPattern.matcher(orderByClause);
        if (orderByLimitMatcher.find()) {
            orderByClause = orderByClause.substring(0, orderByLimitMatcher.start()).trim();
        }
        List<OrderByInfo> orderBy = parseOrderByClause(orderByClause, defaultTableName,
                combinedColumnTypes, tableAliases, columnAliases, subQueries);
        return new ParsedOrderBy(orderBy);
    }

    private ParsedLimitOffset extractLimit(String text, int limitIndex) {
        String afterLimit = text.substring(limitIndex + 5).trim();
        Pattern limitPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:(?:\\s+OFFSET\\s+\\d+)|(?:\\s*;\\s*)?\\s*$)");
        Matcher limitMatcher = limitPattern.matcher(afterLimit);
        if (!limitMatcher.find()) {
            throw new IllegalArgumentException("Недопустимый формат LIMIT: " + afterLimit);
        }
        Integer limitVal = Integer.parseInt(limitMatcher.group(1));
        Integer offsetVal = null;
        Pattern offsetTailPattern = Pattern.compile("(?i)\\s+OFFSET\\s+(\\d+)");
        Matcher offsetTailMatcher = offsetTailPattern.matcher(afterLimit);
        if (offsetTailMatcher.find()) {
            offsetVal = Integer.parseInt(offsetTailMatcher.group(1));
        }
        return new ParsedLimitOffset(limitVal, offsetVal);
    }

    private Integer extractOffset(String text, int offsetIndex) {
        String afterOffset = text.substring(offsetIndex + 6).trim();
        Pattern offsetPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:(?:\\s*;\\s*)?\\s*$)");
        Matcher offsetMatcher = offsetPattern.matcher(afterOffset);
        if (!offsetMatcher.find()) {
            throw new IllegalArgumentException("Недопустимый формат OFFSET: " + afterOffset);
        }
        return Integer.parseInt(offsetMatcher.group(1));
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        if (query == null || clause == null || query.isEmpty() || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        // Регулярные выражения для разных типов токенов
        Pattern quotedStringPattern = Pattern.compile("'([^'\\\\]*+(?:\\\\.[^'\\\\]*+)*+)'");
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern clausePattern = Pattern.compile("\\b" + Pattern.quote(clause.toUpperCase()) + "\\b");

        int parenDepth = 0;
        int lastClauseIndex = -1;
        int currentPos = 0;
        boolean inQuotes = false;

        while (currentPos < query.length()) {
            // Проверяем строки в кавычках
            Matcher quotedStringMatcher = quotedStringPattern.matcher(query).region(currentPos, query.length());
            // Проверяем скобки
            Matcher openParenMatcher = openParenPattern.matcher(query).region(currentPos, query.length());
            Matcher closeParenMatcher = closeParenPattern.matcher(query).region(currentPos, query.length());
            // Проверяем ключевое слово
            Matcher clauseMatcher = clausePattern.matcher(query.toUpperCase()).region(currentPos, query.length());

            int nextPos = query.length();
            String tokenType = null;

            // Определяем следующий токен
            if (quotedStringMatcher.lookingAt()) {
                tokenType = "quotedString";
                nextPos = quotedStringMatcher.end();
            } else if (openParenMatcher.lookingAt() && !inQuotes) {
                tokenType = "openParen";
                nextPos = openParenMatcher.end();
                parenDepth++;
            } else if (closeParenMatcher.lookingAt() && !inQuotes) {
                tokenType = "closeParen";
                nextPos = closeParenMatcher.end();
                parenDepth--;
                if (parenDepth < 0) {
                    LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}",
                            new Object[]{currentPos, query});
                    return -1;
                }
            } else if (clauseMatcher.lookingAt() && !inQuotes && parenDepth == 0) {
                tokenType = "clause";
                lastClauseIndex = currentPos;
                nextPos = clauseMatcher.end();
            }

            // Логируем токен для отладки
            //LOGGER.log(Level.FINEST, "Токен: start={0}, end={1}, type={2}, parenDepth={3}, inQuotes={4}",
            //         new Object[]{currentPos, nextPos, tokenType != null ? tokenType : "none", parenDepth, inQuotes});

            // Если токен не найден, переходим к следующему символу
            if (tokenType == null) {
                currentPos++;
                continue;
            }

            currentPos = nextPos;
        }

        if (parenDepth != 0) {
            LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе: parenDepth={0}, query={1}",
                    new Object[]{parenDepth, query});
            return -1;
        }

        if (lastClauseIndex != -1) {
            LOGGER.log(Level.FINEST, "Найдено последнее {0} на индексе {1} в запросе: {2}",
                    new Object[]{clause, lastClauseIndex, query});
            return lastClauseIndex;
        }

        LOGGER.log(Level.FINEST, "Допустимое {0} не найдено вне подзапросов в запросе: {1}",
                new Object[]{clause, query});
        return -1;
    }

    private int findOnClausePosition(String joinPart) {
        boolean inQuotes = false;
        int parenDepth = 0;
        Pattern onPattern = Pattern.compile("(?i)\\bON\\b");

        for (int i = 0; i < joinPart.length(); i++) {
            char c = joinPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (parenDepth == 0) {
                    Matcher onMatcher = onPattern.matcher(joinPart.substring(i));
                    if (onMatcher.lookingAt()) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }
    private List<String> splitSelectItems(String selectPart) {
        if (selectPart == null || selectPart.trim().isEmpty()) {
            return new ArrayList<>();
        }

        // Split on top-level commas (outside quotes and parentheses). The previous
        // regex split {@code (?=([^']*'[^']*')*[^']*$)(?![^()]*\\)),\\s*} could overflow
        // the regex stack on long inputs with many quotes.
        List<String> selectItems = new ArrayList<>();
        for (String part : splitTopLevelComma(selectPart)) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                selectItems.add(trimmed);
            }
        }

        return selectItems;
    }

    private List<OrderByInfo> parseOrderByClause(String orderByClause, String defaultTableName,
                                                 Map<String, Class<?>> combinedColumnTypes,
                                                 Map<String, String> tableAliases,
                                                 Map<String, String> columnAliases,
                                                 List<SubQuery> subQueries) {
        List<OrderByInfo> orderBy = new ArrayList<>();
        String[] orderItems = orderByClause.split(",");
        Pattern orderPattern = Pattern.compile("(?i)^\\s*(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(ASC|DESC)?\\s*$");

        for (String item : orderItems) {
            String trimmedItem = item.trim();
            Matcher orderMatcher = orderPattern.matcher(trimmedItem);
            if (!orderMatcher.matches()) {
                LOGGER.log(Level.SEVERE, "Invalid ORDER BY item: {0}", trimmedItem);
                throw new IllegalArgumentException("Invalid ORDER BY item: " + trimmedItem);
            }
            String column = unquoteQualifiedIdentifier(orderMatcher.group(1).trim());
            String direction = orderMatcher.group(2) != null ? orderMatcher.group(2).toUpperCase() : SqlKeywords.ASC;
            boolean ascending = direction.equals(SqlKeywords.ASC);

            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
            boolean found = false;

            // Проверка имени столбца
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }

            // Проверка алиаса столбца
            if (!found) {
                for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
                    if (aliasEntry.getValue().equalsIgnoreCase(unqualifiedColumn)) {
                        String actualColumn = aliasEntry.getKey();
                        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
                        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                            if (entryKeyUnqualified.equalsIgnoreCase(normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn)) {
                                found = true;
                                column = actualColumn; // Используем исходное имя столбца
                                break;
                            }
                        }
                        break;
                    }
                }
            }

            // Проверка алиаса таблицы
            if (!found && tableAliases.containsKey(unqualifiedColumn)) {
                found = true;
            }

            // Проверка алиаса подзапроса
            for (SubQuery subQuery : subQueries) {
                if (subQuery.alias != null && subQuery.alias.equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column, alias, or subquery in ORDER BY: {0}, available columns: {1}, aliases: {2}, subqueries: {3}",
                        new Object[]{unqualifiedColumn, combinedColumnTypes.keySet(), tableAliases.keySet(), subQueries});
                throw new IllegalArgumentException("Invalid column name or subquery in ORDER BY: " + unqualifiedColumn);
            }

            orderBy.add(new OrderByInfo(column, ascending));
            LOGGER.log(Level.FINE, "Parsed ORDER BY item: column={0}, ascending={1}", new Object[]{column, ascending});
        }

        return orderBy;
    }

    private List<String> parseGroupByClause(String groupByClause, String defaultTableName, Database database,
                                            Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                            Map<String, String> columnAliases, Map<String, String> groupBySubQueries) {
        List<String> groupBy = new ArrayList<>();
        List<String> items = splitSelectItems(groupByClause);
        Pattern columnPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + "|\\(\\s*SELECT\\s+[^()]*+\\))$", Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains(SqlKeywords.SELECT)) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    parse(subQueryStr, database);
                    String marker = "SUBQUERY_" + System.currentTimeMillis();
                    groupBy.add(marker);
                    if (groupBySubQueries != null) {
                        groupBySubQueries.put(marker, subQueryStr);
                    }
                } else {
                    columnOrSubQuery = unquoteQualifiedIdentifier(columnOrSubQuery);
                    for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
                        if (aliasEntry.getValue().equalsIgnoreCase(columnOrSubQuery)) {
                            columnOrSubQuery = aliasEntry.getKey();
                            break;
                        }
                    }
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, defaultTableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    groupBy.add(normalizedColumn);
                }
            } else {
                LOGGER.log(Level.SEVERE, "Недопустимый элемент GROUP BY: {0}", trimmedItem);
                throw new IllegalArgumentException("Недопустимый элемент GROUP BY: " + trimmedItem);
            }
        }
        return groupBy;
    }

    private Integer parseOffsetClause(String offsetClause) {
        String normalized = offsetClause.toUpperCase().replace(SqlKeywords.OFFSET, "").trim();
        if (normalized.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty OFFSET clause detected");
            return null;
        }
        try {
            int offsetValue = Integer.parseInt(normalized);
            if (offsetValue < 0) {
                throw new IllegalArgumentException("OFFSET value must be non-negative: " + offsetValue);
            }
            LOGGER.log(Level.FINE, "Parsed OFFSET clause: {0}", offsetValue);
            return offsetValue;
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid OFFSET value: {0}", normalized);
            throw new IllegalArgumentException("Invalid OFFSET value: " + normalized);
        }
    }

    private void validateJoinCondition(Condition condition, String leftTable, String rightTable, Map<String, String> tableAliases) {
        LOGGER.log(Level.FINEST, "Validating condition: {0} for tables {1}, {2}, aliases: {3}", new Object[]{condition, leftTable, rightTable, tableAliases});
        if (condition.isGrouped()) {
            for (Condition subCond : condition.subConditions) {
                validateJoinCondition(subCond, leftTable, rightTable, tableAliases);
            }
            return;
        }
        if (condition.column != null) {
            String prefix = condition.column.contains(".") ? condition.column.split("\\.")[0] : null;
            if (prefix != null) {
                if (!tableAliases.getOrDefault(prefix, prefix).equalsIgnoreCase(leftTable) && !tableAliases.getOrDefault(prefix, prefix).equalsIgnoreCase(rightTable)) {
                    throw new IllegalArgumentException("Invalid table or alias in ON condition: " + prefix +
                            ", expected " + leftTable + " or " + rightTable);
                }
            }
        }
        if (condition.rightColumn != null) {
            String prefix = condition.rightColumn.contains(".") ? condition.rightColumn.split("\\.")[0] : null;
            if (prefix != null) {
                if (!tableAliases.getOrDefault(prefix, prefix).equalsIgnoreCase(leftTable) && !tableAliases.getOrDefault(prefix, prefix).equalsIgnoreCase(rightTable)) {
                    throw new IllegalArgumentException("Invalid table or alias in ON condition (right column): " + prefix +
                            ", expected " + leftTable + " or " + rightTable);
                }
            }
        }
    }

    private Query<Void> parseInsertQuery(String normalized, String original, Database database) {
        String[] parts = normalized.split(SqlKeywords.VALUES);
        if (parts.length != 2) {
            throw new SyntaxErrorException("Invalid INSERT query format");
        }

        String tableAndColumns = parts[0].replace(SqlKeywords.INSERT_INTO, "").trim();
        String tableName = unquoteIdentifier(tableAndColumns.substring(0, tableAndColumns.indexOf("(")).trim());
        String columnsPart = original.substring(original.indexOf("(") + 1, original.indexOf(")")).trim();
        List<String> columns = Arrays.stream(columnsPart.split(","))
                .map(String::trim)
                .map(QueryParser::unquoteIdentifier)
                .collect(Collectors.toList());

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.TABLE_NOT_FOUND_PREFIX + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, Sequence> sequences = table.getSequences();
        String primaryKeyColumn = table.getPrimaryKeyColumn();

        if (primaryKeyColumn != null && sequences.containsKey(primaryKeyColumn) && columns.contains(primaryKeyColumn)) {
            throw new IllegalArgumentException("Cannot specify value for sequence-based primary key column: " + primaryKeyColumn);
        }

        String valuesPart = original.substring(parts[0].length() + SqlKeywords.VALUES.length()).trim();
        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            throw new IllegalArgumentException("Invalid VALUES syntax");
        }
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        List<String> valueStrings = splitTopLevelComma(valuesPart);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < valueStrings.size(); i++) {
            String val = valueStrings.get(i).trim();
            String column = columns.get(i);
            Class<?> columnType = columnTypes.get(column);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column in INSERT: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
            }
            values.add(parseConditionValue(val, columnType));
        }

        LOGGER.log(Level.INFO, "Parsed INSERT query: table={0}, columns={1}, values={2}",
                new Object[]{tableName, columns, values});

        return new InsertQuery(columns, values);
    }

    private Query<Void> parseUpdateQuery(String normalized, String original, Database database) {
        String[] parts = normalized.split(SqlKeywords.SET);
        if (parts.length != 2) {
            throw new SyntaxErrorException("Invalid UPDATE query format");
        }

        String tablePart = parts[0].replace(SqlKeywords.UPDATE, "").trim();
        String[] tableTokens = tablePart.split("\\s+");
        String tableName = unquoteIdentifier(tableTokens[0].trim());
        String tableAlias = null;
        if (tableTokens.length > 1) {
            if (tableTokens.length == 3 && tableTokens[1].equalsIgnoreCase(SqlKeywords.AS)) {
                tableAlias = unquoteIdentifier(tableTokens[2].trim());
            } else if (tableTokens.length == 2) {
                tableAlias = unquoteIdentifier(tableTokens[1].trim());
            }
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.TABLE_NOT_FOUND_PREFIX + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, String> tableAliases = new HashMap<>();
        tableAliases.put(tableName, tableName);
        if (tableAlias != null) {
            tableAliases.put(tableAlias, tableName);
            LOGGER.log(Level.FINE, "Parsed UPDATE table alias: {0} -> {1}", new Object[]{tableAlias, tableName});
        }

        String setAndWhere = original.substring(parts[0].length() + SqlKeywords.SET.length()).trim();
        String setPart;
        List<Condition> conditions = new ArrayList<>();

        if (setAndWhere.toUpperCase().contains(SqlKeywords.WHERE)) {
            String[] setWhereParts = setAndWhere.split("(?i)WHERE");
            setPart = setWhereParts[0].trim();
            String conditionStr = setWhereParts[1].trim();
            conditions = parseConditions(conditionStr, new ParseContext(tableName, database, original, false,
                    columnTypes, tableAliases, new HashMap<>()));
        } else {
            setPart = setAndWhere;
        }

        List<String> assignments = splitTopLevelComma(setPart);
        Map<String, Object> updates = new HashMap<>();
        for (String assignment : assignments) {
            String[] kv = assignment.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid SET clause");
            }
            String column = unquoteIdentifier(kv[0].trim());
            String valueStr = kv[1].trim();
            Class<?> columnType = columnTypes.get(column);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column in UPDATE: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
            }
            Object value = parseConditionValue(valueStr, columnType);
            updates.put(column, value);
        }

        LOGGER.log(Level.INFO, "Parsed UPDATE query: table={0}, alias={1}, updates={2}, conditions={3}",
                new Object[]{tableName, tableAlias, updates, conditions});

        return new UpdateQuery(updates, conditions);
    }

    private Query<Void> parseDeleteQuery(String normalized, String original, Database database) {
        LOGGER.log(Level.FINE, "Raw DELETE query: {0}", original);
        LOGGER.log(Level.FINE, "Normalized DELETE query: {0}", normalized);

        // Use normalized query for table name extraction (identifiers are uppercased)
        String[] fromParts = normalized.split("(?i)FROM\\s+", 2);
        if (fromParts.length != 2) {
            LOGGER.log(Level.SEVERE, "Invalid DELETE query format: missing FROM clause, normalized: {0}", normalized);
            throw new SyntaxErrorException("Invalid DELETE query format: missing FROM clause");
        }
        String tableAndConditionNorm = fromParts[1].trim();
        String[] wherePartsNorm = tableAndConditionNorm.split("(?i)WHERE\\s+", 2);
        String tableName = unquoteIdentifier(wherePartsNorm[0].trim());

        // Use original query for WHERE condition extraction (preserves string literal case)
        String[] fromPartsOrig = original.split("(?i)FROM\\s+", 2);
        List<Condition> conditions = new ArrayList<>();

        if (fromPartsOrig.length == 2) {
            String tableAndConditionOrig = fromPartsOrig[1].trim();
            String[] wherePartsOrig = tableAndConditionOrig.split("(?i)WHERE\\s+", 2);

            if (wherePartsOrig.length == 2) {
                String conditionStr = wherePartsOrig[1].trim();
                LOGGER.log(Level.FINE, "Parsing WHERE clause for DELETE: {0}", conditionStr);
                if (conditionStr.isEmpty()) {
                    LOGGER.log(Level.SEVERE, "Empty WHERE clause in DELETE query: {0}", original);
                    throw new IllegalArgumentException("Invalid DELETE query: empty WHERE clause");
                }
                Table table = database.getTable(tableName);
                if (table == null) {
                    throw new IllegalArgumentException(ErrorMessages.TABLE_NOT_FOUND_PREFIX + tableName);
                }
                conditions = parseConditions(conditionStr, new ParseContext(tableName, database, original, false,
                        table.getColumnTypes(), new HashMap<>(), new HashMap<>()));
            } else {
                LOGGER.log(Level.FINE, "No WHERE clause in DELETE query");
            }
        } else {
            LOGGER.log(Level.FINE, "No WHERE clause in DELETE query");
        }

        LOGGER.log(Level.INFO, "Parsed DELETE query: table={0}, conditions={1}",
                new Object[]{tableName, conditions});

        return new DeleteQuery(conditions);
    }

    private Object parseConditionValue(String valueStr, Class<?> columnType) {
        try {
            if (valueStr.equalsIgnoreCase(SqlKeywords.NULL)) {
                return null;
            }
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                return parseStringLiteral(valueStr, columnType);
            }
            if (valueStr.equalsIgnoreCase(SqlKeywords.TRUE) || valueStr.equalsIgnoreCase(SqlKeywords.FALSE)) {
                if (columnType != Boolean.class) {
                    throw new IllegalArgumentException("Boolean value '" + valueStr + ErrorMessages.TYPE_MISMATCH_SUFFIX + columnType.getSimpleName());
                }
                return Boolean.parseBoolean(valueStr);
            }
            return parseNumericLiteral(valueStr, columnType);
        } catch (IllegalArgumentException e) {
            throw e;
        }
    }

    private Object parseStringLiteral(String valueStr, Class<?> columnType) {
        String strippedValue = SqlLexer.extractStringLiteral(valueStr);
        if (columnType == String.class) return strippedValue;
        if (columnType == LocalDate.class && CharOps.isLocalDateLiteral(strippedValue)) return LocalDate.parse(strippedValue);
        if (columnType == LocalDateTime.class && CharOps.isLocalDateTimeMillisLiteral(strippedValue)) return LocalDateTime.parse(strippedValue, DATETIME_MS_FORMATTER);
        if (columnType == LocalDateTime.class && CharOps.isLocalDateTimeLiteral(strippedValue)) return LocalDateTime.parse(strippedValue, DATETIME_FORMATTER);
        if (columnType == UUID.class && strippedValue.matches(UUID_PATTERN)) return UUID.fromString(strippedValue);
        if (columnType == Character.class && strippedValue.length() == 1) return strippedValue.charAt(0);
        throw new IllegalArgumentException("Value '" + strippedValue + ErrorMessages.TYPE_MISMATCH_SUFFIX + columnType.getSimpleName());
    }

    private Object parseNumericLiteral(String valueStr, Class<?> columnType) {
        try {
            if (columnType == BigDecimal.class) {
                return new BigDecimal(valueStr);
            }
            if (columnType == Float.class) {
                return parseBoundedFloat(valueStr);
            }
            if (columnType == Double.class) {
                return parseBoundedDouble(valueStr);
            }
            if (columnType == Byte.class) {
                return parseBoundedByte(valueStr);
            }
            if (columnType == Short.class) {
                return parseBoundedShort(valueStr);
            }
            if (columnType == Integer.class) {
                return Integer.parseInt(valueStr);
            }
            if (columnType == Long.class) {
                return Long.parseLong(valueStr);
            }
            throw new IllegalArgumentException(ErrorMessages.NUMERIC_VALUE_PREFIX + valueStr + ErrorMessages.TYPE_MISMATCH_SUFFIX + columnType.getSimpleName());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value '" + valueStr + "' for column type: " + columnType.getSimpleName());
        }
    }

    private float parseBoundedFloat(String valueStr) {
        float f = new BigDecimal(valueStr).floatValue();
        if (Float.isInfinite(f) || Float.isNaN(f)) {
            throw new IllegalArgumentException(ErrorMessages.NUMERIC_VALUE_PREFIX + valueStr + "' out of range for Float");
        }
        return f;
    }

    private double parseBoundedDouble(String valueStr) {
        double d = new BigDecimal(valueStr).doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            throw new IllegalArgumentException(ErrorMessages.NUMERIC_VALUE_PREFIX + valueStr + "' out of range for Double");
        }
        return d;
    }

    private byte parseBoundedByte(String valueStr) {
        long v = Long.parseLong(valueStr);
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(ErrorMessages.NUMERIC_VALUE_PREFIX + valueStr + "' out of range for Byte");
        }
        return (byte) v;
    }

    private short parseBoundedShort(String valueStr) {
        long v = Long.parseLong(valueStr);
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
            throw new IllegalArgumentException(ErrorMessages.NUMERIC_VALUE_PREFIX + valueStr + "' out of range for Short");
        }
        return (short) v;
    }

    private List<Condition> parseConditions(String conditionStr, ParseContext ctx) {
        LOGGER.log(Level.FINE, "Начало парсинга условий: conditionStr={0}, defaultTableName={1}, isJoinCondition={2}",
                new Object[]{conditionStr, ctx.defaultTableName, ctx.isJoinCondition});

        if (conditionStr == null || conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Пустая строка условий, возвращается пустой список условий");
            return new ArrayList<>();
        }

        // Обрезаем строку до ключевых слов
        String trimmedConditionStr = trimToClause(conditionStr);
        LOGGER.log(Level.FINE, "Условие обрезано до: {0}", trimmedConditionStr);

        List<Token> tokens = tokenizeConditions(trimmedConditionStr);
        return parseTokenizedConditions(tokens, ctx, null, false);
    }

    // Обрезает строку до ключевых слов
    private String trimToClause(String conditionStr) {
        Pattern clausePattern = Pattern.compile("(?i)\\b(WHERE|LIMIT|OFFSET|ORDER BY|GROUP BY)\\b");
        Matcher clauseMatcher = clausePattern.matcher(conditionStr);
        if (clauseMatcher.find()) {
            return conditionStr.substring(0, clauseMatcher.start()).trim();
        }
        return conditionStr.trim();
    }

    // Токенизирует строку условий с использованием регулярных выражений
    private List<Token> tokenizeConditions(String conditionStr) {
        List<Map.Entry<String, Pattern>> patterns = new ArrayList<>();

        // 1. Строковые литералы (в кавычках)
        patterns.add(Map.entry("Quoted String", Pattern.compile("'(?:''|\\\\.|[^'\\\\])*+'")));

        // 2. Условия LIKE и NOT LIKE
        patterns.add(Map.entry("Like Condition",
                Pattern.compile("(?i)" + QUALIFIED_IDENTIFIER_PATTERN + "\\s*(?:NOT\\s+)?LIKE\\s*'(?:''|\\\\.|[^'\\\\])*+'")));

        // 3. Подзапросы
        patterns.add(Map.entry("SubQuery",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(NOT\\s*)?IN\\s*\\(\\s*SELECT\\s+[^)]+\\)")));

        // 4. Условия IN
        patterns.add(Map.entry("In Condition",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(NOT\\s*)?IN\\s*\\([^)]+\\)")));

        // 5. Условия NULL
        patterns.add(Map.entry("Null Condition",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*IS\\s*(NOT\\s+)?NULL\\b")));

        // 6. Условия сравнения (строки, числа, столбцы)
        patterns.add(Map.entry("Comparison String Condition",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|>|<|>=|<=|!=|<>)\\s*('(?:''|\\\\.|[^'\\\\])*+')")));
        patterns.add(Map.entry("Comparison Number Condition",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|>|<|>=|<=|!=|<>)\\s*(\\d+(?:\\.\\d+)?)")));
        patterns.add(Map.entry("Comparison Column Condition",
                Pattern.compile("(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|>|<|>=|<=|!=|<>)\\s*(" + QUALIFIED_IDENTIFIER_PATTERN + ")")));

        // 7. Логические операторы
        patterns.add(Map.entry("Logical Operator", Pattern.compile("(?i)\\b(AND|OR)\\b")));

        // 8. Ключевое слово NOT (отрицание условия)
        patterns.add(Map.entry("NOT Keyword", Pattern.compile("(?i)\\bNOT\\b")));

        // 9. Некорректные токены (исключая строковые литералы и quoted-идентификаторы)
        patterns.add(Map.entry("Invalid Token",
                Pattern.compile("(?i)(?!" + QUALIFIED_IDENTIFIER_PATTERN + "\\s*(?:=|>|<|>=|<=|!=|<>)\\s*)(?!" + QUALIFIED_IDENTIFIER_PATTERN + "\\s*(?:NOT\\s+)?(?:LIKE|IN|IS)\\b)(?!'(?:''|\\\\.|[^'\\\\])*+')(?!" + QUOTED_IDENTIFIER_PATTERN + ")[^\\s()'\"]+")));

        List<Token> tokens = new ArrayList<>();
        int currentPos = 0;
        int stringLength = conditionStr.length();

        while (currentPos < stringLength) {
            // Пропускаем пробелы
            while (currentPos < stringLength && Character.isWhitespace(conditionStr.charAt(currentPos))) {
                currentPos++;
            }
            if (currentPos >= stringLength) {
                break;
            }

            LOGGER.log(Level.FINEST, "Processing token at position {0}: {1}",
                    new Object[]{currentPos, conditionStr.substring(currentPos)});

            boolean matched = false;
            boolean handled = false;
            int nextPos = stringLength;
            String matchedToken = null;
            String matchedPatternName = null;

            // Проверяем строковые литералы первыми
            if (conditionStr.charAt(currentPos) == '\'') {
                Pattern quotedStringPattern = patterns.get(0).getValue(); // Quoted String
                Matcher qsMatcher = quotedStringPattern.matcher(conditionStr).region(currentPos, stringLength);
                if (qsMatcher.lookingAt()) {
                    String qsToken = qsMatcher.group();
                    tokens.add(new Token(TokenType.CONDITION, qsToken));
                    LOGGER.log(Level.FINEST, "Добавлен токен Quoted String: {0}", qsToken);
                    currentPos = qsMatcher.end();
                    handled = true;
                } else {
                    // Quoted string pattern didn't match — try other patterns below
                    for (Map.Entry<String, Pattern> entry : patterns.subList(1, patterns.size())) {
                        String patternName = entry.getKey();
                        Pattern pattern = entry.getValue();
                        Matcher matcher = pattern.matcher(conditionStr).region(currentPos, stringLength);
                        if (matcher.lookingAt()) {
                            String tokenValue = matcher.group().trim();
                            if (!tokenValue.isEmpty()) {
                                nextPos = matcher.end();
                                matchedToken = tokenValue;
                                matchedPatternName = patternName;
                                matched = true;
                                break;
                            }
                        }
                    }
                }
            } else if (conditionStr.charAt(currentPos) == '(') {
                // Группированные условия в скобках выделяются как единый токен (с учётом вложенности)
                int endParen = findMatchingParenthesis(conditionStr, currentPos);
                String groupedToken = conditionStr.substring(currentPos, endParen + 1);
                tokens.add(new Token(TokenType.CONDITION, groupedToken));
                LOGGER.log(Level.FINEST, "Добавлен токен группированного условия: {0}", groupedToken);
                currentPos = endParen + 1;
                handled = true;
            } else {
                // Проверяем остальные паттерны, исключая содержимое строк
                for (Map.Entry<String, Pattern> entry : patterns.subList(1, patterns.size())) {
                    String patternName = entry.getKey();
                    Pattern pattern = entry.getValue();
                    Matcher matcher = pattern.matcher(conditionStr).region(currentPos, stringLength);

                    if (matcher.lookingAt()) {
                        String tokenValue = matcher.group().trim();
                        LOGGER.log(Level.FINEST, "Паттерн '{0}' сработал, токен: {1}, конец: {2}",
                                new Object[]{patternName, tokenValue, matcher.end()});
                        if (!tokenValue.isEmpty()) {
                            nextPos = matcher.end();
                            matchedToken = tokenValue;
                            matchedPatternName = patternName;
                            matched = true;
                            break;
                        }
                    }
                }
            }

            if (handled) {
                // Token already added (quoted string or parenthesized group)
            } else if (matched) {
                if (matchedPatternName.equals("Like Condition")) {
                    Matcher likeMatcher = Pattern.compile(
                                    "(?i)(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(NOT\\s*)?LIKE\\s*('(?:''|\\\\.|[^'\\\\])*+')")
                            .matcher(matchedToken);
                    if (likeMatcher.matches()) {
                        String pattern = likeMatcher.group(3);
                        if (!pattern.endsWith("'")) {
                            LOGGER.log(Level.WARNING, "Незакрытая кавычка в LIKE шаблоне на позиции {0}: {1}",
                                    new Object[]{currentPos, matchedToken});
                            throw new IllegalArgumentException("Незакрытая кавычка в LIKE шаблоне на позиции " + currentPos + ": " + matchedToken);
                        }
                        pattern = pattern.substring(1, pattern.length() - 1);
                        validateLikePattern(pattern, currentPos);
                        tokens.add(new Token(TokenType.CONDITION, matchedToken));
                        LOGGER.log(Level.FINEST, "Добавлен токен Like Condition: {0}", matchedToken);
                    } else {
                        LOGGER.log(Level.WARNING, "Некорректный LIKE токен на позиции {0}: {1}",
                                new Object[]{currentPos, matchedToken});
                        throw new IllegalArgumentException("Некорректный LIKE токен на позиции " + currentPos + ": " + matchedToken);
                    }
                } else if (matchedPatternName.equals("Logical Operator")) {
                    tokens.add(new Token(TokenType.LOGICAL_OPERATOR, matchedToken));
                    LOGGER.log(Level.FINEST, "Добавлен токен Logical Operator: {0}", matchedToken);
                } else if (matchedPatternName.equals("Invalid Token")) {
                    LOGGER.log(Level.WARNING, "Обнаружен некорректный токен на позиции {0}: {1}",
                            new Object[]{currentPos, matchedToken});
                    throw new IllegalArgumentException("Некорректный токен в условии на позиции " + currentPos + ": " + matchedToken);
                } else {
                    tokens.add(new Token(TokenType.CONDITION, matchedToken));
                    LOGGER.log(Level.FINEST, "Добавлен токен условия: {0}", matchedToken);
                }
                currentPos = nextPos;
            } else {
                // Пропускаем пробелы или неизвестные символы
                currentPos++;
            }
        }

        if (tokens.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Не удалось выделить токены из условия: {0}", conditionStr);
            throw new IllegalArgumentException("Невалидное условие: не удалось выделить токенов из '" + conditionStr + "'");
        }

        LOGGER.log(Level.FINE, "Токенизация завершена, получено токенов: {0}, токены: {1}",
                new Object[]{tokens.size(), tokens});
        return tokens;
    }

    private void validateLikePattern(String pattern, int position) {
        if (pattern == null || pattern.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty LIKE pattern at position {0}", position);
            throw new IllegalArgumentException("LIKE pattern cannot be empty at position " + position);
        }
        // Проверка на SQL-конструкции удалена, так как строковый литерал — это просто строка
    }


    // Парсит токенизированные условия
    private List<Condition> parseTokenizedConditions(List<Token> tokens, ParseContext ctx,
                                                     String conjunction, boolean not) {
        List<Condition> conditions = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type == TokenType.LOGICAL_OPERATOR) {
                conjunction = token.value.toUpperCase();
                LOGGER.log(Level.FINEST, "Processing logical operator: {0}, setting conjunction to {1}",
                        new Object[]{token.value, conjunction});
            } else if (token.type == TokenType.CONDITION) {
                String condStr = token.value;
                if (condStr.equalsIgnoreCase(SqlKeywords.NOT)) {
                    not = true;
                    LOGGER.log(Level.FINEST, "Processing NOT keyword, negation enabled for next condition");
                } else if (condStr.toUpperCase().startsWith("'") && condStr.toUpperCase().endsWith("'")) {
                    LOGGER.log(Level.FINEST, "Skipping quoted string token: {0}", condStr);
                } else {
                    if (condStr.startsWith("(") && condStr.endsWith(")")) {
                        int endParen = findMatchingParenthesis(condStr, 0);
                        if (endParen == condStr.length() - 1) {
                            String subCondStr = condStr.substring(1, endParen).trim();
                            List<Token> subTokens = tokenizeConditions(subCondStr);
                            List<Condition> subConditions = parseTokenizedConditions(subTokens, ctx, conjunction, not);
                            conditions.add(new Condition(subConditions, conjunction, not));
                            LOGGER.log(Level.FINE, "Добавлено группированное условие: {0}", subConditions);
                        } else {
                            throw new IllegalArgumentException("Некорректная структура группированного условия: " + condStr);
                        }
                    } else if (condStr.toUpperCase().contains(" IN ")) {
                        Condition condition = parseInCondition(condStr, ctx, conjunction, not);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Добавлено условие IN: {0}", condition);
                    } else {
                        Condition condition = parseSingleCondition(condStr, ctx, conjunction, not, condStr);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Добавлено одиночное условие: {0}", condition);
                    }
                    conjunction = null;
                    not = false;
                }
            }
        }
        return conditions;
    }

    private void validateSubquery(String subquery) {
        if (!subquery.startsWith("(") || !subquery.endsWith(")")) {
            LOGGER.log(Level.SEVERE, "Subquery does not start with '(' or end with ')': {0}", subquery);
            throw new IllegalArgumentException("Invalid subquery syntax: " + subquery);
        }
        long openParen = subquery.chars().filter(c -> c == '(').count();
        long closeParen = subquery.chars().filter(c -> c == ')').count();
        if (openParen != closeParen) {
            LOGGER.log(Level.SEVERE, "Unbalanced parentheses in subquery: {0}, open={1}, close={2}",
                    new Object[]{subquery, openParen, closeParen});
            throw new IllegalArgumentException("Unbalanced parentheses in subquery: " + subquery);
        }
        String upperSubquery = subquery.toUpperCase();
        if (!upperSubquery.contains(SqlKeywords.SELECT + " ") || !upperSubquery.contains("FROM ")) {
            LOGGER.log(Level.SEVERE, "Subquery missing SELECT or FROM clause: {0}", subquery);
            throw new IllegalArgumentException("Subquery must contain SELECT and FROM clauses: " + subquery);
        }
        int selectIndex = upperSubquery.indexOf(SqlKeywords.SELECT + " ");
        int fromIndex = upperSubquery.indexOf("FROM ", selectIndex);
        if (fromIndex == -1 || fromIndex < selectIndex) {
            LOGGER.log(Level.SEVERE, "Invalid subquery structure: SELECT and FROM out of order in {0}", subquery);
            throw new IllegalArgumentException("Invalid subquery structure: " + subquery);
        }
        LOGGER.log(Level.FINE, "Validated subquery: {0}", subquery);
    }

    private Operator parseOperator(String operatorStr) {
        return SqlParsingUtils.parseOperator(operatorStr);
    }

    private void validateColumn(String column, Map<String, Class<?>> combinedColumnTypes) {
        SqlParsingUtils.validateColumn(column, combinedColumnTypes);
    }

    private String getNextToken(String conditionStr, int startIndex) {
        if (conditionStr == null || startIndex < 0 || startIndex >= conditionStr.length()) {
            LOGGER.log(Level.FINE, "Invalid input for getNextToken: conditionStr={0}, startIndex={1}",
                    new Object[]{conditionStr, startIndex});
            return "";
        }

        // Improved regex pattern to better handle nested parentheses and subqueries
        // Possessive quantifiers (++/*+) keep matching linear on deeply nested
        // parentheses and on backslash-heavy strings (java:S5998).
        Pattern tokenPattern = Pattern.compile(
                "(?s)(?:'(?:''|\\\\.|[^'\\\\])*+'|" +              // Match quoted strings
                        "\\((?:[^()']++|'(?:''|\\\\.|[^'\\\\])*+')*+\\)|" +   // Match balanced parentheses (including nested ones)
                        "[^\\s()']++)"                               // Match other tokens
        );

        // Find tokens starting from the given index
        Matcher matcher = tokenPattern.matcher(conditionStr.substring(startIndex));

        if (matcher.find()) {
            String token = matcher.group().trim();
            LOGGER.log(Level.FINE, "Extracted token from index {0}: {1}",
                    new Object[]{startIndex, token});
            return token;
        }

        LOGGER.log(Level.FINE, "No token found from index {0}", startIndex);
        return "";
    }

    private Condition parseSingleCondition(String condStr, ParseContext ctx,
                                           String conjunction, boolean not, String conditionStr) {
        LOGGER.log(Level.FINEST, "Parsing single condition: {0}, full condition={1}", new Object[]{condStr, conditionStr});

        String normalizedCondStr = normalizeCondition(condStr);
        if (isGroupedCondition(normalizedCondStr)) {
            return parseGroupedCondition(normalizedCondStr, ctx, conjunction, not);
        }

        if (isInCondition(normalizedCondStr)) {
            return parseInCondition(normalizedCondStr, ctx, conjunction, not);
        }

        if (isNullCondition(normalizedCondStr)) {
            return parseNullCondition(normalizedCondStr, ctx.defaultTableName, ctx.combinedColumnTypes,
                    ctx.tableAliases, ctx.columnAliases, conjunction, not);
        }

        if (isSubQueryCondition(normalizedCondStr)) {
            return parseSubQueryCondition(normalizedCondStr, ctx, conjunction, not);
        }

        // Проверка на корректность шаблона LIKE
        Pattern likePattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(LIKE|NOT LIKE)\\s*('(?:''|[^'])*+')");
        Matcher likeMatcher = likePattern.matcher(normalizedCondStr);
        if (likeMatcher.matches()) {
            String column = unquoteQualifiedIdentifier(likeMatcher.group(1).trim());
            String operatorStr = likeMatcher.group(2).toUpperCase();
            String value = likeMatcher.group(3).substring(1, likeMatcher.group(3).length() - 1); // Удаляем кавычки
            String actualColumn = resolveColumnAlias(column, ctx.columnAliases);
            String normalizedColumn = normalizeColumnName(actualColumn, ctx.defaultTableName, ctx.tableAliases);
            validateColumn(normalizedColumn, ctx.combinedColumnTypes);

            Operator operator = operatorStr.equals(SqlKeywords.LIKE) ? Operator.LIKE : Operator.NOT_LIKE;
            Object parsedValue = parseConditionValue("'" + value + "'",
                    getColumnType(actualColumn, ctx.combinedColumnTypes, ctx.defaultTableName, ctx.tableAliases, ctx.columnAliases));

            return new Condition(actualColumn, parsedValue, operator, conjunction, not);
        }

        LOGGER.log(Level.FINEST, "Передача в parseComparisonCondition: condStr={0}", normalizedCondStr);
        return parseComparisonCondition(normalizedCondStr, ctx, conjunction, not);
    }

    private boolean isGroupedCondition(String condStr) {
        return condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")");
    }

    private Condition parseGroupedCondition(String condStr, ParseContext ctx,
                                            String conjunction, boolean not) {
        String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
        if (subCondStr.isEmpty()) {
            throw new IllegalArgumentException("Пустое группированное условие: " + condStr);
        }
        List<Condition> subConditions = parseConditions(subCondStr, ctx);
        if (subConditions.isEmpty()) {
            throw new IllegalArgumentException("Не удалось разобрать подусловия в группированном условии: " + subCondStr);
        }
        return new Condition(subConditions, conjunction, not);
    }

    private boolean isInCondition(String condStr) {
        Pattern inPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+(NOT\\s+)?IN\\s*\\(([^)]*+)\\)$");
        return inPattern.matcher(condStr).matches();
    }

    private Condition parseInCondition(String condStr, ParseContext ctx, String conjunction, boolean not) {
        Pattern inPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+(NOT\\s+)?IN\\s*\\(([^)]*+)\\)$");
        Matcher inMatcher = inPattern.matcher(condStr);
        if (!inMatcher.matches()) {
            throw new IllegalArgumentException("Invalid IN condition format: " + condStr);
        }

        String column = unquoteQualifiedIdentifier(inMatcher.group(1).trim());
        boolean inNot = inMatcher.group(2) != null;
        String valuesStr = inMatcher.group(3).trim();
        String actualColumn = resolveColumnAlias(column, ctx.columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, ctx.defaultTableName, ctx.tableAliases);
        Class<?> columnType = getColumnType(normalizedColumn, ctx.combinedColumnTypes, ctx.defaultTableName,
                ctx.tableAliases, ctx.columnAliases);

        if (valuesStr.trim().toUpperCase().startsWith(SqlKeywords.SELECT + " ")) {
            String subQueryStr = valuesStr.trim();
            if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                int subQueryEnd = findMatchingParenthesis(subQueryStr, 0);
                if (subQueryEnd != subQueryStr.length() - 1) {
                    throw new IllegalArgumentException("Invalid subquery syntax in IN condition: " + subQueryStr);
                }
                subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
            }
            validateSubquery(subQueryStr);
            Query<?> subQuery = parse(subQueryStr, ctx.database);
            LOGGER.log(Level.FINE, "Parsed IN subquery: {0}", subQueryStr);
            return new Condition(actualColumn, new SubQuery(subQuery, null), conjunction, inNot);
        }

        // Разделение списка значений с учётом кавычек
        List<String> valueParts = splitInValues(valuesStr);
        List<Object> inValues = new ArrayList<>();
        for (String val : valueParts) {
            String trimmedVal = val.trim();
            if (trimmedVal.isEmpty()) continue;
            Object value = parseConditionValue(trimmedVal, columnType);
            inValues.add(value);
        }
        if (inValues.isEmpty()) {
            throw new IllegalArgumentException("Empty IN list in: " + condStr);
        }

        LOGGER.log(Level.FINE, "Parsed IN condition: column={0}, values={1}", new Object[]{actualColumn, inValues});
        return new Condition(actualColumn, inValues, conjunction, inNot);
    }

    private List<String> splitInValues(String input) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                String value = current.toString().trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Добавляем последнее значение
        String value = current.toString().trim();
        if (!value.isEmpty()) {
            values.add(value);
        }

        return values;
    }

    private boolean isNullCondition(String condStr) {
        Pattern isNullPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+IS\\s+(NOT\\s+)?NULL\\b");
        return isNullPattern.matcher(condStr).matches();
    }

    private Condition parseNullCondition(String condStr, String defaultTableName, Map<String, Class<?>> combinedColumnTypes,
                                         Map<String, String> tableAliases, Map<String, String> columnAliases,
                                         String conjunction, boolean not) {
        Pattern isNullPattern = Pattern.compile("(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(condStr);
        isNullMatcher.matches();
        String column = unquoteQualifiedIdentifier(isNullMatcher.group(1).trim());
        boolean isNotNull = isNullMatcher.group(2) != null;
        String actualColumn = resolveColumnAlias(column, columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);
        return new Condition(actualColumn, isNotNull ? Operator.IS_NOT_NULL : Operator.IS_NULL, conjunction, not);
    }

    private boolean isSubQueryCondition(String condStr) {
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|!=|<>|>=|<=|<|>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+[^)]*+)\\)$",
                Pattern.DOTALL
        );
        return subQueryPattern.matcher(condStr).matches();
    }

    private Condition parseSubQueryCondition(String condStr, ParseContext ctx, String conjunction, boolean not) {
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^(" + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|!=|<>|>=|<=|<|>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+[^)]*+)\\)$",
                Pattern.DOTALL
        );
        Matcher subQueryMatcher = subQueryPattern.matcher(condStr);
        subQueryMatcher.matches();
        String column = unquoteQualifiedIdentifier(subQueryMatcher.group(1).trim());
        String operatorStr = subQueryMatcher.group(2).trim();
        String subQueryStr = subQueryMatcher.group(3).trim();

        validateSubquery("(" + subQueryStr + ")");
        Query<?> subQuery = new Query<List<?>>() {
            @Override
            public List<?> execute(Table table) {
                throw new UnsupportedOperationException("Subquery execution should be handled by SelectQuery: " + subQueryStr);
            }
            @Override
            public String toString() {
                return subQueryStr;
            }
        };
        SubQuery newSubQuery = new SubQuery(subQuery, null);

        Operator operator = parseOperator(operatorStr);
        String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
        validateColumn(normalizedColumn, ctx.combinedColumnTypes);

        LOGGER.log(Level.FINE, "Parsed subquery condition: column={0}, operator={1}, subQuery={2}",
                new Object[]{normalizedColumn, operator, subQueryStr});
        return new Condition(normalizedColumn, newSubQuery, operator, conjunction, not);
    }

    private Condition parseComparisonCondition(String condStr, ParseContext ctx,
                                               String conjunction, boolean not) {
        LOGGER.log(Level.FINEST, "Parsing comparison condition: {0}", condStr);
        String[] operators = {"!=", "<>", ">=", "<=", "=", "<", ">", "\\bLIKE\\b", "\\bNOT LIKE\\b"};
        OperatorInfo operatorInfo = findOperator(condStr, operators);
        if (operatorInfo == null) {
            LOGGER.log(Level.SEVERE, "No valid operator found in condition: {0}", condStr);
            throw new IllegalArgumentException("Invalid condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorInfo.index).trim();
        String rightPart = condStr.substring(operatorInfo.endIndex).trim();
        String actualRightPart = trimRightPart(rightPart);

        String column = unquoteQualifiedIdentifier(leftPart);
        String actualColumn = resolveColumnAlias(column, ctx.columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, ctx.defaultTableName, ctx.tableAliases);
        validateColumn(normalizedColumn, ctx.combinedColumnTypes);

        String rightColumn = null;
        RightPart rightPartResult = parseRightPart(actualRightPart, actualColumn, ctx);
        rightColumn = rightPartResult.column();
        Object value = rightPartResult.value();

        Operator operator = parseOperator(operatorInfo.operator);

        if (ctx.isJoinCondition && !rightColumnIsFromDifferentTable(actualColumn, rightColumn, ctx.tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + condStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, ctx.defaultTableName, ctx.tableAliases);
            validateColumn(normalizedRightColumn, ctx.combinedColumnTypes);
            return new Condition(actualColumn, rightColumn, operator, conjunction, not);
        } else {
            return new Condition(actualColumn, value, operator, conjunction, not);
        }
    }

    private record RightPart(String column, Object value) {}

    private RightPart parseRightPart(String rightPart, String actualColumn, ParseContext ctx) {
        Pattern columnPattern = Pattern.compile("(?i)^" + QUALIFIED_IDENTIFIER_PATTERN + "$");
        String upperRightPart = rightPart.toUpperCase();

        if (upperRightPart.equals(SqlKeywords.TRUE) || upperRightPart.equals(SqlKeywords.FALSE) || upperRightPart.equals(SqlKeywords.NULL)) {
            Class<?> literalColumnType = getColumnType(actualColumn, ctx.combinedColumnTypes, ctx.defaultTableName,
                    ctx.tableAliases, ctx.columnAliases);
            if (upperRightPart.equals(SqlKeywords.NULL)) {
                return new RightPart(null, null);
            } else if (literalColumnType == Boolean.class) {
                return new RightPart(null, Boolean.parseBoolean(rightPart));
            } else {
                throw new IllegalArgumentException("Boolean value '" + rightPart + ErrorMessages.TYPE_MISMATCH_SUFFIX + literalColumnType.getSimpleName());
            }
        } else if (columnPattern.matcher(rightPart).matches()) {
            return new RightPart(unquoteQualifiedIdentifier(rightPart), null);
        } else {
            try {
                return new RightPart(null, parseConditionValue(rightPart,
                        getColumnType(actualColumn, ctx.combinedColumnTypes, ctx.defaultTableName,
                                ctx.tableAliases, ctx.columnAliases)));
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.WARNING, "Failed to parse rightPart as value, rechecking as column: rightPart={0}, error={1}",
                        new Object[]{rightPart, e.getMessage()});
                if (columnPattern.matcher(rightPart).matches()) {
                    return new RightPart(unquoteQualifiedIdentifier(rightPart), null);
                } else {
                    throw e;
                }
            }
        }
    }

    private String resolveRightColumn(String rightPart) {
        return unquoteQualifiedIdentifier(rightPart);
    }

    private String resolveColumnAlias(String column, Map<String, String> columnAliases) {
        String[] columnParts = column.split("\\.");
        String columnName = column.contains(".") ? columnParts[1] : columnParts[0];
        return columnAliases.entrySet().stream()
                .filter(entry -> entry.getValue().equalsIgnoreCase(columnName))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(column);
    }
    private OperatorInfo findOperator(String condStr, String[] operators) {
        int parenDepth = 0;
        boolean inQuotes = false;
        int subQueryStart = -1;

        for (int i = 0; i < condStr.length(); i++) {
            char c = condStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '(') {
                    if (parenDepth == 1 && i + 7 < condStr.length() &&
                            condStr.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        subQueryStart = i;
                    }
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0 && subQueryStart != -1) {
                        subQueryStart = -1;
                    }
                } else if (parenDepth == 0 && subQueryStart == -1 && i < condStr.length() - 1) {
                    OperatorInfo opInfo = tryMatchOperatorAt(condStr, i, operators);
                    if (opInfo != null) {
                        return opInfo;
                    }
                }
            }
        }
        return null;
    }

    private OperatorInfo tryMatchOperatorAt(String condStr, int i, String[] operators) {
        for (String op : operators) {
            String patternStr = op.startsWith("\\b") ? "\\b" + op.substring(2, op.length() - 2) + "\\b" : Pattern.quote(op);
            Pattern opPattern = Pattern.compile("(?i)" + patternStr + "(?=\\s|$|[^\\s])");
            Matcher opMatcher = opPattern.matcher(condStr.substring(i));
            if (opMatcher.lookingAt()) {
                String remaining = condStr.substring(i + opMatcher.group().length()).trim();
                if (!remaining.isEmpty() && !remaining.toUpperCase().startsWith("(SELECT")) {
                    return new OperatorInfo(opMatcher.group().trim(), i, i + opMatcher.group().length());
                }
            }
        }
        return null;
    }

    private String trimRightPart(String rightPart) {
        Pattern keywordPattern = Pattern.compile(
                "\\b(WHERE|LIMIT|OFFSET|ORDER BY|GROUP BY|AND|OR)\\b",
                Pattern.CASE_INSENSITIVE
        );
        Matcher keywordMatcher = keywordPattern.matcher(rightPart);
        if (keywordMatcher.find()) {
            return rightPart.substring(0, keywordMatcher.start()).trim();
        }
        return rightPart.trim();
    }

    private boolean rightColumnIsFromDifferentTable(String leftColumn, String rightColumn, Map<String, String> tableAliases) {
        if (rightColumn == null) {
            return true;
        }
        String leftPrefix = leftColumn.contains(".") ? leftColumn.split("\\.")[0] : null;
        String rightPrefix = rightColumn.contains(".") ? rightColumn.split("\\.")[0] : null;
        if (leftPrefix == null || rightPrefix == null) {
            return true;
        }
        // Compare aliases directly instead of resolved table names
        return !leftPrefix.equalsIgnoreCase(rightPrefix);
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> combinedColumnTypes, String defaultTableName,
                                   Map<String, String> tableAliases, Map<String, String> columnAliases) {
        // Resolve alias
        String actualColumn = columnAliases.entrySet().stream()
                .filter(entry -> entry.getValue().equalsIgnoreCase(column.split("\\.")[column.contains(".") ? 1 : 0]))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(column);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
            if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
    }

    private String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        return SqlParsingUtils.normalizeColumnName(column, defaultTableName, tableAliases);
    }

    private List<HavingCondition> parseHavingConditions(String havingClause, ParseContext ctx,
                                                    List<AggregateFunction> aggregates) {
        List<HavingCondition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        String conjunction = null;
        boolean not = false;
        boolean inAggregateCall = false;
        int subQueryStart = -1; // Added declaration

        for (int i = 0; i < havingClause.length(); i++) {
            char c = havingClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
            } else if (!inQuotes && c == '(') {
                if (parenDepth == 0 && !inAggregateCall && i > 0
                        && (Character.isLetterOrDigit(havingClause.charAt(i - 1)) || havingClause.charAt(i - 1) == '_')) {
                    inAggregateCall = true;
                } else {
                    parenDepth++;
                    if (parenDepth == 1 && i + 7 < havingClause.length() && havingClause.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        subQueryStart = i;
                    }
                }
                currentCondition.append(c);
            } else if (!inQuotes && c == ')') {
                if (parenDepth == 0 && inAggregateCall) {
                    inAggregateCall = false;
                    currentCondition.append(c);
                } else {
                    parenDepth--;
                    if (parenDepth == 0 && subQueryStart != -1) {
                        subQueryStart = -1;
                    }
                    if (parenDepth == 0 && currentCondition.length() > 0) {
                        currentCondition.append(c);
                        String condStr = currentCondition.toString().trim();
                        if (condStr.startsWith("(") && condStr.endsWith(")")) {
                            condStr = condStr.substring(1, condStr.length() - 1).trim();
                            if (!condStr.isEmpty()) {
                                List<HavingCondition> subConditions = parseHavingConditions(condStr, ctx, aggregates);
                                conditions.add(new HavingCondition(subConditions, conjunction, not));
                                LOGGER.log(Level.FINE, "Parsed grouped HAVING condition: {0}, conjunction={1}, not={2}",
                                        new Object[]{subConditions, conjunction, not});
                            }
                        }
                        currentCondition = new StringBuilder();
                        conjunction = null;
                        not = false;
                    } else {
                        currentCondition.append(c);
                    }
                }
            } else if (!inQuotes && parenDepth == 0 && c == ' ' && subQueryStart == -1) {
                String nextToken = getNextToken(havingClause, i + 1);
                if (nextToken.equalsIgnoreCase(SqlKeywords.AND) || nextToken.equalsIgnoreCase(SqlKeywords.OR)) {
                    String condStr = currentCondition.toString().trim();
                    if (!condStr.isEmpty()) {
                        HavingCondition condition = parseSingleHavingCondition(condStr, ctx, aggregates, conjunction, not);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Parsed HAVING condition: {0}", condition);
                    }
                    conjunction = nextToken.toUpperCase();
                    not = false;
                    currentCondition = new StringBuilder();
                    i += nextToken.length();
                } else if (nextToken.equalsIgnoreCase(SqlKeywords.NOT)) {
                    not = true;
                    currentCondition.append(c);
                    i += nextToken.length();
                } else if ((nextToken.equalsIgnoreCase("ORDER") && getNextToken(havingClause, i + nextToken.length() + 2).equalsIgnoreCase("BY")) ||
                        (nextToken.equalsIgnoreCase(SqlKeywords.LIMIT) && subQueryStart == -1) ||
                        (nextToken.equalsIgnoreCase(SqlKeywords.OFFSET) && subQueryStart == -1)) {
                    String condStr = currentCondition.toString().trim();
                    if (!condStr.isEmpty()) {
                        HavingCondition condition = parseSingleHavingCondition(condStr, ctx, aggregates, conjunction, not);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Parsed HAVING condition before LIMIT/OFFSET/ORDER BY: {0}", condition);
                    }
                    break;
                } else {
                    currentCondition.append(c);
                }
            } else {
                currentCondition.append(c);
            }
        }

        String finalCondStr = currentCondition.toString().trim();
        if (!finalCondStr.isEmpty()) {
            HavingCondition condition = parseSingleHavingCondition(finalCondStr, ctx, aggregates, conjunction, not);
            conditions.add(condition);
            LOGGER.log(Level.FINE, "Parsed final HAVING condition: {0}", condition);
        }

        return conditions;
    }

    private HavingCondition parseSingleHavingCondition(String condStr, ParseContext ctx,
                                                       List<AggregateFunction> aggregates,
                                                       String conjunction, boolean not) {
        if (condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")")) {
            String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
            List<HavingCondition> subConditions = parseHavingConditions(subCondStr, ctx, aggregates);
            return new HavingCondition(subConditions, conjunction, not);
        }

        String[] operators = {"=", "!=", "<>", ">=", "<=", "<", ">"};
        String selectedOperator = null;
        int operatorIndex = -1;
        for (String op : operators) {
            Pattern opPattern = Pattern.compile("(?i)\\s+" + Pattern.quote(op) + "\\s+");
            Matcher opMatcher = opPattern.matcher(" " + condStr + " ");
            if (opMatcher.find()) {
                selectedOperator = op;
                operatorIndex = opMatcher.start();
                break;
            }
        }

        if (operatorIndex == -1) {
            throw new IllegalArgumentException("Invalid HAVING condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorIndex).trim();
        String rightPart = condStr.substring(operatorIndex + selectedOperator.length()).trim();

        AggregateFunction aggregate = null;
        for (AggregateFunction agg : aggregates) {
            String aggStr = agg.toString();
            if (aggStr.equalsIgnoreCase(leftPart) || (agg.alias != null && agg.alias.equalsIgnoreCase(leftPart))) {
                aggregate = agg;
                break;
            }
        }

        if (aggregate == null) {
            Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(" + QUALIFIED_IDENTIFIER_PATTERN + "|\\*|\\([^()]*+\\))\\s*\\)(?:\\s+AS\\s+(" + IDENTIFIER_PATTERN + "))?$");
            Matcher aggMatcher = aggPattern.matcher(leftPart);
            if (aggMatcher.matches()) {
                String funcName = aggMatcher.group(1);
                String columnOrSubQuery = aggMatcher.group(2);
                String alias = unquoteIdentifier(aggMatcher.group(3));
                if (columnOrSubQuery.equals("*")) {
                    aggregate = new AggregateFunction(funcName, (String) null, alias);
                } else if (columnOrSubQuery.startsWith("(") && columnOrSubQuery.endsWith(")")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, ctx.database);
                    aggregate = new AggregateFunction(funcName, new SubQuery(subQuery, null), alias);
                } else {
                    columnOrSubQuery = unquoteQualifiedIdentifier(columnOrSubQuery);
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, ctx.defaultTableName, ctx.tableAliases);
                    String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                    boolean found = false;
                    for (Map.Entry<String, Class<?>> entry : ctx.combinedColumnTypes.entrySet()) {
                        String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                        if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Unknown column in HAVING aggregate: " + columnOrSubQuery);
                    }
                    aggregate = new AggregateFunction(funcName, columnOrSubQuery, alias);
                }
            } else {
                throw new IllegalArgumentException("Invalid HAVING condition: left side must be an aggregate function: " + leftPart);
            }
        }

        Class<?> valueType = aggregate.functionName.equals(SqlKeywords.COUNT) ? Long.class :
                (aggregate.column != null ? getColumnType(aggregate.column, ctx.combinedColumnTypes, ctx.defaultTableName,
                        ctx.tableAliases, ctx.columnAliases) : Double.class);
        Object value = parseConditionValue(rightPart, valueType);

        Operator operator;
        switch (selectedOperator) {
            case "=":
                operator = Operator.EQUALS;
                break;
            case "!=":
            case "<>":
                operator = Operator.NOT_EQUALS;
                break;
            case "<":
                operator = Operator.LESS_THAN;
                break;
            case ">":
                operator = Operator.GREATER_THAN;
                break;
            case "<=":
                operator = Operator.LESS_THAN_OR_EQUALS;
                break;
            case ">=":
                operator = Operator.GREATER_THAN_OR_EQUALS;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator in HAVING: " + selectedOperator);
        }

        return new HavingCondition(aggregate, operator, value, conjunction, not);
    }

    private int findMatchingParenthesis(String str, int startIndex) {
        if (str == null || startIndex < 0 || startIndex >= str.length() || str.charAt(startIndex) != '(') {
            LOGGER.log(Level.SEVERE, "Недопустимый вход для findMatchingParenthesis: str={0}, startIndex={1}", new Object[]{str, startIndex});
            throw new IllegalArgumentException("Недопустимый вход для findMatchingParenthesis: startIndex должен указывать на открывающую скобку");
        }

        // Проверка, что строка, начиная с startIndex, похожа на подзапрос
        Pattern subQueryPattern = Pattern.compile("\\s*\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);
        String fromStart = startIndex + 7 < str.length() ? str.substring(startIndex, startIndex + 7) : "";
        if (!subQueryPattern.matcher(fromStart).lookingAt()) {
            LOGGER.log(Level.FINE, "Строка в startIndex не похожа на подзапрос: {0}", fromStart);
        }

        int parenDepth = 0;
        boolean inQuotes = false;

        for (int i = startIndex; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0) {
                        String subQueryStr = str.substring(startIndex, i + 1);
                        // Проверка структуры подзапроса с использованием регулярного выражения
                        Pattern selectPattern = Pattern.compile(
                                "\\s*\\(\\s*SELECT\\s+[^()]*+\\s+FROM\\s+[^()]*+\\s*\\)",
                                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
                        );
                        if (!selectPattern.matcher(subQueryStr).matches()) {
                            LOGGER.log(Level.WARNING, "Подзапрос может быть некорректным: {0}", subQueryStr);
                        }
                        LOGGER.log(Level.FINE, "Найдена парная закрывающая скобка на индексе {0} для подзапроса: {1}",
                                new Object[]{i, subQueryStr});
                        return i;
                    }
                }
            }
        }

        LOGGER.log(Level.SEVERE, "Парная закрывающая скобка не найдена: str={0}, startIndex={1}",
                new Object[]{str, startIndex});
        throw new IllegalArgumentException("Парная закрывающая скобка не найдена в строке: " + str.substring(startIndex));
    }

    private String normalizeCondition(String condition) {
        if (condition == null || condition.isEmpty()) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        boolean inSubQuery = false;
        int parenDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < condition.length(); i++) {
            char c = condition.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                result.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    if (parenDepth == 1 && i + 7 < condition.length() && condition.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        inSubQuery = true;
                    }
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0 && inSubQuery) {
                        inSubQuery = false;
                    }
                }
            }
            result.append(c);
        }

        String normalized = result.toString();
        if (!inSubQuery) {
            normalized = normalized.replaceAll("(?i)\\bEQUALS\\b", "=")
                    .replaceAll("(?i)\\bNOT_EQUALS\\b", "!=")
                    .replaceAll("(?i)\\bGREATER_THAN\\b", ">")
                    .replaceAll("(?i)\\bLIKE\\b", SqlKeywords.LIKE)
                    .replaceAll("(?i)\\bNOT_LIKE\\b", SqlKeywords.NOT_LIKE);
        }

        StringBuilder finalResult = new StringBuilder();
        parenDepth = 0;
        inQuotes = false;
        inSubQuery = false;
        for (int i = 0; i < normalized.length(); i++) {
            char c = normalized.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                finalResult.append(c);
            } else {
                if (!inQuotes) {
                    if (c == '(') {
                        parenDepth++;
                        if (parenDepth == 1 && i + 7 < normalized.length() && normalized.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                            inSubQuery = true;
                        }
                    } else if (c == ')') {
                        parenDepth--;
                        if (parenDepth == 0 && inSubQuery) {
                            inSubQuery = false;
                        }
                    }
                }
                if (!inSubQuery && Character.isWhitespace(c) && finalResult.length() > 0 && Character.isWhitespace(finalResult.charAt(finalResult.length() - 1))) {
                    // collapse consecutive whitespace outside subqueries
                } else {
                    finalResult.append(c);
                }
            }
        }

        return finalResult.toString().trim();
    }

    private String normalizeQueryString(String query) {
        String normalized = query.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*([=><!(),])\\s*", "$1")
                .replaceAll("(?i)\\bEQUALS\\b", "=")
                .replaceAll("(?i)\\bNOT_EQUALS\\b", "!=")
                .replaceAll("(?i)\\bGREATER_THAN\\b", ">") // Add this line
                .replaceAll("(?i)\\bLIKE\\b", SqlKeywords.LIKE)
                .replaceAll("(?i)\\bNOT_LIKE\\b", SqlKeywords.NOT_LIKE)
                .replaceAll("\\s*;", "")
                .replaceAll("(?i)\\bLIMIT\\s*(\\d+)\\b", " LIMIT $1 ")
                .replaceAll("(?i)\\bWHERE\\b", " WHERE ")
                .replaceAll("(?i)\\bFROM\\b", " FROM ")
                .replaceAll("(?i)\\bSELECT\\b", " SELECT ")
                .replaceAll("(?i)\\bAS\\b", " AS ")
                .replaceAll("\\(\\s+", "(")
                .replaceAll("\\s+\\)", ")")
                .replaceAll("(?i)\\bID\\s*=\\s*U\\.ID\\b", "ID=U.ID")
                .replaceAll("(?i)\\bU\\.ID\\s*=\\s*ID\\b", "ID=U.ID");
        normalized = toUpperCasePreservingQuotedIdentifiers(normalized);
        return normalized
                .replaceAll("\\s*=", "=")
                .replaceAll("=\\s*", "=");
    }

    /**
     * Uppercases every character of the input except the contents of
     * double-quoted identifiers, which keep their original case.
     */
    static String toUpperCasePreservingQuotedIdentifiers(String input) {
        if (input == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(input.length());
        boolean inQuotedIdentifier = false;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '"') {
                inQuotedIdentifier = !inQuotedIdentifier;
                sb.append(c);
            } else {
                sb.append(inQuotedIdentifier ? c : Character.toUpperCase(c));
            }
        }
        return sb.toString();
    }

    /**
     * Removes the surrounding double quotes from a quoted identifier.
     * Identifiers that are not quoted are returned unchanged.
     */
    static String unquoteIdentifier(String identifier) {
        return SqlParsingUtils.unquoteIdentifier(identifier);
    }

    /**
     * Removes the surrounding double quotes from each part of a possibly
     * qualified (table.column) identifier.
     */
    private static String unquoteQualifiedIdentifier(String identifier) {
        return SqlParsingUtils.unquoteQualifiedIdentifier(identifier);
    }
}