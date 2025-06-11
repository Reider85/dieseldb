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
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private String originalQuery;
    private static final String[] OPERATORS = {"!=", "<>", ">=", "<=", "=", "<", ">", "LIKE", "NOT LIKE"};

    enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS,
        IN, LIKE, NOT_LIKE, IS_NULL, IS_NOT_NULL
    }

    enum JoinType {
        INNER, LEFT_INNER, RIGHT_INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }

    // Вспомогательный класс для токенов
    private enum TokenType {
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
                    case LIKE -> "LIKE";
                    case NOT_LIKE -> "NOT LIKE";
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
                    case LIKE -> "LIKE";
                    case NOT_LIKE -> "NOT LIKE";
                    default -> operator.toString();
                };
                return (not ? "NOT " : "") + column + " " + operatorStr + " " + subQuery.toString() + (conjunction != null ? " " + conjunction : "");
            }
            String operatorStr = switch (operator) {
                case LIKE -> "LIKE";
                case NOT_LIKE -> "NOT LIKE";
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
    private static class SelectItems {
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
    private static class TableJoins {
        String tableName;
        String tableAlias;
        List<JoinInfo> joins;
        Map<String, String> tableAliases;
        Map<String, Class<?>> combinedColumnTypes;

        TableJoins(String tableName, String tableAlias, List<JoinInfo> joins, Map<String, String> tableAliases,
                   Map<String, Class<?>> combinedColumnTypes) {
            this.tableName = tableName;
            this.tableAlias = tableAlias;
            this.joins = joins;
            this.tableAliases = tableAliases;
            this.combinedColumnTypes = combinedColumnTypes;
        }
    }

    // Вспомогательный класс для хранения дополнительных клауз
    private static class AdditionalClauses {
        List<Condition> conditions;
        List<String> groupBy;
        List<HavingCondition> havingConditions;
        List<OrderByInfo> orderBy;
        Integer limit;
        Integer offset;

        AdditionalClauses(List<Condition> conditions, List<String> groupBy, List<HavingCondition> havingConditions,
                          List<OrderByInfo> orderBy, Integer limit, Integer offset) {
            this.conditions = conditions;
            this.groupBy = groupBy;
            this.havingConditions = havingConditions;
            this.orderBy = orderBy;
            this.limit = limit;
            this.offset = offset;
        }
    }

    private static class OperatorInfo {
        String operator;
        int index;
        int endIndex;

        OperatorInfo(String operator, int index, int endIndex) {
            this.operator = operator;
            this.index = index;
            this.endIndex = endIndex;
        }
    }

    private static class Token {
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
                .replace("%", ".*")
                .replace("_", ".");

        return "^" + regex + "$";
    }

    public Query<?> parse(String query, Database database) {
        try {
            // Normalize and remove surrounding parentheses
            String normalized = query.trim().toUpperCase();
            while (normalized.startsWith("(") && normalized.endsWith(")")) {
                normalized = normalized.substring(1, normalized.length() - 1).trim().toUpperCase();
            }
            LOGGER.log(Level.INFO, "Normalized query: {0}", normalized);
            if (normalized.startsWith("SELECT")) {
                return parseSelectQuery(normalized, query, database);
            } else if (normalized.startsWith("INSERT INTO")) {
                return parseInsertQuery(normalized, query, database);
            } else if (normalized.startsWith("UPDATE")) {
                return parseUpdateQuery(normalized, query, database);
            } else if (normalized.startsWith("DELETE FROM")) {
                return parseDeleteQuery(normalized, query, database);
            } else if (normalized.startsWith("CREATE TABLE")) {
                return parseCreateTableQuery(normalized, query);
            } else if (normalized.startsWith("CREATE UNIQUE CLUSTERED INDEX")) {
                return parseCreateUniqueDurableClusteredIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE UNIQUE INDEX")) {
                return parseCreateUniqueIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE HASH INDEX")) {
                return parseCreateHashIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE INDEX")) {
                return parseCreateIndexQuery(normalized);
            } else if (normalized.equals("BEGIN TRANSACTION") ||
                    normalized.startsWith("BEGIN TRANSACTION ISOLATION LEVEL")) {
                IsolationLevel isolationLevel = null;
                if (normalized.contains("ISOLATION LEVEL READ UNCOMMITTED")) {
                    isolationLevel = IsolationLevel.READ_UNCOMMITTED;
                } else if (normalized.contains("ISOLATION LEVEL READ COMMITTED")) {
                    isolationLevel = IsolationLevel.READ_COMMITTED;
                } else if (normalized.contains("ISOLATION LEVEL REPEATABLE READ")) {
                    isolationLevel = IsolationLevel.REPEATABLE_READ;
                } else if (normalized.contains("ISOLATION LEVEL SERIALIZABLE")) {
                    isolationLevel = IsolationLevel.SERIALIZABLE;
                }
                return new BeginTransactionQuery(isolationLevel);
            } else if (normalized.equals("COMMIT TRANSACTION")) {
                return new CommitTransactionQuery();
            } else if (normalized.equals("ROLLBACK TRANSACTION")) {
                return new RollbackTransactionQuery();
            } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")) {
                return new SetIsolationLevelQuery(IsolationLevel.READ_UNCOMMITTED);
            } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")) {
                return new SetIsolationLevelQuery(IsolationLevel.READ_COMMITTED);
            } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")) {
                return new SetIsolationLevelQuery(IsolationLevel.REPEATABLE_READ);
            } else if (normalized.equals("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")) {
                return new SetIsolationLevelQuery(IsolationLevel.SERIALIZABLE);
            }
            throw new IllegalArgumentException("Unsupported query type");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    private Query<Void> parseCreateIndexQuery(String normalized) {
        String[] parts = normalized.split("ON");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE INDEX query format");
        }
        String indexPart = parts[0].replace("CREATE INDEX", "").trim();
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateHashIndexQuery(String normalized) {
        String[] parts = normalized.split("ON");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE HASH INDEX query format");
        }
        String indexPart = parts[0].replace("CREATE HASH INDEX", "").trim();
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateHashIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateUniqueIndexQuery(String normalized) {
        String[] parts = normalized.split("ON");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE UNIQUE INDEX query format");
        }
        String indexPart = parts[0].replace("CREATE UNIQUE INDEX", "").trim();
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateUniqueIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateUniqueDurableClusteredIndexQuery(String normalized) {
        String[] parts = normalized.split("ON");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE UNIQUE CLUSTERED INDEX query format");
        }
        String indexPart = parts[0].replace("CREATE UNIQUE CLUSTERED INDEX", "").trim();
        String tableAndColumn = parts[1].trim();
        String tableName = tableAndColumn.substring(0, tableAndColumn.indexOf("(")).trim();
        String columnName = tableAndColumn.substring(tableAndColumn.indexOf("(") + 1, tableAndColumn.indexOf(")")).trim();
        return new CreateUniqueClusteredIndexQuery(tableName, columnName);
    }

    private Query<Void> parseCreateTableQuery(String normalized, String original) {
        int firstParen = original.indexOf('(');
        int lastParen = original.lastIndexOf(')');
        if (firstParen == -1 || lastParen == -1 || lastParen < firstParen) {
            throw new IllegalArgumentException("Invalid CREATE TABLE query format: missing or mismatched parentheses");
        }

        String tableName = original.substring(0, firstParen).replace("CREATE TABLE", "").trim();
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
            String colName = colParts[0];
            String type = colParts[1].toUpperCase();
            String constraints = colParts.length > 2 ? colParts[2].toUpperCase() : "";
            boolean isPrimaryKey = constraints.contains("PRIMARY KEY");
            boolean hasSequence = constraints.contains("SEQUENCE") || type.endsWith("_SEQUENCE");

            columns.add(colName);

            if (hasSequence) {
                if (!type.equals("LONG") && !type.equals("INTEGER") && !type.startsWith("LONG_SEQUENCE") && !type.startsWith("INTEGER_SEQUENCE")) {
                    throw new IllegalArgumentException("Sequence is only supported for LONG or INTEGER types: " + colDef);
                }
                String seqDef;
                if (type.endsWith("_SEQUENCE")) {
                    seqDef = colParts.length > 2 ? colParts[2].replaceAll(".*SEQUENCE\\(([^)]+)\\).*", "$1").trim() : "";
                } else {
                    seqDef = constraints.replaceAll(".*SEQUENCE\\(([^)]+)\\).*", "$1").trim();
                }
                String[] seqParts = seqDef.split("\\s+");
                if (seqParts.length < 3) {
                    throw new IllegalArgumentException("Invalid SEQUENCE definition in column: " + colDef);
                }
                String seqName = seqParts[0];
                long start = Long.parseLong(seqParts[1]);
                long increment = Long.parseLong(seqParts[2]);
                Class<?> seqType = type.equals("LONG") || type.startsWith("LONG_SEQUENCE") ? Long.class : Integer.class;
                sequences.put(colName, new Sequence(seqName, seqType, start, increment));
                columnTypes.put(colName, seqType);
            } else {
                switch (type) {
                    case "STRING":
                        columnTypes.put(colName, String.class);
                        break;
                    case "INTEGER":
                        columnTypes.put(colName, Integer.class);
                        break;
                    case "LONG":
                        columnTypes.put(colName, Long.class);
                        break;
                    case "SHORT":
                        columnTypes.put(colName, Short.class);
                        break;
                    case "BYTE":
                        columnTypes.put(colName, Byte.class);
                        break;
                    case "BIGDECIMAL":
                        columnTypes.put(colName, BigDecimal.class);
                        break;
                    case "FLOAT":
                        columnTypes.put(colName, Float.class);
                        break;
                    case "DOUBLE":
                        columnTypes.put(colName, Double.class);
                        break;
                    case "CHAR":
                        columnTypes.put(colName, Character.class);
                        break;
                    case "UUID":
                        columnTypes.put(colName, UUID.class);
                        break;
                    case "BOOLEAN":
                        columnTypes.put(colName, Boolean.class);
                        break;
                    case "DATE":
                        columnTypes.put(colName, LocalDate.class);
                        break;
                    case "DATETIME":
                    case "DATETIME_MS":
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
        String regex = "(?=([^']*'[^']*')*[^']*$)(?![^()]*\\)),\\s*";
        String[] parts = columnsPart.split(regex);

        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                columnDefs.add(trimmed);
            }
        }

        return columnDefs;
    }

    private int findMainFromClause(String query) {
        if (query == null || query.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}", query);
            return -1;
        }

        // Регулярные выражения для разных типов токенов
        Pattern quotedStringPattern = Pattern.compile("(?i)'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'", Pattern.DOTALL);
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern fromPattern = Pattern.compile("(?i)\\bFROM\\b");
        Pattern wordPattern = Pattern.compile("\\S+");

        int bracketDepth = 0;
        int fromIndex = -1;
        int currentPos = 0;
        StringBuilder currentToken = new StringBuilder();

        while (currentPos < query.length()) {
            // Проверяем строки в кавычках
            Matcher quotedStringMatcher = quotedStringPattern.matcher(query).region(currentPos, query.length());
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
                currentToken.append(token);
            } else if (tokenType.equals("openParen")) {
                bracketDepth++;
                currentToken.append(token);
            } else if (tokenType.equals("closeParen")) {
                bracketDepth--;
                if (bracketDepth < 0) {
                    LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}",
                            new Object[]{start, query});
                    return -1;
                }
                currentToken.append(token);
            } else if (tokenType.equals("from") && bracketDepth == 0) {
                fromIndex = start;
                LOGGER.log(Level.FINEST, "Найден основной FROM на позиции {0} в запросе: {1}",
                        new Object[]{fromIndex, query});
                return fromIndex;
            } else if (tokenType.equals("word")) {
                currentToken.append(token);
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
        String selectPartOriginal = original.substring(original.indexOf("SELECT") + 6, fromIndex).trim();
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
        AdditionalClauses clauses = parseAdditionalClauses(tableAndJoinsOriginal, tableName, database, original,
                aggregates, combinedColumnTypes, tableAliases, columnAliases, subQueries);
        List<Condition> conditions = clauses.conditions;
        List<String> groupBy = clauses.groupBy;
        List<HavingCondition> havingConditions = clauses.havingConditions;
        List<OrderByInfo> orderBy = clauses.orderBy;
        Integer limit = clauses.limit;
        Integer offset = clauses.offset;

        LOGGER.log(Level.INFO, "Разобран SELECT-запрос: таблица={0}, столбцы={1}, агрегации={2}, соединения={3}, условия={4}",
                new Object[]{tableName, columns, aggregates, joins, conditions});

        return new SelectQuery(tableName, tableAlias, columns, aggregates, joins, conditions,
                groupBy, havingConditions, orderBy, limit, offset, subQueries, columnAliases, tableAliases, tableJoins.combinedColumnTypes);
    }


    // Парсит элементы SELECT (столбцы, агрегации, подзапросы)
    private SelectItems parseSelectItems(String selectPartOriginal, Database database) {
        List<String> selectItems = splitSelectItems(selectPartOriginal);
        List<String> columns = new ArrayList<>();
        List<AggregateFunction> aggregates = new ArrayList<>();
        List<SubQuery> subQueries = new ArrayList<>();
        Map<String, String> columnAliases = new HashMap<>();

        Pattern countPattern = Pattern.compile("(?i)^COUNT\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern minPattern = Pattern.compile("(?i)^MIN\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern maxPattern = Pattern.compile("(?i)^MAX\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern avgPattern = Pattern.compile("(?i)^AVG\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern sumPattern = Pattern.compile("(?i)^SUM\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+.*?\\s*\\)\\s*(?:AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?\\s*$");

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher countMatcher = countPattern.matcher(trimmedItem);
            Matcher minMatcher = minPattern.matcher(trimmedItem);
            Matcher maxMatcher = maxPattern.matcher(trimmedItem);
            Matcher avgMatcher = avgPattern.matcher(trimmedItem);
            Matcher sumMatcher = sumPattern.matcher(trimmedItem);
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            Matcher subQueryMatcher = subQueryPattern.matcher(trimmedItem);

            if (countMatcher.matches()) {
                String countArg = countMatcher.group(1);
                String alias = countMatcher.group(2);
                if (countArg.startsWith("(") && countArg.endsWith(")")) {
                    String subQueryStr = countArg.substring(1, countArg.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("COUNT", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: COUNT(подзапрос){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    String column = countArg.equals("*") ? null : countArg;
                    aggregates.add(new AggregateFunction("COUNT", column, alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: COUNT({0}){1}",
                            new Object[]{column == null ? "*" : column, alias != null ? " AS " + alias : ""});
                }
            } else if (minMatcher.matches()) {
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("MIN", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: MIN(подзапрос){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("MIN", column, alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: MIN({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (maxMatcher.matches()) {
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("MAX", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: MAX(подзапрос){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("MAX", column, alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: MAX({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (avgMatcher.matches()) {
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("AVG", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: AVG(подзапрос){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("AVG", column, alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: AVG({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (sumMatcher.matches()) {
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("SUM", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: SUM(подзапрос){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("SUM", column, alias));
                    LOGGER.log(Level.FINE, "Разобранная агрегатная функция: SUM({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (subQueryMatcher.matches()) {
                int subQueryEnd = findMatchingParenthesis(trimmedItem, 0);
                if (subQueryEnd == -1) {
                    LOGGER.log(Level.SEVERE, "Не удалось найти парную скобку для подзапроса: {0}", trimmedItem);
                    throw new IllegalArgumentException("Недопустимый синтаксис подзапроса в SELECT: " + trimmedItem);
                }
                String subQueryStr = trimmedItem.substring(1, subQueryEnd).trim();
                if (subQueryStr.isEmpty()) {
                    LOGGER.log(Level.SEVERE, "Обнаружен пустой подзапрос в SELECT: {0}", trimmedItem);
                    throw new IllegalArgumentException("Пустой подзапрос в SELECT: " + trimmedItem);
                }
                String alias = subQueryMatcher.group(1);
                Query<?> subQuery = parse(subQueryStr, database);
                SubQuery newSubQuery = new SubQuery(subQuery, alias);
                subQueries.add(newSubQuery);
                if (alias != null) {
                    String subQueryKey = "SUBQUERY_" + subQueries.size();
                    columnAliases.put(subQueryKey, alias);
                    LOGGER.log(Level.FINE, "Добавлен алиас подзапроса в columnAliases: {0} -> {1}", new Object[]{subQueryKey, alias});
                }
                LOGGER.log(Level.FINE, "Разобран подзапрос в SELECT: {0}{1}, размер subQueries: {2}, подзапрос: {3}",
                        new Object[]{subQueryStr, alias != null ? " AS " + alias : "", subQueries.size(), newSubQuery});
            } else if (columnMatcher.matches()) {
                String column = columnMatcher.group(1);
                String alias = columnMatcher.group(2);
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                    LOGGER.log(Level.FINE, "Разобран столбец с алиасом: {0} AS {1}", new Object[]{column, alias});
                } else {
                    LOGGER.log(Level.FINE, "Разобран столбец: {0}", new Object[]{column});
                }
            } else {
                throw new IllegalArgumentException("Недопустимый элемент SELECT: " + trimmedItem);
            }
        }

        return new SelectItems(columns, aggregates, subQueries, columnAliases);
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
        tableName = mainTableTokens[0].trim();
        if (mainTableTokens.length > 1) {
            if (mainTableTokens.length == 3 && mainTableTokens[1].equalsIgnoreCase("AS")) {
                tableAlias = mainTableTokens[2].trim();
            } else if (mainTableTokens.length == 2) {
                tableAlias = mainTableTokens[1].trim();
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
                String onClause = joinPart.substring(onIndex + 2).trim(); // Пропускаем "ON"
                joinTableTokens = new String[]{joinTablePart, onClause};
            }

            String joinTablePart = joinTableTokens[0].trim();
            String[] joinTableParts = joinTablePart.split("\\s+");
            joinTableName = joinTableParts[0].trim();
            if (joinTableParts.length > 1) {
                if (joinTableParts.length == 3 && joinTableParts[1].equalsIgnoreCase("AS")) {
                    joinTableAlias = joinTableParts[2].trim();
                } else if (joinTableParts.length == 2) {
                    joinTableAlias = joinTableParts[1].trim();
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

                onConditions = parseConditions(onClause, tableName, database, tableAndJoinsOriginal, true,
                        combinedColumnTypes, tableAliases, new HashMap<>());

                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }

                LOGGER.log(Level.FINE, "Разобранные условия ON для {0}: {1}", new Object[]{joinTypeStr, onConditions});
                joins.add(new JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            }
            tableName = joinTableName;
        }

        return new TableJoins(tableName, tableAlias, joins, tableAliases, combinedColumnTypes);
    }

    // Парсит тип соединения
    private JoinType parseJoinType(String joinTypeStr) {
        return switch (joinTypeStr.toUpperCase()) {
            case "JOIN", "INNER JOIN" -> JoinType.INNER;
            case "LEFT JOIN", "LEFT OUTER JOIN" -> JoinType.LEFT_OUTER;
            case "RIGHT JOIN", "RIGHT OUTER JOIN" -> JoinType.RIGHT_OUTER;
            case "FULL JOIN", "FULL OUTER JOIN" -> JoinType.FULL_OUTER;
            case "LEFT INNER JOIN" -> JoinType.LEFT_INNER;
            case "RIGHT INNER JOIN" -> JoinType.RIGHT_INNER;
            case "CROSS JOIN" -> JoinType.CROSS;
            default -> throw new IllegalArgumentException("Неподдерживаемый тип соединения: " + joinTypeStr);
        };
    }

    // Парсит дополнительные клаузы (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET)
    private AdditionalClauses parseAdditionalClauses(String tableAndJoinsOriginal, String tableName, Database database,
                                                     String original, List<AggregateFunction> aggregates,
                                                     Map<String, Class<?>> combinedColumnTypes,
                                                     Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                     List<SubQuery> subQueries) {
        String conditionStr = null;
        List<Condition> conditions = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<HavingCondition> havingConditions = new ArrayList<>();
        List<OrderByInfo> orderBy = new ArrayList<>();
        Integer limit = null;
        Integer offset = null;

        // Проверяем наличие ключевых слов
        int orderByIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "ORDER BY");
        if (orderByIndex != -1) {
            String beforeOrderBy = tableAndJoinsOriginal.substring(0, orderByIndex).trim();
            String orderByClause = tableAndJoinsOriginal.substring(orderByIndex + 8).trim();
            orderBy = parseOrderByClause(orderByClause, tableName, combinedColumnTypes, tableAliases, columnAliases, subQueries); // Updated call
            tableAndJoinsOriginal = beforeOrderBy;
            LOGGER.log(Level.FINE, "Разобранная клауза ORDER BY: {0}", orderBy);
        }

        int limitIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "LIMIT");
        if (limitIndex != -1) {
            String beforeLimit = tableAndJoinsOriginal.substring(0, limitIndex).trim();
            String afterLimit = tableAndJoinsOriginal.substring(limitIndex + 5).trim();
            Pattern limitPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:$|\\s+OFFSET\\s+|\\s*;\\s*$)");
            Matcher limitMatcher = limitPattern.matcher(afterLimit);
            if (limitMatcher.find()) {
                String limitValue = limitMatcher.group(1);
                try {
                    limit = Integer.parseInt(limitValue);
                    LOGGER.log(Level.FINE, "Разобранное значение LIMIT: {0}", limitValue);
                } catch (NumberFormatException e) {
                    LOGGER.log(Level.SEVERE, "Недопустимое значение LIMIT: {0}", limitValue);
                    throw new IllegalArgumentException("Недопустимое значение LIMIT: " + limitValue);
                }
                String[] limitOffsetSplit = afterLimit.split("(?i)\\s+OFFSET\\s+", 2);
                if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                    offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                }
            } else {
                LOGGER.log(Level.SEVERE, "Недопустимый формат LIMIT: {0}", afterLimit);
                throw new IllegalArgumentException("Недопустимый формат LIMIT: " + afterLimit);
            }
            tableAndJoinsOriginal = beforeLimit;
        }

        int whereIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "WHERE");
        if (whereIndex != -1) {
            conditionStr = tableAndJoinsOriginal.substring(whereIndex + 6).trim();
            conditions = parseConditions(conditionStr, tableName, database, original,
                    false, combinedColumnTypes, tableAliases, columnAliases);
        }

        return new AdditionalClauses(conditions, groupBy, havingConditions, orderBy, limit, offset);
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        if (query == null || clause == null || query.isEmpty() || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        // Регулярные выражения для разных типов токенов
        Pattern quotedStringPattern = Pattern.compile("'([^'\\\\]*(?:\\\\.[^'\\\\]*)*)'");
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
                inQuotes = !inQuotes;
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

        List<String> selectItems = new ArrayList<>();
        String regex = "(?=([^']*'[^']*')*[^']*$)(?![^()]*\\)),\\s*";
        String[] parts = selectPart.split(regex);

        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                selectItems.add(trimmed);
            }
        }

        return selectItems;
    }

    private List<OrderByInfo> parseOrderByClause(String orderByClause, String defaultTableName, Map<String, Class<?>> combinedColumnTypes, Map<String, String> columnAliases, Map<String, String> tableAliases, List<SubQuery> selectSubQueries) {
        List<OrderByInfo> orderBy = new ArrayList<>();
        String[] orderByParts = splitOrderByClause(orderByClause);
        Pattern columnPattern = Pattern.compile("(?i)^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+.*?\\s*\\)$");

        for (String part : orderByParts) {
            String trimmedPart = part.trim();
            if (trimmedPart.isEmpty()) {
                throw new IllegalArgumentException("Invalid ORDER BY clause: empty expression");
            }

            String expression;
            boolean ascending = true;

            // Check sorting direction (ASC/DESC)
            Pattern directionPattern = Pattern.compile("(?i)\\s+(ASC|DESC)$");
            Matcher directionMatcher = directionPattern.matcher(trimmedPart);
            if (directionMatcher.find()) {
                expression = trimmedPart.substring(0, directionMatcher.start()).trim();
                String direction = directionMatcher.group(1).toUpperCase();
                ascending = direction.equals("ASC");
            } else {
                expression = trimmedPart;
            }

            boolean found = false;
            String unqualifiedExpression = expression.contains(".") ? expression.split("\\.")[1].trim() : expression;

            // Check if expression is an alias from SELECT
            for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
                if (aliasEntry.getValue().equalsIgnoreCase(unqualifiedExpression)) {
                    orderBy.add(new OrderByInfo(aliasEntry.getKey(), ascending));
                    found = true;
                    LOGGER.log(Level.FINE, "Matched ORDER BY by alias: {0} -> {1}, ascending={2}", new Object[]{unqualifiedExpression, aliasEntry.getKey(), ascending});
                    break;
                }
            }

            // Check if expression matches a subquery alias
            if (!found) {
                for (SubQuery subQuery : selectSubQueries) {
                    if (subQuery.alias != null && subQuery.alias.equalsIgnoreCase(unqualifiedExpression)) {
                        orderBy.add(new OrderByInfo(subQuery.alias, ascending));
                        found = true;
                        LOGGER.log(Level.FINE, "Matched ORDER BY by subquery alias: {0}, ascending={1}", new Object[]{subQuery.alias, ascending});
                        break;
                    }
                }
            }

            // Check if expression is a subquery and matches any SELECT subquery
            if (!found && subQueryPattern.matcher(expression).matches()) {
                String normalizedExpression = normalizeQueryString(expression);
                LOGGER.log(Level.FINEST, "Normalized ORDER BY subquery: {0}", normalizedExpression);
                for (SubQuery subQuery : selectSubQueries) {
                    String subQueryStr = normalizeQueryString("(" + subQuery.query.toString() + ")");
                    LOGGER.log(Level.FINEST, "Comparing ORDER BY subquery: '{0}' vs SELECT subquery: '{1}'", new Object[]{normalizedExpression, subQueryStr});
                    if (normalizedExpression.equalsIgnoreCase(subQueryStr) || areSubQueriesEquivalent(expression, subQuery.query.toString())) {
                        String resolvedExpression = subQuery.alias != null ? subQuery.alias : expression;
                        orderBy.add(new OrderByInfo(resolvedExpression, ascending));
                        found = true;
                        LOGGER.log(Level.FINE, "Matched ORDER BY by subquery content: {0}, resolved as: {1}, ascending={2}", new Object[]{expression, resolvedExpression, ascending});
                        break;
                    }
                }
            }

            // Check if expression is a column
            if (!found && columnPattern.matcher(expression).matches()) {
                String normalizedColumn = expression.contains(".") ? normalizeColumnName(expression, defaultTableName, tableAliases) : defaultTableName + "." + expression;
                String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                    if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn) || entry.getKey().equalsIgnoreCase(normalizedColumn)) {
                        orderBy.add(new OrderByInfo(normalizedColumn, ascending));
                        found = true;
                        LOGGER.log(Level.FINE, "Matched ORDER BY by column: {0}, ascending={1}", new Object[]{normalizedColumn, ascending});
                        break;
                    }
                }
            }

            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column, alias, or subquery in ORDER BY: {0}, available columns: {1}, aliases: {2}, subqueries: {3}",
                        new Object[]{expression, combinedColumnTypes.keySet(), columnAliases.values(), selectSubQueries});
                throw new IllegalArgumentException("Invalid column name or subquery in ORDER BY: " + expression);
            }
        }
        return orderBy;
    }

    private String[] splitOrderByClause(String orderByClause) {
        List<String> parts = new ArrayList<>();
        StringBuilder currentPart = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < orderByClause.length(); i++) {
            char c = orderByClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentPart.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    currentPart.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentPart.append(c);
                    continue;
                } else if (c == ',' && parenDepth == 0) {
                    String part = currentPart.toString().trim();
                    if (!part.isEmpty()) {
                        parts.add(part);
                    }
                    currentPart = new StringBuilder();
                    continue;
                }
            }
            currentPart.append(c);
        }

        String lastPart = currentPart.toString().trim();
        if (!lastPart.isEmpty()) {
            parts.add(lastPart);
        }

        return parts.toArray(new String[0]);
    }

    private Integer parseLimitClause(String clause) {
        Pattern limitPattern = Pattern.compile("(?i)^LIMIT\\s*(\\d+)\\s*$");
        Matcher matcher = limitPattern.matcher(clause.trim());
        if (matcher.matches()) {
            String limitValue = matcher.group(1);
            try {
                Integer limit = Integer.parseInt(limitValue);
                LOGGER.log(Level.FINE, "Спарсенное значение LIMIT: {0}", limitValue);
                return limit;
            } catch (NumberFormatException e) {
                LOGGER.log(Level.SEVERE, "Неверное значение LIMIT: {0}", limitValue);
                throw new IllegalArgumentException("Неверное значение LIMIT: " + limitValue);
            }
        }
        LOGGER.log(Level.SEVERE, "Неверный формат предложения LIMIT: {0}", clause);
        throw new IllegalArgumentException("Неверный формат предложения LIMIT: " + clause);
    }

    private Integer parseOffsetClause(String offsetClause) {
        String normalized = offsetClause.toUpperCase().replace("OFFSET", "").trim();
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
                String resolvedTable = tableAliases.getOrDefault(prefix, prefix);
                if (!resolvedTable.equalsIgnoreCase(leftTable) && !resolvedTable.equalsIgnoreCase(rightTable)) {
                    throw new IllegalArgumentException("Invalid table or alias in ON condition: " + prefix +
                            ", expected " + leftTable + " or " + rightTable);
                }
            }
        }
        if (condition.rightColumn != null) {
            String prefix = condition.rightColumn.contains(".") ? condition.rightColumn.split("\\.")[0] : null;
            if (prefix != null) {
                String resolvedTable = tableAliases.getOrDefault(prefix, prefix);
                if (!resolvedTable.equalsIgnoreCase(leftTable) && !resolvedTable.equalsIgnoreCase(rightTable)) {
                    throw new IllegalArgumentException("Invalid table or alias in ON condition (right column): " + prefix +
                            ", expected " + leftTable + " or " + rightTable);
                }
            }
        }
    }

    private Query<Void> parseInsertQuery(String normalized, String original, Database database) {
        String[] parts = normalized.split("VALUES");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid INSERT query format");
        }

        String tableAndColumns = parts[0].replace("INSERT INTO", "").trim();
        String tableName = tableAndColumns.substring(0, tableAndColumns.indexOf("(")).trim();
        String columnsPart = original.substring(original.indexOf("(") + 1, original.indexOf(")")).trim();
        List<String> columns = Arrays.stream(columnsPart.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, Sequence> sequences = table.getSequences();
        String primaryKeyColumn = table.getPrimaryKeyColumn();

        if (primaryKeyColumn != null && sequences.containsKey(primaryKeyColumn) && columns.contains(primaryKeyColumn)) {
            throw new IllegalArgumentException("Cannot specify value for sequence-based primary key column: " + primaryKeyColumn);
        }

        String valuesPart = parts[1].trim();
        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            throw new IllegalArgumentException("Invalid VALUES syntax");
        }
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        String[] valueStrings = valuesPart.split(",(?=([^']*'[^']*')*[^']*$)");
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < valueStrings.length; i++) {
            String val = valueStrings[i].trim();
            String column = columns.get(i);
            Class<?> columnType = columnTypes.get(column);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column in INSERT: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            values.add(parseConditionValue(column, val, columnType));
        }

        LOGGER.log(Level.INFO, "Parsed INSERT query: table={0}, columns={1}, values={2}",
                new Object[]{tableName, columns, values});

        return new InsertQuery(columns, values);
    }

    private Query<Void> parseUpdateQuery(String normalized, String original, Database database) {
        String[] parts = normalized.split("SET");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid UPDATE query format");
        }

        String tablePart = parts[0].replace("UPDATE", "").trim();
        String[] tableTokens = tablePart.split("\\s+");
        String tableName = tableTokens[0].trim();
        String tableAlias = null;
        if (tableTokens.length > 1) {
            if (tableTokens.length == 3 && tableTokens[1].equalsIgnoreCase("AS")) {
                tableAlias = tableTokens[2].trim();
            } else if (tableTokens.length == 2) {
                tableAlias = tableTokens[1].trim();
            }
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, String> tableAliases = new HashMap<>();
        tableAliases.put(tableName, tableName);
        if (tableAlias != null) {
            tableAliases.put(tableAlias, tableName);
            LOGGER.log(Level.FINE, "Parsed UPDATE table alias: {0} -> {1}", new Object[]{tableAlias, tableName});
        }

        String setAndWhere = parts[1].trim();
        String setPart;
        List<Condition> conditions = new ArrayList<>();

        if (setAndWhere.contains("WHERE")) {
            String[] setWhereParts = setAndWhere.split("WHERE");
            setPart = setWhereParts[0].trim();
            String conditionStr = setWhereParts[1].trim();
            conditions = parseConditions(conditionStr, tableName, database, original, false, columnTypes, tableAliases, new HashMap<>());
        } else {
            setPart = setAndWhere;
        }

        String[] assignments = setPart.split(",(?=([^']*'[^']*')*[^']*$)");
        Map<String, Object> updates = new HashMap<>();
        for (String assignment : assignments) {
            String[] kv = assignment.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid SET clause");
            }
            String column = kv[0].trim();
            String valueStr = kv[1].trim();
            Class<?> columnType = columnTypes.get(column);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column in UPDATE: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            Object value = parseConditionValue(column, valueStr, columnType);
            updates.put(column, value);
        }

        LOGGER.log(Level.INFO, "Parsed UPDATE query: table={0}, alias={1}, updates={2}, conditions={3}",
                new Object[]{tableName, tableAlias, updates, conditions});

        return new UpdateQuery(updates, conditions);
    }

    private Query<Void> parseDeleteQuery(String normalized, String original, Database database) {
        LOGGER.log(Level.FINE, "Raw DELETE query: {0}", original);
        LOGGER.log(Level.FINE, "Normalized DELETE query: {0}", normalized);
        String[] fromParts = normalized.split("(?i)FROM\\s+", 2);
        if (fromParts.length != 2) {
            LOGGER.log(Level.SEVERE, "Invalid DELETE query format: missing FROM clause, normalized: {0}", normalized);
            throw new IllegalArgumentException("Invalid DELETE query format: missing FROM clause");
        }

        String tableAndCondition = fromParts[1].trim();
        LOGGER.log(Level.FINE, "Table and condition: {0}", tableAndCondition);
        String[] whereParts = tableAndCondition.split("(?i)WHERE\\s+", 2);
        String tableName = whereParts[0].trim();
        List<Condition> conditions = new ArrayList<>();

        if (whereParts.length == 2) {
            String conditionStr = whereParts[1].trim();
            LOGGER.log(Level.FINE, "Parsing WHERE clause for DELETE: {0}", conditionStr);
            if (conditionStr.isEmpty()) {
                LOGGER.log(Level.SEVERE, "Empty WHERE clause in DELETE query: {0}", normalized);
                throw new IllegalArgumentException("Invalid DELETE query: empty WHERE clause");
            }
            Table table = database.getTable(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
            conditions = parseConditions(conditionStr, tableName, database, original, false, table.getColumnTypes(), new HashMap<>(), new HashMap<>());
        } else {
            LOGGER.log(Level.FINE, "No WHERE clause in DELETE query");
        }

        LOGGER.log(Level.INFO, "Parsed DELETE query: table={0}, conditions={1}",
                new Object[]{tableName, conditions});

        return new DeleteQuery(conditions);
    }

    private Object parseConditionValue(String conditionColumn, String valueStr, Class<?> columnType) {
        try {
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                String strippedValue = valueStr.substring(1, valueStr.length() - 1);
                if (columnType == String.class) {
                    return strippedValue;
                } else if (columnType == LocalDate.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return LocalDate.parse(strippedValue);
                } else if (columnType == LocalDateTime.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_MS_FORMATTER);
                } else if (columnType == LocalDateTime.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_FORMATTER);
                } else if (columnType == UUID.class && strippedValue.matches(UUID_PATTERN)) {
                    return UUID.fromString(strippedValue);
                } else if (columnType == Character.class && strippedValue.length() == 1) {
                    return strippedValue.charAt(0);
                } else {
                    throw new IllegalArgumentException("Value '" + strippedValue + "' does not match column type: " + columnType.getSimpleName());
                }
            } else if (valueStr.equalsIgnoreCase("TRUE") || valueStr.equalsIgnoreCase("FALSE")) {
                if (columnType == Boolean.class) {
                    return Boolean.parseBoolean(valueStr);
                } else {
                    throw new IllegalArgumentException("Boolean value '" + valueStr + "' does not match column type: " + columnType.getSimpleName());
                }
            } else {
                try {
                    if (columnType == BigDecimal.class) {
                        return new BigDecimal(valueStr);
                    } else if (columnType == Float.class) {
                        BigDecimal bd = new BigDecimal(valueStr);
                        float floatValue = bd.floatValue();
                        if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Float");
                        }
                        return floatValue;
                    } else if (columnType == Double.class) {
                        BigDecimal bd = new BigDecimal(valueStr);
                        double doubleValue = bd.doubleValue();
                        if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Double");
                        }
                        return doubleValue;
                    } else if (columnType == Byte.class) {
                        long parsedLong = Long.parseLong(valueStr);
                        if (parsedLong >= Byte.MIN_VALUE && parsedLong <= Byte.MAX_VALUE) {
                            return (byte) parsedLong;
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Byte");
                        }
                    } else if (columnType == Short.class) {
                        long parsedLong = Long.parseLong(valueStr);
                        if (parsedLong >= Short.MIN_VALUE && parsedLong <= Short.MAX_VALUE) {
                            return (short) parsedLong;
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Short");
                        }
                    } else if (columnType == Integer.class) {
                        return Integer.parseInt(valueStr);
                    } else if (columnType == Long.class) {
                        return Long.parseLong(valueStr);
                    } else {
                        throw new IllegalArgumentException("Numeric value '" + valueStr + "' does not match column type: " + columnType.getSimpleName());
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value '" + valueStr + "' for column type: " + columnType.getSimpleName());
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to parse condition value: column={0}, value={1}, type={2}, error={3}",
                    new Object[]{conditionColumn, valueStr, columnType.getSimpleName(), e.getMessage()});
            throw new IllegalArgumentException("Invalid value for column " + conditionColumn + ": " + valueStr, e);
        }
    }

    private List<Condition> parseConditions(String conditionStr, String defaultTableName, Database database,
                                            String originalQuery, boolean isJoinCondition,
                                            Map<String, Class<?>> combinedColumnTypes,
                                            Map<String, String> tableAliases, Map<String, String> columnAliases) {
        LOGGER.log(Level.FINE, "Начало парсинга условий: conditionStr={0}, defaultTableName={1}, isJoinCondition={2}",
                new Object[]{conditionStr, defaultTableName, isJoinCondition});

        if (conditionStr == null || conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Пустая строка условий, возвращается пустой список условий");
            return new ArrayList<>();
        }

        // Обрезаем строку до ключевых слов
        String trimmedConditionStr = trimToClause(conditionStr);
        LOGGER.log(Level.FINE, "Условие обрезано до: {0}", trimmedConditionStr);

        List<Condition> conditions = new ArrayList<>();
        List<Token> tokens = tokenizeConditions(trimmedConditionStr);
        return parseTokenizedConditions(tokens, defaultTableName, database, originalQuery, isJoinCondition,
                combinedColumnTypes, tableAliases, columnAliases, null, false);
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
        LOGGER.log(Level.FINE, "Начало токенизации условий: {0}", conditionStr);

        if (conditionStr == null || conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.WARNING, "Пустая или null строка условий");
            return new ArrayList<>();
        }

        // Определяем отдельные паттерны
        String quotedStringPattern = "'(?:[^'\\\\]|\\\\.)*'";
        String logicalOperatorPattern = "\\s*(?:AND|OR)\\s*";
        String inPattern = "[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*\\s*IN\\s*\\([^)]+\\)";
        String notInPattern = "[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*\\s*NOT\\s*IN\\s*\\([^)]+\\)";
        String balancedParenthesesPattern = "\\([^()]+\\)";
        String comparisonPattern = "(?:[^\\s()']+\\s*(?:=|>|<|>=|<=|!=|<>|\\bLIKE\\b|\\bNOT LIKE\\b)\\s*(?:[^\\s()']+|'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'))";

        // Объединяем паттерны
        String combinedPattern = String.format("(?i)(?:%s|%s|%s|%s|%s|%s)",
                quotedStringPattern,
                logicalOperatorPattern,
                notInPattern,
                inPattern,
                balancedParenthesesPattern,
                comparisonPattern);

        Pattern tokenPattern = Pattern.compile(combinedPattern);
        Matcher matcher = tokenPattern.matcher(conditionStr);

        List<Token> tokens = new ArrayList<>();
        int lastEnd = 0;
        while (matcher.find()) {
            String tokenValue = matcher.group().trim();
            int start = matcher.start();
            int end = matcher.end();

            if (start > lastEnd) {
                String skipped = conditionStr.substring(lastEnd, start).trim();
                if (!skipped.isEmpty()) {
                    LOGGER.log(Level.WARNING, "Пропущены неожиданные символы между токенами: {0} на позициях {1}-{2}",
                            new Object[]{skipped, lastEnd, start});
                }
            }

            if (tokenValue.isEmpty()) {
                LOGGER.log(Level.FINEST, "Пропущен пустой токен на позициях {0}-{1}", new Object[]{start, end});
                continue;
            }

            if (tokenValue.contains("(") || tokenValue.contains(")")) {
                validateTokenBrackets(tokenValue, start);
            }

            Token token = classifyToken(tokenValue);
            tokens.add(token);
            LOGGER.log(Level.FINEST, "Добавлен токен: {0} на позициях {1}-{2}", new Object[]{token, start, end});

            lastEnd = end;
        }

        if (lastEnd < conditionStr.length()) {
            String remaining = conditionStr.substring(lastEnd).trim();
            if (!remaining.isEmpty()) {
                LOGGER.log(Level.WARNING, "Остаток строки после токенизации: {0} на позициях {1}-{2}",
                        new Object[]{remaining, lastEnd, conditionStr.length()});
                throw new IllegalArgumentException("Невалидные символы в условии после токенизации: " + remaining);
            }
        }

        if (tokens.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Не удалось выделить токены из условия: {0}", conditionStr);
            throw new IllegalArgumentException("Невалидное условие: не удалось выделить токенов из '" + conditionStr + "'");
        }

        LOGGER.log(Level.FINE, "Токенизация завершена, получено токенов: {0}, токены: {1}", new Object[]{tokens.size(), tokens});
        return tokens;
    }

    // Классифицирует токен как условие или логический оператор
    private Token classifyToken(String tokenValue) {
        Pattern logicalOperatorPattern = Pattern.compile("(?i)^\\s*(AND|OR)\\s*$");
        Matcher logicalMatcher = logicalOperatorPattern.matcher(tokenValue);

        if (logicalMatcher.matches()) {
            LOGGER.log(Level.FINEST, "Классифицирован логический оператор: {0}", tokenValue);
            return new Token(TokenType.LOGICAL_OPERATOR, logicalMatcher.group(1).toUpperCase());
        }

        LOGGER.log(Level.FINEST, "Классифицировано как условие: {0}", tokenValue);
        return new Token(TokenType.CONDITION, tokenValue);
    }

    // Проверяет сбалансированность скобок в токене
    private void validateTokenBrackets(String tokenValue, int startIndex) {
        long openCount = tokenValue.chars().filter(c -> c == '(').count();
        long closeCount = tokenValue.chars().filter(c -> c == ')').count();

        if (openCount != closeCount) {
            LOGGER.log(Level.SEVERE, "Несбалансированные скобки в токене на позиции {0}: {1}, открывающих: {2}, закрывающих: {3}",
                    new Object[]{startIndex, tokenValue, openCount, closeCount});
            throw new IllegalArgumentException("Несбалансированные скобки в токене: " + tokenValue);
        }

        LOGGER.log(Level.FINEST, "Скобки в токене сбалансированы: {0}", tokenValue);
    }
    // Парсит токенизированные условия
    private List<Condition> parseTokenizedConditions(List<Token> tokens, String defaultTableName, Database database,
                                                     String originalQuery, boolean isJoinCondition,
                                                     Map<String, Class<?>> combinedColumnTypes,
                                                     Map<String, String> tableAliases,
                                                     Map<String, String> columnAliases,
                                                     String conjunction, boolean not) {
        List<Condition> conditions = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type == TokenType.CONDITION) {
                String condStr = token.value;
                if (condStr.toUpperCase().contains(" IN ")) {
                    Condition condition = parseInCondition(condStr, defaultTableName, database, originalQuery,
                            combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                    conditions.add(condition);
                    LOGGER.log(Level.FINE, "Добавлено условие IN: {0}", condition);
                } else if (condStr.startsWith("(") && condStr.endsWith(")")) {
                    int endParen = findMatchingParenthesis(condStr, 0);
                    if (endParen == condStr.length() - 1) {
                        String subCondStr = condStr.substring(1, endParen).trim();
                        List<Token> subTokens = tokenizeConditions(subCondStr);
                        List<Condition> subConditions = parseTokenizedConditions(subTokens, defaultTableName, database,
                                originalQuery, isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                        conditions.add(new Condition(subConditions, conjunction, not));
                        LOGGER.log(Level.FINE, "Добавлено группированное условие: {0}", subConditions);
                    } else {
                        throw new IllegalArgumentException("Некорректная структура группированного условия: " + condStr);
                    }
                } else {
                    Condition condition = parseSingleCondition(condStr, defaultTableName, database, originalQuery,
                            isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not, condStr);
                    conditions.add(condition);
                    LOGGER.log(Level.FINE, "Добавлено одиночное условие: {0}", condition);
                }
                if (i + 1 < tokens.size() && tokens.get(i + 1).type == TokenType.LOGICAL_OPERATOR) {
                    conjunction = tokens.get(i + 1).value;
                    i++; // Пропускаем логический оператор
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
        if (!upperSubquery.contains("SELECT ") || !upperSubquery.contains("FROM ")) {
            LOGGER.log(Level.SEVERE, "Subquery missing SELECT or FROM clause: {0}", subquery);
            throw new IllegalArgumentException("Subquery must contain SELECT and FROM clauses: " + subquery);
        }
        int selectIndex = upperSubquery.indexOf("SELECT ");
        int fromIndex = upperSubquery.indexOf("FROM ", selectIndex);
        if (fromIndex == -1 || fromIndex < selectIndex) {
            LOGGER.log(Level.SEVERE, "Invalid subquery structure: SELECT and FROM out of order in {0}", subquery);
            throw new IllegalArgumentException("Invalid subquery structure: " + subquery);
        }
        LOGGER.log(Level.FINE, "Validated subquery: {0}", subquery);
    }

    private Operator parseOperator(String operatorStr) {
        return switch (operatorStr.toUpperCase().trim()) {
            case "=" -> Operator.EQUALS;
            case "!=", "<>" -> Operator.NOT_EQUALS;
            case "<" -> Operator.LESS_THAN;
            case ">" -> Operator.GREATER_THAN;
            case "<=" -> Operator.LESS_THAN_OR_EQUALS;
            case ">=" -> Operator.GREATER_THAN_OR_EQUALS;
            case "LIKE" -> Operator.LIKE;
            case "NOT LIKE" -> Operator.NOT_LIKE;
            default -> throw new IllegalArgumentException("Unsupported operator: " + operatorStr);
        };
    }

    private void validateColumn(String column, Map<String, Class<?>> combinedColumnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        boolean found = false;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
            if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                found = true;
                break;
            }
        }
        if (!found) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{column, combinedColumnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + column);
        }
    }

    private String getNextToken(String conditionStr, int startIndex) {
        if (conditionStr == null || startIndex < 0 || startIndex >= conditionStr.length()) {
            LOGGER.log(Level.FINE, "Invalid input for getNextToken: conditionStr={0}, startIndex={1}",
                    new Object[]{conditionStr, startIndex});
            return "";
        }

        // Improved regex pattern to better handle nested parentheses and subqueries
        Pattern tokenPattern = Pattern.compile(
                "(?s)(?:'(?:\\\\.|[^'])*'|" +               // Match quoted strings
                        "\\((?:[^()']+|'(?:\\\\.|[^'])*')*\\)|" +   // Match balanced parentheses (including nested ones)
                        "[^\\s()']+)"                                // Match other tokens
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

    private Condition parseSingleCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                           boolean isJoinCondition, Map<String, Class<?>> combinedColumnTypes,
                                           Map<String, String> tableAliases, Map<String, String> columnAliases,
                                           String conjunction, boolean not, String conditionStr) {
        LOGGER.log(Level.FINEST, "Parsing single condition: {0}, full condition={1}", new Object[]{condStr, conditionStr});

        String normalizedCondStr = normalizeCondition(condStr);
        if (isGroupedCondition(normalizedCondStr)) {
            return parseGroupedCondition(normalizedCondStr, defaultTableName, database, originalQuery, isJoinCondition,
                    combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
        }

        if (isInCondition(normalizedCondStr)) {
            return parseInCondition(normalizedCondStr, defaultTableName, database, originalQuery, combinedColumnTypes,
                    tableAliases, columnAliases, conjunction, not);
        }

        if (isNullCondition(normalizedCondStr)) {
            return parseNullCondition(normalizedCondStr, defaultTableName, combinedColumnTypes, tableAliases, columnAliases,
                    conjunction, not);
        }

        if (isSubQueryCondition(normalizedCondStr)) {
            return parseSubQueryCondition(normalizedCondStr, defaultTableName, database, originalQuery, combinedColumnTypes,
                    tableAliases, columnAliases, conjunction, not);
        }

        LOGGER.log(Level.FINEST, "Передача в parseComparisonCondition: condStr={0}", normalizedCondStr);
        return parseComparisonCondition(normalizedCondStr, defaultTableName, combinedColumnTypes, tableAliases, columnAliases,
                conjunction, not, isJoinCondition, conditionStr);
    }

    private boolean isGroupedCondition(String condStr) {
        return condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")");
    }

    private Condition parseGroupedCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                            boolean isJoinCondition, Map<String, Class<?>> combinedColumnTypes,
                                            Map<String, String> tableAliases, Map<String, String> columnAliases,
                                            String conjunction, boolean not) {
        String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
        if (subCondStr.isEmpty()) {
            throw new IllegalArgumentException("Пустое группированное условие: " + condStr);
        }
        List<Condition> subConditions = parseConditions(subCondStr, defaultTableName, database, originalQuery,
                isJoinCondition, combinedColumnTypes, tableAliases, columnAliases);
        if (subConditions.isEmpty()) {
            throw new IllegalArgumentException("Не удалось разобрать подусловия в группированном условии: " + subCondStr);
        }
        return new Condition(subConditions, conjunction, not);
    }

    private boolean isInCondition(String condStr) {
        Pattern inPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((.*?)\\)$");
        return inPattern.matcher(condStr).matches();
    }

    private Condition parseInCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                       Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                       Map<String, String> columnAliases, String conjunction, boolean not) {
        Pattern inPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((.*?)\\)$");
        Matcher inMatcher = inPattern.matcher(condStr);
        if (!inMatcher.matches()) {
            throw new IllegalArgumentException("Invalid IN condition format: " + condStr);
        }

        String column = inMatcher.group(1).trim();
        boolean inNot = inMatcher.group(2) != null;
        String valuesStr = inMatcher.group(3).trim();
        String actualColumn = resolveColumnAlias(column, columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        Class<?> columnType = getColumnType(normalizedColumn, combinedColumnTypes, defaultTableName, tableAliases, columnAliases);

        if (valuesStr.trim().toUpperCase().startsWith("SELECT ")) {
            String subQueryStr = valuesStr.trim();
            if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                int subQueryEnd = findMatchingParenthesis(subQueryStr, 0);
                if (subQueryEnd != subQueryStr.length() - 1) {
                    throw new IllegalArgumentException("Invalid subquery syntax in IN condition: " + subQueryStr);
                }
                subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
            }
            validateSubquery(subQueryStr);
            Query<?> subQuery = parse(subQueryStr, database);
            LOGGER.log(Level.FINE, "Parsed IN subquery: {0}", subQueryStr);
            return new Condition(actualColumn, new SubQuery(subQuery, null), conjunction, inNot);
        }

        // Разделение списка значений с учётом кавычек
        List<String> valueParts = splitInValues(valuesStr);
        List<Object> inValues = new ArrayList<>();
        for (String val : valueParts) {
            String trimmedVal = val.trim();
            if (trimmedVal.isEmpty()) continue;
            Object value = parseConditionValue(actualColumn, trimmedVal, columnType);
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
                continue;
            }
            if (c == ',' && !inQuotes) {
                String value = current.toString().trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
                current = new StringBuilder();
                continue;
            }
            current.append(c);
        }

        // Добавляем последнее значение
        String value = current.toString().trim();
        if (!value.isEmpty()) {
            values.add(value);
        }

        return values;
    }

    private boolean isNullCondition(String condStr) {
        Pattern isNullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL\\b");
        return isNullPattern.matcher(condStr).matches();
    }

    private Condition parseNullCondition(String condStr, String defaultTableName, Map<String, Class<?>> combinedColumnTypes,
                                         Map<String, String> tableAliases, Map<String, String> columnAliases,
                                         String conjunction, boolean not) {
        Pattern isNullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(condStr);
        isNullMatcher.matches();
        String column = isNullMatcher.group(1).trim();
        boolean isNotNull = isNullMatcher.group(2) != null;
        String actualColumn = resolveColumnAlias(column, columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);
        return new Condition(actualColumn, isNotNull ? Operator.IS_NOT_NULL : Operator.IS_NULL, conjunction, not);
    }

    private boolean isSubQueryCondition(String condStr) {
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|!=|<>|>=|<=|<|>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+.*?)\\)$",
                Pattern.DOTALL
        );
        return subQueryPattern.matcher(condStr).matches();
    }

    private Condition parseSubQueryCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                             Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                             Map<String, String> columnAliases, String conjunction, boolean not) {
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|!=|<>|>=|<=|<|>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+.*?)\\)$",
                Pattern.DOTALL
        );
        Matcher subQueryMatcher = subQueryPattern.matcher(condStr);
        subQueryMatcher.matches();
        String column = subQueryMatcher.group(1).trim();
        String operatorStr = subQueryMatcher.group(2).trim();
        String subQueryStr = subQueryMatcher.group(3).trim();

        validateSubquery("(" + subQueryStr + ")");
        Query<?> subQuery = parse(subQueryStr, database);
        SubQuery newSubQuery = new SubQuery(subQuery, null);

        Operator operator = parseOperator(operatorStr);
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);

        LOGGER.log(Level.FINE, "Parsed subquery condition: column={0}, operator={1}, subQuery={2}",
                new Object[]{normalizedColumn, operator, subQueryStr});
        return new Condition(normalizedColumn, newSubQuery, operator, conjunction, not);
    }

    private Condition parseComparisonCondition(String condStr, String defaultTableName, Map<String, Class<?>> combinedColumnTypes,
                                               Map<String, String> tableAliases, Map<String, String> columnAliases,
                                               String conjunction, boolean not, boolean isJoinCondition, String conditionStr) {
        String[] operators = {"!=", "<>", ">=", "<=", "=", "<", ">", "\\bLIKE\\b", "\\bNOT LIKE\\b"};
        OperatorInfo operatorInfo = findOperator(condStr, operators);
        if (operatorInfo == null) {
            throw new IllegalArgumentException("Invalid condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorInfo.index).trim();
        String rightPart = condStr.substring(operatorInfo.endIndex).trim();
        String actualRightPart = trimRightPart(rightPart);

        String column = leftPart;
        String actualColumn = resolveColumnAlias(column, columnAliases);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);

        Pattern columnPattern = Pattern.compile("(?i)^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
        String rightColumn = null;
        Object value = null;

        if (columnPattern.matcher(actualRightPart).matches()) {
            rightColumn = actualRightPart;
        } else {
            try {
                value = parseConditionValue(actualColumn, actualRightPart,
                        getColumnType(actualColumn, combinedColumnTypes, defaultTableName, tableAliases, columnAliases));
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.WARNING, "Failed to parse rightPart as value, rechecking as column: rightPart={0}, error={1}",
                        new Object[]{actualRightPart, e.getMessage()});
                if (columnPattern.matcher(actualRightPart).matches()) {
                    rightColumn = actualRightPart;
                } else {
                    throw e;
                }
            }
        }

        Operator operator = parseOperator(operatorInfo.operator);

        if (isJoinCondition && !rightColumnIsFromDifferentTable(actualColumn, rightColumn, tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + condStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, defaultTableName, tableAliases);
            validateColumn(normalizedRightColumn, combinedColumnTypes);
            return new Condition(actualColumn, rightColumn, operator, conjunction, not);
        } else {
            return new Condition(actualColumn, value, operator, conjunction, not);
        }
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
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    if (parenDepth == 1 && i + 7 < condStr.length() &&
                            condStr.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        subQueryStart = i;
                    }
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0 && subQueryStart != -1) {
                        subQueryStart = -1;
                    }
                    continue;
                } else if (parenDepth == 0 && subQueryStart == -1 && i < condStr.length() - 1) {
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
        throw new IllegalArgumentException("Unknown column: " + column);
    }

    private String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        if (column.contains(".")) {
            String[] parts = column.split("\\.");
            String tableOrAlias = parts[0].trim();
            String colName = parts[1].trim();
            String tableName = tableAliases.getOrDefault(tableOrAlias, tableOrAlias);
            return tableName + "." + colName;
        }
        return defaultTableName + "." + column.trim();
    }

    private List<HavingCondition> parseHavingConditions(String havingClause, String defaultTableName, Database database,
                                                        String originalQuery, List<AggregateFunction> aggregates,
                                                        Map<String, Class<?>> combinedColumnTypes,
                                                        Map<String, String> tableAliases, Map<String, String> columnAliases) {
        List<HavingCondition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        String conjunction = null;
        boolean not = false;
        int subQueryStart = -1; // Added declaration

        for (int i = 0; i < havingClause.length(); i++) {
            char c = havingClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    if (parenDepth == 1 && i + 7 < havingClause.length() && havingClause.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        subQueryStart = i;
                    }
                    currentCondition.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentCondition.append(c);
                    if (parenDepth == 0 && subQueryStart != -1) {
                        subQueryStart = -1; // End of subquery
                    }
                    if (parenDepth == 0 && currentCondition.length() > 0) {
                        String condStr = currentCondition.toString().trim();
                        if (condStr.startsWith("(") && condStr.endsWith(")")) {
                            condStr = condStr.substring(1, condStr.length() - 1).trim();
                            if (!condStr.isEmpty()) {
                                List<HavingCondition> subConditions = parseHavingConditions(condStr, defaultTableName,
                                        database, originalQuery, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                                conditions.add(new HavingCondition(subConditions, conjunction, not));
                                LOGGER.log(Level.FINE, "Parsed grouped HAVING condition: {0}, conjunction={1}, not={2}",
                                        new Object[]{subConditions, conjunction, not});
                            }
                        }
                        currentCondition = new StringBuilder();
                        conjunction = null;
                        not = false;
                    }
                    continue;
                } else if (parenDepth == 0 && c == ' ' && subQueryStart == -1) {
                    String nextToken = getNextToken(havingClause, i + 1);
                    if (nextToken.equalsIgnoreCase("AND") || nextToken.equalsIgnoreCase("OR")) {
                        String condStr = currentCondition.toString().trim();
                        if (!condStr.isEmpty()) {
                            HavingCondition condition = parseSingleHavingCondition(condStr, defaultTableName, database, originalQuery,
                                    aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                            conditions.add(condition);
                            LOGGER.log(Level.FINE, "Parsed HAVING condition: {0}", condition);
                            conjunction = nextToken.toUpperCase();
                            not = false;
                            currentCondition = new StringBuilder();
                            i += nextToken.length();
                        } else {
                            conjunction = nextToken.toUpperCase();
                            not = false;
                            currentCondition = new StringBuilder();
                            i += nextToken.length();
                        }
                        continue;
                    } else if (nextToken.equalsIgnoreCase("NOT")) {
                        not = true;
                        currentCondition.append(c);
                        i += nextToken.length();
                        continue;
                    } else if ((nextToken.equalsIgnoreCase("ORDER") && getNextToken(havingClause, i + nextToken.length() + 2).equalsIgnoreCase("BY")) ||
                            (nextToken.equalsIgnoreCase("LIMIT") && subQueryStart == -1) ||
                            (nextToken.equalsIgnoreCase("OFFSET") && subQueryStart == -1)) {
                        String condStr = currentCondition.toString().trim();
                        if (!condStr.isEmpty()) {
                            HavingCondition condition = parseSingleHavingCondition(condStr, defaultTableName, database, originalQuery,
                                    aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                            conditions.add(condition);
                            LOGGER.log(Level.FINE, "Parsed HAVING condition before LIMIT/OFFSET/ORDER BY: {0}", condition);
                        }
                        break;
                    }
                }
            }
            currentCondition.append(c);
        }

        String finalCondStr = currentCondition.toString().trim();
        if (!finalCondStr.isEmpty()) {
            HavingCondition condition = parseSingleHavingCondition(finalCondStr, defaultTableName, database, originalQuery,
                    aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
            conditions.add(condition);
            LOGGER.log(Level.FINE, "Parsed final HAVING condition: {0}", condition);
        }

        return conditions;
    }

    private HavingCondition parseSingleHavingCondition(String condStr, String defaultTableName, Database database,
                                                       String originalQuery, List<AggregateFunction> aggregates,
                                                       Map<String, Class<?>> combinedColumnTypes,
                                                       Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                       String conjunction, boolean not) {
        if (condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")")) {
            String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
            List<HavingCondition> subConditions = parseHavingConditions(subCondStr, defaultTableName, database, originalQuery,
                    aggregates, combinedColumnTypes, tableAliases, columnAliases);
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
            Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.*\\))\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
            Matcher aggMatcher = aggPattern.matcher(leftPart);
            if (aggMatcher.matches()) {
                String funcName = aggMatcher.group(1);
                String columnOrSubQuery = aggMatcher.group(2);
                String alias = aggMatcher.group(3);
                if (columnOrSubQuery.startsWith("(") && columnOrSubQuery.endsWith(")")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregate = new AggregateFunction(funcName, new SubQuery(subQuery, null), alias);
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, defaultTableName, tableAliases);
                    String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                    boolean found = false;
                    for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
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

        Class<?> valueType = aggregate.functionName.equals("COUNT") ? Long.class :
                (aggregate.column != null ? getColumnType(aggregate.column, combinedColumnTypes, defaultTableName, tableAliases, columnAliases) : Double.class);
        Object value = parseConditionValue(aggregate.toString(), rightPart, valueType);

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
                                "\\s*\\(\\s*SELECT\\s+.*?\\s+FROM\\s+.*?\\s*\\)",
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

    private boolean areSubQueriesEquivalent(String subQuery1, String subQuery2) {
        String norm1 = normalizeQueryString(subQuery1).replaceAll("\\s+", " ").trim();
        String norm2 = normalizeQueryString(subQuery2).replaceAll("\\s+", " ").trim();

        norm1 = norm1.replace("LIMIT1", "LIMIT 1").replace(" = ", "=").replace("( ", "(").replace(" )", ")");
        norm2 = norm2.replace("LIMIT1", "LIMIT 1").replace(" = ", "=").replace("( ", "(").replace(" )", ")");

        LOGGER.log(Level.FINEST, "Comparing normalized subqueries: norm1='{0}', norm2='{1}'", new Object[]{norm1, norm2});

        if (norm1.equalsIgnoreCase(norm2)) {
            LOGGER.log(Level.FINE, "Subqueries match by normalized string: {0}", norm1);
            return true;
        }

        try {
            Pattern selectPattern = Pattern.compile(
                    "(?i)^\\(\\s*SELECT\\s+([^\\s]+)\\s+FROM\\s+([^\\s]+)(?:\\s+WHERE\\s+(.+?))?(?:\\s+LIMIT\\s+(\\d+))?\\s*\\)$"
            );
            Matcher matcher1 = selectPattern.matcher(norm1);
            Matcher matcher2 = selectPattern.matcher(norm2);

            if (!matcher1.matches() || !matcher2.matches()) {
                LOGGER.log(Level.FINE, "Subquery pattern mismatch: {0} vs {1}", new Object[]{norm1, norm2});
                return false;
            }

            String select1 = matcher1.group(1).trim();
            String select2 = matcher2.group(1).trim();
            String from1 = matcher1.group(2).trim();
            String from2 = matcher2.group(2).trim();
            String where1 = matcher1.group(3) != null ? normalizeQueryString(matcher1.group(3).trim()).replaceAll("\\s+", " ") : "";
            String where2 = matcher2.group(3) != null ? normalizeQueryString(matcher2.group(3).trim()).replaceAll("\\s+", " ") : "";
            String limit1 = matcher1.group(4) != null ? matcher1.group(4).trim() : "";
            String limit2 = matcher2.group(4) != null ? matcher2.group(4).trim() : "";

            where1 = where1.replace("ID=U.ID", "ID = U.ID").replace("U.ID=ID", "ID = U.ID");
            where2 = where2.replace("ID=U.ID", "ID = U.ID").replace("U.ID=ID", "ID = U.ID");

            boolean selectMatch = select1.equalsIgnoreCase(select2);
            boolean fromMatch = from1.equalsIgnoreCase(from2);
            boolean whereMatch = where1.equalsIgnoreCase(where2);
            boolean limitMatch = limit1.equalsIgnoreCase(limit2);

            LOGGER.log(Level.FINEST, "Subquery comparison: select={0}, from={1}, where={2}, limit={3}, where1='{4}', where2='{5}'",
                    new Object[]{selectMatch, fromMatch, whereMatch, limitMatch, where1, where2});

            return selectMatch && fromMatch && whereMatch && limitMatch;
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to compare subquery structures: {0}", e.getMessage());
            return false;
        }
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
                    .replaceAll("(?i)\\bLIKE\\b", "LIKE")
                    .replaceAll("(?i)\\bNOT_LIKE\\b", "NOT LIKE");
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
                continue;
            }
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
            // Only collapse spaces outside subqueries
            if (!inSubQuery && Character.isWhitespace(c) && finalResult.length() > 0 && Character.isWhitespace(finalResult.charAt(finalResult.length() - 1))) {
                continue;
            }
            finalResult.append(c);
        }

        return finalResult.toString().trim();
    }

    private String normalizeQueryString(String query) {
        return query.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*([=><!(),])\\s*", "$1")
                .replaceAll("(?i)\\bEQUALS\\b", "=")
                .replaceAll("(?i)\\bNOT_EQUALS\\b", "!=")
                .replaceAll("(?i)\\bGREATER_THAN\\b", ">") // Add this line
                .replaceAll("(?i)\\bLIKE\\b", "LIKE")
                .replaceAll("(?i)\\bNOT_LIKE\\b", "NOT LIKE")
                .replaceAll("\\s*;", "")
                .replaceAll("(?i)\\bLIMIT\\s*(\\d+)\\b", " LIMIT $1 ")
                .replaceAll("(?i)\\bWHERE\\b", " WHERE ")
                .replaceAll("(?i)\\bFROM\\b", " FROM ")
                .replaceAll("(?i)\\bSELECT\\b", " SELECT ")
                .replaceAll("(?i)\\bAS\\b", " AS ")
                .replaceAll("\\(\\s+", "(")
                .replaceAll("\\s+\\)", ")")
                .replaceAll("(?i)\\bID\\s*=\\s*U\\.ID\\b", "ID=U.ID")
                .replaceAll("(?i)\\bU\\.ID\\s*=\\s*ID\\b", "ID=U.ID")
                .toUpperCase()
                .replaceAll("\\s*=", "=")
                .replaceAll("=\\s*", "=");
    }
}