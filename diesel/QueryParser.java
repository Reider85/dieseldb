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

    private boolean isOperator(String str) {
        for (String op : OPERATORS) {
            if (str.toUpperCase().startsWith(op)) {
                return true;
            }
        }
        return false;
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
            LOGGER.log(Level.FINEST, "Токен: start={0}, end={1}, type={2}, value={3}, bracketDepth={4}",
                    new Object[]{start, nextPos, tokenType, token, bracketDepth});

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
        int fromIndex = findMainFromClause(original);
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid SELECT query format: missing FROM clause");
        }

        String selectPartOriginal = original.substring(original.indexOf("SELECT") + 6, fromIndex).trim();
        String tableAndJoinsOriginal = original.substring(fromIndex + 4).trim();

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
                    LOGGER.log(Level.FINE, "Parsed aggregate function: COUNT(subquery){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    String column = countArg.equals("*") ? null : countArg;
                    aggregates.add(new AggregateFunction("COUNT", column, alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: COUNT({0}){1}",
                            new Object[]{column == null ? "*" : column, alias != null ? " AS " + alias : ""});
                }
            } else if (minMatcher.matches()) {
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("MIN", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: MIN(subquery){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("MIN", column, alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: MIN({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (maxMatcher.matches()) {
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("MAX", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: MAX(subquery){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("MAX", column, alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: MAX({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (avgMatcher.matches()) {
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("AVG", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: AVG(subquery){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("AVG", column, alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: AVG({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (sumMatcher.matches()) {
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                if (column.startsWith("(") && column.endsWith(")")) {
                    String subQueryStr = column.substring(1, column.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    aggregates.add(new AggregateFunction("SUM", new SubQuery(subQuery, null), alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: SUM(subquery){0}", new Object[]{alias != null ? " AS " + alias : ""});
                } else {
                    aggregates.add(new AggregateFunction("SUM", column, alias));
                    LOGGER.log(Level.FINE, "Parsed aggregate function: SUM({0}){1}",
                            new Object[]{column, alias != null ? " AS " + alias : ""});
                }
            } else if (subQueryMatcher.matches()) {
                int subQueryEnd = findMatchingParenthesis(trimmedItem, 0);
                if (subQueryEnd == -1) {
                    LOGGER.log(Level.SEVERE, "Failed to find matching parenthesis for subquery: {0}", trimmedItem);
                    throw new IllegalArgumentException("Invalid subquery syntax in SELECT: " + trimmedItem);
                }
                String subQueryStr = trimmedItem.substring(1, subQueryEnd).trim();
                if (subQueryStr.isEmpty()) {
                    LOGGER.log(Level.SEVERE, "Empty subquery detected in SELECT: {0}", trimmedItem);
                    throw new IllegalArgumentException("Empty subquery in SELECT: " + trimmedItem);
                }
                String alias = subQueryMatcher.group(1);
                Query<?> subQuery = parse(subQueryStr, database);
                SubQuery newSubQuery = new SubQuery(subQuery, alias);
                subQueries.add(newSubQuery);
                if (alias != null) {
                    String subQueryKey = "SUBQUERY_" + subQueries.size();
                    columnAliases.put(subQueryKey, alias);
                    LOGGER.log(Level.FINE, "Added subquery alias to columnAliases: {0} -> {1}", new Object[]{subQueryKey, alias});
                }
                LOGGER.log(Level.FINE, "Parsed subquery in SELECT: {0}{1}, subQueries size: {2}, subQuery: {3}",
                        new Object[]{subQueryStr, alias != null ? " AS " + alias : "", subQueries.size(), newSubQuery});
            } else if (columnMatcher.matches()) {
                String column = columnMatcher.group(1);
                String alias = columnMatcher.group(2);
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                    LOGGER.log(Level.FINE, "Parsed column with alias: {0} AS {1}", new Object[]{column, alias});
                } else {
                    LOGGER.log(Level.FINE, "Parsed column: {0}", new Object[]{column});
                }
            } else {
                throw new IllegalArgumentException("Invalid SELECT item: " + trimmedItem);
            }
        }

        String tableAndJoins = tableAndJoinsOriginal.toUpperCase().trim();
        List<JoinInfo> joins = new ArrayList<>();
        String tableName;
        String tableAlias = null;
        String conditionStr = null;
        Integer limit = null;
        Integer offset = null;
        List<OrderByInfo> orderBy = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<HavingCondition> havingConditions = new ArrayList<>();
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
            LOGGER.log(Level.FINE, "Parsed main table alias: {0} -> {1}", new Object[]{tableAlias, tableName});
        }

        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
        tableAliases.put(tableName, tableName);

        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();

            JoinType joinType;
            switch (joinTypeStr) {
                case "JOIN":
                case "INNER JOIN":
                    joinType = JoinType.INNER;
                    break;
                case "LEFT JOIN":
                case "LEFT OUTER JOIN":
                    joinType = JoinType.LEFT_OUTER;
                    break;
                case "RIGHT JOIN":
                case "RIGHT OUTER JOIN":
                    joinType = JoinType.RIGHT_OUTER;
                    break;
                case "FULL JOIN":
                case "FULL OUTER JOIN":
                    joinType = JoinType.FULL_OUTER;
                    break;
                case "LEFT INNER JOIN":
                    joinType = JoinType.LEFT_INNER;
                    break;
                case "RIGHT INNER JOIN":
                    joinType = JoinType.RIGHT_INNER;
                    break;
                case "CROSS JOIN":
                    joinType = JoinType.CROSS;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported join type: " + joinTypeStr);
            }

            String joinTableName;
            String joinTableAlias = null;
            List<Condition> onConditions = new ArrayList<>();

            String[] joinTableTokens = joinPart.split("\\s+(?=(ON|JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|WHERE|LIMIT|OFFSET|ORDER BY|$))", 2);
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
                LOGGER.log(Level.FINE, "Parsed join table alias: {0} -> {1}", new Object[]{joinTableAlias, joinTableName});
            }

            if (joinType == JoinType.CROSS) {
                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                tableAliases.put(joinTableName, joinTableName);
                if (joinTableTokens.length > 1 && !joinTableTokens[1].trim().isEmpty()) {
                    String remaining = joinTableTokens[1].trim();
                    if (remaining.toUpperCase().startsWith("WHERE ")) {
                        conditionStr = remaining.substring(6).trim();
                    } else if (remaining.toUpperCase().startsWith("LIMIT ")) {
                        String[] limitSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                        limit = parseLimitClause("LIMIT " + limitSplit[0].substring(6).trim());
                        if (limitSplit.length > 1) {
                            offset = parseOffsetClause("OFFSET " + limitSplit[1].trim());
                        }
                    } else if (remaining.toUpperCase().startsWith("OFFSET ")) {
                        offset = parseOffsetClause(remaining);
                    } else if (remaining.toUpperCase().startsWith("ORDER BY ")) {
                        orderBy = parseOrderByClause(remaining.substring(9).trim(), tableName, combinedColumnTypes, columnAliases, tableAliases, subQueries);
                    }
                    if (remaining.toUpperCase().contains(" ON ")) {
                        throw new IllegalArgumentException("CROSS JOIN does not support ON: " + joinPart);
                    }
                }
                LOGGER.log(Level.FINE, "Parsed CROSS JOIN: table={0}, alias={1}", new Object[]{joinTableName, joinTableAlias});
            } else {
                if (joinTableTokens.length != 2) {
                    throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: missing ON");
                }
                String onClause = joinTableTokens[1].trim();
                String[] onSplit = onClause.split("(?i)ON\\s+", 2);
                if (onSplit.length != 2) {
                    throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: invalid ON");
                }
                String onCondition = onSplit[1].trim();
                String remaining = "";

                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                tableAliases.put(joinTableName, joinTableName);

                // Find WHERE clause outside subqueries
                int whereIndex = findClauseOutsideSubquery(onCondition, "WHERE");
                if (whereIndex != -1) {
                    String beforeWhere = onCondition.substring(0, whereIndex).trim();
                    remaining = onCondition.substring(whereIndex + 6).trim(); // "WHERE ".length() == 6
                    LOGGER.log(Level.FINEST, "Processing WHERE clause: remaining={0}", remaining);

                    // Проверяем, содержит ли remaining подзапрос
                    subQueryPattern = Pattern.compile("(?i).*?\\((SELECT\\s+.*?\\s*?)\\).*", Pattern.DOTALL);
                    Matcher subQueryMatcher = subQueryPattern.matcher(remaining);
                    if (subQueryMatcher.matches()) {
                        String subQueryStr = subQueryMatcher.group(1);
                        int subQueryStart = remaining.indexOf("(" + subQueryStr);
                        int subQueryEnd = findMatchingParenthesis(remaining, subQueryStart);
                        if (subQueryEnd == -1) {
                            LOGGER.log(Level.SEVERE, "Failed to find matching parenthesis for subquery in WHERE: {0}", remaining);
                            throw new IllegalArgumentException("Invalid subquery syntax in WHERE clause: " + remaining);
                        }
                        conditionStr = remaining.substring(0, subQueryEnd + 1).trim();
                        LOGGER.log(Level.FINEST, "Extracted subquery condition: {0}", conditionStr);

                        // Остаток после подзапроса (например, LIMIT, OFFSET)
                        String postSubQuery = remaining.substring(subQueryEnd + 1).trim();
                        if (!postSubQuery.isEmpty()) {
                            // Обработка LIMIT, OFFSET и других клауз
                            String[] limitSplit = postSubQuery.split("(?i)\\s+LIMIT\\s+", 2);
                            if (limitSplit.length > 1) {
                                String limitClause = "LIMIT " + limitSplit[1].trim();
                                limit = parseLimitClause(limitClause);
                                String[] offsetSplit = limitClause.split("(?i)\\s+OFFSET\\s+", 2);
                                if (offsetSplit.length > 1) {
                                    offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
                                }
                            }
                        }
                    } else {
                        conditionStr = remaining;
                    }
                    onCondition = beforeWhere;
                    LOGGER.log(Level.FINEST, "Split ON condition at WHERE: onCondition={0}, conditionStr={1}", new Object[]{onCondition, conditionStr});
                } else {
                    conditionStr = onCondition;
                    onCondition = "";
                }

                // Find GROUP BY clause outside subqueries
                int groupByIndex = findClauseOutsideSubquery(onCondition, "GROUP BY");
                if (groupByIndex != -1) {
                    String beforeGroupBy = onCondition.substring(0, groupByIndex).trim();
                    String groupByPart = onCondition.substring(groupByIndex + 9).trim(); // "GROUP BY ".length() == 9
                    onCondition = beforeGroupBy;
                    String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                            ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                            : new String[]{groupByPart, ""};
                    groupBy = parseGroupByClause(havingSplit[0], mainTable.getName(), combinedColumnTypes, tableAliases, columnAliases, subQueries);
                    LOGGER.log(Level.FINE, "Parsed GROUP BY: {0}", groupBy);
                    if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                        String havingClause = havingSplit[1].trim();
                        havingConditions = parseHavingConditions(havingClause, mainTable.getName(), database, original, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                        LOGGER.log(Level.FINE, "Parsed HAVING: {0}", havingConditions);
                    }
                }

                // Find LIMIT clause outside subqueries
                // In parseSelectQuery, replace the LIMIT clause parsing logic
                // Find LIMIT clause outside subqueries
                int limitIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "LIMIT");
                if (limitIndex != -1) {
                    String beforeLimit = tableAndJoinsOriginal.substring(0, limitIndex).trim();
                    String afterLimit = tableAndJoinsOriginal.substring(limitIndex + 5).trim(); // "LIMIT ".length() == 5
                    Pattern limitPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:$|\\s+OFFSET\\s+|\\s*;\\s*$)");
                    Matcher limitMatcher = limitPattern.matcher(afterLimit);
                    if (limitMatcher.find()) {
                        String limitValue = limitMatcher.group(1);
                        try {
                            limit = Integer.parseInt(limitValue);
                            LOGGER.log(Level.FINE, "Parsed LIMIT value: {0}", limitValue);
                        } catch (NumberFormatException e) {
                            LOGGER.log(Level.SEVERE, "Invalid LIMIT value: {0}", limitValue);
                            throw new IllegalArgumentException("Invalid LIMIT value: " + limitValue);
                        }
                        String[] limitOffsetSplit = afterLimit.split("(?i)\\s+OFFSET\\s+", 2);
                        if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                            offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                        }
                    } else {
                        LOGGER.log(Level.SEVERE, "Invalid LIMIT clause format: {0}", afterLimit);
                        throw new IllegalArgumentException("Invalid LIMIT clause format: " + afterLimit);
                    }
                    tableAndJoinsOriginal = beforeLimit; // Exclude LIMIT from further processing
                }
                // Find OFFSET clause outside subqueries
                int offsetIndex = findClauseOutsideSubquery(onCondition, "OFFSET");
                if (offsetIndex != -1) {
                    String beforeOffset = onCondition.substring(0, offsetIndex).trim();
                    String offsetPart = onCondition.substring(offsetIndex + 7).trim(); // "OFFSET ".length() == 7
                    onCondition = beforeOffset;
                    offset = parseOffsetClause("OFFSET " + offsetPart);
                    LOGGER.log(Level.FINEST, "Split ON condition at OFFSET: onCondition={0}, offset={1}",
                            new Object[]{onCondition, offset});
                }

                // Find ORDER BY clause outside subqueries
                int orderByIndex = findClauseOutsideSubquery(onCondition, "ORDER BY");
                if (orderByIndex != -1) {
                    String beforeOrderBy = onCondition.substring(0, orderByIndex).trim();
                    String orderByPart = onCondition.substring(orderByIndex + 9).trim(); // "ORDER BY ".length() == 9
                    onCondition = beforeOrderBy;
                    orderBy = parseOrderByClause(orderByPart, mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases, subQueries);
                    LOGGER.log(Level.FINE, "Parsed ORDER BY: {0}", orderBy);
                }

                LOGGER.log(Level.FINEST, "Parsing ON condition: {0}", onCondition);
                onConditions = parseConditions(onCondition, tableName, database, original, true, combinedColumnTypes, tableAliases, columnAliases);

                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }

                LOGGER.log(Level.FINE, "Parsed ON conditions for {0}: {1}", new Object[]{joinTypeStr, onConditions});
                joins.add(new JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
                tableName = joinTableName;

                // Process remaining clauses
                if (!remaining.isEmpty()) {
                    if (remaining.toUpperCase().startsWith("LIMIT ")) {
                        String[] whereLimitSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                        conditionStr = whereLimitSplit[0].trim();
                        if (whereLimitSplit.length > 1) {
                            limit = parseLimitClause("LIMIT " + whereLimitSplit[0].substring(6).trim());
                            offset = parseOffsetClause("OFFSET " + whereLimitSplit[1].trim());
                        } else {
                            limit = parseLimitClause("LIMIT " + whereLimitSplit[0].trim());
                        }
                    } else if (remaining.toUpperCase().startsWith("OFFSET ")) {
                        String[] whereOffsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                        conditionStr = whereOffsetSplit[0].trim();
                        if (whereOffsetSplit.length > 1) {
                            offset = parseOffsetClause("OFFSET " + whereOffsetSplit[1].trim());
                        }
                    } else if (remaining.toUpperCase().startsWith("ORDER BY ")) {
                        String[] whereOrderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
                        conditionStr = whereOrderBySplit[0].trim();
                        if (whereOrderBySplit.length > 1) {
                            orderBy = parseOrderByClause(whereOrderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases, subQueries);
                        }
                    } else if (remaining.toUpperCase().contains(" GROUP BY ")) {
                        String[] whereGroupBySplit = remaining.split("(?i)(?<=\\bNULL\\b|\\bNOT NULL\\b|\\bIN\\b\\s*\\([^)]*\\)|[^=><!])\\s+GROUP BY\\s+", 2);
                        conditionStr = whereGroupBySplit[0].trim();
                        if (whereGroupBySplit.length > 1) {
                            String groupByPart = whereGroupBySplit[1].trim();
                            String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                                    ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                                    : new String[]{groupByPart, ""};
                            groupBy = parseGroupByClause(havingSplit[0], mainTable.getName(), combinedColumnTypes, tableAliases, columnAliases, subQueries);
                            LOGGER.log(Level.FINE, "Parsed GROUP BY: {0}", groupBy);
                            if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                                String havingClause = havingSplit[1].trim();
                                havingConditions = parseHavingConditions(havingClause, mainTable.getName(), database, original, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                                LOGGER.log(Level.FINE, "Parsed HAVING: {0}", havingConditions);
                            }
                        }
                    } else {
                        conditionStr = remaining;
                    }
                }
            }
        }

        String remaining = joinParts.get(joinParts.size() - 1);
        if (conditionStr == null && remaining.toUpperCase().contains(" WHERE ")) {
            String[] whereSplit = remaining.split("(?i)\\s+WHERE\\s+", 2);
            conditionStr = whereSplit[1].trim();
            remaining = whereSplit[0].trim();
        }

        List<Condition> conditions = new ArrayList<>();
        if (conditionStr != null && !conditionStr.isEmpty()) {
            conditions = parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes, tableAliases, columnAliases);
            LOGGER.log(Level.FINE, "Parsed WHERE conditions: {0}", conditions);
        }

        if (!remaining.isEmpty()) {
            String[] remainingParts = remaining.toUpperCase().contains(" GROUP BY ")
                    ? remaining.split("(?i)(?<=\\bNULL\\b|\\bNOT NULL\\b|\\bIN\\b\\s*\\([^)]*\\)|[^=><!])\\s+GROUP BY\\s+", 2)
                    : new String[]{remaining, ""};
            remaining = remainingParts[0].trim();
            if (remainingParts.length > 1) {
                String groupByPart = remainingParts[1].trim();
                String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                        ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                        : new String[]{groupByPart, ""};
                groupBy = parseGroupByClause(havingSplit[0], mainTable.getName(), combinedColumnTypes, tableAliases, columnAliases, subQueries);
                LOGGER.log(Level.FINE, "Parsed GROUP BY: {0}", groupBy);
                if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                    String havingClause = havingSplit[1].trim();
                    havingConditions = parseHavingConditions(havingClause, mainTable.getName(), database, original, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                    LOGGER.log(Level.FINE, "Parsed HAVING: {0}", havingConditions);
                }
            }

            if (remaining.toUpperCase().contains(" ORDER BY ")) {
                String[] orderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
                remaining = orderBySplit[0].trim();
                if (orderBySplit.length > 1) {
                    orderBy = parseOrderByClause(orderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases, subQueries);
                    LOGGER.log(Level.FINE, "Parsed ORDER BY: {0}", orderBy);
                }
            }

            if (remaining.toUpperCase().contains(" LIMIT ")) {
                String[] limitSplit = remaining.split("(?i)\\s+LIMIT\\s+", 2);
                remaining = limitSplit[0].trim();
                if (limitSplit.length > 1) {
                    String[] limitOffsetSplit = limitSplit[1].toUpperCase().contains(" OFFSET ")
                            ? limitSplit[1].split("(?i)\\s+OFFSET\\s+", 2)
                            : new String[]{limitSplit[1], ""};
                    limit = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
                    if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                        offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                    }
                }
            }

            if (remaining.toUpperCase().contains(" OFFSET ")) {
                String[] offsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                if (offsetSplit.length > 1) {
                    offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
                }
            }
        }

        for (String column : columns) {
            String normalizedColumn = normalizeColumnName(column, mainTable.getName(), tableAliases);
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
                throw new IllegalArgumentException("Unknown column: " + column);
            }
        }

        for (JoinInfo join : joins) {
            if (join.joinType != JoinType.CROSS && join.onConditions.isEmpty()) {
                throw new IllegalArgumentException("Join requires ON condition: " + join);
            }
        }

        LOGGER.log(Level.INFO, "Parsed SELECT query: table={0}, alias={1}, columns={2}, aggregates={3}, subQueries={4}, joins={5}, conditions={6}, groupBy={7}, having={8}, orderBy={9}, limit={10}, offset={11}",
                new Object[]{tableName, tableAlias, columns, aggregates, subQueries, joins, conditions, groupBy, havingConditions, orderBy, limit, offset});

        return new SelectQuery(tableName, tableAlias, columns, aggregates, joins, conditions, groupBy, havingConditions, orderBy, limit, offset, subQueries, columnAliases, tableAliases, combinedColumnTypes);
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        if (query == null || query.isEmpty() || clause == null || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        String normalizedQuery = query.trim();
        String upperClause = clause.toUpperCase();

        // Регулярное выражение для строк в одинарных кавычках
        String quotedStringRegex = "'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'";
        // Регулярное выражение для подзапросов
        String subQueryRegex = "\\(SELECT\\s+.*?\\s*\\)";
        // Регулярное выражение для ключевого слова вне строк и подзапросов
        String clauseRegex = "(?i)" + Pattern.quote("\\b" + upperClause + "\\b");

        // Заменяем строки и подзапросы на плейсхолдеры, чтобы исключить их из поиска
        String processedQuery = normalizedQuery;
        List<String> placeholders = new ArrayList<>();
        int placeholderIndex = 0;

        // Заменяем строки
        Matcher quotedStringMatcher = Pattern.compile(quotedStringRegex).matcher(processedQuery);
        StringBuilder tempQuery = new StringBuilder();
        int lastEnd = 0;
        while (quotedStringMatcher.find()) {
            tempQuery.append(processedQuery, lastEnd, quotedStringMatcher.start());
            String placeholder = "STRING_" + placeholderIndex++;
            tempQuery.append(placeholder);
            placeholders.add(quotedStringMatcher.group());
            lastEnd = quotedStringMatcher.end();
        }
        tempQuery.append(processedQuery.substring(lastEnd));
        processedQuery = tempQuery.toString();

        // Заменяем подзапросы
        tempQuery.setLength(0);
        lastEnd = 0;
        Matcher subQueryMatcher = Pattern.compile(subQueryRegex, Pattern.DOTALL).matcher(processedQuery);
        while (subQueryMatcher.find()) {
            tempQuery.append(processedQuery, lastEnd, subQueryMatcher.start());
            String placeholder = "SUBQUERY_" + placeholderIndex++;
            tempQuery.append(placeholder);
            placeholders.add(subQueryMatcher.group());
            lastEnd = subQueryMatcher.end();
        }
        tempQuery.append(processedQuery.substring(lastEnd));
        processedQuery = tempQuery.toString();

        // Ищем ключевое слово в обработанной строке
        Matcher clauseMatcher = Pattern.compile(clauseRegex).matcher(processedQuery);
        if (clauseMatcher.find()) {
            int indexInProcessed = clauseMatcher.start();
            String beforeMatch = processedQuery.substring(0, indexInProcessed);
            int actualIndex = indexInProcessed;

            // Корректируем индекс, учитывая длину плейсхолдеров
            for (String placeholder : placeholders) {
                String placeholderRegex = Pattern.quote(placeholder);
                Matcher placeholderMatcher = Pattern.compile(placeholderRegex).matcher(beforeMatch);
                while (placeholderMatcher.find()) {
                    int placeholderLength = placeholder.length();
                    int originalLength = placeholders.get(Integer.parseInt(placeholder.substring(placeholder.indexOf("_") + 1))).length();
                    actualIndex += (originalLength - placeholderLength);
                }
            }

            LOGGER.log(Level.FINEST, "Найдено ключевое слово {0} на позиции {1} в запросе: {2}",
                    new Object[]{clause, actualIndex, query});
            return actualIndex;
        }

        LOGGER.log(Level.FINEST, "Ключевое слово {0} не найдено в запросе: {1}", new Object[]{clause, query});
        return -1;
    }

    private List<String> parseGroupByClause(String groupByClause, String defaultTableName, Map<String, Class<?>> combinedColumnTypes,
                                            Map<String, String> tableAliases, Map<String, String> columnAliases, List<SubQuery> selectSubQueries) {
        List<String> groupBy = new ArrayList<>();
        String[] groupByParts = splitGroupByClause(groupByClause);
        Pattern columnPattern = Pattern.compile("(?i)^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+.*?\\s*\\)$");

        for (String part : groupByParts) {
            String trimmedPart = part.trim();
            if (trimmedPart.isEmpty()) {
                throw new IllegalArgumentException("Invalid GROUP BY clause: empty expression");
            }

            String expression = trimmedPart;
            boolean found = false;

            // First check if it's a direct column reference
            if (columnPattern.matcher(expression).matches()) {
                String normalizedColumn = normalizeColumnName(expression, defaultTableName, tableAliases);
                String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;

                // Check against column aliases
                for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
                    if (aliasEntry.getValue().equalsIgnoreCase(unqualifiedColumn)) {
                        groupBy.add(aliasEntry.getKey());
                        found = true;
                        break;
                    }
                }

                // If not found as alias, check against actual columns
                if (!found) {
                    for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                        String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                        if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                            groupBy.add(normalizedColumn);
                            found = true;
                            break;
                        }
                    }
                }
            }

            // If not found yet, check if it's a subquery or subquery alias
            if (!found) {
                // Check against subquery aliases
                for (SubQuery subQuery : selectSubQueries) {
                    if (subQuery.alias != null && subQuery.alias.equalsIgnoreCase(expression)) {
                        groupBy.add(subQuery.alias);
                        found = true;
                        break;
                    }
                }

                // Check if it's a subquery that matches one in the SELECT
                if (!found && subQueryPattern.matcher(expression).matches()) {
                    String normalizedExpression = normalizeQueryString(expression);
                    for (SubQuery subQuery : selectSubQueries) {
                        String subQueryStr = normalizeQueryString("(" + subQuery.query.toString() + ")");
                        if (normalizedExpression.equalsIgnoreCase(subQueryStr) ||
                                areSubQueriesEquivalent(expression, subQuery.query.toString())) {
                            String resolvedExpression = subQuery.alias != null ? subQuery.alias : expression;
                            groupBy.add(resolvedExpression);
                            found = true;
                            break;
                        }
                    }
                }
            }

            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column, alias, or subquery in GROUP BY: {0}, available columns: {1}, aliases: {2}, subqueries: {3}",
                        new Object[]{expression, combinedColumnTypes.keySet(), columnAliases.values(), selectSubQueries});
                throw new IllegalArgumentException("Unknown column in GROUP BY: " + expression);
            }
        }
        return groupBy;
    }

    private String[] splitGroupByClause(String groupByClause) {
        List<String> parts = new ArrayList<>();
        StringBuilder currentPart = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < groupByClause.length(); i++) {
            char c = groupByClause.charAt(i);
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
                } else if (parenDepth == 0 && c == ' ') {
                    String nextToken = getNextToken(groupByClause, i + 1);
                    if (nextToken.equalsIgnoreCase("ORDER") && getNextToken(groupByClause, i + nextToken.length() + 2).equalsIgnoreCase("BY") ||
                            nextToken.equalsIgnoreCase("LIMIT") || nextToken.equalsIgnoreCase("OFFSET")) {
                        String part = currentPart.toString().trim();
                        if (!part.isEmpty()) {
                            parts.add(part);
                        }
                        break;
                    }
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

    private Object parseConditionValue(String column, String value, Class<?> type) {
        try {
            // Обработка строковых значений в кавычках
            if (value.startsWith("'") && value.endsWith("'")) {
                String strippedValue = value.substring(1, value.length() - 1);
                if (type == String.class) {
                    return strippedValue;
                }
                // Для других типов продолжаем с удаленными кавычками
                value = strippedValue;
            }

            // Обработка типов
            if (type == String.class) {
                return value; // Возвращаем строку без дополнительной обработки
            } else if (type == Integer.class) {
                return Integer.parseInt(value);
            } else if (type == Long.class) {
                return Long.parseLong(value);
            } else if (type == BigDecimal.class) {
                return new BigDecimal(value);
            } else if (type == Float.class) {
                return Float.parseFloat(value);
            } else if (type == Double.class) {
                return Double.parseDouble(value);
            } else if (type == Short.class) {
                return Short.parseShort(value);
            } else if (type == Byte.class) {
                return Byte.parseByte(value);
            } else if (type == Character.class) {
                if (value.length() != 1) {
                    throw new IllegalArgumentException("Character value must be a single character: " + value);
                }
                return value.charAt(0);
            } else if (type == Boolean.class) {
                return Boolean.parseBoolean(value);
            } else if (type == UUID.class) {
                return UUID.fromString(value);
            } else if (type == LocalDate.class) {
                return LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE);
            } else if (type == LocalDateTime.class) {
                if (value.contains(".")) {
                    return LocalDateTime.parse(value, DATETIME_MS_FORMATTER);
                } else {
                    return LocalDateTime.parse(value, DATETIME_FORMATTER);
                }
            }
            throw new IllegalArgumentException("Unsupported column type: " + type.getSimpleName() + " for value: " + value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value for column " + column + ": " + value + " (expected type: " + type.getSimpleName() + ")", e);
        }
    }

    private List<Condition> parseConditions(String conditionStr, String defaultTableName, Database database, String originalQuery,
                                            boolean isJoinCondition, Map<String, Class<?>> combinedColumnTypes,
                                            Map<String, String> tableAliases, Map<String, String> columnAliases) {
        List<Condition> conditions = new ArrayList<>();
        if (conditionStr == null || conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Empty or null condition string, returning empty conditions list");
            return conditions;
        }

        // Регулярные выражения
        Pattern quotedStringPattern = Pattern.compile("'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        Pattern inClausePattern = Pattern.compile(
                "\\b[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*\\s*(?:NOT\\s+)?IN\\s*\\([^)]+\\)");
        Pattern logicalOperatorPattern = Pattern.compile("\\bAND\\b|\\bOR\\b", Pattern.CASE_INSENSITIVE);
        Pattern operatorPattern = Pattern.compile("\\s*(?:=|[!=<>]=|[<>]|LIKE|NOT\\s+LIKE)\\s*");
        Pattern keywordPattern = Pattern.compile("\\bLIMIT\\b|\\bOFFSET\\b|\\bORDER BY\\b|\\bGROUP BY\\b", Pattern.CASE_INSENSITIVE);
        Pattern whitespacePattern = Pattern.compile("\\s+");
        Pattern identifierPattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*");
        Pattern numberPattern = Pattern.compile("-?\\d+(\\.\\d+)?");
        Pattern balancedParenPattern = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)");

        // Списки для хранения плейсхолдеров и IN-условий
        StringBuilder processedInput = new StringBuilder();
        List<String> inConditions = new ArrayList<>();
        List<String> placeholders = new ArrayList<>(); // Хранит оригинальные строки
        int placeholderIndex = 0;

        // Заменяем строки на плейсхолдеры
        String normalized = conditionStr;
        Matcher quotedStringMatcher = quotedStringPattern.matcher(normalized);
        int lastEnd = 0;
        while (quotedStringMatcher.find()) {
            processedInput.append(normalized, lastEnd, quotedStringMatcher.start());
            String placeholder = "STRING_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(quotedStringMatcher.group()); // Сохраняем строку
            LOGGER.log(Level.FINEST, "Replaced string with placeholder: {0} -> {1}", new Object[]{quotedStringMatcher.group(), placeholder});
            lastEnd = quotedStringMatcher.end();
        }
        processedInput.append(normalized.substring(lastEnd));
        normalized = processedInput.toString().trim();
        LOGGER.log(Level.FINEST, "After replacing strings: {0}", normalized);

        // Обработка IN-условий
        processedInput.setLength(0);
        lastEnd = 0;
        Matcher inMatcher = inClausePattern.matcher(normalized);
        LOGGER.log(Level.FINEST, "Applying inClausePattern on input: {0}", normalized);
        while (inMatcher.find()) {
            processedInput.append(normalized, lastEnd, inMatcher.start());
            String fullMatch = inMatcher.group();
            inConditions.add(fullMatch);
            processedInput.append("IN_CONDITION_" + inConditions.size());
            LOGGER.log(Level.FINEST, "Replaced IN clause: {0} -> IN_CONDITION_{1}", new Object[]{fullMatch, inConditions.size()});
            lastEnd = inMatcher.end();
        }
        processedInput.append(normalized.substring(lastEnd));
        normalized = processedInput.toString().trim();
        LOGGER.log(Level.FINEST, "After processing IN clauses: {0}", normalized);

        // Разбиваем на токены
        List<String> tokens = new ArrayList<>();
        String remaining = normalized;
        while (!remaining.isEmpty()) {
            String matchedToken = null;
            int matchLength = 0;

            Matcher quotedStringMatcher2 = quotedStringPattern.matcher(remaining);
            if (quotedStringMatcher2.lookingAt()) {
                matchedToken = quotedStringMatcher2.group();
                matchLength = matchedToken.length();
                LOGGER.log(Level.FINEST, "Matched quoted string: {0}", matchedToken);
            } else {
                Matcher balancedParenMatcher = balancedParenPattern.matcher(remaining);
                if (balancedParenMatcher.lookingAt()) {
                    matchedToken = balancedParenMatcher.group();
                    matchLength = matchedToken.length();
                    LOGGER.log(Level.FINEST, "Matched balanced parentheses: {0}", matchedToken);
                } else {
                    Matcher identifierMatcher = identifierPattern.matcher(remaining);
                    if (identifierMatcher.lookingAt()) {
                        matchedToken = identifierMatcher.group();
                        matchLength = matchedToken.length();
                        LOGGER.log(Level.FINEST, "Matched identifier: {0}", matchedToken);
                    } else {
                        Matcher numberMatcher = numberPattern.matcher(remaining);
                        if (numberMatcher.lookingAt()) {
                            matchedToken = numberMatcher.group();
                            matchLength = matchedToken.length();
                            LOGGER.log(Level.FINEST, "Matched number: {0}", matchedToken);
                        } else {
                            Matcher keywordMatcher = keywordPattern.matcher(remaining);
                            if (keywordMatcher.lookingAt()) {
                                matchedToken = keywordMatcher.group();
                                matchLength = matchedToken.length();
                                LOGGER.log(Level.FINEST, "Matched keyword: {0}", matchedToken);
                            } else {
                                Matcher operatorMatcher = operatorPattern.matcher(remaining);
                                if (operatorMatcher.lookingAt()) {
                                    matchedToken = operatorMatcher.group();
                                    matchLength = matchedToken.length();
                                    LOGGER.log(Level.FINEST, "Matched operator: {0}", matchedToken);
                                } else {
                                    Matcher whitespaceMatcher = whitespacePattern.matcher(remaining);
                                    if (whitespaceMatcher.lookingAt()) {
                                        matchedToken = whitespaceMatcher.group();
                                        matchLength = matchedToken.length();
                                        LOGGER.log(Level.FINEST, "Matched whitespace: {0}", matchedToken);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (matchedToken != null && !matchedToken.trim().isEmpty()) {
                tokens.add(matchedToken);
                remaining = remaining.substring(matchLength).trim();
            } else {
                remaining = remaining.substring(1).trim();
            }
        }

        // Обработка токенов
        Pattern inConditionPattern = Pattern.compile("IN_CONDITION_\\d+");
        Pattern groupedConditionPattern = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)");
        StringBuilder currentCondition = new StringBuilder();
        String conjunction = null;
        boolean not = false;
        int inConditionIndex = 0;

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            if (token.isEmpty()) {
                continue;
            }

            // Обработка IN_CONDITION_
            Matcher inConditionMatcher = inConditionPattern.matcher(token);
            if (inConditionMatcher.matches()) {
                LOGGER.log(Level.FINEST, "Matched IN condition: {0}", token);
                inConditionIndex++;
                String inCondition = inConditions.get(inConditionIndex - 1);
                Matcher inCondMatcher = inClausePattern.matcher(inCondition);
                if (inCondMatcher.matches()) {
                    String column = inCondition.split("\\s+")[0].trim();
                    boolean isNot = inCondition.toUpperCase().contains("NOT IN");
                    String valuesStr = inCondition.substring(inCondition.indexOf("(") + 1, inCondition.lastIndexOf(")")).trim();
                    String actualColumn = columnAliases.getOrDefault(column.split("\\.")[column.contains(".") ? 1 : 0], column);
                    String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
                    Class<?> columnType = getColumnType(normalizedColumn, combinedColumnTypes, defaultTableName, tableAliases, columnAliases);

                    if (valuesStr.toUpperCase().startsWith("SELECT ")) {
                        String subQueryStr = valuesStr;
                        if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                            subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
                        }
                        validateSubquery("(" + subQueryStr + ")");
                        Query<?> subQuery = parse(subQueryStr, database);
                        conditions.add(new Condition(actualColumn, new SubQuery(subQuery, null), conjunction, isNot));
                        LOGGER.log(Level.FINE, "Added IN condition with subquery: {0} IN ({subQuery})", new Object[]{actualColumn});
                    } else {
                        String[] valueParts = valuesStr.split(",\\s*");
                        List<Object> inValues = new ArrayList<>();
                        for (String val : valueParts) {
                            Object value = parseConditionValue(actualColumn, val.trim(), columnType);
                            inValues.add(value);
                        }
                        conditions.add(new Condition(actualColumn, inValues, conjunction, isNot));
                        LOGGER.log(Level.FINE, "Added IN condition: {0} IN ({1})", new Object[]{actualColumn, valuesStr});
                    }
                    conjunction = null;
                    not = false;
                    currentCondition = new StringBuilder();
                    continue;
                }
            }

            // Обработка логических операторов
            if (logicalOperatorPattern.matcher(token).matches()) {
                LOGGER.log(Level.FINEST, "Matched logical operator: {0}", token);
                if (!currentCondition.toString().trim().isEmpty()) {
                    String condStr = currentCondition.toString().trim();
                    if (!condStr.matches("^\\(+\\s*\\)+$")) {
                        Condition condition = parseSingleCondition(
                                condStr, defaultTableName, database, originalQuery,
                                isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not, conditionStr, placeholders
                        );
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Added condition before logical operator {0}: {1}", new Object[]{token, condition});
                    }
                    currentCondition = new StringBuilder();
                }
                conjunction = token.toUpperCase();
                not = false;
                continue;
            }

            // Обработка NOT
            if (token.equalsIgnoreCase("NOT")) {
                not = true;
                LOGGER.log(Level.FINEST, "Handled NOT token");
                continue;
            }

            // Обработка групповых условий
            Matcher groupedConditionMatcher = groupedConditionPattern.matcher(token);
            if (groupedConditionMatcher.matches() && !token.toUpperCase().startsWith("(SELECT")) {
                LOGGER.log(Level.FINEST, "Matched grouped condition: {0}", token);
                String groupContent = token.substring(1, token.length() - 1).trim();
                if (!groupContent.isEmpty()) {
                    List<Condition> subConditions = parseConditions(
                            groupContent, defaultTableName, database, originalQuery,
                            isJoinCondition, combinedColumnTypes, tableAliases, columnAliases
                    );
                    conditions.add(new Condition(subConditions, conjunction, not));
                    LOGGER.log(Level.FINE, "Added grouped condition: {0}", subConditions);
                    conjunction = null;
                    not = false;
                    currentCondition = new StringBuilder();
                }
                continue;
            }

            // Обработка ключевых слов (LIMIT, OFFSET и т.д.)
            if (token.matches("(?i)^LIMIT\\b|^OFFSET\\b|^ORDER BY\\b|^GROUP BY\\b")) {
                if (!currentCondition.toString().trim().isEmpty()) {
                    String condStr = currentCondition.toString().trim();
                    if (!condStr.matches("^\\(+\\s*\\)+$")) {
                        Condition condition = parseSingleCondition(
                                condStr, defaultTableName, database, originalQuery,
                                isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not, conditionStr, placeholders
                        );
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Added condition before {0}: {1}", new Object[]{token, condition});
                    }
                }
                break;
            }

            // Добавляем токен в текущее условие
            currentCondition.append(token);
            if (operatorPattern.matcher(token).matches()) {
                currentCondition.append(" ");
            } else if (i < tokens.size() - 1 && !operatorPattern.matcher(tokens.get(i + 1)).matches()) {
                currentCondition.append(" ");
            }
        }

        // Обработка финального условия
        String finalCondStr = currentCondition.toString().trim();
        if (!finalCondStr.isEmpty() && !finalCondStr.matches("^\\(+\\s*\\)+$")) {
            Condition condition = parseSingleCondition(
                    finalCondStr, defaultTableName, database, originalQuery,
                    isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not, conditionStr, placeholders
            );
            conditions.add(condition);
            LOGGER.log(Level.FINE, "Added final condition: {0}", condition);
        }

        if (conditions.isEmpty() && !conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.SEVERE, "Failed to parse any conditions: conditionStr={0}", conditionStr);
            throw new IllegalArgumentException(String.format("Invalid condition: unable to parse any conditions in '%s'", conditionStr));
        }

        LOGGER.log(Level.FINE, "Successfully parsed conditions: {0}", conditions);
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
                                           String conjunction, boolean not, String conditionStr, List<String> placeholders) {
        LOGGER.log(Level.FINEST, "Parsing single condition: '{0}', full condition: '{1}'", new Object[]{condStr, conditionStr});

        String normalizedCondStr = normalizeCondition(condStr).trim();
        if (normalizedCondStr.isEmpty()) {
            throw new IllegalArgumentException("Empty condition: " + condStr);
        }

        // Восстанавливаем строки из плейсхолдеров
        String processedCondStr = normalizedCondStr;
        for (int i = 0; i < placeholders.size(); i++) {
            String placeholder = "STRING_" + i;
            if (processedCondStr.contains(placeholder)) {
                processedCondStr = processedCondStr.replace(placeholder, placeholders.get(i));
                LOGGER.log(Level.FINEST, "Restored placeholder {0} -> {1}", new Object[]{placeholder, placeholders.get(i)});
            }
        }

        // Регулярные выражения
        Pattern groupedConditionPattern = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)");
        Pattern inClausePattern = Pattern.compile(
                "\\b[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*\\s*(?:NOT\\s+)?IN\\s*\\([^)]+\\)");
        Pattern operatorPattern = Pattern.compile("\\s*(?:=|[!=<>]=|[<>]|LIKE|NOT\\s+LIKE)\\s*");
        Pattern trailingParenPattern = Pattern.compile(".*?\\)+$");

        LOGGER.log(Level.FINEST, "Regex patterns used: groupedConditionPattern={0}, inClausePattern={1}, operatorPattern={2}, trailingParenPattern={3}",
                new Object[]{groupedConditionPattern.pattern(), inClausePattern.pattern(), operatorPattern.pattern(), trailingParenPattern.pattern()});

        // Обработка групповых условий
        Matcher groupedConditionMatcher = groupedConditionPattern.matcher(processedCondStr);
        if (groupedConditionMatcher.matches()) {
            LOGGER.log(Level.FINEST, "Matched grouped condition: {0}", processedCondStr);
            String subCondStr = processedCondStr.substring(1, processedCondStr.length() - 1).trim();
            if (!subCondStr.isEmpty()) {
                List<Condition> subConditions = parseConditions(subCondStr, defaultTableName, database, originalQuery,
                        isJoinCondition, combinedColumnTypes, tableAliases, columnAliases);
                return new Condition(subConditions, conjunction, not);
            }
        }

        // Обработка IN
        Matcher inClauseMatcher = inClausePattern.matcher(processedCondStr.toUpperCase());
        if (inClauseMatcher.find()) {
            LOGGER.log(Level.FINEST, "Matched IN clause: {0}", inClauseMatcher.group());
            String[] parts = processedCondStr.split("\\s+IN\\s+", 2);
            if (parts.length == 2 && parts[0].matches("^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")) {
                String column = parts[0].trim();
                boolean inNot = processedCondStr.toUpperCase().contains("NOT IN");
                String valuesStr = parts[1].trim();
                if (!valuesStr.startsWith("(") || !valuesStr.endsWith(")")) {
                    throw new IllegalArgumentException("Invalid IN syntax: " + processedCondStr);
                }
                valuesStr = valuesStr.substring(1, valuesStr.length() - 1);
                String actualColumn = columnAliases.getOrDefault(column.split("\\.")[column.contains(".") ? 1 : 0], column);
                String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
                Class<?> columnType = getColumnType(normalizedColumn, combinedColumnTypes, defaultTableName, tableAliases, columnAliases);

                if (valuesStr.toUpperCase().startsWith("SELECT ")) {
                    String subQueryStr = valuesStr;
                    if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                        subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
                    }
                    validateSubquery("(" + subQueryStr + ")");
                    Query<?> subQuery = parse(subQueryStr, database);
                    LOGGER.log(Level.FINE, "Parsed IN subquery: {0}", subQueryStr);
                    return new Condition(actualColumn, new SubQuery(subQuery, null), conjunction, inNot);
                }

                String[] valueParts = valuesStr.split(",\\s*");
                List<Object> inValues = new ArrayList<>();
                for (String val : valueParts) {
                    Object value = parseConditionValue(actualColumn, val.trim(), columnType);
                    inValues.add(value);
                }
                return new Condition(actualColumn, inValues, conjunction, inNot);
            }
        }

        // Обработка IS NULL / IS NOT NULL
        if (processedCondStr.toUpperCase().endsWith(" IS NULL")) {
            String column = processedCondStr.substring(0, processedCondStr.length() - 8).trim();
            if (column.matches("^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")) {
                String actualColumn = columnAliases.getOrDefault(column.split("\\.")[column.contains(".") ? 1 : 0], column);
                String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
                validateColumn(normalizedColumn, combinedColumnTypes);
                return new Condition(actualColumn, Operator.IS_NULL, conjunction, not);
            }
        }
        if (processedCondStr.toUpperCase().endsWith(" IS NOT NULL")) {
            String column = processedCondStr.substring(0, processedCondStr.length() - 12).trim();
            if (column.matches("^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")) {
                String actualColumn = columnAliases.getOrDefault(column.split("\\.")[column.contains(".") ? 1 : 0], column);
                String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
                validateColumn(normalizedColumn, combinedColumnTypes);
                return new Condition(actualColumn, Operator.IS_NOT_NULL, conjunction, not);
            }
        }

        // Обработка подзапросов
        if (processedCondStr.toUpperCase().contains("(SELECT ")) {
            String[] parts = processedCondStr.split("\\s*(?:=|[!=<>]=|[<>]|LIKE|NOT LIKE)\\s*", 2);
            if (parts.length == 2 && parts[1].trim().startsWith("(SELECT ")) {
                String column = parts[0].trim();
                String operatorStr = processedCondStr.substring(column.length(), processedCondStr.indexOf("(")).trim();
                String subQueryStr = parts[1].trim();
                if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                    subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
                }
                validateSubquery("(" + subQueryStr + ")");
                Query<?> subQuery = parse(subQueryStr, database);
                Operator operator = parseOperator(operatorStr);
                String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
                validateColumn(normalizedColumn, combinedColumnTypes);
                LOGGER.log(Level.FINE, "Parsed subquery condition: column={0}, operator={1}, subQuery={2}",
                        new Object[]{normalizedColumn, operator, subQueryStr});
                return new Condition(normalizedColumn, new SubQuery(subQuery, null), operator, conjunction, not);
            }
        }

        // Обработка операторов
        Matcher operatorMatcher = operatorPattern.matcher(processedCondStr);
        String selectedOperator = null;
        int splitIndex = -1;
        String rightPart = null;
        if (operatorMatcher.find()) {
            selectedOperator = operatorMatcher.group().trim();
            splitIndex = operatorMatcher.start();
            rightPart = processedCondStr.substring(operatorMatcher.end()).trim();
            LOGGER.log(Level.FINEST, "Matched operator: group={0}, start={1}, end={2}",
                    new Object[]{selectedOperator, operatorMatcher.start(), operatorMatcher.end()});
        }

        if (selectedOperator == null) {
            LOGGER.log(Level.SEVERE, "No valid operator found in condition: {0}", processedCondStr);
            throw new IllegalArgumentException("Invalid condition: no operator in '" + processedCondStr + "'");
        }

        String leftPart = processedCondStr.substring(0, splitIndex).trim();
        LOGGER.log(Level.FINEST, "Split condition: leftPart={0}, operator={1}, rightPart={2}",
                new Object[]{leftPart, selectedOperator, rightPart});

        // Очистка rightPart от завершающих скобок
        Matcher trailingParenMatcher = trailingParenPattern.matcher(rightPart);
        if (trailingParenMatcher.matches()) {
            StringBuilder cleanedRightPart = new StringBuilder();
            String remaining = rightPart;
            while (remaining.endsWith(")")) {
                Matcher balancedParenMatcher = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)$").matcher(remaining);
                if (balancedParenMatcher.find()) {
                    String group = balancedParenMatcher.group();
                    cleanedRightPart.insert(0, group.substring(0, group.length() - 1));
                    remaining = remaining.substring(0, remaining.length() - group.length()).trim();
                } else {
                    cleanedRightPart.insert(0, ")");
                    remaining = remaining.substring(0, remaining.length() - 1).trim();
                }
            }
            cleanedRightPart.insert(0, remaining);
            rightPart = cleanedRightPart.toString().trim();
        }

        String column = leftPart;
        String actualColumn = columnAliases.getOrDefault(column.split("\\.")[column.contains(".") ? 1 : 0], column);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);

        String rightColumn = null;
        Object value = null;
        if (rightPart.matches("^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")) {
            rightColumn = rightPart;
        } else {
            try {
                // Удаляем кавычки из строкового значения
                String valueStr = rightPart;
                if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                    valueStr = valueStr.substring(1, valueStr.length() - 1);
                }
                value = parseConditionValue(actualColumn, valueStr, getColumnType(normalizedColumn, combinedColumnTypes, defaultTableName, tableAliases, columnAliases));
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.WARNING, "Failed to parse rightPart as value, retrying as column: rightPart={0}, error={1}",
                        new Object[]{rightPart, e.getMessage()});
                if (rightPart.matches("^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$")) {
                    rightColumn = rightPart;
                } else {
                    throw e;
                }
            }
        }

        Operator operator = parseOperator(selectedOperator);

        if (isJoinCondition && rightColumn != null && !rightColumnIsFromDifferentTable(actualColumn, rightColumn, tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + processedCondStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, defaultTableName, tableAliases);
            validateColumn(normalizedRightColumn, combinedColumnTypes);
            return new Condition(actualColumn, rightColumn, operator, conjunction, not);
        } else {
            return new Condition(actualColumn, value, operator, conjunction, not);
        }
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
        String conjunction = null;
        boolean not = false;

        // Регулярные выражения
        Pattern quotedStringPattern = Pattern.compile("'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        Pattern subQueryPattern = Pattern.compile("\\(SELECT\\s+.*?\\s*\\)", Pattern.DOTALL);
        Pattern logicalOperatorPattern = Pattern.compile("\\bAND\\b|\\bOR\\b", Pattern.CASE_INSENSITIVE);
        Pattern keywordPattern = Pattern.compile("\\bORDER BY\\b|\\bLIMIT\\b|\\bOFFSET\\b", Pattern.CASE_INSENSITIVE);
        Pattern notPattern = Pattern.compile("\\bNOT\\b", Pattern.CASE_INSENSITIVE);
        Pattern balancedParenPattern = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)");

        StringBuilder processedInput = new StringBuilder();
        List<String> placeholders = new ArrayList<>();
        int placeholderIndex = 0;

        // Заменяем строки и подзапросы на плейсхолдеры
        String temp = havingClause;
        Matcher quotedStringMatcher = quotedStringPattern.matcher(temp);
        int lastEnd = 0;
        while (quotedStringMatcher.find()) {
            processedInput.append(temp, lastEnd, quotedStringMatcher.start());
            String placeholder = "STRING_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(quotedStringMatcher.group());
            lastEnd = quotedStringMatcher.end();
        }
        processedInput.append(temp.substring(lastEnd));
        temp = processedInput.toString();
        processedInput.setLength(0);
        lastEnd = 0;

        Matcher subQueryMatcher = subQueryPattern.matcher(temp);
        while (subQueryMatcher.find()) {
            processedInput.append(temp, lastEnd, subQueryMatcher.start());
            String placeholder = "SUBQUERY_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(subQueryMatcher.group());
            lastEnd = subQueryMatcher.end();
        }
        processedInput.append(temp.substring(lastEnd));
        String processedClause = processedInput.toString();

        // Разбиваем на токены
        List<String> tokens = new ArrayList<>();
        Matcher tokenMatcher = Pattern.compile(
                quotedStringPattern.pattern() + "|" +
                        balancedParenPattern.pattern() + "|" +
                        logicalOperatorPattern.pattern() + "|" +
                        keywordPattern.pattern() + "|" +
                        notPattern.pattern() + "|" +
                        "\\S+"
        ).matcher(processedClause);
        while (tokenMatcher.find()) {
            String token = tokenMatcher.group().trim();
            if (!token.isEmpty()) {
                tokens.add(token);
            }
        }

        for (String token : tokens) {
            if (inQuotes) {
                currentCondition.append(token).append(" ");
                if (quotedStringPattern.matcher(token).matches()) {
                    inQuotes = false;
                }
                continue;
            }

            if (quotedStringPattern.matcher(token).matches()) {
                inQuotes = true;
                currentCondition.append(token).append(" ");
                continue;
            }

            if (logicalOperatorPattern.matcher(token).matches()) {
                String condStr = currentCondition.toString().trim();
                if (!condStr.isEmpty()) {
                    HavingCondition condition = parseSingleHavingCondition(condStr, defaultTableName, database, originalQuery,
                            aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                    conditions.add(condition);
                    LOGGER.log(Level.FINE, "Parsed HAVING condition: {0}", condition);
                }
                conjunction = token.toUpperCase();
                not = false;
                currentCondition = new StringBuilder();
                continue;
            }

            if (notPattern.matcher(token).matches()) {
                not = true;
                continue;
            }

            if (balancedParenPattern.matcher(token).matches() && !subQueryPattern.matcher(token).matches()) {
                String condStr = token.substring(1, token.length() - 1).trim();
                if (!condStr.isEmpty()) {
                    List<HavingCondition> subConditions = parseHavingConditions(condStr, defaultTableName, database, originalQuery,
                            aggregates, combinedColumnTypes, tableAliases, columnAliases);
                    conditions.add(new HavingCondition(subConditions, conjunction, not));
                    LOGGER.log(Level.FINE, "Parsed grouped HAVING condition: {0}, conjunction={1}, not={2}",
                            new Object[]{subConditions, conjunction, not});
                }
                conjunction = null;
                not = false;
                currentCondition = new StringBuilder();
                continue;
            }

            if (keywordPattern.matcher(token).matches()) {
                String condStr = currentCondition.toString().trim();
                if (!condStr.isEmpty()) {
                    HavingCondition condition = parseSingleHavingCondition(condStr, defaultTableName, database, originalQuery,
                            aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                    conditions.add(condition);
                    LOGGER.log(Level.FINE, "Parsed HAVING condition before LIMIT/OFFSET/ORDER BY: {0}", condition);
                }
                break;
            }

            currentCondition.append(token).append(" ");
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

        // Регулярное выражение для операторов
        Pattern operatorPattern = Pattern.compile("\\s+(?:=|[!=<>]=|[<>])\\s+");

        String selectedOperator = null;
        int operatorIndex = -1;
        Matcher operatorMatcher = operatorPattern.matcher(" " + condStr + " ");
        if (operatorMatcher.find()) {
            selectedOperator = operatorMatcher.group().trim();
            operatorIndex = operatorMatcher.start();
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

        // Регулярное выражение для строк
        Pattern quotedStringPattern = Pattern.compile("'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        // Регулярное выражение для подзапросов
        Pattern subQueryPattern = Pattern.compile("\\(SELECT\\s+.*?\\s*\\)", Pattern.DOTALL);
        // Регулярное выражение для сбалансированных скобок
        Pattern balancedParenPattern = Pattern.compile("\\([^()']*(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|[^()']*)*\\)");

        String remaining = str.substring(startIndex);
        StringBuilder processedInput = new StringBuilder();
        List<String> placeholders = new ArrayList<>();
        int placeholderIndex = 0;

        // Заменяем строки
        Matcher quotedStringMatcher = quotedStringPattern.matcher(remaining);
        int lastEnd = 0;
        while (quotedStringMatcher.find()) {
            processedInput.append(remaining, lastEnd, quotedStringMatcher.start());
            String placeholder = "STRING_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(quotedStringMatcher.group());
            lastEnd = quotedStringMatcher.end();
        }
        processedInput.append(remaining.substring(lastEnd));
        remaining = processedInput.toString();
        processedInput.setLength(0);
        lastEnd = 0;

        // Заменяем подзапросы
        Matcher subQueryMatcher = subQueryPattern.matcher(remaining);
        while (subQueryMatcher.find()) {
            processedInput.append(remaining, lastEnd, subQueryMatcher.start());
            String placeholder = "SUBQUERY_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(subQueryMatcher.group());
            lastEnd = subQueryMatcher.end();
        }
        processedInput.append(remaining.substring(lastEnd));
        remaining = processedInput.toString();

        // Ищем ближайшую сбалансированную группу скобок
        Matcher balancedParenMatcher = balancedParenPattern.matcher(remaining);
        if (balancedParenMatcher.lookingAt()) {
            String matchedGroup = balancedParenMatcher.group();
            int indexInProcessed = startIndex + matchedGroup.length() - 1;

            // Корректируем индекс, учитывая плейсхолдеры
            String beforeMatch = remaining.substring(0, matchedGroup.length());
            int actualIndex = indexInProcessed;
            for (String placeholder : placeholders) {
                String placeholderRegex = Pattern.quote(placeholder);
                Matcher placeholderMatcher = Pattern.compile(placeholderRegex).matcher(beforeMatch);
                while (placeholderMatcher.find()) {
                    int placeholderLength = placeholder.length();
                    int originalLength = placeholders.get(Integer.parseInt(placeholder.substring(placeholder.indexOf("_") + 1))).length();
                    actualIndex += (originalLength - placeholderLength);
                }
            }

            String subQueryStr = str.substring(startIndex, actualIndex + 1);
            Pattern selectPattern = Pattern.compile("\\s*\\(\\s*SELECT\\s+.*?\\s+FROM\\s+.*?\\s*\\)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            if (!selectPattern.matcher(subQueryStr).matches()) {
                LOGGER.log(Level.WARNING, "Подзапрос может быть некорректным: {0}", subQueryStr);
            }
            LOGGER.log(Level.FINE, "Найдена парная закрывающая скобка на индексе {0} для подзапроса: {1}",
                    new Object[]{actualIndex, subQueryStr});
            return actualIndex;
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
            // Регулярные выражения без групп
            Pattern selectPattern = Pattern.compile("\\s*\\(\\s*SELECT\\s+[^\\s]+\\s+FROM\\s+[^\\s]+.*\\)");
            Pattern wherePattern = Pattern.compile("\\s*WHERE\\s+.+");
            Pattern limitPattern = Pattern.compile("\\s*LIMIT\\s+\\d+");

            Matcher selectMatcher1 = selectPattern.matcher(norm1);
            Matcher selectMatcher2 = selectPattern.matcher(norm2);
            if (!selectMatcher1.matches() || !selectMatcher2.matches()) {
                LOGGER.log(Level.FINE, "Subquery pattern mismatch: {0} vs {1}", new Object[]{norm1, norm2});
                return false;
            }

            String select1 = norm1.replaceAll(".*SELECT\\s+([^\\s]+)\\s+.*", "$1").trim();
            String select2 = norm2.replaceAll(".*SELECT\\s+([^\\s]+)\\s+.*", "$1").trim();
            String from1 = norm1.replaceAll(".*FROM\\s+([^\\s]+).*", "$1").trim();
            String from2 = norm2.replaceAll(".*FROM\\s+([^\\s]+).*", "$1").trim();

            String where1 = "";
            String where2 = "";
            Matcher whereMatcher1 = wherePattern.matcher(norm1);
            Matcher whereMatcher2 = wherePattern.matcher(norm2);
            if (whereMatcher1.find()) {
                where1 = normalizeQueryString(norm1.replaceAll(".*WHERE\\s+(.+?)(?:\\s+LIMIT.*|$)", "$1").trim()).replaceAll("\\s+", " ");
            }
            if (whereMatcher2.find()) {
                where2 = normalizeQueryString(norm2.replaceAll(".*WHERE\\s+(.+?)(?:\\s+LIMIT.*|$)", "$1").trim()).replaceAll("\\s+", " ");
            }

            String limit1 = "";
            String limit2 = "";
            Matcher limitMatcher1 = limitPattern.matcher(norm1);
            Matcher limitMatcher2 = limitPattern.matcher(norm2);
            if (limitMatcher1.find()) {
                limit1 = norm1.replaceAll(".*LIMIT\\s+(\\d+).*", "$1").trim();
            }
            if (limitMatcher2.find()) {
                limit2 = norm2.replaceAll(".*LIMIT\\s+(\\d+).*", "$1").trim();
            }

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

        // Регулярное выражение для строк
        Pattern quotedStringPattern = Pattern.compile("'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        // Регулярное выражение для подзапросов
        Pattern subQueryPattern = Pattern.compile("\\(SELECT\\s+.*?\\s*\\)", Pattern.DOTALL);
        // Регулярное выражение для операторов
        Pattern operatorPattern = Pattern.compile("\\bEQUALS\\b|\\bNOT_EQUALS\\b|\\bGREATER_THAN\\b|\\bLIKE\\b|\\bNOT_LIKE\\b", Pattern.CASE_INSENSITIVE);
        // Регулярное выражение для лишних пробелов
        Pattern whitespacePattern = Pattern.compile("\\s+");

        StringBuilder processedInput = new StringBuilder();
        List<String> placeholders = new ArrayList<>();
        int placeholderIndex = 0;

        // Заменяем строки
        String temp = condition;
        Matcher quotedStringMatcher = quotedStringPattern.matcher(temp);
        int lastEnd = 0;
        while (quotedStringMatcher.find()) {
            processedInput.append(temp, lastEnd, quotedStringMatcher.start());
            String placeholder = "STRING_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(quotedStringMatcher.group());
            lastEnd = quotedStringMatcher.end();
        }
        processedInput.append(temp.substring(lastEnd));
        temp = processedInput.toString();
        processedInput.setLength(0);
        lastEnd = 0;

        // Заменяем подзапросы
        Matcher subQueryMatcher = subQueryPattern.matcher(temp);
        while (subQueryMatcher.find()) {
            processedInput.append(temp, lastEnd, subQueryMatcher.start());
            String placeholder = "SUBQUERY_" + placeholderIndex++;
            processedInput.append(placeholder);
            placeholders.add(subQueryMatcher.group());
            lastEnd = subQueryMatcher.end();
        }
        processedInput.append(temp.substring(lastEnd));
        String normalized = processedInput.toString();

        // Нормализация операторов
        normalized = normalized.replaceAll("(?i)\\bEQUALS\\b", "=")
                .replaceAll("(?i)\\bNOT_EQUALS\\b", "!=")
                .replaceAll("(?i)\\bGREATER_THAN\\b", ">")
                .replaceAll("(?i)\\bLIKE\\b", "LIKE")
                .replaceAll("(?i)\\bNOT_LIKE\\b", "NOT LIKE")
                .replaceAll("\\d+\\s+\\d+", "$0"); // Очистка пробелов в числах

        // Удаление лишних пробелов
        normalized = whitespacePattern.matcher(normalized).replaceAll(" ").trim();

        // Восстанавливаем строки и подзапросы
        for (int i = 0; i < placeholders.size(); i++) {
            String placeholder = (i < placeholderIndex / 2 ? "STRING_" : "SUBQUERY_") + i;
            normalized = normalized.replace(placeholder, placeholders.get(i));
        }

        return normalized;
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