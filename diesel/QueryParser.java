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
    private int findLastClauseIndexOutsideSubquery(String query, String clause) {
        if (query == null || clause == null || query.isEmpty() || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Invalid input: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        String clausePattern = String.format("\\b%s\\b", Pattern.quote(clause.toUpperCase()));
        String regex = String.format(
                "(?i)" +
                        "(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*')" + // Group 1: Quoted strings
                        "|\\((?:[^()']+|'(?:\\\\.[^']*')|\\([^()]*\\))*\\)" + // Group 2: Parenthesized groups
                        "|(%s)" + // Group 3: Clause
                        "|.", // Match any other character
                clausePattern
        );

        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(query.toUpperCase());

        int parenDepth = 0;
        int lastClauseIndex = -1;

        while (matcher.find()) {
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            String group3 = matcher.group(3);

            int start = matcher.start();

            if (group1 != null) {
                continue;
            } else if (group2 != null) {
                if (group2.toUpperCase().contains("SELECT")) {
                    parenDepth++;
                }
                if (parenDepth > 0) {
                    parenDepth--;
                }
            } else if (group3 != null && parenDepth == 0) {
                lastClauseIndex = start; // Update to the latest valid clause
            }
        }

        if (lastClauseIndex != -1) {
            LOGGER.log(Level.FINEST, "Found last {0} clause at index {1} in query: {2}", new Object[]{clause, lastClauseIndex, query});
            return lastClauseIndex;
        }

        LOGGER.log(Level.FINEST, "No valid {0} clause found outside subqueries in query: {1}", new Object[]{clause, query});
        return -1;
    }

    private int findLastClauseIndex(String query, String clause) {
        if (query == null || clause == null || query.isEmpty() || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Invalid input: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        String clausePattern = String.format("\\b%s\\b", Pattern.quote(clause.toUpperCase()));
        String regex = String.format(
                "(?i)" +
                        "(?:'[^'\\\\]*(?:\\\\.[^'\\\\]*)*')" + // Group 1: Quoted strings
                        "|(%s)" + // Group 2: Clause
                        "|.", // Match any other character
                clausePattern
        );

        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(query.toUpperCase());

        int lastClauseIndex = -1;

        while (matcher.find()) {
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);

            int start = matcher.start();

            if (group1 != null) {
                continue;
            } else if (group2 != null) {
                lastClauseIndex = start;
            }
        }

        if (lastClauseIndex != -1) {
            LOGGER.log(Level.FINEST, "Found last {0} clause at index {1} in query: {2}", new Object[]{clause, lastClauseIndex, query});
            return lastClauseIndex;
        }

        LOGGER.log(Level.FINEST, "No {0} clause found in query: {1}", new Object[]{clause, query});
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
                onConditions = parseConditions(onCondition, tableName, database, original, true, combinedColumnTypes, tableAliases, columnAliases);

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
                remaining = offsetSplit[0].trim();
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
        if (query == null || clause == null || query.isEmpty() || clause.isEmpty()) {
            LOGGER.log(Level.FINEST, "Недопустимый ввод: query={0}, clause={1}", new Object[]{query, clause});
            return -1;
        }

        // Регулярные выражения для различных токенов
        Pattern quotedStringPattern = Pattern.compile("'([^'\\\\]*(?:\\\\.[^'\\\\]*)*)'");
        Pattern subQueryStartPattern = Pattern.compile("\\(\\s*SELECT\\b", Pattern.CASE_INSENSITIVE);
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern clausePattern = Pattern.compile("\\b" + Pattern.quote(clause.toUpperCase()) + "\\b");

        int currentPos = 0;
        int lastClauseIndex = -1;
        Deque<String> contextStack = new ArrayDeque<>(); // Стек для отслеживания контекста (скобки, подзапросы)
        boolean inQuotes = false;

        while (currentPos < query.length()) {
            String remaining = query.substring(currentPos);

            // Проверяем строки в кавычках
            Matcher quotedStringMatcher = quotedStringPattern.matcher(remaining);
            if (!inQuotes && quotedStringMatcher.lookingAt()) {
                currentPos += quotedStringMatcher.end();
                continue;
            }

            // Проверяем начало подзапроса
            Matcher subQueryMatcher = subQueryStartPattern.matcher(remaining);
            if (!inQuotes && subQueryMatcher.lookingAt()) {
                contextStack.push("SUBQUERY");
                currentPos += subQueryMatcher.end();
                continue;
            }

            // Проверяем открывающую скобку
            Matcher openParenMatcher = openParenPattern.matcher(remaining);
            if (!inQuotes && openParenMatcher.lookingAt()) {
                contextStack.push("PAREN");
                currentPos += openParenMatcher.end();
                continue;
            }

            // Проверяем закрывающую скобку
            Matcher closeParenMatcher = closeParenPattern.matcher(remaining);
            if (!inQuotes && closeParenMatcher.lookingAt()) {
                if (!contextStack.isEmpty()) {
                    contextStack.pop();
                } else {
                    LOGGER.log(Level.WARNING, "Несбалансированная закрывающая скобка на позиции {0} в запросе: {1}",
                            new Object[]{currentPos, query});
                }
                currentPos += closeParenMatcher.end();
                continue;
            }

            // Проверяем целевой клаузер
            Matcher clauseMatcher = clausePattern.matcher(remaining);
            if (!inQuotes && clauseMatcher.lookingAt() && contextStack.isEmpty()) {
                lastClauseIndex = currentPos;
                LOGGER.log(Level.FINEST, "Найден клаузер {0} на позиции {1}", new Object[]{clause, lastClauseIndex});
                currentPos += clauseMatcher.end();
                continue;
            }

            // Обработка символа кавычек
            if (remaining.charAt(0) == '\'') {
                inQuotes = !inQuotes;
            }

            currentPos++;
        }

        if (!contextStack.isEmpty()) {
            LOGGER.log(Level.WARNING, "Несбалансированные скобки или подзапросы в запросе: {0}, стек: {1}",
                    new Object[]{query, contextStack});
        }

        if (lastClauseIndex != -1) {
            LOGGER.log(Level.FINEST, "Найден последний {0} клаузер на индексе {1} в запросе: {2}",
                    new Object[]{clause, lastClauseIndex, query});
            return lastClauseIndex;
        }

        LOGGER.log(Level.FINEST, "Клаузер {0} не найден вне подзапросов в запросе: {1}", new Object[]{clause, query});
        return -1;
    }
    private int findLastSelectBefore(String query, int endIndex) {
        String upperQuery = query.toUpperCase();
        int parenDepth = 0;
        boolean inQuotes = false;
        int lastSelectIndex = -1;

        for (int i = 0; i < endIndex; i++) {
            char c = query.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (parenDepth == 0 && upperQuery.startsWith("SELECT ", i)) {
                    lastSelectIndex = i;
                }
            }
        }
        return lastSelectIndex;
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

        List<Condition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        String conjunction = null;
        boolean inQuotes = false;

        // Определяем простые регулярные выражения
        Pattern quotedStringPattern = Pattern.compile("'([^'\\\\]*(?:\\\\.[^'\\\\]*)*)'");
        Pattern subQueryPattern = Pattern.compile("\\([^()]*\\)");
        Pattern logicalOperatorPattern = Pattern.compile("\\b(AND|OR)\\b", Pattern.CASE_INSENSITIVE);
        Pattern keywordPattern = Pattern.compile("\\b(LIMIT|OFFSET|ORDER BY|GROUP BY)\\b", Pattern.CASE_INSENSITIVE);
        Pattern tokenPattern = Pattern.compile("[^\\s]+");

        // Токенизация строки условий
        int currentPos = 0;
        while (currentPos < conditionStr.length()) {
            // Пропускаем пробелы
            while (currentPos < conditionStr.length() && Character.isWhitespace(conditionStr.charAt(currentPos))) {
                currentCondition.append(conditionStr.charAt(currentPos));
                currentPos++;
            }
            if (currentPos >= conditionStr.length()) {
                break;
            }

            String remaining = conditionStr.substring(currentPos);
            Matcher quotedStringMatcher = quotedStringPattern.matcher(remaining);
            Matcher subQueryMatcher = subQueryPattern.matcher(remaining);
            Matcher logicalOperatorMatcher = logicalOperatorPattern.matcher(remaining);
            Matcher keywordMatcher = keywordPattern.matcher(remaining);
            Matcher tokenMatcher = tokenPattern.matcher(remaining);

            String token = null;
            int tokenLength = 0;
            boolean isLogicalOperator = false;
            boolean isKeyword = false;

            // Проверяем, какой токен найден
            if (quotedStringMatcher.lookingAt()) {
                token = quotedStringMatcher.group();
                tokenLength = token.length();
                inQuotes = !inQuotes && token.startsWith("'");
            } else if (subQueryMatcher.lookingAt()) {
                token = subQueryMatcher.group();
                tokenLength = token.length();
            } else if (logicalOperatorMatcher.lookingAt() && !inQuotes) {
                token = logicalOperatorMatcher.group();
                tokenLength = token.length();
                isLogicalOperator = true;
            } else if (keywordMatcher.lookingAt() && !inQuotes) {
                token = keywordMatcher.group();
                tokenLength = token.length();
                isKeyword = true;
            } else if (tokenMatcher.lookingAt()) {
                token = tokenMatcher.group();
                tokenLength = token.length();
            }

            if (token == null) {
                // Неизвестный символ, пропускаем
                currentCondition.append(conditionStr.charAt(currentPos));
                currentPos++;
                continue;
            }

            LOGGER.log(Level.FINEST, "Токен: pos={0}, value={1}, isLogicalOperator={2}, isKeyword={3}",
                    new Object[]{currentPos, token, isLogicalOperator, isKeyword});

            if (isKeyword) {
                // Останавливаем парсинг условий при встрече ключевых слов
                String condStr = currentCondition.toString().trim();
                if (!condStr.isEmpty()) {
                    try {
                        Condition condition = parseSingleCondition(condStr, defaultTableName, database, originalQuery,
                                isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, false, conditionStr);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Добавлено условие перед {0}: {1}", new Object[]{token, condition});
                    } catch (IllegalArgumentException e) {
                        LOGGER.log(Level.SEVERE, "Ошибка парсинга условия перед {0}: {1}, condStr={2}",
                                new Object[]{token, e.getMessage(), condStr});
                        throw e;
                    }
                }
                LOGGER.log(Level.FINE, "Прекращение парсинга условий на {0}", token);
                break;
            } else if (isLogicalOperator) {
                // Обрабатываем логический оператор
                String condStr = currentCondition.toString().trim();
                if (!condStr.isEmpty()) {
                    try {
                        Condition condition = parseSingleCondition(condStr, defaultTableName, database, originalQuery,
                                isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, false, conditionStr);
                        conditions.add(condition);
                        LOGGER.log(Level.FINE, "Добавлено условие перед {0}: {1}", new Object[]{token, condition});
                    } catch (IllegalArgumentException e) {
                        LOGGER.log(Level.SEVERE, "Ошибка парсинга условия перед {0}: {1}, condStr={2}",
                                new Object[]{token, e.getMessage(), condStr});
                        throw e;
                    }
                } else {
                    LOGGER.log(Level.WARNING, "Пустое условие перед логическим оператором {0} на позиции {1}",
                            new Object[]{token, currentPos});
                }
                conjunction = token.toUpperCase();
                currentCondition = new StringBuilder();
            } else {
                // Добавляем токен в текущее условие
                currentCondition.append(token);
            }

            currentPos += tokenLength;
        }

        // Обрабатываем оставшееся условие
        String finalCondStr = currentCondition.toString().trim();
        if (!finalCondStr.isEmpty()) {
            LOGGER.log(Level.FINE, "Парсинг финального условия: condStr={0}", finalCondStr);
            try {
                Condition condition = parseSingleCondition(finalCondStr, defaultTableName, database, originalQuery,
                        isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, false, conditionStr);
                conditions.add(condition);
                LOGGER.log(Level.FINE, "Добавлено финальное условие: {0}", condition);
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.SEVERE, "Ошибка парсинга финального условия: {0}, condStr={1}",
                        new Object[]{e.getMessage(), finalCondStr});
                throw e;
            }
        }

        if (conditions.isEmpty() && !conditionStr.trim().isEmpty()) {
            LOGGER.log(Level.SEVERE, "Не удалось разобрать ни одного условия: conditionStr={0}", conditionStr);
            throw new IllegalArgumentException("Invalid condition: unable to parse any conditions in '" + conditionStr + "'");
        }

        LOGGER.log(Level.FINE, "Условия успешно разобраны: {0}", conditions);
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
        if (normalizedCondStr.toUpperCase().startsWith("(") && normalizedCondStr.toUpperCase().endsWith(")")) {
            String subCondStr = normalizedCondStr.substring(1, normalizedCondStr.length() - 1).trim();
            List<Condition> subConditions = parseConditions(subCondStr, defaultTableName, database, originalQuery,
                    isJoinCondition, combinedColumnTypes, tableAliases, columnAliases);
            return new Condition(subConditions, conjunction, not);
        }

        // Handle IN conditions
        Pattern inPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((.*?)\\)$");
        Matcher inMatcher = inPattern.matcher(normalizedCondStr);
        if (inMatcher.matches()) {
            String column = inMatcher.group(1).trim();
            boolean inNot = inMatcher.group(2) != null;
            String valuesStr = inMatcher.group(3).trim();
            String actualColumn = columnAliases.entrySet().stream()
                    .filter(entry -> entry.getValue().equalsIgnoreCase(column.split("\\.")[column.contains(".") ? 1 : 0]))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(column);
            String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;

            Class<?> columnType = null;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                    columnType = entry.getValue();
                    break;
                }
            }
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column in IN condition: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }

            if (valuesStr.trim().toUpperCase().startsWith("SELECT ")) {
                String subQueryStr = valuesStr.trim();
                if (subQueryStr.toUpperCase().startsWith("(SELECT") && subQueryStr.endsWith(")")) {
                    int subQueryEnd = findMatchingParenthesis(subQueryStr, 0);
                    if (subQueryEnd != subQueryStr.length() - 1) {
                        throw new IllegalArgumentException("Invalid subquery syntax in IN condition: " + subQueryStr);
                    }
                    subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
                }
                Query<?> subQuery = parse(subQueryStr, database);
                LOGGER.log(Level.FINE, "Parsed IN subquery: {0}", subQueryStr);
                return new Condition(actualColumn, new SubQuery(subQuery, null), conjunction, inNot);
            }

            String[] valueParts = valuesStr.split(",(?=([^']*'[^']*')*[^']*$)");
            List<Object> inValues = new ArrayList<>();
            for (String val : valueParts) {
                val = val.trim();
                Object value = parseConditionValue(actualColumn, val, columnType);
                inValues.add(value);
            }
            return new Condition(actualColumn, inValues, conjunction, inNot);
        }

        // Handle IS NULL / IS NOT NULL conditions
        Pattern isNullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(normalizedCondStr);
        if (isNullMatcher.matches()) {
            String column = isNullMatcher.group(1).trim();
            boolean isNotNull = isNullMatcher.group(2) != null;
            String actualColumn = columnAliases.entrySet().stream()
                    .filter(entry -> entry.getValue().equalsIgnoreCase(column.split("\\.")[column.contains(".") ? 1 : 0]))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(column);
            String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
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
                LOGGER.log(Level.SEVERE, "Unknown column in IS NULL condition: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            return new Condition(actualColumn, isNotNull ? Operator.IS_NOT_NULL : Operator.IS_NULL, conjunction, not);
        }

        // Handle subquery conditions
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|!=|<>|>=|<=|<|>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+.*?)\\)$",
                Pattern.DOTALL
        );
        Matcher subQueryMatcher = subQueryPattern.matcher(normalizedCondStr);
        if (subQueryMatcher.matches()) {
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

        // Split on logical operators first to handle multiple conditions
        Pattern logicalOperatorPattern = Pattern.compile("(?i)\\s+(AND|OR)\\s+(?=([^']*'[^']*')*[^']*$)");
        Matcher logicalMatcher = logicalOperatorPattern.matcher(normalizedCondStr);
        if (logicalMatcher.find()) {
            String firstCondition = normalizedCondStr.substring(0, logicalMatcher.start()).trim();
            String remainingCondition = normalizedCondStr.substring(logicalMatcher.end()).trim();
            String newConjunction = logicalMatcher.group(1).toUpperCase();

            // Parse the first condition
            Condition firstCond = parseSingleCondition(firstCondition, defaultTableName, database, originalQuery,
                    isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not, conditionStr);

            // Parse the remaining conditions recursively
            List<Condition> remainingConditions = parseConditions(remainingCondition, defaultTableName, database, originalQuery,
                    isJoinCondition, combinedColumnTypes, tableAliases, columnAliases);

            List<Condition> subConditions = new ArrayList<>();
            subConditions.add(firstCond);
            subConditions.addAll(remainingConditions);
            return new Condition(subConditions, newConjunction, not);
        }

        // Handle column comparisons and literal values
        String[] operators = {"!=", "<>", ">=", "<=", "=", "<", ">", "\\bLIKE\\b", "\\bNOT LIKE\\b"};
        String selectedOperator = null;
        int operatorIndex = -1;
        int operatorEndIndex = -1;
        int parenDepth = 0;
        boolean inQuotes = false;
        int subQueryStart = -1;

        for (int i = 0; i < normalizedCondStr.length(); i++) {
            char c = normalizedCondStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    if (parenDepth == 1 && i + 7 < normalizedCondStr.length() &&
                            normalizedCondStr.substring(i, i + 7).toUpperCase().startsWith("(SELECT")) {
                        subQueryStart = i;
                    }
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0 && subQueryStart != -1) {
                        subQueryStart = -1;
                    }
                    continue;
                } else if (parenDepth == 0 && subQueryStart == -1 && i < normalizedCondStr.length() - 1) {
                    for (String op : operators) {
                        String patternStr = op.startsWith("\\b") ? "\\b" + op.substring(2, op.length() - 2) + "\\b" : Pattern.quote(op);
                        Pattern opPattern = Pattern.compile("(?i)" + patternStr + "(?=\\s|$|[^\\s])");
                        Matcher opMatcher = opPattern.matcher(normalizedCondStr.substring(i));
                        if (opMatcher.lookingAt()) {
                            String remaining = normalizedCondStr.substring(i + opMatcher.group().length()).trim();
                            if (!remaining.isEmpty() && !remaining.toUpperCase().startsWith("(SELECT")) {
                                selectedOperator = opMatcher.group().trim();
                                operatorIndex = i;
                                operatorEndIndex = i + selectedOperator.length();
                                break;
                            }
                        }
                    }
                    if (selectedOperator != null && subQueryStart == -1) {
                        break;
                    }
                }
            }
        }

        if (operatorIndex == -1) {
            LOGGER.log(Level.SEVERE, "No valid operator found in condition: {0}", normalizedCondStr);
            throw new IllegalArgumentException("Invalid condition: no valid operator found in '" + normalizedCondStr + "'");
        }

        String leftPart = normalizedCondStr.substring(0, operatorIndex).trim();
        String rightPart = normalizedCondStr.substring(operatorEndIndex).trim();
        LOGGER.log(Level.FINEST, "Split condition: leftPart={0}, operator={1}, rightPart={2}",
                new Object[]{leftPart, selectedOperator, rightPart});

        String column;
        String rightColumn = null;
        Object value = null;

        // Check if rightPart is a column
        Pattern columnPattern = Pattern.compile("(?i)^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
        Matcher columnMatcher = columnPattern.matcher(rightPart);
        if (columnMatcher.matches()) {
            rightColumn = rightPart;
        } else {
            // Try parsing as a value
            column = columnAliases.entrySet().stream()
                    .filter(entry -> entry.getValue().equalsIgnoreCase(leftPart.split("\\.")[leftPart.contains(".") ? 1 : 0]))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(leftPart);
            try {
                value = parseConditionValue(column, rightPart, getColumnType(column, combinedColumnTypes, defaultTableName, tableAliases, columnAliases));
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.WARNING, "Failed to parse rightPart as value, rechecking as column: rightPart={0}, error={1}",
                        new Object[]{rightPart, e.getMessage()});
                // Recheck rightPart for column pattern after stripping potential trailing clauses
                String[] rightParts = rightPart.split("\\s+", 2);
                if (rightParts.length > 0 && columnPattern.matcher(rightParts[0]).matches()) {
                    rightColumn = rightParts[0];
                } else {
                    throw e; // Rethrow if still not a column
                }
            }
        }

        column = leftPart;
        String finalColumn = column;
        String actualColumn = columnAliases.entrySet().stream()
                .filter(entry -> entry.getValue().equalsIgnoreCase(finalColumn.split("\\.")[finalColumn.contains(".") ? 1 : 0]))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(column);
        String normalizedColumn = normalizeColumnName(actualColumn, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);

        Operator operator = parseOperator(selectedOperator);

        if (isJoinCondition && !rightColumnIsFromDifferentTable(actualColumn, rightColumn, tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + normalizedCondStr);
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