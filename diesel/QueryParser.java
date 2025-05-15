package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

class QueryParser {
    private static final Logger LOGGER = Logger.getLogger(QueryParser.class.getName());
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS,
        IN, LIKE, NOT_LIKE, IS_NULL, IS_NOT_NULL
    }

    enum JoinType {
        INNER, LEFT_INNER, RIGHT_INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }

    static class Condition {
        String column;
        Object value;
        String rightColumn;
        List<Object> inValues;
        Operator operator;
        String conjunction;
        boolean not;
        List<Condition> subConditions;

        Condition(String column, Object value, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = value;
            this.rightColumn = null;
            this.inValues = null;
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
            this.operator = operator;
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

        @Override
        public String toString() {
            if (isGrouped()) {
                String subCondStr = subConditions.stream()
                        .map(Condition::toString)
                        .collect(Collectors.joining(" "));
                return (not ? "NOT " : "") + "(" + subCondStr + ")" + (conjunction != null ? " " + conjunction : "");
            }
            if (isInOperator()) {
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
        String alias; // Added for alias support
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
        String alias;

        AggregateFunction(String functionName, String column, String alias) {
            this.functionName = functionName.toUpperCase();
            this.column = column;
            this.alias = alias;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            sb.append(column == null ? "*" : column);
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
            String normalized = query.trim().toUpperCase();
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
        List<String> columnDefs = new ArrayList<>();
        StringBuilder currentDef = new StringBuilder();
        int parenDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < columnsPart.length(); i++) {
            char c = columnsPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentDef.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (c == ',' && parenDepth == 0) {
                    String def = currentDef.toString().trim();
                    if (!def.isEmpty()) {
                        columnDefs.add(def);
                    }
                    currentDef = new StringBuilder();
                    continue;
                }
            }
            currentDef.append(c);
        }

        String lastDef = currentDef.toString().trim();
        if (!lastDef.isEmpty()) {
            columnDefs.add(lastDef);
        }

        return columnDefs;
    }

    private Query<List<Map<String, Object>>> parseSelectQuery(String normalized, String original, Database database) {
        // Разделяем запрос на части до и после ключевого слова FROM
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Неверный формат SELECT-запроса");
        }

        // Извлекаем часть SELECT из исходного запроса
        String selectPartOriginal = original.substring(original.indexOf("SELECT") + 6, original.indexOf("FROM")).trim();
        // Разбиваем элементы SELECT на отдельные элементы
        List<String> selectItems = splitSelectItems(selectPartOriginal);

        // Списки для хранения столбцов и агрегатных функций
        List<String> columns = new ArrayList<>();
        List<AggregateFunction> aggregates = new ArrayList<>();
        // Карта для хранения псевдонимов столбцов
        Map<String, String> columnAliases = new HashMap<>(); // столбец -> псевдоним

        // Регулярные выражения для распознавания агрегатных функций и столбцов
        Pattern countPattern = Pattern.compile("(?i)^COUNT\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern minPattern = Pattern.compile("(?i)^MIN\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern maxPattern = Pattern.compile("(?i)^MAX\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern avgPattern = Pattern.compile("(?i)^AVG\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern sumPattern = Pattern.compile("(?i)^SUM\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");

        // Обработка каждого элемента SELECT
        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher countMatcher = countPattern.matcher(trimmedItem);
            Matcher minMatcher = minPattern.matcher(trimmedItem);
            Matcher maxMatcher = maxPattern.matcher(trimmedItem);
            Matcher avgMatcher = avgPattern.matcher(trimmedItem);
            Matcher sumMatcher = sumPattern.matcher(trimmedItem);
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);

            if (countMatcher.matches()) {
                // Обработка COUNT
                String countArg = countMatcher.group(1);
                String alias = countMatcher.group(2);
                String column = countArg.equals("*") ? null : countArg;
                aggregates.add(new AggregateFunction("COUNT", column, alias));
                LOGGER.log(Level.FINE, "Разобрана агрегатная функция: COUNT({0}){1}",
                        new Object[]{column == null ? "*" : column, alias != null ? " AS " + alias : ""});
            } else if (minMatcher.matches()) {
                // Обработка MIN
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                aggregates.add(new AggregateFunction("MIN", column, alias));
                LOGGER.log(Level.FINE, "Разобрана агрегатная функция: MIN({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (maxMatcher.matches()) {
                // Обработка MAX
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                aggregates.add(new AggregateFunction("MAX", column, alias));
                LOGGER.log(Level.FINE, "Разобрана агрегатная функция: MAX({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (avgMatcher.matches()) {
                // Обработка AVG
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                aggregates.add(new AggregateFunction("AVG", column, alias));
                LOGGER.log(Level.FINE, "Разобрана агрегатная функция: AVG({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (sumMatcher.matches()) {
                // Обработка SUM
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                aggregates.add(new AggregateFunction("SUM", column, alias));
                LOGGER.log(Level.FINE, "Разобрана агрегатная функция: SUM({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (columnMatcher.matches()) {
                // Обработка обычного столбца
                String column = columnMatcher.group(1);
                String alias = columnMatcher.group(2);
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                    LOGGER.log(Level.FINE, "Разобран столбец с псевдонимом: {0} AS {1}", new Object[]{column, alias});
                } else {
                    LOGGER.log(Level.FINE, "Разобран столбец: {0}", new Object[]{column});
                }
            } else {
                throw new IllegalArgumentException("Неверный элемент SELECT: " + trimmedItem);
            }
        }

        // Обработка таблицы и соединений
        String tableAndJoins = parts[1].trim();
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

        // Разделение на части JOIN
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

        // Обработка основной таблицы
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
            LOGGER.log(Level.FINE, "Разобран псевдоним основной таблицы: {0} -> {1}", new Object[]{tableAlias, tableName});
        }

        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Таблица не найдена: " + tableName);
        }

        // Инициализация типов столбцов
        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
        tableAliases.put(tableName, tableName);

        // Обработка JOIN
        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();

            // Определение типа соединения
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
                    throw new IllegalArgumentException("Неподдерживаемый тип соединения: " + joinTypeStr);
            }

            String joinTableName;
            String joinTableAlias = null;
            List<Condition> onConditions = new ArrayList<>();

            // Разделение части JOIN
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
                LOGGER.log(Level.FINE, "Разобран псевдоним таблицы соединения: {0} -> {1}", new Object[]{joinTableAlias, joinTableName});
            }

            if (joinType == JoinType.CROSS) {
                // Обработка CROSS JOIN
                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Таблица соединения не найдена: " + joinTableName);
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
                        orderBy = parseOrderByClause(remaining.substring(9).trim(), tableName, combinedColumnTypes, columnAliases, tableAliases);
                    }
                    if (remaining.toUpperCase().contains(" ON ")) {
                        throw new IllegalArgumentException("CROSS JOIN не поддерживает ON: " + joinPart);
                    }
                }
                LOGGER.log(Level.FINE, "Разобран CROSS JOIN: таблица={0}, псевдоним={1}", new Object[]{joinTableName, joinTableAlias});
            } else {
                // Обработка других типов JOIN
                if (joinTableTokens.length != 2) {
                    throw new IllegalArgumentException("Неверный формат " + joinTypeStr + ": отсутствует ON");
                }
                String onClause = joinTableTokens[1].trim();
                String[] onSplit = onClause.split("(?i)ON\\s+", 2);
                if (onSplit.length != 2) {
                    throw new IllegalArgumentException("Неверный формат " + joinTypeStr + ": неверный ON");
                }
                String onCondition = onSplit[1].trim();

                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Таблица соединения не найдена: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                tableAliases.put(joinTableName, joinTableName);

                // Обработка условий ON и других частей запроса
                if (onCondition.toUpperCase().contains(" WHERE ")) {
                    String[] onWhereSplit = onCondition.split("(?i)\\s+WHERE\\s+", 2);
                    onCondition = onWhereSplit[0].trim();
                    if (onWhereSplit.length > 1) {
                        String remaining = onWhereSplit[1].trim();
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
                                orderBy = parseOrderByClause(whereOrderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases);
                            }
                        } else {
                            conditionStr = remaining;
                        }
                    }
                } else if (onCondition.toUpperCase().startsWith("LIMIT ")) {
                    String[] onLimitSplit = onCondition.split("(?i)\\s+OFFSET\\s+", 2);
                    onCondition = "";
                    if (onLimitSplit.length > 1) {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].substring(6).trim());
                        offset = parseOffsetClause("OFFSET " + onLimitSplit[1].trim());
                    } else {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].trim());
                    }
                } else if (onCondition.toUpperCase().startsWith("OFFSET ")) {
                    onCondition = "";
                    offset = parseOffsetClause(onCondition);
                } else if (onCondition.toUpperCase().startsWith("ORDER BY ")) {
                    String[] onOrderBySplit = onCondition.split("(?i)\\s+ORDER BY\\s+", 2);
                    onCondition = "";
                    if (onOrderBySplit.length > 1) {
                        orderBy = parseOrderByClause(onOrderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases);
                    }
                }

                onConditions = parseConditions(onCondition, tableName, database, original, true, combinedColumnTypes, tableAliases, columnAliases);

                // Проверка условий соединения
                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }

                LOGGER.log(Level.FINE, "Разобраны условия ON для {0}: {1}", new Object[]{joinTypeStr, onConditions});
            }

            joins.add(new JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            tableName = joinTableName;
        }

        // Обработка оставшейся части запроса
        String remaining = joinParts.get(joinParts.size() - 1);
        if (remaining.toUpperCase().contains(" WHERE ")) {
            String[] whereSplit = remaining.split("(?i)\\s+WHERE\\s+", 2);
            conditionStr = whereSplit[1].trim();
            remaining = whereSplit[0].trim();
        }

        if (conditionStr != null && !conditionStr.isEmpty()) {
            // Обработка LIMIT, OFFSET, ORDER BY, GROUP BY, HAVING
            String[] limitSplit = conditionStr.toUpperCase().contains(" LIMIT ")
                    ? conditionStr.split("(?i)\\s+LIMIT\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = limitSplit[0].trim();
            if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
                String limitClause = limitSplit[1].trim();
                String[] limitOffsetSplit = limitClause.toUpperCase().contains(" OFFSET ")
                        ? limitClause.split("(?i)\\s+OFFSET\\s+", 2)
                        : new String[]{limitClause, ""};
                limit = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
                if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                    offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                }
            }

            String[] offsetSplit = conditionStr.toUpperCase().contains(" OFFSET ")
                    ? conditionStr.split("(?i)\\s+OFFSET\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = offsetSplit[0].trim();
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
            }

            String[] orderBySplit = conditionStr.toUpperCase().contains(" ORDER BY ")
                    ? conditionStr.split("(?i)\\s+ORDER BY\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = parseOrderByClause(orderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases);
            }

            String[] groupBySplit = conditionStr.toUpperCase().contains(" GROUP BY ")
                    ? conditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = groupBySplit[0].trim();
            if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
                String groupByPart = groupBySplit[1].trim();
                String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                        ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                        : new String[]{groupByPart, ""};
                groupBy = Arrays.stream(havingSplit[0].split(","))
                        .map(col -> normalizeColumnName(col.trim(), mainTable.getName(), tableAliases))
                        .collect(Collectors.toList());
                LOGGER.log(Level.FINE, "Разобран GROUP BY: {0}", groupBy);
                if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                    String havingClause = havingSplit[1].trim();
                    havingConditions = parseHavingConditions(havingClause, mainTable.getName(), database, original, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                    LOGGER.log(Level.FINE, "Разобран HAVING: {0}", havingConditions);
                }
            }
        }

        // Дополнительная обработка ORDER BY, LIMIT, OFFSET
        if (remaining.toUpperCase().contains(" ORDER BY ")) {
            String[] orderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
            remaining = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = parseOrderByClause(orderBySplit[1].trim(), mainTable.getName(), combinedColumnTypes, columnAliases, tableAliases);
            }
        }

        if (remaining.toUpperCase().contains(" LIMIT ")) {
            String[] limitSplit = remaining.split("(?i)\\s+LIMIT\\s+", 2);
            remaining = limitSplit[0].trim();
            if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
                String limitClause = limitSplit[1].trim();
                String[] limitOffsetSplit = limitClause.toUpperCase().contains(" OFFSET ")
                        ? limitClause.split("(?i)\\s+OFFSET\\s+", 2)
                        : new String[]{limitClause, ""};
                limit = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
                if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                    offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                }
            }
        }

        if (remaining.toUpperCase().contains(" OFFSET ")) {
            String[] offsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
            remaining = offsetSplit[0].trim();
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
            }
        }

        // Обработка условий WHERE
        List<Condition> conditions = new ArrayList<>();
        if (conditionStr != null && !conditionStr.isEmpty()) {
            conditions = parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes, tableAliases, columnAliases);
        }

        // Проверка корректности столбцов
        for (String column : columns) {
            String normalizedColumn = normalizeColumnName(column, mainTable.getName(), tableAliases);
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Неизвестный столбец в SELECT: {0}, доступные столбцы: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Неизвестный столбец: " + column);
            }
        }

        // Проверка корректности столбцов в агрегатных функциях
        for (AggregateFunction agg : aggregates) {
            if (agg.column != null) {
                String normalizedColumn = normalizeColumnName(agg.column, mainTable.getName(), tableAliases);
                String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                boolean found = false;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOGGER.log(Level.SEVERE, "Неизвестный столбец в {0}: {1}, доступные столбцы: {2}",
                            new Object[]{agg.toString(), agg.column, combinedColumnTypes.keySet()});
                    throw new IllegalArgumentException("Неизвестный столбец в " + agg.toString() + ": " + agg.column);
                }
            }
        }

        // Проверка корректности столбцов в GROUP BY
        for (String column : groupBy) {
            String normalizedColumn = normalizeColumnName(column, mainTable.getName(), tableAliases);
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Неизвестный столбец в GROUP BY: {0}, доступные столбцы: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Неизвестный столбец в GROUP BY: " + column);
            }
        }

        // Проверка правил GROUP BY
        if (!groupBy.isEmpty()) {
            for (String column : columns) {
                String normalizedColumn = normalizeColumnName(column, mainTable.getName(), tableAliases);
                if (!groupBy.contains(normalizedColumn)) {
                    boolean isAggregated = aggregates.stream().anyMatch(agg -> agg.column != null && agg.column.equals(normalizedColumn));
                    if (!isAggregated) {
                        LOGGER.log(Level.SEVERE, "Столбец {0} должен быть в GROUP BY или использоваться в агрегатной функции", column);
                        throw new IllegalArgumentException("Столбец " + column + " должен быть в GROUP BY или использоваться в агрегатной функции");
                    }
                }
            }
        }

        // Логирование результата разбора
        LOGGER.log(Level.INFO, "Разобран SELECT-запрос: столбцы={0}, агрегаты={1}, основная таблица={2}, псевдоним={3}, соединения={4}, условия={5}, группировка={6}, having={7}, лимит={8}, смещение={9}, сортировка={10}, псевдонимы={11}",
                new Object[]{columns, aggregates, mainTable.getName(), tableAlias, joins, conditions, groupBy, havingConditions, limit, offset, orderBy, tableAliases});

        // Создание объекта SelectQuery
        return new SelectQuery(columns, aggregates, conditions, joins, mainTable.getName(), limit, offset, orderBy, groupBy, havingConditions, tableAliases);
    }

    private List<String> splitSelectItems(String selectPart) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        boolean afterAs = false;

        for (int i = 0; i < selectPart.length(); i++) {
            char c = selectPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentItem.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    currentItem.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentItem.append(c);
                    continue;
                } else if (c == ',' && parenDepth == 0) {
                    String item = currentItem.toString().trim();
                    if (!item.isEmpty()) {
                        items.add(item);
                    }
                    currentItem = new StringBuilder();
                    afterAs = false;
                    continue;
                } else if (parenDepth == 0 && !afterAs) {
                    // Проверяем, является ли текущая позиция началом AS или пробелом перед алиасом
                    if (i + 3 <= selectPart.length() && selectPart.substring(i, i + 3).equalsIgnoreCase(" AS ") &&
                            Character.isWhitespace(selectPart.charAt(i - 1))) {
                        currentItem.append(c); // Добавляем 'A'
                        currentItem.append(selectPart.charAt(i + 1)); // Добавляем 'S'
                        currentItem.append(selectPart.charAt(i + 2)); // Добавляем ' '
                        i += 2; // Пропускаем "AS "
                        afterAs = true;
                        continue;
                    } else if (Character.isWhitespace(c) && !currentItem.toString().trim().isEmpty()) {
                        // Проверяем, является ли следующий токен потенциальным алиасом (идентификатором)
                        String remaining = selectPart.substring(i + 1).trim();
                        int nextSpace = remaining.indexOf(' ');
                        int nextComma = remaining.indexOf(',');
                        int endIndex = nextSpace == -1 ? (nextComma == -1 ? remaining.length() : nextComma) : Math.min(nextSpace, nextComma == -1 ? remaining.length() : nextComma);
                        String potentialAlias = remaining.substring(0, endIndex).trim();
                        if (potentialAlias.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                            // Это потенциальный алиас
                            currentItem.append(c);
                            currentItem.append(potentialAlias);
                            i += potentialAlias.length();
                            afterAs = true;
                            continue;
                        }
                    }
                }
            }
            currentItem.append(c);
        }

        String lastItem = currentItem.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }

        return items;
    }

    private List<OrderByInfo> parseOrderByClause(String orderByClause, String defaultTableName, Map<String, Class<?>> combinedColumnTypes, Map<String, String> columnAliases, Map<String, String> tableAliases) {
        List<OrderByInfo> orderBy = new ArrayList<>();
        String[] orderByParts = splitOrderByClause(orderByClause);
        for (String part : orderByParts) {
            String[] tokens = part.trim().split("\\s+", 2);
            if (tokens.length == 0) {
                throw new IllegalArgumentException("Invalid ORDER BY clause: empty expression");
            }
            String column = tokens[0].trim();
            boolean ascending = true;
            if (tokens.length > 1) {
                String direction = tokens[1].toUpperCase();
                if (direction.equals("DESC")) {
                    ascending = false;
                } else if (!direction.equals("ASC")) {
                    throw new IllegalArgumentException("Invalid ORDER BY direction: " + direction);
                }
            }

            boolean found = false;
            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;

            // Check if the column or its unqualified part is an alias in columnAliases
            if (columnAliases.containsValue(unqualifiedColumn)) {
                found = true;
            } else {
                // Check if the column matches a key in columnAliases (actual column name)
                for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
                    String aliasedColumn = aliasEntry.getKey();
                    String alias = aliasEntry.getValue();
                    if (aliasedColumn.equalsIgnoreCase(column) || alias.equalsIgnoreCase(unqualifiedColumn)) {
                        found = true;
                        break;
                    }
                }
            }

            // If not an alias, check if it's a physical column
            if (!found) {
                String normalizedColumn = column.contains(".") ? normalizeColumnName(column, defaultTableName, tableAliases) : defaultTableName + "." + column;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
                    if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn) || entry.getKey().equalsIgnoreCase(normalizedColumn)) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column or alias in ORDER BY: {0}, available columns: {1}, aliases: {2}",
                        new Object[]{column, combinedColumnTypes.keySet(), columnAliases.values()});
                throw new IllegalArgumentException("Unknown column in ORDER BY: " + column);
            }

            orderBy.add(new OrderByInfo(column, ascending));
            LOGGER.log(Level.FINE, "Parsed ORDER BY: column={0}, ascending={1}", new Object[]{column, ascending});
        }
        return orderBy;
    }

    private String[] splitOrderByClause(String orderByClause) {
        List<String> parts = new ArrayList<>();
        StringBuilder currentPart = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < orderByClause.length(); i++) {
            char c = orderByClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentPart.append(c);
                continue;
            }
            if (!inQuotes && c == ',') {
                String part = currentPart.toString().trim();
                if (!part.isEmpty()) {
                    parts.add(part);
                }
                currentPart = new StringBuilder();
                continue;
            }
            currentPart.append(c);
        }

        String lastPart = currentPart.toString().trim();
        if (!lastPart.isEmpty()) {
            parts.add(lastPart);
        }

        return parts.toArray(new String[0]);
    }

    private Integer parseLimitClause(String limitClause) {
        String normalized = limitClause.toUpperCase().replace("LIMIT", "").trim();
        if (normalized.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty LIMIT clause detected");
            return null; // No LIMIT specified
        }
        try {
            int limitValue = Integer.parseInt(normalized);
            if (limitValue < 0) {
                throw new IllegalArgumentException("LIMIT value must be non-negative: " + limitValue);
            }
            LOGGER.log(Level.FINE, "Parsed LIMIT clause: {0}", limitValue);
            return limitValue;
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid LIMIT value: {0}", normalized);
            throw new IllegalArgumentException("Invalid LIMIT value: " + normalized);
        }
    }

    private Integer parseOffsetClause(String offsetClause) {
        String normalized = offsetClause.toUpperCase().replace("OFFSET", "").trim();
        if (normalized.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty OFFSET clause detected");
            return null; // No OFFSET specified
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
                    throw new IllegalArgumentException("Invalid numeric value: " + valueStr);
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value: " + valueStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value format: " + valueStr + " for column type: " + columnType.getSimpleName());
        }
    }

    private List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases, Map<String, String> columnAliases) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        LOGGER.log(Level.FINE, "Parsing conditions for table {0}, column types: {1}, isOnClause: {2}, condition: {3}, aliases: {4}",
                new Object[]{tableName, combinedColumnTypes, isOnClause, conditionStr, tableAliases});

        String cleanConditionStr = conditionStr;
        String[] limitSplit = cleanConditionStr.toUpperCase().contains(" LIMIT ")
                ? cleanConditionStr.split("(?i)\\s+LIMIT\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = limitSplit[0].trim();
        if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected LIMIT clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] offsetSplit = cleanConditionStr.toUpperCase().contains(" OFFSET ")
                ? cleanConditionStr.split("(?i)\\s+OFFSET\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = offsetSplit[0].trim();
        if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected OFFSET clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] orderBySplit = cleanConditionStr.toUpperCase().contains(" ORDER BY ")
                ? cleanConditionStr.split("(?i)\\s+ORDER BY\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = orderBySplit[0].trim();
        if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected ORDER BY clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] groupBySplit = cleanConditionStr.toUpperCase().contains(" GROUP BY ")
                ? cleanConditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = groupBySplit[0].trim();
        if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected GROUP BY clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        if (cleanConditionStr.isEmpty()) {
            return new ArrayList<>();
        }

        List<Condition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        int nestingLevel = 0;

        for (int i = 0; i < cleanConditionStr.length(); i++) {
            char c = cleanConditionStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    nestingLevel++;
                    if (parenDepth == 1) {
                        continue;
                    }
                } else if (c == ')') {
                    parenDepth--;
                    nestingLevel--;
                    if (parenDepth == 0) {
                        String subConditionStr = currentCondition.toString().trim();
                        if (!subConditionStr.isEmpty()) {
                            boolean isNot = subConditionStr.toUpperCase().startsWith("NOT ");
                            if (isNot) {
                                subConditionStr = subConditionStr.substring(4).trim();
                            }
                            if (subConditionStr.isEmpty()) {
                                throw new IllegalArgumentException("Empty grouped condition in clause: " + cleanConditionStr);
                            }
                            LOGGER.log(Level.FINE, "Parsing nested condition at level {0}: {1}, isNot: {2}",
                                    new Object[]{nestingLevel + 1, subConditionStr, isNot});
                            List<Condition> subConditions = parseConditions(subConditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes, tableAliases, columnAliases);
                            if (subConditions.isEmpty()) {
                                throw new IllegalArgumentException("No valid conditions found in grouped clause: " + subConditionStr);
                            }
                            String conjunction = determineConjunctionAfter(cleanConditionStr, i);
                            conditions.add(new Condition(subConditions, conjunction, isNot));
                            LOGGER.log(Level.FINE, "Added grouped condition with {0} subconditions, conjunction: {1}",
                                    new Object[]{subConditions.size(), conjunction});
                        } else {
                            throw new IllegalArgumentException("Empty grouped condition in clause: " + cleanConditionStr);
                        }
                        currentCondition = new StringBuilder();
                        continue;
                    }
                } else if (parenDepth == 0) {
                    if (i + 3 <= cleanConditionStr.length() && cleanConditionStr.substring(i, i + 3).equalsIgnoreCase("AND") &&
                            (i == 0 || Character.isWhitespace(cleanConditionStr.charAt(i - 1))) &&
                            (i + 3 == cleanConditionStr.length() || Character.isWhitespace(cleanConditionStr.charAt(i + 3)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "AND", combinedColumnTypes, originalQuery, isOnClause, tableName, tableAliases, columnAliases));
                            LOGGER.log(Level.FINE, "Added condition with AND: {0}", current);
                        }
                        currentCondition = new StringBuilder();
                        i += 2;
                        continue;
                    } else if (i + 2 <= cleanConditionStr.length() && cleanConditionStr.substring(i, i + 2).equalsIgnoreCase("OR") &&
                            (i == 0 || Character.isWhitespace(cleanConditionStr.charAt(i - 1))) &&
                            (i + 2 == cleanConditionStr.length() || Character.isWhitespace(cleanConditionStr.charAt(i + 2)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "OR", combinedColumnTypes, originalQuery, isOnClause, tableName, tableAliases, columnAliases));
                            LOGGER.log(Level.FINE, "Added condition with OR: {0}", current);
                        }
                        currentCondition = new StringBuilder();
                        i += 1;
                        continue;
                    }
                }
            }
            currentCondition.append(c);
        }

        String lastCondition = currentCondition.toString().trim();
        if (!lastCondition.isEmpty()) {
            conditions.add(parseSingleCondition(lastCondition, null, combinedColumnTypes, originalQuery, isOnClause, tableName, tableAliases, columnAliases));
            LOGGER.log(Level.FINE, "Added final condition: {0}", lastCondition);
        }

        return conditions;
    }

    private String determineConjunctionAfter(String conditionStr, int index) {
        String remaining = conditionStr.substring(index + 1).trim();
        if (remaining.toUpperCase().startsWith("AND ")) {
            return "AND";
        } else if (remaining.toUpperCase().startsWith("OR ")) {
            return "OR";
        }
        return null;
    }

    private Condition parseSingleCondition(String conditionStr, String conjunction, Map<String, Class<?>> combinedColumnTypes, String originalQuery, boolean isOnClause, String tableName, Map<String, String> tableAliases, Map<String, String> columnAliases) {
        boolean isNot = conditionStr.toUpperCase().startsWith("NOT ");
        String cleanCondition = isNot ? conditionStr.substring(4).trim() : conditionStr;

        if (cleanCondition.isEmpty()) {
            throw new IllegalArgumentException("Empty condition after NOT: " + conditionStr);
        }

        Pattern inPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IN\\s+(?:\\((.*)\\)|(.*))$");
        Matcher inMatcher = inPattern.matcher(cleanCondition);
        if (inMatcher.matches()) {
            String column = inMatcher.group(1).trim();
            String resolvedColumn;
            // Check if column is an alias
            if (columnAliases.containsValue(column)) {
                String actualColumn = columnAliases.entrySet().stream()
                        .filter(e -> e.getValue().equalsIgnoreCase(column))
                        .findFirst().get().getKey();
                resolvedColumn = normalizeColumnName(actualColumn, tableName, tableAliases);
            } else {
                resolvedColumn = normalizeColumnName(column, tableName, tableAliases);
            }
            String valuesStr = inMatcher.group(2) != null ? inMatcher.group(2).trim() : inMatcher.group(3).trim();
            List<Object> values = parseInValues(resolvedColumn, valuesStr, combinedColumnTypes, originalQuery, columnAliases);
            LOGGER.log(Level.FINE, "Parsed IN condition: column={0}, values={1}, not={2}, conjunction={3}",
                    new Object[]{resolvedColumn, values, isNot, conjunction});
            return new Condition(resolvedColumn, values, conjunction, isNot);
        }

        // Handle NULL conditions
        Pattern nullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL$");
        Matcher nullMatcher = nullPattern.matcher(cleanCondition);
        if (nullMatcher.matches()) {
            String column = nullMatcher.group(1).trim();
            String normalizedColumn = columnAliases.containsValue(column)
                    ? normalizeColumnName(columnAliases.entrySet().stream()
                    .filter(e -> e.getValue().equalsIgnoreCase(column))
                    .findFirst().get().getKey(), tableName, tableAliases)
                    : normalizeColumnName(column, tableName, tableAliases);
            Operator operator = nullMatcher.group(2) != null ? Operator.IS_NOT_NULL : Operator.IS_NULL;
            LOGGER.log(Level.FINE, "Parsed NULL condition: column={0}, operator={1}, not={2}, conjunction={3}",
                    new Object[]{normalizedColumn, operator, isNot, conjunction});
            return new Condition(normalizedColumn, operator, conjunction, isNot);
        }

        String[] operators = {"!=", ">=", "<=", "=", ">", "<", "LIKE", "NOT LIKE"};
        String selectedOperator = null;
        String[] conditionParts = null;
        for (String op : operators) {
            String[] parts = cleanCondition.split("(?i)\\s+" + Pattern.quote(op) + "\\s+", 2);
            if (parts.length == 2) {
                selectedOperator = op.toUpperCase();
                conditionParts = parts;
                break;
            }
        }

        if (conditionParts == null) {
            throw new IllegalArgumentException("Invalid condition syntax: " + cleanCondition);
        }

        String leftPart = conditionParts[0].trim();
        String rightPart = conditionParts[1].trim();

        String column = columnAliases.containsValue(leftPart)
                ? normalizeColumnName(columnAliases.entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(leftPart))
                .findFirst().get().getKey(), tableName, tableAliases)
                : normalizeColumnName(leftPart, tableName, tableAliases);

        if (selectedOperator.equals("LIKE") || selectedOperator.equals("NOT LIKE")) {
            if (!rightPart.startsWith("'") || !rightPart.endsWith("'")) {
                throw new IllegalArgumentException("LIKE/NOT LIKE pattern must be a quoted string: " + rightPart);
            }
            String pattern = rightPart.substring(1, rightPart.length() - 1);
            LOGGER.log(Level.FINE, "Parsed LIKE condition: column={0}, pattern={1}, operator={2}, not={3}, conjunction={4}",
                    new Object[]{column, pattern, selectedOperator, isNot, conjunction});
            return new Condition(column, pattern, selectedOperator.equals("LIKE") ? Operator.LIKE : Operator.NOT_LIKE, conjunction, isNot);
        }

        boolean isColumnComparison = rightPart.matches("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*");
        Class<?> columnType = getColumnType(column, combinedColumnTypes, tableName, tableAliases, columnAliases);

        if (isColumnComparison) {
            String rightColumn = normalizeColumnName(rightPart, tableName, tableAliases);
            Operator operator;
            switch (selectedOperator) {
                case "=":
                    operator = Operator.EQUALS;
                    break;
                case "!=":
                    operator = Operator.NOT_EQUALS;
                    break;
                case ">":
                    operator = Operator.GREATER_THAN;
                    break;
                case ">=":
                    operator = Operator.GREATER_THAN_OR_EQUALS;
                    break;
                case "<":
                    operator = Operator.LESS_THAN;
                    break;
                case "<=":
                    operator = Operator.LESS_THAN_OR_EQUALS;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator for column comparison: " + selectedOperator);
            }
            LOGGER.log(Level.FINE, "Parsed column comparison: left={0}, right={1}, operator={2}, not={3}, conjunction={4}",
                    new Object[]{column, rightColumn, operator, isNot, conjunction});
            return new Condition(column, rightColumn, operator, conjunction, isNot);
        }

        Object value = parseConditionValue(column, rightPart, columnType);
        Operator operator;
        switch (selectedOperator) {
            case "=":
                operator = Operator.EQUALS;
                break;
            case "!=":
                operator = Operator.NOT_EQUALS;
                break;
            case ">":
                operator = Operator.GREATER_THAN;
                break;
            case ">=":
                operator = Operator.GREATER_THAN_OR_EQUALS;
                break;
            case "<":
                operator = Operator.LESS_THAN;
                break;
            case "<=":
                operator = Operator.LESS_THAN_OR_EQUALS;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + selectedOperator);
        }
        LOGGER.log(Level.FINE, "Parsed condition: column={0}, value={1}, operator={2}, not={3}, conjunction={4}",
                new Object[]{column, value, operator, isNot, conjunction});
        return new Condition(column, value, operator, conjunction, isNot);
    }

    private List<Object> parseInValues(String column, String valuesStr, Map<String, Class<?>> combinedColumnTypes, String originalQuery, Map<String, String> columnAliases) {
        List<Object> values = new ArrayList<>();
        String[] valueItems = valuesStr.split(",(?=([^']*'[^']*')*[^']*$)");
        Class<?> columnType = getColumnType(column, combinedColumnTypes, null, new HashMap<>(), columnAliases);
        for (String valueStr : valueItems) {
            values.add(parseConditionValue(column, valueStr.trim(), columnType));
        }
        return values;
    }

    private List<HavingCondition> parseHavingConditions(String havingClause, String tableName, Database database, String originalQuery, List<AggregateFunction> aggregates, Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases, Map<String, String> columnAliases) {
        List<HavingCondition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

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
                    if (parenDepth == 1) continue;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0) {
                        String subConditionStr = currentCondition.toString().trim();
                        if (!subConditionStr.isEmpty()) {
                            boolean isNot = subConditionStr.toUpperCase().startsWith("NOT ");
                            if (isNot) subConditionStr = subConditionStr.substring(4).trim();
                            List<HavingCondition> subConditions = parseHavingConditions(subConditionStr, tableName, database, originalQuery, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                            String conjunction = determineConjunctionAfter(havingClause, i);
                            conditions.add(new HavingCondition(subConditions, conjunction, isNot));
                        }
                        currentCondition = new StringBuilder();
                        continue;
                    }
                } else if (parenDepth == 0) {
                    if (i + 3 <= havingClause.length() && havingClause.substring(i, i + 3).equalsIgnoreCase("AND") &&
                            (i == 0 || Character.isWhitespace(havingClause.charAt(i - 1))) &&
                            (i + 3 == havingClause.length() || Character.isWhitespace(havingClause.charAt(i + 3)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleHavingCondition(current, "AND", aggregates, combinedColumnTypes, originalQuery, tableName, tableAliases, columnAliases));
                        }
                        currentCondition = new StringBuilder();
                        i += 2;
                        continue;
                    } else if (i + 2 <= havingClause.length() && havingClause.substring(i, i + 2).equalsIgnoreCase("OR") &&
                            (i == 0 || Character.isWhitespace(havingClause.charAt(i - 1))) &&
                            (i + 2 == havingClause.length() || Character.isWhitespace(havingClause.charAt(i + 2)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleHavingCondition(current, "OR", aggregates, combinedColumnTypes, originalQuery, tableName, tableAliases, columnAliases));
                        }
                        currentCondition = new StringBuilder();
                        i += 1;
                        continue;
                    }
                }
            }
            currentCondition.append(c);
        }

        String lastCondition = currentCondition.toString().trim();
        if (!lastCondition.isEmpty()) {
            conditions.add(parseSingleHavingCondition(lastCondition, null, aggregates, combinedColumnTypes, originalQuery, tableName, tableAliases, columnAliases));
        }

        return conditions;
    }

    private HavingCondition parseSingleHavingCondition(String conditionStr, String conjunction, List<AggregateFunction> aggregates, Map<String, Class<?>> combinedColumnTypes, String originalQuery, String tableName, Map<String, String> tableAliases, Map<String, String> columnAliases) {
        boolean isNot = conditionStr.toUpperCase().startsWith("NOT ");
        String cleanCondition = isNot ? conditionStr.substring(4).trim() : conditionStr;

        String[] operators = {"!=", ">=", "<=", "=", ">", "<"};
        String selectedOperator = null;
        String[] conditionParts = null;
        for (String op : operators) {
            String[] parts = cleanCondition.split("(?i)\\s+" + Pattern.quote(op) + "\\s+", 2);
            if (parts.length == 2) {
                selectedOperator = op.toUpperCase();
                conditionParts = parts;
                break;
            }
        }

        if (conditionParts == null) {
            throw new IllegalArgumentException("Invalid HAVING condition syntax: " + cleanCondition);
        }

        String leftPart = conditionParts[0].trim();
        String rightPart = conditionParts[1].trim();

        AggregateFunction aggregate = null;
        for (AggregateFunction agg : aggregates) {
            String aggStr = agg.toString();
            if (agg.alias != null && agg.alias.equalsIgnoreCase(leftPart)) {
                aggregate = agg;
                break;
            }
            if (aggStr.equalsIgnoreCase(leftPart)) {
                aggregate = agg;
                break;
            }
        }

        if (aggregate == null) {
            throw new IllegalArgumentException("HAVING condition must reference an aggregate function: " + leftPart);
        }

        Class<?> valueType;
        switch (aggregate.functionName) {
            case "COUNT":
                valueType = Long.class;
                break;
            case "SUM":
            case "AVG":
                valueType = Double.class;
                break;
            case "MIN":
            case "MAX":
                valueType = aggregate.column != null ? getColumnType(normalizeColumnName(aggregate.column, tableName, tableAliases), combinedColumnTypes, tableName, tableAliases, columnAliases) : Double.class;
                break;
            default:
                throw new IllegalArgumentException("Unsupported aggregate function in HAVING: " + aggregate.functionName);
        }

        Object value = parseConditionValue(aggregate.toString(), rightPart, valueType);
        Operator operator;
        switch (selectedOperator) {
            case "=":
                operator = Operator.EQUALS;
                break;
            case "!=":
                operator = Operator.NOT_EQUALS;
                break;
            case ">":
                operator = Operator.GREATER_THAN;
                break;
            case ">=":
                operator = Operator.GREATER_THAN_OR_EQUALS;
                break;
            case "<":
                operator = Operator.LESS_THAN;
                break;
            case "<=":
                operator = Operator.LESS_THAN_OR_EQUALS;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator in HAVING: " + selectedOperator);
        }

        LOGGER.log(Level.FINE, "Parsed HAVING condition: aggregate={0}, value={1}, operator={2}, not={3}, conjunction={4}",
                new Object[]{aggregate, value, operator, isNot, conjunction});
        return new HavingCondition(aggregate, operator, value, conjunction, isNot);
    }

    private String normalizeColumnName(String column, String defaultTable, Map<String, String> tableAliases) {
        if (column.contains(".")) {
            String[] parts = column.split("\\.", 2);
            String prefix = parts[0].trim();
            String colName = parts[1].trim();
            String resolvedTable = tableAliases.getOrDefault(prefix, prefix);
            return resolvedTable + "." + colName;
        }
        return defaultTable + "." + column.trim();
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> combinedColumnTypes, String defaultTable, Map<String, String> tableAliases, Map<String, String> columnAliases) {
        // Check if the column is an alias
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, String> aliasEntry : columnAliases.entrySet()) {
            if (aliasEntry.getValue().equalsIgnoreCase(unqualifiedColumn)) {
                String actualColumn = normalizeColumnName(aliasEntry.getKey(), defaultTable, tableAliases);
                String actualUnqualified = actualColumn.contains(".") ? actualColumn.split("\\.")[1].trim() : actualColumn;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(actualUnqualified)) {
                        return entry.getValue();
                    }
                }
            }
        }

        // Check if the column itself is an alias
        if (columnAliases.containsValue(unqualifiedColumn)) {
            String finalUnqualifiedColumn = unqualifiedColumn;
            String actualColumn = columnAliases.entrySet().stream()
                    .filter(e -> e.getValue().equalsIgnoreCase(finalUnqualifiedColumn))
                    .findFirst().get().getKey();
            String normalized = normalizeColumnName(actualColumn, defaultTable, tableAliases);
            String normalizedUnqualified = normalized.contains(".") ? normalized.split("\\.")[1].trim() : normalized;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(normalizedUnqualified)) {
                    return entry.getValue();
                }
            }
        }

        // Fallback to physical column check
        String normalized = normalizeColumnName(column, defaultTable, tableAliases);
        unqualifiedColumn = normalized.contains(".") ? normalized.split("\\.")[1].trim() : normalized;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("Unknown column or alias: " + column);
    }
}