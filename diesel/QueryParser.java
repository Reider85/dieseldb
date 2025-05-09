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
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, IN, LIKE, NOT_LIKE, IS_NULL, IS_NOT_NULL
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
        String leftColumn;
        String rightColumn;
        String originalTable;
        JoinType joinType;
        List<Condition> onConditions;

        JoinInfo(String originalTable, String tableName, String leftColumn, String rightColumn, JoinType joinType) {
            this(originalTable, tableName, leftColumn, rightColumn, joinType, new ArrayList<>());
        }

        JoinInfo(String originalTable, String tableName, String leftColumn, String rightColumn, JoinType joinType, List<Condition> onConditions) {
            this.originalTable = originalTable;
            this.tableName = tableName;
            this.leftColumn = leftColumn;
            this.rightColumn = rightColumn;
            this.joinType = joinType;
            this.onConditions = onConditions;
        }

        @Override
        public String toString() {
            return "JoinInfo{originalTable=" + originalTable + ", table=" + tableName +
                    ", leftColumn=" + leftColumn + ", rightColumn=" + rightColumn +
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
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid SELECT query format");
        }

        String selectPartOriginal = original.substring(original.indexOf("SELECT") + 6, original.indexOf("FROM")).trim();
        List<String> selectItems = splitSelectItems(selectPartOriginal);

        List<String> columns = new ArrayList<>();
        List<AggregateFunction> aggregates = new ArrayList<>();

        Pattern countPattern = Pattern.compile("(?i)^COUNT\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern minPattern = Pattern.compile("(?i)^MIN\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern maxPattern = Pattern.compile("(?i)^MAX\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern avgPattern = Pattern.compile("(?i)^AVG\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern sumPattern = Pattern.compile("(?i)^SUM\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher countMatcher = countPattern.matcher(trimmedItem);
            Matcher minMatcher = minPattern.matcher(trimmedItem);
            Matcher maxMatcher = maxPattern.matcher(trimmedItem);
            Matcher avgMatcher = avgPattern.matcher(trimmedItem);
            Matcher sumMatcher = sumPattern.matcher(trimmedItem);
            if (countMatcher.matches()) {
                String countArg = countMatcher.group(1);
                String alias = countMatcher.group(2);
                String column = countArg.equals("*") ? null : countArg;
                aggregates.add(new AggregateFunction("COUNT", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: COUNT({0}){1}",
                        new Object[]{column == null ? "*" : column, alias != null ? " AS " + alias : ""});
            } else if (minMatcher.matches()) {
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                aggregates.add(new AggregateFunction("MIN", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: MIN({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (maxMatcher.matches()) {
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                aggregates.add(new AggregateFunction("MAX", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: MAX({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (avgMatcher.matches()) {
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                aggregates.add(new AggregateFunction("AVG", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: AVG({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (sumMatcher.matches()) {
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                aggregates.add(new AggregateFunction("SUM", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: SUM({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else {
                columns.add(trimmedItem);
            }
        }

        String tableAndJoins = parts[1].trim();
        List<JoinInfo> joins = new ArrayList<>();
        String tableName;
        String conditionStr = null;
        Integer limit = null;
        Integer offset = null;
        List<OrderByInfo> orderBy = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();

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

        tableName = joinParts.get(0).split(" ")[0].trim();
        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());

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
            List<Condition> onConditions = new ArrayList<>();

            if (joinType == JoinType.CROSS) {
                String[] crossSplit = joinPart.split("\\s+(?=(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|WHERE|LIMIT|OFFSET|ORDER BY|$))", 2);
                joinTableName = crossSplit[0].trim();
                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                if (crossSplit.length > 1 && !crossSplit[1].trim().isEmpty()) {
                    String remaining = crossSplit[1].trim();
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
                        orderBy = parseOrderByClause(remaining.substring(9).trim(), combinedColumnTypes);
                    }
                    if (remaining.toUpperCase().contains(" ON ")) {
                        throw new IllegalArgumentException("CROSS JOIN does not support ON clause: " + joinPart);
                    }
                }
                LOGGER.log(Level.FINE, "Parsed CROSS JOIN: table={0}", joinTableName);
            } else {
                String[] onSplit = joinPart.split("(?i)ON\\s+", 2);
                if (onSplit.length != 2) {
                    throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: missing ON clause");
                }

                joinTableName = onSplit[0].split(" ")[0].trim();
                String onClause = onSplit[1].trim();

                String onCondition;
                if (onClause.toUpperCase().contains(" WHERE ")) {
                    String[] onWhereSplit = onClause.split("(?i)\\s+WHERE\\s+", 2);
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
                                orderBy = parseOrderByClause(whereOrderBySplit[1].trim(), combinedColumnTypes);
                            }
                        } else {
                            conditionStr = remaining;
                        }
                    }
                } else if (onClause.toUpperCase().startsWith("LIMIT ")) {
                    String[] onLimitSplit = onClause.split("(?i)\\s+OFFSET\\s+", 2);
                    onCondition = "";
                    if (onLimitSplit.length > 1) {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].substring(6).trim());
                        offset = parseOffsetClause("OFFSET " + onLimitSplit[1].trim());
                    } else {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].trim());
                    }
                } else if (onClause.toUpperCase().startsWith("OFFSET ")) {
                    onCondition = "";
                    offset = parseOffsetClause(onClause);
                } else if (onClause.toUpperCase().startsWith("ORDER BY ")) {
                    String[] onOrderBySplit = onClause.split("(?i)\\s+ORDER BY\\s+", 2);
                    onCondition = "";
                    if (onOrderBySplit.length > 1) {
                        orderBy = parseOrderByClause(onOrderBySplit[1].trim(), combinedColumnTypes);
                    }
                } else {
                    onCondition = onClause;
                }

                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());

                onConditions = parseConditions(onCondition, tableName, database, original, true, combinedColumnTypes);

                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName);
                }

                LOGGER.log(Level.FINE, "Parsed ON conditions for {0}: {1}", new Object[]{joinTypeStr, onConditions});
            }

            joins.add(new JoinInfo(tableName, joinTableName, null, null, joinType, onConditions));
            tableName = joinTableName;
        }

        String remaining = joinParts.get(joinParts.size() - 1);
        if (remaining.toUpperCase().contains(" WHERE ")) {
            String[] whereSplit = remaining.split("(?i)\\s+WHERE\\s+", 2);
            conditionStr = whereSplit[1].trim();
            remaining = whereSplit[0].trim();
        }

        if (conditionStr != null && !conditionStr.isEmpty()) {
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
                orderBy = parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
            }

            String[] groupBySplit = conditionStr.toUpperCase().contains(" GROUP BY ")
                    ? conditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = groupBySplit[0].trim();
            if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
                String groupByPart = groupBySplit[1].trim();
                groupBy = Arrays.stream(groupByPart.split(","))
                        .map(col -> normalizeColumnName(col.trim(), mainTable.getName()))
                        .collect(Collectors.toList());
                LOGGER.log(Level.FINE, "Parsed GROUP BY clause: {0}", groupBy);
            }
        }

        if (remaining.toUpperCase().contains(" ORDER BY ")) {
            String[] orderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
            remaining = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
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

        List<Condition> conditions = new ArrayList<>();
        if (conditionStr != null && !conditionStr.isEmpty()) {
            conditions = parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes);
        }

        for (String column : columns) {
            String normalizedColumn = normalizeColumnName(column, mainTable.getName());
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in SELECT: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
        }

        for (AggregateFunction agg : aggregates) {
            if (agg.column != null) {
                String normalizedColumn = normalizeColumnName(agg.column, mainTable.getName());
                String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                boolean found = false;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOGGER.log(Level.SEVERE, "Unknown column in {0}: {1}, available columns: {2}",
                            new Object[]{agg.toString(), agg.column, combinedColumnTypes.keySet()});
                    throw new IllegalArgumentException("Unknown column in " + agg.toString() + ": " + agg.column);
                }
            }
        }

        for (String column : groupBy) {
            String normalizedColumn = normalizeColumnName(column, mainTable.getName());
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in GROUP BY: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column in GROUP BY: " + column);
            }
        }

        if (!groupBy.isEmpty()) {
            for (String column : columns) {
                String normalizedColumn = normalizeColumnName(column, mainTable.getName());
                if (!groupBy.contains(normalizedColumn)) {
                    boolean isAggregated = aggregates.stream().anyMatch(agg -> agg.column != null && agg.column.equals(normalizedColumn));
                    if (!isAggregated) {
                        LOGGER.log(Level.SEVERE, "Column {0} must appear in GROUP BY clause or be used in an aggregate function", column);
                        throw new IllegalArgumentException("Column " + column + " must appear in GROUP BY clause or be used in an aggregate function");
                    }
                }
            }
        }

        LOGGER.log(Level.INFO, "Parsed SELECT query: columns={0}, aggregates={1}, mainTable={2}, joins={3}, conditions={4}, groupBy={5}, limit={6}, offset={7}, orderBy={8}",
                new Object[]{columns, aggregates, mainTable.getName(), joins, conditions, groupBy, limit, offset, orderBy});

        return new SelectQuery(columns, aggregates, conditions, joins, mainTable.getName(), limit, offset, orderBy, groupBy);
    }

    private List<String> splitSelectItems(String selectPart) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

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
                    continue;
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

    private List<OrderByInfo> parseOrderByClause(String orderByClause, Map<String, Class<?>> combinedColumnTypes) {
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

            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in ORDER BY: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
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

    private void validateJoinCondition(Condition condition, String leftTable, String rightTable) {
        LOGGER.log(Level.FINEST, "Validating condition: {0} for tables {1}, {2}", new Object[]{condition, leftTable, rightTable});
        if (condition.isGrouped()) {
            for (Condition subCond : condition.subConditions) {
                validateJoinCondition(subCond, leftTable, rightTable);
            }
            return;
        }
        if (condition.column != null) {
            String tableName = condition.column.contains(".") ? condition.column.split("\\.")[0] : null;
            if (tableName != null && !tableName.equalsIgnoreCase(leftTable) && !tableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("Invalid table in ON condition: " + tableName +
                        ", expected " + leftTable + " or " + rightTable);
            }
        }
        if (condition.rightColumn != null) {
            String tableName = condition.rightColumn.contains(".") ? condition.rightColumn.split("\\.")[0] : null;
            if (tableName != null && !tableName.equalsIgnoreCase(leftTable) && !tableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("Invalid table in ON condition (right column): " + tableName +
                        ", expected " + leftTable + " or " + rightTable);
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
        String tableName = tablePart.split(" ")[0].trim();

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();

        String setAndWhere = parts[1].trim();
        String setPart;
        List<Condition> conditions = new ArrayList<>();

        if (setAndWhere.contains("WHERE")) {
            String[] setWhereParts = setAndWhere.split("WHERE");
            setPart = setWhereParts[0].trim();
            String conditionStr = setWhereParts[1].trim();
            conditions = parseConditions(conditionStr, tableName, database, original, false, columnTypes);
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

        LOGGER.log(Level.INFO, "Parsed UPDATE query: table={0}, updates={1}, conditions={2}",
                new Object[]{tableName, updates, conditions});

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
            conditions = parseConditions(conditionStr, tableName, database, original, false, table.getColumnTypes());
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

    private List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        LOGGER.log(Level.FINE, "Parsing conditions for table {0}, column types: {1}, isOnClause: {2}, condition: {3}",
                new Object[]{tableName, combinedColumnTypes, isOnClause, conditionStr});

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
                            List<Condition> subConditions = parseConditions(subConditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes);
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
                            conditions.add(parseSingleCondition(current, "AND", combinedColumnTypes, originalQuery, isOnClause, tableName));
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
                            conditions.add(parseSingleCondition(current, "OR", combinedColumnTypes, originalQuery, isOnClause, tableName));
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

        if (currentCondition.length() > 0 && parenDepth == 0) {
            String current = currentCondition.toString().trim();
            if (!current.isEmpty()) {
                conditions.add(parseSingleCondition(current, null, combinedColumnTypes, originalQuery, isOnClause, tableName));
                LOGGER.log(Level.FINE, "Added final condition: {0}", current);
            }
        }

        if (parenDepth != 0) {
            throw new IllegalArgumentException("Mismatched parentheses in condition clause: " + cleanConditionStr);
        }

        LOGGER.log(Level.FINE, "Parsed conditions: {0}", conditions);

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

    private Condition parseSingleCondition(String condition, String conjunction, Map<String, Class<?>> columnTypes, String originalQuery, boolean isOnClause, String tableName) {
        LOGGER.log(Level.FINE, "Parsing single condition: {0}, isOnClause: {1}, conjunction: {2}",
                new Object[]{condition, isOnClause, conjunction});
        boolean isNot = condition.toUpperCase().startsWith("NOT ");
        String conditionWithoutNot = isNot ? condition.substring(4).trim() : condition;

        String conditionColumn = normalizeColumnName(conditionWithoutNot.split("\\s+")[0].trim(), isOnClause ? null : tableName);

        if (conditionWithoutNot.toUpperCase().contains(" IN ")) {
            String originalCondition = extractOriginalCondition(originalQuery, conditionWithoutNot);
            Pattern inPattern = Pattern.compile("(?i)((?:\\w+\\.)?\\w+)\\s+IN\\s*\\(([^)]+)\\)");
            Matcher inMatcher = inPattern.matcher(originalCondition);
            if (!inMatcher.find()) {
                LOGGER.log(Level.SEVERE, "Invalid IN condition format: {0}", originalCondition);
                throw new IllegalArgumentException("Invalid IN clause: " + originalCondition);
            }

            String parsedColumn = normalizeColumnName(inMatcher.group(1).trim(), isOnClause ? null : tableName);
            String valuesPart = inMatcher.group(2).trim();

            if (valuesPart.isEmpty()) {
                throw new IllegalArgumentException("IN clause cannot be empty");
            }

            List<String> valueStrings = splitInValues(valuesPart);
            LOGGER.log(Level.FINE, "Split IN values: {0}", Arrays.toString(valueStrings.toArray()));

            List<Object> inValues = new ArrayList<>();
            Class<?> columnType = getColumnType(parsedColumn, columnTypes);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                        new Object[]{parsedColumn, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + parsedColumn);
            }

            for (String val : valueStrings) {
                String trimmedVal = val.trim();
                if (trimmedVal.isEmpty()) continue;
                inValues.add(parseConditionValue(parsedColumn, trimmedVal, columnType));
            }

            LOGGER.log(Level.FINE, "Parsed {0} condition: column={1}, values={2}, not={3}, conjunction={4}",
                    new Object[]{isNot ? "NOT IN" : "IN", parsedColumn, inValues, isNot, conjunction});
            return new Condition(parsedColumn, inValues, conjunction, isNot);
        }

        String[] partsByOperator = null;
        Operator operator = null;

        String upperCondition = conditionWithoutNot.toUpperCase();
        if (upperCondition.contains(" IS NOT NULL")) {
            partsByOperator = conditionWithoutNot.split("\\s*IS NOT NULL\\s*", 2);
            operator = Operator.IS_NOT_NULL;
        } else if (upperCondition.contains(" IS NULL")) {
            partsByOperator = conditionWithoutNot.split("\\s*IS NULL\\s*", 2);
            operator = Operator.IS_NULL;
        } else if (upperCondition.contains(" NOT LIKE ")) {
            partsByOperator = conditionWithoutNot.split("\\s*NOT LIKE\\s*", 2);
            operator = Operator.NOT_LIKE;
        } else if (upperCondition.contains(" LIKE ")) {
            partsByOperator = conditionWithoutNot.split("\\s*LIKE\\s*", 2);
            operator = Operator.LIKE;
        } else if (conditionWithoutNot.contains("!=")) {
            partsByOperator = conditionWithoutNot.split("\\s*!=\\s*", 2);
            operator = Operator.NOT_EQUALS;
        } else if (conditionWithoutNot.contains("=")) {
            partsByOperator = conditionWithoutNot.split("\\s*=\\s*", 2);
            operator = Operator.EQUALS;
        } else if (conditionWithoutNot.contains("<")) {
            partsByOperator = conditionWithoutNot.split("\\s*<\\s*", 2);
            operator = Operator.LESS_THAN;
        } else if (conditionWithoutNot.contains(">")) {
            partsByOperator = conditionWithoutNot.split("\\s*>\\s*", 2);
            operator = Operator.GREATER_THAN;
        }

        if (partsByOperator == null || partsByOperator.length < 1) {
            LOGGER.log(Level.SEVERE, "Invalid condition format: {0}, parts: {1}",
                    new Object[]{condition, partsByOperator == null ? "null" : Arrays.toString(partsByOperator)});
            throw new IllegalArgumentException("Invalid condition clause: must contain =, !=, <, >, IN, NOT IN, LIKE, NOT LIKE, IS NULL, or IS NOT NULL with valid column");
        }

        String parsedColumn = normalizeColumnName(partsByOperator[0].trim(), isOnClause ? null : tableName);
        Class<?> columnType = getColumnType(parsedColumn, columnTypes);
        if (columnType == null) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{parsedColumn, columnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + parsedColumn);
        }

        if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
            if (partsByOperator.length > 1 && !partsByOperator[1].trim().isEmpty()) {
                throw new IllegalArgumentException("IS NULL/IS NOT NULL conditions should not have a value: " + condition);
            }
            LOGGER.log(Level.FINE, "Parsed {0} condition: column={1}, not={2}, conjunction={3}",
                    new Object[]{operator == Operator.IS_NULL ? "IS NULL" : "IS NOT NULL", parsedColumn, isNot, conjunction});
            return new Condition(parsedColumn, operator, conjunction, isNot);
        }

        if (partsByOperator.length != 2) {
            LOGGER.log(Level.SEVERE, "Invalid condition format: {0}, parts: {1}",
                    new Object[]{condition, Arrays.toString(partsByOperator)});
            throw new IllegalArgumentException("Invalid condition clause: must contain =, !=, <, >, IN, NOT IN, LIKE, or NOT LIKE with valid column and value");
        }

        String valueStr = partsByOperator[1].trim();
        if (parsedColumn.isEmpty() || valueStr.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Invalid condition clause: column or value is empty in condition: {0}", condition);
            throw new IllegalArgumentException("Invalid condition clause: column or value is empty in condition: " + condition);
        }

        if ((operator == Operator.LIKE || operator == Operator.NOT_LIKE) && columnType != String.class) {
            throw new IllegalArgumentException("LIKE and NOT LIKE operators are only supported for String columns: " + parsedColumn);
        }

        if (isOnClause && valueStr.matches("[a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_][a-zA-Z0-9_]*")) {
            String rightColumn = valueStr;
            Class<?> rightColumnType = getColumnType(rightColumn, columnTypes);
            if (rightColumnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown right column: {0}, available columns: {1}",
                        new Object[]{rightColumn, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown right column: " + rightColumn);
            }
            if (!rightColumnType.equals(columnType)) {
                throw new IllegalArgumentException("Type mismatch between columns: " + parsedColumn + " (" + columnType.getSimpleName() +
                        ") and " + rightColumn + " (" + rightColumnType.getSimpleName() + ")");
            }
            LOGGER.log(Level.FINE, "Parsed column comparison condition: leftColumn={0}, rightColumn={1}, operator={2}, not={3}, conjunction={4}",
                    new Object[]{parsedColumn, rightColumn, operator, isNot, conjunction});
            return new Condition(parsedColumn, rightColumn, operator, conjunction, isNot);
        }

        Object conditionValue = parseConditionValue(parsedColumn, valueStr, columnType);
        LOGGER.log(Level.FINE, "Parsed condition: column={0}, operator={1}, value={2}, not={3}, conjunction={4}",
                new Object[]{parsedColumn, operator, conditionValue, isNot, conjunction});
        return new Condition(parsedColumn, conditionValue, operator, conjunction, isNot);
    }

    private List<String> splitInValues(String valuesPart) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < valuesPart.length(); i++) {
            char c = valuesPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentValue.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    currentValue.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentValue.append(c);
                    continue;
                } else if (c == ',' && parenDepth == 0) {
                    String value = currentValue.toString().trim();
                    if (!value.isEmpty()) {
                        values.add(value);
                    }
                    currentValue = new StringBuilder();
                    continue;
                }
            }
            currentValue.append(c);
        }

        String lastValue = currentValue.toString().trim();
        if (!lastValue.isEmpty()) {
            values.add(lastValue);
        }

        return values;
    }

    private String extractOriginalCondition(String originalQuery, String condition) {
        String normalizedQuery = originalQuery.trim();
        int whereIndex = normalizedQuery.toUpperCase().indexOf("WHERE");
        int onIndex = normalizedQuery.toUpperCase().indexOf(" ON ");
        int startIndex = whereIndex != -1 ? whereIndex + 5 : (onIndex != -1 ? onIndex + 4 : -1);
        if (startIndex == -1) {
            throw new IllegalArgumentException("No WHERE or ON clause found in query");
        }
        String originalClause = normalizedQuery.substring(startIndex).trim();

        String escapedCondition = Pattern.quote(condition.split("\\s+IN\\s+")[0].trim());
        Pattern conditionPattern = Pattern.compile(
                "\\b" + escapedCondition + "\\s+IN\\s*\\([^)]+\\)",
                Pattern.CASE_INSENSITIVE
        );
        Matcher matcher = conditionPattern.matcher(originalClause);
        if (matcher.find()) {
            return matcher.group();
        }

        LOGGER.log(Level.WARNING, "Condition not found in original query: {0}, clause: {1}", new Object[]{condition, originalClause});
        return condition;
    }

    private String normalizeColumnName(String column, String tableName) {
        if (column == null || column.isEmpty()) {
            return column;
        }
        String normalized = column.trim();
        if (tableName != null && !normalized.contains(".")) {
            normalized = tableName + "." + normalized;
        }
        return normalized;
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> columnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }
}