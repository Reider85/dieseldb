package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

class QueryParser {
    private static final Logger LOGGER = Logger.getLogger(QueryParser.class.getName());
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN
    }

    static class Condition {
        String column;
        Object value;
        Operator operator;
        String conjunction;
        boolean not;
        List<Condition> subConditions;

        Condition(String column, Object value, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = value;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(List<Condition> subConditions, String conjunction, boolean not) {
            this.column = null;
            this.value = null;
            this.operator = null;
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
                        .map(Condition::toString)
                        .collect(Collectors.joining(" "));
                return (not ? "NOT " : "") + "(" + subCondStr + ")" + (conjunction != null ? " " + conjunction : "");
            }
            return (not ? "NOT " : "") + column + " " + operator + " " + value + (conjunction != null ? " " + conjunction : "");
        }
    }

    public Query<?> parse(String query) {
        try {
            String normalized = query.trim().toUpperCase();
            LOGGER.log(Level.INFO, "Normalized query: {0}", normalized);
            if (normalized.startsWith("SELECT")) {
                return parseSelectQuery(normalized, query);
            } else if (normalized.startsWith("INSERT INTO")) {
                return parseInsertQuery(normalized, query);
            } else if (normalized.startsWith("UPDATE")) {
                return parseUpdateQuery(normalized, query);
            } else if (normalized.startsWith("DELETE FROM")) {
                return parseDeleteQuery(normalized, query);
            } else if (normalized.startsWith("CREATE TABLE")) {
                return parseCreateTableQuery(normalized, query);
            } else if (normalized.startsWith("CREATE UNIQUE CLUSTERED INDEX")) {
                return parseCreateUniqueClusteredIndexQuery(normalized);
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

    private Query<Void> parseCreateUniqueClusteredIndexQuery(String normalized) {
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
        String[] parts = normalized.split("\\(");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid CREATE TABLE query format");
        }

        String tableName = parts[0].replace("CREATE TABLE", "").trim();
        String columnsPart = parts[1].replace(")", "").trim();
        String[] columnDefs = columnsPart.split(",");
        List<String> columns = new ArrayList<>();
        Map<String, Class<?>> columnTypes = new HashMap<>();

        for (String colDef : columnDefs) {
            String[] colParts = colDef.trim().split("\\s+");
            if (colParts.length != 2) {
                throw new IllegalArgumentException("Invalid column definition: " + colDef);
            }
            String colName = colParts[0];
            String type = colParts[1];
            columns.add(colName);
            switch (type.toUpperCase()) {
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

        LOGGER.log(Level.INFO, "Parsed CREATE TABLE query: table={0}, columns={1}, types={2}",
                new Object[]{tableName, columns, columnTypes});

        return new CreateTableQuery(tableName, columns, columnTypes);
    }

    private Query<List<Map<String, Object>>> parseSelectQuery(String normalized, String original) {
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid SELECT query format");
        }

        String selectPart = parts[0].replace("SELECT", "").trim();
        String[] selectColumns = selectPart.split(",");
        List<String> columns = Arrays.stream(selectColumns)
                .map(String::trim)
                .collect(Collectors.toList());

        String tableAndCondition = parts[1].trim();
        String tableName = tableAndCondition.split(" ")[0].trim();
        List<Condition> conditions = new ArrayList<>();

        if (tableAndCondition.contains("WHERE")) {
            String[] tableCondition = tableAndCondition.split("WHERE");
            if (tableCondition.length != 2) {
                throw new IllegalArgumentException("Invalid WHERE clause format");
            }
            String conditionStr = tableCondition[1].trim();
            LOGGER.log(Level.FINE, "Parsing WHERE clause: {0}", conditionStr);
            conditions = parseConditions(conditionStr);
        }

        LOGGER.log(Level.INFO, "Parsed SELECT query: columns={0}, table={1}, conditions={2}",
                new Object[]{columns, tableName, conditions});

        return new SelectQuery(columns, conditions);
    }

    private Query<Void> parseInsertQuery(String normalized, String original) {
        String[] parts = normalized.split("VALUES");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid INSERT query format");
        }

        String tableAndColumns = parts[0].replace("INSERT INTO", "").trim();
        String tableName = tableAndColumns.substring(0, tableAndColumns.indexOf("(")).trim();
        String columnsPart = tableAndColumns.substring(tableAndColumns.indexOf("(") + 1,
                tableAndColumns.indexOf(")")).trim();
        List<String> columns = Arrays.stream(columnsPart.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        String valuesPart = parts[1].trim();
        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            throw new IllegalArgumentException("Invalid VALUES syntax");
        }
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        String[] valueStrings = valuesPart.split(",");
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < valueStrings.length; i++) {
            String val = valueStrings[i].trim();
            String column = columns.get(i);
            if (val.startsWith("'") && val.endsWith("'")) {
                String strippedValue = val.substring(1, val.length() - 1);
                if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    try {
                        values.add(LocalDate.parse(strippedValue));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Invalid date format: " + strippedValue);
                    }
                } else if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}")) {
                    try {
                        values.add(LocalDateTime.parse(strippedValue, DATETIME_MS_FORMATTER));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Invalid datetime_ms format: " + strippedValue);
                    }
                } else if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                    try {
                        values.add(LocalDateTime.parse(strippedValue, DATETIME_FORMATTER));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Invalid datetime format: " + strippedValue);
                    }
                } else if (strippedValue.matches(UUID_PATTERN)) {
                    try {
                        values.add(UUID.fromString(strippedValue));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Invalid UUID format: " + strippedValue);
                    }
                } else if (strippedValue.length() == 1) {
                    values.add(strippedValue.charAt(0));
                } else {
                    values.add(strippedValue);
                }
            } else if (val.equalsIgnoreCase("TRUE") || val.equalsIgnoreCase("FALSE")) {
                values.add(Boolean.parseBoolean(val));
            } else if (val.contains(".")) {
                try {
                    BigDecimal bd = new BigDecimal(val);
                    if (bd.abs().compareTo(new BigDecimal(Float.MAX_VALUE)) <= 0 &&
                            bd.abs().compareTo(new BigDecimal(Float.MIN_VALUE)) >= 0) {
                        values.add(bd.floatValue());
                    } else if (bd.abs().compareTo(new BigDecimal(Double.MAX_VALUE)) <= 0 &&
                            bd.abs().compareTo(new BigDecimal(Double.MIN_VALUE)) >= 0) {
                        values.add(bd.doubleValue());
                    } else {
                        values.add(bd);
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value: " + val);
                }
            } else {
                try {
                    if (column.equals("AGE")) {
                        values.add(Integer.parseInt(val));
                    } else {
                        long parsedLong = Long.parseLong(val);
                        if (parsedLong >= Byte.MIN_VALUE && parsedLong <= Byte.MAX_VALUE) {
                            values.add((byte) parsedLong);
                        } else if (parsedLong >= Short.MIN_VALUE && parsedLong <= Short.MAX_VALUE) {
                            values.add((short) parsedLong);
                        } else if (parsedLong >= Integer.MIN_VALUE && parsedLong <= Integer.MAX_VALUE) {
                            values.add((int) parsedLong);
                        } else {
                            values.add(parsedLong);
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value: " + val);
                }
            }
        }

        LOGGER.log(Level.INFO, "Parsed INSERT query: table={0}, columns={1}, values={2}",
                new Object[]{tableName, columns, values});

        return new InsertQuery(columns, values);
    }

    private Query<Void> parseUpdateQuery(String normalized, String original) {
        String[] parts = normalized.split("SET");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid UPDATE query format");
        }

        String tablePart = parts[0].replace("UPDATE", "").trim();
        String tableName = tablePart.split(" ")[0].trim();

        String setAndWhere = parts[1].trim();
        String setPart;
        List<Condition> conditions = new ArrayList<>();

        if (setAndWhere.contains("WHERE")) {
            String[] setWhereParts = setAndWhere.split("WHERE");
            setPart = setWhereParts[0].trim();
            String conditionStr = setWhereParts[1].trim();
            conditions = parseConditions(conditionStr);
        } else {
            setPart = setAndWhere;
        }

        String[] assignments = setPart.split(",");
        Map<String, Object> updates = new HashMap<>();
        for (String assignment : assignments) {
            String[] kv = assignment.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid SET clause");
            }
            String column = kv[0].trim();
            String valueStr = kv[1].trim();
            Object value = parseConditionValue(column, valueStr);
            updates.put(column, value);
        }

        LOGGER.log(Level.INFO, "Parsed UPDATE query: table={0}, updates={1}, conditions={2}",
                new Object[]{tableName, updates, conditions});

        return new UpdateQuery(updates, conditions);
    }

    private Query<Void> parseDeleteQuery(String normalized, String original) {
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
        List<QueryParser.Condition> conditions = new ArrayList<>();

        if (whereParts.length == 2) {
            String conditionStr = whereParts[1].trim();
            LOGGER.log(Level.FINE, "Parsing WHERE clause for DELETE: {0}", conditionStr);
            if (conditionStr.isEmpty()) {
                LOGGER.log(Level.SEVERE, "Empty WHERE clause in DELETE query: {0}", normalized);
                throw new IllegalArgumentException("Invalid DELETE query: empty WHERE clause");
            }
            conditions = parseConditions(conditionStr);
        } else {
            LOGGER.log(Level.FINE, "No WHERE clause in DELETE query");
        }

        LOGGER.log(Level.INFO, "Parsed DELETE query: table={0}, conditions={1}",
                new Object[]{tableName, conditions});

        return new DeleteQuery(conditions);
    }

    private Object parseConditionValue(String conditionColumn, String valueStr) {
        try {
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                String strippedValue = valueStr.substring(1, valueStr.length() - 1);
                if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return LocalDate.parse(strippedValue);
                } else if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_MS_FORMATTER);
                } else if (strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_FORMATTER);
                } else if (strippedValue.matches(UUID_PATTERN)) {
                    return UUID.fromString(strippedValue);
                } else if (strippedValue.length() == 1) {
                    return strippedValue.charAt(0);
                } else {
                    return strippedValue;
                }
            } else if (valueStr.equalsIgnoreCase("TRUE") || valueStr.equalsIgnoreCase("FALSE")) {
                return Boolean.parseBoolean(valueStr);
            } else if (valueStr.contains(".")) {
                BigDecimal bd = new BigDecimal(valueStr);
                if (bd.abs().compareTo(new BigDecimal(Float.MAX_VALUE)) <= 0 &&
                        bd.abs().compareTo(new BigDecimal(Float.MIN_VALUE)) >= 0) {
                    return bd.floatValue();
                } else if (bd.abs().compareTo(new BigDecimal(Double.MAX_VALUE)) <= 0 &&
                        bd.abs().compareTo(new BigDecimal(Double.MIN_VALUE)) >= 0) {
                    return bd.doubleValue();
                } else {
                    return bd;
                }
            } else {
                if (conditionColumn.equalsIgnoreCase("BALANCE")) {
                    return new BigDecimal(valueStr); // Handle BALANCE as BigDecimal
                } else if (conditionColumn.equalsIgnoreCase("AGE")) {
                    return Integer.parseInt(valueStr);
                }
                long parsedLong = Long.parseLong(valueStr);
                if (parsedLong >= Byte.MIN_VALUE && parsedLong <= Byte.MAX_VALUE) {
                    return (byte) parsedLong;
                } else if (parsedLong >= Short.MIN_VALUE && parsedLong <= Short.MAX_VALUE) {
                    return (short) parsedLong;
                } else if (parsedLong >= Integer.MIN_VALUE && parsedLong <= Integer.MAX_VALUE) {
                    return (int) parsedLong;
                } else {
                    return parsedLong;
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value: " + valueStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value format: " + valueStr);
        }
    }

    private List<Condition> parseConditions(String conditionStr) {
        List<Condition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        LOGGER.log(Level.FINE, "Processing condition string: {0}", conditionStr);

        for (int i = 0; i < conditionStr.length(); i++) {
            char c = conditionStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    if (parenDepth == 1) {
                        continue;
                    }
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0) {
                        String subConditionStr = currentCondition.toString().trim();
                        if (!subConditionStr.isEmpty()) {
                            boolean isNot = subConditionStr.startsWith("NOT ");
                            if (isNot) {
                                subConditionStr = subConditionStr.substring(4).trim();
                            }
                            List<Condition> subConditions = parseConditions(subConditionStr);
                            String conjunction = determineConjunctionAfter(conditionStr, i);
                            conditions.add(new Condition(subConditions, conjunction, isNot));
                        }
                        currentCondition = new StringBuilder();
                        continue;
                    }
                } else if (parenDepth == 0) {
                    if (i + 3 <= conditionStr.length() && conditionStr.substring(i, i + 3).equalsIgnoreCase("AND") &&
                            (i == 0 || Character.isWhitespace(conditionStr.charAt(i - 1))) &&
                            (i + 3 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 3)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "AND"));
                            LOGGER.log(Level.FINE, "Added condition with AND: {0}", current);
                        }
                        currentCondition = new StringBuilder();
                        i += 2;
                        continue;
                    } else if (i + 2 <= conditionStr.length() && conditionStr.substring(i, i + 2).equalsIgnoreCase("OR") &&
                            (i == 0 || Character.isWhitespace(conditionStr.charAt(i - 1))) &&
                            (i + 2 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 2)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "OR"));
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
                conditions.add(parseSingleCondition(current, null));
                LOGGER.log(Level.FINE, "Added final condition: {0}", current);
            }
        }

        if (parenDepth != 0) {
            throw new IllegalArgumentException("Mismatched parentheses in WHERE clause");
        }

        LOGGER.log(Level.FINE, "Parsed conditions: {0}", conditions);

        return conditions;
    }

    private Condition parseSingleCondition(String condition, String conjunction) {
        LOGGER.log(Level.FINE, "Parsing condition: {0}", condition);
        boolean isNot = condition.startsWith("NOT ");
        if (isNot) {
            condition = condition.substring(4).trim();
        }

        String[] partsByOperator = null;
        Operator operator = null;

        // Prioritize operators to avoid splitting on the wrong one
        if (condition.contains("!=")) {
            partsByOperator = condition.split("\\s*!=\\s*", 2);
            operator = Operator.NOT_EQUALS;
        } else if (condition.contains("=")) {
            partsByOperator = condition.split("\\s*=\\s*", 2);
            operator = Operator.EQUALS;
        } else if (condition.contains("<")) {
            partsByOperator = condition.split("\\s*<\\s*", 2);
            operator = Operator.LESS_THAN;
        } else if (condition.contains(">")) {
            partsByOperator = condition.split("\\s*>\\s*", 2);
            operator = Operator.GREATER_THAN;
        }

        if (partsByOperator == null || partsByOperator.length != 2) {
            LOGGER.log(Level.SEVERE, "Invalid condition format: {0}, parts: {1}",
                    new Object[]{condition, partsByOperator == null ? "null" : Arrays.toString(partsByOperator)});
            throw new IllegalArgumentException("Invalid WHERE clause: must contain =, !=, <, or > with valid column and value");
        }

        String conditionColumn = partsByOperator[0].trim();
        String valueStr = partsByOperator[1].trim();
        if (conditionColumn.isEmpty() || valueStr.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Invalid WHERE clause: column or value is empty in condition: {0}", condition);
            throw new IllegalArgumentException("Invalid WHERE clause: column or value is empty in condition: " + condition);
        }
        Object conditionValue = parseConditionValue(conditionColumn, valueStr);
        LOGGER.log(Level.FINE, "Parsed condition: column={0}, operator={1}, value={2}, not={3}, conjunction={4}",
                new Object[]{conditionColumn, operator, conditionValue, isNot, conjunction});
        return new Condition(conditionColumn, conditionValue, operator, conjunction, isNot);
    }

    private String determineConjunctionAfter(String conditionStr, int index) {
        int i = index + 1;
        while (i < conditionStr.length() && Character.isWhitespace(conditionStr.charAt(i))) {
            i++;
        }
        if (i + 3 <= conditionStr.length() && conditionStr.substring(i, i + 3).equalsIgnoreCase("AND") &&
                (i + 3 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 3)))) {
            return "AND";
        } else if (i + 2 <= conditionStr.length() && conditionStr.substring(i, i + 2).equalsIgnoreCase("OR") &&
                (i + 2 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 2)))) {
            return "OR";
        }
        return null;
    }
}