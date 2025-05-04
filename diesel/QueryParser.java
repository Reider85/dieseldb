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
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, IN, LIKE, NOT_LIKE
    }

    static class Condition {
        String column;
        Object value;
        List<Object> inValues;
        Operator operator;
        String conjunction;
        boolean not;
        List<Condition> subConditions;

        Condition(String column, Object value, Operator operator, String conjunction, boolean not) {
            this.column = column;
            this.value = value;
            this.inValues = null;
            this.operator = operator;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(String column, List<Object> inValues, String conjunction, boolean not) {
            this.column = column;
            this.value = null;
            this.inValues = inValues;
            this.operator = Operator.IN;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = null;
        }

        Condition(List<Condition> subConditions, String conjunction, boolean not) {
            this.column = null;
            this.value = null;
            this.inValues = null;
            this.operator = null;
            this.conjunction = conjunction;
            this.not = not;
            this.subConditions = subConditions;
        }

        boolean isGrouped() {
            return subConditions != null;
        }

        boolean isInOperator() {
            return operator == Operator.IN;
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
            String operatorStr = switch (operator) {
                case LIKE -> "LIKE";
                case NOT_LIKE -> "NOT LIKE";
                default -> operator.toString();
            };
            return (not ? "NOT " : "") + column + " " + operatorStr + " " + (value instanceof String ? "'" + value + "'" : value) + (conjunction != null ? " " + conjunction : "");
        }
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
        List<String> columns = Arrays.stream(selectPartOriginal.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        String tableAndCondition = parts[1].trim();
        String tableName = tableAndCondition.split(" ")[0].trim();
        List<Condition> conditions = new ArrayList<>();

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        LOGGER.log(Level.FINE, "Table {0} columns: {1}, column types: {2}",
                new Object[]{tableName, table.getColumns(), table.getColumnTypes()});

        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        if (columnTypes.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Table {0} has no columns defined", tableName);
            throw new IllegalStateException("Table " + tableName + " has no columns defined");
        }
        for (String column : columns) {
            if (!columnTypes.containsKey(column)) {
                LOGGER.log(Level.SEVERE, "Unknown column in SELECT: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
        }

        if (tableAndCondition.contains("WHERE")) {
            String[] tableCondition = tableAndCondition.split("WHERE");
            if (tableCondition.length != 2) {
                throw new IllegalArgumentException("Invalid WHERE clause format");
            }
            String conditionStr = tableCondition[1].trim();
            LOGGER.log(Level.FINE, "Parsing WHERE clause: {0}", conditionStr);
            conditions = parseConditions(conditionStr, tableName, database, original);
        }

        LOGGER.log(Level.INFO, "Parsed SELECT query: columns={0}, table={1}, conditions={2}",
                new Object[]{columns, tableName, conditions});

        return new SelectQuery(columns, conditions);
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
            conditions = parseConditions(conditionStr, tableName, database, original);
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
            conditions = parseConditions(conditionStr, tableName, database, original);
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
                        if (bd.abs().compareTo(new BigDecimal(Float.MAX_VALUE)) <= 0 &&
                                bd.abs().compareTo(new BigDecimal(Float.MIN_VALUE)) >= 0) {
                            return bd.floatValue();
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Float");
                        }
                    } else if (columnType == Double.class) {
                        BigDecimal bd = new BigDecimal(valueStr);
                        if (bd.abs().compareTo(new BigDecimal(Double.MAX_VALUE)) <= 0 &&
                                bd.abs().compareTo(new BigDecimal(Double.MIN_VALUE)) >= 0) {
                            return bd.doubleValue();
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Double");
                        }
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

    private List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        LOGGER.log(Level.FINE, "Parsing conditions for table {0}, column types: {1}",
                new Object[]{tableName, columnTypes});

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
                            List<Condition> subConditions = parseConditions(subConditionStr, tableName, database, originalQuery);
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
                            conditions.add(parseSingleCondition(current, "AND", columnTypes, originalQuery));
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
                            conditions.add(parseSingleCondition(current, "OR", columnTypes, originalQuery));
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
                conditions.add(parseSingleCondition(current, null, columnTypes, originalQuery));
                LOGGER.log(Level.FINE, "Added final condition: {0}", current);
            }
        }

        if (parenDepth != 0) {
            throw new IllegalArgumentException("Mismatched parentheses in WHERE clause");
        }

        LOGGER.log(Level.FINE, "Parsed conditions: {0}", conditions);

        return conditions;
    }

    private Condition parseSingleCondition(String condition, String conjunction, Map<String, Class<?>> columnTypes, String originalQuery) {
        LOGGER.log(Level.FINE, "Parsing condition: {0}", condition);
        boolean isNot = condition.toUpperCase().startsWith("NOT ");
        String conditionWithoutNot = isNot ? condition.substring(4).trim() : condition;

        if (conditionWithoutNot.toUpperCase().contains(" IN ")) {
            String originalCondition = extractOriginalCondition(originalQuery, condition);
            String[] parts = originalCondition.split("\\s+IN\\s+", 2);
            if (parts.length != 2) {
                LOGGER.log(Level.SEVERE, "Invalid IN condition format: {0}", condition);
                throw new IllegalArgumentException("Invalid IN clause: " + condition);
            }

            String conditionColumn = parts[0].trim();
            String valuesPart = parts[1].trim();

            if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
                throw new IllegalArgumentException("Invalid IN clause: values must be enclosed in parentheses");
            }
            valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();

            if (valuesPart.isEmpty()) {
                throw new IllegalArgumentException("IN clause cannot be empty");
            }

            List<String> valueStrings = splitInValues(valuesPart);
            LOGGER.log(Level.FINE, "Split IN values: {0}", Arrays.toString(valueStrings.toArray()));

            List<Object> inValues = new ArrayList<>();
            Class<?> columnType = columnTypes.get(conditionColumn);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                        new Object[]{conditionColumn, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + conditionColumn);
            }

            for (String val : valueStrings) {
                String trimmedVal = val.trim();
                if (trimmedVal.isEmpty()) continue;
                inValues.add(parseConditionValue(conditionColumn, trimmedVal, columnType));
            }

            LOGGER.log(Level.FINE, "Parsed {0} condition: column={1}, values={2}, not={3}, conjunction={4}",
                    new Object[]{isNot ? "NOT IN" : "IN", conditionColumn, inValues, isNot, conjunction});
            return new Condition(conditionColumn, inValues, conjunction, isNot);
        }

        String[] partsByOperator = null;
        Operator operator = null;

        if (conditionWithoutNot.toUpperCase().contains(" NOT LIKE ")) {
            partsByOperator = conditionWithoutNot.split("\\s*NOT LIKE\\s*", 2);
            operator = Operator.NOT_LIKE;
        } else if (conditionWithoutNot.toUpperCase().contains(" LIKE ")) {
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

        if (partsByOperator == null || partsByOperator.length != 2) {
            LOGGER.log(Level.SEVERE, "Invalid condition format: {0}, parts: {1}",
                    new Object[]{condition, partsByOperator == null ? "null" : Arrays.toString(partsByOperator)});
            throw new IllegalArgumentException("Invalid WHERE clause: must contain =, !=, <, >, IN, NOT IN, LIKE, or NOT LIKE with valid column and value");
        }

        String conditionColumn = partsByOperator[0].trim();
        String valueStr = partsByOperator[1].trim();
        if (conditionColumn.isEmpty() || valueStr.isEmpty()) {
            LOGGER.log(Level.SEVERE, "Invalid WHERE clause: column or value is empty in condition: {0}", condition);
            throw new IllegalArgumentException("Invalid WHERE clause: column or value is empty in condition: " + condition);
        }
        Class<?> columnType = columnTypes.get(conditionColumn);
        if (columnType == null) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{conditionColumn, columnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + conditionColumn);
        }
        if ((operator == Operator.LIKE || operator == Operator.NOT_LIKE) && columnType != String.class) {
            throw new IllegalArgumentException("LIKE and NOT LIKE operators are only supported for String columns: " + conditionColumn);
        }
        Object conditionValue = parseConditionValue(conditionColumn, valueStr, columnType);
        LOGGER.log(Level.FINE, "Parsed condition: column={0}, operator={1}, value={2}, not={3}, conjunction={4}",
                new Object[]{conditionColumn, operator, conditionValue, isNot, conjunction});
        return new Condition(conditionColumn, conditionValue, operator, conjunction, isNot);
    }

    private List<String> splitInValues(String valuesPart) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < valuesPart.length(); i++) {
            char c = valuesPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentValue.append(c);
                continue;
            }
            if (!inQuotes && c == ',') {
                String value = currentValue.toString().trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
                currentValue = new StringBuilder();
                continue;
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
        String normalizedQuery = originalQuery.trim().toUpperCase();
        int whereIndex = normalizedQuery.indexOf("WHERE");
        if (whereIndex == -1) {
            throw new IllegalArgumentException("No WHERE clause found in query");
        }
        String originalWhereClause = originalQuery.substring(whereIndex + 5).trim();

        StringBuilder conditionBuilder = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        boolean foundIn = false;

        for (int i = 0; i < originalWhereClause.length(); i++) {
            char c = originalWhereClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                conditionBuilder.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    conditionBuilder.append(c);
                    continue;
                }
                if (c == ')') {
                    parenDepth--;
                    conditionBuilder.append(c);
                    if (parenDepth == 0 && foundIn) {
                        return conditionBuilder.toString().trim();
                    }
                    continue;
                }
                if (parenDepth == 0 && !foundIn && i + 2 <= originalWhereClause.length() &&
                        originalWhereClause.substring(i, i + 2).equalsIgnoreCase("IN")) {
                    foundIn = true;
                    conditionBuilder.append(originalWhereClause.substring(i, i + 2));
                    i += 1;
                    continue;
                }
                if (parenDepth == 0 && foundIn && i + 3 <= originalWhereClause.length() &&
                        originalWhereClause.substring(i, i + 3).equalsIgnoreCase("AND") &&
                        (i == 0 || Character.isWhitespace(originalWhereClause.charAt(i - 1))) &&
                        (i + 3 == originalWhereClause.length() || Character.isWhitespace(originalWhereClause.charAt(i + 3)))) {
                    return conditionBuilder.toString().trim();
                }
                if (parenDepth == 0 && foundIn && i + 2 <= originalWhereClause.length() &&
                        originalWhereClause.substring(i, i + 2).equalsIgnoreCase("OR") &&
                        (i == 0 || Character.isWhitespace(originalWhereClause.charAt(i - 1))) &&
                        (i + 2 == originalWhereClause.length() || Character.isWhitespace(originalWhereClause.charAt(i + 2)))) {
                    return conditionBuilder.toString().trim();
                }
            }
            conditionBuilder.append(c);
        }

        if (foundIn) {
            return conditionBuilder.toString().trim();
        }

        LOGGER.log(Level.WARNING, "Could not precisely extract condition for: {0}, returning full WHERE clause", condition);
        return originalWhereClause;
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