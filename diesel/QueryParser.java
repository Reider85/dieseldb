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

        Condition(String column, Object value, Operator operator) {
            this.column = column;
            this.value = value;
            this.operator = operator;
        }

        @Override
        public String toString() {
            return column + " " + operator + " " + value;
        }
    }

    public Query<?> parse(String query) {
        try {
            String normalized = query.trim().toUpperCase();
            if (normalized.startsWith("SELECT")) {
                return parseSelectQuery(normalized, query);
            } else if (normalized.startsWith("INSERT INTO")) {
                return parseInsertQuery(normalized, query);
            } else if (normalized.startsWith("UPDATE")) {
                return parseUpdateQuery(normalized, query);
            } else if (normalized.startsWith("CREATE TABLE")) {
                return parseCreateTableQuery(normalized, query);
            }
            throw new IllegalArgumentException("Unsupported query type");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
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
            String conditionStr = tableCondition[1].trim();
            String[] conditionParts = conditionStr.split("\\s+AND\\s+");

            for (String condition : conditionParts) {
                String[] partsByOperator;
                Operator operator;
                if (condition.contains("!=")) {
                    partsByOperator = condition.split("!=");
                    operator = Operator.NOT_EQUALS;
                } else if (condition.contains("<")) {
                    partsByOperator = condition.split("<");
                    operator = Operator.LESS_THAN;
                } else if (condition.contains(">")) {
                    partsByOperator = condition.split(">");
                    operator = Operator.GREATER_THAN;
                } else if (condition.contains("=")) {
                    partsByOperator = condition.split("=");
                    operator = Operator.EQUALS;
                } else {
                    throw new IllegalArgumentException("Invalid WHERE clause: must contain =, !=, <, or >");
                }

                if (partsByOperator.length != 2) {
                    throw new IllegalArgumentException("Invalid WHERE clause");
                }

                String conditionColumn = partsByOperator[0].trim();
                String valueStr = partsByOperator[1].trim();
                Object conditionValue = parseConditionValue(conditionColumn, valueStr);
                conditions.add(new Condition(conditionColumn, conditionValue, operator));
            }
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
            String[] conditionParts = conditionStr.split("\\s+AND\\s+");

            for (String condition : conditionParts) {
                String[] partsByOperator;
                Operator operator;
                if (condition.contains("!=")) {
                    partsByOperator = condition.split("!=");
                    operator = Operator.NOT_EQUALS;
                } else if (condition.contains("<")) {
                    partsByOperator = condition.split("<");
                    operator = Operator.LESS_THAN;
                } else if (condition.contains(">")) {
                    partsByOperator = condition.split(">");
                    operator = Operator.GREATER_THAN;
                } else if (condition.contains("=")) {
                    partsByOperator = condition.split("=");
                    operator = Operator.EQUALS;
                } else {
                    throw new IllegalArgumentException("Invalid WHERE clause: must contain =, !=, <, or >");
                }

                if (partsByOperator.length != 2) {
                    throw new IllegalArgumentException("Invalid WHERE clause");
                }

                String conditionColumn = partsByOperator[0].trim();
                String valueStr = partsByOperator[1].trim();
                Object conditionValue = parseConditionValue(conditionColumn, valueStr);
                conditions.add(new Condition(conditionColumn, conditionValue, operator));
            }
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
                if (conditionColumn.equals("AGE")) {
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
}