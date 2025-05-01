package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class InsertQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(InsertQuery.class.getName());
    private final List<String> columns;
    private final List<Object> values;

    public InsertQuery(List<String> columns, List<Object> values) {
        this.columns = columns;
        this.values = values;
        LOGGER.log(Level.FINE, "Created InsertQuery with columns: {0}, values: {1}", new Object[]{columns, values});
    }

    @Override
    public Void execute(Table table) {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Column and value counts mismatch");
        }
        Map<String, Object> row = new HashMap<>();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = values.get(i);
            Class<?> expectedType = columnTypes.get(column);
            if (expectedType == null) {
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            if (expectedType == Integer.class && !(value instanceof Integer)) {
                try {
                    value = Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
                }
            } else if (expectedType == Long.class && !(value instanceof Long)) {
                try {
                    value = Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected LONG", value, column));
                }
            } else if (expectedType == Short.class && !(value instanceof Short)) {
                try {
                    value = Short.parseShort(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
                }
            } else if (expectedType == Byte.class && !(value instanceof Byte)) {
                try {
                    value = Byte.parseByte(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
                }
            } else if (expectedType == BigDecimal.class && !(value instanceof BigDecimal)) {
                try {
                    value = new BigDecimal(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected BIGDECIMAL", value, column));
                }
            } else if (expectedType == Float.class && !(value instanceof Float)) {
                try {
                    value = Float.parseFloat(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected FLOAT", value, column));
                }
            } else if (expectedType == Double.class && !(value instanceof Double)) {
                try {
                    value = Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected DOUBLE", value, column));
                }
            } else if (expectedType == Character.class && !(value instanceof Character)) {
                try {
                    if (value.toString().length() == 1) {
                        value = value.toString().charAt(0);
                    } else {
                        throw new IllegalArgumentException("Expected single character");
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected CHAR", value, column));
                }
            } else if (expectedType == UUID.class && !(value instanceof UUID)) {
                try {
                    value = UUID.fromString(value.toString());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected UUID", value, column));
                }
            } else if (expectedType == String.class && !(value instanceof String)) {
                value = value.toString();
            } else if (expectedType == Boolean.class && !(value instanceof Boolean)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected BOOLEAN", value, column));
            } else if (expectedType == LocalDate.class && !(value instanceof LocalDate)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected DATE", value, column));
            } else if (expectedType == LocalDateTime.class && !(value instanceof LocalDateTime)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected DATETIME or DATETIME_MS", value, column));
            }
            row.put(column, value);
        }
        try {
            table.addRow(row);
            LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{table.getName(), row});
        } catch (IllegalStateException e) {
            LOGGER.log(Level.SEVERE, "Insert failed due to unique constraint violation: {0}", e.getMessage());
            throw new IllegalStateException("Insert failed: " + e.getMessage(), e);
        }
        return null;
    }
}