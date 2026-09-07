package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Executes an INSERT INTO statement: converts the raw values to the column
 * types and adds the resulting row to the table.
 *
 * @see Query
 */
class InsertQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(InsertQuery.class.getName());
    private final List<String> columns;
    private final List<Object> values;
    private long lastAffectedRows;

    /**
     * Returns the columns being inserted into.
     *
     * @return the unmodifiable column list
     */
    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Returns the number of rows the last {@link #execute} inserted (always 1
     * on success), exposed for EXPLAIN ANALYZE metrics.
     *
     * @return the affected row count of the last execution
     */
    long getLastAffectedRows() {
        return lastAffectedRows;
    }

    /**
     * Creates an insert query for the given columns and values.
     *
     * @param columns the column names being inserted into
     * @param values  the values, one per column
     */
    public InsertQuery(List<String> columns, List<Object> values) {
        this.columns = columns;
        this.values = values;
        LOGGER.log(Level.FINE, "Created InsertQuery with columns: {0}, values: {1}", new Object[]{columns, values});
    }

    /**
     * Converts and validates each value against the table's column types and
     * inserts the row.
     *
     * @param table the table to insert into
     * @return null on success
     * @throws IllegalArgumentException if the column/value counts mismatch or
     *                                  a value is invalid for its column type
     * @throws IllegalStateException    if a unique constraint is violated
     */
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
                throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
            }
            if (value == null) {
                row.put(column, null);
                continue;
            }
            value = convertValue(value, column, expectedType);
            row.put(column, value);
        }
        insertRow(table, row);
        return null;
    }

    private Object convertValue(Object value, String column, Class<?> expectedType) {
        if (value == null) {
            return null;
        }
        if (expectedType == Integer.class && !(value instanceof Integer)) {
            return parseInteger(value, column);
        }
        if (expectedType == Long.class && !(value instanceof Long)) {
            return parseLong(value, column);
        }
        if (expectedType == Short.class && !(value instanceof Short)) {
            return parseShort(value, column);
        }
        if (expectedType == Byte.class && !(value instanceof Byte)) {
            return parseByte(value, column);
        }
        if (expectedType == BigDecimal.class && !(value instanceof BigDecimal)) {
            return parseBigDecimal(value, column);
        }
        if (expectedType == Float.class && !(value instanceof Float)) {
            return parseFloat(value, column);
        }
        if (expectedType == Double.class && !(value instanceof Double)) {
            return parseDouble(value, column);
        }
        if (expectedType == Character.class && !(value instanceof Character)) {
            return parseCharacter(value, column);
        }
        if (expectedType == UUID.class && !(value instanceof UUID)) {
            return parseUUID(value, column);
        }
        if (expectedType == String.class && !(value instanceof String)) {
            return value.toString();
        }
        if (expectedType == Boolean.class && !(value instanceof Boolean)) {
            throwInvalid(value, column, "BOOLEAN");
        }
        if (expectedType == LocalDate.class && !(value instanceof LocalDate)) {
            throwInvalid(value, column, "DATE");
        }
        if (expectedType == LocalDateTime.class && !(value instanceof LocalDateTime)) {
            throwInvalid(value, column, "DATETIME or DATETIME_MS");
        }
        return value;
    }

    private int parseInteger(Object value, String column) {
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
        }
    }

    private long parseLong(Object value, String column) {
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected LONG", value, column));
        }
    }

    private short parseShort(Object value, String column) {
        try {
            return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
        }
    }

    private byte parseByte(Object value, String column) {
        try {
            return Byte.parseByte(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
        }
    }

    private float parseFloat(Object value, String column) {
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected FLOAT", value, column));
        }
    }

    private double parseDouble(Object value, String column) {
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected DOUBLE", value, column));
        }
    }

    private BigDecimal parseBigDecimal(Object value, String column) {
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        try {
            return new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected BIGDECIMAL", value, column));
        }
    }

    private char parseCharacter(Object value, String column) {
        if (value instanceof Character c) {
            return c;
        }
        if (value.toString().length() != 1) {
            throw new IllegalArgumentException("Expected single character");
        }
        return value.toString().charAt(0);
    }

    private UUID parseUUID(Object value, String column) {
        if (value instanceof UUID uuid) {
            return uuid;
        }
        try {
            return UUID.fromString(value.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected UUID", value, column));
        }
    }

    private Object throwInvalid(Object value, String column, String typeName) {
        throw new IllegalArgumentException(
                String.format("Invalid value '%s' for column %s: expected %s", value, column, typeName));
    }

    private void insertRow(Table table, Map<String, Object> row) {
        try {
            table.addRow(row);
            lastAffectedRows = 1;
            LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{table.getName(), row});
        } catch (IllegalStateException e) {
            LOGGER.log(Level.SEVERE, "Insert failed due to unique constraint violation: {0}", e.getMessage());
            throw new IllegalStateException("Insert failed: " + e.getMessage(), e);
        }
    }
}