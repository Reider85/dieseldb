package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiFunction;
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
        validateInput();
        Map<String, Object> row = buildRow(table);
        insertRow(table, row);
        return null;
    }

    private void validateInput() {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Column and value counts mismatch");
        }
    }

    private Map<String, Object> buildRow(Table table) {
        Map<String, Object> row = new HashMap<>();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = values.get(i);
            Class<?> expectedType = columnTypes.get(column);
            if (expectedType == null) {
                throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
            }
            row.put(column, convertValue(value, column, expectedType));
        }
        return row;
    }

    private static final Map<Class<?>, BiFunction<Object, String, Object>> CONVERTERS;

    static {
        Map<Class<?>, BiFunction<Object, String, Object>> converters = new HashMap<>();
        converters.put(Integer.class, InsertQuery::parseInteger);
        converters.put(Long.class, InsertQuery::parseLong);
        converters.put(Short.class, InsertQuery::parseShort);
        converters.put(Byte.class, InsertQuery::parseByte);
        converters.put(BigDecimal.class, InsertQuery::parseBigDecimal);
        converters.put(Float.class, InsertQuery::parseFloat);
        converters.put(Double.class, InsertQuery::parseDouble);
        converters.put(Character.class, InsertQuery::parseCharacter);
        converters.put(UUID.class, InsertQuery::parseUUID);
        converters.put(String.class, (value, column) -> value.toString());
        CONVERTERS = Collections.unmodifiableMap(converters);
    }

    private Object convertValue(Object value, String column, Class<?> expectedType) {
        if (value == null) {
            return null;
        }
        if (expectedType.isInstance(value)) {
            return value;
        }
        BiFunction<Object, String, Object> converter = CONVERTERS.get(expectedType);
        if (converter != null) {
            return converter.apply(value, column);
        }
        if (expectedType == Boolean.class) {
            throwInvalid(value, column, "BOOLEAN");
        }
        if (expectedType == LocalDate.class) {
            throwInvalid(value, column, "DATE");
        }
        if (expectedType == LocalDateTime.class) {
            throwInvalid(value, column, "DATETIME or DATETIME_MS");
        }
        return value;
    }

    private static int parseInteger(Object value, String column) {
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
        }
    }

    private static long parseLong(Object value, String column) {
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected LONG", value, column));
        }
    }

    private static short parseShort(Object value, String column) {
        try {
            return Short.parseShort(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
        }
    }

    private static byte parseByte(Object value, String column) {
        try {
            return Byte.parseByte(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
        }
    }

    private static float parseFloat(Object value, String column) {
        try {
            return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected FLOAT", value, column));
        }
    }

    private static double parseDouble(Object value, String column) {
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected DOUBLE", value, column));
        }
    }

    private static BigDecimal parseBigDecimal(Object value, String column) {
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

    private static char parseCharacter(Object value, String column) {
        if (value instanceof Character c) {
            return c;
        }
        if (value.toString().length() != 1) {
            throw new IllegalArgumentException(
                    String.format("Invalid value '%s' for column %s: expected CHARACTER", value, column));
        }
        return value.toString().charAt(0);
    }

    private static UUID parseUUID(Object value, String column) {
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