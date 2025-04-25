package diesel;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class UpdateQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(UpdateQuery.class.getName());
    private final Map<String, Object> updates;
    private final String conditionColumn;
    private final Object conditionValue;
    private final QueryParser.Operator operator;

    public UpdateQuery(Map<String, Object> updates, String conditionColumn, Object conditionValue, QueryParser.Operator operator) {
        this.updates = updates;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
        this.operator = operator;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, Object> validatedUpdates = new HashMap<>();

        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String column = entry.getKey();
            Object value = entry.getValue();
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
            validatedUpdates.put(column, value);
        }

        for (Map<String, Object> row : rows) {
            if (conditionColumn == null) {
                validatedUpdates.forEach(row::put);
                continue;
            }
            Object rowValue = row.get(conditionColumn);
            if (rowValue == null) {
                continue;
            }

            LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}",
                    new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                            conditionValue, conditionValue.getClass().getSimpleName(), operator});

            // Handle EQUALS and NOT_EQUALS
            if (operator == QueryParser.Operator.EQUALS || operator == QueryParser.Operator.NOT_EQUALS) {
                boolean isEqual = String.valueOf(rowValue).equals(String.valueOf(conditionValue));
                if (operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual) {
                    validatedUpdates.forEach(row::put);
                }
                continue;
            }

            // Handle LESS_THAN and GREATER_THAN
            if (!(rowValue instanceof Comparable) || !(conditionValue instanceof Comparable)) {
                LOGGER.log(Level.WARNING, "Comparison operators < or > not supported for types: rowValue={0}, conditionValue={1}",
                        new Object[]{rowValue.getClass().getSimpleName(), conditionValue.getClass().getSimpleName()});
                throw new IllegalArgumentException("Comparison operators < or > only supported for numeric types or dates");
            }

            @SuppressWarnings("unchecked")
            Comparable<Object> rowComparable = (Comparable<Object>) rowValue;
            @SuppressWarnings("unchecked")
            Comparable<Object> conditionComparable = (Comparable<Object>) conditionValue;

            if (rowValue.getClass() != conditionValue.getClass()) {
                LOGGER.log(Level.WARNING, "Type mismatch in comparison: rowValue={0}, conditionValue={1}",
                        new Object[]{rowValue.getClass().getSimpleName(), conditionValue.getClass().getSimpleName()});
                throw new IllegalArgumentException("Type mismatch in comparison");
            }

            int comparison = rowComparable.compareTo(conditionValue);
            if (operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0) {
                validatedUpdates.forEach(row::put);
            }
        }
        return null;
    }
}