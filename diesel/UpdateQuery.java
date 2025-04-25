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
    private final List<QueryParser.Condition> conditions;

    public UpdateQuery(Map<String, Object> updates, List<QueryParser.Condition> conditions) {
        this.updates = updates;
        this.conditions = conditions;
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
                value = Integer.parseInt(value.toString());
            } else if (expectedType == Long.class && !(value instanceof Long)) {
                value = Long.parseLong(value.toString());
            } else if (expectedType == Short.class && !(value instanceof Short)) {
                value = Short.parseShort(value.toString());
            } else if (expectedType == Byte.class && !(value instanceof Byte)) {
                value = Byte.parseByte(value.toString());
            } else if (expectedType == BigDecimal.class && !(value instanceof BigDecimal)) {
                value = new BigDecimal(value.toString());
            } else if (expectedType == Float.class && !(value instanceof Float)) {
                value = Float.parseFloat(value.toString());
            } else if (expectedType == Double.class && !(value instanceof Double)) {
                value = Double.parseDouble(value.toString());
            } else if (expectedType == Character.class && !(value instanceof Character)) {
                if (value.toString().length() == 1) {
                    value = value.toString().charAt(0);
                } else {
                    throw new IllegalArgumentException("Expected single character");
                }
            } else if (expectedType == UUID.class && !(value instanceof UUID)) {
                value = UUID.fromString(value.toString());
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
            if (conditions.isEmpty() || conditions.stream().allMatch(condition -> {
                Object rowValue = row.get(condition.column);
                if (rowValue == null) {
                    return false;
                }

                LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}",
                        new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                                condition.value, condition.value.getClass().getSimpleName(), condition.operator});

                if (condition.operator == QueryParser.Operator.EQUALS || condition.operator == QueryParser.Operator.NOT_EQUALS) {
                    boolean isEqual = String.valueOf(rowValue).equals(String.valueOf(condition.value));
                    return condition.operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
                }

                if (!(rowValue instanceof Comparable) || !(condition.value instanceof Comparable)) {
                    LOGGER.log(Level.WARNING, "Comparison operators < or > not supported for types: rowValue={0}, conditionValue={1}",
                            new Object[]{rowValue.getClass().getSimpleName(), condition.value.getClass().getSimpleName()});
                    throw new IllegalArgumentException("Comparison operators < or > only supported for numeric types or dates");
                }

                @SuppressWarnings("unchecked")
                Comparable<Object> rowComparable = (Comparable<Object>) rowValue;
                @SuppressWarnings("unchecked")
                Comparable<Object> conditionComparable = (Comparable<Object>) condition.value;

                if (rowValue.getClass() != condition.value.getClass()) {
                    LOGGER.log(Level.WARNING, "Type mismatch in comparison: rowValue={0}, conditionValue={1}",
                            new Object[]{rowValue.getClass().getSimpleName(), condition.value.getClass().getSimpleName()});
                    throw new IllegalArgumentException("Type mismatch in comparison");
                }

                int comparison = rowComparable.compareTo(condition.value);
                return condition.operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0;
            })) {
                validatedUpdates.forEach(row::put);
            }
        }
        return null;
    }
}