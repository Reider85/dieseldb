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

        // Validate and convert update values to match column types
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
                    throw new IllegalArgumentException("Expected single character for column: " + column);
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

        // Update rows that match the conditions
        for (Map<String, Object> row : rows) {
            if (conditions.isEmpty() || evaluateConditions(row, conditions)) {
                validatedUpdates.forEach(row::put);
            }
        }
        return null;
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions) {
        boolean result = evaluateCondition(row, conditions.get(0));
        for (int i = 1; i < conditions.size(); i++) {
            boolean currentResult = evaluateCondition(row, conditions.get(i));
            String conjunction = conditions.get(i - 1).conjunction;
            if ("AND".equalsIgnoreCase(conjunction)) {
                result = result && currentResult;
            } else if ("OR".equalsIgnoreCase(conjunction)) {
                result = result || currentResult;
            }
        }
        return result;
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition) {
        if (condition.isGrouped()) {
            boolean subResult = evaluateConditions(row, condition.subConditions);
            return condition.not ? !subResult : subResult;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}, not={5}",
                new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                        condition.value, condition.value.getClass().getSimpleName(), condition.operator, condition.not});

        boolean result;

        // Handle EQUALS and NOT_EQUALS
        if (condition.operator == QueryParser.Operator.EQUALS || condition.operator == QueryParser.Operator.NOT_EQUALS) {
            boolean isEqual;
            if (rowValue instanceof Float && condition.value instanceof Float) {
                isEqual = Math.abs(((Float) rowValue) - ((Float) condition.value)) < 1e-7;
            } else if (rowValue instanceof Double && condition.value instanceof Double) {
                isEqual = Math.abs(((Double) rowValue) - ((Double) condition.value)) < 1e-7;
            } else {
                isEqual = String.valueOf(rowValue).equals(String.valueOf(condition.value));
            }
            result = condition.operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
        }
        // Handle LESS_THAN and GREATER_THAN
        else {
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
            result = condition.operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0;
        }

        return condition.not ? !result : result;
    }
}