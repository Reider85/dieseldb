package diesel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final List<QueryParser.Condition> conditions;

    public SelectQuery(List<String> columns, List<QueryParser.Condition> conditions) {
        this.columns = columns;
        this.conditions = conditions;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return table.getRows().stream()
                .filter(row -> {
                    if (conditions.isEmpty()) {
                        return true;
                    }
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
                })
                .map(row -> filterColumns(row, columns))
                .collect(Collectors.toList());
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition) {
        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}",
                new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                        condition.value, condition.value.getClass().getSimpleName(), condition.operator});

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
            return condition.operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
        }

        // Handle LESS_THAN and GREATER_THAN
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
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> result = new HashMap<>();
        for (String col : columns) {
            if (row.containsKey(col)) {
                result.put(col, row.get(col));
            }
        }
        return result;
    }
}