package diesel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final String conditionColumn;
    private final Object conditionValue;
    private final QueryParser.Operator operator;

    public SelectQuery(List<String> columns, String conditionColumn, Object conditionValue, QueryParser.Operator operator) {
        this.columns = columns;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
        this.operator = operator;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return table.getRows().stream()
                .filter(row -> {
                    if (conditionColumn == null) {
                        return true;
                    }
                    Object rowValue = row.get(conditionColumn);
                    if (rowValue == null) {
                        return false;
                    }

                    LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}",
                            new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                                    conditionValue, conditionValue.getClass().getSimpleName(), operator});

                    // Handle EQUALS and NOT_EQUALS
                    if (operator == QueryParser.Operator.EQUALS || operator == QueryParser.Operator.NOT_EQUALS) {
                        boolean isEqual;
                        if (rowValue instanceof Float && conditionValue instanceof Float) {
                            isEqual = Math.abs(((Float) rowValue) - ((Float) conditionValue)) < 1e-7;
                        } else if (rowValue instanceof Double && conditionValue instanceof Double) {
                            isEqual = Math.abs(((Double) rowValue) - ((Double) conditionValue)) < 1e-7;
                        } else {
                            isEqual = String.valueOf(rowValue).equals(String.valueOf(conditionValue));
                        }
                        return operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
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
                    return operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0;
                })
                .map(row -> filterColumns(row, columns))
                .collect(Collectors.toList());
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