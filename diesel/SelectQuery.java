package diesel;
import java.util.*;
import java.util.stream.Collectors;

class SelectQuery implements Query<List<Map<String, Object>>> {
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
                    boolean isEqual;
                    if (row.get(conditionColumn) instanceof Float && conditionValue instanceof Float) {
                        isEqual = Math.abs(((Float) row.get(conditionColumn)) - ((Float) conditionValue)) < 1e-7;
                    } else if (row.get(conditionColumn) instanceof Double && conditionValue instanceof Double) {
                        isEqual = Math.abs(((Double) row.get(conditionColumn)) - ((Double) conditionValue)) < 1e-7;
                    } else {
                        isEqual = String.valueOf(row.get(conditionColumn)).equals(String.valueOf(conditionValue));
                    }
                    return operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
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