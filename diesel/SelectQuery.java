package diesel;
import java.util.*;
import java.util.stream.Collectors;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private final List<String> columns;
    private final String conditionColumn;
    private final String conditionValue;

    public SelectQuery(List<String> columns, String conditionColumn, String conditionValue) {
        this.columns = columns;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return table.getRows().stream()
                .filter(row -> conditionColumn == null || String.valueOf(row.get(conditionColumn)).equals(conditionValue))
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