package diesel;
import java.util.*;
import java.util.stream.Collectors;
// Select query representation (SRP, Polymorphism)
class SelectQuery implements Query<List<Map<String, String>>> {
    private final List<String> columns;
    private final String conditionColumn;
    private final String conditionValue;

    public SelectQuery(List<String> columns, String conditionColumn, String conditionValue) {
        this.columns = columns;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public List<Map<String, String>> execute(Table table) {
        return table.getRows().stream()
                .filter(row -> conditionColumn == null || row.get(conditionColumn).equals(conditionValue))
                .map(row -> filterColumns(row, columns))
                .collect(Collectors.toList());
    }

    private Map<String, String> filterColumns(Map<String, String> row, List<String> columns) {
        Map<String, String> result = new HashMap<>();
        for (String col : columns) {
            if (row.containsKey(col)) {
                result.put(col, row.get(col));
            }
        }
        return result;
    }
}
