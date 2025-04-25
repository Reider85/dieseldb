package diesel;
import java.util.*;
import java.util.stream.Collectors;
class InsertQuery implements Query<Void> {
    private final List<String> columns;
    private final List<String> values;

    public InsertQuery(List<String> columns, List<String> values) {
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Void execute(Table table) {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Column and value counts mismatch");
        }
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        table.addRow(row);
        return null;
    }
}

