package diesel;
import java.util.*;

class InsertQuery implements Query<Void> {
    private final List<String> columns;
    private final List<Object> values;

    public InsertQuery(List<String> columns, List<Object> values) {
        this.columns = columns;
        this.values = values;
    }

    @Override
    public Void execute(Table table) {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Column and value counts mismatch");
        }
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values.get(i));
        }
        table.addRow(row);
        return null;
    }
}