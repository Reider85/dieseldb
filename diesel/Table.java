package diesel;
import java.util.*;
// Table class (Information Expert, SRP)
class Table implements TableStorage {
    private final List<Map<String, String>> rows = new ArrayList<>();
    private final List<String> columns;

    public Table(List<String> columns) {
        this.columns = new ArrayList<>(columns);
    }

    @Override
    public void addRow(Map<String, String> row) {
        if (row.keySet().containsAll(columns)) {
            rows.add(new HashMap<>(row));
        }
    }

    @Override
    public List<Map<String, String>> getRows() {
        return new ArrayList<>(rows);
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }
}
