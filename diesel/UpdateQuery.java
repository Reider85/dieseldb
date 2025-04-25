package diesel;
import java.util.*;
import java.util.stream.Collectors;

class UpdateQuery implements Query<Void> {
    private final Map<String, String> updates; // Column-value pairs to update
    private final String conditionColumn; // Column for WHERE condition
    private final String conditionValue; // Value for WHERE condition

    public UpdateQuery(Map<String, String> updates, String conditionColumn, String conditionValue) {
        this.updates = updates;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, String>> rows = table.getRows();
        for (Map<String, String> row : rows) {
            if (conditionColumn == null || row.get(conditionColumn).equals(conditionValue)) {
                updates.forEach(row::put);
            }
        }
        return null;
    }
}