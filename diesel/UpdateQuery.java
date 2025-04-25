package diesel;
import java.util.*;

class UpdateQuery implements Query<Void> {
    private final Map<String, Object> updates;
    private final String conditionColumn;
    private final String conditionValue;

    public UpdateQuery(Map<String, Object> updates, String conditionColumn, String conditionValue) {
        this.updates = updates;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        for (Map<String, Object> row : rows) {
            if (conditionColumn == null || String.valueOf(row.get(conditionColumn)).equals(conditionValue)) {
                updates.forEach(row::put);
            }
        }
        return null;
    }
}