package diesel;
import java.util.*;

class UpdateQuery implements Query<Void> {
    private final Map<String, String> updates;
    private final String conditionColumn;
    private final String conditionValue;

    public UpdateQuery(Map<String, String> updates, String conditionColumn, String conditionValue) {
        this.updates = updates;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, Object> validatedUpdates = new HashMap<>();

        // Validate and convert update values
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            String column = entry.getKey();
            String value = entry.getValue();
            Class<?> expectedType = columnTypes.get(column);
            if (expectedType == null) {
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            if (expectedType == Integer.class) {
                try {
                    validatedUpdates.put(column, Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
                }
            } else if (expectedType == String.class) {
                validatedUpdates.put(column, value);
            } else {
                throw new IllegalArgumentException("Unsupported column type for column " + column);
            }
        }

        for (Map<String, Object> row : rows) {
            if (conditionColumn == null || String.valueOf(row.get(conditionColumn)).equals(conditionValue)) {
                validatedUpdates.forEach(row::put);
            }
        }
        return null;
    }
}