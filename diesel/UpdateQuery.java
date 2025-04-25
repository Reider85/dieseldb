package diesel;
import java.time.LocalDate;
import java.util.*;

class UpdateQuery implements Query<Void> {
    private final Map<String, Object> updates;
    private final String conditionColumn;
    private final Object conditionValue;

    public UpdateQuery(Map<String, Object> updates, String conditionColumn, Object conditionValue) {
        this.updates = updates;
        this.conditionColumn = conditionColumn;
        this.conditionValue = conditionValue;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        Map<String, Object> validatedUpdates = new HashMap<>();

        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String column = entry.getKey();
            Object value = entry.getValue();
            Class<?> expectedType = columnTypes.get(column);
            if (expectedType == null) {
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            if (expectedType == Integer.class && !(value instanceof Integer)) {
                try {
                    value = Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
                }
            } else if (expectedType == String.class && !(value instanceof String)) {
                value = value.toString();
            } else if (expectedType == Boolean.class && !(value instanceof Boolean)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected BOOLEAN", value, column));
            } else if (expectedType == LocalDate.class && !(value instanceof LocalDate)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected DATE", value, column));
            }
            validatedUpdates.put(column, value);
        }

        for (Map<String, Object> row : rows) {
            if (conditionColumn == null || String.valueOf(row.get(conditionColumn)).equals(String.valueOf(conditionValue))) {
                validatedUpdates.forEach(row::put);
            }
        }
        return null;
    }
}