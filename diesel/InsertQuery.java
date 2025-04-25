package diesel;
import java.util.*;

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
        Map<String, Object> row = new HashMap<>();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String value = values.get(i);
            Class<?> expectedType = columnTypes.get(column);
            Object convertedValue;
            if (expectedType == Integer.class) {
                try {
                    convertedValue = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
                }
            } else if (expectedType == String.class) {
                convertedValue = value;
            } else {
                throw new IllegalArgumentException("Unsupported column type for column " + column);
            }
            row.put(column, convertedValue);
        }
        table.addRow(row);
        return null;
    }
}