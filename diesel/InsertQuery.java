package diesel;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = values.get(i);
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
            } else if (expectedType == Long.class && !(value instanceof Long)) {
                try {
                    value = Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected LONG", value, column));
                }
            } else if (expectedType == Short.class && !(value instanceof Short)) {
                try {
                    value = Short.parseShort(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
                }
            } else if (expectedType == Byte.class && !(value instanceof Byte)) {
                try {
                    value = Byte.parseByte(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
                }
            } else if (expectedType == String.class && !(value instanceof String)) {
                value = value.toString();
            } else if (expectedType == Boolean.class && !(value instanceof Boolean)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected BOOLEAN", value, column));
            } else if (expectedType == LocalDate.class && !(value instanceof LocalDate)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected DATE", value, column));
            } else if (expectedType == LocalDateTime.class && !(value instanceof LocalDateTime)) {
                throw new IllegalArgumentException(
                        String.format("Invalid value '%s' for column %s: expected DATETIME or DATETIME_MS", value, column));
            }
            row.put(column, value);
        }
        table.addRow(row);
        return null;
    }
}