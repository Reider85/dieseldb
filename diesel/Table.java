package diesel;
import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

class Table implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;

    public Table(List<String> columns, Map<String, Class<?>> columnTypes) {
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        this.rows = new ArrayList<>();
    }

    public void addRow(Map<String, Object> row) {
        Map<String, Object> validatedRow = new HashMap<>();
        for (String col : columns) {
            if (!row.containsKey(col)) {
                throw new IllegalArgumentException("Missing value for column: " + col);
            }
            Object value = row.get(col);
            Class<?> expectedType = columnTypes.get(col);
            if (value == null || expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (expectedType == Integer.class && !(value instanceof Integer)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Integer, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == String.class && !(value instanceof String)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected String, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Boolean.class && !(value instanceof Boolean)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Boolean, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == LocalDate.class && !(value instanceof LocalDate)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected LocalDate, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == LocalDateTime.class && !(value instanceof LocalDateTime)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected LocalDateTime, got %s", col, value.getClass().getSimpleName()));
            }
            validatedRow.put(col, value);
        }
        rows.add(validatedRow);
    }

    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }

    public Map<String, Class<?>> getColumnTypes() {
        return new HashMap<>(columnTypes);
    }

    public void saveToFile(String tableName) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tableName + ".table"))) {
            oos.writeObject(this);
            LOGGER.log(Level.INFO, "Table {0} saved to file", tableName);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table {0}: {1}", new Object[]{tableName, e.getMessage()});
        }
    }

    public static Table loadFromFile(String tableName) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tableName + ".table"))) {
            Table table = (Table) ois.readObject();
            LOGGER.log(Level.INFO, "Table {0} loaded from file", tableName);
            return table;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load table {0}: {1}", new Object[]{tableName, e.getMessage()});
            return null;
        }
    }
}