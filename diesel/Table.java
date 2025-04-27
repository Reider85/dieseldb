package diesel;
import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;

class Table implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;
    private transient ReentrantReadWriteLock lock;
    private boolean isFileInitialized;

    public Table(List<String> columns, Map<String, Class<?>> columnTypes) {
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        this.rows = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
        this.isFileInitialized = false;
    }

    public boolean isFileInitialized() {
        return isFileInitialized;
    }

    public void setFileInitialized(boolean fileInitialized) {
        isFileInitialized = fileInitialized;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    // Custom serialization to handle transient lock
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.lock = new ReentrantReadWriteLock();
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
            } else if (expectedType == Long.class && !(value instanceof Long)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Long, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Short.class && !(value instanceof Short)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Short, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Byte.class && !(value instanceof Byte)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Byte, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == BigDecimal.class && !(value instanceof BigDecimal)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected BigDecimal, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Float.class && !(value instanceof Float)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Float, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Double.class && !(value instanceof Double)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Double, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == Character.class && !(value instanceof Character)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected Character, got %s", col, value.getClass().getSimpleName()));
            } else if (expectedType == UUID.class && !(value instanceof UUID)) {
                throw new IllegalArgumentException(
                        String.format("Invalid type for column %s: expected UUID, got %s", col, value.getClass().getSimpleName()));
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
        String fileName = tableName + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, isFileInitialized))) {
            if (!isFileInitialized) {
                // Write header
                writer.write(String.join(",", columns));
                writer.newLine();
                isFileInitialized = true;
            }
            // Write the last row (most recent insert)
            if (!rows.isEmpty()) {
                Map<String, Object> row = rows.get(rows.size() - 1);
                List<String> values = new ArrayList<>();
                for (String column : columns) {
                    Object value = row.get(column);
                    values.add(formatValue(value));
                }
                writer.write(String.join(",", values));
                writer.newLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
            throw new RuntimeException("Failed to save table to file: " + fileName, e);
        }
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return "\"" + value.toString().replace("\"", "\"\"") + "\"";
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime || value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        return value.toString();
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