package diesel;
import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;

class Table implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, BTreeIndex> indexes; // Map of column name to B-tree index
    private boolean isFileInitialized;

    public Table(List<String> columns, Map<String, Class<?>> columnTypes) {
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        this.rows = new ArrayList<>();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.isFileInitialized = false;
    }

    public boolean isFileInitialized() {
        return isFileInitialized;
    }

    public void setFileInitialized(boolean fileInitialized) {
        isFileInitialized = fileInitialized;
    }

    public ReentrantReadWriteLock getRowLock(int rowIndex) {
        return rowLocks.computeIfAbsent(rowIndex, k -> new ReentrantReadWriteLock());
    }

    public void createIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        BTreeIndex index = new BTreeIndex(columnTypes.get(columnName));
        // Populate index with existing data
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created B-tree index on column {0}", columnName);
    }

    public BTreeIndex getIndex(String columnName) {
        return indexes.get(columnName);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(indexes);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = (Map<String, BTreeIndex>) ois.readObject();
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
        int rowIndex = rows.size();
        ReentrantReadWriteLock lock = getRowLock(rowIndex);
        lock.writeLock().lock();
        try {
            rows.add(validatedRow);
            // Update indexes
            for (Map.Entry<String, BTreeIndex> entry : indexes.entrySet()) {
                String column = entry.getKey();
                BTreeIndex index = entry.getValue();
                Object key = validatedRow.get(column);
                if (key != null) {
                    index.insert(key, rowIndex);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
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
                writer.write(String.join(",", columns));
                writer.newLine();
                isFileInitialized = true;
            }
            if (!rows.isEmpty()) {
                int lastRowIndex = rows.size() - 1;
                ReentrantReadWriteLock lock = getRowLock(lastRowIndex);
                lock.readLock().lock();
                try {
                    Map<String, Object> row = rows.get(lastRowIndex);
                    List<String> values = new ArrayList<>();
                    for (String column : columns) {
                        Object value = row.get(column);
                        values.add(formatValue(value));
                    }
                    writer.write(String.join(",", values));
                    writer.newLine();
                } finally {
                    lock.readLock().unlock();
                }
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