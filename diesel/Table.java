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

interface Index {
    void insert(Object key, int rowIndex);
    void remove(Object key, int rowIndex);
    List<Integer> search(Object key);
    Class<?> getKeyType();
}

class Table implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, Index> indexes; // Map of column name to Index (BTreeIndex or HashIndex)
    private transient Map<String, Index> uniqueIndexes; // Map of column name to UniqueHashIndex
    private boolean isFileInitialized;

    public Table(List<String> columns, Map<String, Class<?>> columnTypes) {
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        this.rows = new ArrayList<>();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.uniqueIndexes = new ConcurrentHashMap<>();
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

    public void createBTreeIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        if (indexes.containsKey(columnName) || uniqueIndexes.containsKey(columnName)) {
            throw new IllegalArgumentException("Index already exists on column: " + columnName);
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

    public void createHashIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        if (indexes.containsKey(columnName) || uniqueIndexes.containsKey(columnName)) {
            throw new IllegalArgumentException("Index already exists on column: " + columnName);
        }
        HashIndex index = new HashIndex(columnTypes.get(columnName));
        // Populate index with existing data
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created hash index on column {0}", columnName);
    }

    public void createUniqueIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        if (indexes.containsKey(columnName) || uniqueIndexes.containsKey(columnName)) {
            throw new IllegalArgumentException("Index already exists on column: " + columnName);
        }
        UniqueHashIndex index = new UniqueHashIndex(columnTypes.get(columnName));
        // Populate index with existing data
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                try {
                    index.insert(key, i);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Cannot create unique index: duplicate value '" + key + "' found at row " + i);
                }
            }
        }
        uniqueIndexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created unique hash index on column {0}", columnName);
    }

    public Index getIndex(String columnName) {
        Index index = uniqueIndexes.get(columnName);
        if (index != null) {
            return index;
        }
        return indexes.get(columnName);
    }

    public Map<String, Index> getUniqueIndexes() {
        return new ConcurrentHashMap<>(uniqueIndexes);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(indexes);
        oos.writeObject(uniqueIndexes);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = (Map<String, Index>) ois.readObject();
        this.uniqueIndexes = (Map<String, Index>) ois.readObject();
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
            // Update all indexes
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                String column = entry.getKey();
                Index index = entry.getValue();
                Object key = validatedRow.get(column);
                if (key != null) {
                    index.insert(key, rowIndex);
                }
            }
            for (Map.Entry<String, Index> entry : uniqueIndexes.entrySet()) {
                String column = entry.getKey();
                Index index = entry.getValue();
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