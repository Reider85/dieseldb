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
    private final String name;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, Index> indexes;
    private boolean isFileInitialized;
    private boolean hasClusteredIndex;
    private String clusteredIndexColumn;
    private transient BTreeClusteredIndex clusteredIndex;

    public Table(String name, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        this.rows = new ArrayList<>();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.isFileInitialized = false;
        this.hasClusteredIndex = false;
        this.clusteredIndexColumn = null;
        this.clusteredIndex = null;
        LOGGER.log(Level.FINE, "Created table: {0}", name);
    }

    public String getName() {
        return name;
    }

    public Map<String, Index> getIndexes() {
        return indexes;
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
        BTreeIndex index = new BTreeIndex(columnTypes.get(columnName));
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    public void createHashIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        HashIndex index = new HashIndex(columnTypes.get(columnName));
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created hash index on column {0} for table {1}", new Object[]{columnName, name});
    }

    public void createUniqueIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        UniqueIndex index = new UniqueIndex(columnTypes.get(columnName));
        Set<Object> seenKeys = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            Object key = row.get(columnName);
            if (key != null) {
                if (!seenKeys.add(key)) {
                    throw new IllegalStateException("Duplicate key '" + key + "' found in column " + columnName + " while creating unique index");
                }
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        LOGGER.log(Level.INFO, "Created unique index on column {0} for table {1}", new Object[]{columnName, name});
    }

    public void createUniqueClusteredIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        if (hasClusteredIndex) {
            throw new IllegalStateException("Table already has a clustered index on " + clusteredIndexColumn);
        }

        clusteredIndex = new BTreeClusteredIndex(columnTypes.get(columnName));
        hasClusteredIndex = true;
        clusteredIndexColumn = columnName;

        List<Map<String, Object>> sortedRows = new ArrayList<>(rows);
        sortedRows.sort((row1, row2) -> {
            Object key1 = row1.get(columnName);
            Object key2 = row2.get(columnName);
            if (key1 == null || key2 == null) {
                throw new IllegalStateException("Null key in column " + columnName + " not allowed for unique clustered index");
            }
            return compareKeys(key1, key2);
        });

        Set<Object> seenKeys = new HashSet<>();
        for (Map<String, Object> row : sortedRows) {
            Object key = row.get(columnName);
            if (!seenKeys.add(key)) {
                throw new IllegalStateException("Duplicate key '" + key + "' found in column " + columnName + " while creating unique clustered index");
            }
        }

        rows.clear();
        rows.addAll(sortedRows);
        for (int i = 0; i < rows.size(); i++) {
            Object key = rows.get(i).get(columnName);
            clusteredIndex.insert(key, i);
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                String col = entry.getKey();
                Index idx = entry.getValue();
                Object idxKey = rows.get(i).get(col);
                if (idxKey != null) {
                    idx.insert(idxKey, i);
                }
            }
        }

        LOGGER.log(Level.INFO, "Created unique clustered B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable && k2 instanceof Comparable) {
            return ((Comparable<Object>) k1).compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }

    public Index getIndex(String columnName) {
        return indexes.get(columnName);
    }

    public boolean hasClusteredIndex() {
        return hasClusteredIndex;
    }

    public String getClusteredIndexColumn() {
        return clusteredIndexColumn;
    }

    public BTreeClusteredIndex getClusteredIndex() {
        return clusteredIndex;
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    public Map<String, Class<?>> getColumnTypes() {
        return new HashMap<>(columnTypes);
    }

    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(hasClusteredIndex);
        oos.writeObject(clusteredIndexColumn);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.hasClusteredIndex = (boolean) ois.readObject();
        this.clusteredIndexColumn = (String) ois.readObject();
        if (hasClusteredIndex) {
            this.clusteredIndex = new BTreeClusteredIndex(columnTypes.get(clusteredIndexColumn));
            for (int i = 0; i < rows.size(); i++) {
                Object key = rows.get(i).get(clusteredIndexColumn);
                if (key != null) {
                    clusteredIndex.insert(key, i);
                }
            }
        }
    }

    public void addRow(Map<String, Object> row) {
        LOGGER.log(Level.FINE, "Entering addRow: row={0}", row);
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

        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException("Null key in clustered index column: " + clusteredIndexColumn);
            }
            List<Integer> existing = clusteredIndex.search(key);
            if (!existing.isEmpty()) {
                throw new IllegalStateException("Duplicate key violation: key '" + key + "' in column " + clusteredIndexColumn);
            }

            int insertIndex = findInsertPosition(key);
            ReentrantReadWriteLock lock = getRowLock(insertIndex);
            lock.writeLock().lock();
            try {
                rows.add(insertIndex, validatedRow);
                clusteredIndex.insert(key, insertIndex);
                for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                    String column = entry.getKey();
                    Index index = entry.getValue();
                    Object idxKey = validatedRow.get(column);
                    if (idxKey != null) {
                        index.insert(idxKey, insertIndex);
                    }
                }
                updateIndicesAfterInsert(insertIndex);
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            int rowIndex = rows.size();
            ReentrantReadWriteLock lock = getRowLock(rowIndex);
            lock.writeLock().lock();
            try {
                rows.add(validatedRow);
                for (Map.Entry<String, Index> entry : indexes.entrySet()) {
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
        LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{name, validatedRow});
    }

    private int findInsertPosition(Object key) {
        if (rows.isEmpty()) {
            return 0;
        }
        int low = 0;
        int high = rows.size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            Object midKey = rows.get(mid).get(clusteredIndexColumn);
            int cmp = compareKeys(key, midKey);
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                throw new IllegalStateException("Duplicate key found: " + key);
            }
        }
        return low;
    }

    private void updateIndicesAfterInsert(int insertIndex) {
        LOGGER.log(Level.FINE, "Entering updateIndicesAfterInsert: insertIndex={0}, rows size={1}",
                new Object[]{insertIndex, rows.size()});
        for (int i = insertIndex + 1; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);
            LOGGER.log(Level.FINE, "Processing row {0}: {1}", new Object[]{i, row});

            // Update clustered index
            Object clusteredKey = row.get(clusteredIndexColumn);
            if (clusteredKey != null) {
                List<Integer> clusteredIndices = clusteredIndex.search(clusteredKey);
                if (clusteredIndices.contains(i - 1)) {
                    LOGGER.log(Level.FINE, "Updating clustered index: key={0}, oldIndex={1}, newIndex={2}, currentIndices={3}",
                            new Object[]{clusteredKey, i - 1, i, clusteredIndices});
                    clusteredIndex.remove(clusteredKey, i - 1);
                    clusteredIndex.insert(clusteredKey, i);
                }
            }

            // Update other indices
            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                String column = entry.getKey();
                Index index = entry.getValue();
                Object key = row.get(column);
                if (key != null) {
                    List<Integer> currentIndices = index.search(key);
                    LOGGER.log(Level.FINE, "Checking index for column={0}, key={1}, currentIndices={2}, rowIndex={3}",
                            new Object[]{column, key, currentIndices, i});
                    if (currentIndices.contains(i - 1)) {
                        LOGGER.log(Level.FINE, "Updating index: column={0}, key={1}, oldIndex={2}, newIndex={3}",
                                new Object[]{column, key, i - 1, i});
                        index.remove(key, i - 1);
                        index.insert(key, i);
                    }
                }
            }
        }
        LOGGER.log(Level.FINE, "Exiting updateIndicesAfterInsert: insertIndex={0}", insertIndex);
    }

    public void saveToFile(String tableName) {
        LOGGER.log(Level.FINE, "Entering saveToFile: tableName={0}", tableName);
        String fileName = tableName + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false))) {
            writer.write(String.join(",", columns));
            writer.newLine();

            for (int i = 0; i < rows.size(); i++) {
                ReentrantReadWriteLock lock = getRowLock(i);
                lock.readLock().lock();
                try {
                    Map<String, Object> row = rows.get(i);
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

            isFileInitialized = true;
            LOGGER.log(Level.INFO, "Table {0} saved to file {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
            throw new RuntimeException("Failed to save table to file: " + fileName, e);
        }
        LOGGER.log(Level.FINE, "Exiting saveToFile: tableName={0}", tableName);
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
        String fileName = tableName + ".table";
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            Table table = (Table) ois.readObject();
            table.setFileInitialized(true);
            LOGGER.log(Level.INFO, "Table {0} loaded from file {1}", new Object[]{tableName, fileName});
            return table;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load table {0}: {1}", new Object[]{tableName, e.getMessage()});
            return null;
        }
    }
}