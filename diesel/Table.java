package diesel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.logging.Level;

interface Index {
    void insert(Object key, int rowIndex);
    void remove(Object key, int rowIndex);
    List<Integer> search(Object key);
    Class<?> getKeyType();
}

/**
 * An in-memory table: schema (columns, types, primary key), the row storage,
 * secondary indexes, an optional clustered index and the sequences used to
 * auto-generate values. Rows are protected by per-row read/write locks.
 */
class Table implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int CURRENT_FORMAT_VERSION = 1;
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final String name;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String primaryKeyColumn;
    private final List<Map<String, Object>> rows;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, Index> indexes;
    private transient Map<String, Sequence> sequences;
    private final Map<String, String> indexDefinitions = new ConcurrentHashMap<>();
    private boolean isFileInitialized;
    private boolean hasClusteredIndex;
    private String clusteredIndexColumn;
    private transient BTreeClusteredIndex clusteredIndex;
    private transient Database database;
    private int formatVersion = CURRENT_FORMAT_VERSION;

    public Table(Database database, String name, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn, Map<String, Sequence> sequences) {
        this.database = database;
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.columnTypes.putAll(columnTypes);
        this.primaryKeyColumn = primaryKeyColumn;
        this.rows = new ArrayList<>();
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.sequences = sequences != null ? new ConcurrentHashMap<>(sequences) : new ConcurrentHashMap<>();
        this.isFileInitialized = false;
        this.hasClusteredIndex = false;
        this.clusteredIndexColumn = null;
        this.clusteredIndex = null;

        validateSchema(columns, columnTypes);

        if (primaryKeyColumn != null) {
            if (!this.columnTypes.containsKey(primaryKeyColumn)) {
                throw new IllegalArgumentException("Primary key column " + primaryKeyColumn + " does not exist");
            }
            createUniqueClusteredIndex(primaryKeyColumn);
        }

        LOGGER.log(Level.INFO, "Created table: {0} with columns {1}, types {2}, primary key: {3}, sequences: {4}",
                new Object[]{name, columns, columnTypes, primaryKeyColumn, sequences.keySet()});
    }

    public Database getDatabase() {
        return database;
    }

    public void attachDatabase(Database database) {
        this.database = database;
    }

    private void validateSchema(List<String> columns, Map<String, Class<?>> columnTypes) {
        for (String column : columns) {
            if (!columnTypes.containsKey(column)) {
                LOGGER.log(Level.SEVERE, "Schema validation failed: Column {0} missing in columnTypes {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Column " + column + " missing in columnTypes");
            }
        }
        for (String column : columnTypes.keySet()) {
            if (!columns.contains(column)) {
                LOGGER.log(Level.WARNING, "Column {0} in columnTypes but not in columns list {1}",
                        new Object[]{column, columns});
            }
        }
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

    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    public void createBTreeIndex(String columnName) {
        createSecondaryIndex(columnName, "BTREE", BTreeIndex::new, false);
        LOGGER.log(Level.INFO, "Created B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    public void createHashIndex(String columnName) {
        createSecondaryIndex(columnName, "HASH", HashIndex::new, false);
        LOGGER.log(Level.INFO, "Created hash index on column {0} for table {1}", new Object[]{columnName, name});
    }

    public void createUniqueIndex(String columnName) {
        createSecondaryIndex(columnName, "UNIQUE", UniqueIndex::new, true);
        LOGGER.log(Level.INFO, "Created unique index on column {0} for table {1}", new Object[]{columnName, name});
    }

    /**
     * Builds a secondary index over the already-present rows and registers it
     * under {@code columnName}. When {@code unique} is set, duplicate keys abort
     * the creation.
     */
    private void createSecondaryIndex(String columnName, String definition, Function<Class<?>, Index> indexFactory, boolean unique) {
        if (!columnTypes.containsKey(columnName)) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }
        Index index = indexFactory.apply(columnTypes.get(columnName));
        Set<Object> seenKeys = unique ? new HashSet<>() : null;
        for (int i = 0; i < rows.size(); i++) {
            Object key = rows.get(i).get(columnName);
            if (key != null) {
                if (unique && !seenKeys.add(key)) {
                    throw new IllegalStateException("Duplicate key '" + key + "' found in column " + columnName + " while creating unique index");
                }
                index.insert(key, i);
            }
        }
        indexes.put(columnName, index);
        indexDefinitions.put(columnName, definition);
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

        // Sort the rows by the clustered key and verify uniqueness before reindexing.
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
            insertRowIntoIndexes(rows.get(i), i);
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
        Map<String, Class<?>> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(columnTypes);
        return copy;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }

    public void removeRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException("Row index " + rowIndex + " out of bounds for table " + name);
        }
        rows.remove(rowIndex);
        // Row indexes shift down by one, so the locks of this and all following rows are stale.
        for (int i = rowIndex; i <= rows.size(); i++) {
            rowLocks.remove(i);
        }
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(hasClusteredIndex);
        oos.writeObject(clusteredIndexColumn);
        oos.writeObject(sequences);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        // Rebuild the case-insensitive type map so lookups stay case-insensitive after load.
        Map<String, Class<?>> tempColumnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tempColumnTypes.putAll(columnTypes);
        this.columnTypes.clear();
        this.columnTypes.putAll(tempColumnTypes);
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.hasClusteredIndex = (boolean) ois.readObject();
        this.clusteredIndexColumn = (String) ois.readObject();
        this.sequences = (Map<String, Sequence>) ois.readObject();
        if (hasClusteredIndex) {
            this.clusteredIndex = new BTreeClusteredIndex(columnTypes.get(clusteredIndexColumn));
            for (int i = 0; i < rows.size(); i++) {
                Object key = rows.get(i).get(clusteredIndexColumn);
                if (key != null) {
                    clusteredIndex.insert(key, i);
                }
            }
        }
        // Rebuild all secondary indexes from their persisted definitions.
        if (indexDefinitions != null) {
            for (Map.Entry<String, String> entry : indexDefinitions.entrySet()) {
                String column = entry.getKey();
                Class<?> keyType = columnTypes.get(column);
                Index index;
                switch (entry.getValue()) {
                    case "BTREE":
                        index = new BTreeIndex(keyType);
                        break;
                    case "HASH":
                        index = new HashIndex(keyType);
                        break;
                    case "UNIQUE":
                        index = new UniqueIndex(keyType);
                        break;
                    default:
                        continue;
                }
                for (int i = 0; i < rows.size(); i++) {
                    Object key = rows.get(i).get(column);
                    if (key != null) {
                        index.insert(key, i);
                    }
                }
                indexes.put(column, index);
            }
        }
        this.database = null;
    }

    public void addRow(Map<String, Object> row) {
        Map<String, Object> validatedRow = new HashMap<>();
        for (String col : columns) {
            Object value;
            Sequence sequence = sequences.get(col);
            if (sequence != null) {
                if (col.equals(primaryKeyColumn) && row.containsKey(col)) {
                    throw new IllegalArgumentException("Cannot manually specify value for sequence-based primary key column: " + col);
                }
                value = row.containsKey(col) ? row.get(col) : sequence.nextValue();
            } else if (!row.containsKey(col)) {
                throw new IllegalArgumentException("Missing value for column: " + col);
            } else {
                value = row.get(col);
            }

            checkUniqueConstraint(col, value);

            Class<?> expectedType = columnTypes.get(col);
            if (expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (value == null) {
                validatedRow.put(col, null);
                continue;
            }
            validateColumnValueType(col, expectedType, value);
            validatedRow.put(col, value);
        }

        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException("Null key in clustered index column: " + clusteredIndexColumn);
            }
            List<Integer> existing = clusteredIndex.search(key);
            if (!existing.isEmpty()) {
                LOGGER.log(Level.WARNING, "Duplicate clustered key detected: key '{0}' in column {1}", new Object[]{key, clusteredIndexColumn});
                throw new IllegalStateException("Duplicate key violation: key '" + key + "' in column " + clusteredIndexColumn);
            }
            insertIntoClusteredPosition(validatedRow, key);
        } else {
            insertAtEnd(validatedRow);
        }
        LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{name, validatedRow});
    }

    private void checkUniqueConstraint(String column, Object value) {
        Index index = indexes.get(column);
        if (index instanceof UniqueIndex || index instanceof BTreeClusteredIndex) {
            if (value != null && index.search(value).size() > 0) {
                LOGGER.log(Level.WARNING, "Duplicate key detected: key '{0}' in column {1}; skipping insertion", new Object[]{value, column});
                throw new IllegalStateException("Duplicate key violation: key '" + value + "' already exists in column " + column);
            }
        }
    }

    private void validateColumnValueType(String column, Class<?> expectedType, Object value) {
        boolean validType = (expectedType == Integer.class && value instanceof Integer)
                || (expectedType == Long.class && value instanceof Long)
                || (expectedType == Short.class && value instanceof Short)
                || (expectedType == Byte.class && value instanceof Byte)
                || (expectedType == BigDecimal.class && value instanceof BigDecimal)
                || (expectedType == Float.class && value instanceof Float)
                || (expectedType == Double.class && value instanceof Double)
                || (expectedType == Character.class && value instanceof Character)
                || (expectedType == UUID.class && value instanceof UUID)
                || (expectedType == String.class && value instanceof String)
                || (expectedType == Boolean.class && value instanceof Boolean)
                || (expectedType == LocalDate.class && value instanceof LocalDate)
                || (expectedType == LocalDateTime.class && value instanceof LocalDateTime);
        if (!validType) {
            throw new IllegalArgumentException(
                    String.format("Invalid type for column %s: expected %s, got %s",
                            column, expectedType.getSimpleName(), value.getClass().getSimpleName()));
        }
    }

    private void insertIntoClusteredPosition(Map<String, Object> row, Object clusteredKey) {
        int insertIndex = findInsertPosition(clusteredKey);
        ReentrantReadWriteLock lock = getRowLock(insertIndex);
        lock.writeLock().lock();
        try {
            rows.add(insertIndex, row);
            clusteredIndex.insert(clusteredKey, insertIndex);
            insertRowIntoIndexes(row, insertIndex);
            updateIndicesAfterInsert(insertIndex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void insertAtEnd(Map<String, Object> row) {
        int rowIndex = rows.size();
        ReentrantReadWriteLock lock = getRowLock(rowIndex);
        lock.writeLock().lock();
        try {
            rows.add(row);
            insertRowIntoIndexes(row, rowIndex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Inserts {@code row} into every secondary index at {@code rowIndex}, skipping NULL keys. */
    private void insertRowIntoIndexes(Map<String, Object> row, int rowIndex) {
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            Object key = row.get(entry.getKey());
            if (key != null) {
                entry.getValue().insert(key, rowIndex);
            }
        }
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

    /** Shifts the stored row index of every row after an insert into a clustered table. */
    private void updateIndicesAfterInsert(int insertIndex) {
        for (int i = insertIndex + 1; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);

            Object clusteredKey = row.get(clusteredIndexColumn);
            if (clusteredKey != null) {
                List<Integer> clusteredIndices = clusteredIndex.search(clusteredKey);
                if (clusteredIndices.contains(i - 1)) {
                    clusteredIndex.remove(clusteredKey, i - 1);
                    clusteredIndex.insert(clusteredKey, i);
                }
            }

            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                Object key = row.get(entry.getKey());
                if (key != null) {
                    List<Integer> currentIndices = entry.getValue().search(key);
                    if (currentIndices.contains(i - 1)) {
                        entry.getValue().remove(key, i - 1);
                        entry.getValue().insert(key, i);
                    }
                }
            }
        }
    }

    private String resolveFilePath(String tableName, String extension) {
        String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
        return dir + File.separator + tableName + extension;
    }

    public void saveToFile(String tableName) {
        String fileName = resolveFilePath(tableName, ".csv");
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
                        values.add(formatValue(row.get(column)));
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

    public void saveToSerializedFile(String tableName) {
        String fileName = resolveFilePath(tableName, ".table");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
            oos.writeObject(this);
            oos.flush();
            isFileInitialized = true;
            LOGGER.log(Level.INFO, "Table {0} saved to file {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
            throw new RuntimeException("Failed to save table to file: " + fileName, e);
        }
    }

    public static Table loadFromFile(Database database, String tableName) {
        String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
        String fileName = dir + File.separator + tableName + ".table";
        File file = new File(fileName);
        if (!file.exists()) {
            LOGGER.log(Level.INFO, "Serialized file {0} not found, creating new table {1} with base structure",
                    new Object[]{fileName, tableName});
            Table table = new Table(database, tableName, new ArrayList<>(), new HashMap<>(), null, new HashMap<String, Sequence>());
            table.formatVersion = CURRENT_FORMAT_VERSION;
            table.setFileInitialized(false);
            return table;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            Table table = (Table) ois.readObject();
            if (table.formatVersion != CURRENT_FORMAT_VERSION) {
                throw new IllegalArgumentException("Unsupported table format version: " + table.formatVersion
                        + ", expected: " + CURRENT_FORMAT_VERSION);
            }
            table.database = database;
            table.setFileInitialized(true);
            LOGGER.log(Level.INFO, "Table {0} loaded from file {1}", new Object[]{tableName, fileName});
            return table;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load table {0}: {1}", new Object[]{tableName, e.getMessage()});
            return null;
        }
    }
}
