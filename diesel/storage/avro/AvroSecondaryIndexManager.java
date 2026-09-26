package diesel.storage.avro;

import java.io.*;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import diesel.storage.StorageMessageConstants;

/**
 * Manages a collection of secondary indexes for AVRO tables.
 * 
 * Features:
 * - Create/drop secondary indexes on any column
 * - B-Tree and composite index support
 * - Automatic synchronization with row operations
 * - Persistence to .asi sidecar files
 * - Usage statistics tracking
 */
public class AvroSecondaryIndexManager implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String tableName;
    private transient List<String> columns;
    private transient Map<String, Class<?>> columnTypes;
    
    // Map: indexName -> AvroSecondaryIndex
    private transient Map<String, AvroSecondaryIndex> indexes;
    
    // Persistence file extension
    private static final String INDEX_FILE_EXTENSION = ".asi";
    
    public AvroSecondaryIndexManager(String tableName, List<String> columns, Map<?, ?> columnTypes) {
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnTypes != null) {
            for (Map.Entry<?, ?> entry : columnTypes.entrySet()) {
                if (entry.getKey() instanceof String column && entry.getValue() instanceof Class<?> type) {
                    this.columnTypes.put(column, type);
                } else if (entry.getKey() instanceof Class<?> type && entry.getValue() instanceof String column) {
                    this.columnTypes.put(column, type);
                }
            }
        }
        this.indexes = new ConcurrentHashMap<>();
    }
    
    /**
     * Create a new secondary index on a single column
     */
    public synchronized void createIndex(String indexName, String columnName) {
        if (indexes.containsKey(indexName)) {
            throw new IllegalArgumentException(StorageMessageConstants.INDEX_PREFIX + indexName + "' already exists");
        }
        
        if (findColumnIndex(columnName) < 0) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in table");
        }
        
        Class<?> keyType = getColumnClass(columnName);
        AvroSecondaryIndex index = new AvroSecondaryIndex(indexName, columnName, keyType);
        
        // Rebuild index from existing data (if any)
        rebuildIndex(index);
        
        indexes.put(indexName, index);
    }
    
    /**
     * Create a new composite index on multiple columns
     */
    public synchronized void createCompositeIndex(String indexName, List<String> columnNames) {
        if (indexes.containsKey(indexName)) {
            throw new IllegalArgumentException(StorageMessageConstants.INDEX_PREFIX + indexName + "' already exists");
        }
        
        for (String col : columnNames) {
            if (findColumnIndex(col) < 0) {
                throw new IllegalArgumentException("Column '" + col + "' not found in table");
            }
        }
        
        Class<?> keyType = getCompositeKeyType(columnNames);
        AvroSecondaryIndex index = new AvroSecondaryIndex(indexName, columnNames, keyType);
        
        // Rebuild index from existing data (if any)
        rebuildIndex(index);
        
        indexes.put(indexName, index);
    }
    
    /**
     * Drop an existing index
     */
    public synchronized void dropIndex(String indexName) {
        AvroSecondaryIndex index = indexes.remove(indexName);
        if (index == null) {
            throw new IllegalArgumentException(StorageMessageConstants.INDEX_PREFIX + indexName + "' not found");
        }
        
        index.clear();
    }
    
    /**
     * Get an index by name
     */
    public synchronized AvroSecondaryIndex getIndex(String indexName) {
        return indexes.get(indexName);
    }
    
    /**
     * Get all index names
     */
    public synchronized Set<String> getIndexNames() {
        return new HashSet<>(indexes.keySet());
    }
    
    /**
     * Get all indexes
     */
    public synchronized Collection<AvroSecondaryIndex> getAllIndexes() {
        return new ArrayList<>(indexes.values());
    }
    
    /**
     * Check if an index exists
     */
    public synchronized boolean hasIndex(String indexName) {
        return indexes.containsKey(indexName);
    }
    
    /**
     * Get number of indexes
     */
    public synchronized int getIndexCount() {
        return indexes.size();
    }

    public synchronized boolean isCompatible(List<String> expectedColumns,
                                              Map<String, Class<?>> expectedTypes) {
        if (expectedColumns == null || expectedTypes == null || columns.size() != expectedColumns.size()) {
            return false;
        }
        for (int i = 0; i < columns.size(); i++) {
            if (!columns.get(i).equalsIgnoreCase(expectedColumns.get(i))) {
                return false;
            }
        }
        for (AvroSecondaryIndex index : indexes.values()) {
            List<String> coveredColumns = index.getCoversColumns();
            for (String column : coveredColumns) {
                if (findColumnIndex(column) < 0) {
                    return false;
                }
            }
            if (coveredColumns.size() == 1) {
                Class<?> expectedType = getExpectedType(expectedTypes, coveredColumns.get(0));
                if (expectedType != null && !expectedType.equals(index.getKeyType())) {
                    return false;
                }
            }
        }
        return true;
    }

    private int findColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private Class<?> getExpectedType(Map<String, Class<?>> expectedTypes, String columnName) {
        Class<?> type = expectedTypes.get(columnName);
        if (type != null) {
            return type;
        }
        for (Map.Entry<String, Class<?>> entry : expectedTypes.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }
        return null;
    }
    
    /**
     * Synchronize index on insert operation
     */
    public synchronized void syncOnInsert(Map<String, Object> row, int rowIndex) {
        for (AvroSecondaryIndex index : indexes.values()) {
            Object key = buildKeyForIndex(row, index);
            if (key != null) {
                index.insert(key, rowIndex);
            }
        }
    }
    
    /**
     * Synchronize index on update operation
     */
    public synchronized void syncOnUpdate(Map<String, Object> oldRow, Map<String, Object> newRow, int rowIndex) {
        for (AvroSecondaryIndex index : indexes.values()) {
            Object oldKey = buildKeyForIndex(oldRow, index);
            Object newKey = buildKeyForIndex(newRow, index);
            
            if (oldKey != null && !Objects.equals(oldKey, newKey)) {
                // Key changed, remove old entry
                index.remove(oldKey, rowIndex);
            }
            
            if (newKey != null) {
                // Insert new entry
                index.insert(newKey, rowIndex);
            }
        }
    }
    
    /**
     * Synchronize index on delete operation
     */
    public synchronized void syncOnDelete(Map<String, Object> row, int rowIndex) {
        for (AvroSecondaryIndex index : indexes.values()) {
            Object key = buildKeyForIndex(row, index);
            if (key != null) {
                index.remove(key, rowIndex);
            }
        }
    }
    
    /**
     * Rebuild all indexes from current data
     */
    public synchronized void rebuildAllIndexes(List<Map<String, Object>> allRows) {
        for (AvroSecondaryIndex index : indexes.values()) {
            index.clear();
        }

        for (int rowIndex = 0; rowIndex < allRows.size(); rowIndex++) {
            Map<String, Object> row = allRows.get(rowIndex);
            for (AvroSecondaryIndex index : indexes.values()) {
                Object key = buildKeyForIndex(row, index);
                if (key != null) {
                    index.insert(key, rowIndex);
                }
            }
        }
    }

    public synchronized void shiftPositions(int rowIndex, int delta) {
        for (AvroSecondaryIndex index : indexes.values()) {
            index.shiftPositions(rowIndex, delta);
        }
    }
    
    /**
     * Get usage statistics for all indexes
     */
    public synchronized Map<String, AvroSecondaryIndex.IndexStatistics> getUsageStatistics() {
        Map<String, AvroSecondaryIndex.IndexStatistics> stats = new HashMap<>();
        for (Map.Entry<String, AvroSecondaryIndex> entry : indexes.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().getStatistics());
        }
        return stats;
    }
    
    /**
     * Get total memory usage of all indexes
     */
    public synchronized long getTotalMemoryUsage() {
        long total = 0;
        for (AvroSecondaryIndex index : indexes.values()) {
            total += estimateIndexMemoryUsage(index);
        }
        return total;
    }
    
    /**
     * Save indexes to sidecar file
     */
    public synchronized void saveToFile(String basePath) throws IOException {
        File indexFile = new File(basePath + INDEX_FILE_EXTENSION);
        File tempFile = new File(indexFile.getPath() + ".tmp");
        if (indexes.isEmpty()) {
            Files.deleteIfExists(indexFile.toPath());
            Files.deleteIfExists(tempFile.toPath());
            return;
        }

        File parent = indexFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            throw new IOException("Failed to create index sidecar directory: " + parent.getPath());
        }
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            out.writeObject(this);
        }
        try {
            Files.move(tempFile.toPath(), indexFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }
    
    /**
     * Load indexes from sidecar file
     */
    @SuppressWarnings("unchecked")
    public static AvroSecondaryIndexManager loadFromFile(String basePath) throws IOException, ClassNotFoundException {
        File indexFile = new File(basePath + INDEX_FILE_EXTENSION);
        if (!indexFile.exists()) {
            return null; // No indexes file exists
        }
        
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(indexFile))) {
            return (AvroSecondaryIndexManager) in.readObject();
        }
    }
    
    /**
     * Delete index sidecar file
     */
    public synchronized void deleteIndexFile(String basePath) {
        File indexFile = new File(basePath + INDEX_FILE_EXTENSION);
        if (indexFile.exists()) {
            indexFile.delete();
        }
    }
    
    /**
     * Rebuild a specific index from current data
     */
    private synchronized void rebuildIndex(AvroSecondaryIndex index) {
        // This would need access to the current table data
        // For now, it's a placeholder - the actual rebuild happens during data loading
        // or when indexes are first created after data exists
    }
    
    /**
     * Build a key for a specific index from a row
     */
    private Object buildKeyForIndex(Map<String, Object> row, AvroSecondaryIndex index) {
        List<String> columns = index.getCoversColumns();
        if (columns.size() > 1) {
            // Composite index
            Object[] components = new Object[columns.size()];
            
            for (int i = 0; i < columns.size(); i++) {
                components[i] = getRowValue(row, columns.get(i));
            }
            
            return new AvroSecondaryIndex.CompositeKey(components);
        } else {
            // Single column index
            return getRowValue(row, columns.get(0));
        }
    }

    private Object getRowValue(Map<String, Object> row, String columnName) {
        if (row == null || columnName == null) {
            return null;
        }
        if (row.containsKey(columnName)) {
            return row.get(columnName);
        }
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }
        return null;
    }
    
    /**
     * Get the Java class for a column
     */
    private Class<?> getColumnClass(String columnName) {
        Class<?> type = columnTypes.get(columnName);
        return type != null ? type : String.class;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(columns);
        out.writeObject(columnTypes);
        out.writeObject(indexes);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        @SuppressWarnings("unchecked")
        List<String> readColumns = (List<String>) in.readObject();
        @SuppressWarnings("unchecked")
        Map<String, Class<?>> readColumnTypes = (Map<String, Class<?>>) in.readObject();
        @SuppressWarnings("unchecked")
        Map<String, AvroSecondaryIndex> readIndexes = (Map<String, AvroSecondaryIndex>) in.readObject();
        
        // Normalize columnTypes
        Map<String, Class<?>> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (readColumnTypes != null) {
            for (Map.Entry<?, ?> entry : readColumnTypes.entrySet()) {
                if (entry.getKey() instanceof String key && entry.getValue() instanceof Class<?> type) {
                    normalized.put(key, type);
                } else if (entry.getKey() instanceof Class<?> type && entry.getValue() instanceof String column) {
                    normalized.put(column, type);
                }
            }
        }
        
        this.columns = readColumns;
        this.columnTypes = normalized;
        this.indexes = readIndexes;
    }
    
    /**
     * Get the Java class for a composite key
     */
    private Class<?> getCompositeKeyType(List<String> columnNames) {
        // For composite keys, use CompositeKey class
        return AvroSecondaryIndex.CompositeKey.class;
    }
    
    /**
     * Estimate memory usage of an index (rough approximation)
     */
    private long estimateIndexMemoryUsage(AvroSecondaryIndex index) {
        // Rough estimate: each entry takes about 50 bytes (key + list overhead + row references)
        return (long) index.size() * 50L;
    }
    
    /**
     * Get index names sorted by usage (most used first)
     */
    public synchronized List<String> getIndexesByUsage() {
        List<Map.Entry<String, AvroSecondaryIndex>> entries = new ArrayList<>(indexes.entrySet());
        
        entries.sort((e1, e2) -> {
            AvroSecondaryIndex.IndexStatistics stats1 = e1.getValue().getStatistics();
            AvroSecondaryIndex.IndexStatistics stats2 = e2.getValue().getStatistics();
            
            // Sort by total operations (lookups + range scans)
            long total1 = stats1.lookupCount() + stats1.rangeScanCount();
            long total2 = stats2.lookupCount() + stats2.rangeScanCount();
            
            return Long.compare(total2, total1); // Descending order
        });
        
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, AvroSecondaryIndex> entry : entries) {
            result.add(entry.getKey());
        }
        return result;
    }
    
    /**
     * Clear all indexes
     */
    public synchronized void clearAllIndexes() {
        indexes.clear();
    }
}