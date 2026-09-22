package diesel.storage.avro;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    private final List<String> columns;
    private final Map<Class<?>, Object> columnTypes;
    
    // Map: indexName -> AvroSecondaryIndex
    private final Map<String, AvroSecondaryIndex> indexes;
    
    // Persistence file extension
    private static final String INDEX_FILE_EXTENSION = ".asi";
    
    public AvroSecondaryIndexManager(String tableName, List<String> columns, Map<Class<?>, Object> columnTypes) {
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new ConcurrentHashMap<>(columnTypes);
        this.indexes = new ConcurrentHashMap<>();
    }
    
    /**
     * Create a new secondary index on a single column
     */
    public synchronized void createIndex(String indexName, String columnName) {
        if (indexes.containsKey(indexName)) {
            throw new IllegalArgumentException("Index '" + indexName + "' already exists");
        }
        
        if (!columns.contains(columnName)) {
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
            throw new IllegalArgumentException("Index '" + indexName + "' already exists");
        }
        
        for (String col : columnNames) {
            if (!columns.contains(col)) {
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
            throw new IllegalArgumentException("Index '" + indexName + "' not found");
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
        // Clear all indexes
        for (AvroSecondaryIndex index : indexes.values()) {
            index.clear();
        }
        
        // Rebuild each index
        for (Map<String, Object> row : allRows) {
            int rowIndex = allRows.indexOf(row);
            syncOnInsert(row, rowIndex);
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
        if (indexes.isEmpty()) {
            return; // No indexes to save
        }
        
        File indexFile = new File(basePath + INDEX_FILE_EXTENSION);
        File tempFile = new File(indexFile.getPath() + ".tmp");
        
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            out.writeObject(this);
        }
        
        // Atomic rename
        if (!tempFile.renameTo(indexFile)) {
            throw new IOException("Failed to rename temp file to: " + indexFile.getPath());
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
                components[i] = row.get(columns.get(i));
            }
            
            return new AvroSecondaryIndex.CompositeKey(components);
        } else {
            // Single column index
            return row.get(columns.get(0));
        }
    }
    
    /**
     * Get the Java class for a column
     */
    private Class<?> getColumnClass(String columnName) {
        Object typeObj = columnTypes.get(columnName);
        if (typeObj instanceof Class) {
            return (Class<?>) typeObj;
        }
        
        // Default to String if type is not properly configured
        return String.class;
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