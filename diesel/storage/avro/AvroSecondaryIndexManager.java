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
    
    private transient List<String> columns;
    private transient Map<String, Class<?>> columnTypes;
    
    /**
     * Case-insensitive column-name to position map (o2 Avro-15). Built once per
     * manager instance (constructor + sidecar {@code readObject}); positions index
     * into {@code columns} and stay valid because {@code columns} is never mutated
     * after construction. Survives {@code .asi} reload as a transient rebuild —
     * never serialized.
     */
    private transient Map<String, Integer> columnIndex;
    
    // Map: indexName -> AvroSecondaryIndex
    private transient Map<String, AvroSecondaryIndex> indexes;
    
    // Persistence file extension
    private static final String INDEX_FILE_EXTENSION = ".asi";
    
    public AvroSecondaryIndexManager(String tableName, List<String> columns, Map<?, ?> columnTypes) {
        this.columns = new ArrayList<>(columns);
        rebuildColumnIndex();
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
        // o2 Avro-15: positional case-insensitive check via the pre-built index
        // instead of columns.get(i).equalsIgnoreCase — O(log columns) per entry.
        // CI-duplicate column names collapse to the last position in columnIndex
        // and fail this check conservatively (sidecar rejected -> rebuild fallback).
        for (int i = 0; i < columns.size(); i++) {
            Integer pos = columnIndex.get(expectedColumns.get(i));
            if (pos == null || pos != i) {
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

    /**
     * Rebuilds the case-insensitive {@link #columnIndex} from {@link #columns}.
     * Null column names are skipped (TreeMap CI compare rejects them; the previous
     * linear scan tolerated them by returning -1 for a null query).
     */
    private void rebuildColumnIndex() {
        Map<String, Integer> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                if (column != null) {
                    map.put(column, i);
                }
            }
        }
        this.columnIndex = map;
    }

    /** o2 Avro-15: O(log columns) map lookup instead of an O(columns) linear scan. */
    private int findColumnIndex(String columnName) {
        if (columnName == null || columnIndex == null) {
            return -1;
        }
        Integer pos = columnIndex.get(columnName);
        return pos != null ? pos : -1;
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
        // o2 Avro-14: one lazy case-insensitive view per call, amortized over all indexes
        RowCiView view = new RowCiView(row);
        for (AvroSecondaryIndex index : indexes.values()) {
            Object key = buildKeyForIndex(view, index);
            if (key != null) {
                index.insert(key, rowIndex);
            }
        }
    }
    
    /**
     * Synchronize index on update operation
     */
    public synchronized void syncOnUpdate(Map<String, Object> oldRow, Map<String, Object> newRow, int rowIndex) {
        // o2 Avro-14: one lazy view per row (old/new separately; sharing a built
        // CI map between them would leak values — each view caches its own row's keys)
        RowCiView oldView = new RowCiView(oldRow);
        RowCiView newView = new RowCiView(newRow);
        for (AvroSecondaryIndex index : indexes.values()) {
            Object oldKey = buildKeyForIndex(oldView, index);
            Object newKey = buildKeyForIndex(newView, index);
            
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
        RowCiView view = new RowCiView(row);
        for (AvroSecondaryIndex index : indexes.values()) {
            Object key = buildKeyForIndex(view, index);
            if (key != null) {
                index.remove(key, rowIndex);
            }
        }
    }
    
    /**
     * Rebuild all indexes from current data.
     *
     * <p>o2 Avro-13: every row is wrapped in one {@link RowCiView} shared by all
     * indexes of that row — exact-match lookups stay O(1) hashmap hits and the
     * first case-insensitive miss builds a single per-row TreeMap, replacing the
     * former O(row_size) entrySet scan per (row, index, column).</p>
     */
    public synchronized void rebuildAllIndexes(List<Map<String, Object>> allRows) {
        for (AvroSecondaryIndex index : indexes.values()) {
            index.clear();
        }

        for (int rowIndex = 0; rowIndex < allRows.size(); rowIndex++) {
            RowCiView view = new RowCiView(allRows.get(rowIndex));
            for (AvroSecondaryIndex index : indexes.values()) {
                Object key = buildKeyForIndex(view, index);
                if (key != null) {
                    index.insert(key, rowIndex);
                }
            }
        }
    }

    /**
     * Array-backed rebuild (o2 Avro-19, enables Avro-13's delegate path): values
     * are read positionally from the {@code Object[]} rows, so a bulk load pays
     * zero {@code Map} key scanning — per (row, index, covered column) the cost is
     * one O(log columns) position lookup against the schema-level
     * {@link #columnIndex}.
     *
     * <p><b>Position invariant:</b> the element layout of {@code allRows} must
     * match this manager's {@code columns} order. {@link AvroRowStorage} guarantees
     * it — the same {@code columns} list is passed into this manager's constructor
     * and drives {@code colNames}/{@code fromMap}; sidecar-reloaded managers are
     * position-checked by {@link #isCompatible} before their indexes are trusted.</p>
     */
    public synchronized void rebuildAllIndexesFromArrays(List<Object[]> allRows) {
        for (AvroSecondaryIndex index : indexes.values()) {
            index.clear();
        }

        for (int rowIndex = 0; rowIndex < allRows.size(); rowIndex++) {
            Object[] row = allRows.get(rowIndex);
            for (AvroSecondaryIndex index : indexes.values()) {
                Object key = buildKeyForIndexFromArray(row, index);
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
     * Per-row case-insensitive value view (o2 Avro-13/Avro-14). Exact-match
     * lookups stay O(1) hashmap hits — the CI TreeMap is built lazily on the FIRST
     * case-insensitive miss and then amortized over every index/column lookup of
     * that row, replacing the former O(row_size) {@code entrySet} scan per lookup.
     */
    private static final class RowCiView {
        private final Map<String, Object> row;
        private Map<String, Object> ciRow;

        RowCiView(Map<String, Object> row) {
            this.row = row;
        }

        Object get(String columnName) {
            if (row == null || columnName == null) {
                return null;
            }
            if (row.containsKey(columnName)) {
                return row.get(columnName);
            }
            if (ciRow == null) {
                TreeMap<String, Object> ci = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    // TreeMap(CI) rejects null keys; the old equalsIgnoreCase scan skipped them
                    if (entry.getKey() != null) {
                        ci.put(entry.getKey(), entry.getValue());
                    }
                }
                ciRow = ci;
            }
            return ciRow.get(columnName);
        }
    }

    /**
     * Build a key for a specific index from a row view.
     */
    private Object buildKeyForIndex(RowCiView row, AvroSecondaryIndex index) {
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
     * Array-backed key build (o2 Avro-19): positional access through the
     * schema-level {@link #columnIndex}, no map key scanning at all.
     */
    private Object buildKeyForIndexFromArray(Object[] row, AvroSecondaryIndex index) {
        List<String> covered = index.getCoversColumns();
        if (covered.size() > 1) {
            Object[] components = new Object[covered.size()];
            for (int i = 0; i < covered.size(); i++) {
                components[i] = getArrayValue(row, covered.get(i));
            }
            return new AvroSecondaryIndex.CompositeKey(components);
        }
        return getArrayValue(row, covered.get(0));
    }

    private Object getArrayValue(Object[] row, String columnName) {
        if (row == null || columnName == null) {
            return null;
        }
        Integer pos = columnIndex.get(columnName);
        if (pos == null || pos >= row.length) {
            return null;
        }
        return row[pos];
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
        rebuildColumnIndex(); // o2 Avro-15: transient index rebuilt on sidecar load
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