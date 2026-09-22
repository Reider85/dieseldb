package diesel.storage.avro;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * B-Tree secondary index for AVRO data.
 * 
 * Features:
 * - TreeMap-based O(log n) lookups and range scans
 * - Composite index support via CompositeKey records
 * - Usage statistics tracking
 * - Thread-safe operations
 */
public class AvroSecondaryIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Index metadata
    private final String indexName;
    private final String columnName;
    private final Class<?> keyType;
    private final boolean isComposite;
    private final List<String> compositeColumns;
    
    // B-Tree structure: key -> list of row indices
    private final Map<Object, List<Integer>> indexMap;
    
    // Statistics
    private long lookupCount = 0;
    private long rangeScanCount = 0;
    private long totalResults = 0;
    private long lastAccessTime = System.currentTimeMillis();
    
    public AvroSecondaryIndex(String indexName, String columnName, Class<?> keyType) {
        this.indexName = indexName;
        this.columnName = columnName;
        this.keyType = keyType;
        this.isComposite = false;
        this.compositeColumns = Collections.emptyList();
        this.indexMap = new ConcurrentSkipListMap<>();
    }
    
    public AvroSecondaryIndex(String indexName, List<String> compositeColumns, Class<?> keyType) {
        this.indexName = indexName;
        this.columnName = String.join("_", compositeColumns);
        this.keyType = keyType;
        this.isComposite = true;
        this.compositeColumns = Collections.unmodifiableList(new ArrayList<>(compositeColumns));
        this.indexMap = new ConcurrentSkipListMap<>();
    }
    
    public void insert(Object key, int rowIndex) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null for index: " + indexName);
        }
        
        validateKeyType(key);
        
        indexMap.compute(key, (k, existing) -> {
            List<Integer> rows = existing != null ? existing : new ArrayList<>();
            if (!rows.contains(rowIndex)) {
                rows.add(rowIndex);
                Collections.sort(rows); // Maintain sorted order for range queries
            }
            return rows;
        });
        
        lastAccessTime = System.currentTimeMillis();
    }
    
    public void remove(Object key, int rowIndex) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null for index: " + indexName);
        }
        
        validateKeyType(key);
        
        indexMap.computeIfPresent(key, (k, rows) -> {
            rows.remove(Integer.valueOf(rowIndex));
            return rows.isEmpty() ? null : rows;
        });
        
        lastAccessTime = System.currentTimeMillis();
    }
    
    public List<Integer> search(Object key) {
        if (key == null) {
            return Collections.emptyList();
        }
        
        validateKeyType(key);
        lookupCount++;
        lastAccessTime = System.currentTimeMillis();
        
        List<Integer> result = indexMap.get(key);
        if (result != null) {
            totalResults += result.size();
            return new ArrayList<>(result);
        }
        return Collections.emptyList();
    }
    
    /**
     * Range search for single-column indexes
     */
    public List<Integer> rangeSearch(Object startKey, Object endKey, boolean inclusiveStart, boolean inclusiveEnd) {
        if (isComposite) {
            throw new IllegalStateException("Range search not supported for composite indexes");
        }
        
        if (startKey == null || endKey == null) {
            return Collections.emptyList();
        }
        
        validateKeyType(startKey);
        validateKeyType(endKey);
        
        rangeScanCount++;
        lastAccessTime = System.currentTimeMillis();
        
        List<Integer> allResults = new ArrayList<>();
        
        // Use NavigableMap for efficient range queries
        NavigableMap<Object, List<Integer>> subMap = ((NavigableMap<Object, List<Integer>>) indexMap)
            .subMap(startKey, inclusiveStart, endKey, inclusiveEnd);
        
        for (List<Integer> rows : subMap.values()) {
            allResults.addAll(rows);
        }
        
        Collections.sort(allResults);
        totalResults += allResults.size();
        
        return allResults;
    }
    
    /**
     * Composite index search using CompositeKey
     */
    public List<Integer> compositeSearch(CompositeKey key) {
        if (!isComposite) {
            throw new IllegalStateException("Composite search only supported for composite indexes");
        }
        
        if (key == null) {
            return Collections.emptyList();
        }
        
        validateKeyType(key);
        lookupCount++;
        lastAccessTime = System.currentTimeMillis();
        
        List<Integer> result = indexMap.get(key);
        if (result != null) {
            totalResults += result.size();
            return new ArrayList<>(result);
        }
        return Collections.emptyList();
    }
    
    /**
     * Prefix search for composite indexes (search by leading columns)
     */
    public List<Integer> prefixSearch(CompositeKey prefix) {
        if (!isComposite) {
            throw new IllegalStateException("Prefix search only supported for composite indexes");
        }
        
        if (prefix == null) {
            return Collections.emptyList();
        }
        
        validateKeyType(prefix);
        rangeScanCount++;
        lastAccessTime = System.currentTimeMillis();
        
        List<Integer> allResults = new ArrayList<>();
        
        // Find all keys that start with the prefix
        for (Map.Entry<Object, List<Integer>> entry : indexMap.entrySet()) {
            if (entry.getKey() instanceof CompositeKey) {
                CompositeKey key = (CompositeKey) entry.getKey();
                if (key.startsWith(prefix)) {
                    allResults.addAll(entry.getValue());
                }
            }
        }
        
        Collections.sort(allResults);
        totalResults += allResults.size();
        
        return allResults;
    }
    
    /**
     * Get all keys in the index (for debugging/statistics)
     */
    public Set<Object> getAllKeys() {
        return new HashSet<>(indexMap.keySet());
    }
    
    /**
     * Get total number of entries in the index
     */
    public int size() {
        int total = 0;
        for (List<Integer> rows : indexMap.values()) {
            total += rows.size();
        }
        return total;
    }
    
    /**
     * Get statistics about index usage
     */
    public IndexStatistics getStatistics() {
        double avgResultSize = lookupCount > 0 ? (double) totalResults / lookupCount : 0.0;
        
        return new IndexStatistics(
            indexName,
            columnName,
            isComposite,
            indexMap.size(),
            size(),
            lookupCount,
            rangeScanCount,
            avgResultSize,
            lastAccessTime
        );
    }
    
    /**
     * Clear all data from the index
     */
    public void clear() {
        indexMap.clear();
        lookupCount = 0;
        rangeScanCount = 0;
        totalResults = 0;
        lastAccessTime = System.currentTimeMillis();
    }
    
    private void validateKeyType(Object key) {
        if (key != null && !keyType.isInstance(key)) {
            throw new IllegalArgumentException("Key type mismatch. Expected: " + keyType + 
                ", Got: " + key.getClass() + " for index: " + indexName);
        }
    }
    
    public Class<?> getKeyType() {
        return keyType;
    }
    
    public List<String> getCoversColumns() {
        return isComposite ? compositeColumns : Collections.singletonList(columnName);
    }
    
    public boolean coversColumns(Set<String> columns) {
        if (isComposite) {
            return columns.containsAll(compositeColumns);
        } else {
            return columns.contains(columnName);
        }
    }
    
    public Map<String, Object> getCoveredValues(int rowIndex) {
        // For secondary indexes, we don't store additional column values
        return null;
    }
    
    /**
     * Composite key for multi-column indexes
     */
    public static record CompositeKey(Object[] components) {
        public CompositeKey(Object[] components) {
            if (components == null || components.length == 0) {
                throw new IllegalArgumentException("Composite key must have at least one component");
            }
            this.components = Arrays.copyOf(components, components.length);
        }
        
        public boolean startsWith(CompositeKey prefix) {
            if (prefix.components.length > this.components.length) {
                return false;
            }
            
            for (int i = 0; i < prefix.components.length; i++) {
                if (!Objects.equals(this.components[i], prefix.components[i])) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeKey that = (CompositeKey) o;
            return Arrays.equals(components, that.components);
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(components);
        }
        
        @Override
        public String toString() {
            return Arrays.toString(components);
        }
    }
    
    /**
     * Statistics for index usage tracking
     */
    public static record IndexStatistics(
        String indexName,
        String columnName,
        boolean isComposite,
        int uniqueKeys,
        int totalEntries,
        long lookupCount,
        long rangeScanCount,
        double avgResultSize,
        long lastAccessTime
    ) {}
}