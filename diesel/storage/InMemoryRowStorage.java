package diesel.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Purely in-memory implementation of {@link RowStorage}. Rows are held in an
 * {@link ArrayList} of column-to-value maps. No persistence is performed; callers
 * are responsible for synchronising with durable storage via
 * {@link #saveToFile}/{@link #loadFromFile} when needed.
 */
public class InMemoryRowStorage extends AbstractRowStorage {

    protected final List<Map<String, Object>> rows = new ArrayList<>();

    /**
     * @param tableName   the table name
     * @param columns     the ordered list of column names
     * @param columnTypes the column name to type mapping
     */
    public InMemoryRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
    }

    @Override
    public void open() {
        // No resources to acquire for in-memory storage.
    }

    @Override
    public void close() {
        // No resources to release for in-memory storage.
    }

    @Override
    public List<Map<String, Object>> scan() {
        return new ArrayList<>(rows);
    }

    @Override
    public void insert(Map<String, Object> row) {
        rows.add(new HashMap<>(row));
    }

    @Override
    public void insertAt(int rowIndex, Map<String, Object> row) {
        rows.add(rowIndex, new HashMap<>(row));
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        rows.set(rowIndex, new HashMap<>(row));
    }

    @Override
    public void delete(int rowIndex) {
        rows.remove(rowIndex);
    }

    @Override
    public void saveToFile(String tableName) {
        // No persistence for pure in-memory storage.
    }

    @Override
    public void loadFromFile(String tableName) {
        // No persistence for pure in-memory storage.
    }

    /**
     * Returns the internal row list directly (no copy). Intended for use by
     * {@link diesel.Table} which needs direct index-based access for
     * performance-sensitive code paths.
     *
     * @return the mutable row list
     */
    public List<Map<String, Object>> getInternalRows() {
        return rows;
    }

    /**
     * Replaces the internal row list (used during transaction copy and
     * deserialization).
     *
     * @param newRows the new row list
     */
    @Override
    public void setRows(List<Map<String, Object>> newRows) {
        rows.clear();
        rows.addAll(newRows);
    }

    @Override
    public StorageMetadata getMetadata() {
        Map<String, Object> properties = Map.of(
            "rowCount", rows.size(),
            "maxMemoryUsage", estimateMemoryUsage()
        );
        
        return new StorageMetadata(
            this.getClass().getSimpleName(),
            false, // No persistence for in-memory storage
            false, // No MVCC support in base implementation
            properties
        );
    }

    @Override
    public boolean supportsPersistence() {
        return false; // In-memory storage doesn't persist
    }

    @Override
    public boolean supportsMVCC() {
        return false; // Base implementation doesn't support MVCC
    }

    /**
     * Estimates the current memory usage of this storage.
     *
     * @return estimated memory usage in bytes
     */
    private long estimateMemoryUsage() {
        long total = 0;
        for (Map<String, Object> row : rows) {
            total += 16; // ArrayList overhead per row
            total += row.size() * 32; // HashMap overhead
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                total += entry.getKey().length() * 2; // String chars (2 bytes each)
                total += estimateObjectSize(entry.getValue());
            }
        }
        return total;
    }

    /**
     * Estimates the size of an object in memory.
     *
     * @param obj the object to estimate
     * @return estimated size in bytes
     */
    private long estimateObjectSize(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof String) return ((String) obj).length() * 2;
        if (obj instanceof Integer) return 4;
        if (obj instanceof Long) return 8;
        if (obj instanceof Double) return 8;
        if (obj instanceof Boolean) return 1;
        return 16; // Default estimate for complex objects
    }
}
