package diesel.storage;

import java.util.List;
import java.util.Map;

/**
 * Storage contract for table row data. Implementations handle the physical
 * storage of rows (in-memory, file-based, etc.) while the {@link diesel.Table}
 * class manages schema, indexes, locking and business logic.
 *
 * <p>Adding a new storage backend requires only:
 * <ol>
 *   <li>Creating a class that implements {@code RowStorage} (or extending
 *       {@link AbstractRowStorage})</li>
 *   <li>Registering it in {@link StorageFactory}</li>
 * </ol>
 *
 * @see AbstractRowStorage
 * @see InMemoryRowStorage
 * @see CsvRowStorage
 * @see TsvRowStorage
 * @see StorageFactory
 */
public interface RowStorage {

    /**
     * Initialises the storage backend (opens files, allocates buffers, etc.).
     */
    void open();

    /**
     * Releases all resources held by this storage backend.
     */
    void close();

    /**
     * Returns a snapshot copy of all rows currently held by this storage.
     *
     * @return list of column-to-value maps, one per row
     */
    List<Map<String, Object>> scan();

    /**
     * Appends a new row to the storage.
     *
     * @param row the column-to-value map representing the row
     */
    void insert(Map<String, Object> row);

    /**
     * Replaces the row at {@code rowIndex} with the supplied row.
     *
     * @param rowIndex the zero-based index of the row to replace
     * @param row      the new column-to-value map
     */
    void update(int rowIndex, Map<String, Object> row);

    /**
     * Removes the row at {@code rowIndex}.
     *
     * @param rowIndex the zero-based index of the row to remove
     */
    void delete(int rowIndex);

    /**
     * Persists the current in-memory state to durable storage.
     *
     * @param tableName the table name, used as the file base name
     */
    void saveToFile(String tableName);

    /**
     * Loads durable storage data into memory.
     *
     * @param tableName the table name, used as the file base name
     */
    void loadFromFile(String tableName);
}
