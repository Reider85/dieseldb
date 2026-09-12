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
     * Inserts a row at the given index, shifting subsequent rows down by one.
     * Used to keep the storage aligned with the table's clustered-PK order.
     *
     * @param rowIndex the zero-based index at which to insert the row
     * @param row      the column-to-value map representing the row
     */
    void insertAt(int rowIndex, Map<String, Object> row);

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
     * Replaces the entire row set of this storage. Used when the engine
     * rebuilds its row list (e.g. compaction) so the storage mirror stays
     * aligned with the table's live rows.
     *
     * @param newRows the replacement rows, in order
     */
    void setRows(List<Map<String, Object>> newRows);

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

    /**
     * Informs the storage of the table's primary-key column. Index-aware
     * backends (see {@link AbstractRowStorage}) use it to maintain a
     * primary-key index for fast lookups and parallel bulk loads. The default
     * implementation ignores the hint.
     *
     * @param primaryKeyColumn the primary-key column name, or {@code null} for none
     */
    default void setPrimaryKeyColumn(String primaryKeyColumn) {
        // No-op for storages without a dedicated index manager.
    }

    /**
     * Enters a deferred bulk-update window (prompt 35). Inside the window,
     * index-aware backends skip per-operation index rebuilds and position
     * shifting, marking their index state dirty instead. Queries of the index
     * structures must not be run until {@link #endBulkUpdate()} has performed
     * the single deferred rebuild. The default implementation is a no-op for
     * storages that do not maintain indexes.
     */
    default void beginBulkUpdate() {
        // No-op for storages without a dedicated index manager.
    }

    /**
     * Leaves a deferred bulk-update window. Index-aware backends perform the
     * single index rebuild accumulated while dirty, so the index state is
     * consistent afterwards. Must be paired with {@link #beginBulkUpdate()},
     * even on exceptional paths. The default implementation is a no-op for
     * storages that do not maintain indexes.
     */
    default void endBulkUpdate() {
        // No-op for storages without a dedicated index manager.
    }
}
