package diesel.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Consumer;

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

/**
      * Removes index entries for rows the caller has identified as dead, so
      * that the subsequent full rebuild ({@code Table.compact()}) no longer
      * indexes them. The caller guarantees the dead rows are physically
      * removed right after, so implementations must not shift surviving
      * rows' positions. The default implementation is a no-op for storages
      * that do not maintain indexes.
      *
      * @param deadPositions physical row positions of dead rows
      * @return the number of index entries removed
      */
    default int removeDeadEntries(Set<Integer> deadPositions) {
        return 0;
    }

    // ─── Enhanced Default Methods to Reduce Code Duplication ─────────────

    /**
     * Null-safe static helper: executes {@code operation} when
     * {@code storage} is non-null, otherwise does nothing. Callers pass the
     * storage as an argument (an instance method cannot be invoked on a null
     * receiver — the JVM throws NPE before the method body runs).
     *
     * @param storage the storage to operate on, may be {@code null}
     * @param operation the operation to execute on this storage
     */
    static void callWith(RowStorage storage, Consumer<RowStorage> operation) {
        if (storage != null && operation != null) {
            operation.accept(storage);
        }
    }

    /**
     * Null-safe static helper: executes {@code operation} when both
     * {@code storage} and {@code operation} are non-null, otherwise returns
     * {@code defaultValue}.
     *
     * @param storage the storage to operate on, may be {@code null}
     * @param operation the operation to execute on this storage
     * @param defaultValue the value to return when storage/operation is null
     * @return the result of the operation or {@code defaultValue}
     */
    static <T> T callWith(RowStorage storage, Function<RowStorage, T> operation, T defaultValue) {
        if (storage == null || operation == null) {
            return defaultValue;
        }
        return operation.apply(storage);
    }

    // ─── Batch Operations for Performance ─────────────────────────────────

    /**
     * Appends multiple new rows to the storage. Default implementation
     * iterates over individual insert calls.
     *
     * @param rows the list of column-to-value maps representing the rows
     */
    default void bulkInsert(List<Map<String, Object>> rows) {
        if (rows != null) {
            for (Map<String, Object> row : rows) {
                insert(row);
            }
        }
    }

    /**
     * Updates multiple rows at the given indices. Default implementation
     * iterates over individual update calls.
     *
     * @param rowIndices the zero-based indices of the rows to update
     * @param newRows the new column-to-value maps for each row
     * @throws IllegalArgumentException if rowIndices and newRows size mismatch
     */
    default void bulkUpdate(List<Integer> rowIndices, List<Map<String, Object>> newRows) {
        if (rowIndices == null || newRows == null || rowIndices.size() != newRows.size()) {
            throw new IllegalArgumentException("Row indices and new rows must match in size");
        }
        for (int i = 0; i < rowIndices.size(); i++) {
            update(rowIndices.get(i), newRows.get(i));
        }
    }

    /**
     * Deletes multiple rows at the given indices. Default implementation
     * iterates over individual delete calls.
     *
     * @param rowIndices the zero-based indices of the rows to delete
     */
    default void bulkDelete(List<Integer> rowIndices) {
        if (rowIndices != null) {
            // Delete in reverse order to avoid index shifting issues
            for (int i = rowIndices.size() - 1; i >= 0; i--) {
                delete(rowIndices.get(i));
            }
        }
    }

    // ─── MVCC Support Methods ───────────────────────────────────────────

    /**
     * Inserts a row with MVCC transaction metadata. Default implementation
     * delegates to insert() for storages that don't support MVCC.
     *
     * @param row the column-to-value map representing the row
     * @param txid the transaction ID
     */
    default void insertWithMVCC(Map<String, Object> row, long txid) {
        insert(row);
    }

    /**
     * Updates a row with MVCC transaction metadata. Default implementation
     * delegates to update() for storages that don't support MVCC.
     *
     * @param rowIndex the zero-based index of the row to update
     * @param row the new column-to-value map
     * @param txid the transaction ID
     */
    default void updateWithMVCC(int rowIndex, Map<String, Object> row, long txid) {
        update(rowIndex, row);
    }

    /**
     * Deletes a row with MVCC transaction metadata. Default implementation
     * delegates to delete() for storages that don't support MVCC.
     *
     * @param rowIndex the zero-based index of the row to delete
     * @param txid the transaction ID
     */
    default void deleteWithMVCC(int rowIndex, long txid) {
        delete(rowIndex);
    }

    // ─── Storage Metadata ─────────────────────────────────────────────────

    /**
     * Returns metadata about this storage implementation. Default implementation
     * returns basic information.
     *
     * @return storage metadata
     */
    default StorageMetadata getMetadata() {
        return new StorageMetadata(this.getClass().getSimpleName(), false, false);
    }

    /**
     * Sets metadata for this storage. Default implementation is a no-op.
     *
     * @param metadata the storage metadata
     */
    default void setMetadata(StorageMetadata metadata) {
        // No-op for storages that don't use metadata
    }

    /**
     * Returns whether this storage supports persistence. Default implementation
     * returns false.
     *
     * @return true if persistence is supported
     */
    default boolean supportsPersistence() {
        return false;
    }

    /**
     * Returns whether this storage supports MVCC. Default implementation
     * returns false.
     *
     * @return true if MVCC is supported
     */
    default boolean supportsMVCC() {
        return false;
    }
}
