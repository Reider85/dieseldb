package diesel;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Executes a DELETE statement: removes every row matching the WHERE
 * conditions (or all rows when there are none), preferring index lookups for
 * equality and IN conditions.
 *
 * @see Query
 */
class DeleteQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(DeleteQuery.class.getName());
    private final List<QueryParser.Condition> conditions;
    private long lastAffectedRows;

    /**
     * Creates a delete query with the given conditions.
     *
     * @param conditions the WHERE conditions, empty for deleting all rows
     */
    public DeleteQuery(List<QueryParser.Condition> conditions) {
        this.conditions = conditions;
    }

    /**
     * Returns the WHERE conditions, empty for deleting all rows.
     *
     * @return the unmodifiable condition list
     */
    public List<QueryParser.Condition> getConditions() {
        return Collections.unmodifiableList(conditions);
    }

    /**
     * Returns the number of rows the last {@link #execute} deleted, exposed
     * for EXPLAIN ANALYZE metrics.
     *
     * @return the affected row count of the last execution
     */
    long getLastAffectedRows() {
        return lastAffectedRows;
    }

    /**
     * Deletes the matching rows and removes them from every index.
     *
     * @param table the table to delete from
     * @return null on success
     */
    @Override
    public Void execute(Table table) {
        LOGGER.log(Level.FINE, "Executing DeleteQuery for table: {0}", table.getName());
        validateInput();
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<Integer> rowsToDelete = prepareDelete(table, rows, columnTypes);
        // Reader visibility: index hits can carry MVCC tombstones (index
        // entries survive until vacuum) and rows the reader may not touch.
        filterRowsForReader(table, rowsToDelete);
        List<ReentrantReadWriteLock> locks = acquireLock(table, rows, rowsToDelete);
        table.beginBulkUpdate();
        try {
            MvccReadContext.Context context = MvccReadContext.get();
            Transaction transaction = context == null ? null : context.getTransaction();
            boolean mvcc = transaction != null && transaction.isActive()
                    && !context.isBatch() && transaction.getTxid() > 0;
            if (mvcc) {
                // Pending delete: keep the physical row and its index entries
                // (undo would have to restore them); COMMIT tombstones the row.
                markRowsDeleted(table, rows, rowsToDelete, transaction);
            } else {
                performDelete(table, rows, rowsToDelete);
            }
            updateIndexes(table);
        } finally {
            table.endBulkUpdate();
            releaseLock(locks);
        }
        LOGGER.log(Level.INFO, "Deleted {0} rows from table {1}", new Object[]{rowsToDelete.size(), table.getName()});
        lastAffectedRows = rowsToDelete.size();
        return null;
    }

    /**
     * Drops rows the current reader must not delete: already-tombstoned rows,
     * rows created by a transaction whose writes this reader cannot observe,
     * and rows committed after the reader's snapshot.
     *
     * @param table        the table being deleted from
     * @param rowsToDelete candidate raw row indexes, filtered in place
     */
    private void filterRowsForReader(Table table, List<Integer> rowsToDelete) {
        if (rowsToDelete.isEmpty()) {
            return;
        }
        rowsToDelete.removeIf(rowIndex ->
                rowIndex < 0 || rowIndex >= table.getRawRowCount()
                || !table.isRowVisibleToReader(rowIndex));
    }

    /**
     * Registers every matched row as pending-deleted by this transaction:
     * optimistic write-write conflicts throw before the first mark, then each
     * row keeps its pre-delete metadata for {@link UndoLog.DeleteUndo} and is
     * recorded for COMMIT (tombstone + commit CSN stamping).
     *
     * @param table        the table being deleted from
     * @param rows         the reader's row values (shared with the storage mirror)
     * @param rowsToDelete the locked target row indexes
     * @param transaction  the explicit transaction performing the delete
     */
    private void markRowsDeleted(Table table, List<Map<String, Object>> rows,
                                 List<Integer> rowsToDelete, Transaction transaction) {
        long txid = transaction.getTxid();
        long snapshotCsn = transaction.getSnapshotCsn();
        for (int rowIndex : rowsToDelete) {
            if (transaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
                table.checkSerializableWriteConflict(rowIndex, txid, snapshotCsn);
                // Track read for SSI: we're reading this row to delete it
                transaction.getDatabase().getConflictDetector().noteRead(txid, table.getName(), rowIndex);
            } else {
                table.checkWriteWriteConflict(rowIndex, txid, snapshotCsn);
            }
        }
        for (int rowIndex : rowsToDelete) {
            Map<String, Object> preImage = new HashMap<>(rows.get(rowIndex));
            RowVersionMeta liveMeta = table.getRowVersionMeta(rowIndex);
            RowVersionMeta oldMetaCopy = liveMeta == null ? null : liveMeta.copy();
            table.markDelete(rowIndex, txid, preImage);
            transaction.getUndoLog().addUndoRecord(
                    new UndoLog.DeleteUndo(table.getName(), rowIndex, oldMetaCopy));
            transaction.noteDeletedRow(table.getName(), rowIndex);
            appendDeleteWal(table, txid, rowIndex, preImage);
            
            // Track write for SSI
            if (transaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
                transaction.getDatabase().getConflictDetector().noteWrite(txid, table.getName(), rowIndex);
            }
        }
    }

    /**
     * Appends the logical DELETE record (before-image) to the WAL when
     * enabled (prompt4 #19): the undo phase verifies the row survived without
     * a tombstone, which is what an uncommitted delete leaves behind.
     */
    private void appendDeleteWal(Table table, long txid, int rowIndex, Map<String, Object> preImage) {
        Database database = table.getDatabase();
        if (database == null || !database.isWALEnabled()) {
            return;
        }
        try {
            byte[] before = diesel.wal.DmlPayload.serialize(table.getName(), rowIndex, preImage);
            database.appendWalDml(txid, diesel.wal.WALOpcode.DELETE, before, null);
        } catch (java.io.IOException e) {
            LOGGER.log(Level.WARNING, "WAL DELETE payload failed (statement continues): " + e.getMessage());
        }
    }

    /** True when an explicit (non-batch) MVCC transaction drives this statement. */
    private boolean isMvccReader() {
        MvccReadContext.Context context = MvccReadContext.get();
        Transaction transaction = context == null ? null : context.getTransaction();
        return transaction != null && transaction.isActive()
                && !context.isBatch() && transaction.getTxid() > 0;
    }

    private void validateInput() {
        if (conditions == null) {
            throw new IllegalArgumentException("Delete conditions cannot be null");
        }
    }

    private List<ReentrantReadWriteLock> acquireLock(Table table, List<Map<String, Object>> rows, List<Integer> rowsToDelete) {
        List<ReentrantReadWriteLock> locks = new ArrayList<>();
        for (int rowIndex : rowsToDelete) {
            if (rowIndex >= 0 && rowIndex < rows.size()) {
                ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                lock.writeLock().lock();
                locks.add(lock);
            }
        }
        return locks;
    }

    private void performDelete(Table table, List<Map<String, Object>> rows, List<Integer> rowsToDelete) {
        for (int rowIndex : rowsToDelete) {
            tombstoneRow(table, rows, rowIndex);
        }
    }

    private void releaseLock(List<ReentrantReadWriteLock> locks) {
        for (ReentrantReadWriteLock lock : locks) {
            lock.writeLock().unlock();
        }
    }

    

    /**
     * Identifies rows to delete using index acceleration or full scan.
     *
     * @param table the table to delete from
     * @param rows the table rows
     * @param columnTypes mapping of column names to their types
     * @return list of row indices to delete
     */
    private List<Integer> prepareDelete(Table table, List<Map<String, Object>> rows, Map<String, Class<?>> columnTypes) {
        List<Integer> rowsToDelete = new ArrayList<>();
        if (isMvccReader()) {
            // Index keys track physical (possibly newer) values; a snapshot
            // reader must match against the versions it can actually see.
            if (conditions.isEmpty()) {
                collectAllRows(table, rows, rowsToDelete);
            } else {
                fullScanWithConditions(table, rows, columnTypes, rowsToDelete);
            }
            return rowsToDelete;
        }
        tryIndexEqualsLookup(table, columnTypes, rowsToDelete);
        if (rowsToDelete.isEmpty()) {
            tryIndexInLookup(table, columnTypes, rowsToDelete);
        }
        if (rowsToDelete.isEmpty() && !conditions.isEmpty()) {
            fullScanWithConditions(table, rows, columnTypes, rowsToDelete);
        } else if (conditions.isEmpty()) {
            collectAllRows(table, rows, rowsToDelete);
        }
        return rowsToDelete;
    }

    private void tryIndexEqualsLookup(Table table, Map<String, Class<?>> columnTypes, List<Integer> rowsToDelete) {
        if (conditions.size() != 1 || conditions.get(0).isGrouped()
                || conditions.get(0).operator != QueryParser.Operator.EQUALS || conditions.get(0).not) {
            return;
        }
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof HashIndex || index instanceof UniqueIndex) {
            Object conditionValue = EVAL.convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column));
            rowsToDelete.addAll(index.search(conditionValue));
            LOGGER.log(Level.INFO, "Using {0} index for column {1} with value {2}",
                    new Object[]{index instanceof HashIndex ? "hash" : "unique", condition.column, conditionValue});
        } else if (index instanceof BTreeIndex btree) {
            Object conditionValue = EVAL.convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column));
            rowsToDelete.addAll(btree.search(conditionValue));
            LOGGER.log(Level.INFO, "Using B-tree index for column {0} with value {1}", new Object[]{condition.column, conditionValue});
        }
    }

    private void tryIndexInLookup(Table table, Map<String, Class<?>> columnTypes, List<Integer> rowsToDelete) {
        if (conditions.size() != 1 || conditions.get(0).isGrouped()
                || !conditions.get(0).isInOperator() || conditions.get(0).not) {
            return;
        }
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof HashIndex || index instanceof UniqueIndex || index instanceof BTreeIndex) {
            for (Object value : condition.inValues) {
                Object convertedValue = EVAL.convertConditionValue(value, condition.column, columnTypes.get(condition.column));
                List<Integer> indices = index.search(convertedValue);
                rowsToDelete.addAll(indices);
            }
            List<Integer> deduped = rowsToDelete.stream().distinct().sorted().collect(Collectors.toList());
            rowsToDelete.clear();
            rowsToDelete.addAll(deduped);
            LOGGER.log(Level.INFO, "Using {0} index for IN query on column {1} with values {2}",
                    new Object[]{indexTypeName(index),
                            condition.column, condition.inValues});
        }
    }

    private String indexTypeName(Index index) {
        if (index instanceof HashIndex) return "hash";
        if (index instanceof BTreeIndex) return "B-tree";
        return "unique";
    }

    private void fullScanWithConditions(Table table, List<Map<String, Object>> rows, Map<String, Class<?>> columnTypes, List<Integer> rowsToDelete) {
        IntStream.range(0, rows.size())
                .filter(i -> !table.isDeleted(i))
                .forEach(i -> {
                    // Match against the reader-visible version: a snapshot
                    // reader must not target rows through another writer's
                    // newer values (shadow model, prompt4.md #4).
                    if (evaluateConditions(table.getVisibleRowForReader(i, rows.get(i)), conditions, columnTypes)) {
                        rowsToDelete.add(i);
                    }
                });
    }

    private void collectAllRows(Table table, List<Map<String, Object>> rows, List<Integer> rowsToDelete) {
        IntStream.range(0, rows.size())
                .filter(i -> !table.isDeleted(i))
                .forEach(rowsToDelete::add);
    }

 List<ReentrantReadWriteLock> acquireRowLocks(Table table, List<Map<String, Object>> rows, List<Integer> rowsToDelete) {
        List<ReentrantReadWriteLock> locks = new ArrayList<>();
        for (int rowIndex : rowsToDelete) {
            if (rowIndex >= 0 && rowIndex < rows.size()) {
                ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                lock.writeLock().lock();
                locks.add(lock);
            }
        }
        return locks;
    }

    private void tombstoneRow(Table table, List<Map<String, Object>> rows, int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size() || table.isDeleted(rowIndex)) {
            return;
        }
        Map<String, Object> row = rows.get(rowIndex);
        for (Map.Entry<String, Index> entry : table.getIndexes().entrySet()) {
            String column = entry.getKey();
            Index index = entry.getValue();
            Object key = row.get(column);
            if (key != null) {
                index.remove(key, rowIndex);
            }
        }
        if (table.hasClusteredIndex()) {
            Object clusteredKey = row.get(table.getClusteredIndexColumn());
            if (clusteredKey != null) {
                table.getClusteredIndex().remove(clusteredKey, rowIndex);
            }
        }
        table.markDeleted(rowIndex);
        LOGGER.log(Level.INFO, "Tombstoned row at index {0} from table {1}", new Object[]{rowIndex, table.getName()});
    }

    /**
     * Updates indexes after deletion, performing auto-compaction if needed.
     *
     * @param table the table to update
     */
    private void updateIndexes(Table table) {
        // Phase 4: Auto-compact if tombstone threshold reached
        int rawCount = table.getRawRowCount();
        if (rawCount > 0 && (double) table.getDeletedCount() / rawCount >= 0.3) {
            LOGGER.log(Level.INFO, "Tombstone ratio >= 0.3, compacting table {0}", table.getName());
            table.compact();
        }
    }

    private static final ConditionEvaluator EVAL = new ConditionEvaluator();

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return EVAL.evaluateConditions(row, conditions, columnTypes);
    }
}