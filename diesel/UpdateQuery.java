package diesel;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Executes an UPDATE statement: for every row matching the WHERE conditions,
 * applies the SET assignments, maintaining the secondary indexes.
 *
 * <p>Conditions are evaluated with SQL three-valued logic (see
 * {@link ThreeValuedLogic}), so rows with null values behave like in SQL.
 *
 * @see Query
 */
class UpdateQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(UpdateQuery.class.getName());

    /**
     * Row count threshold above which bulk update mode (disable indices,
     * update rows, rebuild all indexes) is used instead of per-row index
     * maintenance.
     */
    private static final int BULK_UPDATE_THRESHOLD =
            Integer.getInteger("diesel.bulkUpdate.threshold", 100);

    private final Map<String, Object> updates;
    private final List<QueryParser.Condition> conditions;
    private long lastAffectedRows;

    /**
     * Returns the SET column to new-value assignments.
     *
     * @return the unmodifiable updates map
     */
    public Map<String, Object> getUpdates() {
        return Collections.unmodifiableMap(updates);
    }

    /**
     * Returns the WHERE conditions, empty for updating all rows.
     *
     * @return the unmodifiable condition list
     */
    public List<QueryParser.Condition> getConditions() {
        return Collections.unmodifiableList(conditions);
    }

    /**
     * Returns the number of rows the last {@link #execute} matched, exposed
     * for EXPLAIN ANALYZE metrics.
     *
     * @return the affected row count of the last execution
     */
    long getLastAffectedRows() {
        return lastAffectedRows;
    }

    /**
     * Creates an update query with the given SET assignments and conditions.
     *
     * @param updates    the column to new-value map
     * @param conditions the WHERE conditions, empty for updating all rows
     */
    public UpdateQuery(Map<String, Object> updates, List<QueryParser.Condition> conditions) {
        this.updates = updates;
        this.conditions = conditions;
    }

    /**
     * Finds the matching rows (index-accelerated when possible), locks them,
     * converts the new values to the column types and applies the updates,
     * keeping the indexes in sync.  When the number of affected rows exceeds
     * {@link #BULK_UPDATE_THRESHOLD}, a bulk path is used that disables
     * indices, applies all mutations, then rebuilds indexes in a single pass.
     *
     * @param table the table to update
     * @return null on success
     * @throws IllegalArgumentException if a value cannot be converted to its
     *                                  column type
     */
    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        List<Integer> rowsToUpdate = new ArrayList<>();

        try {
            // Phase 1: Identify rows to update (index-accelerated or full scan)
            identifyRows(table, rows, columnTypes, rowsToUpdate);
            // Phase 1b: Apply reader visibility — index hits can carry
            // tombstones left by MVCC deletes (index entries survive until
            // vacuum) and rows the reader may not see at all.
            filterRowsForReader(table, rowsToUpdate);

            // Phase 2: Acquire write locks
            for (int rowIndex : rowsToUpdate) {
                ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                lock.writeLock().lock();
                acquiredLocks.add(lock);
            }

            // Phase 3: MVCC pending-change registration. Conflict-check every
            // target row first so a late failure leaves no stale pending flags,
            // then capture pre-images for undo before any value is mutated.
            MvccReadContext.Context context = MvccReadContext.get();
            Transaction transaction = context == null ? null : context.getTransaction();
            boolean mvcc = transaction != null && transaction.isActive()
                    && !context.isBatch() && transaction.getTxid() > 0;
            if (mvcc) {
                markRowsUpdated(table, rows, rowsToUpdate, transaction);
            }

            int affectedCount = rowsToUpdate.size();

            if (affectedCount >= BULK_UPDATE_THRESHOLD) {
                applyBulkUpdate(rows, rowsToUpdate, columnTypes, table);
            } else {
                applyPerRowUpdate(rows, rowsToUpdate, columnTypes, table);
            }

            // Phase 4: Statistics + version bump + logging
            if (affectedCount > 0) {
                table.bumpVersion();
            }
            table.markStatsDirty();
            LOGGER.log(Level.INFO, "Updated {0} rows in table {1}",
                    new Object[]{affectedCount, table.getName()});
            lastAffectedRows = affectedCount;
            return null;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Drops rows the current reader must not update: tombstoned rows (MVCC
     * deletes keep their index entries until vacuum) and rows filtered by the
     * MVCC visibility rules (pending foreign inserts, rows committed after the
     * reader's snapshot). No-op when the table carries no version metadata.
     *
     * @param table        the table being updated
     * @param rowsToUpdate candidate raw row indexes, filtered in place
     */
    private void filterRowsForReader(Table table, List<Integer> rowsToUpdate) {
        if (rowsToUpdate.isEmpty()) {
            return;
        }
        rowsToUpdate.removeIf(rowIndex ->
                rowIndex < 0 || rowIndex >= table.getRawRowCount()
                || !table.isRowVisibleToReader(rowIndex));
    }

    /**
     * Registers every matched row as pending-updated by this transaction:
     * optimistic write-write conflicts throw before the first mark, then each
     * row gets a pre-image snapshot for {@link UndoLog.UpdateUndo} and an entry
     * in the transaction's modified-row set (so COMMIT stamps it with the
     * commit CSN).
     *
     * @param table       the table being updated
     * @param rows        the reader's row values (shared with the storage mirror)
     * @param rowsToUpdate the locked target row indexes
     * @param transaction the explicit transaction performing the update
     */
    private void markRowsUpdated(Table table, List<Map<String, Object>> rows,
                                 List<Integer> rowsToUpdate, Transaction transaction) {
        long txid = transaction.getTxid();
        long snapshotCsn = transaction.getSnapshotCsn();
        for (int rowIndex : rowsToUpdate) {
            if (transaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
                table.checkSerializableWriteConflict(rowIndex, txid, snapshotCsn);
                // Track read for SSI: we're reading this row to update it
                transaction.getDatabase().getConflictDetector().noteRead(txid, table.getName(), rowIndex);
            } else {
                table.checkWriteWriteConflict(rowIndex, txid, snapshotCsn);
            }
        }
        for (int rowIndex : rowsToUpdate) {
            Map<String, Object> oldValues = new HashMap<>(rows.get(rowIndex));
            RowVersionMeta oldMetaCopy = table.getRowVersionMeta(rowIndex) == null
                    ? null : table.getRowVersionMeta(rowIndex).copy();
            table.markUpdate(rowIndex, txid, oldValues);
            transaction.getUndoLog().addUndoRecord(
                    new UndoLog.UpdateUndo(table.getName(), rowIndex, oldValues, oldMetaCopy));
            transaction.noteModifiedRow(table.getName(), rowIndex);
            
            // Track write for SSI
            if (transaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
                transaction.getDatabase().getConflictDetector().noteWrite(txid, table.getName(), rowIndex);
            }
        }
    }

    private void applyBulkUpdate(List<Map<String, Object>> rows, List<Integer> rowsToUpdate,
                                 Map<String, Class<?>> columnTypes, Table table) {
        LOGGER.log(Level.INFO, "Bulk update mode: {0} rows >= threshold {1}",
                new Object[]{rowsToUpdate.size(), BULK_UPDATE_THRESHOLD});
        table.disableIndices();
        try {
            for (int rowIndex : rowsToUpdate) {
                Map<String, Object> row = rows.get(rowIndex);
                for (Map.Entry<String, Object> update : updates.entrySet()) {
                    String column = update.getKey();
                    Object newValue = update.getValue();
                    Class<?> columnType = columnTypes.get(column);
                    Object convertedValue = EVAL.convertConditionValue(newValue, column, columnType);
                    Object oldValue = row.get(column);
                    if (!Objects.equals(oldValue, convertedValue)) {
                        row.put(column, convertedValue);
                    }
                }
                table.updateRowInPlace(rowIndex, row);
            }
        } finally {
            table.enableAndRebuildIndices();
        }
    }

    private void applyPerRowUpdate(List<Map<String, Object>> rows, List<Integer> rowsToUpdate,
                                   Map<String, Class<?>> columnTypes, Table table) {
        for (int rowIndex : rowsToUpdate) {
            Map<String, Object> row = rows.get(rowIndex);
            for (Map.Entry<String, Object> update : updates.entrySet()) {
                String column = update.getKey();
                Object newValue = update.getValue();
                Class<?> columnType = columnTypes.get(column);
                Object convertedValue = EVAL.convertConditionValue(newValue, column, columnType);
                Object oldValue = row.get(column);

                if (!Objects.equals(oldValue, convertedValue)) {
                    Index index = table.getIndex(column);
                    if (index != null) {
                        if (oldValue != null) {
                            index.remove(oldValue, rowIndex);
                        }
                        if (convertedValue != null) {
                            table.evictDeadUniqueEntries(column, convertedValue);
                            index.insert(convertedValue, rowIndex);
                        }
                    }
                    row.put(column, convertedValue);
                }
            }
            table.updateRowInPlace(rowIndex, row);
        }
    }

    /**
     * Identifies rows matching the WHERE conditions using index lookups
     * when possible, falling back to a full table scan.
     */
    private void identifyRows(Table table, List<Map<String, Object>> rows,
                              Map<String, Class<?>> columnTypes,
                              List<Integer> rowsToUpdate) {
        if (isMvccReader()) {
            // Index keys track physical (possibly newer) values; a snapshot
            // reader must match against the versions it can actually see.
            if (conditions.isEmpty()) {
                fullTableScanAll(rows, table, rowsToUpdate);
            } else {
                fullTableScanWithCondition(rows, columnTypes, table, rowsToUpdate);
            }
            return;
        }
        if (conditions.size() == 1 && !conditions.get(0).isGrouped()
                && conditions.get(0).operator == QueryParser.Operator.EQUALS
                && !conditions.get(0).not) {
            identifyEqualsRows(table, columnTypes, rowsToUpdate);
        } else if (conditions.size() == 1 && !conditions.get(0).isGrouped()
                && conditions.get(0).isInOperator() && !conditions.get(0).not
                && conditions.get(0).subQuery == null) {
            identifyInRows(table, columnTypes, rowsToUpdate);
        } else if (conditions.size() == 1 && !conditions.get(0).isGrouped()
                && !conditions.get(0).not && !conditions.get(0).isInOperator()
                && conditions.get(0).rightColumn == null
                && conditions.get(0).subQuery == null) {
            identifyRangeRows(table, columnTypes, rowsToUpdate);
        }

        // Fallback: full table scan when no index was used
        if (rowsToUpdate.isEmpty() && !conditions.isEmpty()) {
            fullTableScanWithCondition(rows, columnTypes, table, rowsToUpdate);
        } else if (conditions.isEmpty()) {
            fullTableScanAll(rows, table, rowsToUpdate);
        }
    }

    /** True when an explicit (non-batch) MVCC transaction drives this statement. */
    private boolean isMvccReader() {
        MvccReadContext.Context context = MvccReadContext.get();
        Transaction transaction = context == null ? null : context.getTransaction();
        return transaction != null && transaction.isActive()
                && !context.isBatch() && transaction.getTxid() > 0;
    }

    private void identifyEqualsRows(Table table, Map<String, Class<?>> columnTypes,
                                    List<Integer> rowsToUpdate) {
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof HashIndex || index instanceof UniqueIndex) {
            Object conditionValue = EVAL.convertConditionValue(
                    condition.value, condition.column,
                    columnTypes.get(condition.column));
            rowsToUpdate.addAll(index.search(conditionValue));
            LOGGER.log(Level.INFO, "Using {0} index for UPDATE WHERE {1} = {2}",
                    new Object[]{index instanceof HashIndex ? "hash" : "unique",
                            condition.column, conditionValue});
        } else if (index instanceof BTreeIndex btree) {
            Object conditionValue = EVAL.convertConditionValue(
                    condition.value, condition.column,
                    columnTypes.get(condition.column));
            rowsToUpdate.addAll(btree.search(conditionValue));
            LOGGER.log(Level.INFO, "Using B-tree index for UPDATE WHERE {0} = {1}",
                    new Object[]{condition.column, conditionValue});
        }
    }

    private void identifyInRows(Table table, Map<String, Class<?>> columnTypes,
                                List<Integer> rowsToUpdate) {
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof HashIndex || index instanceof UniqueIndex || index instanceof BTreeIndex) {
            for (Object value : condition.inValues) {
                Object convertedValue = EVAL.convertConditionValue(
                        value, condition.column,
                        columnTypes.get(condition.column));
                rowsToUpdate.addAll(index.search(convertedValue));
            }
            List<Integer> deduped = rowsToUpdate.stream().distinct().sorted().collect(Collectors.toList());
            rowsToUpdate.clear();
            rowsToUpdate.addAll(deduped);
            LOGGER.log(Level.INFO, "Using {0} index for UPDATE WHERE {1} IN (...)",
                    new Object[]{index instanceof HashIndex ? "hash"
                            : index instanceof BTreeIndex ? "B-tree" : "unique",
                            condition.column});
        }
    }

    private void identifyRangeRows(Table table, Map<String, Class<?>> columnTypes,
                                   List<Integer> rowsToUpdate) {
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof BTreeIndex btree) {
            Object conditionValue = EVAL.convertConditionValue(
                    condition.value, condition.column,
                    columnTypes.get(condition.column));
            switch (condition.operator) {
                case GREATER_THAN_OR_EQUALS -> rowsToUpdate.addAll(btree.rangeSearchLow(conditionValue));
                case LESS_THAN_OR_EQUALS -> rowsToUpdate.addAll(btree.rangeSearchHigh(conditionValue));
                default -> { /* GREATER_THAN/LESS_THAN need exclusive bounds — fall through to full scan */ }
            }
            if (!rowsToUpdate.isEmpty()) {
                LOGGER.log(Level.INFO, "Using B-tree range index for UPDATE WHERE {0} {1} {2}",
                        new Object[]{condition.column, condition.operator, conditionValue});
            }
        }
    }

     private void fullTableScanWithCondition(List<Map<String, Object>> rows,
                                             Map<String, Class<?>> columnTypes,
                                             Table table, List<Integer> rowsToUpdate) {
         IntStream.range(0, rows.size())
                 .filter(i -> !table.isDeleted(i))
                 .forEach(i -> {
                     // Match against the reader-visible version: a snapshot
                     // reader must not target rows through another writer's
                     // newer values (shadow model, prompt4.md #4).
                     if (evaluateConditions(table.getVisibleRowForReader(i, rows.get(i)), conditions, columnTypes)) {
                         rowsToUpdate.add(i);
                     }
                 });
     }

     private void fullTableScanAll(List<Map<String, Object>> rows,
                                       Table table, List<Integer> rowsToUpdate) {
         IntStream.range(0, rows.size())
                 .filter(i -> !table.isDeleted(i))
                 .forEach(rowsToUpdate::add);
     }

    private static final ConditionEvaluator EVAL = new ConditionEvaluator();

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return EVAL.evaluateConditions(row, conditions, columnTypes);
    }
}
