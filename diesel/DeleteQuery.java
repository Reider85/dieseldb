package diesel;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
        validateConditions();
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<Integer> rowsToDelete = prepareDelete(table, rows, columnTypes);
        executeDelete(table, rowsToDelete);
        updateIndexes(table, rowsToDelete.size());
        LOGGER.log(Level.INFO, "Deleted {0} rows from table {1}", new Object[]{rowsToDelete.size(), table.getName()});
        lastAffectedRows = rowsToDelete.size();
        return null;
    }

    /**
     * Validates WHERE conditions before execution.
     */
    private void validateConditions() {
        // Basic validation - ensure conditions are not null
        if (conditions == null) {
            throw new IllegalArgumentException("Delete conditions cannot be null");
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

        // Phase 1: Identify rows to delete (index-accelerated or full scan)
        if (conditions.size() == 1 && !conditions.get(0).isGrouped() && conditions.get(0).operator == QueryParser.Operator.EQUALS && !conditions.get(0).not) {
            QueryParser.Condition condition = conditions.get(0);
            Index index = table.getIndex(condition.column);
            if (index instanceof HashIndex || index instanceof UniqueIndex) {
                Object conditionValue = EVAL.convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column), columnTypes);
                rowsToDelete = index.search(conditionValue);
                LOGGER.log(Level.INFO, "Using {0} index for column {1} with value {2}",
                        new Object[]{index instanceof HashIndex ? "hash" : "unique", condition.column, conditionValue});
            } else if (index instanceof BTreeIndex btree) {
                Object conditionValue = EVAL.convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column), columnTypes);
                rowsToDelete = btree.search(conditionValue);
                LOGGER.log(Level.INFO, "Using B-tree index for column {0} with value {1}", new Object[]{condition.column, conditionValue});
            }
        } else if (conditions.size() == 1 && !conditions.get(0).isGrouped() && conditions.get(0).isInOperator() && !conditions.get(0).not) {
            QueryParser.Condition condition = conditions.get(0);
            Index index = table.getIndex(condition.column);
            if (index instanceof HashIndex || index instanceof UniqueIndex || index instanceof BTreeIndex) {
                for (Object value : condition.inValues) {
                    Object convertedValue = EVAL.convertConditionValue(value, condition.column, columnTypes.get(condition.column), columnTypes);
                    List<Integer> indices = index.search(convertedValue);
                    rowsToDelete.addAll(indices);
                }
                rowsToDelete = rowsToDelete.stream().distinct().sorted().collect(Collectors.toList());
                LOGGER.log(Level.INFO, "Using {0} index for IN query on column {1} with values {2}",
                        new Object[]{index instanceof HashIndex ? "hash" : index instanceof BTreeIndex ? "B-tree" : "unique",
                                condition.column, condition.inValues});
            }
        }

        if (rowsToDelete.isEmpty() && !conditions.isEmpty()) {
            for (int i = 0; i < rows.size(); i++) {
                if (table.isDeleted(i)) continue;
                Map<String, Object> row = rows.get(i);
                if (evaluateConditions(row, conditions, columnTypes)) {
                    rowsToDelete.add(i);
                }
            }
        } else if (conditions.isEmpty()) {
            for (int i = 0; i < rows.size(); i++) {
                if (table.isDeleted(i)) continue;
                rowsToDelete.add(i);
            }
        }

        return rowsToDelete;
    }

    /**
     * Executes the deletion: acquires locks, tombstones rows, removes index entries.
     *
     * @param table the table to delete from
     * @param rowsToDelete list of row indices to delete
     */
    private void executeDelete(Table table, List<Integer> rowsToDelete) {
        List<Map<String, Object>> rows = table.getRows();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();

        try {
            // Phase 2: Acquire write locks
            for (int rowIndex : rowsToDelete) {
                if (rowIndex >= 0 && rowIndex < rows.size()) {
                    ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                    lock.writeLock().lock();
                    acquiredLocks.add(lock);
                }
            }

            // Phase 3: Tombstone + remove index entries (no physical removal, no re-index)
            for (int rowIndex : rowsToDelete) {
                if (rowIndex >= 0 && rowIndex < rows.size() && !table.isDeleted(rowIndex)) {
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
            }
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Updates indexes after deletion, performing auto-compaction if needed.
     *
     * @param table the table to update
     * @param deletedCount number of rows deleted
     */
    private void updateIndexes(Table table, int deletedCount) {
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