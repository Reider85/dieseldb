package diesel;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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

            // Phase 2: Acquire write locks
            for (int rowIndex : rowsToUpdate) {
                ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                lock.writeLock().lock();
                acquiredLocks.add(lock);
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
                    Object convertedValue = EVAL.convertConditionValue(newValue, column, columnType, columnTypes);
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
                Object convertedValue = EVAL.convertConditionValue(newValue, column, columnType, columnTypes);
                Object oldValue = row.get(column);

                if (!Objects.equals(oldValue, convertedValue)) {
                    Index index = table.getIndex(column);
                    if (index != null) {
                        if (oldValue != null) {
                            index.remove(oldValue, rowIndex);
                        }
                        if (convertedValue != null) {
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
        if (conditions.size() == 1 && !conditions.get(0).isGrouped()
                && conditions.get(0).operator == QueryParser.Operator.EQUALS
                && !conditions.get(0).not) {
            identifyEqualsRows(table, columnTypes, rowsToUpdate);
        } else if (conditions.size() == 1 && !conditions.get(0).isGrouped()
                && conditions.get(0).isInOperator() && !conditions.get(0).not) {
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

    private void identifyEqualsRows(Table table, Map<String, Class<?>> columnTypes,
                                    List<Integer> rowsToUpdate) {
        QueryParser.Condition condition = conditions.get(0);
        Index index = table.getIndex(condition.column);
        if (index instanceof HashIndex || index instanceof UniqueIndex) {
            Object conditionValue = EVAL.convertConditionValue(
                    condition.value, condition.column,
                    columnTypes.get(condition.column), columnTypes);
            rowsToUpdate.addAll(index.search(conditionValue));
            LOGGER.log(Level.INFO, "Using {0} index for UPDATE WHERE {1} = {2}",
                    new Object[]{index instanceof HashIndex ? "hash" : "unique",
                            condition.column, conditionValue});
        } else if (index instanceof BTreeIndex btree) {
            Object conditionValue = EVAL.convertConditionValue(
                    condition.value, condition.column,
                    columnTypes.get(condition.column), columnTypes);
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
                        columnTypes.get(condition.column), columnTypes);
                rowsToUpdate.addAll(index.search(convertedValue));
            }
            rowsToUpdate = rowsToUpdate.stream().distinct().sorted()
                    .collect(Collectors.toList());
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
                    columnTypes.get(condition.column), columnTypes);
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
                     if (evaluateConditions(rows.get(i), conditions, columnTypes)) {
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
