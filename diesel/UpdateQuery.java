package diesel;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
     * Finds the matching rows, locks them, converts the new values to the
     * column types and applies the updates, keeping the indexes in sync.
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
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                if (conditions.isEmpty() || evaluateConditions(row, conditions, columnTypes)) {
                    rowsToUpdate.add(i);
                }
            }

            for (int rowIndex : rowsToUpdate) {
                ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                lock.writeLock().lock();
                acquiredLocks.add(lock);
            }

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
            }

            LOGGER.log(Level.INFO, "Updated {0} rows in table {1}", new Object[]{rowsToUpdate.size(), table.getName()});
            lastAffectedRows = rowsToUpdate.size();
            return null;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
            }
        }
    }

    private static final ConditionEvaluator EVAL = new ConditionEvaluator();

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return EVAL.evaluateConditions(row, conditions, columnTypes);
    }
}