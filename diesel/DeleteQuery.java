package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;

class DeleteQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(DeleteQuery.class.getName());
    private final List<QueryParser.Condition> conditions;

    public DeleteQuery(List<QueryParser.Condition> conditions) {
        this.conditions = conditions != null ? conditions : new ArrayList<>();
        LOGGER.log(Level.FINE, "Created DeleteQuery with conditions: {0}", this.conditions);
    }

    @Override
    public Void execute(Table table) {
        LOGGER.log(Level.FINE, "Executing DeleteQuery for table: {0}", table.getName());
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        List<Integer> rowsToDelete = new ArrayList<>();

        try {
            // Identify rows to delete
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                if (conditions.isEmpty() || evaluateConditions(row, conditions, columnTypes)) {
                    ReentrantReadWriteLock lock = table.getRowLock(i);
                    lock.writeLock().lock();
                    acquiredLocks.add(lock);
                    rowsToDelete.add(i);
                }
            }

            // Delete rows in reverse order to avoid index shifting
            Collections.sort(rowsToDelete, Collections.reverseOrder());
            for (int rowIndex : rowsToDelete) {
                Map<String, Object> row = rows.get(rowIndex);
                // Update indexes
                for (Map.Entry<String, Index> entry : table.getIndexes().entrySet()) {
                    String column = entry.getKey();
                    Index index = entry.getValue();
                    Object key = row.get(column);
                    if (key != null) {
                        index.remove(key, rowIndex);
                    }
                }
                rows.remove(rowIndex);
                LOGGER.log(Level.INFO, "Deleted row at index {0}", rowIndex);
            }

            // Update indexes for remaining rows
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                for (Map.Entry<String, Index> entry : table.getIndexes().entrySet()) {
                    String column = entry.getKey();
                    Index index = entry.getValue();
                    Object key = row.get(column);
                    if (key != null) {
                        index.remove(key, i);
                        index.insert(key, i);
                    }
                }
            }

            LOGGER.log(Level.INFO, "Deleted {0} rows from table {1}", new Object[]{rowsToDelete.size(), table.getName()});
            return null;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
            }
        }
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        if (conditions.isEmpty()) {
            LOGGER.log(Level.FINE, "No conditions provided; all rows match");
            return true; // Delete all rows if no conditions
        }

        boolean result = evaluateCondition(row, conditions.get(0), columnTypes);
        LOGGER.log(Level.FINE, "Evaluated first condition: {0}, result: {1}", new Object[]{conditions.get(0), result});

        for (int i = 1; i < conditions.size(); i++) {
            boolean currentResult = evaluateCondition(row, conditions.get(i), columnTypes);
            String conjunction = conditions.get(i - 1).conjunction;
            LOGGER.log(Level.FINE, "Evaluated condition: {0}, result: {1}, conjunction: {2}",
                    new Object[]{conditions.get(i), currentResult, conjunction});
            if ("AND".equalsIgnoreCase(conjunction)) {
                result = result && currentResult;
            } else if ("OR".equalsIgnoreCase(conjunction)) {
                result = result || currentResult;
            } else {
                LOGGER.log(Level.WARNING, "Invalid conjunction: {0}, treating as AND", conjunction);
                result = result && currentResult;
            }
        }

        LOGGER.log(Level.FINE, "Final condition evaluation result: {0}", result);
        return result;
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        if (condition.isGrouped()) {
            boolean subResult = evaluateConditions(row, condition.subConditions, columnTypes);
            boolean result = condition.not ? !subResult : subResult;
            LOGGER.log(Level.FINE, "Evaluated grouped condition: {0}, result: {1}", new Object[]{condition, result});
            return result;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);
        LOGGER.log(Level.FINE, "Condition values: rowValue={0}, conditionValue={1}, column={2}, operator={3}",
                new Object[]{rowValue, conditionValue, condition.column, condition.operator});

        boolean result;
        if (condition.operator == QueryParser.Operator.EQUALS || condition.operator == QueryParser.Operator.NOT_EQUALS) {
            boolean isEqual;
            if (rowValue instanceof Float && conditionValue instanceof Float) {
                isEqual = Math.abs(((Float) rowValue) - ((Float) conditionValue)) < 1e-7;
            } else if (rowValue instanceof Double && conditionValue instanceof Double) {
                isEqual = Math.abs(((Double) rowValue) - ((Double) conditionValue)) < 1e-7;
            } else if (rowValue instanceof BigDecimal && conditionValue instanceof BigDecimal) {
                isEqual = ((BigDecimal) rowValue).compareTo((BigDecimal) conditionValue) == 0;
            } else {
                isEqual = String.valueOf(rowValue).equals(String.valueOf(conditionValue));
            }
            result = condition.operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
        } else {
            if (!(rowValue instanceof Comparable) || !(conditionValue instanceof Comparable)) {
                throw new IllegalArgumentException("Comparison operators < or > only supported for numeric types or dates");
            }
            @SuppressWarnings("unchecked")
            Comparable<Object> rowComparable = (Comparable<Object>) rowValue;
            @SuppressWarnings("unchecked")
            Comparable<Object> conditionComparable = (Comparable<Object>) conditionValue;
            int comparison = rowComparable.compareTo(conditionValue);
            result = condition.operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0;
        }

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue: {1}, conditionValue: {2}, result: {3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return result;
    }

    private Object convertConditionValue(Object conditionValue, String column, Class<?> rowValueType, Map<String, Class<?>> columnTypes) {
        Class<?> expectedType = columnTypes.get(column);
        if (expectedType == null) {
            throw new IllegalArgumentException("Unknown column: " + column);
        }

        if (conditionValue == null || rowValueType == conditionValue.getClass()) {
            return conditionValue;
        }

        try {
            if (expectedType == BigDecimal.class && !(conditionValue instanceof BigDecimal)) {
                return new BigDecimal(conditionValue.toString());
            } else if (expectedType == Float.class && !(conditionValue instanceof Float)) {
                return Float.parseFloat(conditionValue.toString());
            } else if (expectedType == Double.class && !(conditionValue instanceof Double)) {
                return Double.parseDouble(conditionValue.toString());
            } else if (expectedType == Integer.class && !(conditionValue instanceof Integer)) {
                return Integer.parseInt(conditionValue.toString());
            } else if (expectedType == Long.class && !(conditionValue instanceof Long)) {
                return Long.parseLong(conditionValue.toString());
            } else if (expectedType == Short.class && !(conditionValue instanceof Short)) {
                return Short.parseShort(conditionValue.toString());
            } else if (expectedType == Byte.class && !(conditionValue instanceof Byte)) {
                return Byte.parseByte(conditionValue.toString());
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot convert condition value '" + conditionValue + "' to type " + expectedType.getSimpleName());
        }

        return conditionValue;
    }
}