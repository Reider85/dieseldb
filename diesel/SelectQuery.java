package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final List<QueryParser.Condition> conditions;

    public SelectQuery(List<String> columns, List<QueryParser.Condition> conditions) {
        this.columns = columns;
        this.conditions = conditions;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        List<Integer> candidateRowIndices = null;

        // Check if we can use an index for a single equality condition
        if (conditions.size() == 1 && conditions.get(0).operator == QueryParser.Operator.EQUALS && !conditions.get(0).not) {
            QueryParser.Condition condition = conditions.get(0);
            Index index = table.getIndex(condition.column);
            if (index instanceof HashIndex) {
                Object conditionValue = convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column), columnTypes);
                candidateRowIndices = index.search(conditionValue);
                LOGGER.log(Level.INFO, "Using hash index for column {0} with value {1}", new Object[]{condition.column, conditionValue});
            } else if (index instanceof BTreeIndex) {
                Object conditionValue = convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column), columnTypes);
                candidateRowIndices = ((BTreeIndex) index).search(conditionValue);
                LOGGER.log(Level.INFO, "Using B-tree index for column {0} with value {1}", new Object[]{condition.column, conditionValue});
            }
        }
        // Check for range queries (LESS_THAN or GREATER_THAN) - only for BTreeIndex
        else if (conditions.size() == 2 && conditions.get(0).column.equals(conditions.get(1).column) &&
                ((conditions.get(0).operator == QueryParser.Operator.GREATER_THAN && conditions.get(1).operator == QueryParser.Operator.LESS_THAN) ||
                        (conditions.get(0).operator == QueryParser.Operator.LESS_THAN && conditions.get(1).operator == QueryParser.Operator.GREATER_THAN)) &&
                conditions.get(0).conjunction.equalsIgnoreCase("AND") && !conditions.get(0).not && !conditions.get(1).not) {
            QueryParser.Condition cond1 = conditions.get(0);
            QueryParser.Condition cond2 = conditions.get(1);
            Index index = table.getIndex(cond1.column);
            if (index instanceof BTreeIndex) {
                Object low = convertConditionValue(cond1.operator == QueryParser.Operator.GREATER_THAN ? cond1.value : cond2.value,
                        cond1.column, columnTypes.get(cond1.column), columnTypes);
                Object high = convertConditionValue(cond1.operator == QueryParser.Operator.LESS_THAN ? cond1.value : cond2.value,
                        cond1.column, columnTypes.get(cond1.column), columnTypes);
                candidateRowIndices = ((BTreeIndex) index).rangeSearch(low, high);
                LOGGER.log(Level.INFO, "Using B-tree index for range query on column {0} between {1} and {2}",
                        new Object[]{cond1.column, low, high});
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();
        try {
            if (candidateRowIndices != null) {
                // Use index results
                for (int rowIndex : candidateRowIndices) {
                    if (rowIndex >= 0 && rowIndex < rows.size()) {
                        Map<String, Object> row = rows.get(rowIndex);
                        ReentrantReadWriteLock lock = table.getRowLock(rowIndex);
                        lock.readLock().lock();
                        acquiredLocks.add(lock);
                        result.add(filterColumns(row, columns));
                    }
                }
            } else {
                // Full table scan
                for (int i = 0; i < rows.size(); i++) {
                    Map<String, Object> row = rows.get(i);
                    if (conditions.isEmpty() || evaluateConditions(row, conditions, columnTypes)) {
                        ReentrantReadWriteLock lock = table.getRowLock(i);
                        lock.readLock().lock();
                        acquiredLocks.add(lock);
                        result.add(filterColumns(row, columns));
                    }
                }
            }
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        boolean result = evaluateCondition(row, conditions.get(0), columnTypes);
        for (int i = 1; i < conditions.size(); i++) {
            boolean currentResult = evaluateCondition(row, conditions.get(i), columnTypes);
            String conjunction = conditions.get(i - 1).conjunction;
            if ("AND".equalsIgnoreCase(conjunction)) {
                result = result && currentResult;
            } else if ("OR".equalsIgnoreCase(conjunction)) {
                result = result || currentResult;
            }
        }
        return result;
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        if (condition.isGrouped()) {
            boolean subResult = evaluateConditions(row, condition.subConditions, columnTypes);
            return condition.not ? !subResult : subResult;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);

        LOGGER.log(Level.INFO, "Comparing rowValue={0} (type={1}), conditionValue={2} (type={3}), operator={4}, not={5}",
                new Object[]{rowValue, rowValue.getClass().getSimpleName(),
                        conditionValue, conditionValue.getClass().getSimpleName(), condition.operator, condition.not});

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
                LOGGER.log(Level.WARNING, "Comparison operators < or > not supported for types: rowValue={0}, conditionValue={1}",
                        new Object[]{rowValue.getClass().getSimpleName(), conditionValue.getClass().getSimpleName()});
                throw new IllegalArgumentException("Comparison operators < or > only supported for numeric types or dates");
            }

            @SuppressWarnings("unchecked")
            Comparable<Object> rowComparable = (Comparable<Object>) rowValue;
            @SuppressWarnings("unchecked")
            Comparable<Object> conditionComparable = (Comparable<Object>) conditionValue;

            int comparison = rowComparable.compareTo(conditionValue);
            result = condition.operator == QueryParser.Operator.LESS_THAN ? comparison < 0 : comparison > 0;
        }

        return condition.not ? !result : result;
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

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> result = new HashMap<>();
        for (String col : columns) {
            if (row.containsKey(col)) {
                result.put(col, row.get(col));
            }
        }
        return result;
    }
}