package diesel;

import java.math.BigDecimal;
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
                    Object convertedValue = convertConditionValue(newValue, column, columnType, columnTypes);
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

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return ThreeValuedLogic.isTrue(evaluateConditions3vl(row, conditions, columnTypes));
    }

    /**
     * Вычисляет список условий по правилам трёхзначной логики SQL
     * (см. {@link ThreeValuedLogic}). Правый операнд не вычисляется, если левый
     * уже определяет результат: {@code TRUE OR X = TRUE}, {@code FALSE AND X = FALSE}.
     */
    private Boolean evaluateConditions3vl(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        if (conditions.isEmpty()) {
            return Boolean.TRUE;
        }
        Boolean result = evaluateCondition3vl(row, conditions.get(0), columnTypes);
        for (int i = 1; i < conditions.size(); i++) {
            QueryParser.Condition condition = conditions.get(i);
            String conjunction = condition.conjunction;
            if (conjunction != null && conjunction.equalsIgnoreCase("OR")) {
                if (ThreeValuedLogic.orIsDetermined(result)) {
                    continue;
                }
                result = ThreeValuedLogic.or(result, evaluateCondition3vl(row, condition, columnTypes));
            } else if (conjunction == null || conjunction.equalsIgnoreCase("AND")) {
                if (ThreeValuedLogic.andIsDetermined(result)) {
                    continue;
                }
                result = ThreeValuedLogic.and(result, evaluateCondition3vl(row, condition, columnTypes));
            }
        }
        return result;
    }

    private Boolean evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        if (condition.isGrouped()) {
            Boolean subResult = evaluateConditions3vl(row, condition.subConditions, columnTypes);
            return condition.not ? ThreeValuedLogic.not(subResult) : subResult;
        }

        if (condition.isNullOperator()) {
            Object value = row.get(condition.column);
            boolean isNull = value == null;
            boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
            LOGGER.log(Level.FINE, "Evaluated IS NULL condition: {0}, value: {1}, result: {2}",
                    new Object[]{condition, value, result});
            return condition.not ? Boolean.valueOf(!result) : Boolean.valueOf(result);
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.FINE, "Row value for column {0} is null, condition is UNKNOWN", condition.column);
            return null;
        }

        if (condition.isInOperator()) {
            boolean inResult = false;
            for (Object value : condition.inValues) {
                Object convertedValue = convertConditionValue(value, condition.column, rowValue.getClass(), columnTypes);
                boolean isEqual;
                if (rowValue instanceof Float && convertedValue instanceof Float) {
                    isEqual = Math.abs(((Float) rowValue) - ((Float) convertedValue)) < 1e-7;
                } else if (rowValue instanceof Double && convertedValue instanceof Double) {
                    isEqual = Math.abs(((Double) rowValue) - ((Double) convertedValue)) < 1e-7;
                } else if (rowValue instanceof BigDecimal && convertedValue instanceof BigDecimal) {
                    isEqual = ((BigDecimal) rowValue).compareTo((BigDecimal) convertedValue) == 0;
                } else {
                    isEqual = String.valueOf(rowValue).equals(String.valueOf(convertedValue));
                }
                if (isEqual) {
                    inResult = true;
                    break;
                }
            }
            boolean result = condition.not ? !inResult : inResult;
            LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue: {1}, values: {2}, result: {3}",
                    new Object[]{condition, rowValue, condition.inValues, result});
            return Boolean.valueOf(result);
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);
        if (conditionValue == null) {
            LOGGER.log(Level.FINE, "Condition value for column {0} is null, condition is UNKNOWN", condition.column);
            return null;
        }
        LOGGER.log(Level.FINE, "Condition values: rowValue={0}, conditionValue={1}, column={2}, operator={3}",
                new Object[]{rowValue, conditionValue, condition.column, condition.operator});

        boolean result;
        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            if (!(rowValue instanceof String) || !(conditionValue instanceof String)) {
                throw new IllegalArgumentException("LIKE and NOT LIKE operators are only supported for String types");
            }
            String rowStr = (String) rowValue;
            try {
                String regex = QueryParser.convertLikePatternToRegex((String) conditionValue);
                boolean matches = rowStr.matches(regex);
                result = condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid LIKE pattern: " + conditionValue, e);
            }
        } else if (condition.operator == QueryParser.Operator.EQUALS || condition.operator == QueryParser.Operator.NOT_EQUALS) {
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
                throw new IllegalArgumentException("Comparison operators <, >, <=, >= only supported for comparable types");
            }
            int comparison = compareValues(rowValue, conditionValue);
            result = switch (condition.operator) {
                case LESS_THAN -> comparison < 0;
                case LESS_THAN_OR_EQUALS -> comparison <= 0;
                case GREATER_THAN -> comparison > 0;
                case GREATER_THAN_OR_EQUALS -> comparison >= 0;
                default -> throw new IllegalStateException("Unsupported operator: " + condition.operator);
            };
        }

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue: {1}, conditionValue: {2}, result: {3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return Boolean.valueOf(result);
    }

    private Object convertConditionValue(Object value, String column, Class<?> targetType, Map<String, Class<?>> columnTypes) {
        if (value == null) {
            return null;
        }
        // Prompt 22 (java:S2259): columnTypes.get(column) may be null when the
        // table schema lacks the column; pass the value through rather than
        // NPE on targetType.isAssignableFrom below.
        if (targetType == null) {
            return value;
        }

        Class<?> valueType = value.getClass();
        if (targetType.isAssignableFrom(valueType)) {
            return value;
        }

        String stringValue = String.valueOf(value);
        try {
            if (targetType == String.class) {
                return stringValue;
            } else if (targetType == Integer.class) {
                return Integer.parseInt(stringValue);
            } else if (targetType == Long.class) {
                return Long.parseLong(stringValue);
            } else if (targetType == Short.class) {
                return Short.parseShort(stringValue);
            } else if (targetType == Byte.class) {
                return Byte.parseByte(stringValue);
            } else if (targetType == Float.class) {
                return Float.parseFloat(stringValue);
            } else if (targetType == Double.class) {
                return Double.parseDouble(stringValue);
            } else if (targetType == BigDecimal.class) {
                return new BigDecimal(stringValue);
            } else if (targetType == Boolean.class) {
                return Boolean.parseBoolean(stringValue);
            } else if (targetType == UUID.class) {
                return UUID.fromString(stringValue);
            } else if (targetType == Character.class) {
                if (stringValue.length() == 1) {
                    return stringValue.charAt(0);
                } else {
                    throw new IllegalArgumentException("Invalid character value for column " + column);
                }
            } else {
                throw new IllegalArgumentException("Unsupported type conversion for column " + column + ": " + targetType.getSimpleName());
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert value '" + stringValue + "' to type " + targetType.getSimpleName() + " for column " + column, e);
        }
    }

    private int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }
        if (left instanceof Number && right instanceof Number) {
            if (left instanceof BigDecimal || right instanceof BigDecimal) {
                BigDecimal leftBD = left instanceof BigDecimal ? (BigDecimal) left : new BigDecimal(left.toString());
                BigDecimal rightBD = right instanceof BigDecimal ? (BigDecimal) right : new BigDecimal(right.toString());
                return leftBD.compareTo(rightBD);
            }
            if (left instanceof Float && right instanceof Float) {
                return Float.compare((Float) left, (Float) right);
            }
            if (left instanceof Double && right instanceof Double) {
                return Double.compare((Double) left, (Double) right);
            }
            return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
        }
        @SuppressWarnings("unchecked")
        Comparable<Object> leftComparable = (Comparable<Object>) left;
        return leftComparable.compareTo(right);
    }
}