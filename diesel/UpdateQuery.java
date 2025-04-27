package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;

class UpdateQuery implements Query<Void> {
    private static final Logger LOGGER = Logger.getLogger(UpdateQuery.class.getName());
    private final Map<String, Object> updates;
    private final List<QueryParser.Condition> conditions;

    public UpdateQuery(Map<String, Object> updates, List<QueryParser.Condition> conditions) {
        this.updates = updates;
        this.conditions = conditions;
    }

    @Override
    public Void execute(Table table) {
        List<Map<String, Object>> rows = table.getRows();
        Map<String, Class<?>> columnTypes = table.getColumnTypes();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();

        try {
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                if (conditions.isEmpty() || evaluateConditions(row, conditions, columnTypes)) {
                    ReentrantReadWriteLock lock = table.getRowLock(i);
                    lock.writeLock().lock();
                    acquiredLocks.add(lock);

                    // Check unique indexes for new values
                    for (Map.Entry<String, Object> update : updates.entrySet()) {
                        String column = update.getKey();
                        Index uniqueIndex = table.getUniqueIndexes().get(column);
                        if (uniqueIndex != null) {
                            Object newValue = update.getValue();
                            if (newValue != null) {
                                List<Integer> existingRows = uniqueIndex.search(newValue);
                                if (!existingRows.isEmpty() && !existingRows.contains(i)) {
                                    LOGGER.log(Level.WARNING, "Unique constraint violation on column {0}: value {1} already exists at row {2}",
                                            new Object[]{column, newValue, existingRows.get(0)});
                                    throw new IllegalArgumentException(
                                            "Unique constraint violation: value '" + newValue + "' already exists in column " + column);
                                }
                            }
                        }
                    }

                    // Store old values for index updates
                    Map<String, Object> oldValues = new HashMap<>();
                    for (String column : updates.keySet()) {
                        oldValues.put(column, row.get(column));
                    }

                    // Apply updates
                    for (Map.Entry<String, Object> update : updates.entrySet()) {
                        String column = update.getKey();
                        Object value = update.getValue();
                        Class<?> expectedType = columnTypes.get(column);
                        if (expectedType == null) {
                            throw new IllegalArgumentException("Unknown column: " + column);
                        }
                        if (expectedType == Integer.class && !(value instanceof Integer)) {
                            try {
                                value = Integer.parseInt(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected INTEGER", value, column));
                            }
                        } else if (expectedType == Long.class && !(value instanceof Long)) {
                            try {
                                value = Long.parseLong(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected LONG", value, column));
                            }
                        } else if (expectedType == Short.class && !(value instanceof Short)) {
                            try {
                                value = Short.parseShort(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected SHORT", value, column));
                            }
                        } else if (expectedType == Byte.class && !(value instanceof Byte)) {
                            try {
                                value = Byte.parseByte(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected BYTE", value, column));
                            }
                        } else if (expectedType == BigDecimal.class && !(value instanceof BigDecimal)) {
                            try {
                                value = new BigDecimal(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected BIGDECIMAL", value, column));
                            }
                        } else if (expectedType == Float.class && !(value instanceof Float)) {
                            try {
                                value = Float.parseFloat(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected FLOAT", value, column));
                            }
                        } else if (expectedType == Double.class && !(value instanceof Double)) {
                            try {
                                value = Double.parseDouble(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected DOUBLE", value, column));
                            }
                        } else if (expectedType == Character.class && !(value instanceof Character)) {
                            try {
                                if (value.toString().length() == 1) {
                                    value = value.toString().charAt(0);
                                } else {
                                    throw new IllegalArgumentException("Expected single character");
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected CHAR", value, column));
                            }
                        } else if (expectedType == UUID.class && !(value instanceof UUID)) {
                            try {
                                value = UUID.fromString(value.toString());
                            } catch (IllegalArgumentException e) {
                                throw new IllegalArgumentException(
                                        String.format("Invalid value '%s' for column %s: expected UUID", value, column));
                            }
                        } else if (expectedType == String.class && !(value instanceof String)) {
                            value = value.toString();
                        } else if (expectedType == Boolean.class && !(value instanceof Boolean)) {
                            throw new IllegalArgumentException(
                                    String.format("Invalid value '%s' for column %s: expected BOOLEAN", value, column));
                        }
                        row.put(column, value);
                    }

                    // Update indexes
                    for (Map.Entry<String, Object> update : updates.entrySet()) {
                        String column = update.getKey();
                        Index index = table.getIndex(column);
                        if (index != null) {
                            Object oldValue = oldValues.get(column);
                            Object newValue = update.getValue();
                            if (oldValue != null) {
                                index.remove(oldValue, i);
                            }
                            if (newValue != null) {
                                index.insert(newValue, i);
                            }
                            LOGGER.log(Level.INFO, "Updated {0} index for column {1} at row {2}: {3} -> {4}",
                                    new Object[]{index instanceof UniqueHashIndex ? "unique hash" : index instanceof HashIndex ? "hash" : "B-tree",
                                            column, i, oldValue, newValue});
                        }
                    }
                }
            }
            return null;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
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
}