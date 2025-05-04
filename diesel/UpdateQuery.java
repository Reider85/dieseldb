package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

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
            return null;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.writeLock().unlock();
            }
        }
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        boolean result = true;
        String lastConjunction = null;

        for (QueryParser.Condition condition : conditions) {
            boolean conditionResult = evaluateCondition(row, condition, columnTypes);

            if (lastConjunction == null) {
                result = conditionResult;
            } else if (lastConjunction.equalsIgnoreCase("AND")) {
                result = result && conditionResult;
            } else if (lastConjunction.equalsIgnoreCase("OR")) {
                result = result || conditionResult;
            }

            lastConjunction = condition.conjunction;
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
            return condition.not ? !inResult : inResult;
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);

        boolean result;
        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            if (!(rowValue instanceof String) || !(conditionValue instanceof String)) {
                throw new IllegalArgumentException("LIKE and NOT LIKE operators are only supported for String types");
            }
            String rowStr = (String) rowValue;
            String pattern = ((String) conditionValue).replace("%", ".*").replace("_", ".");
            boolean matches = rowStr.matches(pattern);
            result = condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
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

    private Object convertConditionValue(Object value, String column, Class<?> targetType, Map<String, Class<?>> columnTypes) {
        if (value == null) {
            return null;
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
}