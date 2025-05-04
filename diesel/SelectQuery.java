package diesel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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

        if (conditions.size() == 1 && !conditions.get(0).isGrouped() && conditions.get(0).operator == QueryParser.Operator.EQUALS && !conditions.get(0).not) {
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
            } else if (index instanceof UniqueIndex) {
                Object conditionValue = convertConditionValue(condition.value, condition.column, columnTypes.get(condition.column), columnTypes);
                candidateRowIndices = index.search(conditionValue);
                LOGGER.log(Level.INFO, "Using unique index for column {0} with value {1}", new Object[]{condition.column, conditionValue});
            }
        } else if (conditions.size() == 1 && !conditions.get(0).isGrouped() && conditions.get(0).isInOperator() && !conditions.get(0).not) {
            QueryParser.Condition condition = conditions.get(0);
            Index index = table.getIndex(condition.column);
            if (index instanceof HashIndex || index instanceof UniqueIndex || index instanceof BTreeIndex) {
                candidateRowIndices = new ArrayList<>();
                for (Object value : condition.inValues) {
                    Object convertedValue = convertConditionValue(value, condition.column, columnTypes.get(condition.column), columnTypes);
                    List<Integer> indices = index.search(convertedValue);
                    candidateRowIndices.addAll(indices);
                }
                candidateRowIndices = candidateRowIndices.stream().distinct().sorted().collect(Collectors.toList());
                LOGGER.log(Level.INFO, "Using {0} index for IN query on column {1} with values {2}",
                        new Object[]{index instanceof HashIndex ? "hash" : index instanceof BTreeIndex ? "B-tree" : "unique",
                                condition.column, condition.inValues});
            }
        } else if (conditions.size() == 2 && !conditions.get(0).isGrouped() && !conditions.get(1).isGrouped() &&
                conditions.get(0).column != null && conditions.get(1).column != null &&
                conditions.get(0).column.equals(conditions.get(1).column) &&
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
            LOGGER.log(Level.INFO, "Selected {0} rows from table {1}", new Object[]{result.size(), table.getName()});
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (String column : columns) {
            filtered.put(column, row.get(column));
        }
        return filtered;
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
            boolean result = condition.not ? !subResult : subResult;
            LOGGER.log(Level.FINE, "Evaluated grouped condition: {0}, result: {1}", new Object[]{condition, result});
            return result;
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
            boolean result = condition.not ? !inResult : inResult;
            LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue: {1}, values: {2}, result: {3}",
                    new Object[]{condition, rowValue, condition.inValues, result});
            return result;
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);
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

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue: {1}, conditionValue: {2}, result: {3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return result;
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