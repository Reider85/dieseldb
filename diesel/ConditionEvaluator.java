package diesel;

import static diesel.ThreeValuedLogic.TRUE;
import static diesel.ThreeValuedLogic.FALSE;
import static diesel.ThreeValuedLogic.UNKNOWN;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shared condition evaluation logic for DELETE, UPDATE, and SELECT queries.
 * Eliminates code duplication by providing a single implementation of
 * SQL three-valued logic condition evaluation.
 */
class ConditionEvaluator {
    private static final Logger LOGGER = Logger.getLogger(ConditionEvaluator.class.getName());

    /**
     * Evaluates a list of conditions using SQL three-valued logic with short-circuit evaluation.
     */
    ThreeValuedLogic evaluateConditions3vl(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        if (conditions.isEmpty()) {
            return TRUE;
        }
        ThreeValuedLogic result = evaluateCondition3vl(row, conditions.get(0), columnTypes);
        for (int i = 1; i < conditions.size(); i++) {
            QueryParser.Condition condition = conditions.get(i);
            String conjunction = condition.conjunction;
            if (Objects.equals(conjunction, SqlKeywords.OR)) {
                if (result.orIsDetermined()) {
                    continue;
                }
                result = result.or(evaluateCondition3vl(row, condition, columnTypes));
            } else if (conjunction == null || conjunction.equalsIgnoreCase(SqlKeywords.AND)) {
                if (result.andIsDetermined()) {
                    continue;
                }
                result = result.and(evaluateCondition3vl(row, condition, columnTypes));
            }
        }
        return result;
    }

    /**
     * Convenience wrapper that returns a boolean result.
     */
    boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return evaluateConditions3vl(row, conditions, columnTypes).isTrue();
    }

    /**
     * Evaluates a single condition against a row using SQL three-valued logic.
     */
    ThreeValuedLogic evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        if (condition.isGrouped()) {
            ThreeValuedLogic subResult = evaluateConditions3vl(row, condition.subConditions, columnTypes);
            return condition.not ? subResult.not() : subResult;
        }

        if (condition.isNullOperator()) {
            Object value = row.get(condition.column);
            boolean isNull = value == null;
            boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
            LOGGER.log(Level.FINE, "Evaluated IS NULL condition: {0}, value: {1}, result: {2}",
                    new Object[]{condition, value, result});
            return (condition.not ? !result : result) ? TRUE : FALSE;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.FINE, "Row value for column {0} is null, condition is UNKNOWN", condition.column);
            return UNKNOWN;
        }

        if (condition.isInOperator()) {
            boolean inResult = false;
            for (Object value : condition.inValues) {
                Object convertedValue = convertConditionValue(value, condition.column, rowValue.getClass(), columnTypes);
                if (valuesEqual(rowValue, convertedValue)) {
                    inResult = true;
                    break;
                }
            }
            boolean result = condition.not ? !inResult : inResult;
            LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue: {1}, values: {2}, result: {3}",
                    new Object[]{condition, rowValue, condition.inValues, result});
            return result ? TRUE : FALSE;
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass(), columnTypes);
        if (conditionValue == null) {
            LOGGER.log(Level.FINE, "Condition value for column {0} is null, condition is UNKNOWN", condition.column);
            return UNKNOWN;
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
            boolean isEqual = valuesEqual(rowValue, conditionValue);
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
                default -> throw new IllegalStateException(ErrorMessages.UNSUPPORTED_OPERATOR_PREFIX + condition.operator);
            };
        }

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue: {1}, conditionValue: {2}, result: {3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return result ? TRUE : FALSE;
    }

    /**
     * Checks equality with epsilon comparison for floating-point types.
     */
    boolean valuesEqual(Object rowValue, Object conditionValue) {
            if (rowValue instanceof Float rf && conditionValue instanceof Float cf) {
                return Math.abs(rf - cf) < 1e-7;
            } else if (rowValue instanceof Double rd && conditionValue instanceof Double cd) {
                return Math.abs(rd - cd) < 1e-7;
            } else if (rowValue instanceof BigDecimal rbd && conditionValue instanceof BigDecimal cbd) {
                return rbd.compareTo(cbd) == 0;
        } else {
            return String.valueOf(rowValue).equals(String.valueOf(conditionValue));
        }
    }

    /**
     * Converts a condition value to the target column type.
     */
    Object convertConditionValue(Object value, String column, Class<?> targetType, Map<String, Class<?>> columnTypes) {
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

    /**
     * Compares two values numerically or as Comparable.
     */
    int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }
        if (left instanceof Number && right instanceof Number) {
            if (left instanceof BigDecimal lb && right instanceof BigDecimal rb) {
                BigDecimal leftBD = lb;
                BigDecimal rightBD = rb;
                return leftBD.compareTo(rightBD);
            }
            if (left instanceof Float lf && right instanceof Float rf) {
                return Float.compare(lf, rf);
            }
            if (left instanceof Double ld && right instanceof Double rd) {
                return Double.compare(ld, rd);
            }
            return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
        }
        @SuppressWarnings("unchecked")
        Comparable<Object> leftComparable = (Comparable<Object>) left;
        return leftComparable.compareTo(right);
    }
}
