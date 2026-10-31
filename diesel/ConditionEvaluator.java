package diesel;

import static diesel.ThreeValuedLogic.TRUE;
import static diesel.ThreeValuedLogic.FALSE;
import static diesel.ThreeValuedLogic.UNKNOWN;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import diesel.storage.json.JsonPathResolver;

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
        return evaluateConditions3vl(row, conditions, columnTypes, java.util.Collections.emptyMap());
    }

    /**
     * Evaluates a list of conditions with per-column collations (prompt4 #49).
     * A column mapped to {@code CASE_INSENSITIVE} compares strings with
     * {@link String#equalsIgnoreCase}; columns absent from {@code collations}
     * keep the default case-sensitive comparison.
     */
    ThreeValuedLogic evaluateConditions3vl(Map<String, Object> row, List<QueryParser.Condition> conditions,
                                           Map<String, Class<?>> columnTypes, Map<String, String> collations) {
        if (conditions.isEmpty()) {
            return TRUE;
        }
        ThreeValuedLogic result = evaluateCondition3vl(row, conditions.get(0), columnTypes, collations);
        for (int i = 1; i < conditions.size(); i++) {
            QueryParser.Condition condition = conditions.get(i);
            String conjunction = condition.conjunction;
            if (Objects.equals(conjunction, SqlKeywords.OR)) {
                if (!result.orIsDetermined()) {
                    result = result.or(evaluateCondition3vl(row, condition, columnTypes, collations));
                }
            } else if ((conjunction == null || conjunction.equalsIgnoreCase(SqlKeywords.AND))
                    && !result.andIsDetermined()) {
                result = result.and(evaluateCondition3vl(row, condition, columnTypes, collations));
            }
        }
        return result;
    }

    /**
     * Convenience wrapper that returns a boolean result.
     */
    boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        return evaluateConditions(row, conditions, columnTypes, java.util.Collections.emptyMap());
    }

    /**
     * Convenience wrapper with collations (prompt4 #49).
     */
    boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions,
                               Map<String, Class<?>> columnTypes, Map<String, String> collations) {
        return evaluateConditions3vl(row, conditions, columnTypes, collations).isTrue();
    }

    /**
     * Evaluates a single condition against a row using SQL three-valued logic.
     */
    ThreeValuedLogic evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        return evaluateCondition3vl(row, condition, columnTypes, java.util.Collections.emptyMap());
    }

    private ThreeValuedLogic evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition,
                                                  Map<String, Class<?>> columnTypes, Map<String, String> collations) {
        if (condition.isGrouped()) {
            return evaluateGroupedCondition(row, condition, columnTypes, collations);
        }

        if (condition.isNullOperator()) {
            return evaluateIsNullCondition(row, condition);
        }

        Object rowValue = resolveRowValue(row, condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.FINE, "Row value for column {0} is null, condition is UNKNOWN", condition.column);
            return UNKNOWN;
        }

        if (condition.isInOperator()) {
            return evaluateInCondition(rowValue, condition, columnTypes, collations);
        }

        Object conditionValue = convertConditionValue(condition.value, condition.column, rowValue.getClass());
        if (conditionValue == null) {
            LOGGER.log(Level.FINE, "Condition value for column {0} is null, condition is UNKNOWN", condition.column);
            return UNKNOWN;
        }
        LOGGER.log(Level.FINE, "Condition values: rowValue={0}, conditionValue={1}, column={2}, operator={3}",
                new Object[]{rowValue, conditionValue, condition.column, condition.operator});

        boolean result = evaluateOperatorCondition(rowValue, conditionValue, condition, collations);

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue: {1}, conditionValue: {2}, result: {3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return result ? TRUE : FALSE;
    }

    /**
     * Resolves a dotted path against the row's columns. For dotted paths like
     * "PROFL.user.address.city", it extracts the value from the JSON text in
     * the PROFL column.
     */
    private Object resolveRowValue(Map<String, Object> row, String columnKey) {
        Object direct = row.get(columnKey);
        if (direct != null) {
            return direct;
        }
        
        // Check if this is a dotted path that needs resolution
        if (columnKey.contains(".")) {
            List<String> columns = new ArrayList<>(row.keySet());
            TreeMap<String, Integer> indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < columns.size(); i++) {
                indexByName.put(columns.get(i), i);
            }
            
            JsonPathResolver.SchemaIndex schemaIndex = new JsonPathResolver.SchemaIndex(columns, indexByName);
            JsonPathResolver.ResolvedPath resolved = schemaIndex.resolve(columnKey);
            
            if (resolved.columnIndex() >= 0 && !resolved.segments().isEmpty()) {
                Object baseValue = row.get(resolved.column());
                if (baseValue instanceof String jsonText) {
                    return JsonPathResolver.extract(jsonText, resolved.segments(), 
                            new diesel.storage.json.JsonParserConfig.Builder().build());
                }
            }
        }
        
        return null;
    }

    private ThreeValuedLogic evaluateGroupedCondition(Map<String, Object> row, QueryParser.Condition condition,
                                                      Map<String, Class<?>> columnTypes, Map<String, String> collations) {
        ThreeValuedLogic subResult = evaluateConditions3vl(row, condition.subConditions, columnTypes, collations);
        return condition.not ? subResult.not() : subResult;
    }

    private ThreeValuedLogic evaluateIsNullCondition(Map<String, Object> row, QueryParser.Condition condition) {
        Object value = row.get(condition.column);
        boolean isNull = value == null;
        boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
        LOGGER.log(Level.FINE, "Evaluated IS NULL condition: {0}, value: {1}, result: {2}",
                new Object[]{condition, value, result});
        boolean finalResult = condition.not ? !result : result;
        return finalResult ? TRUE : FALSE;
    }

    private ThreeValuedLogic evaluateInCondition(Object rowValue, QueryParser.Condition condition,
                                                 Map<String, Class<?>> columnTypes, Map<String, String> collations) {
        boolean caseInsensitive = isCaseInsensitive(condition.column, collations);
        boolean inResult = false;
        for (Object value : condition.inValues) {
            Object convertedValue = convertConditionValue(value, condition.column, rowValue.getClass());
            if (valuesEqual(rowValue, convertedValue, caseInsensitive)) {
                inResult = true;
                break;
            }
        }
        boolean result = condition.not ? !inResult : inResult;
        LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue: {1}, values: {2}, result: {3}",
                new Object[]{condition, rowValue, condition.inValues, result});
        return result ? TRUE : FALSE;
    }

    private boolean evaluateOperatorCondition(Object rowValue, Object conditionValue, QueryParser.Condition condition,
                                              Map<String, String> collations) {
        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            return evaluateLikeCondition(rowValue, conditionValue, condition);
        }
        if (condition.operator == QueryParser.Operator.EQUALS || condition.operator == QueryParser.Operator.NOT_EQUALS) {
            return evaluateEqualityCondition(rowValue, conditionValue, condition, collations);
        }
        return evaluateComparisonCondition(rowValue, conditionValue, condition);
    }

    private boolean evaluateLikeCondition(Object rowValue, Object conditionValue, QueryParser.Condition condition) {
        if (!(rowValue instanceof String rowStr) || !(conditionValue instanceof String condStr)) {
            throw new IllegalArgumentException("LIKE and NOT LIKE operators are only supported for String types");
        }
        try {
            String regex = QueryParser.convertLikePatternToRegex(condStr);
            boolean matches = rowStr.matches(regex);
            return condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid LIKE pattern: " + conditionValue, e);
        }
    }

    private boolean evaluateEqualityCondition(Object rowValue, Object conditionValue, QueryParser.Condition condition,
                                              Map<String, String> collations) {
        boolean isEqual = valuesEqual(rowValue, conditionValue, isCaseInsensitive(condition.column, collations));
        return condition.operator == QueryParser.Operator.EQUALS ? isEqual : !isEqual;
    }

    private boolean evaluateComparisonCondition(Object rowValue, Object conditionValue, QueryParser.Condition condition) {
        if (!(rowValue instanceof Comparable) || !(conditionValue instanceof Comparable)) {
            throw new IllegalArgumentException("Comparison operators <, >, <=, >= only supported for comparable types");
        }
        int comparison = compareValues(rowValue, conditionValue);
        return switch (condition.operator) {
            case LESS_THAN -> comparison < 0;
            case LESS_THAN_OR_EQUALS -> comparison <= 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUALS -> comparison >= 0;
            default -> throw new IllegalStateException(ErrorMessages.UNSUPPORTED_OPERATOR_PREFIX + condition.operator);
        };
    }

    /**
     * Checks equality with epsilon comparison for floating-point types.
     */
    boolean valuesEqual(Object rowValue, Object conditionValue) {
        return valuesEqual(rowValue, conditionValue, false);
    }

    /**
     * Checks equality honoring a CASE_INSENSITIVE collation (prompt4 #49):
     * when {@code caseInsensitive} is true and both operands are Strings the
     * comparison uses {@link String#equalsIgnoreCase}; every other type keeps
     * the exact-equality semantics of the two-argument overload.
     */
    boolean valuesEqual(Object rowValue, Object conditionValue, boolean caseInsensitive) {
        if (rowValue instanceof Float rf && conditionValue instanceof Float cf) {
            return Math.abs(rf - cf) < 1e-7;
        } else if (rowValue instanceof Double rd && conditionValue instanceof Double cd) {
            return Math.abs(rd - cd) < 1e-7;
        } else if (rowValue instanceof BigDecimal rbd && conditionValue instanceof BigDecimal cbd) {
            return rbd.compareTo(cbd) == 0;
        } else if (caseInsensitive && rowValue instanceof String rs && conditionValue instanceof String cs) {
            return rs.equalsIgnoreCase(cs);
        } else {
            return String.valueOf(rowValue).equals(String.valueOf(conditionValue));
        }
    }

    /**
     * Whether the column carries the CASE_INSENSITIVE collation (prompt4 #49).
     * A qualified {@code TABLE.COLUMN} key is stripped before the lookup so
     * both bare and qualified condition columns resolve against the map.
     */
    private static boolean isCaseInsensitive(String column, Map<String, String> collations) {
        if (collations == null || collations.isEmpty() || column == null) {
            return false;
        }
        String bare = column;
        int dot = column.indexOf('.');
        if (dot >= 0 && dot + 1 < column.length()) {
            bare = column.substring(dot + 1);
        }
        return "CASE_INSENSITIVE".equals(collations.get(bare))
                || "CASE_INSENSITIVE".equals(collations.get(column));
    }

    /**
     * Converts a condition value to the target column type.
     */
    @Nullable
    Object convertConditionValue(Object value, String column, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType == null) {
            return value;
        }

        Class<?> valueType = value.getClass();
        if (targetType.isAssignableFrom(valueType)) {
            return value;
        }

        String stringValue = String.valueOf(value);
        try {
            return parseConvertedValue(stringValue, targetType, column);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot convert value '" + stringValue + "' to type " + targetType.getSimpleName() + " for column " + column, e);
        }
    }

    private Object parseConvertedValue(String stringValue, Class<?> targetType, String column) {
        if (targetType == String.class) {
            return stringValue;
        }
        if (targetType == Integer.class) {
            return Integer.parseInt(stringValue);
        }
        if (targetType == Long.class) {
            return Long.parseLong(stringValue);
        }
        if (targetType == Short.class) {
            return Short.parseShort(stringValue);
        }
        if (targetType == Byte.class) {
            return Byte.parseByte(stringValue);
        }
        if (targetType == Float.class) {
            return Float.parseFloat(stringValue);
        }
        if (targetType == Double.class) {
            return Double.parseDouble(stringValue);
        }
        if (targetType == BigDecimal.class) {
            return new BigDecimal(stringValue);
        }
        if (targetType == Boolean.class) {
            // Prompt 4 #48 (R3-013b): strict parsing - Boolean.parseBoolean("1")
            // silently yields false, so an Integer 1 condition value would match
            // the FALSE rows on DELETE/UPDATE; parseBooleanLiteral accepts
            // 1/0/yes/no/t/f and throws otherwise.
            return SqlParsingUtils.parseBooleanLiteral(stringValue);
        }
        if (targetType == UUID.class) {
            return UUID.fromString(stringValue);
        }
        if (targetType == Character.class) {
            return parseCharacterValue(stringValue, column);
        }
        throw new IllegalArgumentException("Unsupported type conversion for column " + column + ": " + targetType.getSimpleName());
    }

    private char parseCharacterValue(String stringValue, String column) {
        if (stringValue.length() == 1) {
            return stringValue.charAt(0);
        }
        throw new IllegalArgumentException("Invalid character value for column " + column);
    }

    /**
     * Compares two values numerically or as Comparable.
     */
    int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return compareNulls(left, right);
        }
        if (left instanceof Number && right instanceof Number) {
            return compareNumbers(left, right);
        }
        @SuppressWarnings("unchecked")
        Comparable<Object> leftComparable = (Comparable<Object>) left;
        return leftComparable.compareTo(right);
    }

    private int compareNulls(Object left, Object right) {
        if (left == right) {
            return 0;
        }
        return left == null ? -1 : 1;
    }

    private int compareNumbers(Object left, Object right) {
        if (left instanceof BigDecimal lb && right instanceof BigDecimal rb) {
            return lb.compareTo(rb);
        }
        if (left instanceof Float lf && right instanceof Float rf) {
            return Float.compare(lf, rf);
        }
        if (left instanceof Double ld && right instanceof Double rd) {
            return Double.compare(ld, rd);
        }
        return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
    }
}
