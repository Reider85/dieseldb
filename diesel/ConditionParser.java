package diesel;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ConditionParser {
    private static final Logger LOGGER = Logger.getLogger(ConditionParser.class.getName());

    List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        LOGGER.log(Level.FINE, "Parsing conditions for table {0}, column types: {1}, isOnClause: {2}, condition: {3}",
                new Object[]{tableName, combinedColumnTypes, isOnClause, conditionStr});

        String cleanConditionStr = conditionStr;
        String[] limitSplit = cleanConditionStr.toUpperCase().contains(" LIMIT ")
                ? cleanConditionStr.split("(?i)\\s+LIMIT\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = limitSplit[0].trim();
        if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected LIMIT clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] offsetSplit = cleanConditionStr.toUpperCase().contains(" OFFSET ")
                ? cleanConditionStr.split("(?i)\\s+OFFSET\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = offsetSplit[0].trim();
        if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected OFFSET clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] orderBySplit = cleanConditionStr.toUpperCase().contains(" ORDER BY ")
                ? cleanConditionStr.split("(?i)\\s+ORDER BY\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = orderBySplit[0].trim();
        if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected ORDER BY clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        String[] groupBySplit = cleanConditionStr.toUpperCase().contains(" GROUP BY ")
                ? cleanConditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                : new String[]{cleanConditionStr, ""};
        cleanConditionStr = groupBySplit[0].trim();
        if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
            LOGGER.log(Level.FINE, "Detected GROUP BY clause in condition string, stopping at: {0}", cleanConditionStr);
        }

        if (cleanConditionStr.isEmpty()) {
            return new ArrayList<>();
        }

        List<Condition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        int nestingLevel = 0;

        for (int i = 0; i < cleanConditionStr.length(); i++) {
            char c = cleanConditionStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    nestingLevel++;
                    if (parenDepth == 1) {
                        continue;
                    }
                } else if (c == ')') {
                    parenDepth--;
                    nestingLevel--;
                    if (parenDepth == 0) {
                        String subConditionStr = currentCondition.toString().trim();
                        if (!subConditionStr.isEmpty()) {
                            boolean isNot = subConditionStr.toUpperCase().startsWith("NOT ");
                            if (isNot) {
                                subConditionStr = subConditionStr.substring(4).trim();
                            }
                            if (subConditionStr.isEmpty()) {
                                throw new IllegalArgumentException("Empty grouped condition in clause: " + cleanConditionStr);
                            }
                            LOGGER.log(Level.FINE, "Parsing nested condition at level {0}: {1}, isNot: {2}",
                                    new Object[]{nestingLevel + 1, subConditionStr, isNot});
                            List<Condition> subConditions = parseConditions(subConditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes);
                            if (subConditions.isEmpty()) {
                                throw new IllegalArgumentException("No valid conditions found in grouped clause: " + subConditionStr);
                            }
                            String conjunction = determineConjunctionAfter(cleanConditionStr, i);
                            conditions.add(new Condition(subConditions, conjunction, isNot));
                            LOGGER.log(Level.FINE, "Added grouped condition with {0} subconditions, conjunction: {1}",
                                    new Object[]{subConditions.size(), conjunction});
                        } else {
                            throw new IllegalArgumentException("Empty grouped condition in clause: " + cleanConditionStr);
                        }
                        currentCondition = new StringBuilder();
                        continue;
                    }
                } else if (parenDepth == 0) {
                    if (i + 3 <= cleanConditionStr.length() && cleanConditionStr.substring(i, i + 3).equalsIgnoreCase("AND") &&
                            (i == 0 || Character.isWhitespace(cleanConditionStr.charAt(i - 1))) &&
                            (i + 3 == cleanConditionStr.length() || Character.isWhitespace(cleanConditionStr.charAt(i + 3)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "AND", combinedColumnTypes, originalQuery, isOnClause, tableName));
                            LOGGER.log(Level.FINE, "Added condition with AND: {0}", current);
                        }
                        currentCondition = new StringBuilder();
                        i += 2;
                        continue;
                    } else if (i + 2 <= cleanConditionStr.length() && cleanConditionStr.substring(i, i + 2).equalsIgnoreCase("OR") &&
                            (i == 0 || Character.isWhitespace(cleanConditionStr.charAt(i - 1))) &&
                            (i + 2 == cleanConditionStr.length() || Character.isWhitespace(cleanConditionStr.charAt(i + 2)))) {
                        String current = currentCondition.toString().trim();
                        if (!current.isEmpty()) {
                            conditions.add(parseSingleCondition(current, "OR", combinedColumnTypes, originalQuery, isOnClause, tableName));
                            LOGGER.log(Level.FINE, "Added condition with OR: {0}", current);
                        }
                        currentCondition = new StringBuilder();
                        i += 1;
                        continue;
                    }
                }
            }
            currentCondition.append(c);
        }

        if (currentCondition.length() > 0 && parenDepth == 0) {
            String current = currentCondition.toString().trim();
            if (!current.isEmpty()) {
                conditions.add(parseSingleCondition(current, null, combinedColumnTypes, originalQuery, isOnClause, tableName));
                LOGGER.log(Level.FINE, "Added final condition: {0}", current);
            }
        }

        if (parenDepth != 0) {
            throw new IllegalArgumentException("Mismatched parentheses in condition clause: " + cleanConditionStr);
        }

        LOGGER.log(Level.FINE, "Parsed conditions: {0}", conditions);

        return conditions;
    }

    List<Condition> parseHavingConditions(String havingStr, String tableName, Database database, String originalQuery, List<AggregateFunction> aggregates, List<String> groupBy, Map<String, Class<?>> combinedColumnTypes) {
        LOGGER.log(Level.FINE, "Received HAVING clause: {0}", havingStr);
        if (havingStr == null || havingStr.trim().isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty or null HAVING clause received");
            return new ArrayList<>();
        }
        LOGGER.log(Level.FINE, "Passing HAVING clause to parseConditions: {0}", havingStr);
        List<Condition> havingConditions = parseConditions(havingStr, tableName, database, originalQuery, false, combinedColumnTypes);

        for (Condition condition : havingConditions) {
            validateHavingCondition(condition, aggregates, groupBy, combinedColumnTypes);
        }

        return havingConditions;
    }

    private void validateHavingCondition(Condition condition, List<AggregateFunction> aggregates, List<String> groupBy, Map<String, Class<?>> combinedColumnTypes) {
        if (condition.isGrouped()) {
            for (Condition subCond : condition.subConditions) {
                validateHavingCondition(subCond, aggregates, groupBy, combinedColumnTypes);
            }
            return;
        }

        if (condition.isInOperator() || condition.isColumnComparison() || condition.isNullOperator()) {
            throw new IllegalArgumentException("HAVING clause does not support IN, column comparisons, IS NULL, or IS NOT NULL: " + condition);
        }

        String column = condition.column;
        if (column == null) {
            throw new IllegalArgumentException("Invalid HAVING condition: no column specified");
        }

        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)?\\s*\\)");
        Matcher aggMatcher = aggPattern.matcher(column);

        boolean isAggregate = aggMatcher.matches();
        boolean isGroupByColumn = groupBy.contains(column);

        if (!isAggregate && !isGroupByColumn) {
            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
            isGroupByColumn = groupBy.stream().anyMatch(gb -> {
                String unqualifiedGroupBy = gb.contains(".") ? gb.split("\\.")[1].trim() : gb;
                return unqualifiedGroupBy.equalsIgnoreCase(unqualifiedColumn);
            });

            if (!isGroupByColumn) {
                boolean isAliasedAggregate = aggregates.stream().anyMatch(agg ->
                        agg.alias != null && agg.alias.equalsIgnoreCase(unqualifiedColumn));
                if (!isAliasedAggregate) {
                    isAliasedAggregate = aggregates.stream().anyMatch(agg ->
                            agg.toString().equalsIgnoreCase(column));
                    if (!isAliasedAggregate) {
                        LOGGER.log(Level.SEVERE, "HAVING clause must reference an aggregate function or a GROUP BY column: {0}", column);
                        throw new IllegalArgumentException("HAVING clause must reference an aggregate function or a GROUP BY column: " + column);
                    }
                }
            }
        }

        if (isAggregate) {
            String aggFunction = aggMatcher.group(1).toUpperCase();
            String aggColumn = aggMatcher.group(2);
            if (!aggFunction.equals("COUNT") && aggColumn == null) {
                throw new IllegalArgumentException("Aggregate function " + aggFunction + " requires a column argument in HAVING clause");
            }
            if (aggColumn != null && !aggColumn.equals("*")) {
                String normalizedAggColumn = normalizeColumnName(aggColumn, null);
                String unqualifiedAggColumn = normalizedAggColumn.contains(".") ? normalizedAggColumn.split("\\.")[1].trim() : normalizedAggColumn;
                boolean found = false;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(unqualifiedAggColumn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOGGER.log(Level.SEVERE, "Unknown column in aggregate function in HAVING: {0}, available columns: {1}",
                            new Object[]{aggColumn, combinedColumnTypes.keySet()});
                    throw new IllegalArgumentException("Unknown column in aggregate function in HAVING: " + aggColumn);
                }
            }
        }

        if (!(condition.operator == QueryParser.Operator.EQUALS ||
                condition.operator == QueryParser.Operator.LESS_THAN ||
                condition.operator == QueryParser.Operator.GREATER_THAN)) {
            throw new IllegalArgumentException("HAVING clause only supports =, <, > operators: " + condition);
        }
    }

    private Condition parseSingleCondition(String condition, String conjunction, Map<String, Class<?>> columnTypes, String originalQuery, boolean isOnClause, String tableName) {
        LOGGER.log(Level.FINE, "Parsing single condition: {0}, isOnClause: {1}, conjunction: {2}",
                new Object[]{condition, isOnClause, conjunction});
        boolean isNot = condition.toUpperCase().startsWith("NOT ");
        String conditionWithoutNot = isNot ? condition.substring(4).trim() : condition;
        LOGGER.log(Level.FINE, "Condition without NOT: {0}", conditionWithoutNot);

        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)?\\s*\\)");
        Matcher aggMatcher = aggPattern.matcher(conditionWithoutNot);
        String conditionColumn = null;
        int aggEndIndex = -1;
        boolean isAggregate = false;
        LOGGER.log(Level.FINE, "Checking aggregate condition: {0}", conditionWithoutNot);
        if (aggMatcher.find()) {
            conditionColumn = conditionWithoutNot.substring(0, aggMatcher.end()).trim();
            aggEndIndex = aggMatcher.end();
            isAggregate = true;
        } else {
            LOGGER.log(Level.FINE, "No valid aggregate function found in: {0}", conditionWithoutNot);
        }

        // Handle IN conditions
        if (conditionWithoutNot.toUpperCase().contains(" IN ")) {
            String originalCondition = extractOriginalCondition(originalQuery, conditionWithoutNot);
            LOGGER.log(Level.FINE, "Extracted original IN condition: {0}", originalCondition);

            Pattern inPattern = Pattern.compile("(?i)((?:\\w+\\.)?\\w+)\\s+IN\\s*(?:\\(([^)]+)\\)|([^)]+))");
            Matcher inMatcher = inPattern.matcher(originalCondition);
            if (!inMatcher.find()) {
                LOGGER.log(Level.SEVERE, "Invalid IN condition format: {0}, original query: {1}",
                        new Object[]{originalCondition, originalQuery});
                throw new IllegalArgumentException("Invalid IN clause: " + originalCondition);
            }

            String parsedColumn = normalizeColumnName(inMatcher.group(1).trim(), isOnClause ? null : tableName);
            String valuesPart = inMatcher.group(2) != null ? inMatcher.group(2).trim() : inMatcher.group(3).trim();

            if (valuesPart.isEmpty()) {
                throw new IllegalArgumentException("IN clause cannot be empty");
            }

            List<String> valueStrings = splitInValues(valuesPart);
            LOGGER.log(Level.FINE, "Split IN values: {0}", Arrays.toString(valueStrings.toArray()));

            List<Object> inValues = new ArrayList<>();
            Class<?> columnType = getColumnType(parsedColumn, columnTypes);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                        new Object[]{parsedColumn, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + parsedColumn);
            }

            for (String val : valueStrings) {
                String trimmedVal = val.trim();
                if (trimmedVal.isEmpty()) continue;
                inValues.add(QueryParser.parseConditionValue(parsedColumn, trimmedVal, columnType));
            }

            LOGGER.log(Level.FINE, "Parsed {0} condition: column={1}, values={2}, not={3}, conjunction={4}",
                    new Object[]{isNot ? "NOT IN" : "IN", parsedColumn, inValues, isNot, conjunction});
            return new Condition(parsedColumn, inValues, conjunction, isNot);
        }

        String remainingCondition = aggEndIndex >= 0 ? conditionWithoutNot.substring(aggEndIndex).trim() : conditionWithoutNot;

        if (isAggregate) {
            String[] partsByOperator = null;
            QueryParser.Operator operator = null;
            String upperRemaining = remainingCondition.toUpperCase();

            if (upperRemaining.contains(" = ")) {
                partsByOperator = remainingCondition.split("\\s*=\\s*", 2);
                operator = QueryParser.Operator.EQUALS;
            } else if (upperRemaining.contains(" < ")) {
                partsByOperator = remainingCondition.split("\\s*<\\s*", 2);
                operator = QueryParser.Operator.LESS_THAN;
            } else if (upperRemaining.contains(" > ")) {
                partsByOperator = remainingCondition.split("\\s*>\\s*", 2);
                operator = QueryParser.Operator.GREATER_THAN;
            } else if (upperRemaining.contains(" != ")) {
                partsByOperator = remainingCondition.split("\\s*!=\\s*", 2);
                operator = QueryParser.Operator.NOT_EQUALS;
            } else if (upperRemaining.contains(" <> ")) {
                partsByOperator = remainingCondition.split("\\s*<>\\s*", 2);
                operator = QueryParser.Operator.NOT_EQUALS;
            } else {
                throw new IllegalArgumentException("Invalid condition operator for aggregate function in: " + conditionWithoutNot);
            }

            if (partsByOperator.length != 2) {
                throw new IllegalArgumentException("Invalid aggregate condition format: " + conditionWithoutNot);
            }

            String rightOperand = partsByOperator[1].trim();
            Class<?> valueType = conditionColumn.toUpperCase().startsWith("COUNT") ? Long.class : Double.class;
            Object value = QueryParser.parseConditionValue(conditionColumn, rightOperand, valueType);
            LOGGER.log(Level.FINE, "Parsed aggregate condition: column={0}, operator={1}, value={2}, not={3}, conjunction={4}",
                    new Object[]{conditionColumn, operator, value, isNot, conjunction});
            return new Condition(conditionColumn, value, operator, conjunction, isNot);
        }

        // Handle non-aggregate conditions
        String[] partsByOperator = null;
        QueryParser.Operator operator = null;
        String upperRemaining = remainingCondition.toUpperCase();

        if (upperRemaining.contains(" IS NOT NULL")) {
            partsByOperator = remainingCondition.split("\\s*IS NOT NULL\\s*", 2);
            operator = QueryParser.Operator.IS_NOT_NULL;
        } else if (upperRemaining.contains(" IS NULL")) {
            partsByOperator = remainingCondition.split("\\s*IS NULL\\s*", 2);
            operator = QueryParser.Operator.IS_NULL;
        } else if (upperRemaining.contains(" NOT LIKE ")) {
            partsByOperator = remainingCondition.split("\\s*NOT LIKE\\s*", 2);
            operator = QueryParser.Operator.NOT_LIKE;
        } else if (upperRemaining.contains(" LIKE ")) {
            partsByOperator = remainingCondition.split("\\s*LIKE\\s*", 2);
            operator = QueryParser.Operator.LIKE;
        } else if (upperRemaining.contains(" != ")) {
            partsByOperator = remainingCondition.split("\\s*!=\\s*", 2);
            operator = QueryParser.Operator.NOT_EQUALS;
        } else if (upperRemaining.contains(" <> ")) {
            partsByOperator = remainingCondition.split("\\s*<>\\s*", 2);
            operator = QueryParser.Operator.NOT_EQUALS;
        } else if (upperRemaining.contains(" = ")) {
            partsByOperator = remainingCondition.split("\\s*=\\s*", 2);
            operator = QueryParser.Operator.EQUALS;
        } else if (upperRemaining.contains(" < ")) {
            partsByOperator = remainingCondition.split("\\s*<\\s*", 2);
            operator = QueryParser.Operator.LESS_THAN;
        } else if (upperRemaining.contains(" > ")) {
            partsByOperator = remainingCondition.split("\\s*>\\s*", 2);
            operator = QueryParser.Operator.GREATER_THAN;
        } else {
            throw new IllegalArgumentException("Invalid condition operator in: " + conditionWithoutNot);
        }

        if (operator == QueryParser.Operator.IS_NULL || operator == QueryParser.Operator.IS_NOT_NULL) {
            String column = normalizeColumnName(partsByOperator[0].trim(), isOnClause ? null : tableName);
            Class<?> columnType = getColumnType(column, columnTypes);
            if (columnType == null) {
                LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
            LOGGER.log(Level.FINE, "Parsed {0} condition: column={1}, conjunction={2}",
                    new Object[]{operator, column, conjunction});
            return new Condition(column, operator, conjunction, isNot);
        }

        if (partsByOperator.length != 2) {
            throw new IllegalArgumentException("Invalid condition format: " + conditionWithoutNot);
        }

        String leftOperand = partsByOperator[0].trim();
        String rightOperand = partsByOperator[1].trim();

        String column = normalizeColumnName(leftOperand, isOnClause ? null : tableName);
        String rightColumn = null;
        Object value = null;

        Class<?> columnType = getColumnType(column, columnTypes);
        if (columnType == null) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{column, columnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + column);
        }

        // Check if rightOperand is a column name, especially for ON clauses
        if (rightOperand.matches("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*")) {
            rightColumn = normalizeColumnName(rightOperand, isOnClause ? null : tableName);
            Class<?> rightColumnType = getColumnType(rightColumn, columnTypes);
            if (rightColumnType != null) {
                if (operator == QueryParser.Operator.LIKE || operator == QueryParser.Operator.NOT_LIKE) {
                    throw new IllegalArgumentException("LIKE and NOT LIKE are not supported for column comparisons: " + conditionWithoutNot);
                }
                LOGGER.log(Level.FINE, "Parsed column comparison condition: left={0}, right={1}, operator={2}, conjunction={3}",
                        new Object[]{column, rightColumn, operator, conjunction});
                return new Condition(column, rightColumn, operator, conjunction, isNot);
            }
        }

        if (operator == QueryParser.Operator.LIKE || operator == QueryParser.Operator.NOT_LIKE) {
            if (!rightOperand.startsWith("'") || !rightOperand.endsWith("'")) {
                throw new IllegalArgumentException("LIKE pattern must be a string literal: " + rightOperand);
            }
            String pattern = rightOperand.substring(1, rightOperand.length() - 1);
            value = QueryParser.convertLikePatternToRegex(pattern);
        } else {
            value = QueryParser.parseConditionValue(column, rightOperand, columnType);
        }

        // Validate that value is not an Operator
        if (value instanceof QueryParser.Operator) {
            LOGGER.log(Level.SEVERE, "Invalid condition: value is an Operator: {0} in condition: {1}",
                    new Object[]{value, conditionWithoutNot});
            throw new IllegalArgumentException("Invalid condition: value cannot be an Operator: " + conditionWithoutNot);
        }

        LOGGER.log(Level.FINE, "Parsed condition: column={0}, operator={1}, value={2}, rightColumn={3}, not={4}, conjunction={5}",
                new Object[]{column, operator, value, rightColumn, isNot, conjunction});
        return new Condition(column, value, operator, conjunction, isNot);
    }

    private String extractOriginalCondition(String originalQuery, String conditionWithoutNot) {
        String normalizedCondition = conditionWithoutNot.toUpperCase();
        String originalUpper = originalQuery.toUpperCase();
        int startIdx = originalUpper.indexOf(normalizedCondition);
        if (startIdx == -1) {
            return conditionWithoutNot;
        }
        return originalQuery.substring(startIdx, startIdx + conditionWithoutNot.length());
    }

    private List<String> splitInValues(String valuesPart) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < valuesPart.length(); i++) {
            char c = valuesPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentValue.append(c);
                continue;
            }
            if (!inQuotes && c == ',') {
                String value = currentValue.toString().trim();
                if (!value.isEmpty()) {
                    values.add(value);
                }
                currentValue = new StringBuilder();
                continue;
            }
            currentValue.append(c);
        }

        String lastValue = currentValue.toString().trim();
        if (!lastValue.isEmpty()) {
            values.add(lastValue);
        }

        return values;
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> columnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String normalizeColumnName(String column, String tableName) {
        if (column == null || column.isEmpty()) {
            return column;
        }
        String normalized = column.trim();
        if (tableName != null && !normalized.contains(".")) {
            normalized = tableName + "." + normalized;
        }
        return normalized;
    }

    private String determineConjunctionAfter(String conditionStr, int index) {
        String remaining = conditionStr.substring(index + 1).trim();
        if (remaining.toUpperCase().startsWith("AND ")) {
            return "AND";
        } else if (remaining.toUpperCase().startsWith("OR ")) {
            return "OR";
        }
        return null;
    }
}