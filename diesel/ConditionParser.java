package diesel;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ConditionParser {
    private static final Logger LOGGER = Logger.getLogger(ConditionParser.class.getName());
    private static final int MAX_RECURSION_DEPTH = 100; // Prevent infinite recursion

    List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes) {
        return parseConditions(conditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes, 0);
    }

    private List<Condition> parseConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes, int depth) {
        if (depth > MAX_RECURSION_DEPTH) {
            LOGGER.log(Level.SEVERE, "Maximum recursion depth exceeded while parsing condition: {0}", conditionStr);
            throw new IllegalArgumentException("Maximum recursion depth exceeded in condition parsing");
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        LOGGER.log(Level.FINE, "Parsing conditions for table {0}, column types: {1}, isOnClause: {2}, condition: {3}, depth: {4}",
                new Object[]{tableName, combinedColumnTypes, isOnClause, conditionStr, depth});

        String cleanConditionStr = conditionStr.trim();
        if (cleanConditionStr.isEmpty()) {
            return new ArrayList<>();
        }

        // Remove clauses like LIMIT, OFFSET, ORDER BY, GROUP BY
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

        // Parse the condition string into a list of conditions
        return parseComplexConditions(cleanConditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes, depth + 1);
    }

    private List<Condition> parseComplexConditions(String conditionStr, String tableName, Database database, String originalQuery, boolean isOnClause, Map<String, Class<?>> combinedColumnTypes, int depth) {
        List<Condition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        boolean inAggregateFunction = false;
        boolean inInClause = false;
        String lastConjunction = null;

        for (int i = 0; i < conditionStr.length(); i++) {
            char c = conditionStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (inQuotes) {
                currentCondition.append(c);
                continue;
            }
            if (c == '(') {
                parenDepth++;
                String current = currentCondition.toString().trim().toUpperCase();
                if (parenDepth == 1 && current.endsWith(" IN")) {
                    inInClause = true;
                    LOGGER.log(Level.FINE, "Detected IN clause start at index {0}: {1}", new Object[]{i, current});
                }
                currentCondition.append(c);
                continue;
            }
            if (c == ')') {
                parenDepth--;
                currentCondition.append(c);
                if (parenDepth == 0 && inAggregateFunction) {
                    inAggregateFunction = false;
                }
                if (parenDepth == 0 && inInClause) {
                    inInClause = false;
                    String subConditionStr = currentCondition.toString().trim();
                    if (!subConditionStr.isEmpty()) {
                        boolean isNot = subConditionStr.toUpperCase().startsWith("NOT ");
                        if (isNot) {
                            subConditionStr = subConditionStr.substring(4).trim();
                        }
                        conditions.add(parseSingleCondition(subConditionStr, lastConjunction, combinedColumnTypes, originalQuery, isOnClause, tableName));
                        LOGGER.log(Level.FINE, "Parsed IN condition: {0}, conjunction: {1}", new Object[]{subConditionStr, lastConjunction});
                        currentCondition = new StringBuilder();
                        lastConjunction = determineConjunctionAfter(conditionStr, i);
                    }
                }
                if (parenDepth == 0 && !inInClause) {
                    String subConditionStr = currentCondition.toString().trim();
                    if (!subConditionStr.isEmpty() && !subConditionStr.toUpperCase().contains(" IN ")) {
                        boolean isNot = subConditionStr.toUpperCase().startsWith("NOT ");
                        if (isNot) {
                            subConditionStr = subConditionStr.substring(4).trim();
                        }
                        if (subConditionStr.startsWith("(") && subConditionStr.endsWith(")")) {
                            subConditionStr = subConditionStr.substring(1, subConditionStr.length() - 1).trim();
                        }
                        if (!subConditionStr.isEmpty()) {
                            // Check if the condition is an aggregate function
                            Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)?\\s*\\)");
                            Matcher aggMatcher = aggPattern.matcher(subConditionStr);
                            if (aggMatcher.matches()) {
                                conditions.add(parseSingleCondition(subConditionStr, lastConjunction, combinedColumnTypes, originalQuery, isOnClause, tableName));
                            } else {
                                List<Condition> subConditions = parseConditions(subConditionStr, tableName, database, originalQuery, isOnClause, combinedColumnTypes, depth + 1);
                                if (!subConditions.isEmpty()) {
                                    conditions.add(new Condition(subConditions, lastConjunction, isNot));
                                    LOGGER.log(Level.FINE, "Added grouped condition with {0} subconditions, conjunction: {1}", new Object[]{subConditions.size(), lastConjunction});
                                }
                            }
                        }
                        currentCondition = new StringBuilder();
                        lastConjunction = determineConjunctionAfter(conditionStr, i);
                    }
                }
                continue;
            }
            if (parenDepth == 0 && !inAggregateFunction && !inInClause) {
                if (i + 3 <= conditionStr.length() && conditionStr.substring(i, i + 3).equalsIgnoreCase("AND") &&
                        (i == 0 || Character.isWhitespace(conditionStr.charAt(i - 1))) &&
                        (i + 3 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 3)))) {
                    String current = currentCondition.toString().trim();
                    if (!current.isEmpty()) {
                        conditions.add(parseSingleCondition(current, "AND", combinedColumnTypes, originalQuery, isOnClause, tableName));
                        LOGGER.log(Level.FINE, "Added condition with AND: {0}", current);
                    }
                    currentCondition = new StringBuilder();
                    lastConjunction = "AND";
                    i += 2;
                    continue;
                } else if (i + 2 <= conditionStr.length() && conditionStr.substring(i, i + 2).equalsIgnoreCase("OR") &&
                        (i == 0 || Character.isWhitespace(conditionStr.charAt(i - 1))) &&
                        (i + 2 == conditionStr.length() || Character.isWhitespace(conditionStr.charAt(i + 2)))) {
                    String current = currentCondition.toString().trim();
                    if (!current.isEmpty()) {
                        conditions.add(parseSingleCondition(current, "OR", combinedColumnTypes, originalQuery, isOnClause, tableName));
                        LOGGER.log(Level.FINE, "Added condition with OR: {0}", current);
                    }
                    currentCondition = new StringBuilder();
                    lastConjunction = "OR";
                    i += 1;
                    continue;
                }
            }
            currentCondition.append(c);
            // Check for aggregate functions
            if (!inQuotes && parenDepth == 0 && c == '(' && currentCondition.toString().trim().toUpperCase().matches(".*\\b(COUNT|MIN|MAX|AVG|SUM)\\s*$")) {
                inAggregateFunction = true;
            }
        }

        if (currentCondition.length() > 0 && parenDepth == 0) {
            String current = currentCondition.toString().trim();
            if (!current.isEmpty()) {
                conditions.add(parseSingleCondition(current, lastConjunction, combinedColumnTypes, originalQuery, isOnClause, tableName));
                LOGGER.log(Level.FINE, "Added final condition: {0}", current);
            }
        }

        if (parenDepth != 0) {
            throw new IllegalArgumentException("Mismatched parentheses in condition clause: " + conditionStr);
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
        List<Condition> havingConditions = parseConditions(havingStr, tableName, database, originalQuery, false, combinedColumnTypes, 0);

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
                        throw new IllegalArgumentException("HAVING clause must reference an aggregate function or a GROUP BY column: " + condition);
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
                String normalizedAggColumn = NormalizationUtils.normalizeColumnName(aggColumn, null);
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

        // Extract NOT and validate condition
        ConditionParseContext context = extractNotAndValidate(condition);
        String normalizedCondition = context.normalizedCondition;
        boolean isNot = context.isNot;

        // Check for aggregate functions
        AggregateParseResult aggResult = parseAggregateCondition(normalizedCondition);
        if (aggResult.isAggregate) {
            return parseAggregateCondition(aggResult, normalizedCondition, conjunction, columnTypes, isNot);
        }

        // Check for IN clause
        if (normalizedCondition.toUpperCase().contains(" IN ")) {
            return parseInCondition(normalizedCondition, conjunction, columnTypes, originalQuery, isOnClause, tableName, isNot);
        }

        // Parse operator and split condition
        OperatorParseResult opResult = parseOperatorAndSplit(normalizedCondition);
        if (opResult == null) {
            LOGGER.log(Level.SEVERE, "Invalid condition operator in non-aggregate condition: {0}", normalizedCondition);
            throw new IllegalArgumentException("Invalid condition operator in: " + normalizedCondition);
        }

        // Create the final Condition object
        return createCondition(opResult, normalizedCondition, conjunction, columnTypes, isOnClause, tableName, isNot);
    }

    private ConditionParseContext extractNotAndValidate(String condition) {
        boolean isNot = condition.toUpperCase().startsWith("NOT ");
        String conditionWithoutNot = isNot ? condition.substring(4).trim() : condition;
        LOGGER.log(Level.FINE, "Condition without NOT: {0}", conditionWithoutNot);

        String normalizedCondition = conditionWithoutNot.trim();
        if (normalizedCondition.isEmpty()) {
            throw new IllegalArgumentException("Empty condition after normalization");
        }

        if (normalizedCondition.toUpperCase().contains("COUNT") && !normalizedCondition.matches("(?i).*COUNT\\s*\\(.*\\).*")) {
            LOGGER.log(Level.SEVERE, "Corrupted COUNT function in condition: {0}", normalizedCondition);
            throw new IllegalArgumentException("Corrupted COUNT function in condition: " + normalizedCondition);
        }

        return new ConditionParseContext(normalizedCondition, isNot);
    }

    private AggregateParseResult parseAggregateCondition(String normalizedCondition) {
        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)?\\s*\\)");
        Matcher aggMatcher = aggPattern.matcher(normalizedCondition);
        String conditionColumn = null;
        int aggEndIndex = -1;
        boolean isAggregate = false;

        LOGGER.log(Level.FINE, "Checking aggregate condition: {0}", normalizedCondition);
        if (aggMatcher.find()) {
            conditionColumn = aggMatcher.group(0).trim();
            aggEndIndex = aggMatcher.end();
            isAggregate = true;
            if (!conditionColumn.contains("(") || !conditionColumn.contains(")")) {
                LOGGER.log(Level.SEVERE, "Invalid aggregate function format: {0}", conditionColumn);
                throw new IllegalArgumentException("Invalid aggregate function format: " + conditionColumn);
            }
            LOGGER.log(Level.FINE, "Aggregate matched: function={0}, argument={1}, fullMatch={2}, endIndex={3}",
                    new Object[]{aggMatcher.group(1), aggMatcher.group(2), conditionColumn, aggEndIndex});
        }

        return new AggregateParseResult(isAggregate, conditionColumn, aggEndIndex);
    }

    private Condition parseAggregateCondition(AggregateParseResult aggResult, String normalizedCondition, String conjunction, Map<String, Class<?>> columnTypes, boolean isNot) {
        String remainingCondition = aggResult.aggEndIndex >= 0 ? normalizedCondition.substring(aggResult.aggEndIndex).trim() : normalizedCondition;
        LOGGER.log(Level.FINE, "Remaining condition after aggregate: {0}", remainingCondition);

        if (remainingCondition.isEmpty()) {
            LOGGER.log(Level.SEVERE, "No operator found after aggregate function: condition={0}", normalizedCondition);
            throw new IllegalArgumentException("No operator found after aggregate function: " + normalizedCondition);
        }

        String normalizedRemaining = remainingCondition.trim().replaceAll("\\s+", " ");
        LOGGER.log(Level.FINE, "Normalized remaining condition for operator parsing: {0}", normalizedRemaining);

        Pattern operatorPattern = Pattern.compile("^(\\>=|\\<=|\\>|\\<|\\=|\\!\\=\\<\\>)\\s*(.*)$");
        Matcher operatorMatcher = operatorPattern.matcher(normalizedRemaining);
        QueryParser.Operator operator = null;
        String rightOperand = null;

        if (operatorMatcher.find()) {
            String op = operatorMatcher.group(1);
            rightOperand = operatorMatcher.group(2).trim();
            LOGGER.log(Level.FINE, "Matched operator: {0}, rightOperand: {1}", new Object[]{op, rightOperand});
            switch (op) {
                case "=":
                    operator = QueryParser.Operator.EQUALS;
                    break;
                case "<":
                    operator = QueryParser.Operator.LESS_THAN;
                    break;
                case ">":
                    operator = QueryParser.Operator.GREATER_THAN;
                    break;
                case "!=":
                case "<>":
                    operator = QueryParser.Operator.NOT_EQUALS;
                    break;
                case ">=":
                    operator = QueryParser.Operator.GREATER_THAN_OR_EQUAL;
                    break;
                case "<=":
                    operator = QueryParser.Operator.LESS_THAN_OR_EQUAL;
                    break;
            }
        }

        if (operator == null || rightOperand == null || rightOperand.trim().isEmpty()) {
            LOGGER.log(Level.SEVERE, "Invalid aggregate condition format: condition={0}, remaining={1}",
                    new Object[]{normalizedCondition, normalizedRemaining});
            throw new IllegalArgumentException("Invalid aggregate condition format: " + normalizedCondition);
        }

        Class<?> valueType = aggResult.conditionColumn.toUpperCase().startsWith("COUNT") ? Long.class : Double.class;
        Object value;
        try {
            value = QueryParser.parseConditionValue(aggResult.conditionColumn, rightOperand, valueType);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse value in aggregate condition: value={0}, condition={1}, error={2}",
                    new Object[]{rightOperand, normalizedCondition, e.getMessage()});
            throw new IllegalArgumentException("Invalid value in aggregate condition: " + rightOperand, e);
        }

        LOGGER.log(Level.FINE, "Parsed aggregate condition: column={0}, operator={1}, value={2}, not={3}, conjunction={4}",
                new Object[]{aggResult.conditionColumn, operator, value, isNot, conjunction});
        return new Condition(aggResult.conditionColumn, value, operator, conjunction, isNot);
    }

    private Condition parseInCondition(String normalizedCondition, String conjunction, Map<String, Class<?>> columnTypes, String originalQuery, boolean isOnClause, String tableName, boolean isNot) {
        String originalCondition = extractOriginalCondition(originalQuery, normalizedCondition);
        LOGGER.log(Level.FINE, "Extracted original IN condition: {0}", originalCondition);

        Pattern inPattern = Pattern.compile("(?i)((?:[a-zA-Z_][a-zA-Z0-9_]*(?:\\.)?)?[a-zA-Z_][a-zA-Z0-9_]*)\\s+IN\\s*\\(([^)]+)\\)");
        Matcher inMatcher = inPattern.matcher(originalCondition);
        if (!inMatcher.find()) {
            LOGGER.log(Level.SEVERE, "Invalid IN condition format: {0}, original query: {1}",
                    new Object[]{originalCondition, originalQuery});
            throw new IllegalArgumentException("Invalid IN clause: " + originalCondition);
        }

        String parsedColumn = NormalizationUtils.normalizeColumnName(inMatcher.group(1).trim(), isOnClause ? null : tableName);
        String valuesPart = inMatcher.group(2).trim();

        if (valuesPart.isEmpty()) {
            throw new IllegalArgumentException("IN clause cannot be empty");
        }

        List<String> valueStrings = Splitter.splitInValues(valuesPart);
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

    private OperatorParseResult parseOperatorAndSplit(String normalizedCondition) {
        String upperRemaining = normalizedCondition.toUpperCase();
        String[] partsByOperator = null;
        QueryParser.Operator operator = null;

        if (upperRemaining.contains(" IS NOT NULL")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*IS NOT NULL\\s*", 2);
            operator = QueryParser.Operator.IS_NOT_NULL;
        } else if (upperRemaining.contains(" IS NULL")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*IS NULL\\s*", 2);
            operator = QueryParser.Operator.IS_NULL;
        } else if (upperRemaining.contains(" NOT LIKE ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*NOT LIKE\\s*", 2);
            operator = QueryParser.Operator.NOT_LIKE;
        } else if (upperRemaining.contains(" LIKE ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*LIKE\\s*", 2);
            operator = QueryParser.Operator.LIKE;
        } else if (upperRemaining.contains(" != ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*!=\\s*", 2);
            operator = QueryParser.Operator.NOT_EQUALS;
        } else if (upperRemaining.contains(" <> ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*<>\\s*", 2);
            operator = QueryParser.Operator.NOT_EQUALS;
        } else if (upperRemaining.contains(" = ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*=\\s*", 2);
            operator = QueryParser.Operator.EQUALS;
        } else if (upperRemaining.contains(" <= ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*<=\\s*", 2);
            operator = QueryParser.Operator.LESS_THAN_OR_EQUAL;
        } else if (upperRemaining.contains(" >= ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*>=\\s*", 2);
            operator = QueryParser.Operator.GREATER_THAN_OR_EQUAL;
        } else if (upperRemaining.contains(" < ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*<\s*", 2);
            operator = QueryParser.Operator.LESS_THAN;
        } else if (upperRemaining.contains(" > ")) {
            partsByOperator = normalizedCondition.split("(?i)\\s*>\s*", 2);
            operator = QueryParser.Operator.GREATER_THAN;
        }

        if (partsByOperator == null || partsByOperator.length != 2) {
            return null;
        }

        return new OperatorParseResult(partsByOperator, operator);
    }

    private Condition createCondition(OperatorParseResult opResult, String normalizedCondition, String conjunction, Map<String, Class<?>> columnTypes, boolean isOnClause, String tableName, boolean isNot) {
        String leftOperand = opResult.partsByOperator[0].trim();
        String rightOperand = opResult.partsByOperator[1].trim();
        QueryParser.Operator operator = opResult.operator;

        String column = NormalizationUtils.normalizeColumnName(leftOperand, isOnClause ? null : tableName);
        String rightColumn = null;
        Object value = null;

        if (operator == QueryParser.Operator.IS_NULL || operator == QueryParser.Operator.IS_NOT_NULL) {
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

        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)?\\s*\\)");
        if (aggPattern.matcher(column).matches()) {
            LOGGER.log(Level.FINE, "Column is an aggregate function: {0}", column);
            Class<?> valueType = column.toUpperCase().startsWith("COUNT") ? Long.class : Double.class;
            try {
                value = QueryParser.parseConditionValue(column, rightOperand, valueType);
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.SEVERE, "Failed to parse value for aggregate condition: value={0}, condition={1}, error={2}",
                        new Object[]{rightOperand, column, e.getMessage()});
                throw new IllegalArgumentException("Invalid value for aggregate condition: " + rightOperand, e);
            }
            return new Condition(column, value, operator, conjunction, isNot);
        }

        Class<?> columnType = getColumnType(column, columnTypes);
        if (columnType == null) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{column, columnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + column);
        }

        if (rightOperand.matches("[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*")) {
            rightColumn = NormalizationUtils.normalizeColumnName(rightOperand, isOnClause ? null : tableName);
            Class<?> rightColumnType = getColumnType(rightColumn, columnTypes);
            if (rightColumnType != null) {
                if (operator == QueryParser.Operator.LIKE || operator == QueryParser.Operator.NOT_LIKE) {
                    throw new IllegalArgumentException("LIKE and NOT LIKE are not supported for column comparisons: " + normalizedCondition);
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
            try {
                value = QueryParser.parseConditionValue(column, rightOperand, columnType);
            } catch (IllegalArgumentException e) {
                LOGGER.log(Level.SEVERE, "Failed to parse condition value: column={0}, value={1}, type={2}, error={3}",
                        new Object[]{column, rightOperand, columnType, e.getMessage()});
                throw new IllegalArgumentException("Failed to parse condition value: " + rightOperand, e);
            }
        }

        if (value instanceof QueryParser.Operator) {
            LOGGER.log(Level.SEVERE, "Invalid condition: value is an Operator: {0} in condition: {1}",
                    new Object[]{value, normalizedCondition});
            throw new IllegalArgumentException("Invalid condition: value cannot be an Operator: " + normalizedCondition);
        }

        LOGGER.log(Level.FINE, "Parsed condition: column={0}, operator={1}, value={2}, rightColumn={3}, not={4}, conjunction={5}",
                new Object[]{column, operator, value, rightColumn, isNot, conjunction});
        return new Condition(column, value, operator, conjunction, isNot);
    }

    private String extractOriginalCondition(String originalQuery, String conditionWithoutNot) {
        // Normalize both query and condition to handle case sensitivity and extra spaces
        String normalizedQuery = originalQuery.replaceAll("\\s+", " ").trim();
        String normalizedCondition = conditionWithoutNot.replaceAll("\\s+", " ").trim();
        String originalUpper = normalizedQuery.toUpperCase();
        String conditionUpper = normalizedCondition.toUpperCase();
        int startIdx = originalUpper.indexOf(conditionUpper);
        if (startIdx == -1) {
            LOGGER.log(Level.WARNING, "Condition not found in original query: {0}, returning as is", conditionWithoutNot);
            return conditionWithoutNot;
        }
        // Extract the exact substring from the original query
        return normalizedQuery.substring(startIdx, startIdx + normalizedCondition.length());
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> columnTypes) {
        if (column == null || columnTypes == null) {
            return null;
        }
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, Class<?>> entry : columnTypes.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String determineConjunctionAfter(String conditionStr, int index) {
        if (index + 1 >= conditionStr.length()) {
            return null;
        }
        String remaining = conditionStr.substring(index + 1).trim();
        if (remaining.toUpperCase().startsWith("AND ")) {
            return "AND";
        } else if (remaining.toUpperCase().startsWith("OR ")) {
            return "OR";
        }
        return null;
    }
}