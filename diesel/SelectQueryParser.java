package diesel;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class SelectQueryParser {
    private static final Logger LOGGER = Logger.getLogger(SelectQueryParser.class.getName());
    private final ConditionParser conditionParser = new ConditionParser();
    private final OrderByParser orderByParser = new OrderByParser();
    private final GroupByParser groupByParser = new GroupByParser();
    private final HavingParser havingParser = new HavingParser(conditionParser, orderByParser);

    Query<List<Map<String, Object>>> parseSelectQuery(String normalized, String original, Database database) {
        LOGGER.log(Level.FINE, "Parsing SELECT query: normalized={0}, original={1}", new Object[]{normalized, original});

        String[] selectParts = splitSelectQuery(normalized);
        String selectPartOriginal = extractSelectPartOriginal(original);
        List<String> selectItems = Splitter.splitSelectItems(selectPartOriginal);

        ParsedSelectItems parsedItems = parseSelectItems(selectItems);
        String tableAndJoins = selectParts[1].trim();

        ParsedTableAndJoins parsedTableAndJoins = parseTableAndJoins(tableAndJoins, database);
        ParsedClauses parsedClauses = parseRemainingClauses(parsedTableAndJoins.remaining, original,
                parsedTableAndJoins.mainTable, parsedTableAndJoins.combinedColumnTypes, database);

        validateColumnsAndAggregates(parsedItems.columns, parsedItems.aggregates,
                parsedTableAndJoins.combinedColumnTypes, parsedTableAndJoins.mainTable);

        LOGGER.log(Level.FINE, "Final parsed SELECT query: table={0}, columns={1}, aggregates={2}, joins={3}, conditions={4}, groupBy={5}, having={6}, orderBy={7}, limit={8}, offset={9}",
                new Object[]{parsedTableAndJoins.mainTable.getName(), parsedItems.columns, parsedItems.aggregates,
                        parsedTableAndJoins.joins, parsedClauses.conditions, parsedClauses.groupBy,
                        parsedClauses.havingConditions, parsedClauses.orderBy, parsedClauses.limit, parsedClauses.offset});

        return new SelectQuery(parsedItems.columns, parsedItems.aggregates, parsedClauses.conditions,
                parsedTableAndJoins.joins, parsedTableAndJoins.mainTable.getName(), parsedClauses.limit,
                parsedClauses.offset, parsedClauses.orderBy, parsedClauses.groupBy, parsedClauses.havingConditions);
    }

    private String[] splitSelectQuery(String normalized) {
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid SELECT query format");
        }
        LOGGER.log(Level.FINE, "Split parts: selectPart={0}, tableAndJoins={1}", new Object[]{parts[0], parts[1]});
        return parts;
    }

    private String extractSelectPartOriginal(String original) {
        return original.substring(original.indexOf("SELECT") + 6, original.indexOf("FROM")).trim();
    }

    private ParsedSelectItems parseSelectItems(List<String> selectItems) {
        List<String> columns = new ArrayList<>();
        List<AggregateFunction> aggregates = new ArrayList<>();

        Pattern countPattern = Pattern.compile("(?i)^COUNT\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern minPattern = Pattern.compile("(?i)^MIN\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern maxPattern = Pattern.compile("(?i)^MAX\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern avgPattern = Pattern.compile("(?i)^AVG\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern sumPattern = Pattern.compile("(?i)^SUM\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$");

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher countMatcher = countPattern.matcher(trimmedItem);
            Matcher minMatcher = minPattern.matcher(trimmedItem);
            Matcher maxMatcher = maxPattern.matcher(trimmedItem);
            Matcher avgMatcher = avgPattern.matcher(trimmedItem);
            Matcher sumMatcher = sumPattern.matcher(trimmedItem);

            if (countMatcher.matches()) {
                String countArg = countMatcher.group(1);
                String alias = countMatcher.group(2);
                String column = countArg.equals("*") ? null : countArg;
                aggregates.add(new AggregateFunction("COUNT", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: COUNT({0}){1}",
                        new Object[]{column == null ? "*" : column, alias != null ? " AS " + alias : ""});
            } else if (minMatcher.matches()) {
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                aggregates.add(new AggregateFunction("MIN", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: MIN({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (maxMatcher.matches()) {
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                aggregates.add(new AggregateFunction("MAX", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: MAX({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (avgMatcher.matches()) {
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                aggregates.add(new AggregateFunction("AVG", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: AVG({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (sumMatcher.matches()) {
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                aggregates.add(new AggregateFunction("SUM", column, alias));
                LOGGER.log(Level.FINE, "Parsed aggregate function: SUM({0}){1}",
                        new Object[]{column, alias != null ? " AS " + alias : ""});
            } else {
                columns.add(trimmedItem);
            }
        }

        List<String> normalizedColumns = columns.stream()
                .map(col -> {
                    String[] colParts = col.split("\\s+AS\\s+", 2);
                    String colName = colParts[0].trim();
                    String alias = colParts.length > 1 ? colParts[1].trim() : null;
                    String normalizedCol = NormalizationUtils.normalizeColumnName(colName, null);
                    return alias != null ? normalizedCol + " AS " + alias : normalizedCol;
                })
                .collect(Collectors.toList());

        return new ParsedSelectItems(normalizedColumns, aggregates);
    }

    private ParsedTableAndJoins parseTableAndJoins(String tableAndJoins, Database database) {
        List<JoinInfo> joins = new ArrayList<>();
        Map<String, Class<?>> combinedColumnTypes = new HashMap<>();

        Pattern joinPattern = Pattern.compile("(?i)\\s*(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|LEFT INNER JOIN|RIGHT INNER JOIN|LEFT OUTER JOIN|RIGHT OUTER JOIN|FULL OUTER JOIN)\\s+");
        Matcher joinMatcher = joinPattern.matcher(tableAndJoins);
        List<String> joinParts = new ArrayList<>();
        int lastEnd = 0;
        while (joinMatcher.find()) {
            joinParts.add(tableAndJoins.substring(lastEnd, joinMatcher.start()).trim());
            joinParts.add(joinMatcher.group(1).trim());
            lastEnd = joinMatcher.end();
        }
        joinParts.add(tableAndJoins.substring(lastEnd).trim());

        String tableName = joinParts.get(0).split(" ")[0].trim();
        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        combinedColumnTypes.putAll(mainTable.getColumnTypes());

        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();
            JoinInfo joinInfo = parseJoin(joinTypeStr, joinPart, tableName, database, combinedColumnTypes);
            joins.add(joinInfo);
            tableName = joinInfo.tableName;
        }

        String remaining = joinParts.get(joinParts.size() - 1);
        return new ParsedTableAndJoins(mainTable, joins, combinedColumnTypes, remaining);
    }

    private JoinInfo parseJoin(String joinTypeStr, String joinPart, String leftTable, Database database,
                               Map<String, Class<?>> combinedColumnTypes) {
        QueryParser.JoinType joinType = determineJoinType(joinTypeStr);
        String joinTableName;
        List<Condition> onConditions = new ArrayList<>();

        if (joinType == QueryParser.JoinType.CROSS) {
            String[] crossSplit = joinPart.split("\\s+(?=(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|WHERE|LIMIT|OFFSET|ORDER BY|$))", 2);
            joinTableName = crossSplit[0].trim();
            if (crossSplit.length > 1 && crossSplit[1].contains(" ON ")) {
                throw new IllegalArgumentException("CROSS JOIN does not support ON clause: " + joinPart);
            }
        } else {
            // Split on 'ON' and stop before WHERE, LIMIT, etc.
            Pattern onPattern = Pattern.compile("(?i)ON\\s+((?:(?!\\s+(WHERE|LIMIT|OFFSET|ORDER BY|GROUP BY|HAVING|JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN)).)*)");
            Matcher onMatcher = onPattern.matcher(joinPart);
            if (!onMatcher.find()) {
                throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: missing or malformed ON clause");
            }
            String onCondition = onMatcher.group(1).trim();
            joinTableName = joinPart.substring(0, onMatcher.start()).trim().split(" ")[0].trim();
            if (joinTableName.isEmpty()) {
                throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: missing table name");
            }
            onConditions = conditionParser.parseConditions(onCondition, leftTable, database, joinPart, true, combinedColumnTypes);
            for (Condition cond : onConditions) {
                validateJoinCondition(cond, leftTable, joinTableName);
            }
            LOGGER.log(Level.FINE, "Parsed ON conditions for {0}: {1}", new Object[]{joinTypeStr, onConditions});
        }

        Table joinTable = database.getTable(joinTableName);
        if (joinTable == null) {
            throw new IllegalArgumentException("Join table not found: " + joinTableName);
        }
        combinedColumnTypes.putAll(joinTable.getColumnTypes());
        LOGGER.log(Level.FINE, "Parsed {0}: table={1}", new Object[]{joinTypeStr, joinTableName});

        return new JoinInfo(leftTable, joinTableName, null, null, joinType, onConditions);
    }

    private QueryParser.JoinType determineJoinType(String joinTypeStr) {
        switch (joinTypeStr) {
            case "JOIN":
            case "INNER JOIN":
                return QueryParser.JoinType.INNER;
            case "LEFT JOIN":
            case "LEFT OUTER JOIN":
                return QueryParser.JoinType.LEFT_OUTER;
            case "RIGHT JOIN":
            case "RIGHT OUTER JOIN":
                return QueryParser.JoinType.RIGHT_OUTER;
            case "FULL JOIN":
            case "FULL OUTER JOIN":
                return QueryParser.JoinType.FULL_OUTER;
            case "LEFT INNER JOIN":
                return QueryParser.JoinType.LEFT_INNER;
            case "RIGHT INNER JOIN":
                return QueryParser.JoinType.RIGHT_INNER;
            case "CROSS JOIN":
                return QueryParser.JoinType.CROSS;
            default:
                throw new IllegalArgumentException("Unsupported join type: " + joinTypeStr);
        }
    }

    private ParsedClauses parseRemainingClauses(String remaining, String original, Table mainTable,
                                                Map<String, Class<?>> combinedColumnTypes, Database database) {
        String conditionStr = null;
        String havingStr = null;
        Integer limit = null;
        Integer offset = null;
        List<OrderByInfo> orderBy = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();

        if (remaining.toUpperCase().contains(" WHERE ")) {
            String[] whereSplit = remaining.split("(?i)\\s+WHERE\\s+", 2);
            conditionStr = whereSplit[1].trim();
            remaining = whereSplit[0].trim();
        }

        if (conditionStr != null && !conditionStr.isEmpty()) {
            ParsedConditionClauses conditionClauses = parseConditionClauses(conditionStr, combinedColumnTypes);
            conditionStr = conditionClauses.conditionStr;
            limit = conditionClauses.limit != null ? conditionClauses.limit : limit;
            offset = conditionClauses.offset != null ? conditionClauses.offset : offset;
            orderBy = conditionClauses.orderBy.isEmpty() ? orderBy : conditionClauses.orderBy;
            groupBy = conditionClauses.groupBy.isEmpty() ? groupBy : conditionClauses.groupBy;
            havingStr = conditionClauses.havingStr != null ? conditionClauses.havingStr : havingStr;
        }

        if (remaining.toUpperCase().contains(" GROUP BY ")) {
            String originalRemaining = original.substring(original.toUpperCase().indexOf("FROM") + 4).trim();
            String[] groupBySplit = originalRemaining.split("(?i)\\s+GROUP BY\\s+", 2);
            remaining = groupBySplit[0].trim();
            if (groupBySplit.length > 1) {
                String groupByPart = groupBySplit[1].trim();
                String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                        ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                        : new String[]{groupByPart, ""};
                groupByPart = havingSplit[0].trim();
                if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                    havingStr = havingSplit[1].trim();
                }
                groupBy = groupByParser.parseGroupByClause(groupByPart, mainTable.getName());
                LOGGER.log(Level.FINE, "Parsed GROUP BY: {0}", groupBy);
            }
        }

        if (remaining.toUpperCase().contains(" ORDER BY ")) {
            String[] orderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
            remaining = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = orderByParser.parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
                LOGGER.log(Level.FINE, "Parsed ORDER BY: {0}", orderBy);
            }
        }

        if (remaining.toUpperCase().contains(" LIMIT ")) {
            String[] limitSplit = remaining.split("(?i)\\s+LIMIT\\s+", 2);
            remaining = limitSplit[0].trim();
            if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
                String limitClause = limitSplit[1].trim();
                String[] limitOffsetSplit = limitClause.toUpperCase().contains(" OFFSET ")
                        ? limitClause.split("(?i)\\s+OFFSET\\s+", 2)
                        : new String[]{limitClause, ""};
                limit = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
                if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                    offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                }
                LOGGER.log(Level.FINE, "Parsed LIMIT: {0}, OFFSET: {1}", new Object[]{limit, offset});
            }
        }

        if (remaining.toUpperCase().contains(" OFFSET ")) {
            String[] offsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
            remaining = offsetSplit[0].trim();
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
                LOGGER.log(Level.FINE, "Parsed OFFSET: {0}", offset);
            }
        }

        List<Condition> conditions = conditionStr != null && !conditionStr.trim().isEmpty()
                ? conditionParser.parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes)
                : new ArrayList<>();
        LOGGER.log(Level.FINE, "Parsed WHERE conditions: {0}", conditions);

        List<Condition> havingConditions = havingStr != null && !havingStr.trim().isEmpty()
                ? havingParser.parseHavingClause(havingStr, mainTable.getName(), database, original,
                parseSelectItems(Splitter.splitSelectItems(extractSelectPartOriginal(original))).aggregates,
                groupBy, combinedColumnTypes, orderBy, new Integer[]{limit, offset})
                : new ArrayList<>();
        LOGGER.log(Level.FINE, "Parsed HAVING conditions: {0}", havingConditions);

        return new ParsedClauses(conditions, havingConditions, orderBy, groupBy, limit, offset);
    }

    private ParsedConditionClauses parseConditionClauses(String conditionStr, Map<String, Class<?>> combinedColumnTypes) {
        String parsedConditionStr = conditionStr;
        Integer limit = null;
        Integer offset = null;
        List<OrderByInfo> orderBy = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        String havingStr = null;

        String[] limitSplit = conditionStr.toUpperCase().contains(" LIMIT ")
                ? conditionStr.split("(?i)\\s+LIMIT\\s+", 2)
                : new String[]{conditionStr, ""};
        parsedConditionStr = limitSplit[0].trim();
        if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
            String limitClause = limitSplit[1].trim();
            String[] limitOffsetSplit = limitClause.toUpperCase().contains(" OFFSET ")
                    ? limitClause.split("(?i)\\s+OFFSET\\s+", 2)
                    : new String[]{limitClause, ""};
            limit = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
            if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
            }
        }

        String[] offsetSplit = parsedConditionStr.toUpperCase().contains(" OFFSET ")
                ? parsedConditionStr.split("(?i)\\s+OFFSET\\s+", 2)
                : new String[]{parsedConditionStr, ""};
        parsedConditionStr = offsetSplit[0].trim();
        if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
            offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
        }

        String[] orderBySplit = parsedConditionStr.toUpperCase().contains(" ORDER BY ")
                ? parsedConditionStr.split("(?i)\\s+ORDER BY\\s+", 2)
                : new String[]{parsedConditionStr, ""};
        parsedConditionStr = orderBySplit[0].trim();
        if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
            orderBy = orderByParser.parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
        }

        String[] groupBySplit = parsedConditionStr.toUpperCase().contains(" GROUP BY ")
                ? parsedConditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                : new String[]{parsedConditionStr, ""};
        parsedConditionStr = groupBySplit[0].trim();
        if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
            String groupByPart = groupBySplit[1].trim();
            String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                    ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                    : new String[]{groupByPart, ""};
            groupByPart = havingSplit[0].trim();
            if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                havingStr = havingSplit[1].trim();
            }
            groupBy = groupByParser.parseGroupByClause(groupByPart, null);
        }

        return new ParsedConditionClauses(parsedConditionStr, limit, offset, orderBy, groupBy, havingStr);
    }

    private void validateColumnsAndAggregates(List<String> columns, List<AggregateFunction> aggregates,
                                              Map<String, Class<?>> combinedColumnTypes, Table mainTable) {
        for (String col : columns) {
            String colName = col.contains(" AS ") ? col.split("\\s+AS\\s+")[0].trim() : col;
            if (!combinedColumnTypes.containsKey(colName.contains(".") ? colName.split("\\.")[1] : colName)) {
                throw new IllegalArgumentException("Unknown column: " + colName);
            }
        }

        for (AggregateFunction agg : aggregates) {
            if (agg.column != null) {
                String normalizedAggCol = NormalizationUtils.normalizeColumnName(agg.column, mainTable.getName());
                String unqualifiedCol = normalizedAggCol.contains(".") ? normalizedAggCol.split("\\.")[1] : normalizedAggCol;
                if (!combinedColumnTypes.containsKey(unqualifiedCol)) {
                    throw new IllegalArgumentException("Unknown column in aggregate function: " + agg.column);
                }
            }
        }
    }

    private Integer parseLimitClause(String limitClause) {
        String normalized = limitClause.toUpperCase().replace("LIMIT", "").trim();
        if (normalized.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty LIMIT clause detected");
            return null;
        }
        try {
            int limitValue = Integer.parseInt(normalized);
            if (limitValue < 0) {
                throw new IllegalArgumentException("LIMIT value must be non-negative: " + limitValue);
            }
            LOGGER.log(Level.FINE, "Parsed LIMIT clause: {0}", limitValue);
            return limitValue;
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid LIMIT value: {0}", normalized);
            throw new IllegalArgumentException("Invalid LIMIT value: " + normalized);
        }
    }

    private Integer parseOffsetClause(String offsetClause) {
        String normalized = offsetClause.toUpperCase().replace("OFFSET", "").trim();
        if (normalized.isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty OFFSET clause detected");
            return null;
        }
        try {
            int offsetValue = Integer.parseInt(normalized);
            if (offsetValue < 0) {
                throw new IllegalArgumentException("OFFSET value must be non-negative: " + offsetValue);
            }
            LOGGER.log(Level.FINE, "Parsed OFFSET clause: {0}", offsetValue);
            return offsetValue;
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Invalid OFFSET value: {0}", normalized);
            throw new IllegalArgumentException("Invalid OFFSET value: " + normalized);
        }
    }

    private void validateJoinCondition(Condition condition, String leftTable, String rightTable) {
        if (condition.isGrouped()) {
            for (Condition subCond : condition.subConditions) {
                validateJoinCondition(subCond, leftTable, rightTable);
            }
            return;
        }

        String leftCol = condition.column;
        if (leftCol == null) {
            throw new IllegalArgumentException("Invalid JOIN condition: missing column: " + condition);
        }

        String leftTableName = leftCol.contains(".") ? leftCol.split("\\.")[0] : "";
        if (!leftTableName.equalsIgnoreCase(leftTable) && !leftTableName.equalsIgnoreCase(rightTable)) {
            throw new IllegalArgumentException("JOIN condition must reference the joining tables: " + condition);
        }

        if (condition.operator == QueryParser.Operator.LIKE ||
                condition.operator == QueryParser.Operator.NOT_LIKE ||
                condition.operator == QueryParser.Operator.IS_NULL ||
                condition.operator == QueryParser.Operator.IS_NOT_NULL ||
                condition.operator == QueryParser.Operator.IN) {
            LOGGER.log(Level.FINE, "Validated JOIN condition with operator {0}: {1}", new Object[]{condition.operator, condition});
            return;
        }

        String rightCol = condition.rightColumn;
        if (rightCol != null) {
            String rightTableName = rightCol.contains(".") ? rightCol.split("\\.")[0] : "";
            if (!rightTableName.equalsIgnoreCase(leftTable) && !rightTableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("JOIN condition must reference the joining tables: " + condition);
            }
            if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
                throw new IllegalArgumentException("LIKE and NOT LIKE are not supported for column-to-column comparisons: " + condition);
            }
        }

        if (condition.operator == QueryParser.Operator.EQUALS ||
                condition.operator == QueryParser.Operator.NOT_EQUALS ||
                condition.operator == QueryParser.Operator.LESS_THAN ||
                condition.operator == QueryParser.Operator.GREATER_THAN ||
                condition.operator == QueryParser.Operator.LESS_THAN_OR_EQUAL ||
                condition.operator == QueryParser.Operator.GREATER_THAN_OR_EQUAL) {
            LOGGER.log(Level.FINE, "Validated JOIN condition: {0} between tables {1} and {2}",
                    new Object[]{condition, leftTable, rightTable});
            return;
        }

        throw new IllegalArgumentException("Unsupported operator in JOIN condition: " + condition);
    }
}
