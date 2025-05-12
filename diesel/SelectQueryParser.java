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
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid SELECT query format");
        }
        LOGGER.log(Level.FINE, "Split parts: selectPart={0}, tableAndJoins={1}", new Object[]{parts[0], parts[1]});

        String selectPartOriginal = original.substring(original.indexOf("SELECT") + 6, original.indexOf("FROM")).trim();
        List<String> selectItems = Splitter.splitSelectItems(selectPartOriginal);

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

        String tableAndJoins = parts[1].trim();
        List<JoinInfo> joins = new ArrayList<>();
        String tableName;
        String conditionStr = null;
        String havingStr = null;
        Integer limit = null;
        Integer offset = null;
        List<OrderByInfo> orderBy = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();

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

        tableName = joinParts.get(0).split(" ")[0].trim();
        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());

        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();

            QueryParser.JoinType joinType;
            switch (joinTypeStr) {
                case "JOIN":
                case "INNER JOIN":
                    joinType = QueryParser.JoinType.INNER;
                    break;
                case "LEFT JOIN":
                case "LEFT OUTER JOIN":
                    joinType = QueryParser.JoinType.LEFT_OUTER;
                    break;
                case "RIGHT JOIN":
                case "RIGHT OUTER JOIN":
                    joinType = QueryParser.JoinType.RIGHT_OUTER;
                    break;
                case "FULL JOIN":
                case "FULL OUTER JOIN":
                    joinType = QueryParser.JoinType.FULL_OUTER;
                    break;
                case "LEFT INNER JOIN":
                    joinType = QueryParser.JoinType.LEFT_INNER;
                    break;
                case "RIGHT INNER JOIN":
                    joinType = QueryParser.JoinType.RIGHT_INNER;
                    break;
                case "CROSS JOIN":
                    joinType = QueryParser.JoinType.CROSS;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported join type: " + joinTypeStr);
            }

            String joinTableName;
            List<Condition> onConditions = new ArrayList<>();

            if (joinType == QueryParser.JoinType.CROSS) {
                String[] crossSplit = joinPart.split("\\s+(?=(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|WHERE|LIMIT|OFFSET|ORDER BY|$))", 2);
                joinTableName = crossSplit[0].trim();
                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());
                if (crossSplit.length > 1 && !crossSplit[1].trim().isEmpty()) {
                    String remaining = crossSplit[1].trim();
                    if (remaining.toUpperCase().startsWith("WHERE ")) {
                        conditionStr = remaining.substring(6).trim();
                    } else if (remaining.toUpperCase().startsWith("LIMIT ")) {
                        String[] limitSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                        limit = parseLimitClause("LIMIT " + limitSplit[0].substring(6).trim());
                        if (limitSplit.length > 1) {
                            offset = parseOffsetClause("OFFSET " + limitSplit[1].trim());
                        }
                    } else if (remaining.toUpperCase().startsWith("OFFSET ")) {
                        offset = parseOffsetClause(remaining);
                    } else if (remaining.toUpperCase().startsWith("ORDER BY ")) {
                        orderBy = orderByParser.parseOrderByClause(remaining.substring(9).trim(), combinedColumnTypes);
                    }
                    if (remaining.toUpperCase().contains(" ON ")) {
                        throw new IllegalArgumentException("CROSS JOIN does not support ON clause: " + joinPart);
                    }
                }
                LOGGER.log(Level.FINE, "Parsed CROSS JOIN: table={0}", joinTableName);
            } else {
                String[] onSplit = joinPart.split("(?i)ON\\s+", 2);
                if (onSplit.length != 2) {
                    throw new IllegalArgumentException("Invalid " + joinTypeStr + " format: missing ON clause");
                }

                joinTableName = onSplit[0].split(" ")[0].trim();
                String onClause = onSplit[1].trim();

                String onCondition;
                if (onClause.toUpperCase().contains(" WHERE ")) {
                    String[] onWhereSplit = onClause.split("(?i)\\s+WHERE\\s+", 2);
                    onCondition = onWhereSplit[0].trim();
                    if (onWhereSplit.length > 1) {
                        String remaining = onWhereSplit[1].trim();
                        if (remaining.toUpperCase().startsWith("LIMIT ")) {
                            String[] whereLimitSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                            conditionStr = whereLimitSplit[0].trim();
                            if (whereLimitSplit.length > 1) {
                                limit = parseLimitClause("LIMIT " + whereLimitSplit[0].substring(6).trim());
                                offset = parseOffsetClause("OFFSET " + whereLimitSplit[1].trim());
                            } else {
                                limit = parseLimitClause("LIMIT " + whereLimitSplit[0].trim());
                            }
                        } else if (remaining.toUpperCase().startsWith("OFFSET ")) {
                            String[] whereOffsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
                            conditionStr = whereOffsetSplit[0].trim();
                            if (whereOffsetSplit.length > 1) {
                                offset = parseOffsetClause("OFFSET " + whereOffsetSplit[1].trim());
                            }
                        } else if (remaining.toUpperCase().startsWith("ORDER BY ")) {
                            String[] whereOrderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
                            conditionStr = whereOrderBySplit[0].trim();
                            if (whereOrderBySplit.length > 1) {
                                orderBy = orderByParser.parseOrderByClause(whereOrderBySplit[1].trim(), combinedColumnTypes);
                            }
                        } else {
                            conditionStr = remaining;
                        }
                    }
                } else if (onClause.toUpperCase().startsWith("LIMIT ")) {
                    String[] onLimitSplit = onClause.split("(?i)\\s+OFFSET\\s+", 2);
                    onCondition = "";
                    if (onLimitSplit.length > 1) {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].substring(6).trim());
                        offset = parseOffsetClause("OFFSET " + onLimitSplit[1].trim());
                    } else {
                        limit = parseLimitClause("LIMIT " + onLimitSplit[0].trim());
                    }
                } else if (onClause.toUpperCase().startsWith("OFFSET ")) {
                    onCondition = "";
                    offset = parseOffsetClause(onClause);
                } else if (onClause.toUpperCase().startsWith("ORDER BY ")) {
                    String[] onOrderBySplit = onClause.split("(?i)\\s+ORDER BY\\s+", 2);
                    onCondition = "";
                    if (onOrderBySplit.length > 1) {
                        orderBy = orderByParser.parseOrderByClause(onOrderBySplit[1].trim(), combinedColumnTypes);
                    }
                } else {
                    onCondition = onClause;
                }

                Table joinTable = database.getTable(joinTableName);
                if (joinTable == null) {
                    throw new IllegalArgumentException("Join table not found: " + joinTableName);
                }
                combinedColumnTypes.putAll(joinTable.getColumnTypes());

                onConditions = conditionParser.parseConditions(onCondition, tableName, database, original, true, combinedColumnTypes);

                for (Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName);
                }

                LOGGER.log(Level.FINE, "Parsed ON conditions for {0}: {1}", new Object[]{joinTypeStr, onConditions});
            }

            joins.add(new JoinInfo(tableName, joinTableName, null, null, joinType, onConditions));
            tableName = joinTableName;
        }

        String remaining = joinParts.get(joinParts.size() - 1);
        if (remaining.toUpperCase().contains(" WHERE ")) {
            String[] whereSplit = remaining.split("(?i)\\s+WHERE\\s+", 2);
            conditionStr = whereSplit[1].trim();
            remaining = whereSplit[0].trim();
        }

        if (conditionStr != null && !conditionStr.isEmpty()) {
            String[] limitSplit = conditionStr.toUpperCase().contains(" LIMIT ")
                    ? conditionStr.split("(?i)\\s+LIMIT\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = limitSplit[0].trim();
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

            String[] offsetSplit = conditionStr.toUpperCase().contains(" OFFSET ")
                    ? conditionStr.split("(?i)\\s+OFFSET\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = offsetSplit[0].trim();
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
            }

            String[] orderBySplit = conditionStr.toUpperCase().contains(" ORDER BY ")
                    ? conditionStr.split("(?i)\\s+ORDER BY\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = orderByParser.parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
            }

            String[] groupBySplit = conditionStr.toUpperCase().contains(" GROUP BY ")
                    ? conditionStr.split("(?i)\\s+GROUP BY\\s+", 2)
                    : new String[]{conditionStr, ""};
            conditionStr = groupBySplit[0].trim();
            if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
                String groupByPart = groupBySplit[1].trim();
                String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                        ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                        : new String[]{groupByPart, ""};
                groupByPart = havingSplit[0].trim();
                if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                    havingStr = havingSplit[1].trim();
                }
                groupBy = groupByParser.parseGroupByClause(groupByPart, mainTable.getName());
            }
        }

        if (remaining.toUpperCase().contains(" GROUP BY ")) {
            String originalRemaining = original.substring(original.toUpperCase().indexOf("FROM") + 4).trim();
            String[] groupBySplit = originalRemaining.split("(?i)\\s+GROUP BY\\s+", 2);
            remaining = groupBySplit[0].trim();
            LOGGER.log(Level.FINE, "After GROUP BY split: remaining={0}, groupByPart={1}",
                    new Object[]{remaining, groupBySplit.length > 1 ? groupBySplit[1].trim() : ""});
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

        List<Condition> conditions = new ArrayList<>();
        if (conditionStr != null && !conditionStr.trim().isEmpty()) {
            conditions = conditionParser.parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes);
            LOGGER.log(Level.FINE, "Parsed WHERE conditions: {0}", conditions);
        }

        List<Condition> havingConditions = new ArrayList<>();
        if (havingStr != null && !havingStr.trim().isEmpty()) {
            Integer[] limitAndOffset = new Integer[]{limit, offset};
            havingConditions = havingParser.parseHavingClause(havingStr, mainTable.getName(), database, original,
                    aggregates, groupBy, combinedColumnTypes, orderBy, limitAndOffset);
            limit = limitAndOffset[0];
            offset = limitAndOffset[1];
            LOGGER.log(Level.FINE, "Parsed HAVING conditions: {0}", havingConditions);
        }

        List<String> normalizedColumns = columns.stream()
                .map(col -> {
                    String[] colParts = col.split("\\s+AS\\s+", 2);
                    String colName = colParts[0].trim();
                    String alias = colParts.length > 1 ? colParts[1].trim() : null;
                    String normalizedCol = NormalizationUtils.normalizeColumnName(colName, mainTable.getName());
                    return alias != null ? normalizedCol + " AS " + alias : normalizedCol;
                })
                .collect(Collectors.toList());

        for (String col : normalizedColumns) {
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

        LOGGER.log(Level.FINE, "Final parsed SELECT query: table={0}, columns={1}, aggregates={2}, joins={3}, conditions={4}, groupBy={5}, having={6}, orderBy={7}, limit={8}, offset={9}",
                new Object[]{mainTable.getName(), normalizedColumns, aggregates, joins, conditions, groupBy, havingConditions, orderBy, limit, offset});

        return new SelectQuery(normalizedColumns, aggregates, conditions, joins, mainTable.getName(), limit, offset, orderBy, groupBy, havingConditions);
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

        // Check that the left column references one of the joining tables
        String leftTableName = leftCol.contains(".") ? leftCol.split("\\.")[0] : "";
        if (!leftTableName.equalsIgnoreCase(leftTable) && !leftTableName.equalsIgnoreCase(rightTable)) {
            throw new IllegalArgumentException("JOIN condition must reference the joining tables: " + condition);
        }

        // Allow LIKE, NOT LIKE, IS NULL, IS NOT NULL, and IN operators
        if (condition.operator == QueryParser.Operator.LIKE ||
                condition.operator == QueryParser.Operator.NOT_LIKE ||
                condition.operator == QueryParser.Operator.IS_NULL ||
                condition.operator == QueryParser.Operator.IS_NOT_NULL ||
                condition.operator == QueryParser.Operator.IN) {
            LOGGER.log(Level.FINE, "Validated JOIN condition with operator {0}: {1}", new Object[]{condition.operator, condition});
            return;
        }

        // For comparison operators, allow both column-to-column and column-to-literal comparisons
        String rightCol = condition.rightColumn;
        if (rightCol != null) {
            // Column-to-column comparison
            String rightTableName = rightCol.contains(".") ? rightCol.split("\\.")[0] : "";
            if (!rightTableName.equalsIgnoreCase(leftTable) && !rightTableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("JOIN condition must reference the joining tables: " + condition);
            }
            if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
                throw new IllegalArgumentException("LIKE and NOT LIKE are not supported for column-to-column comparisons: " + condition);
            }
        }

        // Allow all comparison operators for column-to-literal comparisons
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