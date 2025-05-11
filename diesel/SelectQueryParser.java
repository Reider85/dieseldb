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
        List<String> selectItems = splitSelectItems(selectPartOriginal);

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
            if (groupBySplit.length > 1 && !groupBySplit[1].trim().isEmpty()) {
                String groupByPart = groupBySplit[1].trim();
                LOGGER.log(Level.FINE, "GroupBy part: {0}", groupByPart);
                String[] havingSplit = groupByPart.toUpperCase().contains(" HAVING ")
                        ? groupByPart.split("(?i)\\s+HAVING\\s+", 2)
                        : new String[]{groupByPart, ""};
                groupByPart = havingSplit[0].trim();
                LOGGER.log(Level.FINE, "After HAVING split: groupByPart={0}, havingStr={1}",
                        new Object[]{groupByPart, havingSplit.length > 1 ? havingSplit[1].trim() : ""});
                if (havingSplit.length > 1 && !havingSplit[1].trim().isEmpty()) {
                    havingStr = havingSplit[1].trim();
                    LOGGER.log(Level.FINE, "HAVING clause: {0}", havingStr);
                } else {
                    LOGGER.log(Level.FINE, "No HAVING clause detected");
                }
                LOGGER.log(Level.FINE, "Parsing GROUP BY clause: {0}", groupByPart);
                groupBy = groupByParser.parseGroupByClause(groupByPart, mainTable.getName());
                LOGGER.log(Level.FINE, "Parsed GROUP BY clause: {0}", groupBy);
            }
        }

        if (remaining.toUpperCase().contains(" ORDER BY ")) {
            String[] orderBySplit = remaining.split("(?i)\\s+ORDER BY\\s+", 2);
            remaining = orderBySplit[0].trim();
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy = orderByParser.parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes);
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
            }
        }

        if (remaining.toUpperCase().contains(" OFFSET ")) {
            String[] offsetSplit = remaining.split("(?i)\\s+OFFSET\\s+", 2);
            remaining = offsetSplit[0].trim();
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                offset = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
            }
        }

        List<Condition> conditions = new ArrayList<>();
        if (conditionStr != null && !conditionStr.isEmpty()) {
            conditions = conditionParser.parseConditions(conditionStr, mainTable.getName(), database, original, false, combinedColumnTypes);
        }

        List<Condition> havingConditions = new ArrayList<>();
        if (havingStr != null && !havingStr.isEmpty()) {
            LOGGER.log(Level.FINE, "Before parsing HAVING: havingStr={0}", havingStr);
            if (groupBy.isEmpty()) {
                throw new IllegalArgumentException("HAVING clause requires a GROUP BY clause");
            }
            Integer[] limitAndOffset = new Integer[]{limit, offset};
            havingConditions = havingParser.parseHavingClause(havingStr, mainTable.getName(), database, original,
                    aggregates, groupBy, combinedColumnTypes, orderBy, limitAndOffset);
            limit = limitAndOffset[0];
            offset = limitAndOffset[1];
            LOGGER.log(Level.FINE, "Parsed HAVING clause: {0}", havingConditions);
        }

        validateSelectColumns(columns, aggregates, groupBy, combinedColumnTypes, mainTable.getName());

        LOGGER.log(Level.INFO, "Parsed SELECT query: columns={0}, aggregates={1}, mainTable={2}, joins={3}, conditions={4}, groupBy={5}, havingConditions={6}, limit={7}, offset={8}, orderBy={9}",
                new Object[]{columns, aggregates, mainTable.getName(), joins, conditions, groupBy, havingConditions, limit, offset, orderBy});

        return new SelectQuery(columns, aggregates, conditions, joins, mainTable.getName(), limit, offset, orderBy, groupBy, havingConditions);
    }

    private void validateSelectColumns(List<String> columns, List<AggregateFunction> aggregates, List<String> groupBy, Map<String, Class<?>> combinedColumnTypes, String mainTableName) {
        for (String column : columns) {
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in SELECT: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column: " + column);
            }
        }

        for (AggregateFunction agg : aggregates) {
            if (agg.column != null) {
                String normalizedColumn = normalizeColumnName(agg.column, mainTableName);
                String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
                boolean found = false;
                for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOGGER.log(Level.SEVERE, "Unknown column in {0}: {1}, available columns: {2}",
                            new Object[]{agg.toString(), agg.column, combinedColumnTypes.keySet()});
                    throw new IllegalArgumentException("Unknown column in " + agg.toString() + ": " + agg.column);
                }
            }
        }

        for (String column : groupBy) {
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            String unqualifiedColumn = normalizedColumn.contains(".") ? normalizedColumn.split("\\.")[1].trim() : normalizedColumn;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in GROUP BY: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column in GROUP BY: " + column);
            }
        }

        if (!groupBy.isEmpty()) {
            for (String column : columns) {
                String normalizedColumn = normalizeColumnName(column, mainTableName);
                if (!groupBy.contains(normalizedColumn)) {
                    boolean isAggregated = aggregates.stream().anyMatch(agg -> agg.column != null && agg.column.equals(normalizedColumn));
                    if (!isAggregated) {
                        LOGGER.log(Level.SEVERE, "Column {0} must appear in GROUP BY clause or be used in an aggregate function", column);
                        throw new IllegalArgumentException("Column " + column + " must appear in GROUP BY clause or be used in an aggregate function");
                    }
                }
            }
        }
    }

    private List<String> splitSelectItems(String selectPart) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < selectPart.length(); i++) {
            char c = selectPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentItem.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    currentItem.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentItem.append(c);
                    continue;
                } else if (c == ',' && parenDepth == 0) {
                    String item = currentItem.toString().trim();
                    if (!item.isEmpty()) {
                        items.add(item);
                    }
                    currentItem = new StringBuilder();
                    continue;
                }
            }
            currentItem.append(c);
        }

        String lastItem = currentItem.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }

        return items;
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
        LOGGER.log(Level.FINEST, "Validating condition: {0} for tables {1}, {2}", new Object[]{condition, leftTable, rightTable});
        if (condition.isGrouped()) {
            for (Condition subCond : condition.subConditions) {
                validateJoinCondition(subCond, leftTable, rightTable);
            }
            return;
        }
        if (condition.column != null) {
            String tableName = condition.column.contains(".") ? condition.column.split("\\.")[0] : null;
            if (tableName != null && !tableName.equalsIgnoreCase(leftTable) && !tableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("Invalid table in ON condition: " + tableName +
                        ", expected " + leftTable + " or " + rightTable);
            }
        }
        if (condition.rightColumn != null) {
            String tableName = condition.rightColumn.contains(".") ? condition.rightColumn.split("\\.")[0] : null;
            if (tableName != null && !tableName.equalsIgnoreCase(leftTable) && !tableName.equalsIgnoreCase(rightTable)) {
                throw new IllegalArgumentException("Invalid table in ON condition (right column): " + tableName +
                        ", expected " + leftTable + " or " + rightTable);
            }
        }
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
}