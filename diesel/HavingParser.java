package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class HavingParser {
    private static final Logger LOGGER = Logger.getLogger(HavingParser.class.getName());
    private final ConditionParser conditionParser;
    private final OrderByParser orderByParser;

    public HavingParser(ConditionParser conditionParser, OrderByParser orderByParser) {
        this.conditionParser = conditionParser;
        this.orderByParser = orderByParser;
    }

    List<Condition> parseHavingClause(String havingStr, String tableName, Database database, String originalQuery,
                                      List<AggregateFunction> aggregates, List<String> groupBy,
                                      Map<String, Class<?>> combinedColumnTypes, List<OrderByInfo> orderBy,
                                      Integer[] limitAndOffset) {
        LOGGER.log(Level.FINE, "Received HAVING clause: {0}", havingStr);
        if (havingStr == null || havingStr.trim().isEmpty()) {
            LOGGER.log(Level.WARNING, "Empty or null HAVING clause received");
            return new ArrayList<>();
        }

        String cleanedHavingStr = cleanHavingClause(havingStr, combinedColumnTypes, orderBy, limitAndOffset);
        LOGGER.log(Level.FINE, "Cleaned HAVING clause: {0}", cleanedHavingStr);

        if (cleanedHavingStr.isEmpty()) {
            LOGGER.log(Level.WARNING, "HAVING clause is empty after cleanup");
            return new ArrayList<>();
        }

        LOGGER.log(Level.FINE, "Passing HAVING clause to parseConditions: {0}", cleanedHavingStr);
        List<Condition> havingConditions = conditionParser.parseConditions(cleanedHavingStr, tableName, database,
                originalQuery, false, combinedColumnTypes);

        for (Condition condition : havingConditions) {
            validateHavingCondition(condition, aggregates, groupBy, combinedColumnTypes);
        }

        LOGGER.log(Level.FINE, "Parsed HAVING conditions: {0}", havingConditions);
        return havingConditions;
    }

    private String cleanHavingClause(String havingStr, Map<String, Class<?>> combinedColumnTypes,
                                     List<OrderByInfo> orderBy, Integer[] limitAndOffset) {
        String originalHavingStr = havingStr;
        String cleanedHavingStr = havingStr;

        if (havingStr.toUpperCase().contains(" ORDER BY ")) {
            String[] orderBySplit = havingStr.split("(?i)\\s+ORDER BY\\s+", 2);
            cleanedHavingStr = orderBySplit[0].trim();
            LOGGER.log(Level.FINE, "After ORDER BY split: {0}, original: {1}", new Object[]{cleanedHavingStr, originalHavingStr});
            if (orderBySplit.length > 1 && !orderBySplit[1].trim().isEmpty()) {
                orderBy.addAll(orderByParser.parseOrderByClause(orderBySplit[1].trim(), combinedColumnTypes));
                LOGGER.log(Level.FINE, "Parsed ORDER BY from HAVING: {0}", orderBy);
            }
        } else {
            LOGGER.log(Level.FINE, "No ORDER BY clause detected in HAVING");
        }

        if (cleanedHavingStr.toUpperCase().contains(" LIMIT ")) {
            String[] limitSplit = cleanedHavingStr.split("(?i)\\s+LIMIT\\s+", 2);
            cleanedHavingStr = limitSplit[0].trim();
            LOGGER.log(Level.FINE, "After LIMIT split: {0}, original: {1}", new Object[]{cleanedHavingStr, originalHavingStr});
            if (limitSplit.length > 1 && !limitSplit[1].trim().isEmpty()) {
                String limitClause = limitSplit[1].trim();
                String[] limitOffsetSplit = limitClause.toUpperCase().contains(" OFFSET ")
                        ? limitClause.split("(?i)\\s+OFFSET\\s+", 2)
                        : new String[]{limitClause, ""};
                limitAndOffset[0] = parseLimitClause("LIMIT " + limitOffsetSplit[0].trim());
                if (limitOffsetSplit.length > 1 && !limitOffsetSplit[1].trim().isEmpty()) {
                    limitAndOffset[1] = parseOffsetClause("OFFSET " + limitOffsetSplit[1].trim());
                }
            }
        } else {
            LOGGER.log(Level.FINE, "No LIMIT clause detected in HAVING");
        }

        if (cleanedHavingStr.toUpperCase().contains(" OFFSET ")) {
            String[] offsetSplit = cleanedHavingStr.split("(?i)\\s+OFFSET\\s+", 2);
            cleanedHavingStr = offsetSplit[0].trim();
            LOGGER.log(Level.FINE, "After OFFSET split: {0}, original: {1}", new Object[]{cleanedHavingStr, originalHavingStr});
            if (offsetSplit.length > 1 && !offsetSplit[1].trim().isEmpty()) {
                limitAndOffset[1] = parseOffsetClause("OFFSET " + offsetSplit[1].trim());
            }
        } else {
            LOGGER.log(Level.FINE, "No OFFSET clause detected in HAVING");
        }

        if (!cleanedHavingStr.equals(originalHavingStr)) {
            LOGGER.log(Level.WARNING, "HAVING clause modified: original={0}, modified={1}",
                    new Object[]{originalHavingStr, cleanedHavingStr});
        }

        return cleanedHavingStr;
    }

    private void validateHavingCondition(Condition condition, List<AggregateFunction> aggregates, List<String> groupBy,
                                         Map<String, Class<?>> combinedColumnTypes) {
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