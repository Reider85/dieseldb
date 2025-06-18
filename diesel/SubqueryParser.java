package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubqueryParser {
    private static final Logger LOGGER = Logger.getLogger(SubqueryParser.class.getName());
    private final QueryParser queryParser;

    public SubqueryParser() {
        this.queryParser = new QueryParser();
    }

    // Модифицируем метод parse для обработки всех подзапросов
    public Query<?> parse(String query, Database database) {
        LOGGER.log(Level.INFO, "Parsing query: {0}", query);
        String normalizedQuery = normalizeQueryString(query).trim();

        if (!containsSubquery(normalizedQuery)) {
            LOGGER.log(Level.FINE, "No subqueries found, delegating to QueryParser: {0}", query);
            return queryParser.parse(query, database);
        }

        LOGGER.log(Level.FINE, "Subqueries detected, parsing with SubqueryParser: {0}", query);
        try {
            if (normalizedQuery.toUpperCase().startsWith("SELECT")) {
                // Проверяем, является ли весь запрос подзапросом в IN
                Pattern inSubqueryPattern = Pattern.compile(
                        "(?i)^SELECT\\s+.*?\\s+WHERE\\s+.*?\\s+IN\\s*\\((SELECT(?:[^()']+|'(?:\\\\.|[^'\\\\])*'|\\([^()]*\\))*?)\\)(?:\\s+LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?)?$",
                        Pattern.DOTALL);
                Matcher inMatcher = inSubqueryPattern.matcher(normalizedQuery);
                if (inMatcher.matches()) {
                    LOGGER.log(Level.FINE, "Detected IN-subquery pattern, using custom parsing");
                    return parseSelectQuery(query, normalizedQuery, database);
                }
                return parseSelectQuery(query, normalizedQuery, database);
            } else {
                LOGGER.log(Level.FINE, "Non-SELECT query, delegating to QueryParser: {0}", query);
                return queryParser.parse(query, database);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    public boolean containsSubquery(String query) {
        Pattern subqueryPattern = Pattern.compile("(?i)\\(\\s*SELECT\\b", Pattern.DOTALL);
        Matcher matcher = subqueryPattern.matcher(query);
        return matcher.find();
    }

    private String normalizeQueryString(String query) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        return query.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*([=><!(),])\\s*", "$1")
                .replaceAll("(?i)(\\))\\s*(LIMIT|WHERE|ORDER\\s+BY|GROUP\\s+BY|HAVING|INNER\\s+JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|FULL\\s+JOIN|CROSS\\s+JOIN)", "$1 $2")
                .replaceAll("\\s*;", "");
    }

    private Query<List<Map<String, Object>>> parseSelectQuery(String originalQuery, String normalizedQuery, Database database) {
        int fromIndex = findMainFromClause(originalQuery);
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid SELECT query: missing FROM clause");
        }

        String selectPartOriginal = originalQuery.substring(originalQuery.toUpperCase().indexOf("SELECT") + 6, fromIndex).trim();
        String tableAndJoinsOriginal = originalQuery.substring(fromIndex + 4).trim();

        QueryParser.SelectItems selectItems = parseSelectItems(selectPartOriginal, database);
        List<String> columns = selectItems.columns;
        List<QueryParser.AggregateFunction> aggregates = selectItems.aggregates;
        List<QueryParser.SubQuery> subQueries = selectItems.subQueries;
        Map<String, String> columnAliases = selectItems.columnAliases;

        QueryParser.TableJoins tableJoins = parseTableAndJoins(tableAndJoinsOriginal, database);
        String tableName = tableJoins.tableName;
        String tableAlias = tableJoins.tableAlias;
        List<QueryParser.JoinInfo> joins = tableJoins.joins;
        Map<String, String> tableAliases = tableJoins.tableAliases;
        Map<String, Class<?>> combinedColumnTypes = tableJoins.combinedColumnTypes;

        QueryParser.AdditionalClauses clauses = parseAdditionalClauses(tableAndJoinsOriginal, tableName, database, originalQuery,
                aggregates, combinedColumnTypes, tableAliases, columnAliases, subQueries);
        List<QueryParser.Condition> conditions = clauses.conditions;
        List<String> groupBy = clauses.groupBy;
        List<QueryParser.HavingCondition> havingConditions = clauses.havingConditions;
        List<QueryParser.OrderByInfo> orderBy = clauses.orderBy;
        Integer limit = clauses.limit;
        Integer offset = clauses.offset;

        LOGGER.log(Level.INFO, "Parsed SELECT query: table={0}, columns={1}, aggregates={2}, joins={3}, conditions={4}",
                new Object[]{tableName, columns, aggregates, joins, conditions});

        return new SelectQuery(tableName, tableAlias, columns, aggregates, joins, conditions,
                groupBy, havingConditions, orderBy, limit, offset, subQueries, columnAliases, tableAliases, combinedColumnTypes);
    }

    private int findMainFromClause(String query) {
        Pattern quotedStringPattern = Pattern.compile("'(?:\\\\.|[^'\\\\])*'");
        Pattern subqueryPattern = Pattern.compile("\\(\\s*SELECT\\b", Pattern.DOTALL);
        Pattern fromPattern = Pattern.compile("(?i)\\bFROM\\b");
        int bracketDepth = 0;
        int currentPos = 0;
        boolean inQuotes = false;

        while (currentPos < query.length()) {
            Matcher quotedStringMatcher = quotedStringPattern.matcher(query).region(currentPos, query.length());
            if (quotedStringMatcher.lookingAt()) {
                currentPos = quotedStringMatcher.end();
                continue;
            }

            char c = query.charAt(currentPos);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentPos++;
                continue;
            }

            if (!inQuotes) {
                Matcher subqueryMatcher = subqueryPattern.matcher(query).region(currentPos, query.length());
                if (subqueryMatcher.lookingAt()) {
                    currentPos = findMatchingClosingParen(query, currentPos + 1);
                    if (currentPos == -1) {
                        return -1;
                    }
                    currentPos++;
                    continue;
                }

                if (c == '(') {
                    bracketDepth++;
                } else if (c == ')') {
                    bracketDepth--;
                    if (bracketDepth < 0) {
                        return -1;
                    }
                } else if (bracketDepth == 0) {
                    Matcher fromMatcher = fromPattern.matcher(query).region(currentPos, query.length());
                    if (fromMatcher.lookingAt()) {
                        return currentPos;
                    }
                }
            }
            currentPos++;
        }
        return -1;
    }

    private int findMatchingClosingParen(String query, int startPos) {
        int depth = 1;
        boolean inQuotes = false;
        for (int i = startPos; i < query.length(); i++) {
            char c = query.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private QueryParser.SelectItems parseSelectItems(String selectPart, Database database) {
        List<String> selectItems = splitCommaSeparatedItems(selectPart);
        List<String> columns = new ArrayList<>();
        List<QueryParser.AggregateFunction> aggregates = new ArrayList<>();
        List<QueryParser.SubQuery> subQueries = new ArrayList<>();
        Map<String, String> columnAliases = new HashMap<>();

        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+.*?\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\*|\\(\\s*SELECT\\s+.*?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            Matcher subQueryMatcher = subQueryPattern.matcher(trimmedItem);
            Matcher aggMatcher = aggPattern.matcher(trimmedItem);

            if (subQueryMatcher.matches()) {
                String subQueryStr = trimmedItem.substring(1, trimmedItem.lastIndexOf(")")).trim();
                String alias = subQueryMatcher.group(1);
                validateSubQuery(subQueryStr);
                Query<?> subQuery = queryParser.parse(subQueryStr, database);
                subQueries.add(new QueryParser.SubQuery(subQuery, alias));
                if (alias != null) {
                    columnAliases.put("SUBQUERY_" + subQueries.size(), alias);
                }
                LOGGER.log(Level.FINE, "Parsed subquery in SELECT: {0}{1}", new Object[]{subQueryStr, alias != null ? " AS " + alias : ""});
            } else if (aggMatcher.matches()) {
                String funcName = aggMatcher.group(1);
                String arg = aggMatcher.group(2);
                String alias = aggMatcher.group(3);
                if (arg.toUpperCase().startsWith("(") && arg.toUpperCase().contains("SELECT")) {
                    String subQueryStr = arg.substring(1, arg.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    aggregates.add(new QueryParser.AggregateFunction(funcName, new QueryParser.SubQuery(subQuery, null), alias));
                } else {
                    String column = arg.equals("*") ? null : arg;
                    aggregates.add(new QueryParser.AggregateFunction(funcName, column, alias));
                }
                LOGGER.log(Level.FINE, "Parsed aggregate: {0}({1}){2}", new Object[]{funcName, arg, alias != null ? " AS " + alias : ""});
            } else if (columnMatcher.matches()) {
                String column = columnMatcher.group(1);
                String alias = columnMatcher.group(2);
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                }
                LOGGER.log(Level.FINE, "Parsed column: {0}{1}", new Object[]{column, alias != null ? " AS " + alias : ""});
            } else {
                throw new IllegalArgumentException("Invalid SELECT item: " + trimmedItem);
            }
        }

        return new QueryParser.SelectItems(columns, aggregates, subQueries, columnAliases);
    }

    private QueryParser.TableJoins parseTableAndJoins(String tableAndJoins, Database database) {
        String normalized = normalizeQueryString(tableAndJoins).trim();
        List<QueryParser.JoinInfo> joins = new ArrayList<>();
        String tableName;
        String tableAlias = null;
        Map<String, String> tableAliases = new HashMap<>();

        Pattern joinPattern = Pattern.compile("(?i)\\s*(JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN)\\s+");
        Matcher joinMatcher = joinPattern.matcher(normalized);
        List<String> joinParts = new ArrayList<>();
        int lastEnd = 0;
        while (joinMatcher.find()) {
            joinParts.add(normalized.substring(lastEnd, joinMatcher.start()).trim());
            joinParts.add(joinMatcher.group(1).trim());
            lastEnd = joinMatcher.end();
        }
        joinParts.add(normalized.substring(lastEnd).trim());

        String mainTablePart = joinParts.get(0).trim();
        String[] mainTableTokens = mainTablePart.split("\\s+");
        tableName = mainTableTokens[0].trim();
        if (mainTableTokens.length > 1) {
            tableAlias = mainTableTokens[mainTableTokens.length - 1].trim();
        }
        if (tableAlias != null) {
            tableAliases.put(tableAlias, tableName);
        }

        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
        tableAliases.put(tableName, tableName);

        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();
            LOGGER.log(Level.FINEST, "Processing join part: {0}", joinPart);
            QueryParser.JoinType joinType = parseJoinType(joinTypeStr);
            String joinTableName;
            String joinTableAlias = null;
            List<QueryParser.Condition> onConditions = new ArrayList<>();

            int onIndex = findOnClausePosition(joinPart);
            String joinTablePart = onIndex == -1 ? joinPart : joinPart.substring(0, onIndex).trim();
            String onClause = "";
            if (onIndex != -1) {
                // Найти конец ON до следующей клаузы
                int endIndex = joinPart.length();
                Pattern clausePattern = Pattern.compile("(?i)\\b(WHERE|GROUP\\s+BY|ORDER\\s+BY|LIMIT)\\b");
                Matcher clauseMatcher = clausePattern.matcher(joinPart).region(onIndex + 2, joinPart.length());
                if (clauseMatcher.find()) {
                    endIndex = clauseMatcher.start();
                }
                onClause = joinPart.substring(onIndex + 2, endIndex).trim();
            }
            LOGGER.log(Level.FINEST, "Extracted onClause: {0}", onClause);

            String[] joinTableTokens = joinTablePart.split("\\s+");
            joinTableName = joinTableTokens[0].trim();
            if (joinTableTokens.length > 1) {
                joinTableAlias = joinTableTokens[joinTableTokens.length - 1].trim();
            }
            if (joinTableAlias != null) {
                tableAliases.put(joinTableAlias, joinTableName);
            }

            Table joinTable = database.getTable(joinTableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Join table not found: " + joinTableName);
            }
            combinedColumnTypes.putAll(joinTable.getColumnTypes());
            tableAliases.put(joinTableName, joinTableName);

            if (joinType != QueryParser.JoinType.CROSS && !onClause.isEmpty()) {
                onConditions = parseConditions(onClause, tableName, database, joinPart, true,
                        combinedColumnTypes, tableAliases, new HashMap<>());
                for (QueryParser.Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }
            }

            joins.add(new QueryParser.JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            tableName = joinTableName;
        }

        return new QueryParser.TableJoins(tableName, tableAlias, joins, tableAliases, combinedColumnTypes);
    }

    private QueryParser.JoinType parseJoinType(String joinTypeStr) {
        return switch (joinTypeStr.toUpperCase()) {
            case "JOIN", "INNER JOIN" -> QueryParser.JoinType.INNER;
            case "LEFT JOIN" -> QueryParser.JoinType.LEFT_OUTER;
            case "RIGHT JOIN" -> QueryParser.JoinType.RIGHT_OUTER;
            case "FULL JOIN" -> QueryParser.JoinType.FULL_OUTER;
            case "CROSS JOIN" -> QueryParser.JoinType.CROSS;
            default -> throw new IllegalArgumentException("Unsupported join type: " + joinTypeStr);
        };
    }

    private int findOnClausePosition(String joinPart) {
        int parenDepth = 0;
        boolean inQuotes = false;
        int onIndex = -1;
        Pattern clausePattern = Pattern.compile("(?i)\\b(WHERE|GROUP\\s+BY|ORDER\\s+BY|LIMIT)\\b");

        for (int i = 0; i < joinPart.length(); i++) {
            char c = joinPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (parenDepth == 0 && i + 2 < joinPart.length() &&
                        joinPart.substring(i, i + 2).toUpperCase().equals("ON")) {
                    onIndex = i;
                    i += 2;
                } else if (onIndex != -1 && parenDepth == 0) {
                    Matcher clauseMatcher = clausePattern.matcher(joinPart).region(i, joinPart.length());
                    if (clauseMatcher.lookingAt()) {
                        return onIndex;
                    }
                }
            }
        }
        return onIndex;
    }

    private void validateJoinCondition(QueryParser.Condition cond, String leftTable, String rightTable, Map<String, String> tableAliases) {
        if (cond.isGrouped()) {
            for (QueryParser.Condition subCond : cond.subConditions) {
                validateJoinCondition(subCond, leftTable, rightTable, tableAliases);
            }
        } else if (cond.isColumnComparison()) {
            String leftPrefix = cond.column.contains(".") ? cond.column.split("\\.")[0] : null;
            String rightPrefix = cond.rightColumn.contains(".") ? cond.rightColumn.split("\\.")[0] : null;
            if (leftPrefix == null || rightPrefix == null) {
                throw new IllegalArgumentException("Join condition must reference qualified columns: " + cond);
            }
            String leftTableName = tableAliases.getOrDefault(leftPrefix, leftPrefix);
            String rightTableName = tableAliases.getOrDefault(rightPrefix, rightPrefix);
            if (!(leftTableName.equals(leftTable) && rightTableName.equals(rightTable)) &&
                    !(leftTableName.equals(rightTable) && rightTableName.equals(leftTable))) {
                throw new IllegalArgumentException("Join condition must compare columns from " + leftTable + " and " + rightTable + ": " + cond);
            }
        } else {
            throw new IllegalArgumentException("Join condition must be a column comparison: " + cond);
        }
    }

    private QueryParser.AdditionalClauses parseAdditionalClauses(String tableAndJoins, String tableName, Database database,
                                                                 String originalQuery, List<QueryParser.AggregateFunction> aggregates,
                                                                 Map<String, Class<?>> combinedColumnTypes,
                                                                 Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                                 List<QueryParser.SubQuery> subQueries) {
        List<QueryParser.Condition> conditions = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<QueryParser.HavingCondition> havingConditions = new ArrayList<>();
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        Integer limit = null;
        Integer offset = null;

        // Найти индексы всех клауз
        int whereIndex = findClauseOutsideSubquery(tableAndJoins, "WHERE");
        int groupByIndex = findClauseOutsideSubquery(tableAndJoins, "GROUP BY");
        int orderByIndex = findClauseOutsideSubquery(tableAndJoins, "ORDER BY");
        int limitIndex = findClauseOutsideSubquery(tableAndJoins, "LIMIT");

        // Обработка WHERE
        if (whereIndex != -1) {
            int[] clauseIndices = {groupByIndex, orderByIndex, limitIndex};
            int whereEndIndex = tableAndJoins.length();
            for (int idx : clauseIndices) {
                if (idx != -1 && idx > whereIndex && idx < whereEndIndex) {
                    whereEndIndex = idx;
                }
            }
            // Находим позицию после WHERE и пробелов
            int startPos = whereIndex;
            String whereClause = tableAndJoins.substring(whereIndex, whereEndIndex).trim();
            Pattern wherePattern = Pattern.compile("(?i)^WHERE\\s+");
            Matcher whereMatcher = wherePattern.matcher(whereClause);
            if (whereMatcher.find()) {
                String conditionStr = whereClause.substring(whereMatcher.end()).trim();
                LOGGER.log(Level.FINEST, "Raw extracted conditionStr: {0}", conditionStr);
                // Удаляем LIMIT из conditionStr
                Pattern limitPattern = Pattern.compile("(?i)\\s*LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?\\s*$", Pattern.DOTALL);
                conditionStr = limitPattern.matcher(conditionStr).replaceAll("").trim();
                LOGGER.log(Level.FINEST, "After removing LIMIT: {0}", conditionStr);
                // Дополнительная очистка WHERE, если оно осталось
                conditionStr = conditionStr.replaceFirst("(?i)^\\s*WHERE\\s+", "").trim();
                LOGGER.log(Level.FINEST, "After removing WHERE: {0}", conditionStr);
                if (!conditionStr.isEmpty()) {
                    LOGGER.log(Level.FINEST, "Extracted WHERE condition: {0}", conditionStr);
                    conditions = parseConditions(conditionStr, tableName, database, originalQuery, false,
                            combinedColumnTypes, tableAliases, columnAliases);
                }
            } else {
                LOGGER.log(Level.WARNING, "WHERE clause not found in substring: {0}", whereClause);
            }
        }

        // Обработка GROUP BY
        if (groupByIndex != -1 && groupByIndex > whereIndex) {
            int groupByEndIndex = tableAndJoins.length();
            int[] clauseIndices = {orderByIndex, limitIndex};
            for (int idx : clauseIndices) {
                if (idx != -1 && idx > groupByIndex && idx < groupByEndIndex) {
                    groupByEndIndex = idx;
                }
            }
            String groupByClause = tableAndJoins.substring(groupByIndex + 8, groupByEndIndex).trim();
            int havingIndex = findClauseOutsideSubquery(groupByClause, "HAVING");
            String havingClause = null;
            if (havingIndex != -1) {
                havingClause = groupByClause.substring(havingIndex + 6).trim();
                groupByClause = groupByClause.substring(0, havingIndex).trim();
            }
            groupBy = parseGroupByClause(groupByClause, tableName, database, combinedColumnTypes, tableAliases, columnAliases);
            if (havingClause != null) {
                havingConditions = parseHavingConditions(havingClause, tableName, database, originalQuery,
                        aggregates, combinedColumnTypes, tableAliases, columnAliases);
            }
        }

        // Обработка ORDER BY
        if (orderByIndex != -1 && orderByIndex > whereIndex && orderByIndex > groupByIndex) {
            int orderByEndIndex = limitIndex != -1 ? limitIndex : tableAndJoins.length();
            // Дополнительная проверка, чтобы исключить LIMIT из ORDER BY
            String orderByClause = tableAndJoins.substring(orderByIndex + 8, orderByEndIndex).trim();
            // Удаляем LIMIT, если он остался в orderByClause
            Pattern limitPattern = Pattern.compile("(?i)\\s*LIMIT\\s+\\d+(\\s+OFFSET\\s+\\d+)?\\s*$", Pattern.DOTALL);
            orderByClause = limitPattern.matcher(orderByClause).replaceAll("");
            orderBy = parseOrderByClause(orderByClause, tableName, database, combinedColumnTypes, tableAliases, columnAliases, subQueries);
        }

        // Обработка LIMIT
        if (limitIndex != -1 && limitIndex > whereIndex && limitIndex > groupByIndex && limitIndex > orderByIndex) {
            String afterLimit = tableAndJoins.substring(limitIndex + 5).trim();
            Pattern limitPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:$|\\s+OFFSET\\s+|\\s*;\\s*$)");
            Matcher limitMatcher = limitPattern.matcher(afterLimit);
            if (limitMatcher.find()) {
                limit = Integer.parseInt(limitMatcher.group(1));
                String remaining = afterLimit.substring(limitMatcher.end()).trim();
                Pattern offsetPattern = Pattern.compile("(?i)^OFFSET\\s+(\\d+)\\s*(?:$|\\s*;\\s*$)");
                Matcher offsetMatcher = offsetPattern.matcher(remaining);
                if (offsetMatcher.find()) {
                    offset = Integer.parseInt(offsetMatcher.group(1));
                }
            }
        }

        return new QueryParser.AdditionalClauses(conditions, groupBy, havingConditions, orderBy, limit, offset);
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        Pattern quotedStringPattern = Pattern.compile("(?i)'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern clausePattern = Pattern.compile("(?i)\\b" + Pattern.quote(clause) + "\\b");
        Pattern wordPattern = Pattern.compile("\\S+");

        int parenDepth = 0;
        int clauseIndex = -1;
        int currentPos = 0;
        boolean inSubQuery = false;

        while (currentPos < query.length()) {
            Matcher quotedStringMatcher = quotedStringPattern.matcher(query).region(currentPos, query.length());
            Matcher openParenMatcher = openParenPattern.matcher(query).region(currentPos, query.length());
            Matcher closeParenMatcher = closeParenPattern.matcher(query).region(currentPos, query.length());
            Matcher clauseMatcher = clausePattern.matcher(query).region(currentPos, query.length());
            Matcher wordMatcher = wordPattern.matcher(query).region(currentPos, query.length());

            int nextPos = query.length();
            String token = null;
            String tokenType = null;
            int start = currentPos;

            if (quotedStringMatcher.lookingAt()) {
                token = quotedStringMatcher.group();
                nextPos = quotedStringMatcher.end();
                tokenType = "quotedString";
            } else if (openParenMatcher.lookingAt()) {
                token = openParenMatcher.group();
                nextPos = openParenMatcher.end();
                tokenType = "openParen";
            } else if (closeParenMatcher.lookingAt()) {
                token = closeParenMatcher.group();
                nextPos = closeParenMatcher.end();
                tokenType = "closeParen";
            } else if (clauseMatcher.lookingAt()) {
                token = clauseMatcher.group();
                nextPos = clauseMatcher.end();
                tokenType = "clause";
            } else if (wordMatcher.lookingAt()) {
                token = wordMatcher.group();
                nextPos = wordMatcher.end();
                tokenType = "word";
            }

            if (token == null) {
                currentPos++;
                continue;
            }

            if (tokenType.equals("quotedString")) {
                // Пропускаем строки в кавычках
            } else if (tokenType.equals("openParen")) {
                parenDepth++;
                if (parenDepth == 1 && currentPos + 7 < query.length() && query.substring(currentPos, currentPos + 7).toUpperCase().startsWith("(SELECT")) {
                    inSubQuery = true;
                }
            } else if (tokenType.equals("closeParen")) {
                parenDepth--;
                if (parenDepth == 0 && inSubQuery) {
                    inSubQuery = false;
                }
                if (parenDepth < 0) {
                    LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}", new Object[]{start, query});
                    return -1;
                }
            } else if (tokenType.equals("clause") && parenDepth == 0 && !inSubQuery) {
                LOGGER.log(Level.FINEST, "Considering clause {0} at position {1}, query: {2}", new Object[]{token, start, query});
                clauseIndex = start;
            }

            currentPos = nextPos;
        }

        if (parenDepth != 0) {
            LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе: parenDepth={0}, query={1}", new Object[]{parenDepth, query});
            return -1;
        }

        if (clauseIndex == -1) {
            LOGGER.log(Level.FINEST, "Клауза {0} не найдена вне подзапросов в запросе: {1}", new Object[]{clause, query});
        } else {
            LOGGER.log(Level.FINEST, "Найдена клауза {0} на индексе {1} в запросе: {2}", new Object[]{clause, clauseIndex, query});
        }
        return clauseIndex;
    }

    private List<QueryParser.OrderByInfo> parseOrderByClause(String orderByClause, String tableName, Database database,
                                                             Map<String, Class<?>> combinedColumnTypes,
                                                             Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                             List<QueryParser.SubQuery> subQueries) {
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        List<String> items = splitCommaSeparatedItems(orderByClause);
        Pattern columnPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(\\s*SELECT\\s+.*?\\))\\s*(?:LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?)?\\s*(ASC|DESC)?$",
                Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                String direction = columnMatcher.group(2);
                boolean ascending = direction == null || direction.equalsIgnoreCase("ASC");

                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains("SELECT")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    subQueries.add(new QueryParser.SubQuery(subQuery, null));
                    orderBy.add(new QueryParser.OrderByInfo("SUBQUERY_" + subQueries.size(), ascending));
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, tableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    orderBy.add(new QueryParser.OrderByInfo(normalizedColumn, ascending));
                }
            } else {
                throw new IllegalArgumentException("Invalid ORDER BY item: " + trimmedItem);
            }
        }
        return orderBy;
    }

    private List<String> parseGroupByClause(String groupByClause, String tableName, Database database,
                                            Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                            Map<String, String> columnAliases) {
        List<String> groupBy = new ArrayList<>();
        List<String> items = splitCommaSeparatedItems(groupByClause);
        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(\\s*SELECT\\s+.*?\\))$", Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains("SELECT")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    groupBy.add("SUBQUERY_" + System.currentTimeMillis());
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, tableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    groupBy.add(normalizedColumn);
                }
            } else {
                throw new IllegalArgumentException("Invalid GROUP BY item: " + trimmedItem);
            }
        }
        return groupBy;
    }

    private List<String> splitCommaSeparatedItems(String input) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        int parenDepth = 0;
        boolean inQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
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

    private List<QueryParser.Condition> parseConditions(String conditionStr, String defaultTableName, Database database,
                                                        String originalQuery, boolean isJoinCondition,
                                                        Map<String, Class<?>> combinedColumnTypes,
                                                        Map<String, String> tableAliases, Map<String, String> columnAliases) {
        if (conditionStr == null || conditionStr.trim().isEmpty()) {
            return new ArrayList<>();
        }
        LOGGER.log(Level.FINEST, "Original conditionStr in parseConditions: {0}", conditionStr);
        // Удаляем WHERE, если присутствует
        conditionStr = conditionStr.replaceFirst("(?i)^\\s*WHERE\\s+", "").trim();
        LOGGER.log(Level.FINEST, "After removing WHERE in parseConditions: {0}", conditionStr);
        // Очищаем строку от LIMIT
        Pattern limitPattern = Pattern.compile("(?i)\\s*LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?\\s*$", Pattern.DOTALL);
        conditionStr = limitPattern.matcher(conditionStr).replaceAll("").trim();
        LOGGER.log(Level.FINEST, "After removing LIMIT in parseConditions: {0}", conditionStr);
        if (conditionStr.isEmpty()) {
            return new ArrayList<>();
        }
        LOGGER.log(Level.FINEST, "Cleaned condition string: {0}", conditionStr);
        try {
            List<Token> tokens = tokenizeConditions(conditionStr);
            return parseTokenizedConditions(tokens, defaultTableName, database, originalQuery, isJoinCondition,
                    combinedColumnTypes, tableAliases, columnAliases, null, false);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to tokenize condition: {0}, Error: {1}", new Object[]{conditionStr, e.getMessage()});
            throw e;
        }
    }

    private static class Token {
        final TokenType type;
        final String value;

        Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        enum TokenType {
            CONDITION, LOGICAL_OPERATOR
        }
    }

    private List<Token> tokenizeConditions(String conditionStr) {
        // Предварительная обработка: добавляем пробелы перед LIMIT
        String processedStr = conditionStr.replaceAll("(?i)(\\))\\s*(LIMIT\\s+\\d+)", "$1 $2");
        List<Map.Entry<String, Pattern>> patterns = new ArrayList<>();
        patterns.add(Map.entry("Quoted String", Pattern.compile("'(?:\\\\.|[^'\\\\])*'")));
        patterns.add(Map.entry("Grouped Condition", Pattern.compile("\\((?:[^()']+|'(?:\\\\.|[^'\\\\])*')*\\)")));
        patterns.add(Map.entry("In Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(NOT\\s*)?IN\\s*\\((?:[^()']+|'(?:\\\\.|[^'\\\\])*'|\\([^()]*\\))*\\)")));
        patterns.add(Map.entry("Subquery Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>|LIKE|NOT\\s+LIKE)\\s*\\(\\s*SELECT\\s+.*?\\s*\\)(?:\\s+LIMIT\\s*\\d+(?:\\s+OFFSET\\s*\\d+)?)?(?=\\s*(?:$|\\)|AND|OR|LIMIT|WHERE|GROUP\\s+BY|ORDER\\s+BY|HAVING))", Pattern.DOTALL)));
        patterns.add(Map.entry("Like Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(NOT\\s*)?LIKE\\s*'(?:\\\\.|[^'\\\\])*'")));
        patterns.add(Map.entry("Null Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*IS\\s*(NOT\\s+)?NULL\\b")));
        patterns.add(Map.entry("Comparison Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>)\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|'[^']*'|[0-9]+(?:\\.[0-9]+)?)")));
        patterns.add(Map.entry("Logical Operator", Pattern.compile("(?i)\\b(AND|OR)\\b")));

        List<Token> tokens = new ArrayList<>();
        int currentPos = 0;
        int stringLength = processedStr.length();

        while (currentPos < stringLength) {
            while (currentPos < stringLength && Character.isWhitespace(processedStr.charAt(currentPos))) {
                currentPos++;
            }
            if (currentPos >= stringLength) {
                break;
            }

            boolean matched = false;
            String matchedToken = null;
            String matchedPatternName = null;
            int nextPos = stringLength;

            for (Map.Entry<String, Pattern> entry : patterns) {
                Pattern pattern = entry.getValue();
                Matcher matcher = pattern.matcher(processedStr).region(currentPos, stringLength);
                if (matcher.lookingAt()) {
                    String tokenValue = matcher.group().trim();
                    if (!tokenValue.isEmpty()) {
                        matchedToken = tokenValue;
                        matchedPatternName = entry.getKey();
                        nextPos = matcher.end();
                        matched = true;
                        break;
                    }
                }
            }

            if (matched) {
                Token.TokenType type = matchedPatternName.equals("Logical Operator") ?
                        Token.TokenType.LOGICAL_OPERATOR : Token.TokenType.CONDITION;
                tokens.add(new Token(type, matchedToken));
                LOGGER.log(Level.FINEST, "Tokenized: {0}, type: {1}", new Object[]{matchedToken, type});
                currentPos = nextPos;
            } else {
                throw new IllegalArgumentException("Invalid token at position " + currentPos + ": " + processedStr.substring(currentPos));
            }
        }

        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("No valid tokens found in condition: " + processedStr);
        }
        return tokens;
    }
    private List<QueryParser.Condition> parseTokenizedConditions(List<Token> tokens, String defaultTableName, Database database,
                                                                 String originalQuery, boolean isJoinCondition,
                                                                 Map<String, Class<?>> combinedColumnTypes,
                                                                 Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                                 String conjunction, boolean not) {
        List<QueryParser.Condition> conditions = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (token.type == Token.TokenType.LOGICAL_OPERATOR) {
                conjunction = token.value.toUpperCase();
                continue;
            }
            String condStr = token.value;
            if (condStr.startsWith("(") && condStr.endsWith(")")) {
                String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
                if (subCondStr.toUpperCase().startsWith("SELECT")) {
                    validateSubQuery(subCondStr);
                    Query<?> subQuery = queryParser.parse(subCondStr, database);
                    conditions.add(new QueryParser.Condition(defaultTableName + ".unknown", new QueryParser.SubQuery(subQuery, null), conjunction, not));
                } else {
                    List<Token> subTokens = tokenizeConditions(subCondStr);
                    List<QueryParser.Condition> subConditions = parseTokenizedConditions(subTokens, defaultTableName, database,
                            originalQuery, isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                    conditions.add(new QueryParser.Condition(subConditions, conjunction, not));
                }
            } else if (condStr.toUpperCase().contains(" IN ")) {
                conditions.add(parseInCondition(condStr, defaultTableName, database, originalQuery,
                        combinedColumnTypes, tableAliases, columnAliases, conjunction, not));
            } else if (condStr.toUpperCase().contains("SELECT")) {
                conditions.add(parseSubQueryCondition(condStr, defaultTableName, database, originalQuery,
                        combinedColumnTypes, tableAliases, columnAliases, conjunction, not));
            } else {
                conditions.add(parseSingleCondition(condStr, defaultTableName, database, originalQuery,
                        isJoinCondition, combinedColumnTypes, tableAliases, columnAliases, conjunction, not));
            }
            conjunction = null;
        }
        return conditions;
    }

    // Модифицируем метод parseInCondition
// Удаляем createCustomSubQuery, так как он несовместим с Query<T>
// Вместо этого модифицируем parseInCondition для хранения подзапроса как строки

    private QueryParser.Condition parseInCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                                   Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                                   Map<String, String> columnAliases, String conjunction, boolean not) {
        Pattern inPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((SELECT(?:[^()']+|'(?:\\\\.|[^'\\\\])*'|\\([^()]*\\))*?)\\)(?:\\s+LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?)?$",
                Pattern.DOTALL);
        Matcher inMatcher = inPattern.matcher(condStr);
        if (!inMatcher.matches()) {
            Pattern valuesInPattern = Pattern.compile(
                    "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((.*?)\\)$",
                    Pattern.DOTALL);
            Matcher valuesMatcher = valuesInPattern.matcher(condStr);
            if (!valuesMatcher.matches()) {
                throw new IllegalArgumentException("Invalid IN condition format: " + condStr);
            }

            String column = valuesMatcher.group(1).trim();
            boolean inNot = valuesMatcher.group(2) != null;
            String valuesStr = valuesMatcher.group(3).trim();
            String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
            Class<?> columnType = getColumnType(normalizedColumn, combinedColumnTypes);

            LOGGER.log(Level.FINEST, "Parsing IN condition: column={0}, values={1}, not={2}", new Object[]{normalizedColumn, valuesStr, inNot});

            List<String> valueParts = splitInValues(valuesStr);
            List<Object> inValues = new ArrayList<>();
            for (String val : valueParts) {
                String trimmedVal = val.trim();
                if (trimmedVal.isEmpty()) continue;
                Object value = parseConditionValue(normalizedColumn, trimmedVal, columnType);
                inValues.add(value);
            }
            if (inValues.isEmpty()) {
                throw new IllegalArgumentException("Empty IN list in: " + condStr);
            }
            LOGGER.log(Level.FINE, "Parsed IN values condition: {0} {1}IN {2}", new Object[]{normalizedColumn, inNot ? "NOT " : "", inValues});
            return new QueryParser.Condition(normalizedColumn, inValues, conjunction, inNot);
        }

        String column = inMatcher.group(1).trim();
        boolean inNot = inMatcher.group(2) != null;
        String subQueryStr = inMatcher.group(3).trim();
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);

        LOGGER.log(Level.FINEST, "Parsing IN subquery condition: column={0}, subquery={1}, not={2}", new Object[]{normalizedColumn, subQueryStr, inNot});

        validateSubQuery(subQueryStr);
        // Создаём заглушку Query<?>, которая хранит строку подзапроса
        Query<?> subQuery = new Query<List<?>>() {
            @Override
            public List<?> execute(Table table) {
                // Это не должно вызываться, так как подзапрос должен обрабатываться в SelectQuery
                throw new UnsupportedOperationException("Subquery execution should be handled by SelectQuery: " + subQueryStr);
            }
            @Override
            public String toString() {
                return subQueryStr;
            }
        };
        QueryParser.SubQuery subQueryObj = new QueryParser.SubQuery(subQuery, null);
        LOGGER.log(Level.FINE, "Parsed IN subquery condition: {0} {1}IN (subquery: {2})", new Object[]{normalizedColumn, inNot ? "NOT " : "", subQueryStr});
        return new QueryParser.Condition(normalizedColumn, subQueryObj, conjunction, inNot);
    }

    private List<String> splitInValues(String input) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                current.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    current.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    current.append(c);
                    continue;
                } else if (c == ',' && parenDepth == 0) {
                    String value = current.toString().trim();
                    if (!value.isEmpty()) {
                        values.add(value);
                    }
                    current = new StringBuilder();
                    continue;
                }
            }
            current.append(c);
        }

        String value = current.toString().trim();
        if (!value.isEmpty()) {
            values.add(value);
        }
        return values;
    }

    private QueryParser.Condition parseSubQueryCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                                         Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                                         Map<String, String> columnAliases, String conjunction, boolean not) {
        // Обновленное регулярное выражение для подзапросов
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>|LIKE|NOT\\s+LIKE)\\s*\\((SELECT(?:[^()']+|'(?:\\\\.|[^'\\\\])*'|\\([^()]*\\))*?)\\)(?:\\s+LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?)?$",
                Pattern.DOTALL);
        Matcher subQueryMatcher = subQueryPattern.matcher(condStr);
        if (!subQueryMatcher.matches()) {
            throw new IllegalArgumentException("Invalid subquery condition format: " + condStr);
        }

        String column = subQueryMatcher.group(1).trim();
        String operatorStr = subQueryMatcher.group(2).trim();
        String subQueryStr = subQueryMatcher.group(3).trim();
        validateSubQuery(subQueryStr);
        Query<?> subQuery = queryParser.parse(subQueryStr, database);
        QueryParser.SubQuery newSubQuery = new QueryParser.SubQuery(subQuery, null);
        QueryParser.Operator operator = parseOperator(operatorStr);
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);
        LOGGER.log(Level.FINE, "Parsed subquery condition: {0} {1} (subquery: {2})", new Object[]{normalizedColumn, operatorStr, subQueryStr});
        return new QueryParser.Condition(normalizedColumn, newSubQuery, operator, conjunction, not);
    }

    private void validateSubQuery(String subQueryStr) {
        if (!subQueryStr.toUpperCase().startsWith("SELECT")) {
            throw new IllegalArgumentException("Invalid subquery: must start with SELECT: " + subQueryStr);
        }
        if (!subQueryStr.toUpperCase().contains(" FROM ")) {
            throw new IllegalArgumentException("Invalid subquery: missing FROM clause: " + subQueryStr);
        }
        int parenDepth = 0;
        boolean inQuotes = false;
        for (int i = 0; i < subQueryStr.length(); i++) {
            char c = subQueryStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth < 0) {
                        throw new IllegalArgumentException("Unbalanced parentheses in subquery: " + subQueryStr);
                    }
                }
            }
        }
        if (parenDepth != 0) {
            throw new IllegalArgumentException("Unbalanced parentheses in subquery: " + subQueryStr);
        }
    }

    private QueryParser.Condition parseSingleCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                                       boolean isJoinCondition, Map<String, Class<?>> combinedColumnTypes,
                                                       Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                       String conjunction, boolean not) {
        Pattern likePattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(LIKE|NOT\\s+LIKE)\\s*('(?:[^']|)*')");
        Matcher likeMatcher = likePattern.matcher(condStr);
        if (likeMatcher.matches()) {
            String column = likeMatcher.group(1).trim();
            String operatorStr = likeMatcher.group(2).toUpperCase();
            String value = likeMatcher.group(3).substring(1, likeMatcher.group(3).length() - 1);
            String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
            validateColumn(normalizedColumn, combinedColumnTypes);
            QueryParser.Operator operator = operatorStr.equals("LIKE") ? QueryParser.Operator.LIKE : QueryParser.Operator.NOT_LIKE;
            Object parsedValue = parseConditionValue(normalizedColumn, "'" + value + "'", getColumnType(normalizedColumn, combinedColumnTypes));
            return new QueryParser.Condition(normalizedColumn, parsedValue, operator, conjunction, not);
        }

        Pattern isNullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(condStr);
        if (isNullMatcher.matches()) {
            String column = isNullMatcher.group(1).trim();
            boolean isNotNull = isNullMatcher.group(2) != null;
            String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
            validateColumn(normalizedColumn, combinedColumnTypes);
            QueryParser.Operator operator = isNotNull ? QueryParser.Operator.IS_NOT_NULL : QueryParser.Operator.IS_NULL;
            return new QueryParser.Condition(normalizedColumn, operator, conjunction, not);
        }

        String[] operators = {"!=", "<>", ">=", "<=", "=", "<", ">"};
        QueryParser.OperatorInfo operatorInfo = findOperator(condStr, operators);
        if (operatorInfo == null) {
            throw new IllegalArgumentException("Invalid condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorInfo.index).trim();
        String rightPart = condStr.substring(operatorInfo.endIndex).trim();
        String column = leftPart;
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);

        Pattern columnPattern = Pattern.compile("(?i)^[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
        String rightColumn = null;
        Object value = null;

        if (columnPattern.matcher(rightPart).matches()) {
            rightColumn = rightPart;
        } else {
            value = parseConditionValue(normalizedColumn, rightPart, getColumnType(normalizedColumn, combinedColumnTypes));
        }

        QueryParser.Operator operator = parseOperator(operatorInfo.operator);

        if (isJoinCondition && !rightColumnIsFromDifferentTable(normalizedColumn, rightColumn, tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + condStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, defaultTableName, tableAliases);
            validateColumn(normalizedRightColumn, combinedColumnTypes);
            return new QueryParser.Condition(normalizedColumn, normalizedRightColumn, operator, conjunction, not);
        } else {
            return new QueryParser.Condition(normalizedColumn, value, operator, conjunction, not);
        }
    }

    private QueryParser.Operator parseOperator(String operatorStr) {
        return switch (operatorStr.toUpperCase().trim()) {
            case "=" -> QueryParser.Operator.EQUALS;
            case "!=", "<>" -> QueryParser.Operator.NOT_EQUALS;
            case "<" -> QueryParser.Operator.LESS_THAN;
            case ">" -> QueryParser.Operator.GREATER_THAN;
            case "<=" -> QueryParser.Operator.LESS_THAN_OR_EQUALS;
            case ">=" -> QueryParser.Operator.GREATER_THAN_OR_EQUALS;
            case "LIKE" -> QueryParser.Operator.LIKE;
            case "NOT LIKE" -> QueryParser.Operator.NOT_LIKE;
            default -> throw new IllegalArgumentException("Unsupported operator: " + operatorStr);
        };
    }

    private QueryParser.OperatorInfo findOperator(String condStr, String[] operators) {
        int parenDepth = 0;
        boolean inQuotes = false;
        for (int i = 0; i < condStr.length(); i++) {
            char c = condStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    continue;
                } else if (parenDepth == 0) {
                    for (String op : operators) {
                        Pattern opPattern = Pattern.compile("(?i)\\s+" + Pattern.quote(op) + "\\s+");
                        Matcher opMatcher = opPattern.matcher(" " + condStr + " ").region(0, condStr.length() + 2);
                        if (opMatcher.find() && opMatcher.start() <= i + 1 && opMatcher.end() >= i + 1) {
                            String matchedOp = opMatcher.group().trim();
                            int actualIndex = opMatcher.start() - 1;
                            int actualEndIndex = opMatcher.end() - 1;
                            return new QueryParser.OperatorInfo(matchedOp, actualIndex, actualEndIndex);
                        }
                    }
                }
            }
        }
        return null;
    }

    private boolean rightColumnIsFromDifferentTable(String leftColumn, String rightColumn, Map<String, String> tableAliases) {
        if (rightColumn == null) {
            return true;
        }
        String leftPrefix = leftColumn.contains(".") ? leftColumn.split("\\.")[0] : null;
        String rightPrefix = rightColumn.contains(".") ? rightColumn.split("\\.")[0] : null;
        if (leftPrefix == null || rightPrefix == null) {
            return true;
        }
        return !leftPrefix.equalsIgnoreCase(rightPrefix);
    }

    private Object parseConditionValue(String column, String value, Class<?> columnType) {
        try {
            if (value.startsWith("'") && value.endsWith("'")) {
                String strValue = value.substring(1, value.length() - 1);
                if (columnType == String.class) {
                    return strValue;
                } else if (columnType == UUID.class) {
                    return UUID.fromString(strValue);
                } else if (columnType == LocalDate.class) {
                    return LocalDate.parse(strValue);
                } else if (columnType == LocalDateTime.class) {
                    return strValue.contains(".") ?
                            LocalDateTime.parse(strValue, QueryParser.DATETIME_MS_FORMATTER) :
                            LocalDateTime.parse(strValue, QueryParser.DATETIME_FORMATTER);
                }
            } else {
                if (columnType == Integer.class) {
                    return Integer.parseInt(value);
                } else if (columnType == Long.class) {
                    return Long.parseLong(value);
                } else if (columnType == Short.class) {
                    return Short.parseShort(value);
                } else if (columnType == Byte.class) {
                    return Byte.parseByte(value);
                } else if (columnType == BigDecimal.class) {
                    return new BigDecimal(value);
                } else if (columnType == Float.class) {
                    return Float.parseFloat(value);
                } else if (columnType == Double.class) {
                    return Double.parseDouble(value);
                } else if (columnType == Boolean.class) {
                    return Boolean.parseBoolean(value);
                }
            }
            throw new IllegalArgumentException("Unsupported value type for column " + column + ": " + value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse value '" + value + "' for column " + column + ": " + e.getMessage());
        }
    }

    private Class<?> getColumnType(String column, Map<String, Class<?>> combinedColumnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
            if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("Unknown column: " + column);
    }

    private void validateColumn(String column, Map<String, Class<?>> combinedColumnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        boolean found = false;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
            if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new IllegalArgumentException("Unknown column: " + column);
        }
    }

    private String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        if (column.contains(".")) {
            String[] parts = column.split("\\.");
            String tableOrAlias = parts[0].trim();
            String colName = parts[1].trim();
            String tableName = tableAliases.getOrDefault(tableOrAlias, tableOrAlias);
            return tableName + "." + colName;
        }
        return defaultTableName + "." + column.trim();
    }

    private List<QueryParser.HavingCondition> parseHavingConditions(String havingClause, String defaultTableName, Database database,
                                                                    String originalQuery, List<QueryParser.AggregateFunction> aggregates,
                                                                    Map<String, Class<?>> combinedColumnTypes,
                                                                    Map<String, String> tableAliases, Map<String, String> columnAliases) {
        List<QueryParser.HavingCondition> conditions = new ArrayList<>();
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes = false;
        int parenDepth = 0;
        String conjunction = null;
        boolean not = false;

        for (int i = 0; i < havingClause.length(); i++) {
            char c = havingClause.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentCondition.append(c);
                continue;
            }
            if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                    currentCondition.append(c);
                    continue;
                } else if (c == ')') {
                    parenDepth--;
                    currentCondition.append(c);
                    if (parenDepth == 0 && currentCondition.length() > 0) {
                        String condStr = currentCondition.toString().trim();
                        if (condStr.startsWith("(") && condStr.endsWith(")")) {
                            condStr = condStr.substring(1, condStr.length() - 1).trim();
                            if (!condStr.isEmpty()) {
                                List<QueryParser.HavingCondition> subConditions = parseHavingConditions(condStr, defaultTableName,
                                        database, originalQuery, aggregates, combinedColumnTypes, tableAliases, columnAliases);
                                conditions.add(new QueryParser.HavingCondition(subConditions, conjunction, not));
                            }
                        }
                        currentCondition = new StringBuilder();
                        conjunction = null;
                        not = false;
                    }
                    continue;
                } else if (parenDepth == 0 && c == ' ') {
                    String nextToken = getNextToken(havingClause, i + 1);
                    if (nextToken.equalsIgnoreCase("AND") || nextToken.equalsIgnoreCase("OR")) {
                        String condStr = currentCondition.toString().trim();
                        if (!condStr.isEmpty()) {
                            QueryParser.HavingCondition condition = parseSingleHavingCondition(condStr, defaultTableName, database, originalQuery,
                                    aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
                            conditions.add(condition);
                            conjunction = nextToken.toUpperCase();
                            not = false;
                            currentCondition = new StringBuilder();
                            i += nextToken.length();
                        }
                        continue;
                    } else if (nextToken.equalsIgnoreCase("NOT")) {
                        not = true;
                        currentCondition.append(c);
                        i += nextToken.length();
                        continue;
                    }
                }
            }
            currentCondition.append(c);
        }

        String finalCondStr = currentCondition.toString().trim();
        if (!finalCondStr.isEmpty()) {
            QueryParser.HavingCondition condition = parseSingleHavingCondition(finalCondStr, defaultTableName, database, originalQuery,
                    aggregates, combinedColumnTypes, tableAliases, columnAliases, conjunction, not);
            conditions.add(condition);
        }

        return conditions;
    }

    private QueryParser.HavingCondition parseSingleHavingCondition(String condStr, String defaultTableName, Database database,
                                                                   String originalQuery, List<QueryParser.AggregateFunction> aggregates,
                                                                   Map<String, Class<?>> combinedColumnTypes,
                                                                   Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                                   String conjunction, boolean not) {
        String[] operators = {"=", "!=", "<>", ">=", "<=", "<", ">"};
        String selectedOperator = null;
        int operatorIndex = -1;
        for (String op : operators) {
            Pattern opPattern = Pattern.compile("(?i)\\s+" + Pattern.quote(op) + "\\s+");
            Matcher opMatcher = opPattern.matcher(" " + condStr + " ");
            if (opMatcher.find()) {
                selectedOperator = op;
                operatorIndex = opMatcher.start() - 1;
                break;
            }
        }

        if (operatorIndex == -1) {
            throw new IllegalArgumentException("Invalid HAVING condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorIndex).trim();
        String rightPart = condStr.substring(operatorIndex + selectedOperator.length()).trim();

        QueryParser.AggregateFunction aggregate = null;
        for (QueryParser.AggregateFunction agg : aggregates) {
            String aggStr = agg.toString();
            if (aggStr.equalsIgnoreCase(leftPart) || (agg.alias != null && agg.alias.equalsIgnoreCase(leftPart))) {
                aggregate = agg;
                break;
            }
        }

        if (aggregate == null) {
            Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(\\s*SELECT\\s+.*?\\))\\s*\\)(?:\\s+AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
            Matcher aggMatcher = aggPattern.matcher(leftPart);
            if (aggMatcher.matches()) {
                String funcName = aggMatcher.group(1);
                String columnOrSubQuery = aggMatcher.group(2);
                String alias = aggMatcher.group(3);
                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains("SELECT")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    aggregate = new QueryParser.AggregateFunction(funcName, new QueryParser.SubQuery(subQuery, null), alias);
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, defaultTableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    aggregate = new QueryParser.AggregateFunction(funcName, columnOrSubQuery, alias);
                }
            } else {
                throw new IllegalArgumentException("Invalid HAVING condition: left side must be an aggregate function: " + leftPart);
            }
        }

        Class<?> valueType = aggregate.functionName.equals("COUNT") ? Long.class :
                (aggregate.column != null ? getColumnType(aggregate.column, combinedColumnTypes) : Double.class);
        Object value = parseConditionValue(aggregate.toString(), rightPart, valueType);

        QueryParser.Operator operator = parseOperator(selectedOperator);

        return new QueryParser.HavingCondition(aggregate, operator, value, conjunction, not);
    }

    private String getNextToken(String str, int startIndex) {
        Pattern tokenPattern = Pattern.compile("(?s)(?:'(?:\\\\.|[^'])*'|[^\\s()']+)");
        Matcher matcher = tokenPattern.matcher(str).region(startIndex, str.length());
        if (matcher.find()) {
            return matcher.group().trim();
        }
        return "";
    }
}
