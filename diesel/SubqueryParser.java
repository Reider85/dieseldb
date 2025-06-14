package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SubqueryParser {
    private static final Logger LOGGER = Logger.getLogger(SubqueryParser.class.getName());
    private final QueryParser queryParser;

    public SubqueryParser() {
        this.queryParser = new QueryParser();
    }

    public Query<?> parse(String query, Database database) {
        LOGGER.log(Level.INFO, "Попытка парсинга запроса: {0}", query);
        String normalizedQuery = normalizeQueryString(query).trim();

        // Проверка на наличие подзапросов
        if (!containsSubquery(normalizedQuery)) {
            LOGGER.log(Level.FINE, "Подзапросы не найдены, используется QueryParser: {0}", query);
            return queryParser.parse(query, database);
        }

        LOGGER.log(Level.FINE, "Обнаружены подзапросы, парсинг через SubqueryParser: {0}", query);
        try {
            if (normalizedQuery.startsWith("SELECT")) {
                return parseSelectQuery(query, normalizedQuery, database);
            } else {
                // Для других типов запросов (INSERT, UPDATE, DELETE и т.д.) используем QueryParser
                LOGGER.log(Level.FINE, "Не SELECT запрос, делегируем QueryParser: {0}", query);
                return queryParser.parse(query, database);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Ошибка парсинга запроса: {0}, Ошибка: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    public boolean containsSubquery(String query) {
        // Проверяем наличие подзапросов вида (SELECT ...)
        Pattern subqueryPattern = Pattern.compile("(?i)\\(\\s*SELECT\\b", Pattern.DOTALL);
        Matcher matcher = subqueryPattern.matcher(query);
        return matcher.find();
    }

    private Query<List<Map<String, Object>>> parseSelectQuery(String originalQuery, String normalizedQuery, Database database) {
        // Находим индекс основного FROM
        int fromIndex = findMainFromClause(originalQuery);
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Недопустимый формат SELECT-запроса: отсутствует FROM");
        }

        // Извлекаем части запроса
        String selectPartOriginal = originalQuery.substring(originalQuery.toUpperCase().indexOf("SELECT") + 6, fromIndex).trim();
        String tableAndJoinsOriginal = originalQuery.substring(fromIndex + 4).trim();

        // Парсим элементы SELECT
        QueryParser.SelectItems selectItems = parseSelectItems(selectPartOriginal, database);
        List<String> columns = selectItems.columns;
        List<QueryParser.AggregateFunction> aggregates = selectItems.aggregates;
        List<QueryParser.SubQuery> subQueries = selectItems.subQueries;
        Map<String, String> columnAliases = selectItems.columnAliases;

        // Парсим таблицы и соединения
        QueryParser.TableJoins tableJoins = parseTableAndJoins(tableAndJoinsOriginal, database);
        String tableName = tableJoins.tableName;
        String tableAlias = tableJoins.tableAlias;
        List<QueryParser.JoinInfo> joins = tableJoins.joins;
        Map<String, String> tableAliases = tableJoins.tableAliases;
        Map<String, Class<?>> combinedColumnTypes = tableJoins.combinedColumnTypes;

        // Парсим дополнительные клаузы
        QueryParser.AdditionalClauses clauses = parseAdditionalClauses(tableAndJoinsOriginal, tableName, database, originalQuery,
                aggregates, combinedColumnTypes, tableAliases, columnAliases, subQueries);
        List<QueryParser.Condition> conditions = clauses.conditions;
        List<String> groupBy = clauses.groupBy;
        List<QueryParser.HavingCondition> havingConditions = clauses.havingConditions;
        List<QueryParser.OrderByInfo> orderBy = clauses.orderBy;
        Integer limit = clauses.limit;
        Integer offset = clauses.offset;

        LOGGER.log(Level.INFO, "Разобран SELECT-запрос с подзапросами: таблица={0}, столбцы={1}, агрегации={2}, соединения={3}, условия={4}",
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
            // Пропускаем строки в кавычках
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
                // Проверяем начало подзапроса
                Matcher subqueryMatcher = subqueryPattern.matcher(query).region(currentPos, query.length());
                if (subqueryMatcher.lookingAt()) {
                    int subqueryStart = currentPos;
                    currentPos = findMatchingClosingParen(query, currentPos + 1);
                    if (currentPos == -1) {
                        LOGGER.log(Level.SEVERE, "Несбалансированные скобки в подзапросе на позиции {0}: {1}",
                                new Object[]{subqueryStart, query});
                        return -1;
                    }
                    currentPos++; // Пропускаем закрывающую скобку
                    continue;
                }

                if (c == '(') {
                    bracketDepth++;
                    currentPos++;
                    continue;
                } else if (c == ')') {
                    bracketDepth--;
                    if (bracketDepth < 0) {
                        LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}",
                                new Object[]{currentPos, query});
                        return -1;
                    }
                    currentPos++;
                    continue;
                }

                // Проверяем FROM на верхнем уровне
                Matcher fromMatcher = fromPattern.matcher(query).region(currentPos, query.length());
                if (fromMatcher.lookingAt() && bracketDepth == 0) {
                    LOGGER.log(Level.FINE, "Найден основной FROM на позиции {0}", currentPos);
                    return currentPos;
                }
            }
            currentPos++;
        }

        if (bracketDepth != 0) {
            LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе: bracketDepth={0}, query={1}",
                    new Object[]{bracketDepth, query});
            return -1;
        }

        LOGGER.log(Level.FINE, "Основной FROM не найден в запросе: {0}", query);
        return -1;
    }

    // Вспомогательный метод для поиска закрывающей скобки
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
    private QueryParser.SelectItems parseSelectItems(String selectPartOriginal, Database database) {
        List<String> selectItems = splitSelectItems(selectPartOriginal);
        List<String> columns = new ArrayList<>();
        List<QueryParser.AggregateFunction> aggregates = new ArrayList<>();
        List<QueryParser.SubQuery> subQueries = new ArrayList<>();
        Map<String, String> columnAliases = new HashMap<>();

        Pattern countPattern = Pattern.compile("(?i)^COUNT\\s*\\(\\s*(\\*|[a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.+?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern minPattern = Pattern.compile("(?i)^MIN\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.+?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern maxPattern = Pattern.compile("(?i)^MAX\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.+?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern avgPattern = Pattern.compile("(?i)^AVG\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.+?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern sumPattern = Pattern.compile("(?i)^SUM\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(.+?\\))\\s*\\)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$", Pattern.DOTALL);
        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)(?:\\s+(?:AS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*SELECT\\s+.*?\\s*\\)\\s*(?:AS\\s+([a-zA-Z_][a-zA-Z0-9_]*))?\\s*$", Pattern.DOTALL);

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher countMatcher = countPattern.matcher(trimmedItem);
            Matcher minMatcher = minPattern.matcher(trimmedItem);
            Matcher maxMatcher = maxPattern.matcher(trimmedItem);
            Matcher avgMatcher = avgPattern.matcher(trimmedItem);
            Matcher sumMatcher = sumPattern.matcher(trimmedItem);
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            Matcher subQueryMatcher = subQueryPattern.matcher(trimmedItem);

            if (countMatcher.matches()) {
                String countArg = countMatcher.group(1);
                String alias = countMatcher.group(2);
                handleAggregateFunction("COUNT", countArg, alias, aggregates, subQueries, database);
            } else if (minMatcher.matches()) {
                String column = minMatcher.group(1);
                String alias = minMatcher.group(2);
                handleAggregateFunction("MIN", column, alias, aggregates, subQueries, database);
            } else if (maxMatcher.matches()) {
                String column = maxMatcher.group(1);
                String alias = maxMatcher.group(2);
                handleAggregateFunction("MAX", column, alias, aggregates, subQueries, database);
            } else if (avgMatcher.matches()) {
                String column = avgMatcher.group(1);
                String alias = avgMatcher.group(2);
                handleAggregateFunction("AVG", column, alias, aggregates, subQueries, database);
            } else if (sumMatcher.matches()) {
                String column = sumMatcher.group(1);
                String alias = sumMatcher.group(2);
                handleAggregateFunction("SUM", column, alias, aggregates, subQueries, database);
            } else if (subQueryMatcher.matches()) {
                String subQueryStr = trimmedItem.substring(1, trimmedItem.lastIndexOf(")")).trim();
                String alias = subQueryMatcher.group(1);
                Query<?> subQuery = parse(subQueryStr, database);
                subQueries.add(new QueryParser.SubQuery(subQuery, alias));
                if (alias != null) {
                    columnAliases.put("SUBQUERY_" + subQueries.size(), alias);
                }
                LOGGER.log(Level.FINE, "Разобран подзапрос в SELECT: {0}{1}", new Object[]{subQueryStr, alias != null ? " AS " + alias : ""});
            } else if (columnMatcher.matches()) {
                String column = columnMatcher.group(1);
                String alias = columnMatcher.group(2);
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                }
                LOGGER.log(Level.FINE, "Разобран столбец: {0}{1}", new Object[]{column, alias != null ? " AS " + alias : ""});
            } else {
                throw new IllegalArgumentException("Недопустимый элемент SELECT: " + trimmedItem);
            }
        }

        return new QueryParser.SelectItems(columns, aggregates, subQueries, columnAliases);
    }

    private void handleAggregateFunction(String functionName, String arg, String alias,
                                         List<QueryParser.AggregateFunction> aggregates,
                                         List<QueryParser.SubQuery> subQueries, Database database) {
        if (arg.startsWith("(") && arg.endsWith(")")) {
            String subQueryStr = arg.substring(1, arg.length() - 1).trim();
            Query<?> subQuery = parse(subQueryStr, database);
            aggregates.add(new QueryParser.AggregateFunction(functionName, new QueryParser.SubQuery(subQuery, null), alias));
            LOGGER.log(Level.FINE, "Разобранная агрегатная функция: {0}(подзапрос){1}",
                    new Object[]{functionName, alias != null ? " AS " + alias : ""});
        } else {
            String column = arg.equals("*") ? null : arg;
            aggregates.add(new QueryParser.AggregateFunction(functionName, column, alias));
            LOGGER.log(Level.FINE, "Разобранная агрегатная функция: {0}({1}){2}",
                    new Object[]{functionName, column == null ? "*" : column, alias != null ? " AS " + alias : ""});
        }
    }

    private List<String> splitSelectItems(String selectPart) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        int parenDepth = 0;
        boolean inQuotes = false;

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

    private QueryParser.TableJoins parseTableAndJoins(String tableAndJoinsOriginal, Database database) {
        String tableAndJoins = normalizeQueryString(tableAndJoinsOriginal).trim();
        List<QueryParser.JoinInfo> joins = new ArrayList<>();
        String tableName;
        String tableAlias = null;
        Map<String, String> tableAliases = new HashMap<>();

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

        String mainTablePart = joinParts.get(0).trim();
        String[] mainTableTokens = mainTablePart.split("\\s+");
        tableName = mainTableTokens[0].trim();
        if (mainTableTokens.length > 1) {
            if (mainTableTokens.length == 3 && mainTableTokens[1].equalsIgnoreCase("AS")) {
                tableAlias = mainTableTokens[2].trim();
            } else if (mainTableTokens.length == 2) {
                tableAlias = mainTableTokens[1].trim();
            }
        }
        if (tableAlias != null) {
            tableAliases.put(tableAlias, tableName);
        }

        Table mainTable = database.getTable(tableName);
        if (mainTable == null) {
            throw new IllegalArgumentException("Таблица не найдена: " + tableName);
        }

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
        tableAliases.put(tableName, tableName);

        for (int i = 1; i < joinParts.size() - 1; i += 2) {
            String joinTypeStr = joinParts.get(i).toUpperCase();
            String joinPart = joinParts.get(i + 1).trim();
            QueryParser.JoinType joinType = parseJoinType(joinTypeStr);
            String joinTableName;
            String joinTableAlias = null;
            List<QueryParser.Condition> onConditions = new ArrayList<>();

            Pattern clausePattern = Pattern.compile("(?i)\\s+(WHERE|LIMIT|OFFSET|ORDER BY|GROUP BY|$)\\b");
            Matcher clauseMatcher = clausePattern.matcher(joinPart);
            String onClausePart = joinPart;
            if (clauseMatcher.find()) {
                onClausePart = joinPart.substring(0, clauseMatcher.start()).trim();
            }

            String[] joinTableTokens;
            if (joinType == QueryParser.JoinType.CROSS) {
                joinTableTokens = new String[]{joinPart.trim()};
            } else {
                int onIndex = findOnClausePosition(joinPart);
                if (onIndex == -1) {
                    throw new IllegalArgumentException("Недопустимый формат " + joinTypeStr + ": отсутствует ON");
                }
                String joinTablePart = joinPart.substring(0, onIndex).trim();
                String onClause = joinPart.substring(onIndex + 2).trim();
                joinTableTokens = new String[]{joinTablePart, onClause};
            }

            String joinTablePart = joinTableTokens[0].trim();
            String[] joinTableParts = joinTablePart.split("\\s+");
            joinTableName = joinTableParts[0].trim();
            if (joinTableParts.length > 1) {
                if (joinTableParts.length == 3 && joinTableParts[1].equalsIgnoreCase("AS")) {
                    joinTableAlias = joinTableParts[2].trim();
                } else if (joinTableParts.length == 2) {
                    joinTableAlias = joinTableParts[1].trim();
                }
            }
            if (joinTableAlias != null) {
                tableAliases.put(joinTableAlias, joinTableName);
            }

            Table joinTable = database.getTable(joinTableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Таблица соединения не найдена: " + joinTableName);
            }
            combinedColumnTypes.putAll(joinTable.getColumnTypes());
            tableAliases.put(joinTableName, joinTableName);

            if (joinType != QueryParser.JoinType.CROSS) {
                String onClause = joinTableTokens[1].trim();
                onConditions = parseConditions(onClause, tableName, database, tableAndJoinsOriginal, true,
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
            case "LEFT JOIN", "LEFT OUTER JOIN" -> QueryParser.JoinType.LEFT_OUTER;
            case "RIGHT JOIN", "RIGHT OUTER JOIN" -> QueryParser.JoinType.RIGHT_OUTER;
            case "FULL JOIN", "FULL OUTER JOIN" -> QueryParser.JoinType.FULL_OUTER;
            case "LEFT INNER JOIN" -> QueryParser.JoinType.LEFT_INNER;
            case "RIGHT INNER JOIN" -> QueryParser.JoinType.RIGHT_INNER;
            case "CROSS JOIN" -> QueryParser.JoinType.CROSS;
            default -> throw new IllegalArgumentException("Неподдерживаемый тип соединения: " + joinTypeStr);
        };
    }

    private int findOnClausePosition(String joinPart) {
        int parenDepth = 0;
        boolean inQuotes = false;
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
                    return i;
                }
            }
        }
        return -1;
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

    private QueryParser.AdditionalClauses parseAdditionalClauses(String tableAndJoinsOriginal, String tableName, Database database,
                                                                 String originalQuery, List<QueryParser.AggregateFunction> aggregates,
                                                                 Map<String, Class<?>> combinedColumnTypes,
                                                                 Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                                 List<QueryParser.SubQuery> subQueries) {
        String conditionStr = null;
        List<QueryParser.Condition> conditions = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<QueryParser.HavingCondition> havingConditions = new ArrayList<>();
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        Integer limit = null;
        Integer offset = null;

        int orderByIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "ORDER BY");
        if (orderByIndex != -1) {
            String beforeOrderBy = tableAndJoinsOriginal.substring(0, orderByIndex).trim();
            String orderByClause = tableAndJoinsOriginal.substring(orderByIndex + 8).trim();
            orderBy = parseOrderByClause(orderByClause, tableName, database, combinedColumnTypes, tableAliases, columnAliases, subQueries);
            tableAndJoinsOriginal = beforeOrderBy;
        }

        int limitIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "LIMIT");
        if (limitIndex != -1) {
            String beforeLimit = tableAndJoinsOriginal.substring(0, limitIndex).trim();
            String afterLimit = tableAndJoinsOriginal.substring(limitIndex + 5).trim();
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
            tableAndJoinsOriginal = beforeLimit;
        }

        int groupByIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "GROUP BY");
        if (groupByIndex != -1) {
            String beforeGroupBy = tableAndJoinsOriginal.substring(0, groupByIndex).trim();
            String groupByClause = tableAndJoinsOriginal.substring(groupByIndex + 8).trim();
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
            tableAndJoinsOriginal = beforeGroupBy;
        }

        int whereIndex = findClauseOutsideSubquery(tableAndJoinsOriginal, "WHERE");
        if (whereIndex != -1) {
            conditionStr = tableAndJoinsOriginal.substring(whereIndex + 5).trim();
            conditions = parseConditions(conditionStr, tableName, database, originalQuery, false,
                    combinedColumnTypes, tableAliases, columnAliases);
        }

        return new QueryParser.AdditionalClauses(conditions, groupBy, havingConditions, orderBy, limit, offset);
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        int parenDepth = 0;
        boolean inQuotes = false;
        Pattern clausePattern = Pattern.compile("(?i)\\b" + Pattern.quote(clause) + "\\b");
        Matcher matcher = clausePattern.matcher(query);
        while (matcher.find()) {
            int start = matcher.start();
            boolean insideSubquery = false;
            for (int i = 0; i < start; i++) {
                char c = query.charAt(i);
                if (c == '\'') {
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (c == '(') {
                        parenDepth++;
                    } else if (c == ')') {
                        parenDepth--;
                    }
                }
            }
            if (parenDepth == 0 && !inQuotes) {
                return start;
            }
        }
        return -1;
    }

    private List<QueryParser.OrderByInfo> parseOrderByClause(String orderByClause, String tableName, Database database,
                                                             Map<String, Class<?>> combinedColumnTypes,
                                                             Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                             List<QueryParser.SubQuery> subQueries) {
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        List<String> items = splitCommaSeparatedItems(orderByClause);
        Pattern columnPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*|\\(\\s*SELECT\\s+.*?\\))\\s*(ASC|DESC)?$", Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                String direction = columnMatcher.group(2);
                boolean ascending = direction == null || direction.equalsIgnoreCase("ASC");

                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains("SELECT")) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    Query<?> subQuery = parse(subQueryStr, database);
                    subQueries.add(new QueryParser.SubQuery(subQuery, null));
                    orderBy.add(new QueryParser.OrderByInfo("SUBQUERY_" + subQueries.size(), ascending));
                    LOGGER.log(Level.FINE, "Разобран подзапрос в ORDER BY: {0}", subQueryStr);
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, tableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    orderBy.add(new QueryParser.OrderByInfo(normalizedColumn, ascending));
                    LOGGER.log(Level.FINE, "Разобран столбец в ORDER BY: {0} {1}", new Object[]{normalizedColumn, direction});
                }
            } else {
                throw new IllegalArgumentException("Недопустимый элемент ORDER BY: " + trimmedItem);
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
                    Query<?> subQuery = parse(subQueryStr, database);
                    groupBy.add("SUBQUERY_" + System.currentTimeMillis()); // Уникальный идентификатор для подзапроса
                    LOGGER.log(Level.FINE, "Разобран подзапрос в GROUP BY: {0}", subQueryStr);
                } else {
                    String normalizedColumn = normalizeColumnName(columnOrSubQuery, tableName, tableAliases);
                    validateColumn(normalizedColumn, combinedColumnTypes);
                    groupBy.add(normalizedColumn);
                    LOGGER.log(Level.FINE, "Разобран столбец в GROUP BY: {0}", normalizedColumn);
                }
            } else {
                throw new IllegalArgumentException("Недопустимый элемент GROUP BY: " + trimmedItem);
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
        List<QueryParser.Token> tokens = tokenizeConditions(conditionStr);
        return parseTokenizedConditions(tokens, defaultTableName, database, originalQuery, isJoinCondition,
                combinedColumnTypes, tableAliases, columnAliases, null, false);
    }

    private List<QueryParser.Token> tokenizeConditions(String conditionStr) {
        List<Map.Entry<String, Pattern>> patterns = new ArrayList<>();
        patterns.add(Map.entry("Quoted String", Pattern.compile("(?i)'(?:\\\\.|[^'\\\\])*'")));
        patterns.add(Map.entry("Grouped Condition", Pattern.compile("(?i)\\((?:[^()']+|'(?:\\\\.|[^'\\\\])*')*\\)")));
        patterns.add(Map.entry("Like Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(NOT\\s*)?LIKE\\s*'(?:\\\\.|[^'\\\\])*'")));
        patterns.add(Map.entry("In Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(NOT\\s*)?IN\\s*\\((?:[^()']+|'(?:\\\\.|[^'\\\\])*')*\\)")));
        patterns.add(Map.entry("Null Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*IS\\s*(NOT\\s+)?NULL\\b")));
        patterns.add(Map.entry("Comparison String Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>)\\s*'(?:\\\\.|[^'\\\\])*'")));
        patterns.add(Map.entry("Comparison Number Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>)\\s*([0-9]+(?:\\.[0-9]+)?)")));
        patterns.add(Map.entry("Comparison Column Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>)\\s*([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)")));
        patterns.add(Map.entry("Subquery Condition",
                Pattern.compile("(?i)([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>|LIKE|NOT LIKE)\\s*\\(\\s*SELECT\\s+.*?\\s*\\)", Pattern.DOTALL)));
        patterns.add(Map.entry("Logical Operator", Pattern.compile("(?i)\\b(AND|OR)\\b")));

        List<QueryParser.Token> tokens = new ArrayList<>();
        int currentPos = 0;
        int stringLength = conditionStr.length();

        while (currentPos < stringLength) {
            while (currentPos < stringLength && Character.isWhitespace(conditionStr.charAt(currentPos))) {
                currentPos++;
            }
            if (currentPos >= stringLength) {
                break;
            }

            boolean matched = false;
            int nextPos = stringLength;
            String matchedToken = null;
            String matchedPatternName = null;

            for (Map.Entry<String, Pattern> entry : patterns) {
                String patternName = entry.getKey();
                Pattern pattern = entry.getValue();
                Matcher matcher = pattern.matcher(conditionStr).region(currentPos, stringLength);

                if (matcher.lookingAt()) {
                    String tokenValue = matcher.group().trim();
                    if (!tokenValue.isEmpty()) {
                        nextPos = matcher.end();
                        matchedToken = tokenValue;
                        matchedPatternName = patternName;
                        matched = true;
                        break;
                    }
                }
            }

            if (matched) {
                QueryParser.TokenType type = matchedPatternName.equals("Logical Operator") ?
                        QueryParser.TokenType.LOGICAL_OPERATOR : QueryParser.TokenType.CONDITION;
                tokens.add(new QueryParser.Token(type, matchedToken));
                LOGGER.log(Level.FINE, "Добавлен токен: {0}, тип: {1}", new Object[]{matchedToken, type});
                currentPos = nextPos;
            } else {
                currentPos++;
            }
        }

        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Невалидное условие: не удалось выделить токенов из '" + conditionStr + "'");
        }
        return tokens;
    }

    private List<QueryParser.Condition> parseTokenizedConditions(List<QueryParser.Token> tokens, String defaultTableName, Database database,
                                                                 String originalQuery, boolean isJoinCondition,
                                                                 Map<String, Class<?>> combinedColumnTypes,
                                                                 Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                                 String conjunction, boolean not) {
        List<QueryParser.Condition> conditions = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            QueryParser.Token token = tokens.get(i);
            if (token.type == QueryParser.TokenType.LOGICAL_OPERATOR) {
                conjunction = token.value.toUpperCase();
                continue;
            }
            if (token.type == QueryParser.TokenType.CONDITION) {
                String condStr = token.value;
                if (condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")")) {
                    String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
                    if (subCondStr.toUpperCase().startsWith("SELECT")) {
                        Query<?> subQuery = parse(subCondStr, database);
                        conditions.add(new QueryParser.Condition(defaultTableName + ".unknown", new QueryParser.SubQuery(subQuery, null), conjunction, not));
                    } else {
                        List<QueryParser.Token> subTokens = tokenizeConditions(subCondStr);
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
        }
        return conditions;
    }

    private QueryParser.Condition parseInCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                                   Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                                                   Map<String, String> columnAliases, String conjunction, boolean not) {
        Pattern inPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+(NOT\\s+)?IN\\s*\\((.*?)\\)$", Pattern.DOTALL);
        Matcher inMatcher = inPattern.matcher(condStr);
        if (!inMatcher.matches()) {
            throw new IllegalArgumentException("Invalid IN condition format: " + condStr);
        }

        String column = inMatcher.group(1).trim();
        boolean inNot = inMatcher.group(2) != null;
        String valuesStr = inMatcher.group(3).trim();
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
        Class<?> columnType = getColumnType(normalizedColumn, combinedColumnTypes);

        LOGGER.log(Level.FINEST, "Parsing IN condition: column={0}, values={1}, not={2}", new Object[]{normalizedColumn, valuesStr, inNot});

        if (valuesStr.toUpperCase().startsWith("SELECT")) {
            String subQueryStr = valuesStr;
            if (subQueryStr.startsWith("(") && subQueryStr.endsWith(")")) {
                subQueryStr = subQueryStr.substring(1, subQueryStr.length() - 1).trim();
            }
            LOGGER.log(Level.FINEST, "Parsing subquery for IN condition: {0}", subQueryStr);
            Query<?> subQuery = parse(subQueryStr, database);
            QueryParser.SubQuery subQueryObj = new QueryParser.SubQuery(subQuery, null);
            LOGGER.log(Level.FINE, "Parsed IN subquery condition: {0} {1}IN (subquery)", new Object[]{normalizedColumn, inNot ? "NOT " : ""});
            return new QueryParser.Condition(normalizedColumn, subQueryObj, conjunction, inNot);
        }

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
        Pattern subQueryPattern = Pattern.compile(
                "(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(=|>|<|>=|<=|!=|<>|LIKE|NOT LIKE)\\s*\\((SELECT\\s+.*?)\\)$",
                Pattern.DOTALL
        );
        Matcher subQueryMatcher = subQueryPattern.matcher(condStr);
        if (!subQueryMatcher.matches()) {
            throw new IllegalArgumentException("Invalid subquery condition format: " + condStr);
        }

        String column = subQueryMatcher.group(1).trim();
        String operatorStr = subQueryMatcher.group(2).trim();
        String subQueryStr = subQueryMatcher.group(3).trim();
        Query<?> subQuery = parse(subQueryStr, database);
        QueryParser.SubQuery newSubQuery = new QueryParser.SubQuery(subQuery, null);
        QueryParser.Operator operator = parseOperator(operatorStr);
        String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
        validateColumn(normalizedColumn, combinedColumnTypes);
        return new QueryParser.Condition(normalizedColumn, newSubQuery, operator, conjunction, not);
    }

    // Обновленный parseSingleCondition
    private QueryParser.Condition parseSingleCondition(String condStr, String defaultTableName, Database database, String originalQuery,
                                                       boolean isJoinCondition, Map<String, Class<?>> combinedColumnTypes,
                                                       Map<String, String> tableAliases, Map<String, String> columnAliases,
                                                       String conjunction, boolean not) {
        LOGGER.log(Level.FINEST, "Parsing single condition: {0}", condStr);

        // Проверка на LIKE
        Pattern likePattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s*(LIKE|NOT LIKE)\\s*('(?:[^']|)*')");
        Matcher likeMatcher = likePattern.matcher(condStr);
        if (likeMatcher.matches()) {
            String column = likeMatcher.group(1).trim();
            String operatorStr = likeMatcher.group(2).toUpperCase();
            String value = likeMatcher.group(3).substring(1, likeMatcher.group(3).length() - 1);
            String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
            validateColumn(normalizedColumn, combinedColumnTypes);
            QueryParser.Operator operator = operatorStr.equals("LIKE") ? QueryParser.Operator.LIKE : QueryParser.Operator.NOT_LIKE;
            Object parsedValue = parseConditionValue(normalizedColumn, "'" + value + "'", getColumnType(normalizedColumn, combinedColumnTypes));
            LOGGER.log(Level.FINE, "Parsed LIKE condition: {0} {1} '{2}'", new Object[]{normalizedColumn, operatorStr, value});
            return new QueryParser.Condition(normalizedColumn, parsedValue, operator, conjunction, not);
        }

        // Проверка на IS NULL
        Pattern isNullPattern = Pattern.compile("(?i)^([a-zA-Z_][a-zA-Z0-9_]*(?:\\.[a-zA-Z_][a-zA-Z0-9_]*)*)\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(condStr);
        if (isNullMatcher.matches()) {
            String column = isNullMatcher.group(1).trim();
            boolean isNotNull = isNullMatcher.group(2) != null;
            String normalizedColumn = normalizeColumnName(column, defaultTableName, tableAliases);
            validateColumn(normalizedColumn, combinedColumnTypes);
            QueryParser.Operator operator = isNotNull ? QueryParser.Operator.IS_NOT_NULL : QueryParser.Operator.IS_NULL;
            LOGGER.log(Level.FINE, "Parsed NULL condition: {0} {1}", new Object[]{normalizedColumn, operator});
            return new QueryParser.Condition(normalizedColumn, operator, conjunction, not);
        }

        // Проверка на сравнение
        String[] operators = {"!=", "<>", ">=", "<=", "=", "<", ">", "\\bLIKE\\b", "\\bNOT LIKE\\b"};
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
        LOGGER.log(Level.FINEST, "Operator parsed: {0}, string representation: {1}", new Object[]{operator, operatorInfo.operator});

        if (isJoinCondition && !rightColumnIsFromDifferentTable(normalizedColumn, rightColumn, tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + condStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, defaultTableName, tableAliases);
            validateColumn(normalizedRightColumn, combinedColumnTypes);
            LOGGER.log(Level.FINE, "Parsed column comparison: {0} {1} {2}", new Object[]{normalizedColumn, operatorInfo.operator, normalizedRightColumn});
            return new QueryParser.Condition(normalizedColumn, normalizedRightColumn, operator, conjunction, not);
        } else {
            LOGGER.log(Level.FINE, "Parsed value comparison: {0} {1} {2}", new Object[]{normalizedColumn, operatorInfo.operator, value});
            return new QueryParser.Condition(normalizedColumn, value, operator, conjunction, not);
        }
    }

    // Переопределяем parseOperator для возврата Operator
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

    // Новый метод для получения строкового представления оператора
    private String getOperatorString(QueryParser.Operator operator) {
        return switch (operator) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case LESS_THAN -> "<";
            case GREATER_THAN -> ">"; // Возвращаем ">" вместо "GREATER_THAN"
            case LESS_THAN_OR_EQUALS -> "<=";
            case GREATER_THAN_OR_EQUALS -> ">=";
            case LIKE -> "LIKE";
            case NOT_LIKE -> "NOT LIKE";
            case IS_NULL -> "IS NULL";
            case IS_NOT_NULL -> "IS NOT NULL";
            default -> throw new IllegalArgumentException("Unsupported operator: " + operator);
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
                        String patternStr = op.startsWith("\\b") ? "\\b" + op.substring(2, op.length() - 2) + "\\b" : Pattern.quote(op);
                        Pattern opPattern = Pattern.compile("(?i)" + patternStr);
                        Matcher opMatcher = opPattern.matcher(condStr).region(i, condStr.length());
                        if (opMatcher.lookingAt()) {
                            return new QueryParser.OperatorInfo(opMatcher.group().trim(), i, i + opMatcher.group().length());
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
        if (condStr.toUpperCase().startsWith("(") && condStr.toUpperCase().endsWith(")")) {
            String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
            List<QueryParser.HavingCondition> subConditions = parseHavingConditions(subCondStr, defaultTableName, database, originalQuery,
                    aggregates, combinedColumnTypes, tableAliases, columnAliases);
            return new QueryParser.HavingCondition(subConditions, conjunction, not);
        }

        String[] operators = {"=", "!=", "<>", ">=", "<=", "<", ">"};
        String selectedOperator = null;
        int operatorIndex = -1;
        for (String op : operators) {
            Pattern opPattern = Pattern.compile("(?i)\\s+" + Pattern.quote(op) + "\\s+");
            Matcher opMatcher = opPattern.matcher(" " + condStr + " ");
            if (opMatcher.find()) {
                selectedOperator = op;
                operatorIndex = opMatcher.start();
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
                    Query<?> subQuery = parse(subQueryStr, database);
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

        QueryParser.Operator operator = switch (selectedOperator) {
            case "=" -> QueryParser.Operator.EQUALS;
            case "!=", "<>" -> QueryParser.Operator.NOT_EQUALS;
            case "<" -> QueryParser.Operator.LESS_THAN;
            case ">" -> QueryParser.Operator.GREATER_THAN;
            case "<=" -> QueryParser.Operator.LESS_THAN_OR_EQUALS;
            case ">=" -> QueryParser.Operator.GREATER_THAN_OR_EQUALS;
            default -> throw new IllegalArgumentException("Unsupported operator in HAVING: " + selectedOperator);
        };

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
    protected String normalizeQueryString(String query) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        return query.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*([=><!(),])\\s*", "$1")
                .replaceAll("\\s*;", "")
                .replaceAll("(?i)\\bLIMIT\\s*(\\d+)\\b", " LIMIT $1 ")
                .replaceAll("(?i)\\bWHERE\\b", " WHERE ")
                .replaceAll("(?i)\\bFROM\\b", " FROM ")
                .replaceAll("(?i)\\bSELECT\\b", " SELECT ")
                .replaceAll("(?i)\\bAS\\b", " AS ")
                .replaceAll("\\(\\s+", "(")
                .replaceAll("\\s+\\)", ")")
                .toUpperCase();
        // Убраны замены EQUALS, NOT_EQUALS, GREATER_THAN, чтобы сохранить >, <, =
    }

    // Вспомогательный метод для проверки, является ли подстрока оператором сравнения
    private boolean isComparisonOperator(String substring) {
        String[] operators = {"=", ">", "<", ">=", "<=", "!=", "<>"};
        for (String op : operators) {
            if (substring.startsWith(op)) {
                return true;
            }
        }
        return false;
    }

    // Вспомогательный метод для извлечения оператора сравнения
    private String extractOperator(String substring) {
        String[] operators = {"!=", "<>", ">=", "<=", "=", ">", "<"}; // Порядок важен: сначала двухсимвольные операторы
        for (String op : operators) {
            if (substring.startsWith(op)) {
                return op;
            }
        }
        return "";
    }
}
