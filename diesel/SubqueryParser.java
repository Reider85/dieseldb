package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for queries containing subqueries.
 *
 * <p>It detects whether a query contains a subquery and, when it does, parses
 * the whole statement by wrapping the inner {@link QueryParser} results into
 * executable {@link Query} objects (scalar subqueries in the SELECT/WHERE/
 * HAVING clauses and IN-lists, correlated and non-correlated). Queries without
 * subqueries are delegated to {@link QueryParser} unchanged.
 *
 * @see QueryParser
 * @see Query
 */
public class SubqueryParser {
    private static final Logger LOGGER = Logger.getLogger(SubqueryParser.class.getName());
    private static final String QUOTED_IDENTIFIER_PATTERN = "\"[^\"]*\"";
    private static final String SIMPLE_IDENTIFIER_PATTERN = "[a-z_]\\w*";
    private static final String IDENTIFIER_PATTERN = "(?:" + QUOTED_IDENTIFIER_PATTERN + "|" + SIMPLE_IDENTIFIER_PATTERN + ")";
    private static final String QUALIFIED_IDENTIFIER_PATTERN = IDENTIFIER_PATTERN + "(?:\\." + IDENTIFIER_PATTERN + ")*+";

    // Prompt 14 (java:S5843): the first three probe patterns are replaced by String
    // methods (startsWith/contains) in isInSubqueryPattern(). Only the anchored
    // LIMIT/OFFSET tail remains a regex (it is not a start/contain check).
    private static final Pattern IN_SUBQUERY_TAIL = Pattern.compile(
            "(?i)(?:\\s+LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?)?$", Pattern.DOTALL);

    private final QueryParser queryParser;

    /**
     * Creates a subquery parser backed by an internal {@link QueryParser}.
     */
    public SubqueryParser() {
        this.queryParser = new QueryParser();
    }

    private static String unquoteIdentifier(String identifier) {
        return SqlParsingUtils.unquoteIdentifier(identifier);
    }

    private static String unquoteQualifiedIdentifier(String identifier) {
        return SqlParsingUtils.unquoteQualifiedIdentifier(identifier);
    }

    /**
     * Parses a query, handling its subqueries when present.
     *
     * @param query    the SQL query to parse
     * @param database the database the query will run against
     * @return the parsed query object
     * @throws IllegalArgumentException if the query is unsupported
     */
    public Query<?> parse(String query, Database database) {
        // Prompt 22 (java:S2259): a null query would NPE inside the delegated
        // QueryParser.parse call; reject it here with the same contract.
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        LOGGER.log(Level.INFO, "Parsing query: {0}", query);
        String normalizedQuery = normalizeQueryString(query).trim();

        if (!containsSubquery(normalizedQuery)) {
            LOGGER.log(Level.FINE, "No subqueries found, delegating to QueryParser: {0}", query);
            return queryParser.parse(query, database);
        }

        LOGGER.log(Level.FINE, "Subqueries detected, parsing with SubqueryParser: {0}", query);
        try {
            if (normalizedQuery.toUpperCase().startsWith(SqlKeywords.SELECT)) {
                // Проверяем, является ли весь запрос SELECT ... WHERE ... IN (SELECT ...)
                if (isInSubqueryPattern(normalizedQuery)) {
                    LOGGER.log(Level.FINE, "Detected IN-subquery pattern, using custom parsing");
                    return parseSelectQuery(query, database);
                }
                return parseSelectQuery(query, database);
            } else {
                LOGGER.log(Level.FINE, "Non-SELECT query, delegating to QueryParser: {0}", query);
                return queryParser.parse(query, database);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    /**
     * Returns whether the whole query matches the shape
     * {@code SELECT ... WHERE ... IN (SELECT ...)} with an optional trailing
     * {@code LIMIT [OFFSET]}.
     *
     * <p>Prompt 12/14 (java:S5843): the original monolithic regex had complexity 46.
     * It is decomposed here into simple probes — the first three are now String
     * methods (startsWith/contains) with only the anchored LIMIT/OFFSET tail as a regex.
     *
     * @param query the normalized SQL query to inspect
     * @return true when the query is a SELECT with an IN-subquery in its WHERE
     */
    private static boolean isInSubqueryPattern(String query) {
        // Prompt 14 (java:S5843): replace the three regex probes with equivalent
        // String methods. The query is already whitespace-normalized, so the
        // single-space strings match the original \s+ boundaries exactly.
        String uq = query.toUpperCase();
        return uq.startsWith(SqlKeywords.SELECT + " ")
                && uq.contains(SqlKeywords.WHERE + " ")
                && uq.contains("IN (SELECT")
                && IN_SUBQUERY_TAIL.matcher(query).matches();
    }

    /**
     * Returns whether the query contains a subquery, detected by the presence
     * of an opening parenthesis followed by the SELECT keyword.
     *
     * @param query the SQL query to inspect
     * @return true when the query contains a {@code (SELECT ...)} subquery
     */
    public boolean containsSubquery(String query) {
        // Prompt 22 (java:S2259): a null query cannot contain a subquery.
        if (query == null) {
            return false;
        }
        Pattern subqueryPattern = Pattern.compile("(?i)\\(\\s*SELECT\\b", Pattern.DOTALL);
        Matcher matcher = subqueryPattern.matcher(query);
        return matcher.find();
    }

    private String normalizeQueryString(String query) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        String normalized = query.trim()
                .replaceAll("\\s+", " ")
                .replaceAll("([=><!])", " $1 ")
                .replaceAll("(?i)\\s*\\(\\s*SELECT\\b", " (SELECT")
                .replaceAll("(?i)(\\))\\s*(LIMIT|WHERE|ORDER\\s+BY|GROUP\\s+BY|HAVING|AS|INNER\\s+JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|FULL\\s+JOIN|CROSS\\s+JOIN)", "$1 $2")
                .replaceAll("\\s*;", "")
                .replaceAll("\\s+", " ");
        LOGGER.log(Level.FINEST, "Normalized query: {0}", normalized);
        return normalized;
    }

    private Query<List<Map<String, Object>>> parseSelectQuery(String originalQuery, Database database) {
        int fromIndex = findMainFromClause(originalQuery);
        if (fromIndex == -1) {
            throw new IllegalArgumentException("Invalid SELECT query: missing FROM clause");
        }

        String selectPartOriginal = originalQuery.substring(originalQuery.toUpperCase().indexOf(SqlKeywords.SELECT) + 6, fromIndex).trim();
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

        ParseContext ctx = new ParseContext(tableName, database, originalQuery, false,
                combinedColumnTypes, tableAliases, columnAliases);
        QueryParser.AdditionalClauses clauses = parseAdditionalClauses(tableAndJoinsOriginal, ctx, aggregates, subQueries);
        List<QueryParser.Condition> conditions = clauses.conditions;
        List<String> groupBy = clauses.groupBy;
        List<QueryParser.HavingCondition> havingConditions = clauses.havingConditions;
        List<QueryParser.OrderByInfo> orderBy = clauses.orderBy;
        Integer limit = clauses.limit;
        Integer offset = clauses.offset;

        LOGGER.log(Level.INFO, "Parsed SELECT query: table={0}, columns={1}, aggregates={2}, joins={3}, conditions={4}",
                new Object[]{tableName, columns, aggregates, joins, conditions});

        SelectQuery selectQuery = SelectQuery.builder()
                .tableName(tableName)
                .tableAlias(tableAlias)
                .columns(columns)
                .aggregates(aggregates)
                .joins(joins)
                .conditions(conditions)
                .groupBy(groupBy)
                .havingConditions(havingConditions)
                .orderBy(orderBy)
                .limit(limit)
                .offset(offset)
                .tableAliases(columnAliases)
                .extraTableAliases(tableAliases)
                .columnTypes(combinedColumnTypes)
                .groupBySubQueries(clauses.groupBySubQueries)
                .build();
        selectQuery.setDerivedMainTable(tableJoins.derivedMainTable);
        return selectQuery;
    }

    private int trySkipStringOrIdentifierLiteral(String query, int currentPos) {
        Matcher quotedStringMatcher = Pattern.compile("'(?:\\\\.|[^'\\\\])*+'").matcher(query).region(currentPos, query.length());
        if (quotedStringMatcher.lookingAt()) {
            return quotedStringMatcher.end();
        }
        Matcher quotedIdentifierMatcher = Pattern.compile("\"[^\"]*\"").matcher(query).region(currentPos, query.length());
        if (quotedIdentifierMatcher.lookingAt()) {
            return quotedIdentifierMatcher.end();
        }
        return -1;
    }

    private int trySkipSubqueryClause(String query, int currentPos) {
        Matcher subqueryMatcher = Pattern.compile("\\(\\s*SELECT\\b", Pattern.DOTALL).matcher(query).region(currentPos, query.length());
        if (subqueryMatcher.lookingAt()) {
            int endPos = findMatchingClosingParen(query, currentPos + 1);
            if (endPos == -1) {
                return -2;
            }
            return endPos + 1;
        }
        return -1;
    }

    private int findMainFromClause(String query) {
        int bracketDepth = 0;
        int currentPos = 0;
        boolean inQuotes = false;
        Pattern fromPattern = Pattern.compile("(?i)\\bFROM\\b");

        while (currentPos < query.length()) {
            int literalEnd = trySkipStringOrIdentifierLiteral(query, currentPos);
            if (literalEnd != -1) {
                currentPos = literalEnd;
            } else if (query.charAt(currentPos) == '\'') {
                inQuotes = !inQuotes;
                currentPos++;
            } else if (inQuotes) {
                currentPos++;
            } else {
                int subqueryEnd = trySkipSubqueryClause(query, currentPos);
                if (subqueryEnd == -2) {
                    return -1;
                }
                if (subqueryEnd != -1) {
                    currentPos = subqueryEnd;
                } else if (query.charAt(currentPos) == '(') {
                    bracketDepth++;
                    currentPos++;
                } else if (query.charAt(currentPos) == ')') {
                    bracketDepth--;
                    if (bracketDepth < 0) {
                        return -1;
                    }
                    currentPos++;
                } else if (bracketDepth == 0) {
                    if (fromPattern.matcher(query).region(currentPos, query.length()).lookingAt()) {
                        return currentPos;
                    }
                    currentPos++;
                } else {
                    currentPos++;
                }
            }
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

        Pattern columnPattern = Pattern.compile(ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$");
        Pattern subQueryPattern = Pattern.compile("(?i)^\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\)(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$", Pattern.DOTALL);
        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*+\\(\\s*+(" + QUALIFIED_IDENTIFIER_PATTERN + "|\\*|\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\))\\s*+\\)(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$", Pattern.DOTALL);
        Pattern starPattern = Pattern.compile("^\\*$");

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            Matcher subQueryMatcher = subQueryPattern.matcher(trimmedItem);
            Matcher aggMatcher = aggPattern.matcher(trimmedItem);

            if (subQueryMatcher.matches()) {
                String subQueryStr = trimmedItem.substring(1, trimmedItem.lastIndexOf(")")).trim();
                String alias = unquoteIdentifier(subQueryMatcher.group(1));
                validateSubQuery(subQueryStr);
                Query<?> subQuery = queryParser.parse(subQueryStr, database);
                subQueries.add(new QueryParser.SubQuery(subQuery, alias));
                if (alias != null) {
                    columnAliases.put(ErrorMessages.SUBQUERY_PREFIX + subQueries.size(), alias);
                }
                LOGGER.log(Level.FINE, "Parsed subquery in SELECT: {0}{1}", new Object[]{subQueryStr, alias != null ? " AS " + alias : ""});
            } else if (aggMatcher.matches()) {
                String funcName = aggMatcher.group(1);
                String arg = aggMatcher.group(2);
                String alias = unquoteIdentifier(aggMatcher.group(3));
                if (arg.toUpperCase().startsWith("(") && arg.toUpperCase().contains(SqlKeywords.SELECT)) {
                    String subQueryStr = arg.substring(1, arg.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    aggregates.add(new QueryParser.AggregateFunction(funcName, new QueryParser.SubQuery(subQuery, null), alias));
                } else {
                    String column = arg.equals("*") ? null : unquoteQualifiedIdentifier(arg);
                    aggregates.add(new QueryParser.AggregateFunction(funcName, column, alias));
                }
                LOGGER.log(Level.FINE, "Parsed aggregate: {0}({1}){2}", new Object[]{funcName, arg, alias != null ? " AS " + alias : ""});
            } else if (columnMatcher.matches()) {
                String column = unquoteQualifiedIdentifier(columnMatcher.group(1));
                String alias = unquoteIdentifier(columnMatcher.group(2));
                columns.add(column);
                if (alias != null) {
                    columnAliases.put(column, alias);
                }
                LOGGER.log(Level.FINE, "Parsed column: {0}{1}", new Object[]{column, alias != null ? " AS " + alias : ""});
            } else if (starPattern.matcher(trimmedItem).matches()) {
                columns.add("*");
                LOGGER.log(Level.FINE, "Parsed column: *");
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
        Table derivedMainTable = null;
        Map<String, Class<?>> combinedColumnTypes;

        if (isDerivedTablePart(mainTablePart)) {
            String[] derived = parseDerivedTablePart(mainTablePart);
            String subQueryStr = derived[0];
            String alias = derived[1];
            LOGGER.log(Level.INFO, "Parsed derived main table: subquery={0}, alias={1}", new Object[]{subQueryStr, alias});
            Table virtualTable = materializeDerivedTable(subQueryStr, alias, database);
            tableName = alias != null ? alias : virtualTable.getName();
            tableAlias = tableName;
            tableAliases.put(tableName, tableName);
            combinedColumnTypes = new HashMap<>(virtualTable.getColumnTypes());
            derivedMainTable = virtualTable;
        } else {
            String[] mainTableTokens = mainTablePart.split("\\s+");
            tableName = unquoteIdentifier(mainTableTokens[0].trim());
            if (mainTableTokens.length > 1) {
                tableAlias = unquoteIdentifier(mainTableTokens[mainTableTokens.length - 1].trim());
            }
            if (tableAlias != null) {
                tableAliases.put(tableAlias, tableName);
            }

            Table mainTable = database.getTable(tableName);
            if (mainTable == null) {
                throw new IllegalArgumentException(ErrorMessages.TABLE_NOT_FOUND_PREFIX + tableName);
            }

            combinedColumnTypes = new HashMap<>(mainTable.getColumnTypes());
            tableAliases.put(tableName, tableName);
        }

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
            String onClause = extractOnClause(joinPart);
            LOGGER.log(Level.FINEST, "Extracted onClause: {0}", onClause);

            String[] joinTableTokens = joinTablePart.split("\\s+");
            joinTableName = unquoteIdentifier(joinTableTokens[0].trim());
            if (joinTableTokens.length > 1) {
                joinTableAlias = unquoteIdentifier(joinTableTokens[joinTableTokens.length - 1].trim());
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
                // Передаём ON-клаузу целиком
                onConditions = parseConditions(onClause, new ParseContext(tableName, database, joinPart, true,
                        combinedColumnTypes, tableAliases, new HashMap<>()));
                for (QueryParser.Condition cond : onConditions) {
                    validateJoinCondition(cond, tableName, joinTableName, tableAliases);
                }
            }

            joins.add(new QueryParser.JoinInfo(tableName, joinTableName, joinTableAlias, null, null, joinType, onConditions));
            tableName = joinTableName;
        }

        return new QueryParser.TableJoins(tableName, tableAlias, joins, tableAliases, combinedColumnTypes, derivedMainTable);
    }

    /**
     * Returns whether the main-table part of the FROM clause is a derived
     * table, i.e. a parenthesized {@code (SELECT ...)} expression.
     *
     * @param mainTablePart the trimmed first FROM element
     * @return true when the element opens with a parenthesized SELECT
     */
    private boolean isDerivedTablePart(String mainTablePart) {
        String trimmed = mainTablePart.trim();
        return trimmed.startsWith("(") && trimmed.substring(1).trim().toUpperCase().startsWith(SqlKeywords.SELECT);
    }

    /**
     * Splits a derived-table FROM element into the inner subquery text and its
     * alias (or null when no alias was given).
     *
     * @param mainTablePart the trimmed first FROM element
     * @return a two-element array {@code [subQuery, alias]}
     */
    private String[] parseDerivedTablePart(String mainTablePart) {
        int closeParen = findMatchingClosingParen(mainTablePart, 1);
        if (closeParen == -1) {
            throw new IllegalArgumentException("Invalid derived table: unbalanced parentheses in " + mainTablePart);
        }
        String subQuery = mainTablePart.substring(1, closeParen).trim();
        String rest = mainTablePart.substring(closeParen + 1).trim();
        String alias = null;
        if (!rest.isEmpty()) {
            String[] tokens = rest.split("\\s+");
            if (tokens[0].equalsIgnoreCase(SqlKeywords.AS) && tokens.length > 1) {
                alias = unquoteIdentifier(tokens[1]);
            } else {
                alias = unquoteIdentifier(tokens[tokens.length - 1]);
            }
        }
        return new String[]{subQuery, alias};
    }

    /**
     * Executes a derived-table subquery and materializes its result into an
     * in-memory virtual table that the outer query can scan like a real table.
     * Column names come from the result rows; column types are inferred from
     * the first non-null value of each column.
     *
     * @param subQuery the inner SELECT text
     * @param alias    the derived table alias, or null
     * @param database the owning database
     * @return the populated virtual table
     */
    private Table materializeDerivedTable(String subQuery, String alias, Database database) {
        Object result = database.executeQuery(subQuery, null);
        if (!(result instanceof List<?> rawRows)) {
            throw new IllegalArgumentException("Derived table subquery must return a row set: " + subQuery);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        Map<String, Class<?>> columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Object raw : rawRows) {
            if (!(raw instanceof Map<?, ?> m)) {
                throw new IllegalArgumentException("Derived table subquery returned a non-row result: " + subQuery);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) m;
            rows.add(row);
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                Class<?> type = columnTypes.get(entry.getKey());
                if (type == null && entry.getValue() != null) {
                    columnTypes.put(entry.getKey(), entry.getValue().getClass());
                }
            }
        }

        String tableName = alias != null ? alias : "DERIVED_" + Math.abs(subQuery.hashCode());
        List<String> columns = new ArrayList<>();
        if (!rows.isEmpty()) {
            columns.addAll(rows.get(0).keySet());
        }
        for (String column : new ArrayList<>(columns)) {
            if (!columnTypes.containsKey(column)) {
                columnTypes.put(column, String.class);
            }
        }
        for (String column : new ArrayList<>(columnTypes.keySet())) {
            if (!columns.contains(column)) {
                columns.add(column);
            }
        }

        Table virtualTable = new Table(database, tableName, columns, columnTypes, null, null);
        for (Map<String, Object> row : rows) {
            virtualTable.addRow(row);
        }
        LOGGER.log(Level.INFO, "Materialized derived table {0} with {1} rows and columns {2}",
                new Object[]{tableName, rows.size(), columns});
        return virtualTable;
    }

    private QueryParser.JoinType parseJoinType(String joinTypeStr) {
        return switch (joinTypeStr.toUpperCase()) {
            case SqlKeywords.JOIN, SqlKeywords.INNER_JOIN -> QueryParser.JoinType.INNER;
            case SqlKeywords.LEFT_JOIN -> QueryParser.JoinType.LEFT_OUTER;
            case SqlKeywords.RIGHT_JOIN -> QueryParser.JoinType.RIGHT_OUTER;
            case SqlKeywords.FULL_JOIN -> QueryParser.JoinType.FULL_OUTER;
            case SqlKeywords.CROSS_JOIN -> QueryParser.JoinType.CROSS;
            default -> throw new IllegalArgumentException("Unsupported join type: " + joinTypeStr);
        };
    }

    private int findOnClausePosition(String joinPart) {
        int parenDepth = 0;
        boolean inQuotes = false;
        int onIndex = -1;

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
                        joinPart.substring(i, i + 2).toUpperCase().equals(SqlKeywords.ON)) {
                    onIndex = i;
                    i += 2;
                }
            }
        }
        return onIndex;
    }

    private String extractOnClause(String joinPart) {
        int onIndex = findOnClausePosition(joinPart);
        if (onIndex == -1) {
            return "";
        }
        int parenDepth = 0;
        boolean inQuotes = false;
        int endIndex = joinPart.length();

        Pattern clausePattern = Pattern.compile("(?i)\\b(WHERE|GROUP\\s+BY|ORDER\\s+BY|LIMIT)\\b");
        for (int i = onIndex + 2; i < joinPart.length(); i++) {
            char c = joinPart.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (parenDepth == 0) {
                    Matcher clauseMatcher = clausePattern.matcher(joinPart).region(i, joinPart.length());
                    if (clauseMatcher.lookingAt()) {
                        endIndex = i;
                        break;
                    }
                }
            }
        }
        return joinPart.substring(onIndex + 2, endIndex).trim();
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
        } else if (cond.subQuery != null) {
            // Разрешаем подзапросы в ON-условиях
            String leftPrefix = cond.column.contains(".") ? cond.column.split("\\.")[0] : null;
            String leftTableName = tableAliases.getOrDefault(leftPrefix, leftPrefix);
            if (!leftTableName.equals(leftTable) && !leftTableName.equals(rightTable)) {
                throw new IllegalArgumentException("Subquery condition must reference column from " + leftTable + " or " + rightTable + ": " + cond);
            }
        } else {
            // Разрешаем другие типы условий (например, сравнение с константой)
            LOGGER.log(Level.FINEST, "Allowing non-column condition type: {0}", cond);
        }
    }

    private List<QueryParser.Condition> processWhereClause(String tableAndJoins, int whereIndex,
                                                              int groupByIndex, int orderByIndex, int limitIndex,
                                                              ParseContext ctx) {
        int[] clauseIndices = {groupByIndex, orderByIndex, limitIndex};
        int whereEndIndex = tableAndJoins.length();
        for (int idx : clauseIndices) {
            if (idx != -1 && idx > whereIndex && idx < whereEndIndex) {
                whereEndIndex = idx;
            }
        }
        String whereClause = tableAndJoins.substring(whereIndex, whereEndIndex).trim();
        Pattern wherePattern = Pattern.compile("(?i)^WHERE\\s+");
        Matcher whereMatcher = wherePattern.matcher(whereClause);
        if (!whereMatcher.find()) {
            LOGGER.log(Level.WARNING, "WHERE clause not found in substring: {0}", whereClause);
            return new ArrayList<>();
        }
        String conditionStr = whereClause.substring(whereMatcher.end()).trim();
        LOGGER.log(Level.FINEST, "Raw extracted conditionStr: {0}", conditionStr);
        Pattern limitPattern = Pattern.compile("(?i)\\s*LIMIT\\s+\\d+(?:\\s+OFFSET\\s+\\d+)?\\s*$", Pattern.DOTALL);
        conditionStr = limitPattern.matcher(conditionStr).replaceAll("").trim();
        LOGGER.log(Level.FINEST, "After removing LIMIT: {0}", conditionStr);
        conditionStr = conditionStr.replaceFirst("(?i)^\\s*WHERE\\s+", "").trim();
        LOGGER.log(Level.FINEST, "After removing WHERE: {0}", conditionStr);
        if (conditionStr.isEmpty()) {
            return new ArrayList<>();
        }
        LOGGER.log(Level.FINEST, "Extracted WHERE condition: {0}", conditionStr);
        return parseConditions(conditionStr, ctx);
    }

    private void processGroupByAndHaving(String tableAndJoins, int groupByIndex, int orderByIndex,
                                              int limitIndex, ParseContext ctx,
                                              List<QueryParser.AggregateFunction> aggregates,
                                              Map<String, String> groupBySubQueries,
                                              List<String> groupBy,
                                              List<QueryParser.HavingCondition> havingConditions) {
        int groupByEndIndex = tableAndJoins.length();
        int[] clauseIndices = {orderByIndex, limitIndex};
        for (int idx : clauseIndices) {
            if (idx != -1 && idx > groupByIndex && idx < groupByEndIndex) {
                groupByEndIndex = idx;
            }
        }
        String groupByClause = tableAndJoins.substring(groupByIndex + 8, groupByEndIndex).trim();
        int havingIndex = findClauseOutsideSubquery(groupByClause, SqlKeywords.HAVING);
        String havingClause = null;
        if (havingIndex != -1) {
            havingClause = groupByClause.substring(havingIndex + 6).trim();
            groupByClause = groupByClause.substring(0, havingIndex).trim();
        }
        groupBy.addAll(parseGroupByClause(groupByClause, ctx.defaultTableName, ctx.database, ctx.combinedColumnTypes,
                ctx.tableAliases, groupBySubQueries));
        if (havingClause != null) {
            havingConditions.addAll(parseHavingConditions(havingClause, ctx, aggregates));
        }
    }

    private List<QueryParser.OrderByInfo> processOrderByClause(String tableAndJoins, int orderByIndex,
                                                                    int limitIndex, ParseContext ctx,
                                                                    List<QueryParser.SubQuery> subQueries) {
        int orderByEndIndex = limitIndex != -1 ? limitIndex : tableAndJoins.length();
        String orderByClause = tableAndJoins.substring(orderByIndex + 8, orderByEndIndex).trim();
        Pattern limitPattern = Pattern.compile("(?i)\\s*LIMIT\\s+\\d+(\\s+OFFSET\\s+\\d+)?\\s*$", Pattern.DOTALL);
        orderByClause = limitPattern.matcher(orderByClause).replaceAll("");
        return parseOrderByClause(orderByClause, ctx.defaultTableName, ctx.database, ctx.combinedColumnTypes,
                ctx.tableAliases, subQueries);
    }

    private Integer[] parseLimitOffset(String tableAndJoins, int limitIndex) {
        String afterLimit = tableAndJoins.substring(limitIndex + 5).trim();
        Pattern limitPattern = Pattern.compile("^\\s*(\\d+)\\s*(?:(?:\\s+OFFSET\\s+)|(?:\\s*;\\s*)?\\s*$)");
        Matcher limitMatcher = limitPattern.matcher(afterLimit);
        if (!limitMatcher.find()) {
            return new Integer[]{null, null};
        }
        Integer limit = Integer.parseInt(limitMatcher.group(1));
        Integer offset = null;
        String remaining = afterLimit.substring(limitMatcher.end()).trim();
        Pattern offsetPattern = Pattern.compile("(?i)^OFFSET\\s+(\\d+)\\s*(?:(?:\\s*;\\s*)?\\s*$)");
        Matcher offsetMatcher = offsetPattern.matcher(remaining);
        if (offsetMatcher.find()) {
            offset = Integer.parseInt(offsetMatcher.group(1));
        }
        return new Integer[]{limit, offset};
    }

    private QueryParser.AdditionalClauses parseAdditionalClauses(String tableAndJoins, ParseContext ctx,
                                                             List<QueryParser.AggregateFunction> aggregates,
                                                             List<QueryParser.SubQuery> subQueries) {
        List<QueryParser.Condition> conditions = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();
        List<QueryParser.HavingCondition> havingConditions = new ArrayList<>();
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        Integer limit = null;
        Integer offset = null;
        Map<String, String> groupBySubQueries = new HashMap<>();

        int whereIndex = findClauseOutsideSubquery(tableAndJoins, SqlKeywords.WHERE);
        int groupByIndex = findClauseOutsideSubquery(tableAndJoins, SqlKeywords.GROUP_BY);
        int orderByIndex = findClauseOutsideSubquery(tableAndJoins, SqlKeywords.ORDER_BY);
        int limitIndex = findClauseOutsideSubquery(tableAndJoins, SqlKeywords.LIMIT);

        if (whereIndex != -1) {
            conditions.addAll(processWhereClause(tableAndJoins, whereIndex, groupByIndex, orderByIndex, limitIndex, ctx));
        }

        if (groupByIndex != -1 && groupByIndex > whereIndex) {
            processGroupByAndHaving(tableAndJoins, groupByIndex, orderByIndex, limitIndex, ctx, aggregates,
                    groupBySubQueries, groupBy, havingConditions);
        }

        if (orderByIndex != -1 && orderByIndex > whereIndex && orderByIndex > groupByIndex) {
            orderBy.addAll(processOrderByClause(tableAndJoins, orderByIndex, limitIndex, ctx, subQueries));
        }

        if (limitIndex != -1 && limitIndex > whereIndex && limitIndex > groupByIndex && limitIndex > orderByIndex) {
            Integer[] limitOffset = parseLimitOffset(tableAndJoins, limitIndex);
            limit = limitOffset[0];
            offset = limitOffset[1];
        }

        return new QueryParser.AdditionalClauses(conditions, groupBy, havingConditions, orderBy, limit, offset, groupBySubQueries);
    }

    private int findClauseOutsideSubquery(String query, String clause) {
        Pattern quotedStringPattern = Pattern.compile("(?i)'[^'\\\\]*+(?:\\\\.[^'\\\\]*+)*+'");
        Pattern openParenPattern = Pattern.compile("\\(");
        Pattern closeParenPattern = Pattern.compile("\\)");
        Pattern clausePattern = Pattern.compile("(?i)\\b" + Pattern.quote(clause) + "\\b");
        Pattern wordPattern = Pattern.compile("[^\\s()']+");

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
                tokenType = ErrorMessages.TAG_QUOTED_STRING;
            } else if (openParenMatcher.lookingAt()) {
                token = openParenMatcher.group();
                nextPos = openParenMatcher.end();
                tokenType = ErrorMessages.TAG_OPEN_PAREN;
            } else if (closeParenMatcher.lookingAt()) {
                token = closeParenMatcher.group();
                nextPos = closeParenMatcher.end();
                tokenType = ErrorMessages.TAG_CLOSE_PAREN;
            } else if (clauseMatcher.lookingAt()) {
                token = clauseMatcher.group();
                nextPos = clauseMatcher.end();
                tokenType = MessageConstants.TOKEN_CLAUSE;
            } else if (wordMatcher.lookingAt()) {
                token = wordMatcher.group();
                nextPos = wordMatcher.end();
                tokenType = "word";
            }

            if (token == null) {
                currentPos++;
                continue;
            }

            if (tokenType.equals(ErrorMessages.TAG_QUOTED_STRING)) {
                // Пропускаем строки в кавычках
            } else if (tokenType.equals(ErrorMessages.TAG_OPEN_PAREN)) {
                parenDepth++;
                if (parenDepth == 1 && currentPos + 7 < query.length() && query.substring(currentPos, currentPos + 7).toUpperCase().startsWith(ErrorMessages.SELECT_KEYWORD)) {
                    inSubQuery = true;
                }
            } else if (tokenType.equals(ErrorMessages.TAG_CLOSE_PAREN)) {
                parenDepth--;
                if (parenDepth == 0 && inSubQuery) {
                    inSubQuery = false;
                }
                if (parenDepth < 0) {
                    LOGGER.log(Level.SEVERE, "Несбалансированные скобки в запросе на позиции {0}: {1}", new Object[]{start, query});
                    return -1;
                }
            } else if (tokenType.equals(MessageConstants.TOKEN_CLAUSE) && parenDepth == 0 && !inSubQuery) {
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
                                                             Map<String, String> tableAliases,
                                                             List<QueryParser.SubQuery> subQueries) {
        List<QueryParser.OrderByInfo> orderBy = new ArrayList<>();
        List<String> items = splitCommaSeparatedItems(orderByClause);
        Pattern columnPattern = Pattern.compile(
                ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + "|\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\))\\s*+(?:LIMIT\\s++\\d++(?:\\s++OFFSET\\s++\\d++)?)?\\s*+(ASC|DESC)?$",
                Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                String direction = columnMatcher.group(2);
                boolean ascending = direction == null || direction.equalsIgnoreCase(SqlKeywords.ASC);

                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains(SqlKeywords.SELECT)) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    subQueries.add(new QueryParser.SubQuery(subQuery, null));
                    orderBy.add(new QueryParser.OrderByInfo(ErrorMessages.SUBQUERY_PREFIX + subQueries.size(), ascending));
                } else {
                    columnOrSubQuery = unquoteQualifiedIdentifier(columnOrSubQuery);
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
                                            Map<String, String> groupBySubQueries) {
        List<String> groupBy = new ArrayList<>();
        List<String> items = splitCommaSeparatedItems(groupByClause);
        Pattern columnPattern = Pattern.compile(ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + "|\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\))$", Pattern.DOTALL);

        for (String item : items) {
            String trimmedItem = item.trim();
            Matcher columnMatcher = columnPattern.matcher(trimmedItem);
            if (columnMatcher.matches()) {
                String columnOrSubQuery = columnMatcher.group(1);
                if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains(SqlKeywords.SELECT)) {
                    String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    queryParser.parse(subQueryStr, database);
                    String marker = ErrorMessages.SUBQUERY_PREFIX + System.currentTimeMillis();
                    groupBy.add(marker);
                    if (groupBySubQueries != null) {
                        groupBySubQueries.put(marker, subQueryStr);
                    }
                } else {
                    columnOrSubQuery = unquoteQualifiedIdentifier(columnOrSubQuery);
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
        boolean inQuotedIdentifier = false;

for (int i = 0; i < input.length(); i++) {
             char c = input.charAt(i);
             if (c == '\'') {
                 inQuotes = !inQuotes;
                 currentItem.append(c);
             } else if (c == '"' && !inQuotes) {
                 inQuotedIdentifier = !inQuotedIdentifier;
                 currentItem.append(c);
             } else if (!inQuotes && !inQuotedIdentifier) {
                 if (c == '(') {
                     parenDepth++;
                     currentItem.append(c);
                 } else if (c == ')') {
                     parenDepth--;
                     currentItem.append(c);
                 } else if (c == ',' && parenDepth == 0) {
                     addCommaSeparatedItem(currentItem, items);
                 } else {
                     currentItem.append(c);
                 }
             } else {
                 currentItem.append(c);
             }
         }

        String lastItem = currentItem.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }
        return items;
    }

    private static void addCommaSeparatedItem(StringBuilder currentItem, List<String> items) {
        String item = currentItem.toString().trim();
        if (!item.isEmpty()) {
            items.add(item);
        }
        currentItem.setLength(0);
    }

    private List<QueryParser.Condition> parseConditions(String conditionStr, ParseContext ctx) {
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
            return parseTokenizedConditions(tokens, ctx, null, false);
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
            CONDITION, LOGICAL_OPERATOR, TABLE_ALIAS
        }
    }

    private List<Token> tokenizeConditions(String conditionStr) {
        LOGGER.log(Level.FINEST, "Starting tokenization of conditionStr: {0}", conditionStr);
        String processedStr = conditionStr.replaceAll("(?i)(\\))\\s*(LIMIT\\s+\\d+)", "$1 $2");
        LOGGER.log(Level.FINEST, "After preprocessing for LIMIT: {0}", processedStr);
        List<Map.Entry<String, Pattern>> patterns = new ArrayList<>();

        // Обновленные паттерны (possessive quantifiers keep matching linear on
        // deeply nested parentheses and backslash-heavy strings, java:S5998)
        patterns.add(Map.entry("Quoted String", Pattern.compile("'(?:\\\\.|[^'\\\\])*+'")));
        patterns.add(Map.entry("Grouped Condition", Pattern.compile("\\((?:[^()']++|'(?:\\\\.|[^'\\\\])*+')*+\\)")));
        patterns.add(Map.entry("In Condition",
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*+(NOT\\s*+)?IN\\s*+\\((?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\)(?:\\s++AS\\s++" + IDENTIFIER_PATTERN + ")?")));
        patterns.add(Map.entry("Subquery Comparison",
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*+(=|>|<|>=|<=|!=|<>)\\s*+\\(\\s*+SELECT\\b(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\)\\s*+(?:AS\\s++" + IDENTIFIER_PATTERN + ")?")));
        patterns.add(Map.entry("Subquery Like",
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*+(NOT\\s++LIKE|LIKE)\\s*+\\(\\s*+SELECT\\b(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\)\\s*+(?:AS\\s++" + IDENTIFIER_PATTERN + ")?")));
        patterns.add(Map.entry(MessageConstants.TOKEN_LIKE_CONDITION,
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(NOT\\s*)?LIKE\\s*'(?:\\\\.|[^'\\\\])*+'")));
        patterns.add(Map.entry("Null Condition",
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*IS\\s*(NOT\\s+)?NULL\\b")));
        patterns.add(Map.entry("Comparison Condition",
                Pattern.compile(ErrorMessages.CASE_INSENSITIVE_GROUP_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(=|>|<|>=|<=|!=|<>)\\s*(" + QUALIFIED_IDENTIFIER_PATTERN + "|'[^']*'|[-]?\\d+(?:\\.\\d*)?)")));
        patterns.add(Map.entry("Table Alias", Pattern.compile("(?i)\\b" + SIMPLE_IDENTIFIER_PATTERN + "(?=\\s*\\." + SIMPLE_IDENTIFIER_PATTERN + "\\b)")));
        patterns.add(Map.entry(MessageConstants.TOKEN_LOGICAL_OPERATOR, Pattern.compile("(?i)\\b(AND|OR)\\b")));
        patterns.add(Map.entry("NOT Keyword", Pattern.compile("(?i)\\bNOT\\b")));
        patterns.add(Map.entry("Alias", Pattern.compile("(?i)\\bAS\\s+" + IDENTIFIER_PATTERN + "\\b")));

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
                Matcher matcher = entry.getValue().matcher(processedStr).region(currentPos, stringLength);
                if (matcher.lookingAt()) {
                    String tokenValue = matcher.group().trim();
                    if (!tokenValue.isEmpty()) {
                        matchedToken = tokenValue;
                        matchedPatternName = entry.getKey();
                        nextPos = matcher.end();
                        matched = true;
                        LOGGER.log(Level.FINEST, "Applied pattern {0}: Matched token='{1}', End position={2}, Remaining string='{3}'",
                                new Object[]{matchedPatternName, tokenValue, nextPos, processedStr.substring(nextPos)});
                        break;
                    }
                } else {
                    LOGGER.log(Level.FINEST, "Applied pattern {0}: No match at position {1}, Current string='{2}'",
                            new Object[]{entry.getKey(), currentPos, processedStr.substring(currentPos)});
                }
            }

            if (matched) {
                Token.TokenType type = switch (matchedPatternName) {
                    case MessageConstants.TOKEN_LOGICAL_OPERATOR -> Token.TokenType.LOGICAL_OPERATOR;
                    case "Table Alias" -> Token.TokenType.TABLE_ALIAS;
                    default -> Token.TokenType.CONDITION;
                };
                tokens.add(new Token(type, matchedToken));
                LOGGER.log(Level.FINEST, "Tokenized: {0}, type: {1}, position: {2}", new Object[]{matchedToken, type, currentPos});
                currentPos = nextPos;
            } else {
                String remaining = processedStr.substring(currentPos);
                LOGGER.log(Level.SEVERE, "Failed to match token at position {0}: {1}", new Object[]{currentPos, remaining});
                throw new IllegalArgumentException("Invalid token at position " + currentPos + ": " + remaining);
            }
        }

        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("No valid tokens found in condition: " + processedStr);
        }
        LOGGER.log(Level.FINE, "Tokenization completed, tokens: {0}", tokens);
        return tokens;
    }
    private List<QueryParser.Condition> parseTokenizedConditions(List<Token> tokens, ParseContext ctx,
                                                             String conjunction, boolean not) {
        List<QueryParser.Condition> conditions = new ArrayList<>();
        String currentConjunction = conjunction;
        String lastAlias = null;

        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            LOGGER.log(Level.FINEST, "Processing token: {0}, type: {1}", new Object[]{token.value, token.type});

            if (token.type == Token.TokenType.LOGICAL_OPERATOR) {
                currentConjunction = token.value.toUpperCase();
                LOGGER.log(Level.FINEST, "Set conjunction: {0}", currentConjunction);
            } else if (token.type == Token.TokenType.TABLE_ALIAS) {
                lastAlias = token.value;
                LOGGER.log(Level.FINEST, "Captured table alias: {0}", lastAlias);
            } else {
                String condStr = token.value;
                String effectiveTableName = lastAlias != null ? ctx.tableAliases.getOrDefault(lastAlias, lastAlias) : ctx.defaultTableName;

                if (condStr.equalsIgnoreCase(SqlKeywords.NOT)) {
                    not = true;
                    LOGGER.log(Level.FINEST, "Processing NOT keyword, negation enabled for next condition");
                } else {
                    if (condStr.startsWith("(") && condStr.endsWith(")")) {
                        String subCondStr = condStr.substring(1, condStr.length() - 1).trim();
                        if (subCondStr.toUpperCase().startsWith(SqlKeywords.SELECT)) {
                            validateSubQuery(subCondStr);
                            Query<?> subQuery = queryParser.parse(subCondStr, ctx.database);
                            String columnName = effectiveTableName + ".unknown";
                            conditions.add(new QueryParser.Condition(columnName, new QueryParser.SubQuery(subQuery, null), currentConjunction, not));
                        } else {
                            List<Token> subTokens = tokenizeConditions(subCondStr);
                            List<QueryParser.Condition> subConditions = parseTokenizedConditions(subTokens,
                                    withDefaultTableName(ctx, effectiveTableName), currentConjunction, not);
                            conditions.add(new QueryParser.Condition(subConditions, currentConjunction, not));
                        }
                    } else if (condStr.toUpperCase().contains(" IN ")) {
                        conditions.add(parseInCondition(condStr, withDefaultTableName(ctx, effectiveTableName), currentConjunction, not));
                    } else if (condStr.toUpperCase().contains(SqlKeywords.SELECT)) {
                        LOGGER.log(Level.FINEST, "Attempting to parse subquery condition: {0}", condStr);
                        conditions.add(parseSubQueryCondition(condStr, withDefaultTableName(ctx, effectiveTableName), currentConjunction, not));
                    } else {
                        LOGGER.log(Level.FINEST, "Parsing single condition: {0}", condStr);
                        conditions.add(parseSingleCondition(condStr, withDefaultTableName(ctx, effectiveTableName), currentConjunction, not));
                    }

                    lastAlias = null;
                    currentConjunction = null;
                    not = false;
                }
            }
        }

        LOGGER.log(Level.FINE, "Parsed conditions: {0}", conditions);
        return conditions;
    }

    private ParseContext withDefaultTableName(ParseContext ctx, String defaultTableName) {
        if (ctx.defaultTableName.equals(defaultTableName)) {
            return ctx;
        }
        return new ParseContext(defaultTableName, ctx.database, ctx.originalQuery, ctx.isJoinCondition,
                ctx.combinedColumnTypes, ctx.tableAliases, ctx.columnAliases);
    }

    // Модифицируем метод parseInCondition
// Удаляем createCustomSubQuery, так как он несовместим с Query<T>
// Вместо этого модифицируем parseInCondition для хранения подзапроса как строки

    private QueryParser.Condition parseInCondition(String condStr, ParseContext ctx, String conjunction, boolean not) {
        Pattern inPattern = Pattern.compile(
                ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s++(NOT\\s++)?IN\\s*+\\((SELECT(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+)\\)\\s*+(?:AS\\s++" + IDENTIFIER_PATTERN + ")?(?:\\s++LIMIT\\s++\\d++(?:\\s++OFFSET\\s++\\d++)?)?$",
                Pattern.DOTALL);
        Matcher inMatcher = inPattern.matcher(condStr);
        if (!inMatcher.matches()) {
            return parseInValuesCondition(condStr, ctx, conjunction, not);
        }
        return parseInSubQueryCondition(inMatcher, ctx, conjunction, not);
    }

    private QueryParser.Condition parseInValuesCondition(String condStr, ParseContext ctx, String conjunction, boolean not) {
        Pattern valuesInPattern = Pattern.compile(
                ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+(NOT\\s+)?IN\\s*\\(([^)]*+)\\)$",
                Pattern.DOTALL);
        Matcher valuesMatcher = valuesInPattern.matcher(condStr);
        if (!valuesMatcher.matches()) {
            throw new IllegalArgumentException("Invalid IN condition format: " + condStr);
        }

        String column = unquoteQualifiedIdentifier(valuesMatcher.group(1).trim());
        boolean inNot = valuesMatcher.group(2) != null;
        String valuesStr = valuesMatcher.group(3).trim();
        String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
        Class<?> columnType = getColumnType(normalizedColumn, ctx.combinedColumnTypes);

        LOGGER.log(Level.FINEST, "Parsing IN condition: column={0}, values={1}, not={2}", new Object[]{normalizedColumn, valuesStr, inNot});

        List<String> valueParts = splitInValues(valuesStr);
        List<Object> inValues = parseInValues(valueParts, normalizedColumn, columnType);
        if (inValues.isEmpty()) {
            throw new IllegalArgumentException("Empty IN list in: " + condStr);
        }
        LOGGER.log(Level.FINE, "Parsed IN values condition: {0} {1}IN {2}", new Object[]{normalizedColumn, inNot ? "NOT " : "", inValues});
        return new QueryParser.Condition(normalizedColumn, inValues, conjunction, inNot);
    }

    private List<Object> parseInValues(List<String> valueParts, String normalizedColumn, Class<?> columnType) {
        List<Object> inValues = new ArrayList<>();
        for (String val : valueParts) {
            String trimmedVal = val.trim();
            if (trimmedVal.isEmpty()) continue;
            Object value = parseConditionValue(normalizedColumn, trimmedVal, columnType);
            inValues.add(value);
        }
        return inValues;
    }

    private QueryParser.Condition parseInSubQueryCondition(Matcher inMatcher, ParseContext ctx, String conjunction, boolean not) {
        String column = unquoteQualifiedIdentifier(inMatcher.group(1).trim());
        boolean inNot = inMatcher.group(2) != null;
        String subQueryStr = inMatcher.group(3).trim();
        String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);

        LOGGER.log(Level.FINEST, "Parsing IN subquery condition: column={0}, subquery={1}, not={2}", new Object[]{normalizedColumn, subQueryStr, inNot});

        validateSubQuery(subQueryStr);
        Query<?> subQuery = createSubQueryHolder(subQueryStr);
        QueryParser.SubQuery subQueryObj = new QueryParser.SubQuery(subQuery, null);
        LOGGER.log(Level.FINE, "Parsed IN subquery condition: {0} {1}IN (subquery: {2})", new Object[]{normalizedColumn, inNot ? "NOT " : "", subQueryStr});
        return new QueryParser.Condition(normalizedColumn, subQueryObj, conjunction, inNot);
    }

    private Query<?> createSubQueryHolder(String subQueryStr) {
        return new Query<List<?>>() {
            @Override
            @SuppressWarnings("unused")
            public List<?> execute(Table table) {
                throw new UnsupportedOperationException("Subquery execution should be handled by SelectQuery: " + subQueryStr);
            }
            @Override
            public String toString() {
                return subQueryStr;
            }
        };
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
             } else if (!inQuotes) {
                 if (c == '(') {
                     parenDepth++;
                     current.append(c);
                 } else if (c == ')') {
                     parenDepth--;
                     current.append(c);
                 } else if (c == ',' && parenDepth == 0) {
                     addInValue(current, values);
                 } else {
                     current.append(c);
                 }
             } else {
                 current.append(c);
             }
         }

        String value = current.toString().trim();
        if (!value.isEmpty()) {
            values.add(value);
        }
        return values;
    }

    private static void addInValue(StringBuilder current, List<String> values) {
        String val = current.toString().trim();
        if (!val.isEmpty()) {
            values.add(val);
        }
        current.setLength(0);
    }

    private QueryParser.Condition parseSubQueryCondition(String condStr, ParseContext ctx, String conjunction, boolean not) {
        Pattern subQueryPattern = Pattern.compile(
                ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*+(=|>|<|>=|<=|!=|<>|LIKE|NOT\\s++LIKE)\\s*+\\(\\s*+(SELECT\\b.*+)$",
                Pattern.DOTALL);
        Matcher subQueryMatcher = subQueryPattern.matcher(condStr);
        if (!subQueryMatcher.matches()) {
            throw new IllegalArgumentException("Invalid subquery condition format: " + condStr);
        }

        String column = unquoteQualifiedIdentifier(subQueryMatcher.group(1).trim());
        String operatorStr = subQueryMatcher.group(2).trim();
        String subQueryContent = subQueryMatcher.group(3).trim();

        String subQueryStr = extractSubQueryString(subQueryContent);
        validateSubQuery(subQueryStr);
        Query<?> subQuery = createSubQueryHolder(subQueryStr);
        QueryParser.SubQuery newSubQuery = new QueryParser.SubQuery(subQuery, null);
        QueryParser.Operator operator = parseOperator(operatorStr);
        String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
        validateColumn(normalizedColumn, ctx.combinedColumnTypes);
        LOGGER.log(Level.FINE, "Parsed subquery condition: {0} {1} (subquery: {2})", new Object[]{normalizedColumn, operatorStr, subQueryStr});
        return new QueryParser.Condition(normalizedColumn, newSubQuery, operator, conjunction, not);
    }

    private String extractSubQueryString(String subQueryContent) {
        int parenDepth = 1;
        int endIndex = -1;
        boolean inQuotes = false;
        for (int i = 0; i < subQueryContent.length(); i++) {
            char c = subQueryContent.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                    if (parenDepth == 0) {
                        endIndex = i;
                        break;
                    }
                }
            }
        }
        if (endIndex == -1) {
            throw new IllegalArgumentException(ErrorMessages.UNBALANCED_PARENS_SUBQUERY + subQueryContent);
        }
        return subQueryContent.substring(0, endIndex).trim();
    }

    private void validateSubQuery(String subQueryStr) {
        if (!subQueryStr.toUpperCase().startsWith(SqlKeywords.SELECT)) {
            throw new IllegalArgumentException("Invalid subquery: must start with SELECT: " + subQueryStr);
        }
        if (!subQueryStr.toUpperCase().contains(MessageConstants.SQL_FROM_SPACED)) {
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
                        throw new IllegalArgumentException(ErrorMessages.UNBALANCED_PARENS_SUBQUERY + subQueryStr);
                    }
                }
            }
        }
        if (parenDepth != 0) {
            throw new IllegalArgumentException(ErrorMessages.UNBALANCED_PARENS_SUBQUERY + subQueryStr);
        }
    }

    private QueryParser.Condition parseSingleCondition(String condStr, ParseContext ctx,
                                                   String conjunction, boolean not) {
        LOGGER.log(Level.FINEST, "Parsing single condition: {0}, full condition={1}", new Object[]{condStr, condStr});
        // Нормализуем строку для добавления пробелов вокруг операторов
        condStr = condStr.replaceAll("([=><!])", " $1 ").replaceAll("\\s+", " ").trim();
        LOGGER.log(Level.FINEST, "Normalized condition: {0}", condStr);

        Pattern likePattern = Pattern.compile(ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s*(LIKE|NOT\\s+LIKE)\\s*('(?:''|[^'])*+')");
        Matcher likeMatcher = likePattern.matcher(condStr);
        if (likeMatcher.matches()) {
            String column = unquoteQualifiedIdentifier(likeMatcher.group(1).trim());
            String operatorStr = likeMatcher.group(2).toUpperCase();
            String value = likeMatcher.group(3).substring(1, likeMatcher.group(3).length() - 1);
            String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
            validateColumn(normalizedColumn, ctx.combinedColumnTypes);
            QueryParser.Operator operator = operatorStr.equals(SqlKeywords.LIKE) ? QueryParser.Operator.LIKE : QueryParser.Operator.NOT_LIKE;
            Object parsedValue = parseConditionValue(normalizedColumn, "'" + value + "'", getColumnType(normalizedColumn, ctx.combinedColumnTypes));
            return new QueryParser.Condition(normalizedColumn, parsedValue, operator, conjunction, not);
        }

        Pattern isNullPattern = Pattern.compile(ErrorMessages.CASE_INSENSITIVE_START_PATTERN + QUALIFIED_IDENTIFIER_PATTERN + ")\\s+IS\\s+(NOT\\s+)?NULL\\b");
        Matcher isNullMatcher = isNullPattern.matcher(condStr);
        if (isNullMatcher.matches()) {
            String column = unquoteQualifiedIdentifier(isNullMatcher.group(1).trim());
            boolean isNotNull = isNullMatcher.group(2) != null;
            String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
            validateColumn(normalizedColumn, ctx.combinedColumnTypes);
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
        String column = unquoteQualifiedIdentifier(leftPart);
        String normalizedColumn = normalizeColumnName(column, ctx.defaultTableName, ctx.tableAliases);
        validateColumn(normalizedColumn, ctx.combinedColumnTypes);

        Pattern columnPattern = Pattern.compile("(?i)^" + QUALIFIED_IDENTIFIER_PATTERN + "$");
        String rightColumn = null;
        Object value = null;

        String upperRightPart = rightPart.toUpperCase();
        if (upperRightPart.equals(SqlKeywords.TRUE) || upperRightPart.equals(SqlKeywords.FALSE) || upperRightPart.equals(SqlKeywords.NULL)) {
            Class<?> literalColumnType = getColumnType(normalizedColumn, ctx.combinedColumnTypes);
            if (upperRightPart.equals(SqlKeywords.NULL)) {
                value = null;
            } else if (literalColumnType == Boolean.class) {
                value = Boolean.parseBoolean(rightPart);
            } else {
                throw new IllegalArgumentException(MessageConstants.ERROR_BOOLEAN_VALUE_PREFIX + rightPart + "' does not match column type: " + literalColumnType.getSimpleName());
            }
        } else if (columnPattern.matcher(rightPart).matches()) {
            rightColumn = unquoteQualifiedIdentifier(rightPart);
        } else {
            value = parseConditionValue(normalizedColumn, rightPart, getColumnType(normalizedColumn, ctx.combinedColumnTypes));
        }

        QueryParser.Operator operator = parseOperator(operatorInfo.operator);

        if (ctx.isJoinCondition && !rightColumnIsFromDifferentTable(normalizedColumn, rightColumn, ctx.tableAliases)) {
            throw new IllegalArgumentException("Join condition must compare columns from different tables: " + condStr);
        }

        if (rightColumn != null) {
            String normalizedRightColumn = normalizeColumnName(rightColumn, ctx.defaultTableName, ctx.tableAliases);
            validateColumn(normalizedRightColumn, ctx.combinedColumnTypes);
            return new QueryParser.Condition(normalizedColumn, normalizedRightColumn, operator, conjunction, not);
        } else {
            return new QueryParser.Condition(normalizedColumn, value, operator, conjunction, not);
        }
    }

    private QueryParser.Operator parseOperator(String operatorStr) {
        return SqlParsingUtils.parseOperator(operatorStr);
    }

    private QueryParser.OperatorInfo findOperator(String condStr, String[] operators) {
        int parenDepth = 0;
        boolean inQuotes = false;
        for (int i = 0; i < condStr.length(); i++) {
            char c = condStr.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == '(') {
                    parenDepth++;
                } else if (c == ')') {
                    parenDepth--;
                } else if (parenDepth == 0) {
                    for (String op : operators) {
                        Pattern opPattern = Pattern.compile("(?i)" + Pattern.quote(op));
                        Matcher opMatcher = opPattern.matcher(condStr);
                        if (opMatcher.find(i) && opMatcher.start() == i) {
                            String matchedOp = opMatcher.group();
                            int actualIndex = opMatcher.start();
                            int actualEndIndex = opMatcher.end();
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
                String strValue = SqlLexer.extractStringLiteral(value);
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
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to parse value '" + value + "' for column " + column + ": " + e.getMessage(), e);
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
        throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
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
            throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
        }
    }

    private String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        return SqlParsingUtils.normalizeColumnName(column, defaultTableName, tableAliases);
    }

    private List<QueryParser.HavingCondition> parseHavingConditions(String havingClause, ParseContext ctx,
                                                                List<QueryParser.AggregateFunction> aggregates) {
        List<QueryParser.HavingCondition> conditions = new ArrayList<>();
        HavingParseState state = new HavingParseState();

        for (int i = 0; i < havingClause.length(); i++) {
            char c = havingClause.charAt(i);
            if (c == '\'') {
                state.inQuotes = !state.inQuotes;
                state.currentCondition.append(c);
            } else if (!state.inQuotes && c == '(') {
                handleOpenParen(c, i, havingClause, state);
            } else if (!state.inQuotes && c == ')') {
                handleCloseParen(state, conditions, ctx, aggregates);
            } else if (!state.inQuotes && state.parenDepth == 0 && c == ' ') {
                int newI = handleSpaceSeparator(i, havingClause, state, conditions, ctx, aggregates);
                if (newI == AND_OR_BREAK_SENTINEL) {
                    break;
                }
                if (newI != i) {
                    i = newI;
                } else {
                    state.currentCondition.append(c);
                }
            } else {
                state.currentCondition.append(c);
            }
        }

        appendFinalCondition(state, conditions, ctx, aggregates);
        return conditions;
    }

    private static final int AND_OR_BREAK_SENTINEL = -1;

    private void handleOpenParen(char c, int i, String havingClause, HavingParseState state) {
        state.parenDepth++;
        state.currentCondition.append(c);
    }

    private void handleCloseParen(HavingParseState state, List<QueryParser.HavingCondition> conditions,
                                  ParseContext ctx, List<QueryParser.AggregateFunction> aggregates) {
        state.parenDepth--;
        state.currentCondition.append(')');
        if (state.parenDepth == 0 && state.currentCondition.length() > 0) {
            String condStr = state.currentCondition.toString().trim();
            if (condStr.startsWith("(") && condStr.endsWith(")")) {
                condStr = condStr.substring(1, condStr.length() - 1).trim();
                if (!condStr.isEmpty()) {
                    List<QueryParser.HavingCondition> subConditions = parseHavingConditions(condStr, ctx, aggregates);
                    conditions.add(new QueryParser.HavingCondition(subConditions, state.conjunction, state.not));
                }
                resetState(state);
            }
        }
    }

    private int handleSpaceSeparator(int i, String havingClause, HavingParseState state,
                                     List<QueryParser.HavingCondition> conditions, ParseContext ctx,
                                     List<QueryParser.AggregateFunction> aggregates) {
        String nextToken = getNextToken(havingClause, i + 1);
        if (nextToken.equalsIgnoreCase(SqlKeywords.AND) || nextToken.equalsIgnoreCase(SqlKeywords.OR)) {
            String condStr = state.currentCondition.toString().trim();
            if (!condStr.isEmpty()) {
                QueryParser.HavingCondition condition = parseSingleHavingCondition(condStr, ctx, aggregates, state.conjunction, state.not);
                conditions.add(condition);
                state.conjunction = nextToken.toUpperCase();
                state.not = false;
                state.currentCondition = new StringBuilder();
                return i + nextToken.length();
            }
        } else if (nextToken.equalsIgnoreCase(SqlKeywords.NOT)) {
            state.not = true;
            state.currentCondition.append(' ');
            return i + nextToken.length();
        }
        return i;
    }

    private void appendFinalCondition(HavingParseState state, List<QueryParser.HavingCondition> conditions,
                                      ParseContext ctx, List<QueryParser.AggregateFunction> aggregates) {
        String finalCondStr = state.currentCondition.toString().trim();
        if (!finalCondStr.isEmpty()) {
            QueryParser.HavingCondition condition = parseSingleHavingCondition(finalCondStr, ctx, aggregates, state.conjunction, state.not);
            conditions.add(condition);
        }
    }

    private void resetState(HavingParseState state) {
        state.currentCondition = new StringBuilder();
        state.conjunction = null;
        state.not = false;
    }

    private static final class HavingParseState {
        StringBuilder currentCondition = new StringBuilder();
        boolean inQuotes;
        int parenDepth;
        String conjunction;
        boolean not;
    }

    private QueryParser.OperatorInfo findHavingOperator(String condStr) {
        String[] operators = {"=", "!=", "<>", ">=", "<=", "<", ">"};
        int parenDepth = 0;
        boolean inQuotes = false;
        for (int i = 0; i < condStr.length(); i++) {
            char ch = condStr.charAt(i);
            if (ch == '\'') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (ch == '(') {
                    parenDepth++;
                } else if (ch == ')') {
                    parenDepth--;
                } else if (parenDepth == 0) {
                    for (String op : operators) {
                        if (condStr.regionMatches(true, i, op, 0, op.length())) {
                            char prevChar = i > 0 ? condStr.charAt(i - 1) : ' ';
                            char nextChar = i + op.length() < condStr.length() ? condStr.charAt(i + op.length()) : ' ';
                            if (Character.isWhitespace(prevChar) && Character.isWhitespace(nextChar)) {
                                return new QueryParser.OperatorInfo(op, i, i + op.length());
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private QueryParser.AggregateFunction resolveAggregate(String leftPart, ParseContext ctx,
                                                           List<QueryParser.AggregateFunction> aggregates) {
        for (QueryParser.AggregateFunction agg : aggregates) {
            String aggStr = agg.toString();
            String aggBase = agg.alias != null
                    ? aggStr.replaceFirst("(?i)\\s+AS\\s+" + Pattern.quote(agg.alias) + "$", "")
                    : aggStr;
            if (aggBase.equalsIgnoreCase(leftPart) || (agg.alias != null && agg.alias.equalsIgnoreCase(leftPart))) {
                return agg;
            }
        }

        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*+\\(\\s*+(" + QUALIFIED_IDENTIFIER_PATTERN + "|\\*|\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\))\\s*+\\)(?:\\s++AS\\s++(" + IDENTIFIER_PATTERN + "))?$", Pattern.DOTALL);
        Matcher aggMatcher = aggPattern.matcher(leftPart);
        if (!aggMatcher.matches()) {
            throw new IllegalArgumentException("Invalid HAVING condition: left side must be an aggregate function: " + leftPart);
        }
        String funcName = aggMatcher.group(1);
        String columnOrSubQuery = aggMatcher.group(2);
        String alias = unquoteIdentifier(aggMatcher.group(3));
        if (columnOrSubQuery.toUpperCase().startsWith("(") && columnOrSubQuery.toUpperCase().contains(SqlKeywords.SELECT)) {
            String subQueryStr = columnOrSubQuery.substring(1, columnOrSubQuery.length() - 1).trim();
            validateSubQuery(subQueryStr);
            Query<?> subQuery = queryParser.parse(subQueryStr, ctx.database);
            return new QueryParser.AggregateFunction(funcName, new QueryParser.SubQuery(subQuery, null), alias);
        }
        columnOrSubQuery = unquoteQualifiedIdentifier(columnOrSubQuery);
        String normalizedColumn = normalizeColumnName(columnOrSubQuery, ctx.defaultTableName, ctx.tableAliases);
        validateColumn(normalizedColumn, ctx.combinedColumnTypes);
        return new QueryParser.AggregateFunction(funcName, columnOrSubQuery, alias);
    }

    private Object resolveHavingValue(String rightPart, QueryParser.AggregateFunction aggregate, ParseContext ctx) {
        if (!rightPart.startsWith("(") || !rightPart.toUpperCase().contains(SqlKeywords.SELECT)) {
            Class<?> valueType = aggregate.functionName.equals(SqlKeywords.COUNT) ? Long.class :
                    (aggregate.column != null ? getColumnType(aggregate.column, ctx.combinedColumnTypes) : Double.class);
            return parseConditionValue(aggregate.toString(), rightPart, valueType);
        }
        String cleanRightPart = rightPart.replaceFirst("(?i)\\s+AS\\s+" + IDENTIFIER_PATTERN + "\\s*$", "").trim();
        if (!(cleanRightPart.startsWith("(") && cleanRightPart.endsWith(")"))) {
            throw new IllegalArgumentException("Invalid HAVING subquery on right side: " + rightPart);
        }
        String subQueryStr = cleanRightPart.substring(1, cleanRightPart.length() - 1).trim();
        validateSubQuery(subQueryStr);
        Object subQueryResult = ctx.database.executeQuery(subQueryStr, null);
        if (!(subQueryResult instanceof List<?> subRows)) {
            throw new IllegalArgumentException("HAVING subquery must return a list of rows: " + rightPart);
        }
        if (subRows.isEmpty() || !(subRows.get(0) instanceof Map<?, ?> firstMap) || firstMap.isEmpty()) {
            return null;
        }
        return firstMap.values().iterator().next();
    }

    private QueryParser.HavingCondition parseSingleHavingCondition(String condStr, ParseContext ctx,
                                                               List<QueryParser.AggregateFunction> aggregates,
                                                               String conjunction, boolean not) {
        QueryParser.OperatorInfo operatorInfo = findHavingOperator(condStr);
        if (operatorInfo == null) {
            throw new IllegalArgumentException("Invalid HAVING condition: no valid operator found in '" + condStr + "'");
        }

        String leftPart = condStr.substring(0, operatorInfo.index).trim();
        String rightPart = condStr.substring(operatorInfo.endIndex).trim();

        QueryParser.AggregateFunction aggregate = resolveAggregate(leftPart, ctx, aggregates);
        Object value = resolveHavingValue(rightPart, aggregate, ctx);
        QueryParser.Operator operator = parseOperator(operatorInfo.operator);

        return new QueryParser.HavingCondition(aggregate, operator, value, conjunction, not);
    }

    private String getNextToken(String str, int startIndex) {
        Pattern tokenPattern = Pattern.compile("(?s)(?:'(?:\\\\.|[^'\\\\])*+'|[^\\s()']++)");
        Matcher matcher = tokenPattern.matcher(str).region(startIndex, str.length());
        if (matcher.find()) {
            return matcher.group().trim();
        }
        return "";
    }
}
