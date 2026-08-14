package diesel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Executes a SELECT statement against a table: applies WHERE conditions
 * (optionally via an index), joins, GROUP BY / HAVING aggregation, ORDER BY,
 * LIMIT/OFFSET and the SELECT column/alias projection, including aggregate
 * functions and scalar/in-list subqueries.
 *
 * @see QueryParser
 * @see Query
 */
class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final List<QueryParser.AggregateFunction> aggregates;
    private final List<QueryParser.Condition> conditions;
    private final List<QueryParser.JoinInfo> joins;
    private final String mainTableName;
    private final Integer limit;
    private final Integer offset;
    private final List<QueryParser.OrderByInfo> orderBy;
    private final List<String> groupBy;
    private final List<QueryParser.HavingCondition> havingConditions;
    private final Map<String, String> tableAliases;
    private final List<QueryParser.SubQuery> subQueries;
    private final Map<String, String> groupBySubQueries;
    private final Map<String, Object> scalarSubQueryCache = new HashMap<>();
    private final Map<String, List<Object>> inSubQueryCache = new HashMap<>();
    private final UUID transactionId; // Changed from String to UUID

    /**
     * Memoizes {@code normalizeColumnName} results so the millions of
     * repeated per-row normalizations in JOIN/WHERE hot loops collapse to a
     * single hash lookup. The cache is cleared at the start of {@link #execute}
     * because table aliases are resolved lazily during join setup.
     */
    private final Map<String, String> normalizeCache = new HashMap<>();

    /**
     * Caches the compiled {@link Pattern}s produced by {@link #likeComparison},
     * so a LIKE predicate evaluated once per joined pair (e.g. 360k pairs) no
     * longer re-compiles the same regex on every row. Cleared per execution.
     */
    private final Map<String, Pattern> likePatternCache = new HashMap<>();

    /**
     * Pre-resolved SELECT projection, built once per execution so the hot
     * per-row {@link #filterColumns} loop never re-splits/re-matches column
     * strings (the old code ran a regex split + alias match per column per row).
     */
    private List<ColumnProjection> projectionPlan;

    /**
     * Pre-resolved row keys for each ORDER BY clause, so {@link #compareRows}
     * never re-splits/rewrites column strings per comparison (external sort
     * performs O(n log n) comparisons).
     */
    private final List<String> orderByKeys = new ArrayList<>();

    /**
     * Aggregate result keys produced by the GROUP BY path (aggregate alias or
     * canonical name, e.g. {@code "COUNT(*)"}). They are not part of the plain
     * column projection, so they must be copied onto the final output rows
     * after {@link #filterColumns}.
     */
    private final List<String> groupAggregateKeys = new ArrayList<>();

    /**
     * Maximum number of result rows kept in memory before the engine spills
     * overflow rows to temporary files on disk. Loaded once from
     * {@code config.properties} ({@code max.inmemory.rows}), defaulting to 10000.
     * Streaming (spill-to-disk joins and external sort) only kicks in when an
     * estimated result set exceeds this threshold, so small queries keep their
     * current in-memory behaviour unchanged.
     */
    private static final long MAX_IN_MEMORY_ROWS;

    static {
        long value = 10000;
        try {
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String raw = props.getProperty("max.inmemory.rows");
                if (raw != null) {
                    value = Long.parseLong(raw.trim());
                }
            }
        } catch (Exception ignored) {
            // Keep the default on any config error
        }
        MAX_IN_MEMORY_ROWS = value;
    }

    /**
     * Creates a SELECT query over the given table, without subqueries in the
     * GROUP BY clause.
     *
     * @param tableName         the main table name
     * @param tableAlias        the main table alias, or null
     * @param columns           the selected columns (or aggregates/subqueries)
     * @param aggregates        the parsed aggregate functions
     * @param joins             the parsed join clauses
     * @param conditions        the WHERE conditions
     * @param groupBy           the GROUP BY columns
     * @param havingConditions  the HAVING conditions
     * @param orderBy           the ORDER BY list
     * @param limit             the LIMIT, or null
     * @param offset            the OFFSET, or null
     * @param subQueries        the scalar subqueries in the SELECT clause
     * @param tableAliases      the alias to table name mapping
     * @param extraTableAliases extra aliases from joins
     * @param columnTypes       the combined column types
     */
    public SelectQuery(String tableName, String tableAlias, List<String> columns,
                       List<QueryParser.AggregateFunction> aggregates, List<QueryParser.JoinInfo> joins,
                       List<QueryParser.Condition> conditions, List<String> groupBy,
                       List<QueryParser.HavingCondition> havingConditions, List<QueryParser.OrderByInfo> orderBy,
                       Integer limit, Integer offset, List<QueryParser.SubQuery> subQueries,
                       Map<String, String> tableAliases, Map<String, String> extraTableAliases,
                       Map<String, Class<?>> columnTypes) {
        this(tableName, tableAlias, columns, aggregates, joins, conditions, groupBy, havingConditions, orderBy,
                limit, offset, subQueries, tableAliases, extraTableAliases, columnTypes, new HashMap<>());
    }

    /**
     * Creates a SELECT query over the given table, including any subqueries in
     * the GROUP BY clause.
     *
     * @param tableName         the main table name
     * @param tableAlias        the main table alias, or null
     * @param columns           the selected columns (or aggregates/subqueries)
     * @param aggregates        the parsed aggregate functions
     * @param joins             the parsed join clauses
     * @param conditions        the WHERE conditions
     * @param groupBy           the GROUP BY columns
     * @param havingConditions  the HAVING conditions
     * @param orderBy           the ORDER BY list
     * @param limit             the LIMIT, or null
     * @param offset            the OFFSET, or null
     * @param subQueries        the scalar subqueries in the SELECT clause
     * @param tableAliases      the alias to table name mapping
     * @param extraTableAliases extra aliases from joins
     * @param columnTypes       the combined column types
     * @param groupBySubQueries the subqueries used in the GROUP BY clause
     */
    public SelectQuery(String tableName, String tableAlias, List<String> columns,
                       List<QueryParser.AggregateFunction> aggregates, List<QueryParser.JoinInfo> joins,
                       List<QueryParser.Condition> conditions, List<String> groupBy,
                       List<QueryParser.HavingCondition> havingConditions, List<QueryParser.OrderByInfo> orderBy,
                       Integer limit, Integer offset, List<QueryParser.SubQuery> subQueries,
                       Map<String, String> tableAliases, Map<String, String> extraTableAliases,
                       Map<String, Class<?>> columnTypes, Map<String, String> groupBySubQueries) {
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
        this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : new ArrayList<>();
        this.conditions = conditions != null ? new ArrayList<>(conditions) : new ArrayList<>();
        this.joins = joins != null ? new ArrayList<>(joins) : new ArrayList<>();
        this.mainTableName = tableName; // Используем tableName вместо mainTableName
        this.limit = limit;
        this.offset = offset;
        this.orderBy = orderBy != null ? new ArrayList<>(orderBy) : new ArrayList<>();
        this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : new ArrayList<>();
        this.havingConditions = havingConditions != null ? new ArrayList<>(havingConditions) : new ArrayList<>();
        this.tableAliases = tableAliases != null ? new HashMap<>(tableAliases) : new HashMap<>();
        this.subQueries = subQueries != null ? new ArrayList<>(subQueries) : new ArrayList<>();
        this.groupBySubQueries = groupBySubQueries != null ? new HashMap<>(groupBySubQueries) : new HashMap<>();
        this.transactionId = UUID.randomUUID(); // Генерируем UUID, если он не передан
        // Добавляем tableAlias в tableAliases, если он не null
        if (tableAlias != null && !tableAlias.isEmpty()) {
            this.tableAliases.put(tableAlias, tableName);
        }
        // Добавляем mainTableName в tableAliases
        this.tableAliases.putIfAbsent(tableName, tableName);
        // Обрабатываем extraTableAliases
        if (extraTableAliases != null) {
            this.tableAliases.putAll(extraTableAliases);
        }
    }

    /**
     * Executes the query against the table, resolving join tables through the
     * attached database.
     *
     * @param table the main table
     * @return the result rows as a list of column-to-value maps
     * @throws IllegalArgumentException if a join table is missing
     */
    @Override
    public List<Map<String, Object>> execute(Table table) {
        Database database = table.getDatabase();
        List<Map<String, Object>> result = new ArrayList<>();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        Map<String, Table> tables = new HashMap<>();
        tables.put(mainTableName, table);

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>();
        table.getColumnTypes().forEach((col, type) -> combinedColumnTypes.put(mainTableName + "." + col, type));

        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = database.getTable(join.tableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Join table not found: " + join.tableName);
            }
            tables.put(join.tableName, joinTable);
            joinTable.getColumnTypes().forEach((col, type) -> combinedColumnTypes.put(join.tableName + "." + col, type));
            if (join.alias != null) {
                tableAliases.put(join.alias, join.tableName);
            }
        }

        reorderJoinsForNestedLoop(tables);
        normalizeCache.clear();
        likePatternCache.clear();
        groupAggregateKeys.clear();
        orderByKeys.clear();
        orderByKeys.addAll(resolveOrderByKeys());
        projectionPlan = buildProjectionPlan();

        try {
            List<Map<String, Object>> mainRows = getIndexedRows(table, conditions, mainTableName, combinedColumnTypes);
            if (mainRows == null) {
                mainRows = table.getRows();
            }

            List<Map<String, Map<String, Object>>> joinedRows = new ArrayList<>();
            for (Map<String, Object> mainRow : mainRows) {
                Map<String, Map<String, Object>> wrapped = new HashMap<>(2);
                wrapped.put(mainTableName, mainRow);
                joinedRows.add(wrapped);
            }

            boolean useStreaming = shouldUseStreaming(mainRows, tables);
            StreamingResultIterator spill = null;
            List<Map<String, Object>> spillFallback = null;
            boolean[] spillActive = { false };
            if (useStreaming) {
                spillFallback = new ArrayList<>();
                try {
                    spill = new StreamingResultIterator(MAX_IN_MEMORY_ROWS);
                    spillActive[0] = true;
                } catch (IOException e) {
                    spillActive[0] = false;
                }
            }

            for (QueryParser.JoinInfo join : joins) {
                Table joinTable = tables.get(join.tableName);
                List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();

                boolean useHashJoin = canUseHashJoin(join, combinedColumnTypes);
                if (hasOrInOnConditions(join)) {
                    LOGGER.warning("WARNING: JOIN with OR condition may produce large result set");
                }
                LOGGER.log(Level.FINE, "Join on {0}: useHashJoin={1}", new Object[]{join.tableName, useHashJoin});

                if (useHashJoin) {
                    Table buildTable = joinTable.rowCount() <= mainRows.size() ? joinTable : tables.get(mainTableName);
                    Table probeTable = buildTable == joinTable ? tables.get(mainTableName) : joinTable;
                    String buildTableName = buildTable == joinTable ? join.tableName : mainTableName;
                    String probeTableName = probeTable == joinTable ? join.tableName : mainTableName;

                    QueryParser.Condition equalityCondition = join.onConditions.stream()
                            .filter(c -> c.operator == QueryParser.Operator.EQUALS && c.isColumnComparison())
                            .findFirst()
                            .orElse(null);
                    if (equalityCondition == null) {
                        throw new IllegalStateException("No equality condition for hash join");
                    }
                    String buildColumn = resolveJoinColumn(equalityCondition, buildTableName);
                    String probeColumn = resolveJoinColumn(equalityCondition, probeTableName);
                    if (buildColumn == null || probeColumn == null) {
                        throw new IllegalStateException("Hash join equality column does not reference tables " + buildTableName + " and " + probeTableName);
                    }

                    Map<Object, List<Map<String, Object>>> hashTable = new HashMap<>();
                    List<Map<String, Object>> buildRows = getIndexedRows(buildTable, join.onConditions, buildTableName, combinedColumnTypes);
                    if (buildRows == null) {
                        buildRows = buildTable.getRows();
                    }
                    String buildColumnKey = normalizeColumnKey(buildColumn, buildTableName);
                    for (int i = 0; i < buildRows.size(); i++) {
                        Map<String, Object> row = buildRows.get(i);
                        Object key = row.get(buildColumnKey);
                        if (key != null) {
                            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                            ReentrantReadWriteLock lock = buildTable.getRowLock(i);
                            lock.readLock().lock();
                            acquiredLocks.add(lock);
                        }
                    }

                    // Remove the redundant declaration and clear the existing newJoinedRows
                    newJoinedRows.clear(); // Clear the list to reuse it
                    List<Map<String, Object>> probeRows = getIndexedRows(probeTable, join.onConditions, probeTableName, combinedColumnTypes);
                    if (probeRows == null) {
                        probeRows = probeTable.getRows();
                    }

                    // When the ON clause is exactly one plain equality, the hash
                    // match already guarantees the condition, so we skip the
                    // flatten+evaluate round trip entirely.
                    boolean onlyEquality = join.onConditions.size() == 1 && equalityCondition != null && !equalityCondition.not;
                    boolean lastStream = useStreaming && join == joins.get(joins.size() - 1);
                    String probeColumnKey = normalizeColumnKey(probeColumn, probeTableName);

                    for (Map<String, Object> probeRow : probeRows) {
                        Object probeKey = probeRow.get(probeColumnKey);
                        if (probeKey != null) {
                            List<Map<String, Object>> matches = hashTable.get(probeKey);
                            if (matches != null) {
                                for (Map<String, Object> buildRow : matches) {
                                    if (onlyEquality) {
                                        if (lastStream) {
                                            Map<String, Object> flatRow = new HashMap<>((probeRow.size() + buildRow.size()) * 4 / 3 + 1);
                                            flattenInto(flatRow, probeRow, probeTableName);
                                            flattenInto(flatRow, buildRow, buildTableName);
                                            spillFilteredRow(spill, spillActive, spillFallback, flatRow, conditions, combinedColumnTypes, tables);
                                        } else {
                                            Map<String, Map<String, Object>> newRow = new HashMap<>(2);
                                            newRow.put(probeTableName, probeRow);
                                            newRow.put(buildTableName, buildRow);
                                            newJoinedRows.add(newRow);
                                        }
                                    } else {
                                        Map<String, Object> flattenedRow = flattenJoinedPair(probeRow, probeTableName, buildRow, buildTableName);
                                        if (evaluateConditions(flattenedRow, join.onConditions, combinedColumnTypes, tables)) {
                                            if (lastStream) {
                                                spillFilteredRow(spill, spillActive, spillFallback, flattenedRow, conditions, combinedColumnTypes, tables);
                                            } else {
                                                Map<String, Map<String, Object>> newRow = new HashMap<>(2);
                                                newRow.put(probeTableName, probeRow);
                                                newRow.put(buildTableName, buildRow);
                                                newJoinedRows.add(newRow);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    joinedRows = newJoinedRows;
                    LOGGER.log(Level.FINE, "Hash join completed: {0} rows produced for join on {1}",
                            new Object[]{newJoinedRows.size(), join.tableName});
                } else {
                    List<Map<String, Object>> joinRows = getIndexedRows(joinTable, join.onConditions, join.tableName, combinedColumnTypes);
                    if (joinRows == null) {
                        joinRows = joinTable.getRows();
                    }

                    String rightPrefix = join.tableName + ".";
                    List<String> rightSrcKeys;
                    List<String> rightTargetKeys;
                    if (!joinRows.isEmpty()) {
                        rightSrcKeys = new ArrayList<>(joinRows.get(0).keySet());
                        rightTargetKeys = new ArrayList<>(rightSrcKeys.size());
                        for (String sk : rightSrcKeys) {
                            rightTargetKeys.add(rightPrefix + sk);
                        }
                    } else {
                        rightSrcKeys = Collections.emptyList();
                        rightTargetKeys = Collections.emptyList();
                    }

                    boolean lastStream = useStreaming && join == joins.get(joins.size() - 1);
                    boolean equalsJoin = join.onConditions.isEmpty() && join.leftColumn != null && join.rightColumn != null;
                    String leftJoinKey = equalsJoin ? normalizeColumnKey(join.leftColumn, join.originalTable) : null;
                    String rightJoinKey = equalsJoin ? normalizeColumnKey(join.rightColumn, join.tableName) : null;

                    // Row locks are keyed by row index, not by pair, so acquire
                    // the whole join table's read locks once instead of once per
                    // (outer x inner) pair.
                    for (int j = 0; j < joinRows.size(); j++) {
                        ReentrantReadWriteLock joinLock = joinTable.getRowLock(j);
                        joinLock.readLock().lock();
                        acquiredLocks.add(joinLock);
                    }

                    for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
                        Map<String, Object> evalRow = flattenJoinedRow(currentJoin);
                        if (lastStream) {
                            Map<String, Object> leftRow = equalsJoin ? currentJoin.get(join.originalTable) : null;
                            Object leftValue = equalsJoin ? leftRow.get(leftJoinKey) : null;
                            for (int j = 0; j < joinRows.size(); j++) {
                                Map<String, Object> rightRow = joinRows.get(j);
                                Map<String, Object> flatRow = new HashMap<>(evalRow.size() + rightSrcKeys.size() + 1);
                                flatRow.putAll(evalRow);
                                for (int k = 0; k < rightSrcKeys.size(); k++) {
                                    flatRow.put(rightTargetKeys.get(k), rightRow.get(rightSrcKeys.get(k)));
                                }
                                boolean matches;
                                if (join.joinType == QueryParser.JoinType.CROSS) {
                                    matches = true;
                                } else if (equalsJoin) {
                                    matches = valuesEqual(leftValue, rightRow.get(rightJoinKey));
                                } else if (!join.onConditions.isEmpty()) {
                                    matches = evaluateConditions(flatRow, join.onConditions, combinedColumnTypes, tables);
                                } else {
                                    throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
                                }
                                if (matches) {
                                    spillFilteredRow(spill, spillActive, spillFallback, flatRow, conditions, combinedColumnTypes, tables);
                                }
                            }
                        } else {
                            for (int j = 0; j < joinRows.size(); j++) {
                                Map<String, Object> rightRow = joinRows.get(j);
                                Map<String, Map<String, Object>> newRow = new HashMap<>(currentJoin);
                                newRow.put(join.tableName, rightRow);

                                if (join.joinType == QueryParser.JoinType.CROSS) {
                                    newJoinedRows.add(newRow);
                                } else if (equalsJoin) {
                                    Map<String, Object> leftRow = currentJoin.get(join.originalTable);
                                    if (!valuesEqual(leftRow.get(leftJoinKey), rightRow.get(rightJoinKey))) {
                                        continue;
                                    }
                                    newJoinedRows.add(newRow);
                                } else if (!join.onConditions.isEmpty()) {
                                    for (int k = 0; k < rightSrcKeys.size(); k++) {
                                        evalRow.put(rightTargetKeys.get(k), rightRow.get(rightSrcKeys.get(k)));
                                    }
                                    if (!evaluateConditions(evalRow, join.onConditions, combinedColumnTypes, tables)) {
                                        continue;
                                    }
                                    newJoinedRows.add(newRow);
                                    LOGGER.log(Level.FINE, "JOIN ON condition satisfied for {0} with conditions: {1}",
                                            new Object[]{join.tableName, join.onConditions});
                                } else {
                                    throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
                                }
                            }
                        }
                    }
                }
                joinedRows = newJoinedRows;
            }

            List<Map<String, Object>> filteredRows;
            if (useStreaming) {
                filteredRows = new ArrayList<>();
                if (spillActive[0] && spill != null) {
                    try {
                        spill.finishWriting();
                        while (spill.hasNext()) {
                            filteredRows.add(spill.next());
                        }
                    } catch (IOException e) {
                        spillActive[0] = false;
                    } finally {
                        spill.close();
                    }
                }
                if (!spillActive[0] && spillFallback != null) {
                    filteredRows = spillFallback;
                }
            } else {
                filteredRows = new ArrayList<>();
                for (Map<String, Map<String, Object>> joinedRow : joinedRows) {
                    Map<String, Object> flattenedRow = flattenJoinedRow(joinedRow);
                    if (conditions.isEmpty() || evaluateConditions(flattenedRow, conditions, combinedColumnTypes, tables)) {
                        filteredRows.add(flattenedRow);
                    }
                }
            }

            List<Map<String, Object>> finalRows;
            if (!groupBy.isEmpty()) {
                Map<List<Object>, List<Map<String, Object>>> groupedRows = filteredRows.stream()
                        .collect(Collectors.groupingBy(row -> groupBy.stream()
                                .map(col -> groupBySubQueries.containsKey(col)
                                        ? evaluateGroupBySubQuery(groupBySubQueries.get(col), row, database)
                                        : row.get(normalizeColumnName(col, mainTableName)))
                                .collect(Collectors.toList())));

                groupAggregateKeys.clear();
                for (QueryParser.AggregateFunction agg : aggregates) {
                    String resultKey = agg.alias != null ? agg.alias : agg.toString();
                    groupAggregateKeys.add(resultKey);
                }

                finalRows = new ArrayList<>();
                for (List<Object> groupKey : groupedRows.keySet()) {
                    List<Map<String, Object>> group = groupedRows.get(groupKey);
                    Map<String, Object> resultRow = new HashMap<>();

                    for (int i = 0; i < groupBy.size(); i++) {
                        String column = groupBy.get(i);
                        String normalizedColumn = normalizeColumnName(column, mainTableName);
                        resultRow.put(normalizedColumn, groupKey.get(i));
                    }

                    for (QueryParser.AggregateFunction agg : aggregates) {
                        String resultKey = agg.alias != null ? agg.alias : agg.toString();
                        resultRow.put(resultKey, computeAggregate(agg, group, combinedColumnTypes));
                    }

                    for (QueryParser.HavingCondition havingCondition : havingConditions) {
                        addMissingHavingAggregates(havingCondition, resultRow, group, combinedColumnTypes);
                    }

                    for (String column : columns) {
                        String normalizedColumn = normalizeColumnName(column, mainTableName);
                        if (!resultRow.containsKey(normalizedColumn)) {
                            Object value = group.get(0).get(normalizedColumn);
                            resultRow.put(normalizedColumn, value);
                        }
                    }

                    if (!havingConditions.isEmpty()) {
                        if (!evaluateHavingConditions(resultRow, havingConditions)) {
                            continue;
                        }
                    }

                    finalRows.add(resultRow);
                }
                LOGGER.log(Level.FINE, "Applied GROUP BY with {0} columns, produced {1} groups",
                        new Object[]{groupBy.size(), finalRows.size()});
            } else {
                finalRows = filteredRows;
            }

            if (!orderBy.isEmpty()) {
                finalRows.sort((row1, row2) -> compareRows(row1, row2, orderBy));
                LOGGER.log(Level.FINE, "Applied ORDER BY with {0} clauses (streaming={1})",
                        new Object[]{orderBy.size(), useStreaming && finalRows.size() > MAX_IN_MEMORY_ROWS});
            }

            int rowsSkipped = (offset != null) ? offset : 0;
            int maxRows = (limit != null) ? limit : Integer.MAX_VALUE;
            List<Map<String, Object>> selectedRows = new ArrayList<>();
            for (int i = 0; i < finalRows.size() && selectedRows.size() < maxRows; i++) {
                if (rowsSkipped > 0) {
                    rowsSkipped--;
                    continue;
                }
                selectedRows.add(finalRows.get(i));
            }

            if (!aggregates.isEmpty() && groupBy.isEmpty()) {
                Map<String, Object> resultRow = new HashMap<>();
                for (QueryParser.AggregateFunction agg : aggregates) {
                    String resultKey = agg.alias != null ? agg.alias : agg.toString();
                    resultRow.put(resultKey, computeAggregate(agg, selectedRows, combinedColumnTypes));
                }
                result.add(resultRow);
            } else {
                if (groupAggregateKeys.isEmpty()) {
                    for (Map<String, Object> row : selectedRows) {
                        result.add(filterColumns(row, columns));
                    }
                } else {
                    for (Map<String, Object> row : selectedRows) {
                        Map<String, Object> filteredRow = filterColumns(row, columns);
                        for (String aggregateKey : groupAggregateKeys) {
                            if (row.containsKey(aggregateKey)) {
                                filteredRow.put(aggregateKey, row.get(aggregateKey));
                            }
                        }
                        result.add(filteredRow);
                    }
                }
            }

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}, aggregates {3}, groupBy {4}, having={5}, limit={6}, offset={7}, orderBy={8}",
                    new Object[]{result.size(), mainTableName, joins, aggregates, groupBy, havingConditions, limit, offset, orderBy});
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private boolean shouldUseStreaming(List<Map<String, Object>> mainRows, Map<String, Table> tables) {
        if (joins.isEmpty()) {
            return false;
        }
        long estimate = mainRows.size();
        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = tables.get(join.tableName);
            if (joinTable == null) {
                continue;
            }
            long joinTableSize = joinTable.rowCount();
            if (join.joinType == QueryParser.JoinType.CROSS || hasOrInOnConditions(join)) {
                estimate *= joinTableSize;
            }
        }
        return estimate > MAX_IN_MEMORY_ROWS;
    }

    private void spillFilteredRow(StreamingResultIterator spill, boolean[] spillActive,
                                  List<Map<String, Object>> fallback,
                                  Map<String, Object> flattenedRow,
                                  List<QueryParser.Condition> whereConditions,
                                  Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        if (!whereConditions.isEmpty() && !evaluateConditions(flattenedRow, whereConditions, combinedColumnTypes, tables)) {
            return;
        }
        if (spillActive[0] && spill != null) {
            try {
                spill.add(flattenedRow);
                return;
            } catch (IOException e) {
                spillActive[0] = false;
                try {
                    spill.finishWriting();
                    while (spill.hasNext()) {
                        fallback.add(spill.next());
                    }
                } catch (IOException ignored) {
                } finally {
                    spill.close();
                }
            }
        }
        fallback.add(flattenedRow);
    }

    private static final byte BIN_NULL = 0;
    private static final byte BIN_STRING = 1;
    private static final byte BIN_CHAR = 2;
    private static final byte BIN_INT = 3;
    private static final byte BIN_LONG = 4;
    private static final byte BIN_BOOL = 5;
    private static final byte BIN_BIGDEC = 6;
    private static final byte BIN_FLOAT = 7;
    private static final byte BIN_DOUBLE = 8;
    private static final byte BIN_DATE = 9;
    private static final byte BIN_DATETIME = 10;
    private static final byte BIN_UUID = 11;

    private static void writeBinaryRow(DataOutputStream out, Map<String, Object> row) throws IOException {
        out.writeInt(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            out.writeUTF(entry.getKey());
            Object v = entry.getValue();
            if (v == null) {
                out.writeByte(BIN_NULL);
            } else if (v instanceof String s) {
                out.writeByte(BIN_STRING);
                writeBinaryUtf(out, s);
            } else if (v instanceof Character c) {
                out.writeByte(BIN_CHAR);
                out.writeChar(c);
            } else if (v instanceof Integer) {
                out.writeByte(BIN_INT);
                out.writeInt((Integer) v);
            } else if (v instanceof Long) {
                out.writeByte(BIN_LONG);
                out.writeLong((Long) v);
            } else if (v instanceof Boolean) {
                out.writeByte(BIN_BOOL);
                out.writeBoolean((Boolean) v);
            } else if (v instanceof BigDecimal) {
                out.writeByte(BIN_BIGDEC);
                writeBinaryUtf(out, v.toString());
            } else if (v instanceof Float) {
                out.writeByte(BIN_FLOAT);
                out.writeFloat((Float) v);
            } else if (v instanceof Double) {
                out.writeByte(BIN_DOUBLE);
                out.writeDouble((Double) v);
            } else if (v instanceof LocalDate) {
                out.writeByte(BIN_DATE);
                writeBinaryUtf(out, v.toString());
            } else if (v instanceof LocalDateTime) {
                out.writeByte(BIN_DATETIME);
                writeBinaryUtf(out, v.toString());
            } else if (v instanceof UUID) {
                out.writeByte(BIN_UUID);
                writeBinaryUtf(out, v.toString());
            } else {
                out.writeByte(BIN_STRING);
                writeBinaryUtf(out, String.valueOf(v));
            }
        }
    }

    private static void writeBinaryUtf(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static Map<String, Object> readBinaryRow(DataInputStream in) throws IOException {
        int n;
        try {
            n = in.readInt();
        } catch (EOFException eof) {
            return null;
        }
        Map<String, Object> row = new HashMap<>(n);
        for (int k = 0; k < n; k++) {
            String key = in.readUTF();
            byte t = in.readByte();
            Object v;
            switch (t) {
                case BIN_NULL:
                    v = null;
                    break;
                case BIN_STRING:
                    v = readBinaryUtf(in);
                    break;
                case BIN_CHAR:
                    v = in.readChar();
                    break;
                case BIN_INT:
                    v = in.readInt();
                    break;
                case BIN_LONG:
                    v = in.readLong();
                    break;
                case BIN_BOOL:
                    v = in.readBoolean();
                    break;
                case BIN_BIGDEC:
                    v = new BigDecimal(readBinaryUtf(in));
                    break;
                case BIN_FLOAT:
                    v = in.readFloat();
                    break;
                case BIN_DOUBLE:
                    v = in.readDouble();
                    break;
                case BIN_DATE:
                    v = LocalDate.parse(readBinaryUtf(in));
                    break;
                case BIN_DATETIME:
                    v = LocalDateTime.parse(readBinaryUtf(in));
                    break;
                case BIN_UUID:
                    v = UUID.fromString(readBinaryUtf(in));
                    break;
                default:
                    v = readBinaryUtf(in);
            }
            row.put(key, v);
        }
        return row;
    }

    private static String readBinaryUtf(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    /**
     * One entry of the pre-resolved SELECT projection plan used by
     * {@link #filterColumns}. {@code normalized} is the row key read on the
     * fast path; when it is absent the alias fallback keys are tried.
     */
    private static final class ColumnProjection {
        final String normalized;
        final String alias;
        final List<String> fallbackKeys;

        ColumnProjection(String normalized, String alias, List<String> fallbackKeys) {
            this.normalized = normalized;
            this.alias = alias;
            this.fallbackKeys = fallbackKeys;
        }
    }

    private static final class StreamingResultIterator implements Iterator<Map<String, Object>>, AutoCloseable {
        private final long maxInMemoryRows;
        private final List<Map<String, Object>> inMemory = new ArrayList<>();
        private File spillFile;
        private DataOutputStream writer;
        private DataInputStream reader;
        private boolean spilled = false;
        private boolean writing = true;
        private int readIndex = 0;
        private Map<String, Object> peekedRow;
        private boolean peeked = false;

        StreamingResultIterator(long maxInMemoryRows) throws IOException {
            this.maxInMemoryRows = maxInMemoryRows;
        }

        void add(Map<String, Object> row) throws IOException {
            if (!writing) {
                throw new IllegalStateException("iterator is closed for writing");
            }
            if (!spilled) {
                if (inMemory.size() < maxInMemoryRows) {
                    inMemory.add(row);
                    return;
                }
                spillFile = Files.createTempFile("diesel-spill-", ".tmp").toFile();
                writer = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(spillFile), 1024 * 1024));
                for (Map<String, Object> r : inMemory) {
                    writeBinaryRow(writer, r);
                }
                inMemory.clear();
                spilled = true;
            }
            writeBinaryRow(writer, row);
        }

        void finishWriting() throws IOException {
            if (writer != null) {
                writer.flush();
                writer.close();
                writer = null;
            }
            if (spilled && reader == null && spillFile != null) {
                reader = new DataInputStream(
                        new BufferedInputStream(new FileInputStream(spillFile), 1024 * 1024));
            }
            writing = false;
        }

        @Override
        public boolean hasNext() {
            if (writing) {
                throw new IllegalStateException("iterator still writing");
            }
            if (!spilled) {
                return readIndex < inMemory.size();
            }
            if (!peeked) {
                try {
                    peekedRow = readBinaryRow(reader);
                } catch (IOException e) {
                    peekedRow = null;
                }
                peeked = true;
            }
            return peekedRow != null;
        }

        @Override
        public Map<String, Object> next() {
            if (writing) {
                throw new IllegalStateException("iterator still writing");
            }
            if (!spilled) {
                return inMemory.get(readIndex++);
            }
            if (!peeked) {
                hasNext();
            }
            Map<String, Object> row = peekedRow;
            peeked = false;
            peekedRow = null;
            return row;
        }

        @Override
        public void close() {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException ignored) {
            }
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException ignored) {
            }
            if (spillFile != null) {
                try {
                    Files.deleteIfExists(spillFile.toPath());
                } catch (IOException ignored) {
                }
            }
        }
    }

    private boolean evaluateHavingConditions(Map<String, Object> row, List<QueryParser.HavingCondition> havingConditions) {
        if (havingConditions.isEmpty()) {
            return true;
        }
        boolean result = evaluateHavingCondition(row, havingConditions.get(0));
        for (int i = 1; i < havingConditions.size(); i++) {
            QueryParser.HavingCondition condition = havingConditions.get(i);
            boolean conditionResult = evaluateHavingCondition(row, condition);
            String conjunction = condition.conjunction;
            if (conjunction == null || conjunction.equals("AND")) {
                result = result && conditionResult;
            } else if (conjunction.equals("OR")) {
                result = result || conditionResult;
            }
        }
        return result;
    }

    private boolean evaluateHavingCondition(Map<String, Object> row, QueryParser.HavingCondition condition) {
        boolean conditionResult;
        if (condition.isGrouped()) {
            conditionResult = evaluateHavingConditions(row, condition.subConditions);
            conditionResult = condition.not ? !conditionResult : conditionResult;
        } else {
            String key = condition.aggregate.alias != null ? condition.aggregate.alias : condition.aggregate.toString();
            Object actualValue = row.get(key);
            if (actualValue == null) {
                conditionResult = condition.not;
            } else {
                int comparison = compareValues(actualValue, condition.value);
                conditionResult = switch (condition.operator) {
                    case EQUALS -> comparison == 0;
                    case NOT_EQUALS -> comparison != 0;
                    case LESS_THAN -> comparison < 0;
                    case GREATER_THAN -> comparison > 0;
                    case LESS_THAN_OR_EQUALS -> comparison <= 0;
                    case GREATER_THAN_OR_EQUALS -> comparison >= 0;
                    default -> throw new IllegalStateException("Unsupported operator in HAVING: " + condition.operator);
                };
                conditionResult = condition.not ? !conditionResult : conditionResult;
            }
        }
        return conditionResult;
    }

    private void addMissingHavingAggregates(QueryParser.HavingCondition condition, Map<String, Object> resultRow,
                                            List<Map<String, Object>> group, Map<String, Class<?>> combinedColumnTypes) {
        if (condition.isGrouped()) {
            for (QueryParser.HavingCondition subCondition : condition.subConditions) {
                addMissingHavingAggregates(subCondition, resultRow, group, combinedColumnTypes);
            }
            return;
        }
        String key = condition.aggregate.alias != null ? condition.aggregate.alias : condition.aggregate.toString();
        if (!resultRow.containsKey(key)) {
            resultRow.put(key, computeAggregate(condition.aggregate, group, combinedColumnTypes));
        }
    }

    private Object computeAggregate(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows,
                                    Map<String, Class<?>> combinedColumnTypes) {
        if (agg.functionName.equals("COUNT")) {
            long count;
            if (agg.column == null) {
                count = rows.size();
            } else {
                String columnKey = normalizeColumnName(agg.column, mainTableName);
                count = rows.stream().filter(row -> row.get(columnKey) != null).count();
            }
            return count;
        } else if (agg.functionName.equals("MIN")) {
            if (agg.column == null) {
                throw new IllegalArgumentException("MIN requires a column argument");
            }
            String columnKey = normalizeColumnName(agg.column, mainTableName);
            return rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                    .min(this::compareValues).orElse(null);
        } else if (agg.functionName.equals("MAX")) {
            if (agg.column == null) {
                throw new IllegalArgumentException("MAX requires a column argument");
            }
            String columnKey = normalizeColumnName(agg.column, mainTableName);
            return rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                    .max(this::compareValues).orElse(null);
        } else if (agg.functionName.equals("AVG")) {
            if (agg.column == null) {
                throw new IllegalArgumentException("AVG requires a column argument");
            }
            String columnKey = normalizeColumnName(agg.column, mainTableName);
            List<Object> values = rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (values.isEmpty()) {
                return null;
            }
            BigDecimal sum = BigDecimal.ZERO;
            long count = 0;
            for (Object value : values) {
                if (value instanceof Number) {
                    sum = sum.add(new BigDecimal(value.toString()));
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            BigDecimal avg = sum.divide(BigDecimal.valueOf(count), 10, RoundingMode.HALF_UP);
            Class<?> columnType = combinedColumnTypes.get(columnKey);
            if (columnType == Float.class) {
                return avg.floatValue();
            } else if (columnType == Double.class) {
                return avg.doubleValue();
            } else if (columnType == Integer.class) {
                return avg.intValue();
            } else if (columnType == Long.class) {
                return avg.longValue();
            } else if (columnType == Short.class) {
                return avg.shortValue();
            } else if (columnType == Byte.class) {
                return avg.byteValue();
            }
            return avg;
        } else if (agg.functionName.equals("SUM")) {
            if (agg.column == null) {
                throw new IllegalArgumentException("SUM requires a column argument");
            }
            String columnKey = normalizeColumnName(agg.column, mainTableName);
            List<Object> values = rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (values.isEmpty()) {
                return null;
            }
            BigDecimal sum = BigDecimal.ZERO;
            for (Object value : values) {
                if (value instanceof Number) {
                    sum = sum.add(new BigDecimal(value.toString()));
                }
            }
            Class<?> columnType = combinedColumnTypes.get(columnKey);
            if (columnType == Float.class) {
                return sum.floatValue();
            } else if (columnType == Double.class) {
                return sum.doubleValue();
            } else if (columnType == Integer.class) {
                return sum.intValue();
            } else if (columnType == Long.class) {
                return sum.longValue();
            } else if (columnType == Short.class) {
                return sum.shortValue();
            } else if (columnType == Byte.class) {
                return sum.byteValue();
            }
            return sum;
        } else {
            throw new UnsupportedOperationException("Aggregate function not supported: " + agg.functionName);
        }
    }

    private int compareRows(Map<String, Object> row1, Map<String, Object> row2, List<QueryParser.OrderByInfo> orderBy) {
        for (int i = 0; i < orderBy.size(); i++) {
            QueryParser.OrderByInfo order = orderBy.get(i);
            Object value1 = row1.get(orderByKeys.get(i));
            Object value2 = row2.get(orderByKeys.get(i));

            if (value1 == null && value2 == null) {
                continue;
            }
            if (value1 == null) {
                return order.ascending ? -1 : 1;
            }
            if (value2 == null) {
                return order.ascending ? 1 : -1;
            }

            int comparison = compareValues(value1, value2);
            if (comparison != 0) {
                return order.ascending ? comparison : -comparison;
            }
        }
        return 0;
    }

    /**
     * Resolves the flattened row key each ORDER BY clause reads, so the hot
     * comparator never re-does alias/select-column resolution per comparison.
     * Must be called after all table aliases have been registered.
     *
     * @return the resolved row keys, aligned with {@code orderBy}
     */
    private List<String> resolveOrderByKeys() {
        List<String> keys = new ArrayList<>(orderBy.size());
        for (QueryParser.OrderByInfo order : orderBy) {
            String column = order.column;
            String normalizedColumn = null;
            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;

            for (String selectColumn : columns) {
                String[] parts = selectColumn.trim().split("\\s+AS\\s+|\\s+", 2);
                String columnAlias = parts.length > 1 ? parts[1].trim() : normalizeColumnKey(selectColumn, mainTableName);
                if (unqualifiedColumn.equalsIgnoreCase(columnAlias)) {
                    normalizedColumn = columnAlias;
                    break;
                }
            }

            if (normalizedColumn == null) {
                for (Map.Entry<String, String> aliasEntry : tableAliases.entrySet()) {
                    if (column.equalsIgnoreCase(aliasEntry.getKey() + "." + unqualifiedColumn)) {
                        normalizedColumn = aliasEntry.getValue() + "." + unqualifiedColumn;
                        break;
                    }
                }
            }

            if (normalizedColumn == null) {
                normalizedColumn = normalizeColumnName(column, mainTableName);
            }
            keys.add(normalizedColumn);
        }
        return keys;
    }

    private boolean canUseHashJoin(QueryParser.JoinInfo join, Map<String, Class<?>> combinedColumnTypes) {
        if (join.joinType != QueryParser.JoinType.INNER &&
                join.joinType != QueryParser.JoinType.LEFT_INNER &&
                join.joinType != QueryParser.JoinType.RIGHT_INNER) {
            return false;
        }
        boolean hasEquality = false;
        for (QueryParser.Condition condition : join.onConditions) {
            if ("OR".equalsIgnoreCase(condition.conjunction)) {
                return false;
            }
            if (condition.operator == QueryParser.Operator.EQUALS && condition.isColumnComparison()) {
                hasEquality = true;
            }
        }
        return hasEquality;
    }

    /**
     * Returns true when any ON condition of the join uses a logical OR
     * conjunction. Such joins fall back to the nested loop and may produce
     * a large (cross-product-like) result set.
     *
     * @param join the join clause to inspect
     * @return true if an OR conjunction is present in the ON conditions
     */
    private boolean hasOrInOnConditions(QueryParser.JoinInfo join) {
        if (join.onConditions == null) {
            return false;
        }
        return join.onConditions.stream()
                .anyMatch(c -> c.conjunction != null && "OR".equalsIgnoreCase(c.conjunction));
    }

    /**
     * Reorders multi-join clauses so that smaller tables are joined first,
     * keeping intermediate nested-loop results small. Only applied when every
     * join is an inner-style join, where join order does not change the
     * result set.
     *
     * @param tables the resolved table map (table name to table)
     */
    private void reorderJoinsForNestedLoop(Map<String, Table> tables) {
        if (joins.size() <= 1) {
            return;
        }
        boolean allInner = joins.stream().allMatch(j -> j.joinType == QueryParser.JoinType.INNER
                || j.joinType == QueryParser.JoinType.LEFT_INNER
                || j.joinType == QueryParser.JoinType.RIGHT_INNER);
        if (!allInner) {
            return;
        }
        joins.sort(Comparator.comparingInt(j -> tables.get(j.tableName).rowCount()));
    }

    private String resolveJoinColumn(QueryParser.Condition condition, String tableName) {
        String leftTable = normalizeColumnName(condition.column, mainTableName).split("\\.")[0];
        String rightTable = normalizeColumnName(condition.rightColumn, mainTableName).split("\\.")[0];
        if (leftTable.equals(tableName)) {
            return condition.column;
        }
        if (rightTable.equals(tableName)) {
            return condition.rightColumn;
        }
        return null;
    }

    private List<Map<String, Object>> getIndexedRows(Table table, List<QueryParser.Condition> conditions, String tableName, Map<String, Class<?>> combinedColumnTypes) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }

        // An index pre-filter must never be applied when conditions are OR-combined:
        // the pre-filter uses the first indexed condition only, so rows that match
        // a later OR branch would be dropped before the WHERE evaluation runs.
        for (QueryParser.Condition condition : conditions) {
            if (condition.conjunction != null && "OR".equalsIgnoreCase(condition.conjunction)) {
                return null;
            }
        }

        for (QueryParser.Condition condition : conditions) {
            if (condition.isGrouped() || condition.isColumnComparison()) {
                continue;
            }

            // Negated conditions (NOT IN / NOT EQUALS / ...) must not use the index
            // pre-filter: the index lookup returns the rows the condition rejects.
            if (condition.not) {
                continue;
            }

            String columnName = normalizeColumnName(condition.column, tableName);
            String unqualifiedColumn = normalizeColumnKey(columnName, tableName);
            Index index = table.getIndex(unqualifiedColumn);
            if (index == null && table.hasClusteredIndex() && unqualifiedColumn.equals(table.getClusteredIndexColumn())) {
                index = table.getClusteredIndex();
            }

            if (index != null) {
                List<Integer> rowIndices = new ArrayList<>();
                if (condition.operator == QueryParser.Operator.EQUALS && condition.value != null) {
                    rowIndices.addAll(index.search(condition.value));
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for EQUALS condition, found {2} rows",
                            new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
                } else if (condition.isInOperator() && condition.inValues != null) {
                    for (Object inValue : condition.inValues) {
                        rowIndices.addAll(index.search(inValue));
                    }
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for IN condition, found {2} rows",
                            new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
                } else if (index instanceof BTreeIndex && (condition.operator == QueryParser.Operator.LESS_THAN || condition.operator == QueryParser.Operator.GREATER_THAN)) {
                    BTreeIndex bTreeIndex = (BTreeIndex) index;
                    Object low = condition.operator == QueryParser.Operator.GREATER_THAN ? condition.value : null;
                    Object high = condition.operator == QueryParser.Operator.LESS_THAN ? condition.value : null;
                    rowIndices.addAll(bTreeIndex.rangeSearch(low, high));
                    LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for range condition {2}, found {3} rows",
                            new Object[]{tableName, unqualifiedColumn, condition.operator, rowIndices.size()});
                }

                if (!rowIndices.isEmpty()) {
                    List<Map<String, Object>> indexedRows = new ArrayList<>();
                    int tableSize = table.rowCount();
                    for (int idx : rowIndices) {
                        if (idx >= 0 && idx < tableSize) {
                            indexedRows.add(table.getRows().get(idx));
                        }
                    }
                    return indexedRows;
                }
            }
        }

        return null;
    }

    private boolean valuesEqual(Object left, Object right) {
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Float && right instanceof Float) {
            return Math.abs(((Float) left) - ((Float) right)) < 1e-7;
        } else if (left instanceof Double && right instanceof Double) {
            return Math.abs(((Double) left) - ((Double) right)) < 1e-7;
        } else if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).compareTo((BigDecimal) right) == 0;
        } else if (left.getClass() == right.getClass()) {
            return left.equals(right);
        }
        return String.valueOf(left).equals(String.valueOf(right));
    }

    private Map<String, Object> flattenJoinedRow(Map<String, Map<String, Object>> joinedRow) {
        Map<String, Object> flattened = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> tableEntry : joinedRow.entrySet()) {
            String tableName = tableEntry.getKey();
            flattenInto(flattened, tableEntry.getValue(), tableName);
        }
        return flattened;
    }

    /**
     * Flattens two plain table rows under their table prefixes into a single
     * row map in one allocation. Used by the hash-join probe loop, which only
     * ever deals with a (probe, build) pair.
     */
    private Map<String, Object> flattenJoinedPair(Map<String, Object> leftRow, String leftTable,
                                                  Map<String, Object> rightRow, String rightTable) {
        Map<String, Object> flat = new HashMap<>((leftRow.size() + rightRow.size()) * 4 / 3 + 1);
        flattenInto(flat, leftRow, leftTable);
        flattenInto(flat, rightRow, rightTable);
        return flat;
    }

    private void flattenInto(Map<String, Object> target, Map<String, Object> row, String tableName) {
        for (Map.Entry<String, Object> columnEntry : row.entrySet()) {
            target.put(tableName + "." + columnEntry.getKey(), columnEntry.getValue());
        }
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        return ThreeValuedLogic.isTrue(evaluateConditions3vl(row, conditions, combinedColumnTypes, tables));
    }

    /**
     * Вычисляет список условий по правилам трёхзначной логики SQL
     * (см. {@link ThreeValuedLogic}). Правый операнд не вычисляется, если левый
     * уже определяет результат: {@code TRUE OR X = TRUE}, {@code FALSE AND X = FALSE}.
     */
    private Boolean evaluateConditions3vl(Map<String, Object> row, List<QueryParser.Condition> conditions,
                                          Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        if (conditions.isEmpty()) {
            return Boolean.TRUE;
        }
        logConditionEvaluation(conditions);

        // SQL precedence: AND binds tighter than OR, so the flat condition list
        // is evaluated as a disjunction of AND-segments. Each segment short-
        // circuits: FALSE AND anything = FALSE (skip the rest of the segment)
        // and TRUE OR anything = TRUE (skip the remaining segments). A null
        // (UNKNOWN) result must not be conflated with an uninitialized
        // accumulator, hence the explicit initialized flags.
        Boolean orResult = null;
        boolean orInitialized = false;
        Boolean andResult = null;
        boolean andInitialized = false;

        for (int i = 0; i < conditions.size(); i++) {
            QueryParser.Condition condition = conditions.get(i);
            String conjunction = condition.conjunction;
            boolean orBoundary = i > 0 && conjunction != null && conjunction.equalsIgnoreCase("OR");

            if (orBoundary) {
                orResult = orInitialized ? ThreeValuedLogic.or(orResult, andResult) : andResult;
                orInitialized = true;
                if (ThreeValuedLogic.orIsDetermined(orResult)) {
                    return orResult;
                }
                andResult = null;
                andInitialized = false;
            }

            if (!andInitialized) {
                andResult = evaluateCondition3vl(row, condition, combinedColumnTypes, tables);
                andInitialized = true;
                continue;
            }

            if (ThreeValuedLogic.andIsDetermined(andResult)) {
                while (i + 1 < conditions.size()) {
                    String nextConj = conditions.get(i + 1).conjunction;
                    if (nextConj != null && nextConj.equalsIgnoreCase("OR")) {
                        break;
                    }
                    i++;
                }
                continue;
            }

            Boolean value = evaluateCondition3vl(row, condition, combinedColumnTypes, tables);
            andResult = ThreeValuedLogic.and(andResult, value);
        }

        return orInitialized ? ThreeValuedLogic.or(orResult, andResult) : andResult;
    }

    private void logConditionEvaluation(List<QueryParser.Condition> conditions) {
        if (!LOGGER.isLoggable(Level.FINE)) {
            return;
        }
        StringBuilder sb = new StringBuilder("Evaluating WHERE: ");
        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                String conjunction = conditions.get(i).conjunction;
                sb.append(' ').append(conjunction != null ? conjunction : "AND").append(' ');
            }
            sb.append(conditions.get(i));
        }
        LOGGER.fine(sb.toString());
    }

    private Boolean evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition,
                                         Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        if (condition.isGrouped()) {
            Boolean subResult = evaluateConditions3vl(row, condition.subConditions, combinedColumnTypes, tables);
            return condition.not ? ThreeValuedLogic.not(subResult) : subResult;
        }

        if (condition.isNullOperator()) {
            String column = normalizeColumnName(condition.column, mainTableName);
            Object value = row.get(column);
            boolean isNull = value == null;
            boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
            return condition.not ? Boolean.valueOf(!result) : Boolean.valueOf(result);
        }

        if (condition.isInOperator()) {
            String column = normalizeColumnName(condition.column, mainTableName);
            Object value = row.get(column);
            if (value == null) {
                return null;
            }

            List<Object> inValues;
            if (condition.subQuery != null) {
                Database database = tables.get(mainTableName).getDatabase();
                String subQueryString = condition.subQuery.toString().trim();
                if (subQueryString.startsWith("(") && subQueryString.endsWith(")")) {
                    subQueryString = subQueryString.substring(1, subQueryString.length() - 1).trim();
                }
                // Ключ кэша строится после подстановки значений внешних колонок:
                // некоррелированный подзапрос выполняется один раз на весь SELECT,
                // коррелированный - один раз на каждый уникальный набор значений.
                String resolvedSubQuery = substituteOuterReferences(subQueryString, row);
                inValues = inSubQueryCache.computeIfAbsent(resolvedSubQuery, key -> {
                    LOGGER.log(Level.FINE, "Executing subquery: {0}", key);
                    Object subQueryResult = database.executeQuery(key, transactionId);
                    if (!(subQueryResult instanceof List)) {
                        throw new IllegalStateException("Subquery must return a list of rows");
                    }
                    List<Object> values = new ArrayList<>();
                    for (Map<String, Object> subRow : (List<Map<String, Object>>) subQueryResult) {
                        if (!subRow.isEmpty()) {
                            values.add(subRow.values().iterator().next());
                        }
                    }
                    return values;
                });
            } else {
                inValues = condition.inValues;
            }

            if (inValues == null) {
                throw new IllegalStateException("IN condition has no values or subquery results");
            }

            // Fast path: exact-equals lookup in a pre-built HashSet (built once at
            // parse time). If the row value equals some list value, valuesEqual is
            // guaranteed to agree (same class + equals -> valuesEqual true), so a hit
            // is definitive. Only on a miss do we fall back to the linear
            // valuesEqual scan, which preserves the epsilon (Float/Double) and
            // scale-insensitive (BigDecimal) semantics that a HashSet cannot express.
            boolean inResult = condition.inValueSet != null && condition.inValueSet.contains(value)
                    || inValues.stream().anyMatch(v -> valuesEqual(v, value));
            boolean result = condition.not ? !inResult : inResult;
            return Boolean.valueOf(result);
        }

        if (condition.isColumnComparison()) {
            String leftColumn = normalizeColumnName(condition.column, mainTableName);
            String rightColumn = normalizeColumnName(condition.rightColumn, mainTableName);
            Object leftValue = row.get(leftColumn);
            Object rightValue = row.get(rightColumn);
            return compareConditionOperand(leftValue, rightValue, condition);
        }

        String column = normalizeColumnName(condition.column, mainTableName);
        Object rowValue = row.get(column);
        if (condition.subQuery != null) {
            Database database = tables.get(mainTableName).getDatabase();
            String subQueryString = condition.subQuery.toString();
            String resolvedKey = substituteOuterReferences(subQueryString, row);
            Object subQueryValue = scalarSubQueryCache.computeIfAbsent(resolvedKey,
                    key -> evaluateGroupBySubQuery(key, Collections.emptyMap(), database));
            return compareConditionOperand(rowValue, subQueryValue, condition);
        }
        Object conditionValue = condition.value;
        return compareConditionOperand(rowValue, conditionValue, condition);
    }

    private Boolean compareConditionOperand(Object leftValue, Object rightValue, QueryParser.Condition condition) {
        if (leftValue == null || rightValue == null) {
            return null;
        }
        boolean comparisonResult;
        switch (condition.operator) {
            case EQUALS:
                comparisonResult = valuesEqual(leftValue, rightValue);
                break;
            case NOT_EQUALS:
                comparisonResult = !valuesEqual(leftValue, rightValue);
                break;
            case LESS_THAN:
                comparisonResult = compareValues(leftValue, rightValue) < 0;
                break;
            case GREATER_THAN:
                comparisonResult = compareValues(leftValue, rightValue) > 0;
                break;
            case LESS_THAN_OR_EQUALS:
                comparisonResult = compareValues(leftValue, rightValue) <= 0;
                break;
            case GREATER_THAN_OR_EQUALS:
                comparisonResult = compareValues(leftValue, rightValue) >= 0;
                break;
            case LIKE:
                comparisonResult = likeComparison(leftValue, rightValue);
                break;
            case NOT_LIKE:
                comparisonResult = !likeComparison(leftValue, rightValue);
                break;
            default:
                throw new IllegalStateException("Unsupported operator: " + condition.operator);
        }
        return condition.not ? Boolean.valueOf(!comparisonResult) : Boolean.valueOf(comparisonResult);
    }

    private int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }

        if (left instanceof Number && right instanceof Number) {
            if (left instanceof BigDecimal && right instanceof BigDecimal) {
                return ((BigDecimal) left).compareTo((BigDecimal) right);
            }
            // Fast paths for the common integral types: exact and allocation-free.
            // Mixed integral types compare as long, matching decimal ordering.
            if (isIntegral(left) && isIntegral(right)) {
                return Long.compare(((Number) left).longValue(), ((Number) right).longValue());
            }
            BigDecimal leftBD = new BigDecimal(left.toString());
            BigDecimal rightBD = new BigDecimal(right.toString());
            return leftBD.compareTo(rightBD);
        } else if (left instanceof LocalDate && right instanceof LocalDate) {
            return ((LocalDate) left).compareTo((LocalDate) right);
        } else if (left instanceof LocalDateTime && right instanceof LocalDateTime) {
            return ((LocalDateTime) left).compareTo((LocalDateTime) right);
        } else if (left instanceof Boolean && right instanceof Boolean) {
            return ((Boolean) left).compareTo((Boolean) right);
        } else if (left instanceof UUID && right instanceof UUID) {
            return ((UUID) left).compareTo((UUID) right);
        } else if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        } else if (left instanceof Character && right instanceof Character) {
            return ((Character) left).compareTo((Character) right);
        } else {
            throw new IllegalArgumentException("Incompatible types for comparison: " + left.getClass() + " and " + right.getClass());
        }
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte;
    }

    private boolean likeComparison(Object value, Object pattern) {
        if (value == null || pattern == null) {
            return false;
        }
        String valueStr = value.toString();
        String patternStr = pattern.toString();
        Pattern compiled = likePatternCache.get(patternStr);
        if (compiled == null) {
            compiled = Pattern.compile(patternStr.replace("%", ".*").replace("_", "."));
            likePatternCache.put(patternStr, compiled);
        }
        return compiled.matcher(valueStr).matches();
    }

    private List<ColumnProjection> buildProjectionPlan() {
        List<ColumnProjection> plan = new ArrayList<>(columns.size());
        for (String column : columns) {
            String trimmed = column.trim();
            if (trimmed.equals("*")) {
                plan.add(new ColumnProjection(null, null, Collections.emptyList()));
                continue;
            }
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            String columnAlias = normalizeColumnKey(column, mainTableName);
            String[] parts = trimmed.split("\\s+AS\\s+|\\s+", 2);
            if (parts.length > 1) {
                columnAlias = parts[1].trim();
                if (!columnAlias.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                    columnAlias = normalizeColumnKey(column, mainTableName);
                }
            }
            List<String> fallbackKeys = new ArrayList<>();
            if (!column.contains(".")) {
                String unqualifiedColumn = column.trim();
                for (Map.Entry<String, String> aliasEntry : tableAliases.entrySet()) {
                    fallbackKeys.add(aliasEntry.getValue() + "." + unqualifiedColumn);
                }
            }
            plan.add(new ColumnProjection(normalizedColumn, columnAlias, fallbackKeys));
        }
        return plan;
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        List<ColumnProjection> plan = projectionPlan;
        if (plan == null) {
            plan = buildProjectionPlan();
        }
        Map<String, Object> filtered = new HashMap<>();
        for (int ci = 0; ci < columns.size(); ci++) {
            ColumnProjection proj = plan.get(ci);
            if (proj.normalized == null) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    String key = entry.getKey();
                    String unqualifiedKey = key.contains(".") ? key.split("\\.", 2)[1].trim() : key.trim();
                    filtered.put(unqualifiedKey, entry.getValue());
                }
                continue;
            }
            if (row.containsKey(proj.normalized)) {
                filtered.put(proj.alias, row.get(proj.normalized));
            } else {
                for (String candidate : proj.fallbackKeys) {
                    if (row.containsKey(candidate)) {
                        filtered.put(proj.alias, row.get(candidate));
                        break;
                    }
                }
            }
        }
        return filtered;
    }

    private String normalizeColumnName(String column, String defaultTable) {
        String cacheKey = defaultTable + "|" + column;
        String cached = normalizeCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        String result = computeNormalizeColumnName(column, defaultTable);
        normalizeCache.put(cacheKey, result);
        return result;
    }

    private String computeNormalizeColumnName(String column, String defaultTable) {
        if (column.contains(".")) {
            String[] parts = column.split("\\.", 2);
            String prefix = parts[0].trim();
            String colName = parts[1].trim();
            String resolvedTable = tableAliases.getOrDefault(prefix, prefix);
            if (!tableAliases.containsValue(resolvedTable)) {
                resolvedTable = defaultTable;
            }
            return resolvedTable + "." + colName;
        }
        return defaultTable + "." + column.trim();
    }

    private String normalizeColumnKey(String column, String defaultTable) {
        String normalized = normalizeColumnName(column, defaultTable);
        return normalized.contains(".") ? normalized.split("\\.")[1].trim() : normalized.trim();
    }

    private Object evaluateGroupBySubQuery(String subQueryString, Map<String, Object> outerRow, Database database) {
        String s = subQueryString.trim();
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1).trim();
        }
        s = substituteOuterReferences(s, outerRow);
        Object result = database.executeQuery(s, transactionId);
        if (!(result instanceof List)) {
            throw new IllegalStateException("GROUP BY subquery must return a list of rows");
        }
        List<?> rows = (List<?>) result;
        if (rows.isEmpty()) {
            return null;
        }
        Object firstRow = rows.get(0);
        if (!(firstRow instanceof Map) || ((Map<?, ?>) firstRow).isEmpty()) {
            return null;
        }
        return ((Map<?, ?>) firstRow).values().iterator().next();
    }

    private String substituteOuterReferences(String query, Map<String, Object> outerRow) {
        Matcher matcher = Pattern.compile("(?i)\\b([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)\\b").matcher(query);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String prefix = matcher.group(1);
            String column = matcher.group(2);
            String replacement = null;
            String table = tableAliases.get(prefix);
            if (table != null && outerRow.containsKey(table + "." + column)) {
                replacement = formatLiteralValue(outerRow.get(table + "." + column));
            } else {
                for (Map.Entry<String, Object> entry : outerRow.entrySet()) {
                    if (entry.getKey().endsWith("." + column)) {
                        replacement = formatLiteralValue(entry.getValue());
                        break;
                    }
                }
            }
            if (replacement != null) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)));
            }
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String formatLiteralValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime) {
            return "'" + value + "'";
        }
        if (value instanceof Boolean) {
            return value.toString().toUpperCase();
        }
        return value.toString();
    }

    /**
     * Returns the selected plain columns.
     *
     * @return the unmodifiable column list
     */
    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    /**
     * Returns the selected aggregate functions.
     *
     * @return the unmodifiable aggregate list
     */
    public List<QueryParser.AggregateFunction> getAggregates() {
        return Collections.unmodifiableList(aggregates);
    }

    /**
     * Returns the WHERE conditions.
     *
     * @return the unmodifiable condition list
     */
    public List<QueryParser.Condition> getConditions() {
        return Collections.unmodifiableList(conditions);
    }

    /**
     * Returns the join clauses.
     *
     * @return the unmodifiable join list
     */
    public List<QueryParser.JoinInfo> getJoins() {
        return Collections.unmodifiableList(joins);
    }

    /**
     * Returns the main table name.
     *
     * @return the main table name
     */
    public String getTableName() {
        return mainTableName;
    }

    /**
     * Returns the LIMIT, or null when not set.
     *
     * @return the limit
     */
    public Integer getLimit() {
        return limit;
    }

    /**
     * Returns the OFFSET, or null when not set.
     *
     * @return the offset
     */
    public Integer getOffset() {
        return offset;
    }

    /**
     * Returns the ORDER BY clauses.
     *
     * @return the unmodifiable order-by list
     */
    public List<QueryParser.OrderByInfo> getOrderBy() {
        return Collections.unmodifiableList(orderBy);
    }

    /**
     * Returns the GROUP BY columns.
     *
     * @return the unmodifiable group-by list
     */
    public List<String> getGroupBy() {
        return Collections.unmodifiableList(groupBy);
    }

    /**
     * Returns the HAVING conditions.
     *
     * @return the unmodifiable having list
     */
    public List<QueryParser.HavingCondition> getHavingConditions() {
        return Collections.unmodifiableList(havingConditions);
    }

    /**
     * Returns the alias to table name mapping.
     *
     * @return the unmodifiable alias map
     */
    public Map<String, String> getTableAliases() {
        return Collections.unmodifiableMap(tableAliases);
    }

    /**
     * Renders the query back to its SQL form.
     *
     * @return the SQL representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT ");
        List<String> selectItems = new ArrayList<>();
        selectItems.addAll(columns);
        selectItems.addAll(aggregates.stream().map(QueryParser.AggregateFunction::toString).toList());
        sb.append(String.join(", ", selectItems));
        sb.append(" FROM ").append(mainTableName);
        String mainTableAlias = tableAliases.entrySet().stream()
                .filter(e -> e.getValue().equals(mainTableName) && !e.getKey().equals(mainTableName))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
        if (mainTableAlias != null) {
            sb.append(" ").append(mainTableAlias);
        }

        for (QueryParser.JoinInfo join : joins) {
            sb.append(" ").append(join.joinType.toString().replace("_", " ")).append(" ");
            sb.append(join.tableName);
            if (join.alias != null) {
                sb.append(" ").append(join.alias);
            }
            if (!join.onConditions.isEmpty()) {
                sb.append(" ON ");
                sb.append(join.onConditions.stream()
                        .map(QueryParser.Condition::toString)
                        .collect(Collectors.joining(" ")));
            }
        }

        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(conditions.stream()
                    .map(QueryParser.Condition::toString)
                    .collect(Collectors.joining(" ")));
        }

        if (!groupBy.isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(String.join(", ", groupBy));
        }

        if (!havingConditions.isEmpty()) {
            sb.append(" HAVING ");
            sb.append(havingConditions.stream()
                    .map(QueryParser.HavingCondition::toString)
                    .collect(Collectors.joining(" ")));
        }

        if (!orderBy.isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(orderBy.stream()
                    .map(QueryParser.OrderByInfo::toString)
                    .collect(Collectors.joining(", ")));
        }

        if (limit != null) {
            sb.append(" LIMIT ").append(limit);
        }

        if (offset != null) {
            sb.append(" OFFSET ").append(offset);
        }

        return sb.toString();
    }
}