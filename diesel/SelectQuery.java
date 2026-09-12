package diesel;

import static diesel.ThreeValuedLogic.TRUE;
import static diesel.ThreeValuedLogic.FALSE;
import static diesel.ThreeValuedLogic.UNKNOWN;

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
import java.util.Objects;
import java.util.stream.IntStream;

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
    private Table derivedMainTable;
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
    private final Map<String, String> groupBySubQueries;
    private final Map<String, Object> scalarSubQueryCache = new HashMap<>();
    private final Map<String, List<Object>> inSubQueryCache = new HashMap<>();
    private final UUID transactionId; // Changed from String to UUID

    /** Prompt 82: adaptive optimizer singleton. */
    private static final QueryOptimizer OPTIMIZER = QueryOptimizer.getInstance();

    /** Prompt 82: per-execution adaptive state (null when disabled or no joins). */
    private transient QueryOptimizer.QueryExecutionState adaptiveState;

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
     * Hash-join metrics from the last executed join: the hash table size
     * (number of distinct keys), the build time and the probe time in
     * milliseconds, and whether the partitioned (spill-to-disk) variant was
     * used. Reset at the start of {@link #execute}.
     */
    private long lastHashJoinTableSize;
    private long lastHashJoinBuildTimeMs;
    private long lastHashJoinProbeTimeMs;
    private boolean lastJoinUsedPartitioning;

    /**
     * Prompt 83: index-only scan metrics. {@code lastIndexLookupCount} tracks
     * the number of single-column index lookups performed during the last
     * execution; {@code lastIndexOnlyScanCount} tracks how many of those
     * lookups were served entirely from the covering index without touching
     * the table rows.
     */
    private long lastIndexLookupCount;
    private long lastIndexOnlyScanCount;

    /**
     * Phase timings of the most recently executed query (Prompt 18): the plan
     * phase (join reordering, ORDER BY key resolution, projection plan), the
     * sort phase (the ORDER BY sort itself) and the execute phase (everything
     * else: scan, join, filter, grouping, projection). The values are in
     * nanoseconds and are reset at the start of {@link #execute}, so they are
     * meaningful only after a successful execution. {@link Database} combines
     * them with its own parse-time measurement and reports the breakdown to
     * {@link QueryProfiler}.
     */
    private long lastPlanNanos;
    private long lastExecuteNanos;
    private long lastSortNanos;

    long getLastPlanNanos() {
        return lastPlanNanos;
    }

    long getLastExecuteNanos() {
        return lastExecuteNanos;
    }

    long getLastSortNanos() {
        return lastSortNanos;
    }

    long getLastHashJoinTableSize() {
        return lastHashJoinTableSize;
    }

    long getLastHashJoinBuildTimeMs() {
        return lastHashJoinBuildTimeMs;
    }

    long getLastHashJoinProbeTimeMs() {
        return lastHashJoinProbeTimeMs;
    }

    boolean isLastJoinUsedPartitioning() {
        return lastJoinUsedPartitioning;
    }

    /** @return the number of index lookups performed in the last execution (Prompt 83) */
    long getLastIndexLookupCount() {
        return lastIndexLookupCount;
    }

    /** @return the number of index-only scans in the last execution (Prompt 83) */
    long getLastIndexOnlyScanCount() {
        return lastIndexOnlyScanCount;
    }

    /**
     * Maximum number of result rows kept in memory before the engine spills
     * overflow rows to temporary files on disk. Loaded from
     * {@code config.properties} ({@code max.inmemory.rows}), defaulting to 10000.
     * Streaming (spill-to-disk joins and external sort) only kicks in when an
     * estimated result set exceeds this threshold, so small queries keep their
     * current in-memory behaviour unchanged.
     *
     * <p>Also used as the hash-join memory guard: when the build side of a hash
     * join is estimated to exceed this many rows, the engine falls back to the
     * block nested loop join instead of materialising a large hash table.
     */
    private static long maxInMemoryRows = 10000;

    /**
     * Estimated upper bound (in bytes) of the in-memory hash table the engine
     * is willing to build for a hash join. When the estimate exceeds this,
     * the engine switches to the spill-to-disk partitioned hash join so a very
     * large build side cannot trigger an OutOfMemoryError. Loaded from
     * {@code config.properties} ({@code max.hash.table.size.mb}, defaulting
     * to 512 MB).
     */
    private static long maxHashTableSizeBytes = 512L * 1024L * 1024L;

    /**
     * Maximum number of rows a single SELECT result (or an intermediate join /
     * filter / group-by stage of it) may produce before execution aborts with an
     * exception. Guards against an accidental cross join silently generating
     * billions of rows and exhausting memory. Loaded from
     * {@code config.properties} ({@code max.result.rows}, defaulting to 1000000)
     * and overridable per query with the MAX_ROWS SQL comment hint
     * (&#47;* MAX_ROWS=N *&#47;), or disabled entirely with {@code /* MAX_ROWS=0 *&#47;}.
     */
    private static long MAX_RESULT_ROWS = 1_000_000;

    /**
     * Fixed per-row-unit overhead of building and probing an in-memory hash
     * table, used by the statistics-based cost model in
     * {@link #preferNestedLoopByStatistics}: the hash join allocates the build
     * table and hashes every key, which only pays off once the inputs are
     * large enough that the nested-loop row product exceeds this constant.
     */
    private static long HASH_JOIN_OVERHEAD_ROWS = 1000;

    static {
        loadHashJoinConfig();
    }

    /**
     * (Re)loads {@code maxInMemoryRows}, {@code maxHashTableSizeBytes} and
     * {@code MAX_RESULT_ROWS} from {@code config.properties}. Package-private so
     * tests can force the low-memory hash-join paths by pointing the thresholds
     * at tiny values.
     */
    static void loadHashJoinConfig() {
        long inMemoryRows = ConfigLoader.getLong("max.inmemory.rows", 10000);
        long hashMb = ConfigLoader.getLong("max.hash.table.size.mb", 512);
        long maxResultRows = ConfigLoader.getLong("max.result.rows", 1_000_000);
        HASH_JOIN_OVERHEAD_ROWS = ConfigLoader.getLong("hash.join.overhead.rows", 1000);
        MEMORY_SAMPLE_INTERVAL = ConfigLoader.getLong("select.memory.sample.interval", 4096);
        maxInMemoryRows = inMemoryRows;
        maxHashTableSizeBytes = hashMb * 1024L * 1024L;
        MAX_RESULT_ROWS = maxResultRows;
    }

    /**
     * Test override for the hash-join memory thresholds.
     *
     * @param maxInMemoryRows new value for {@code max.inmemory.rows}
     * @param maxHashTableSizeMb new value for {@code max.hash.table.size.mb}
     */
    static void setHashJoinConfigForTest(long maxInMemoryRows, long maxHashTableSizeMb) {
        SelectQuery.maxInMemoryRows = maxInMemoryRows;
        maxHashTableSizeBytes = maxHashTableSizeMb * 1024L * 1024L;
    }

    /**
     * Test override for the maximum result row limit.
     *
     * @param maxResultRows new value for {@code max.result.rows}, or 0 for unlimited
     */
    static void setMaxResultRowsForTest(long maxResultRows) {
        MAX_RESULT_ROWS = maxResultRows;
    }

    /** @return the configured default result row limit (0 means unlimited) */
    static long getMaxResultRows() {
        return MAX_RESULT_ROWS;
    }

    /** Per-query row limit, taken from {@link #MAX_RESULT_ROWS} unless overridden
     * by the MAX_ROWS SQL hint. A value of 0 (or less) disables the limit. */
    private long maxResultRows = MAX_RESULT_ROWS;

    /** True once the 80%-of-limit warning has been logged for this query. */
    private boolean resultLimitWarningLogged;

    /** Rows produced between two consecutive heap-memory samples. */
    private static long MEMORY_SAMPLE_INTERVAL = 4096;

    /**
     * Per-thread snapshot of the peak memory and row-count metrics of the most
     * recently executed SELECT on that thread. Keyed by thread so that a server
     * worker can report the metrics of its own query after an OutOfMemoryError.
     */
    private static final ThreadLocal<QueryMemoryTracker> QUERY_MEMORY =
            ThreadLocal.withInitial(QueryMemoryTracker::new);

    /**
     * Tracks the peak heap usage observed while a SELECT grows its result rows.
     * Sampled periodically (every {@link #MEMORY_SAMPLE_INTERVAL} rows) and at
     * pipeline boundaries, so the values are approximate but cheap: sampling
     * adds no per-row overhead.
     */
    private static final class QueryMemoryTracker {
        long peakBytes;
        long rowsAtPeak;
        long rowCount;

        void sample(long rows) {
            rowCount = rows;
            long usedBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            // Ignore row-0 observations: the heap at query/pipeline start reflects
            // pre-query state (e.g. uncollected garbage from file-persisted inserts),
            // so treating it as the peak would freeze rowsAtPeak at 0.
            if (rows > 0 && usedBytes > peakBytes) {
                peakBytes = usedBytes;
                rowsAtPeak = rows;
            }
        }

        void reset() {
            peakBytes = 0;
            rowsAtPeak = 0;
            rowCount = 0;
        }
    }

    /** @return peak heap bytes used by the last SELECT on this thread (0 = none yet) */
    static long getLastQueryPeakMemoryBytes() {
        return QUERY_MEMORY.get().peakBytes;
    }

    /** @return number of rows produced when the peak heap usage was observed */
    static long getLastQueryRowsAtPeak() {
        return QUERY_MEMORY.get().rowsAtPeak;
    }

    /** @return rows produced by the last SELECT on this thread */
    static long getLastQueryRowCount() {
        return QUERY_MEMORY.get().rowCount;
    }

    /**
     * Overrides the result row limit for this query with the value of the
     * MAX_ROWS SQL comment hint. Called by {@link Database} after parsing.
     *
     * @param maxRows the limit, or 0 to disable
     */
    void setMaxResultRows(long maxRows) {
        maxResultRows = maxRows;
    }

    /**
     * Enforces {@link #maxResultRows} on a growing row collection: logs a single
     * warning once the collection reaches 80% of the limit, and aborts the query
     * with an explanatory exception as soon as the limit is exceeded. Also keeps
     * the {@link #QUERY_MEMORY} metrics fresh with a cheap periodic heap sample.
     *
     * @param size   the current number of rows already materialised
     * @param stage  a short label for the pipeline stage in the error message
     * @throws IllegalArgumentException when {@code size} reaches the row limit
     */
    private void checkResultRowLimit(long size, String stage) {
        if ((size & (MEMORY_SAMPLE_INTERVAL - 1)) == 0) {
            QUERY_MEMORY.get().sample(size);
        }
        if (maxResultRows <= 0) {
            return;
        }
        if (!resultLimitWarningLogged && size >= maxResultRows - maxResultRows / 5) {
            LOGGER.warning("WARNING: query result is approaching the maximum allowed row limit: " + size
                    + " of " + maxResultRows + " rows (80%). Consider adding LIMIT or a MAX_ROWS hint.");
            resultLimitWarningLogged = true;
        }
        if (size >= maxResultRows) {
            throw new IllegalArgumentException("Query result exceeds the maximum allowed row limit of "
                    + maxResultRows + " rows at stage '" + stage + "'. Add LIMIT or a /* MAX_ROWS=N */ hint to override.");
        }
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
     * @param tableAliases      the alias to table name mapping
     * @param extraTableAliases extra aliases from joins
     * @param columnTypes       the combined column types
     */
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
     * @param tableAliases      the alias to table name mapping
     * @param extraTableAliases extra aliases from joins
     * @param columnTypes       the combined column types
     */
    private SelectQuery(String tableName, String tableAlias, List<String> columns,
                        List<QueryParser.AggregateFunction> aggregates, List<QueryParser.JoinInfo> joins,
                        List<QueryParser.Condition> conditions, List<String> groupBy,
                        List<QueryParser.HavingCondition> havingConditions, List<QueryParser.OrderByInfo> orderBy,
                        Integer limit, Integer offset,
                        Map<String, String> tableAliases, Map<String, String> extraTableAliases,
                        Map<String, Class<?>> columnTypes) {
        SelectQueryCore core = new SelectQueryCore(tableName, tableAlias, columns, aggregates);
        SelectQueryJoins joinsObj = new SelectQueryJoins(joins);
        SelectQueryWhere where = new SelectQueryWhere(conditions);
        SelectQueryGroupBy groupByObj = new SelectQueryGroupBy(groupBy, havingConditions, new HashMap<>());
        SelectQueryOrderBy orderByObj = new SelectQueryOrderBy(orderBy);
        SelectQueryLimitOffset limitOffset = new SelectQueryLimitOffset(limit, offset);
        SelectQueryMetadata metadata = new SelectQueryMetadata(tableAliases, extraTableAliases, columnTypes);
        
        this.columns = core.getColumns();
        this.aggregates = core.getAggregates();
        this.conditions = where.getConditions();
        this.joins = joinsObj.getJoins();
        this.mainTableName = core.getTableName(); // Используем tableName вместо mainTableName
        this.limit = limitOffset.getLimit();
        this.offset = limitOffset.getOffset();
        this.orderBy = orderByObj.getOrderBy();
        this.groupBy = groupByObj.getGroupBy();
        this.havingConditions = groupByObj.getHavingConditions();
        this.tableAliases = metadata.getTableAliases();
        this.groupBySubQueries = groupByObj.getGroupBySubQueries();
        this.transactionId = UUID.randomUUID(); // Генерируем UUID, если он не передан
        // Добавляем tableAlias в tableAliases, если он не null
        String aliasFromCore = core.getTableAlias();
        if (aliasFromCore != null && !aliasFromCore.isEmpty()) {
            this.tableAliases.put(aliasFromCore, this.mainTableName);
        }
        // Добавляем mainTableName в tableAliases
        this.tableAliases.putIfAbsent(this.mainTableName, this.mainTableName);
        // Обрабатываем extraTableAliases
        Map<String, String> extra = metadata.getExtraTableAliases();
        if (!extra.isEmpty()) {
            this.tableAliases.putAll(extra);
        }
    }

    /**
     * Creates a SELECT query over the given table, including any subqueries in
     * the GROUP BY clause. Use {@link #builder()} to construct an instance
     * (java:S107, Prompt 32).
     */
    private SelectQuery(SelectQueryCore core, SelectQueryJoins joins,
                        SelectQueryWhere where, SelectQueryGroupBy groupBy,
                        SelectQueryOrderBy orderBy, SelectQueryLimitOffset limitOffset,
                        SelectQueryMetadata metadata) {
        this.columns = core.getColumns();
        this.aggregates = core.getAggregates();
        this.conditions = where.getConditions();
        this.joins = joins.getJoins();
        this.mainTableName = core.getTableName(); // Используем tableName вместо mainTableName
        this.limit = limitOffset.getLimit();
        this.offset = limitOffset.getOffset();
        this.orderBy = orderBy.getOrderBy();
        this.groupBy = groupBy.getGroupBy();
        this.havingConditions = groupBy.getHavingConditions();
        this.tableAliases = metadata.getTableAliases();
        this.groupBySubQueries = groupBy.getGroupBySubQueries();
        this.transactionId = UUID.randomUUID(); // Генерируем UUID, если он не передан
        // Добавляем tableAlias в tableAliases, если он не null
        String tableAlias = core.getTableAlias();
        if (tableAlias != null && !tableAlias.isEmpty()) {
            this.tableAliases.put(tableAlias, this.mainTableName);
        }
        // Добавляем mainTableName в tableAliases
        this.tableAliases.putIfAbsent(this.mainTableName, this.mainTableName);
        // Обрабатываем extraTableAliases
        Map<String, String> extra = metadata.getExtraTableAliases();
        if (!extra.isEmpty()) {
            this.tableAliases.putAll(extra);
        }
    }

    /**
     * Parameter object for core SELECT query elements: table identification and selection.
     */
    public static final class SelectQueryCore {
        private final String tableName;
        private final String tableAlias;
        private final List<String> columns;
        private final List<QueryParser.AggregateFunction> aggregates;

        public SelectQueryCore(String tableName, String tableAlias,
                               List<String> columns, List<QueryParser.AggregateFunction> aggregates) {
            Objects.requireNonNull(tableName, "SelectQueryCore tableName must not be null");
            this.tableName = tableName;
            this.tableAlias = tableAlias;
            this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
            this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : new ArrayList<>();
        }

        public String getTableName() {
            return tableName;
        }

        public String getTableAlias() {
            return tableAlias;
        }

        public List<String> getColumns() {
            return new ArrayList<>(columns);
        }

        public List<QueryParser.AggregateFunction> getAggregates() {
            return new ArrayList<>(aggregates);
        }
    }

    /**
     * Parameter object for JOIN clauses in a SELECT query.
     */
    public static final class SelectQueryJoins {
        private final List<QueryParser.JoinInfo> joins;

        public SelectQueryJoins(List<QueryParser.JoinInfo> joins) {
            this.joins = joins != null ? new ArrayList<>(joins) : new ArrayList<>();
        }

        public List<QueryParser.JoinInfo> getJoins() {
            return new ArrayList<>(joins);
        }
    }

    /**
     * Parameter object for WHERE conditions in a SELECT query.
     */
    public static final class SelectQueryWhere {
        private final List<QueryParser.Condition> conditions;

        public SelectQueryWhere(List<QueryParser.Condition> conditions) {
            this.conditions = conditions != null ? new ArrayList<>(conditions) : new ArrayList<>();
        }

        public List<QueryParser.Condition> getConditions() {
            return new ArrayList<>(conditions);
        }
    }

    /**
     * Parameter object for GROUP BY and HAVING clauses in a SELECT query.
     */
    public static final class SelectQueryGroupBy {
        private final List<String> groupBy;
        private final List<QueryParser.HavingCondition> havingConditions;
        private final Map<String, String> groupBySubQueries;

        public SelectQueryGroupBy(List<String> groupBy,
                                  List<QueryParser.HavingCondition> havingConditions,
                                  Map<String, String> groupBySubQueries) {
            this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : new ArrayList<>();
            this.havingConditions = havingConditions != null ? new ArrayList<>(havingConditions) : new ArrayList<>();
            this.groupBySubQueries = groupBySubQueries != null ? new HashMap<>(groupBySubQueries) : new HashMap<>();
        }

        public List<String> getGroupBy() {
            return new ArrayList<>(groupBy);
        }

        public List<QueryParser.HavingCondition> getHavingConditions() {
            return new ArrayList<>(havingConditions);
        }

        public Map<String, String> getGroupBySubQueries() {
            return new HashMap<>(groupBySubQueries);
        }
    }

    /**
     * Parameter object for ORDER BY clause in a SELECT query.
     */
    public static final class SelectQueryOrderBy {
        private final List<QueryParser.OrderByInfo> orderBy;

        public SelectQueryOrderBy(List<QueryParser.OrderByInfo> orderBy) {
            this.orderBy = orderBy != null ? new ArrayList<>(orderBy) : new ArrayList<>();
        }

        public List<QueryParser.OrderByInfo> getOrderBy() {
            return new ArrayList<>(orderBy);
        }
    }

    /**
     * Parameter object for LIMIT and OFFSET clauses in a SELECT query.
     */
    public static final class SelectQueryLimitOffset {
        private final Integer limit;
        private final Integer offset;

        public SelectQueryLimitOffset(Integer limit, Integer offset) {
            this.limit = limit;
            this.offset = offset;
        }

        public Integer getLimit() {
            return limit;
        }

        public Integer getOffset() {
            return offset;
        }
    }

    /**
     * Parameter object for table and column metadata in a SELECT query.
     */
    public static final class SelectQueryMetadata {
        private final Map<String, String> tableAliases;
        private final Map<String, String> extraTableAliases;
        private final Map<String, Class<?>> columnTypes;

        public SelectQueryMetadata(Map<String, String> tableAliases,
                                   Map<String, String> extraTableAliases,
                                   Map<String, Class<?>> columnTypes) {
            this.tableAliases = tableAliases != null ? new HashMap<>(tableAliases) : new HashMap<>();
            this.extraTableAliases = extraTableAliases != null ? new HashMap<>(extraTableAliases) : new HashMap<>();
            this.columnTypes = columnTypes != null ? new HashMap<>(columnTypes) : new HashMap<>();
        }

        public Map<String, String> getTableAliases() {
            return new HashMap<>(tableAliases);
        }

        public Map<String, String> getExtraTableAliases() {
            return new HashMap<>(extraTableAliases);
        }

        public Map<String, Class<?>> getColumnTypes() {
            return new HashMap<>(columnTypes);
        }
    }

    /**
     * Builder for {@link SelectQuery}, so callers never face the long
     * constructor (java:S107, Prompt 32).
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String tableName;
        private String tableAlias;
        private List<String> columns;
        private List<QueryParser.AggregateFunction> aggregates;
        private List<QueryParser.JoinInfo> joins;
        private List<QueryParser.Condition> conditions;
        private List<String> groupBy;
        private List<QueryParser.HavingCondition> havingConditions;
        private List<QueryParser.OrderByInfo> orderBy;
        private Integer limit;
        private Integer offset;
        private Map<String, String> tableAliases;
        private Map<String, String> extraTableAliases;
        private Map<String, Class<?>> columnTypes;
        private Map<String, String> groupBySubQueries;

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableAlias(String tableAlias) {
            this.tableAlias = tableAlias;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder aggregates(List<QueryParser.AggregateFunction> aggregates) {
            this.aggregates = aggregates;
            return this;
        }

        public Builder joins(List<QueryParser.JoinInfo> joins) {
            this.joins = joins;
            return this;
        }

        public Builder conditions(List<QueryParser.Condition> conditions) {
            this.conditions = conditions;
            return this;
        }

        public Builder groupBy(List<String> groupBy) {
            this.groupBy = groupBy;
            return this;
        }

        public Builder havingConditions(List<QueryParser.HavingCondition> havingConditions) {
            this.havingConditions = havingConditions;
            return this;
        }

        public Builder orderBy(List<QueryParser.OrderByInfo> orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        public Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public Builder offset(Integer offset) {
            this.offset = offset;
            return this;
        }

        public Builder tableAliases(Map<String, String> tableAliases) {
            this.tableAliases = tableAliases;
            return this;
        }

        public Builder extraTableAliases(Map<String, String> extraTableAliases) {
            this.extraTableAliases = extraTableAliases;
            return this;
        }

        public Builder columnTypes(Map<String, Class<?>> columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public Builder groupBySubQueries(Map<String, String> groupBySubQueries) {
            this.groupBySubQueries = groupBySubQueries;
            return this;
        }

        public SelectQuery build() {
            SelectQueryCore core = new SelectQueryCore(tableName, tableAlias, columns, aggregates);
            SelectQueryJoins joinsObj = new SelectQueryJoins(joins);
            SelectQueryWhere where = new SelectQueryWhere(conditions);
            SelectQueryGroupBy groupByObj = new SelectQueryGroupBy(groupBy, havingConditions, groupBySubQueries);
            SelectQueryOrderBy orderByObj = new SelectQueryOrderBy(orderBy);
            SelectQueryLimitOffset limitOffset = new SelectQueryLimitOffset(limit, offset);
            SelectQueryMetadata metadata = new SelectQueryMetadata(tableAliases, extraTableAliases, columnTypes);
            
            return new SelectQuery(core, joinsObj, where, groupByObj, orderByObj, limitOffset, metadata);
        }
    }

    /**
     * Parameter Object for the join execution pipeline (java:S107, Prompt 32).
     * The join helpers shared up to seventeen arguments; this holder carries
     * the execution-wide context (spill state, WHERE conditions, column types,
     * table map and row locks) plus the per-join state (join, equality and
     * streaming flags, table names). The per-join fields are set once per join
     * iteration in {@link #applyJoins}, so {@link #emitHashJoinMatch} - which
     * runs per (probe, build) pair millions of times - never allocates.
     */
    private static final class JoinContext {
        final boolean[] spillActive;
        final StreamingResultIterator spill;
        final List<Map<String, Object>> spillFallback;
        final List<QueryParser.Condition> whereConditions;
        final Map<String, Class<?>> combinedColumnTypes;
        final Map<String, Table> tables;
        final List<ReentrantReadWriteLock> acquiredLocks;

        QueryParser.JoinInfo join;
        boolean onlyEquality;
        boolean lastStream;
        String buildTableName;
        String probeTableName;

        JoinContext(StreamingResultIterator spill, boolean[] spillActive,
                    List<Map<String, Object>> spillFallback,
                    List<QueryParser.Condition> whereConditions,
                    Map<String, Class<?>> combinedColumnTypes,
                    Map<String, Table> tables,
                    List<ReentrantReadWriteLock> acquiredLocks) {
            Objects.requireNonNull(spillActive, "JoinContext spillActive must not be null");
            Objects.requireNonNull(whereConditions, "JoinContext whereConditions must not be null");
            Objects.requireNonNull(combinedColumnTypes, "JoinContext combinedColumnTypes must not be null");
            Objects.requireNonNull(tables, "JoinContext tables must not be null");
            Objects.requireNonNull(acquiredLocks, "JoinContext acquiredLocks must not be null");
            this.spill = spill;
            this.spillActive = spillActive;
            this.spillFallback = spillFallback;
            this.whereConditions = whereConditions;
            this.combinedColumnTypes = combinedColumnTypes;
            this.tables = tables;
            this.acquiredLocks = acquiredLocks;
        }

        void forJoin(QueryParser.JoinInfo join, boolean onlyEquality, boolean lastStream,
                     String buildTableName, String probeTableName) {
            this.join = join;
            this.onlyEquality = onlyEquality;
            this.lastStream = lastStream;
            this.buildTableName = buildTableName;
            this.probeTableName = probeTableName;
        }
    }

    /**
     * Sets the in-memory virtual table produced by a derived table
     * ({@code SELECT ... FROM (SELECT ...) AS subq}). When present, execution
     * scans this table instead of looking up a table by name.
     *
     * @param derivedMainTable the materialized derived table, or null
     */
    public void setDerivedMainTable(Table derivedMainTable) {
        this.derivedMainTable = derivedMainTable;
    }

    /**
     * @return the virtual table backing a derived main table, or null when this
     *         query scans a real table
     */
    public Table getDerivedMainTable() {
        return derivedMainTable;
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
        // Prompt 22 (java:S2259): guard the public parameter and the
        // documented-nullable owning database before either is dereferenced
        // (a JOIN would NPE on database.getTable below).
        Objects.requireNonNull(table, "Main table must not be null");
        Database database = Objects.requireNonNull(table.getDatabase(),
                ErrorMessages.TABLE_PREFIX + mainTableName + ErrorMessages.NOT_ATTACHED_TO_DB);
        return executeSelect(table, database);
    }

    /**
     * Executes this SELECT and returns its result rows as an iterator, so a
     * server-side cursor (Prompt 81) can hand the client paginated batches
     * without the caller receiving the whole list at once. The rows are still
     * materialised in memory by {@link #execute} (the streaming pipeline is
     * currently disabled); the iterator simply exposes them lazily.
     *
     * @param table the main table
     * @return an iterator over the projected result rows
     * @throws IllegalArgumentException if a join table is missing
     */
    public Iterator<Map<String, Object>> executeAsIterator(Table table) {
        List<Map<String, Object>> rows = execute(table);
        return rows.iterator();
    }

    // Prompt 29 (execute() complexity 59): the main flow is split into
    // focused phases, each under the complexity threshold - executeSelect
    // (setup + orchestration), applyJoins, applyWhereFilter, applyGroupBy,
    // applyOrderBy and applyLimitOffset. All phases are behavior-preserving.
    private List<Map<String, Object>> executeSelect(Table table, Database database) {
        // Prompt 18: per-query phase timing. The plan phase covers the setup
        // below (join reordering, ORDER BY key resolution, projection plan);
        // the execute phase covers everything between the main scan and the
        // result projection minus the ORDER BY sort, which is timed separately.
        long planStart = System.nanoTime();
        long execStart;
        long beforeSort;
        long sortStart;
        lastPlanNanos = 0;
        lastExecuteNanos = 0;
        lastSortNanos = 0;
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
        lastHashJoinTableSize = 0;
        lastHashJoinBuildTimeMs = 0;
        lastHashJoinProbeTimeMs = 0;
        lastJoinUsedPartitioning = false;
        lastIndexLookupCount = 0;
        lastIndexOnlyScanCount = 0;
        resultLimitWarningLogged = false;
        QUERY_MEMORY.get().reset();
        QUERY_MEMORY.get().sample(0);

        // Prompt 82: initialise adaptive execution state from the learned plan cache.
        // Skip entirely for single-table queries (no joins = no replanning possible).
        if (!joins.isEmpty()) {
            List<String> tableNames = new ArrayList<>(tables.keySet());
            adaptiveState = OPTIMIZER.beginExecution(tableNames, joins);
        } else {
            adaptiveState = null;
        }
        projectionPlan = buildProjectionPlan();
        lastPlanNanos = System.nanoTime() - planStart;
        execStart = System.nanoTime();

        try {
            ensureWhereIndexes(table, conditions, mainTableName);
            List<Map<String, Object>> mainRows = getIndexedRows(table, conditions, mainTableName, combinedColumnTypes);
            if (mainRows == null) {
                List<Map<String, Object>> rawRows = table.getRows();
                mainRows = new ArrayList<>(rawRows.size());
                for (int i = 0; i < rawRows.size(); i++) {
                    if (!table.isDeleted(i)) {
                        mainRows.add(rawRows.get(i));
                    }
                }
            }

            List<Map<String, Map<String, Object>>> joinedRows = new ArrayList<>();
            for (Map<String, Object> mainRow : mainRows) {
                checkResultRowLimit(joinedRows.size(), "main scan");
                Map<String, Map<String, Object>> wrapped = new HashMap<>(2);
                wrapped.put(mainTableName, mainRow);
                joinedRows.add(wrapped);
            }

            boolean useStreaming = shouldUseStreaming();
            StreamingResultIterator spill = null;
            List<Map<String, Object>> spillFallback = null;
            boolean[] spillActive = { false };
            if (useStreaming) {
                spillFallback = new ArrayList<>();
                try {
                    spill = new StreamingResultIterator(maxInMemoryRows);
                    spillActive[0] = true;
                } catch (IOException e) {
                    spillActive[0] = false;
                }
            }

            JoinContext joinCtx = new JoinContext(spill, spillActive, spillFallback, conditions,
                combinedColumnTypes, tables, acquiredLocks);
            joinedRows = applyJoins(mainRows, joinedRows, useStreaming, joinCtx);

            List<Map<String, Object>> filteredRows = applyWhereFilter(joinedRows, useStreaming,
                    spill, spillActive, spillFallback, combinedColumnTypes, tables);

            List<Map<String, Object>> finalRows = applyGroupBy(filteredRows, database, combinedColumnTypes);

            beforeSort = System.nanoTime();
            lastExecuteNanos += beforeSort - execStart;
            applyOrderBy(finalRows, useStreaming);
            sortStart = System.nanoTime();
            lastSortNanos = sortStart - beforeSort;
            execStart = sortStart;

            List<Map<String, Object>> result = applyLimitOffset(finalRows, combinedColumnTypes);

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}, aggregates {3}, groupBy {4}, having={5}, limit={6}, offset={7}, orderBy={8}",
                    new Object[]{result.size(), mainTableName, joins, aggregates, groupBy, havingConditions, limit, offset, orderBy});
            lastExecuteNanos += System.nanoTime() - execStart;
            QUERY_MEMORY.get().sample(result.size());

            // Prompt 82: record execution outcome for adaptive learning.
            if (adaptiveState != null) {
                OPTIMIZER.recordExecution(adaptiveState, lastExecuteNanos + lastSortNanos, result.size());
            }

            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private record HashJoinSetup(Table buildTable, Table probeTable, String buildTableName,
            String probeTableName, String buildColumnKey, String probeColumnKey,
            List<Map<String, Object>> buildRows, boolean onlyEquality) {}

    private HashJoinSetup resolveHashJoinSideTables(QueryParser.JoinInfo join, Table joinTable,
            List<Map<String, Object>> mainRows, JoinContext ctx) {
        Table buildTable = joinTable.rowCount() <= mainRows.size() ? joinTable : ctx.tables.get(mainTableName);
        Table probeTable = buildTable == joinTable ? ctx.tables.get(mainTableName) : joinTable;
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

        List<Map<String, Object>> buildRows = getIndexedRows(buildTable, join.onConditions, buildTableName, ctx.combinedColumnTypes);
        if (buildRows == null) {
            buildRows = buildTable.getLiveRows();
        }
        String buildColumnKey = normalizeColumnKey(buildColumn, buildTableName);
        boolean onlyEquality = join.onConditions.size() == 1 && equalityCondition != null && !equalityCondition.not;

        return new HashJoinSetup(buildTable, probeTable, buildTableName, probeTableName,
                buildColumnKey, normalizeColumnKey(probeColumn, probeTableName), buildRows, onlyEquality);
    }

    private boolean shouldReplanToNestedLoop() {
        return adaptiveState != QueryOptimizer.QueryExecutionState.DISABLED
                && adaptiveState.stepIndex() > 0
                && adaptiveState.hasSignificantDeviation()
                && !adaptiveState.replanned;
    }

    private List<Map<String, Map<String, Object>>> executeHashJoin(QueryParser.JoinInfo join,
            Table joinTable, List<Map<String, Object>> mainRows,
            List<Map<String, Map<String, Object>>> joinedRows,
            boolean lastStream, JoinContext ctx) throws IOException {
        HashJoinSetup setup = resolveHashJoinSideTables(join, joinTable, mainRows, ctx);

        long estimatedRows = setup.buildRows().size();
        long estimatedBytes = estimateHashTableSizeBytes(setup.buildRows(), setup.buildTable());

        if (preferNestedLoopByStatistics(setup.buildTable(), setup.probeTable())) {
            LOGGER.log(Level.FINE, "Statistics prefer nested loop over hash join for join on {0} ({1} x {2} rows)",
                    new Object[]{join.tableName, setup.buildTable().getStatistics().getRowCount(), setup.probeTable().getStatistics().getRowCount()});
            ctx.forJoin(join, setup.onlyEquality(), lastStream, setup.buildTableName(), setup.probeTableName());
            return runBlockNestedLoopJoin(joinedRows, joinTable, ctx);
        }
        if (estimatedRows > maxInMemoryRows || estimatedBytes > maxHashTableSizeBytes) {
            try {
                ctx.forJoin(join, setup.onlyEquality(), lastStream, setup.buildTableName(), setup.probeTableName());
                List<Map<String, Map<String, Object>>> result = runPartitionedHashJoin(setup.buildRows(), setup.buildTable(), setup.probeTable(),
                        setup.buildColumnKey(), setup.probeColumnKey(), ctx);
                LOGGER.log(Level.INFO, "Partitioned hash join completed: {0} rows produced for join on {1}",
                        new Object[]{result.size(), join.tableName});
                return result;
            } catch (IOException e) {
                LOGGER.warning("Partitioned hash join failed, falling back to block nested loop join: " + e.getMessage());
                ctx.forJoin(join, setup.onlyEquality(), lastStream, setup.buildTableName(), setup.probeTableName());
                return runBlockNestedLoopJoin(joinedRows, joinTable, ctx);
            }
        }
        ctx.forJoin(join, setup.onlyEquality(), lastStream, setup.buildTableName(), setup.probeTableName());
        List<Map<String, Map<String, Object>>> result = runInMemoryHashJoin(setup.buildRows(), setup.buildTable(), setup.probeTable(),
                setup.buildColumnKey(), setup.probeColumnKey(), ctx);
        LOGGER.log(Level.FINE, "Hash join completed: {0} rows produced for join on {1}",
                new Object[]{result.size(), join.tableName});
        return result;
    }

    private void recordJoinAdaptiveMetrics(QueryParser.JoinInfo join, Table joinTable,
            boolean useHashJoin, List<Map<String, Map<String, Object>>> newJoinedRows) {
        if (adaptiveState != QueryOptimizer.QueryExecutionState.DISABLED) {
            long estimatedRowsForJoin = joinTable.getStatistics().getRowCount();
            long actualRowsProduced = newJoinedRows.size();
            adaptiveState.reportStep(estimatedRowsForJoin, actualRowsProduced);
            adaptiveState.chooseAlgorithm(
                    useHashJoin ? "hash-" + join.tableName : "nl-" + join.tableName);
        }
    }

    // Prompt 29: applies every JOIN (hash / partitioned hash / block nested
    // loop / statistics-preferred nested loop) and returns the joined rows.
    private List<Map<String, Map<String, Object>>> applyJoins(
            List<Map<String, Object>> mainRows,
            List<Map<String, Map<String, Object>>> joinedRows,
            boolean useStreaming,
            JoinContext ctx) {
        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = ctx.tables.get(join.tableName);
            List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();
            boolean lastStream = useStreaming && join == joins.get(joins.size() - 1);

            boolean useHashJoin = canUseHashJoin(join);
            if (hasOrInOnConditions(join)) {
                LOGGER.warning("WARNING: JOIN with OR condition may produce large result set");
            }

            if (shouldReplanToNestedLoop() && useHashJoin) {
                useHashJoin = false;
                adaptiveState.markReplanned();
                LOGGER.log(Level.INFO, "Adaptive replan: switching to nested loop for join on {0} (previous deviation={1})",
                        new Object[]{join.tableName, String.format("%.2f", adaptiveState.maxDeviation)});
            }

            LOGGER.log(Level.FINE, "Join on {0}: useHashJoin={1}", new Object[]{join.tableName, useHashJoin});

            ensureJoinColumnIndexes(ctx.tables, join);

            if (useHashJoin) {
                try {
                    newJoinedRows = executeHashJoin(join, joinTable, mainRows, joinedRows, lastStream, ctx);
                } catch (IOException e) {
                    throw new QuerySyntaxException("", e.getMessage());
                }
            } else {
                ctx.forJoin(join, false, lastStream, null, null);
                newJoinedRows = runBlockNestedLoopJoin(joinedRows, joinTable, ctx);
            }
            joinedRows = newJoinedRows;

            recordJoinAdaptiveMetrics(join, joinTable, useHashJoin, newJoinedRows);
        }
        return joinedRows;
    }

    // Prompt 29: applies the WHERE filter (streaming drain or flatten +
    // evaluate per joined row) and returns the filtered rows.
    private List<Map<String, Object>> applyWhereFilter(
            List<Map<String, Map<String, Object>>> joinedRows,
            boolean useStreaming,
            StreamingResultIterator spill,
            boolean[] spillActive,
            List<Map<String, Object>> spillFallback,
            Map<String, Class<?>> combinedColumnTypes,
            Map<String, Table> tables) {
        List<Map<String, Object>> filteredRows;
        if (useStreaming) {
            filteredRows = new ArrayList<>();
            if (spillActive[0] && spill != null) {
                try {
                    spill.finishWriting();
                    while (spill.hasNext()) {
                        checkResultRowLimit(filteredRows.size(), "filter");
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
                    checkResultRowLimit(filteredRows.size(), "filter");
                    filteredRows.add(flattenedRow);
                }
            }
        }
        return filteredRows;
    }

    // Prompt 29: groups the filtered rows (with per-group aggregates and
    // HAVING) and returns the final grouped rows.
    private List<Map<String, Object>> applyGroupBy(List<Map<String, Object>> filteredRows,
            Database database, Map<String, Class<?>> combinedColumnTypes) {
        if (groupBy.isEmpty()) {
            return filteredRows;
        }

        Map<List<Object>, List<Map<String, Object>>> groupedRows = groupRowsByColumns(filteredRows, database);
        initializeGroupAggregateKeys();

        List<Map<String, Object>> finalRows = new ArrayList<>();
        groupedRows.entrySet().stream()
                .map(entry -> buildGroupResultRow(entry.getKey(), entry.getValue(), combinedColumnTypes))
                .filter(resultRow -> havingConditions.isEmpty() || evaluateHavingConditions(resultRow, havingConditions))
                .forEach(resultRow -> {
                    checkResultRowLimit(finalRows.size(), "group by");
                    finalRows.add(resultRow);
                });

        LOGGER.log(Level.FINE, "Applied GROUP BY with {0} columns, produced {1} groups",
                new Object[]{groupBy.size(), finalRows.size()});
        return finalRows;
    }

    private Map<List<Object>, List<Map<String, Object>>> groupRowsByColumns(List<Map<String, Object>> filteredRows,
                                                                            Database database) {
        return filteredRows.stream()
                .collect(Collectors.groupingBy(row -> groupBy.stream()
                        .map(col -> groupBySubQueries.containsKey(col)
                                ? evaluateGroupBySubQuery(groupBySubQueries.get(col), row, database)
                                : row.get(normalizeColumnName(col, mainTableName)))
                        .collect(Collectors.toList())));
    }

    private void initializeGroupAggregateKeys() {
        groupAggregateKeys.clear();
        for (QueryParser.AggregateFunction agg : aggregates) {
            String resultKey = agg.alias != null ? agg.alias : agg.toString();
            groupAggregateKeys.add(resultKey);
        }
    }

    private Map<String, Object> buildGroupResultRow(List<Object> groupKey, List<Map<String, Object>> group,
                                                    Map<String, Class<?>> combinedColumnTypes) {
        Map<String, Object> resultRow = new HashMap<>();

        populateGroupByColumns(groupKey, resultRow);
        computeGroupAggregates(group, resultRow, combinedColumnTypes);
        populateNonAggregateColumns(group, resultRow);

        return resultRow;
    }

    private void populateGroupByColumns(List<Object> groupKey, Map<String, Object> resultRow) {
        for (int i = 0; i < groupBy.size(); i++) {
            String column = groupBy.get(i);
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            resultRow.put(normalizedColumn, groupKey.get(i));
        }
    }

    private void computeGroupAggregates(List<Map<String, Object>> group, Map<String, Object> resultRow,
                                        Map<String, Class<?>> combinedColumnTypes) {
        for (QueryParser.AggregateFunction agg : aggregates) {
            String resultKey = agg.alias != null ? agg.alias : agg.toString();
            resultRow.put(resultKey, computeAggregate(agg, group, combinedColumnTypes));
        }
        for (QueryParser.HavingCondition havingCondition : havingConditions) {
            addMissingHavingAggregates(havingCondition, resultRow, group, combinedColumnTypes);
        }
    }

    private void populateNonAggregateColumns(List<Map<String, Object>> group, Map<String, Object> resultRow) {
        for (String column : columns) {
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            if (!resultRow.containsKey(normalizedColumn)) {
                Object value = group.get(0).get(normalizedColumn);
                resultRow.put(normalizedColumn, value);
            }
        }
    }

    // Prompt 29: sorts the final rows by the ORDER BY clauses.
    private void applyOrderBy(List<Map<String, Object>> finalRows, boolean useStreaming) {
        if (!orderBy.isEmpty()) {
            finalRows.sort((row1, row2) -> compareRows(row1, row2, orderBy));
            LOGGER.log(Level.FINE, "Applied ORDER BY with {0} clauses (streaming={1})",
                    new Object[]{orderBy.size(), useStreaming && finalRows.size() > maxInMemoryRows});
        }
    }

    // Prompt 29: applies LIMIT/OFFSET (and the single-row aggregates-without-
    // GROUP BY case), projects the selected columns and returns the result.
    private List<Map<String, Object>> applyLimitOffset(List<Map<String, Object>> finalRows,
            Map<String, Class<?>> combinedColumnTypes) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (offset != null && limit == null) {
            LOGGER.warning("OFFSET without LIMIT may be inefficient");
        }

        if (!aggregates.isEmpty() && groupBy.isEmpty()) {
            // Aggregates without GROUP BY produce a single row. The aggregate
            // must be computed over ALL (filtered and sorted) rows; LIMIT/OFFSET
            // then applies to that single result row, so
            // SELECT COUNT(*) FROM t LIMIT 1 returns the full count and
            // SELECT COUNT(*) FROM t LIMIT 0 returns an empty result.
            Map<String, Object> resultRow = new HashMap<>();
            for (QueryParser.AggregateFunction agg : aggregates) {
                String resultKey = agg.alias != null ? agg.alias : agg.toString();
                resultRow.put(resultKey, computeAggregate(agg, finalRows, combinedColumnTypes));
            }
            int rowsSkipped = (offset != null) ? offset : 0;
            int maxRows = (limit != null) ? limit : Integer.MAX_VALUE;
            if (rowsSkipped == 0 && maxRows > 0) {
                checkResultRowLimit(result.size(), ErrorMessages.STAGE_RESULT);
                result.add(resultRow);
            }
        } else {
            int rowsSkipped = (offset != null) ? offset : 0;
            int maxRows = (limit != null) ? limit : Integer.MAX_VALUE;
            List<Map<String, Object>> selectedRows = IntStream.range(0, finalRows.size())
                    .skip(rowsSkipped)
                    .limit(maxRows)
                    .mapToObj(finalRows::get)
                    .collect(Collectors.toList());
            if (groupAggregateKeys.isEmpty()) {
                for (Map<String, Object> row : selectedRows) {
                    checkResultRowLimit(result.size(), ErrorMessages.STAGE_RESULT);
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
                    checkResultRowLimit(result.size(), ErrorMessages.STAGE_RESULT);
                    result.add(filteredRow);
                }
            }
        }
        return result;
    }

    private boolean shouldUseStreaming() {
        // Prompt 17: streaming is disabled. The streaming result iterator wrote
        // every flat row to a temp file once the estimate exceeded
        // max.inmemory.rows and then read it all back, but the pipeline
        // materialises the full result in memory anyway (filteredRows /
        // finalRows / ORDER BY / GROUP BY all need it), so the disk round-trip
        // only added serialization cost without saving memory. Measured on the
        // 360k-row 600x600 joins: 2-4x faster with the in-memory path, no OOM
        // (MAX_RESULT_ROWS still bounds the result).
        return false;
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
                        checkResultRowLimit(fallback.size(), "join spill fallback");
                        fallback.add(spill.next());
                    }
                } catch (IOException ignored) {
                    // Best-effort drain; row is added directly below on failure
                    LOGGER.fine("Best-effort spill drain failed: " + ignored.getMessage());
                } finally {
                    spill.close();
                }
            }
        }
        checkResultRowLimit(fallback.size(), "join spill fallback");
        fallback.add(flattenedRow);
    }

    /**
     * Runs a classic in-memory hash join: build a hash table over
     * {@code buildRows} keyed by the join column, then probe it with the rows
     * of {@code probeTable}. Records the hash table size and the build/probe
     * times as metrics.
     */
    private List<Map<String, Map<String, Object>>> runInMemoryHashJoin(
            List<Map<String, Object>> buildRows, Table buildTable, Table probeTable,
            String buildColumnKey, String probeColumnKey, JoinContext ctx) {

        long buildStart = System.nanoTime();
        Map<Object, List<Map<String, Object>>> hashTable = new HashMap<>();
        for (int i = 0; i < buildRows.size(); i++) {
            Map<String, Object> row = buildRows.get(i);
            Object key = row.get(buildColumnKey);
            if (key != null) {
                hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                ReentrantReadWriteLock lock = buildTable.getRowLock(i);
                lock.readLock().lock();
                ctx.acquiredLocks.add(lock);
            }
        }
        long buildTimeMs = (System.nanoTime() - buildStart) / 1_000_000;

        long probeStart = System.nanoTime();
        List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();
        List<Map<String, Object>> probeRows = getIndexedRows(probeTable, ctx.join.onConditions, ctx.probeTableName, ctx.combinedColumnTypes);
        if (probeRows == null) {
            probeRows = probeTable.getLiveRows();
        }
        for (Map<String, Object> probeRow : probeRows) {
            Object probeKey = probeRow.get(probeColumnKey);
            if (probeKey != null) {
                List<Map<String, Object>> matches = hashTable.get(probeKey);
                if (matches != null) {
                    for (Map<String, Object> buildRow : matches) {
                        emitHashJoinMatch(probeRow, buildRow, newJoinedRows, ctx);
                    }
                }
            }
        }
        long probeTimeMs = (System.nanoTime() - probeStart) / 1_000_000;

        lastHashJoinTableSize = hashTable.size();
        lastHashJoinBuildTimeMs = buildTimeMs;
        lastHashJoinProbeTimeMs = probeTimeMs;
        lastJoinUsedPartitioning = false;
        LOGGER.log(Level.FINE, "Hash join metrics: hashTableSize={0}, buildTime={1} ms, probeTime={2} ms",
                new Object[]{hashTable.size(), buildTimeMs, probeTimeMs});
        return newJoinedRows;
    }

    /**
     * Runs a partitioned (grace) hash join for build sides whose estimated hash
     * table exceeds {@code max.hash.table.size.mb}. The build and probe rows
     * are split by join-key hash into {@code partitionCount} partitions that
     * are spilled to temporary files, then joined one partition at a time, so
     * peak memory is bounded by a single partition instead of the whole table.
     *
     * @throws IOException if the temporary partition files cannot be written
     */
    private void spillBuildPartitions(List<Map<String, Object>> buildRows, Table buildTable,
            String buildColumnKey, int partitionCount, File tempDir, JoinContext ctx) throws IOException {
        DataOutputStream[] writers = new DataOutputStream[partitionCount];
        IntStream.range(0, buildRows.size())
                .filter(i -> buildRows.get(i).get(buildColumnKey) != null)
                .forEach(i -> {
                    Map<String, Object> row = buildRows.get(i);
                    Object key = row.get(buildColumnKey);
                    int p = (key.hashCode() & 0x7fffffff) % partitionCount;
                    if (writers[p] == null) {
                        try {
                            writers[p] = new DataOutputStream(new BufferedOutputStream(
                                    new FileOutputStream(new File(tempDir, "build-" + p + ErrorMessages.BIN_EXTENSION)), 1 << 20));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    try {
                        writeBinaryRow(writers[p], row);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    ReentrantReadWriteLock lock = buildTable.getRowLock(i);
                    lock.readLock().lock();
                    ctx.acquiredLocks.add(lock);
                });
        closePartitionWriters(writers);
    }

    private void spillProbePartitions(List<Map<String, Object>> probeRows,
            String probeColumnKey, int partitionCount, File tempDir) throws IOException {
        DataOutputStream[] writers = new DataOutputStream[partitionCount];
        probeRows.stream()
                .filter(row -> row.get(probeColumnKey) != null)
                .forEach(row -> {
                    Object key = row.get(probeColumnKey);
                    int p = (key.hashCode() & 0x7fffffff) % partitionCount;
                    if (writers[p] == null) {
                        try {
                            writers[p] = new DataOutputStream(new BufferedOutputStream(
                                    new FileOutputStream(new File(tempDir, "probe-" + p + ErrorMessages.BIN_EXTENSION)), 1 << 20));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    try {
                        writeBinaryRow(writers[p], row);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        closePartitionWriters(writers);
    }

    private static void closePartitionWriters(DataOutputStream[] writers) throws IOException {
        for (DataOutputStream writer : writers) {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    private long joinPartitions(int partitionCount, File tempDir, String buildColumnKey,
            String probeColumnKey, List<Map<String, Map<String, Object>>> newJoinedRows, JoinContext ctx) throws IOException {
        long totalHashEntries = 0;
        for (int p = 0; p < partitionCount; p++) {
            File buildFile = new File(tempDir, "build-" + p + ErrorMessages.BIN_EXTENSION);
            File probeFile = new File(tempDir, "probe-" + p + ErrorMessages.BIN_EXTENSION);
            if (!buildFile.exists() || !probeFile.exists()) {
                continue;
            }

            Map<Object, List<Map<String, Object>>> partitionHash = new HashMap<>();
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(buildFile), 1 << 20))) {
                Map<String, Object> row;
                while ((row = readBinaryRow(in)) != null) {
                    Object key = row.get(buildColumnKey);
                    if (key != null) {
                        partitionHash.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                    }
                }
            }
            totalHashEntries += partitionHash.size();

            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(probeFile), 1 << 20))) {
                Map<String, Object> probeRow;
                while ((probeRow = readBinaryRow(in)) != null) {
                    Object probeKey = probeRow.get(probeColumnKey);
                    if (probeKey != null) {
                        List<Map<String, Object>> matches = partitionHash.get(probeKey);
                        if (matches != null) {
                            for (Map<String, Object> buildRow : matches) {
                                emitHashJoinMatch(probeRow, buildRow, newJoinedRows, ctx);
                            }
                        }
                    }
                }
            }
        }
        return totalHashEntries;
    }

    private static void cleanupPartitionFiles(File tempDir) {
        File[] tempFiles = tempDir.listFiles();
        if (tempFiles != null) {
            for (File tempFile : tempFiles) {
                try {
                    Files.deleteIfExists(tempFile.toPath());
                } catch (IOException ignored) {
                    LOGGER.fine("Temp spill file cleanup failed: " + ignored.getMessage());
                }
            }
        }
        try {
            Files.deleteIfExists(tempDir.toPath());
        } catch (IOException ignored) {
            LOGGER.fine("Temp spill dir cleanup failed: " + ignored.getMessage());
        }
    }

    private List<Map<String, Map<String, Object>>> runPartitionedHashJoin(
            List<Map<String, Object>> buildRows, Table buildTable, Table probeTable,
            String buildColumnKey, String probeColumnKey, JoinContext ctx) throws IOException {

        int partitionCount = choosePartitionCount(buildRows.size());
        File tempDir = Files.createTempDirectory("diesel-hj-" + System.nanoTime() + "-").toFile();

        long buildStart = System.nanoTime();
        spillBuildPartitions(buildRows, buildTable, buildColumnKey, partitionCount, tempDir, ctx);

        List<Map<String, Object>> probeRows = getIndexedRows(probeTable, ctx.join.onConditions, ctx.probeTableName, ctx.combinedColumnTypes);
        if (probeRows == null) {
            probeRows = probeTable.getLiveRows();
        }
        spillProbePartitions(probeRows, probeColumnKey, partitionCount, tempDir);
        long buildTimeMs = (System.nanoTime() - buildStart) / 1_000_000;

        long probeStart = System.nanoTime();
        List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();
        long totalHashEntries = joinPartitions(partitionCount, tempDir, buildColumnKey, probeColumnKey, newJoinedRows, ctx);
        long probeTimeMs = (System.nanoTime() - probeStart) / 1_000_000;

        cleanupPartitionFiles(tempDir);

        lastHashJoinTableSize = totalHashEntries;
        lastHashJoinBuildTimeMs = buildTimeMs;
        lastHashJoinProbeTimeMs = probeTimeMs;
        lastJoinUsedPartitioning = true;
        LOGGER.log(Level.INFO, "Partitioned hash join metrics: partitions={0}, hashTableSize={1}, buildTime={2} ms, probeTime={3} ms",
                new Object[]{partitionCount, totalHashEntries, buildTimeMs, probeTimeMs});
        return newJoinedRows;
    }

    /**
     * Emits one matching (probe, build) pair the same way for both the
     * in-memory and the partitioned hash join: either flattened straight into
     * the streaming spill, or wrapped as a joined row.
     */
    private void emitHashJoinMatch(Map<String, Object> probeRow, Map<String, Object> buildRow,
                               List<Map<String, Map<String, Object>>> newJoinedRows, JoinContext ctx) {
        if (ctx.onlyEquality) {
            if (ctx.lastStream) {
                Map<String, Object> flatRow = new HashMap<>((probeRow.size() + buildRow.size()) * 4 / 3 + 1);
                flattenInto(flatRow, probeRow, ctx.probeTableName);
                flattenInto(flatRow, buildRow, ctx.buildTableName);
                spillFilteredRow(ctx.spill, ctx.spillActive, ctx.spillFallback, flatRow,
                        ctx.whereConditions, ctx.combinedColumnTypes, ctx.tables);
            } else {
                Map<String, Map<String, Object>> newRow = new HashMap<>(2);
                newRow.put(ctx.probeTableName, probeRow);
                newRow.put(ctx.buildTableName, buildRow);
                checkResultRowLimit(newJoinedRows.size(), ErrorMessages.STAGE_JOIN);
                newJoinedRows.add(newRow);
            }
        } else {
            Map<String, Object> flattenedRow = flattenJoinedPair(probeRow, ctx.probeTableName, buildRow, ctx.buildTableName);
            if (evaluateConditions(flattenedRow, ctx.join.onConditions, ctx.combinedColumnTypes, ctx.tables)) {
                if (ctx.lastStream) {
                    spillFilteredRow(ctx.spill, ctx.spillActive, ctx.spillFallback, flattenedRow,
                            ctx.whereConditions, ctx.combinedColumnTypes, ctx.tables);
                } else {
                    Map<String, Map<String, Object>> newRow = new HashMap<>(2);
                    newRow.put(ctx.probeTableName, probeRow);
                    newRow.put(ctx.buildTableName, buildRow);
                    checkResultRowLimit(newJoinedRows.size(), ErrorMessages.STAGE_JOIN);
                    newJoinedRows.add(newRow);
                }
            }
        }
    }

    /**
     * Runs the nested-loop join (also used as the block nested loop fallback
     * when a hash join would exceed the in-memory row budget). Iterates every
     * (outer x inner) pair and applies CROSS / key-equality / ON-condition
     * matching, streaming flattened results when the last join is streaming.
     */
    private record JoinKeyMapping(boolean equalsJoin, String leftJoinKey, String rightJoinKey,
            List<String> rightSrcKeys, List<String> rightTargetKeys) {}

    private JoinKeyMapping resolveJoinKeyMapping(List<Map<String, Object>> joinRows, JoinContext ctx) {
        String rightPrefix = ctx.join.tableName + ".";
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

        boolean equalsJoin = ctx.join.onConditions.isEmpty() && ctx.join.leftColumn != null && ctx.join.rightColumn != null;
        String leftJoinKey = equalsJoin ? normalizeColumnKey(ctx.join.leftColumn, ctx.join.originalTable) : null;
        String rightJoinKey = equalsJoin ? normalizeColumnKey(ctx.join.rightColumn, ctx.join.tableName) : null;

        return new JoinKeyMapping(equalsJoin, leftJoinKey, rightJoinKey, rightSrcKeys, rightTargetKeys);
    }

    private void processStreamingNestedLoop(Map<String, Map<String, Object>> currentJoin,
            Map<String, Object> evalRow, List<Map<String, Object>> joinRows,
            JoinKeyMapping keyMapping, JoinContext ctx) {
        Map<String, Object> leftRow = keyMapping.equalsJoin() ? currentJoin.get(ctx.join.originalTable) : null;
        Object leftValue = keyMapping.equalsJoin() ? leftRow.get(keyMapping.leftJoinKey()) : null;
        for (int j = 0; j < joinRows.size(); j++) {
            Map<String, Object> rightRow = joinRows.get(j);
            Map<String, Object> flatRow = new HashMap<>(evalRow.size() + keyMapping.rightSrcKeys().size() + 1);
            flatRow.putAll(evalRow);
            for (int k = 0; k < keyMapping.rightSrcKeys().size(); k++) {
                flatRow.put(keyMapping.rightTargetKeys().get(k), rightRow.get(keyMapping.rightSrcKeys().get(k)));
            }
            boolean matches;
            if (ctx.join.joinType == QueryParser.JoinType.CROSS) {
                matches = true;
            } else if (keyMapping.equalsJoin()) {
                matches = valuesEqual(leftValue, rightRow.get(keyMapping.rightJoinKey()));
            } else if (!ctx.join.onConditions.isEmpty()) {
                matches = evaluateConditions(flatRow, ctx.join.onConditions, ctx.combinedColumnTypes, ctx.tables);
            } else {
                throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
            }
            if (matches) {
                spillFilteredRow(ctx.spill, ctx.spillActive, ctx.spillFallback, flatRow,
                        ctx.whereConditions, ctx.combinedColumnTypes, ctx.tables);
            }
        }
    }

    private void processNonStreamingNestedLoop(Map<String, Map<String, Object>> currentJoin,
            Map<String, Object> evalRow, List<Map<String, Object>> joinRows,
            JoinKeyMapping keyMapping, List<Map<String, Map<String, Object>>> newJoinedRows, JoinContext ctx) {
        for (int j = 0; j < joinRows.size(); j++) {
            Map<String, Object> rightRow = joinRows.get(j);
            Map<String, Map<String, Object>> newRow = new HashMap<>(currentJoin);
            newRow.put(ctx.join.tableName, rightRow);

            if (ctx.join.joinType == QueryParser.JoinType.CROSS) {
                checkResultRowLimit(newJoinedRows.size(), ErrorMessages.STAGE_JOIN);
                newJoinedRows.add(newRow);
            } else if (keyMapping.equalsJoin()) {
                Map<String, Object> leftRow = currentJoin.get(ctx.join.originalTable);
                if (valuesEqual(leftRow.get(keyMapping.leftJoinKey()), rightRow.get(keyMapping.rightJoinKey()))) {
                    checkResultRowLimit(newJoinedRows.size(), ErrorMessages.STAGE_JOIN);
                    newJoinedRows.add(newRow);
                }
            } else if (!ctx.join.onConditions.isEmpty()) {
                for (int k = 0; k < keyMapping.rightSrcKeys().size(); k++) {
                    evalRow.put(keyMapping.rightTargetKeys().get(k), rightRow.get(keyMapping.rightSrcKeys().get(k)));
                }
                if (evaluateConditions(evalRow, ctx.join.onConditions, ctx.combinedColumnTypes, ctx.tables)) {
                    checkResultRowLimit(newJoinedRows.size(), ErrorMessages.STAGE_JOIN);
                    newJoinedRows.add(newRow);
                    LOGGER.log(Level.FINE, "JOIN ON condition satisfied for {0} with conditions: {1}",
                            new Object[]{ctx.join.tableName, ctx.join.onConditions});
                }
            } else {
                throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
            }
        }
    }

    private List<Map<String, Map<String, Object>>> runBlockNestedLoopJoin(
            List<Map<String, Map<String, Object>>> joinedRows, Table joinTable, JoinContext ctx) {

        List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();
        List<Map<String, Object>> joinRows = getIndexedRows(joinTable, ctx.join.onConditions, ctx.join.tableName, ctx.combinedColumnTypes);
        if (joinRows == null) {
            joinRows = joinTable.getLiveRows();
        }

        JoinKeyMapping keyMapping = resolveJoinKeyMapping(joinRows, ctx);

        for (int j = 0; j < joinRows.size(); j++) {
            ReentrantReadWriteLock joinLock = joinTable.getRowLock(j);
            joinLock.readLock().lock();
            ctx.acquiredLocks.add(joinLock);
        }

        for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
            Map<String, Object> evalRow = flattenJoinedRow(currentJoin);
            if (ctx.lastStream) {
                processStreamingNestedLoop(currentJoin, evalRow, joinRows, keyMapping, ctx);
            } else {
                processNonStreamingNestedLoop(currentJoin, evalRow, joinRows, keyMapping, newJoinedRows, ctx);
            }
        }
        return newJoinedRows;
    }

    /**
     * Rough estimate of the in-memory hash table size (in bytes) that would be
     * built from {@code buildRows}, based on the build table's column types
     * plus the per-entry {@link HashMap} overhead.
     */
    private long estimateHashTableSizeBytes(List<Map<String, Object>> buildRows, Table buildTable) {
        if (buildRows == null || buildRows.isEmpty()) {
            return 0;
        }
        long avgRowBytes = 0;
        Map<String, Class<?>> columnTypes = buildTable.getColumnTypes();
        if (columnTypes.isEmpty()) {
            avgRowBytes = 64;
        } else {
            for (Class<?> type : columnTypes.values()) {
                if (type == Long.class) {
                    avgRowBytes += 8;
                } else if (type == Integer.class) {
                    avgRowBytes += 4;
                } else if (type == Double.class) {
                    avgRowBytes += 8;
                } else if (type == Float.class) {
                    avgRowBytes += 4;
                } else if (type == Boolean.class) {
                    avgRowBytes += 1;
                } else if (type == BigDecimal.class) {
                    avgRowBytes += 16;
                } else {
                    // String/Character/LocalDate/LocalDateTime/UUID/other objects
                    avgRowBytes += 32;
                }
            }
        }
        return buildRows.size() * (avgRowBytes + 48L);
    }

    /**
     * Chooses the number of partitions for the partitioned hash join so that
     * each partition holds at most roughly {@code max.inmemory.rows} rows,
     * bounded to [1, 256].
     */
    private int choosePartitionCount(long buildRowCount) {
        if (buildRowCount <= 0) {
            return 1;
        }
        long perPartition = Math.max(1, maxInMemoryRows);
        int byRows = (int) Math.max(1, Math.ceil((double) buildRowCount / perPartition));
        return Math.max(1, Math.min(256, byRows));
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
            } else if (v instanceof Integer i) {
                out.writeByte(BIN_INT);
                out.writeInt(i);
            } else if (v instanceof Long l) {
                out.writeByte(BIN_LONG);
                out.writeLong(l);
            } else if (v instanceof Boolean b) {
                out.writeByte(BIN_BOOL);
                out.writeBoolean(b);
            } else if (v instanceof BigDecimal bd) {
                out.writeByte(BIN_BIGDEC);
                writeBinaryUtf(out, bd.toString());
            } else if (v instanceof Float f) {
                out.writeByte(BIN_FLOAT);
                out.writeFloat(f);
            } else if (v instanceof Double d) {
                out.writeByte(BIN_DOUBLE);
                out.writeDouble(d);
            } else if (v instanceof LocalDate ld) {
                out.writeByte(BIN_DATE);
                writeBinaryUtf(out, ld.toString());
            } else if (v instanceof LocalDateTime ldt) {
                out.writeByte(BIN_DATETIME);
                writeBinaryUtf(out, ldt.toString());
            } else if (v instanceof UUID u) {
                out.writeByte(BIN_UUID);
                writeBinaryUtf(out, u.toString());
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
                // Best-effort close; failure is non-critical
                LOGGER.fine("Best-effort writer close failed: " + ignored.getMessage());
            }
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException ignored) {
                // Best-effort close; failure is non-critical
                LOGGER.fine("Best-effort reader close failed: " + ignored.getMessage());
            }
            if (spillFile != null) {
                try {
                    Files.deleteIfExists(spillFile.toPath());
                } catch (IOException ignored) {
                    // Best-effort temp file cleanup
                    LOGGER.fine("Best-effort spill file cleanup failed: " + ignored.getMessage());
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
            if (conjunction == null || conjunction.equals(SqlKeywords.AND)) {
                result = result && conditionResult;
            } else if (conjunction.equals(SqlKeywords.OR)) {
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
        return switch (agg.functionName) {
            case SqlKeywords.COUNT -> computeCount(agg, rows);
            case SqlKeywords.MIN -> computeMin(agg, rows);
            case SqlKeywords.MAX -> computeMax(agg, rows);
            case SqlKeywords.AVG -> computeAvg(agg, rows, combinedColumnTypes);
            case SqlKeywords.SUM -> computeSum(agg, rows, combinedColumnTypes);
            default -> throw new UnsupportedOperationException("Aggregate function not supported: " + agg.functionName);
        };
    }

    private long computeCount(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows) {
        if (agg.column == null) {
            return AggregateFunctions.count(rows);
        }
        String columnKey = normalizeColumnName(agg.column, mainTableName);
        List<Object> values = rows.stream().map(row -> row.get(columnKey)).collect(Collectors.toList());
        return AggregateFunctions.count(values);
    }

    private Object computeMin(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows) {
        if (agg.column == null) {
            throw new IllegalArgumentException("MIN requires a column argument");
        }
        String columnKey = normalizeColumnName(agg.column, mainTableName);
        List<Number> values = collectNumericValues(rows, columnKey);
        if (values.isEmpty()) {
            return null;
        }
        if (values.get(0) instanceof Integer) {
            return AggregateFunctions.minInt(values);
        }
        return rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                .min(this::compareValues).orElse(null);
    }

    private Object computeMax(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows) {
        if (agg.column == null) {
            throw new IllegalArgumentException("MAX requires a column argument");
        }
        String columnKey = normalizeColumnName(agg.column, mainTableName);
        List<Number> values = collectNumericValues(rows, columnKey);
        if (values.isEmpty()) {
            return null;
        }
        if (values.get(0) instanceof Integer) {
            return AggregateFunctions.maxInt(values);
        }
        return rows.stream().map(row -> row.get(columnKey)).filter(Objects::nonNull)
                .max(this::compareValues).orElse(null);
    }

    private Object computeAvg(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows,
                              Map<String, Class<?>> combinedColumnTypes) {
        if (agg.column == null) {
            throw new IllegalArgumentException("AVG requires a column argument");
        }
        String columnKey = normalizeColumnName(agg.column, mainTableName);
        List<Number> values = collectNumericValues(rows, columnKey);
        if (values.isEmpty()) {
            return null;
        }
        Double avg = AggregateFunctions.average(values);
        if (avg != null) {
            return coerceNumericResult(BigDecimal.valueOf(avg), combinedColumnTypes.get(columnKey));
        }
        return null;
    }

    private Object computeSum(QueryParser.AggregateFunction agg, List<Map<String, Object>> rows,
                              Map<String, Class<?>> combinedColumnTypes) {
        if (agg.column == null) {
            throw new IllegalArgumentException("SUM requires a column argument");
        }
        String columnKey = normalizeColumnName(agg.column, mainTableName);
        List<Number> values = collectNumericValues(rows, columnKey);
        if (values.isEmpty()) {
            return null;
        }
        Object firstValue = values.get(0);
        if (firstValue instanceof Integer) {
            return coerceNumericResult(BigDecimal.valueOf(AggregateFunctions.sumInt(values)), combinedColumnTypes.get(columnKey));
        } else if (firstValue instanceof Long) {
            return coerceNumericResult(BigDecimal.valueOf(AggregateFunctions.sumLong(values)), combinedColumnTypes.get(columnKey));
        } else if (firstValue instanceof Float) {
            return coerceNumericResult(BigDecimal.valueOf(AggregateFunctions.sumFloat(values)), combinedColumnTypes.get(columnKey));
        } else if (firstValue instanceof Double) {
            return coerceNumericResult(BigDecimal.valueOf(AggregateFunctions.sumDouble(values)), combinedColumnTypes.get(columnKey));
        }
        BigDecimal sum = BigDecimal.ZERO;
        for (Number value : values) {
            sum = sum.add(new BigDecimal(value.toString()));
        }
        return coerceNumericResult(sum, combinedColumnTypes.get(columnKey));
    }

    private List<Number> collectNumericValues(List<Map<String, Object>> rows, String columnKey) {
        return rows.stream().map(row -> row.get(columnKey))
                .filter(Objects::nonNull)
                .filter(v -> v instanceof Number)
                .map(v -> (Number) v)
                .collect(Collectors.toList());
    }

     private int compareRows(Map<String, Object> row1, Map<String, Object> row2, List<QueryParser.OrderByInfo> orderBy) {
         return IntStream.range(0, orderBy.size())
                 .filter(i -> {
                     Object value1 = row1.get(orderByKeys.get(i));
                     Object value2 = row2.get(orderByKeys.get(i));
                     return !(value1 == null && value2 == null);
                 })
                 .map(i -> {
                     QueryParser.OrderByInfo order = orderBy.get(i);
                     Object value1 = row1.get(orderByKeys.get(i));
                     Object value2 = row2.get(orderByKeys.get(i));
                     if (value1 == null) return order.ascending ? -1 : 1;
                     if (value2 == null) return order.ascending ? 1 : -1;
                     int c = compareValues(value1, value2);
                     return applySortDirection(c, order.ascending);
                 })
                 .filter(c -> c != 0)
                 .findFirst()
                 .orElse(0);
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
            String normalizedColumn = resolveColumnFromSelect(column);
            if (normalizedColumn == null) {
                normalizedColumn = resolveColumnFromAlias(column);
            }
            if (normalizedColumn == null) {
                normalizedColumn = normalizeColumnName(column, mainTableName);
            }
            keys.add(normalizedColumn);
        }
        return keys;
    }

    private String resolveColumnFromSelect(String column) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (String selectColumn : columns) {
            String[] parts = selectColumn.trim().split("\\s+AS\\s+|\\s+", 2);
            String selectBase = parts[0].trim();
            String selectAlias = parts.length > 1 ? parts[1].trim() : null;
            String baseUnqualified = selectBase.contains(".") ? selectBase.split("\\.")[1].trim() : selectBase;
            if (unqualifiedColumn.equalsIgnoreCase(selectAlias == null ? baseUnqualified : selectAlias)) {
                return normalizeColumnName(selectBase, mainTableName);
            }
        }
        return null;
    }

    private String resolveColumnFromAlias(String column) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        for (Map.Entry<String, String> aliasEntry : tableAliases.entrySet()) {
            if (column.equalsIgnoreCase(aliasEntry.getKey() + "." + unqualifiedColumn)) {
                return aliasEntry.getValue() + "." + unqualifiedColumn;
            }
        }
        return null;
    }

    /**
     * Decides between a hash join and a nested loop join using the stored table
     * statistics ({@link Table.TableStatistics}, refreshed by ANALYZE TABLE and
     * asynchronously after INSERT/DELETE). The nested-loop cost is the row-count
     * product (every outer row rescans the inner table), while the hash join
     * costs are linear in both sides plus a fixed overhead for allocating and
     * hashing the build table; larger average row sizes weigh the nested loop a
     * bit more, because each repeated inner-scan comparison walks bigger values.
     * Inputs below the crossover use the nested loop, above it the hash join
     * wins - the classic optimizer trade-off, now driven by statistics instead
     * of hardcoded table sizes.
     *
     * @param buildTable the smaller table, chosen as the hash-build side
     * @param probeTable the other table, used as the probe side
     * @return true when the statistics say a nested loop is cheaper
     */
    private boolean preferNestedLoopByStatistics(Table buildTable, Table probeTable) {
        Table.TableStatistics buildStats = buildTable.getStatistics();
        Table.TableStatistics probeStats = probeTable.getStatistics();
        long buildRows = Math.max(1, buildStats.getRowCount());
        long probeRows = Math.max(1, probeStats.getRowCount());

        // Prompt 52: when the smaller side is below 1 000 rows the hash-table
        // build is cheap and O(n + m) always beats O(n × m), so we force
        // the hash-join path regardless of the full cost model.
        long smallerRows = Math.min(buildRows, probeRows);
        if (smallerRows < HASH_JOIN_OVERHEAD_ROWS) {
            return false;
        }

        long avgSize = Math.max(1, (buildStats.getAvgRowSizeBytes() + probeStats.getAvgRowSizeBytes()) / 2);
        double sizeWeight = 1.0 + avgSize / 10000.0;
        double nestedLoopCost = buildRows * (double) probeRows * sizeWeight;
        double hashJoinCost = (buildRows + probeRows) * sizeWeight + HASH_JOIN_OVERHEAD_ROWS;
        return nestedLoopCost < hashJoinCost;
    }

    private boolean canUseHashJoin(QueryParser.JoinInfo join) {
        if (join.joinType != QueryParser.JoinType.INNER &&
                join.joinType != QueryParser.JoinType.LEFT_INNER &&
                join.joinType != QueryParser.JoinType.RIGHT_INNER) {
            return false;
        }
        boolean hasEquality = false;
        for (QueryParser.Condition condition : join.onConditions) {
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
                .anyMatch(c -> Objects.equals(c.conjunction, SqlKeywords.OR));
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

    /**
     * Prompt 15: ensures both sides of a JOIN equality condition carry an index.
     * Handles both the legacy {@code leftColumn/rightColumn} form and the
     * {@code onConditions} form with an EQUALS column comparison. The check is a
     * constant-time index-map lookup, and the auto-creation is a one-time
     * O(n log n) build per (table, column) that later joins reuse, so this never
     * re-runs for the same column. Best effort: unresolved sides (e.g. a
     * three-table chain where the condition references an intermediate table)
     * are simply skipped.
     */
    private void ensureJoinColumnIndexes(Map<String, Table> tables, QueryParser.JoinInfo join) {
        if (join.leftColumn != null && join.rightColumn != null) {
            ensureJoinColumnIndex(tables.get(join.originalTable), join.originalTable, join.leftColumn);
            ensureJoinColumnIndex(tables.get(join.tableName), join.tableName, join.rightColumn);
            return;
        }
        join.onConditions.stream()
                .filter(condition -> condition.operator == QueryParser.Operator.EQUALS && condition.isColumnComparison() && !condition.not)
                .forEach(condition -> {
                    ensureJoinColumnIndex(tables.get(mainTableName), mainTableName, resolveJoinColumn(condition, mainTableName));
                    ensureJoinColumnIndex(tables.get(join.tableName), join.tableName, resolveJoinColumn(condition, join.tableName));
                });
    }

    /**
     * Auto-creates an in-memory B-tree index over {@code column} on
     * {@code table} when no index covers it yet, logging the advisory warning
     * "Consider creating index on TABLE.COLUMN for faster JOIN". Clustered
     * (primary-key) columns are already indexed and are skipped. Never throws:
     * indexing failures only produce a FINE log record so the query always
     * proceeds.
     */
    private void ensureJoinColumnIndex(Table table, String tableName, String column) {
        if (table == null || column == null) {
            return;
        }
        String unqualified = normalizeColumnKey(column, tableName);
        if (table.getIndex(unqualified) != null) {
            return;
        }
        if (table.hasClusteredIndex() && unqualified.equals(table.getClusteredIndexColumn())) {
            return;
        }
        try {
            table.createBTreeIndex(unqualified);
            LOGGER.warning("Consider creating index on " + tableName + "." + unqualified + " for faster JOIN");
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "Skipped auto-creating index on {0}.{1}: {2}",
                    new Object[]{tableName, unqualified, e.getMessage()});
        }
    }

    /**
     * Logs advisory warnings for WHERE-condition columns that lack an index.
     * Unlike {@link #ensureJoinColumnIndex}, this method never auto-creates an
     * index — WHERE patterns are more varied and auto-indexing could pollute
     * the index space.  It only emits an INFO-level warning.
     */
    private void ensureWhereIndexes(Table table, List<QueryParser.Condition> conditions, String tableName) {
        if (conditions == null || conditions.isEmpty()) {
            return;
        }
        for (QueryParser.Condition condition : conditions) {
            if (Objects.equals(condition.conjunction, SqlKeywords.OR) || condition.not
                    || condition.operator == QueryParser.Operator.NOT_EQUALS
                    || condition.isGrouped() || condition.isColumnComparison()
                    || condition.operator == QueryParser.Operator.LIKE
                    || condition.operator == QueryParser.Operator.NOT_LIKE
                    || condition.operator == QueryParser.Operator.IS_NULL
                    || condition.operator == QueryParser.Operator.IS_NOT_NULL) {
                // skip conditions unsuitable for auto-indexing
            } else {
                String columnName = normalizeColumnName(condition.column, tableName);
                if (columnName != null) {
                    String unqualified = normalizeColumnKey(columnName, tableName);
                    if (table.getIndex(unqualified) == null
                            && !(table.hasClusteredIndex() && unqualified.equals(table.getClusteredIndexColumn()))) {
                        try {
                            table.createBTreeIndex(unqualified);
                            LOGGER.warning("Auto-created index on " + tableName + "." + unqualified + " for WHERE filtering");
                        } catch (RuntimeException e) {
                            LOGGER.log(Level.FINE, "Skipped auto-creating index on {0}.{1}: {2}",
                                    new Object[]{tableName, unqualified, e.getMessage()});
                        }
                    }
                }
            }
        }
    }

    private List<Map<String, Object>> getIndexedRows(Table table, List<QueryParser.Condition> conditions, String tableName, Map<String, Class<?>> combinedColumnTypes) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }
        if (hasOrConditions(conditions)) {
            return null;
        }

        List<Set<Integer>> indexedSets = collectIndexRowSets(conditions, table, tableName);
        List<Integer> compositeRows = lookupCompositeIndex(table, conditions, tableName);

        if (indexedSets.isEmpty() && (compositeRows == null || compositeRows.isEmpty())) {
            return null;
        }

        Set<Integer> result = intersectIndexedResults(indexedSets, compositeRows);
        if (result.isEmpty()) {
            return Collections.emptyList();
        }

        List<Integer> sortedResult = new ArrayList<>(result);
        Collections.sort(sortedResult);

        List<Map<String, Object>> coveredRows = tryCoveringIndex(table, new LinkedHashSet<>(sortedResult), conditions, tableName);
        if (coveredRows != null) {
            return coveredRows;
        }

        return collectRowsFromIndices(table, sortedResult);
    }

    private boolean hasOrConditions(List<QueryParser.Condition> conditions) {
        for (QueryParser.Condition condition : conditions) {
            if (Objects.equals(condition.conjunction, SqlKeywords.OR)) {
                return true;
            }
        }
        return false;
    }

    private List<Set<Integer>> collectIndexRowSets(List<QueryParser.Condition> conditions, Table table, String tableName) {
        List<Set<Integer>> indexedSets = new ArrayList<>();
        for (QueryParser.Condition condition : conditions) {
            if (!condition.isGrouped() && !condition.isColumnComparison() && !condition.not) {
                String columnName = normalizeColumnName(condition.column, tableName);
                String unqualifiedColumn = normalizeColumnKey(columnName, tableName);
                Index index = table.getIndex(unqualifiedColumn);
                if (index == null && table.hasClusteredIndex() && unqualifiedColumn.equals(table.getClusteredIndexColumn())) {
                    index = table.getClusteredIndex();
                }
                if (index != null) {
                    List<Integer> rowIndices = lookupIndex(index, condition, tableName, unqualifiedColumn, table);
                    if (rowIndices != null && !rowIndices.isEmpty()) {
                        indexedSets.add(new LinkedHashSet<>(rowIndices));
                        lastIndexLookupCount++;
                    }
                }
            }
        }
        return indexedSets;
    }

    private Set<Integer> intersectIndexedResults(List<Set<Integer>> indexedSets, List<Integer> compositeRows) {
        Set<Integer> result;
        if (!indexedSets.isEmpty()) {
            indexedSets.sort(Comparator.comparingInt(Set::size));
            result = new LinkedHashSet<>(indexedSets.get(0));
            for (int i = 1; i < indexedSets.size(); i++) {
                result.retainAll(indexedSets.get(i));
            }
            if (result.isEmpty()) {
                return result;
            }
            if (compositeRows != null && !compositeRows.isEmpty()) {
                result.retainAll(new LinkedHashSet<>(compositeRows));
            }
        } else {
            result = new LinkedHashSet<>(compositeRows);
        }
        return result;
    }

    private List<Map<String, Object>> collectRowsFromIndices(Table table, List<Integer> sortedResult) {
        List<Map<String, Object>> indexedRows = new ArrayList<>(sortedResult.size());
        int tableSize = table.getRawRowCount();
        List<Map<String, Object>> rawRows = table.getRows();
        for (int idx : sortedResult) {
            if (idx >= 0 && idx < tableSize && !table.isDeleted(idx)) {
                indexedRows.add(rawRows.get(idx));
            }
        }
        return indexedRows;
    }

    /**
     * Attempts to use a composite index for multi-column WHERE conditions.
     * Returns the matching row indices, or null if no composite index applies.
     */
    private List<Integer> lookupCompositeIndex(Table table, List<QueryParser.Condition> conditions, String tableName) {
        // Collect AND-connected equality conditions
        Map<String, Object> equalityColumns = new LinkedHashMap<>();
        for (QueryParser.Condition condition : conditions) {
            if (!condition.isGrouped() && !condition.isColumnComparison() && !condition.not
                    && condition.operator == QueryParser.Operator.EQUALS && condition.value != null) {
                String columnName = normalizeColumnName(condition.column, tableName);
                if (columnName != null) {
                    String unqualified = normalizeColumnKey(columnName, tableName);
                    // Convert condition value to the column's actual type so it matches
                    // the typed keys stored in the composite index (e.g. Integer→Long).
                    Class<?> colType = table.getColumnTypes().get(unqualified);
                    Object converted = colType != null
                            ? new ConditionEvaluator().convertConditionValue(condition.value, unqualified, colType, table.getColumnTypes())
                            : condition.value;
                    equalityColumns.put(unqualified, converted);
                }
            }
        }
        if (equalityColumns.size() < 2) return null;

        // Find best matching composite index
        for (Map.Entry<String, Index> entry : table.getIndexes().entrySet()) {
            if (entry.getValue() instanceof CompositeBTreeIndex compIndex) {
                List<String> compCols = compIndex.getColumns();
                // Check if all composite columns are in the equality conditions
                if (compCols.size() <= equalityColumns.size()) {
                    List<Object> compositeKey = new ArrayList<>(compCols.size());
                    boolean allMatch = true;
                    for (String col : compCols) {
                        if (!equalityColumns.containsKey(col)) { allMatch = false; break; }
                        compositeKey.add(equalityColumns.get(col));
                    }
                    if (allMatch) {
                        return compIndex.search(compositeKey);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Attempts to use a covering index to avoid table row lookups.
     * Returns covered rows, or null if covering index cannot be used.
     */
    private Set<String> collectRequiredSelectColumns() {
        Set<String> requiredColumns = new HashSet<>();
        if (columns != null) {
            for (String col : columns) {
                if (!col.contains("(") && !col.equals("*")) {
                    requiredColumns.add(col);
                }
            }
        }
        return requiredColumns;
    }

    private List<Map<String, Object>> buildCoveredRows(Table table, CoveringBTreeIndex coverIndex, Set<Integer> rowIndices) {
        List<Map<String, Object>> coveredRows = new ArrayList<>(rowIndices.size());
        int tableSize = table.getRawRowCount();
        for (int idx : rowIndices) {
            if (idx >= 0 && idx < tableSize && !table.isDeleted(idx)) {
                Map<String, Object> covered = coverIndex.getCoveredValues(idx);
                if (covered != null) {
                    coveredRows.add(covered);
                }
            }
        }
        lastIndexOnlyScanCount++;
        return coveredRows;
    }

private List<Map<String, Object>> tryCoveringIndex(Table table, Set<Integer> rowIndices,
                                                         List<QueryParser.Condition> conditions, String tableName) {
        Set<String> requiredColumns = collectRequiredSelectColumns();
        if (requiredColumns.isEmpty()) {
            return null;
        }
        for (QueryParser.Condition condition : conditions) {
            if (isCoverableCondition(condition)) {
                String columnName = normalizeColumnName(condition.column, tableName);
                if (columnName != null) {
                    String unqualified = normalizeColumnKey(columnName, tableName);
                    Index index = table.getIndex(unqualified);
                    if (index instanceof CoveringBTreeIndex coverIndex && coverIndex.coversColumns(requiredColumns)) {
                        return buildCoveredRows(table, coverIndex, rowIndices);
                    }
                }
            }
        }
        return null;
    }

    private boolean isCoverableCondition(QueryParser.Condition condition) {
        return !condition.isGrouped() && !condition.isColumnComparison() && !condition.not;
    }

    /**
     * Performs an index lookup for a single WHERE condition.  Returns the
     * matching row indices, or {@code null} if the condition cannot use an
     * index.
     */
    private List<Integer> lookupIndex(Index index, QueryParser.Condition condition, String tableName, String unqualifiedColumn, Table table) {
        // Convert condition value to the column's actual type so it matches
        // the typed keys stored in the index (e.g. parsed Integer → stored Long).
        Class<?> colType = table.getColumnTypes().get(unqualifiedColumn);
        ConditionEvaluator evaluator = new ConditionEvaluator();
        if (condition.operator == QueryParser.Operator.EQUALS && condition.value != null) {
            Object val = colType != null ? evaluator.convertConditionValue(condition.value, unqualifiedColumn, colType, table.getColumnTypes()) : condition.value;
            List<Integer> rowIndices = index.search(val);
            LOGGER.log(Level.FINE, "Used index on {0}.{1} for EQUALS condition, found {2} rows",
                    new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
            return rowIndices;
        }
        if (condition.isInOperator() && condition.inValues != null) {
            List<Integer> rowIndices = new ArrayList<>();
            for (Object inValue : condition.inValues) {
                Object val = colType != null ? evaluator.convertConditionValue(inValue, unqualifiedColumn, colType, table.getColumnTypes()) : inValue;
                rowIndices.addAll(index.search(val));
            }
            LOGGER.log(Level.FINE, "Used index on {0}.{1} for IN condition, found {2} rows",
                    new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
            return rowIndices;
        }
        if (index instanceof BTreeIndex bTreeIndex) {
            // Convert condition value for range searches too.
            if (colType != null && condition.value != null) {
                Object converted = evaluator.convertConditionValue(condition.value, unqualifiedColumn, colType, table.getColumnTypes());
                QueryParser.Condition adjusted = new QueryParser.Condition(condition.column, converted, condition.operator, condition.conjunction, condition.not);
                return lookupBTreeRange(bTreeIndex, adjusted, tableName, unqualifiedColumn);
            }
            return lookupBTreeRange(bTreeIndex, condition, tableName, unqualifiedColumn);
        }
        return null;
    }

    /**
     * Handles range lookups ({@code <}, {@code >}, {@code <=}, {@code >=})
     * on a BTree index.
     */
    private List<Integer> lookupBTreeRange(BTreeIndex bTreeIndex, QueryParser.Condition condition, String tableName, String unqualifiedColumn) {
        return switch (condition.operator) {
            case LESS_THAN -> {
                List<Integer> r = bTreeIndex.rangeSearchHighParallel(condition.value);
                LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for < {2}, found {3} rows",
                        new Object[]{tableName, unqualifiedColumn, condition.value, r.size()});
                yield r;
            }
            case LESS_THAN_OR_EQUALS -> {
                List<Integer> r = bTreeIndex.rangeSearchHighParallel(condition.value);
                LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for <= {2}, found {3} rows",
                        new Object[]{tableName, unqualifiedColumn, condition.value, r.size()});
                yield r;
            }
            case GREATER_THAN -> {
                List<Integer> r = bTreeIndex.rangeSearchLowParallel(condition.value);
                LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for > {2}, found {3} rows",
                        new Object[]{tableName, unqualifiedColumn, condition.value, r.size()});
                yield r;
            }
            case GREATER_THAN_OR_EQUALS -> {
                List<Integer> r = bTreeIndex.rangeSearchLowParallel(condition.value);
                LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for >= {2}, found {3} rows",
                        new Object[]{tableName, unqualifiedColumn, condition.value, r.size()});
                yield r;
            }
            default -> null;
        };
    }

    private boolean valuesEqual(Object left, Object right) {
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Float lf && right instanceof Float rf) {
            return Math.abs(lf - rf) < 1e-7;
        } else if (left instanceof Double ld && right instanceof Double rd) {
            return Math.abs(ld - rd) < 1e-7;
        } else if (left instanceof BigDecimal lbd && right instanceof BigDecimal rbd) {
            return lbd.compareTo(rbd) == 0;
        } else if (left.getClass() == right.getClass()) {
            return left.equals(right);
        }
        return String.valueOf(left).equals(String.valueOf(right));
    }

    private Map<String, Object> flattenJoinedRow(Map<String, Map<String, Object>> joinedRow) {
        int capacity = 1;
        for (Map<String, Object> row : joinedRow.values()) {
            capacity += row.size();
        }
        Map<String, Object> flattened = new HashMap<>(capacity);
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
        return evaluateConditions3vl(row, conditions, combinedColumnTypes, tables).isTrue();
    }

    /**
     * Вычисляет список условий по правилам трёхзначной логики SQL
     * (см. {@link ThreeValuedLogic}). Правый операнд не вычисляется, если левый
     * уже определяет результат: {@code TRUE OR X = TRUE}, {@code FALSE AND X = FALSE}.
     */
    private ThreeValuedLogic evaluateConditions3vl(Map<String, Object> row, List<QueryParser.Condition> conditions,
                                           Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        if (conditions.isEmpty()) {
            return TRUE;
        }
        logConditionEvaluation(conditions);

        // SQL precedence: AND binds tighter than OR, so the flat condition list
        // is evaluated as a disjunction of AND-segments. Each segment short-
        // circuits: FALSE AND anything = FALSE (skip the rest of the segment)
        // and TRUE OR anything = TRUE (skip the remaining segments). UNKNOWN
        // result must not be conflated with an uninitialized accumulator, hence
        // the explicit initialized flags.
        ThreeValuedLogic orResult = UNKNOWN;
        boolean orInitialized = false;
        ThreeValuedLogic andResult = UNKNOWN;
        boolean andInitialized = false;

        for (int i = 0; i < conditions.size(); i++) {
            QueryParser.Condition condition = conditions.get(i);
            String conjunction = condition.conjunction;
            boolean orBoundary = i > 0 && Objects.equals(conjunction, SqlKeywords.OR);

            if (orBoundary) {
                orResult = orInitialized ? orResult.or(andResult) : andResult;
                orInitialized = true;
                if (orResult.orIsDetermined()) {
                    return orResult;
                }
                andResult = UNKNOWN;
                andInitialized = false;
            }

            if (!andInitialized) {
                andResult = evaluateCondition3vl(row, condition, combinedColumnTypes, tables);
                andInitialized = true;
            } else if (andResult.andIsDetermined()) {
                while (i + 1 < conditions.size()) {
                    String nextConj = conditions.get(i + 1).conjunction;
                    if (Objects.equals(nextConj, SqlKeywords.OR)) {
                        break;
                    }
                    i++;
                }
            } else {
                ThreeValuedLogic value = evaluateCondition3vl(row, condition, combinedColumnTypes, tables);
                andResult = andResult.and(value);
            }
        }

        return orInitialized ? orResult.or(andResult) : andResult;
    }

    private void logConditionEvaluation(List<QueryParser.Condition> conditions) {
        if (!LOGGER.isLoggable(Level.FINE)) {
            return;
        }
        StringBuilder sb = new StringBuilder("Evaluating WHERE: ");
        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) {
                String conjunction = conditions.get(i).conjunction;
                sb.append(' ').append(conjunction != null ? conjunction : SqlKeywords.AND).append(' ');
            }
            sb.append(conditions.get(i));
        }
        LOGGER.fine(sb.toString());
    }

    private ThreeValuedLogic evaluateCondition3vl(Map<String, Object> row, QueryParser.Condition condition,
                                         Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        if (condition.isGrouped()) {
            return evaluateGroupedCondition(row, condition, combinedColumnTypes, tables);
        }
        if (condition.isNullOperator()) {
            return evaluateIsNullCondition(row, condition);
        }
        if (condition.isInOperator()) {
            return evaluateInCondition(row, condition, tables);
        }
        if (condition.isColumnComparison()) {
            return evaluateColumnComparison(row, condition);
        }
        return evaluateScalarCondition(row, condition, tables);
    }

    private ThreeValuedLogic evaluateGroupedCondition(Map<String, Object> row, QueryParser.Condition condition,
                                                      Map<String, Class<?>> combinedColumnTypes, Map<String, Table> tables) {
        ThreeValuedLogic subResult = evaluateConditions3vl(row, condition.subConditions, combinedColumnTypes, tables);
        return condition.not ? subResult.not() : subResult;
    }

    private ThreeValuedLogic evaluateIsNullCondition(Map<String, Object> row, QueryParser.Condition condition) {
        String column = normalizeColumnName(condition.column, mainTableName);
        Object value = row.get(column);
        boolean isNull = value == null;
        boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
        return evaluateNestedCondition(condition.not, result);
    }

    private ThreeValuedLogic evaluateInCondition(Map<String, Object> row, QueryParser.Condition condition,
                                                 Map<String, Table> tables) {
        String column = normalizeColumnName(condition.column, mainTableName);
        Object value = row.get(column);
        if (value == null) {
            return UNKNOWN;
        }

        List<Object> inValues = resolveInValues(condition, row, tables);
        if (inValues == null) {
            throw new IllegalStateException("IN condition has no values or subquery results");
        }

        boolean inResult = condition.inValueSet != null && condition.inValueSet.contains(value)
                || inValues.stream().anyMatch(v -> valuesEqual(v, value));
        boolean result = condition.not ? !inResult : inResult;
        return result ? TRUE : FALSE;
    }

    private List<Object> resolveInValues(QueryParser.Condition condition, Map<String, Object> row, Map<String, Table> tables) {
        if (condition.subQuery != null) {
            Database database = Objects.requireNonNull(tables.get(mainTableName).getDatabase(),
                    ErrorMessages.TABLE_PREFIX + mainTableName + ErrorMessages.NOT_ATTACHED_TO_DB);
            String subQueryString = condition.subQuery.toString().trim();
            if (subQueryString.startsWith("(") && subQueryString.endsWith(")")) {
                subQueryString = subQueryString.substring(1, subQueryString.length() - 1).trim();
            }
            String resolvedSubQuery = substituteOuterReferences(subQueryString, row);
            return inSubQueryCache.computeIfAbsent(resolvedSubQuery, key -> executeSubQueryForIn(key, database));
        }
        return condition.inValues;
    }

    private List<Object> executeSubQueryForIn(String key, Database database) {
        LOGGER.log(Level.FINE, "Executing subquery: {0}", key);
        Object subQueryResult = database.executeQuery(key, transactionId);
        if (!(subQueryResult instanceof List<?> subList)) {
            throw new IllegalStateException("Subquery must return a list of rows");
        }
        List<Object> values = new ArrayList<>();
        for (Map<String, Object> subRow : (List<Map<String, Object>>) subList) {
            if (!subRow.isEmpty()) {
                values.add(subRow.values().iterator().next());
            }
        }
        return values;
    }

    private ThreeValuedLogic evaluateColumnComparison(Map<String, Object> row, QueryParser.Condition condition) {
        String leftColumn = normalizeColumnName(condition.column, mainTableName);
        String rightColumn = normalizeColumnName(condition.rightColumn, mainTableName);
        Object leftValue = row.get(leftColumn);
        Object rightValue = row.get(rightColumn);
        return compareConditionOperand(leftValue, rightValue, condition);
    }

    private ThreeValuedLogic evaluateScalarCondition(Map<String, Object> row, QueryParser.Condition condition,
                                                     Map<String, Table> tables) {
        String column = normalizeColumnName(condition.column, mainTableName);
        Object rowValue = row.get(column);
        if (condition.subQuery != null) {
            Database database = Objects.requireNonNull(tables.get(mainTableName).getDatabase(),
                    ErrorMessages.TABLE_PREFIX + mainTableName + ErrorMessages.NOT_ATTACHED_TO_DB);
            String subQueryString = condition.subQuery.toString();
            String resolvedKey = substituteOuterReferences(subQueryString, row);
            Object subQueryValue = scalarSubQueryCache.computeIfAbsent(resolvedKey,
                    key -> evaluateGroupBySubQuery(key, Collections.emptyMap(), database));
            return compareConditionOperand(rowValue, subQueryValue, condition);
        }
        Object conditionValue = condition.value;
        return compareConditionOperand(rowValue, conditionValue, condition);
    }

    private ThreeValuedLogic compareConditionOperand(Object leftValue, Object rightValue, QueryParser.Condition condition) {
        if (leftValue == null || rightValue == null) {
            return UNKNOWN;
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
                throw new IllegalStateException(ErrorMessages.UNSUPPORTED_OPERATOR_PREFIX + condition.operator);
        }
        return evaluateNestedCondition(condition.not, comparisonResult);
    }

    private int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return evaluateNestedCondition(left == right, 0, left == null, -1, 1);
        }
        if (left instanceof BigDecimal lbd && right instanceof BigDecimal rbd) {
            return lbd.compareTo(rbd);
        }
        if (left instanceof Number lNum && right instanceof Number rNum) {
            if (isIntegral(left) && isIntegral(right)) {
                return Long.compare(lNum.longValue(), rNum.longValue());
            }
            return new BigDecimal(lNum.toString()).compareTo(new BigDecimal(rNum.toString()));
        }
        if (left instanceof LocalDate ld1 && right instanceof LocalDate ld2) {
            return ld1.compareTo(ld2);
        }
        if (left instanceof LocalDateTime ldt1 && right instanceof LocalDateTime ldt2) {
            return ldt1.compareTo(ldt2);
        }
        if (left instanceof Boolean b1 && right instanceof Boolean b2) {
            return b1.compareTo(b2);
        }
        if (left instanceof UUID u1 && right instanceof UUID u2) {
            return u1.compareTo(u2);
        }
        if (left instanceof String s1 && right instanceof String s2) {
            return s1.compareTo(s2);
        }
        if (left instanceof Character c1 && right instanceof Character rc) {
            return c1.compareTo(rc);
        }
        throw new IllegalArgumentException("Incompatible types for comparison: " + left.getClass() + " and " + right.getClass());
    }

    private int applySortDirection(int c, boolean ascending) {
        return c != 0 ? (ascending ? c : -c) : 0;
    }

    private ThreeValuedLogic evaluateNestedCondition(boolean notFlag, boolean value) {
        return (notFlag ? !value : value) ? TRUE : FALSE;
    }

    private int evaluateNestedCondition(boolean isEqual, int equalValue, boolean isNull, int nullValue, int nonNullValue) {
        return isEqual ? equalValue : (isNull ? nullValue : nonNullValue);
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte;
    }

    /**
     * Coerces a {@link BigDecimal} aggregate result to the column's declared
     * Java type so the returned value matches what the table stores.
     */
    private static Object coerceNumericResult(BigDecimal value, Class<?> columnType) {
        if (columnType == null) return value;
        if (columnType == Float.class) return value.floatValue();
        if (columnType == Double.class) return value.doubleValue();
        if (columnType == Integer.class) return value.intValue();
        if (columnType == Long.class) return value.longValue();
        if (columnType == Short.class) return value.shortValue();
        if (columnType == Byte.class) return value.byteValue();
        return value;
    }

    private boolean likeComparison(Object value, Object pattern) {
        if (value == null || pattern == null) {
            return false;
        }
        String valueStr = value.toString();
        String patternStr = pattern.toString();
        Pattern compiled = likePatternCache.get(patternStr);
        if (compiled == null) {
            compiled = Pattern.compile(patternStr.replaceAll("%+", "%").replace("%", ".*").replace("_", "."));
            likePatternCache.put(patternStr, compiled);
        }
        return compiled.matcher(valueStr).matches();
    }

    private List<ColumnProjection> buildProjectionPlan() {
        List<ColumnProjection> plan = new ArrayList<>(columns.size());
        columns.forEach(column -> {
            String trimmed = column.trim();
            if (trimmed.equals("*")) {
                plan.add(new ColumnProjection(null, null, Collections.emptyList()));
            } else {
                String normalizedColumn = normalizeColumnName(column, mainTableName);
                String columnAlias = normalizeColumnKey(column, mainTableName);
                String[] parts = trimmed.split("\\s+AS\\s+|\\s+", 2);
                if (parts.length > 1) {
                    columnAlias = parts[1].trim();
                    if (!CharOps.isAsciiIdentifier(columnAlias)) {
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
        });
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
            if (proj.normalized != null && row.containsKey(proj.normalized)) {
                filtered.put(proj.alias, row.get(proj.normalized));
            } else if (proj.normalized == null) {
                extractAllColumns(row, filtered);
            } else {
                Object value = findFallbackValue(row, proj.fallbackKeys);
                if (value != null) {
                    filtered.put(proj.alias, value);
                }
            }
        }
        return filtered;
    }

    private void extractAllColumns(Map<String, Object> row, Map<String, Object> filtered) {
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String key = entry.getKey();
            String unqualifiedKey = key.contains(".") ? key.split("\\.", 2)[1].trim() : key.trim();
            filtered.put(unqualifiedKey, entry.getValue());
        }
    }

    private Object findFallbackValue(Map<String, Object> row, List<String> fallbackKeys) {
        for (String candidate : fallbackKeys) {
            if (row.containsKey(candidate)) {
                return row.get(candidate);
            }
        }
        return null;
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
        // Prompt 22 (java:S2259): the database argument must be non-null.
        Objects.requireNonNull(database, "Subquery needs a database");
        String s = subQueryString.trim();
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1).trim();
        }
        s = substituteOuterReferences(s, outerRow);
        Object result = database.executeQuery(s, transactionId);
        if (!(result instanceof List<?> rows)) {
            throw new IllegalStateException("GROUP BY subquery must return a list of rows");
        }
        if (rows.isEmpty()) {
            return null;
        }
        Object firstRow = rows.get(0);
        if (!(firstRow instanceof Map<?, ?> map) || map.isEmpty()) {
            return null;
        }
        return map.values().iterator().next();
    }

    private String substituteOuterReferences(String query, Map<String, Object> outerRow) {
        Matcher matcher = Pattern.compile("(?i)\\b([A-Za-z_]\\w*)\\.([A-Za-z_]\\w*)\\b").matcher(query);
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
            return SqlKeywords.NULL;
        }
        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
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
     * Builds a textual execution-plan tree for EXPLAIN. Analysis-only: never
     * executes the query, so no table rows are read or mutated. It reports the
     * table scans, the join algorithm actually chosen at runtime (In-Memory
     * Hash Join / Partitioned Hash Join / Nested Loop), the estimated rows,
     * the indexes the WHERE/ON conditions would use, and the remaining clauses
     * (filter, group by, having, order by, limit/offset).
     *
     * @param mainTable the resolved main table (real or derived)
     * @return the multi-line plan text
     */
    private List<QueryParser.JoinInfo> orderPlanJoins(Database database) {
        List<QueryParser.JoinInfo> ordered = new ArrayList<>(joins);
        if (ordered.size() > 1) {
            boolean allInner = ordered.stream().allMatch(j -> j.joinType == QueryParser.JoinType.INNER
                    || j.joinType == QueryParser.JoinType.LEFT_INNER
                    || j.joinType == QueryParser.JoinType.RIGHT_INNER);
            if (allInner) {
                ordered.sort(Comparator.comparingInt(j -> database.getTable(j.tableName).rowCount()));
            }
        }
        return ordered;
    }

    private void appendPlanJoin(StringBuilder sb, QueryParser.JoinInfo join, Table joinTable,
            String scanName, Table mainTable) {
        sb.append("  Join ").append(join.joinType).append('\n');
        sb.append("    tables: ").append(scanName);
        if (join.alias != null) {
            sb.append(" AS ").append(join.alias);
        }
        sb.append(" <-> ").append(join.tableName).append('\n');
        sb.append("    estimated rows: ").append(joinTable != null ? joinTable.rowCount() : 0).append('\n');
        sb.append("    algorithm: ").append(describeJoinAlgorithm(join, joinTable, mainTable)).append('\n');
        QueryParser.Condition equality = findHashEquality(join);
        if (equality != null) {
            sb.append("    keys: ").append(normalizeColumnName(equality.column, mainTableName))
                    .append(" = ").append(normalizeColumnName(equality.rightColumn, mainTableName)).append('\n');
        }
        if (join.onConditions != null && !join.onConditions.isEmpty()) {
            sb.append("    on: ").append(join.onConditions.stream()
                    .map(QueryParser.Condition::toString)
                    .collect(Collectors.joining(" "))).append('\n');
        }
    }

    private void appendPlanJoins(StringBuilder sb, List<QueryParser.JoinInfo> joinsForDisplay,
            String scanName, Table mainTable) {
        if (joinsForDisplay.isEmpty()) {
            return;
        }
        for (QueryParser.JoinInfo join : joinsForDisplay) {
            Table joinTable = mainTable.getDatabase().getTable(join.tableName);
            appendPlanJoin(sb, join, joinTable, scanName, mainTable);
        }
    }

    private void appendPlanClauses(StringBuilder sb) {
        if (!conditions.isEmpty()) {
            sb.append("  Filter (WHERE): ").append(conditions.stream()
                    .map(QueryParser.Condition::toString)
                    .collect(Collectors.joining(" "))).append('\n');
        }
        if (!groupBy.isEmpty()) {
            sb.append("  Group By: ").append(String.join(", ", groupBy)).append('\n');
        }
        if (!havingConditions.isEmpty()) {
            sb.append("  Having: ").append(havingConditions.stream()
                    .map(QueryParser.HavingCondition::toString)
                    .collect(Collectors.joining(" "))).append('\n');
        }
        if (!orderBy.isEmpty()) {
            sb.append("  Order By: ").append(orderBy.stream()
                    .map(QueryParser.OrderByInfo::toString)
                    .collect(Collectors.joining(", "))).append('\n');
        }
        if (limit != null || offset != null) {
            sb.append("  Limit: ").append(limit == null ? "none" : limit)
                    .append(", Offset: ").append(offset == null ? "none" : offset).append('\n');
        }
    }

    String describePlan(Table mainTable) {
        // Prompt 22 (java:S2259): the owning database is documented-nullable
        // and is dereferenced for every join table below.
        Objects.requireNonNull(mainTable, "Main table must not be null");
        Database database = Objects.requireNonNull(mainTable.getDatabase(),
                ErrorMessages.TABLE_PREFIX + mainTableName + ErrorMessages.NOT_ATTACHED_TO_DB);
        for (QueryParser.JoinInfo join : joins) {
            tableAliases.putIfAbsent(join.tableName, join.tableName);
            if (join.alias != null) {
                tableAliases.put(join.alias, join.tableName);
            }
        }

        String scanName = derivedMainTable != null ? mainTable.getName() : mainTableName;
        StringBuilder sb = new StringBuilder("Execution Plan\n");
        sb.append("  Operation: SELECT\n");
        sb.append("  Scan ").append(scanName).append(" (estimated rows: ").append(mainTable.rowCount()).append(")\n");
        sb.append(MessageConstants.SQL_INDEX_PREFIX).append(describeScanIndex(mainTable, mainTableName, conditions)).append('\n');

        appendPlanJoins(sb, orderPlanJoins(database), scanName, mainTable);
        appendPlanClauses(sb);
        return sb.toString();
    }

    /**
     * Returns the single equality condition a hash join would key on, or null
     * when the join cannot use a hash join.
     */
    private QueryParser.Condition findHashEquality(QueryParser.JoinInfo join) {
        if (!canUseHashJoin(join)) {
            return null;
        }
        return join.onConditions.stream()
                .filter(c -> c.operator == QueryParser.Operator.EQUALS && c.isColumnComparison())
                .findFirst()
                .orElse(null);
    }

    /**
     * Reports which join algorithm the engine would pick at runtime for this
     * join, mirroring the strategy branch of {@link #execute}: a hash join is
     * only possible for inner-style joins with a plain equality key and no OR
     * in the ON clause, and when the estimated hash table would exceed the
     * memory budget the plan shows the spill-to-disk partitioned variant.
     */
    private String describeJoinAlgorithm(QueryParser.JoinInfo join, Table joinTable, Table mainTable) {
        if (join.joinType == QueryParser.JoinType.CROSS) {
            return "Nested Loop (cross join)";
        }
        if (!canUseHashJoin(join)) {
            if (hasOrInOnConditions(join)) {
                return "Nested Loop (OR condition may produce a large result set)";
            }
            return "Nested Loop";
        }
        Table buildTable = joinTable != null && joinTable.rowCount() <= mainTable.rowCount() ? joinTable : mainTable;
        // Prompt 37 (java:S2259): buildTable could be null when both joinTable
        // and mainTable are null; dereferencing it below would NPE.
        if (buildTable == null) {
            throw new QueryParseException("Cannot determine build table for join strategy");
        }
        Table probeTable = buildTable == joinTable ? mainTable : joinTable;
        if (probeTable != null && preferNestedLoopByStatistics(buildTable, probeTable)) {
            return "Nested Loop (chosen by statistics)";
        }
        long estimatedRows = buildTable.rowCount();
        long estimatedBytes = estimateHashTableSizeBytes(buildTable.getLiveRows(), buildTable);
        if (estimatedRows > maxInMemoryRows || estimatedBytes > maxHashTableSizeBytes) {
            return "Partitioned Hash Join (spill to disk)";
        }
        return "In-Memory Hash Join";
    }

    /**
     * Reports which index (if any) the engine would use to pre-filter the main
     * table scan for the given WHERE conditions, mirroring {@link #getIndexedRows}.
     * When multiple AND-conditions have applicable indexes, all of them are
     * reported as intersected.
     */
    private String describeScanIndex(Table table, String tableName, List<QueryParser.Condition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        for (QueryParser.Condition condition : conditions) {
            if (Objects.equals(condition.conjunction, SqlKeywords.OR)) {
                return "none (OR conditions disable the index pre-filter)";
            }
        }
        List<String> hints = new ArrayList<>();
        for (QueryParser.Condition condition : conditions) {
            String hint = tryResolveIndexHint(condition, tableName, table);
            if (hint != null) {
                hints.add(hint);
            }
        }
        if (hints.isEmpty()) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        if (hints.size() == 1) {
            return hints.get(0);
        }
        return hints.get(0) + " (intersected with " + String.join(", ", hints.subList(1, hints.size())) + ")";
    }

    /**
     * Returns an EXPLAIN-friendly index hint string for the given condition,
     * or {@code null} when the condition should be skipped (grouped, column
     * comparison, negated, or no matching index).
     */
    private String tryResolveIndexHint(QueryParser.Condition condition, String tableName, Table table) {
        if (condition.isGrouped() || condition.isColumnComparison() || condition.not) {
            return null;
        }
        String unqualifiedColumn = normalizeColumnKey(normalizeColumnName(condition.column, tableName), tableName);
        Index index = table.getIndex(unqualifiedColumn);
        if (index == null && table.hasClusteredIndex() && unqualifiedColumn.equals(table.getClusteredIndexColumn())) {
            index = table.getClusteredIndex();
        }
        if (index == null) {
            return null;
        }
        String hint;
        if (condition.operator == QueryParser.Operator.EQUALS || condition.isInOperator()) {
            hint = indexTypeName(index) + MessageConstants.SQL_INDEX_ON + tableName + "." + unqualifiedColumn;
        } else if (index instanceof BTreeIndex) {
            hint = switch (condition.operator) {
                case LESS_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUALS ->
                    indexTypeName(index) + MessageConstants.SQL_INDEX_ON + tableName + "." + unqualifiedColumn + " (range)";
                default -> null;
            };
        } else {
            return null;
        }
        // Add covering info if applicable
        if (hint != null && index instanceof CoveringBTreeIndex coverIndex) {
            hint += " (covers " + String.join(", ", coverIndex.getCoverColumns()) + ")";
        }
        return hint;
    }

    /**
     * Human-readable name of an index implementation for EXPLAIN output.
     */
    static String indexTypeName(Index index) {
        if (index instanceof CompositeBTreeIndex) {
            return "Composite B-tree";
        }
        if (index instanceof CoveringBTreeIndex) {
            return "Covering B-tree";
        }
        if (index instanceof HashIndex) {
            return "Hash";
        }
        if (index instanceof BTreeIndex) {
            return "B-tree";
        }
        if (index instanceof UniqueIndex) {
            return "Unique";
        }
        if (index instanceof BTreeClusteredIndex) {
            return "Clustered";
        }
        return index.getClass().getSimpleName().replace("Index", "");
    }

    /**
     * Renders the query back to its SQL form.
     *
     * @return the SQL representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(SqlKeywords.SELECT + " ");
        List<String> selectItems = new ArrayList<>();
        selectItems.addAll(columns);
        selectItems.addAll(aggregates.stream().map(QueryParser.AggregateFunction::toString).toList());
        sb.append(String.join(", ", selectItems));
        sb.append(MessageConstants.SQL_FROM_SPACED).append(mainTableName);
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
            sb.append(MessageConstants.SQL_WHERE_SPACED);
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