package diesel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central database engine. Owns the shared table map, the active client
 * transactions and the auto-commit flag, parses incoming SQL and dispatches
 * each parsed query to its dedicated execution path.
 *
 * <p>The engine is the single entry point for both in-memory use and the
 * client/server mode: {@link #executeQuery} accepts any supported SQL string
 * together with the caller's transaction id, and every data mutation is
 * persisted to the configured data directory (CSV plus serialized table
 * files).
 *
 * <p>Example:
 * <pre>{@code
 * Database db = new Database("data");
 * db.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(users_seq 1 1), NAME STRING)", null);
 * db.executeQuery("INSERT INTO USERS (NAME) VALUES ('Alice')", null);
 * }</pre>
 *
 * @see Table
 * @see Transaction
 * @see QueryParser
 */
class Database {
    private static final Logger LOGGER = Logger.getLogger(Database.class.getName());
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final QueryCache queryCache = new QueryCache();
    private final Map<UUID, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_UNCOMMITTED;
    private boolean autoCommit = true;
    private String dataDir = ".";

    /**
     * Creates an empty in-memory database whose data files live in the
     * current working directory (".").
     */
    public Database() {
    }

    /**
     * Creates an in-memory database whose data files live in the given
     * directory, creating the directory when it does not exist yet.
     *
     * @param dataDir directory for the CSV and serialized table files, or
     *                null/empty to keep the current working directory
     */
    public Database(String dataDir) {
        if (dataDir != null && !dataDir.isBlank()) {
            this.dataDir = dataDir;
        }
        File dir = new File(this.dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /**
     * Returns the directory used for the CSV and serialized table files.
     *
     * @return the data directory, never null
     */
    public String getDataDir() {
        return dataDir;
    }

    /**
     * Returns this database's query-plan cache (Prompt 16), which serves
     * repeated SELECTs from the cached AST and tracks hit/miss metrics.
     *
     * @return the query cache, never null
     */
    QueryCache getQueryCache() {
        return queryCache;
    }

    /**
     * Changes the directory used for the CSV and serialized table files,
     * creating it when it does not exist yet.
     *
     * @param dataDir new data directory, or null/empty to keep the current one
     */
    public void setDataDir(String dataDir) {
        if (dataDir != null && !dataDir.isBlank()) {
            this.dataDir = dataDir;
        }
        File dir = new File(this.dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /**
     * Creates a table with the given schema and registers it in the shared
     * table map. When a primary key column is specified, a unique clustered
     * index is built over it automatically.
     *
     * @param tableName        the table name, must be unique
     * @param columns          the ordered list of column names
     * @param columnTypes      the column name to type mapping
     * @param primaryKeyColumn the primary key column, or null for none
     * @throws IllegalArgumentException if the table already exists or the
     *                                  primary key column is not part of the schema
     */
    public void createTable(String tableName, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn) {
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException(ErrorMessages.TABLE_PREFIX + tableName + " already exists");
        }
        Table newTable = new Table(this, tableName, columns, columnTypes, primaryKeyColumn, new HashMap<String, Sequence>());
        tables.put(tableName, newTable);
        queryCache.invalidateAll();
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, newTable);
            }
        }
        LOGGER.log(Level.INFO, "Created table {0} with primary key {1}", new Object[]{tableName, primaryKeyColumn});
    }

    /**
     * Parses and executes a query. {@code transactionId} is the caller's active
     * transaction session, or null when the caller is not in a transaction.
     *
     * <p>The result depends on the query type: SELECT yields a
     * {@code List<Map<String, Object>>} of rows, INSERT/UPDATE/DELETE yield
     * null, and transaction/DDL statements yield a String status message.
     * Errors are wrapped in a {@link RuntimeException} whose message is
     * prefixed with {@code Query execution failed: }.
     *
     * @param query         the SQL query to execute
     * @param transactionId the caller's transaction id, or null when not in a
     *                      transaction
     * @return the query result (row list, null, or a status String)
     * @throws RuntimeException when the query cannot be parsed or executed
     */
    public Object executeQuery(String query, UUID transactionId) {
        // Prompt 22 (java:S2259): a null query would NPE below on
        // cleanQuery.trim() before the try-block that formats execution
        // errors; reject it up front with a clear IllegalArgumentException.
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        LOGGER.log(Level.FINE, "Executing query: {0}", query);
        Long maxRowsHint = parseMaxRowsHint(query);
        String cleanQuery = stripMaxRowsHint(query);

        // Prompt 16: plain SELECTs are served from the query-plan cache when
        // the normalized (literal-free) structure and the actual literal
        // values match an entry from the current schema epoch; the MAX_ROWS
        // hint is excluded because it mutates the parsed AST per execution.
        QueryCache.NormalizedSql normalizedSelect = null;
        Query<?> parsedQuery = null;
        long parseNanos = 0;
        if (maxRowsHint == null) {
            String trimmed = cleanQuery.trim();
            if (QueryParser.toUpperCasePreservingQuotedIdentifiers(trimmed).startsWith(SqlKeywords.SELECT)) {
                try {
                    normalizedSelect = QueryCache.normalize(trimmed);
                    parsedQuery = queryCache.get(normalizedSelect, queryCache.currentEpoch());
                } catch (IllegalArgumentException e) {
                    normalizedSelect = null;
                }
            }
        }
        if (parsedQuery == null) {
            long parseStart = System.nanoTime();
            parsedQuery = parse(cleanQuery);
            parseNanos = System.nanoTime() - parseStart;
            // Derived tables materialize their inner SELECT at parse time, so
            // their plans are never reused (a later hit would scan stale rows).
            if (normalizedSelect != null && parsedQuery instanceof SelectQuery sq
                    && sq.getDerivedMainTable() == null) {
                queryCache.put(normalizedSelect, parsedQuery, parseNanos, queryCache.currentEpoch());
            }
        }
        applyMaxRowsHint(parsedQuery, maxRowsHint);
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;

        // Prompt 18: the execution phase is timed around the dispatch and the
        // phase breakdown is reported to the QueryProfiler (parse time is known
        // above; plan/sort time come from the SelectQuery that just executed).
        long execStart = System.nanoTime();
        try {
            Object result = dispatch(parsedQuery, cleanQuery, currentTransaction, transactionId);
            recordQueryProfiling(parsedQuery, cleanQuery, parseNanos, System.nanoTime() - execStart);
            return result;
        } catch (DieselException e) {
            recordQueryProfiling(parsedQuery, cleanQuery, parseNanos, System.nanoTime() - execStart);
            throw e;
        } catch (RuntimeException e) {
            recordQueryProfiling(parsedQuery, cleanQuery, parseNanos, System.nanoTime() - execStart);
            throw e;
        }
    }

    /**
     * Routes a parsed query to its execution path. Extracted from
     * {@link #executeQuery} so the execution phase can be timed as a unit for
     * the query profiler.
     */
    private Object dispatch(Query<?> parsedQuery, String cleanQuery, Transaction currentTransaction, UUID transactionId) {
        if (parsedQuery instanceof SetIsolationLevelQuery q) {
            return executeSetIsolationLevel(q);
        }
        if (parsedQuery instanceof SetAutoCommitQuery q) {
            return executeSetAutoCommit(q);
        }
        if (parsedQuery instanceof BeginTransactionQuery q) {
            return executeBeginTransaction(q, currentTransaction);
        }
        if (parsedQuery instanceof CommitTransactionQuery q) {
            return executeCommit(currentTransaction, transactionId);
        }
        if (parsedQuery instanceof RollbackTransactionQuery q) {
            return executeRollback(currentTransaction, transactionId);
        }
        if (parsedQuery instanceof CreateTableQuery q) {
            return executeCreateTable(q);
        }
        if (parsedQuery instanceof CreateCompositeIndexQuery q) {
            return executeCreateCompositeIndex(q, currentTransaction);
        }
        if (parsedQuery instanceof CreateCoveringIndexQuery q) {
            return executeCreateCoveringIndex(q, currentTransaction);
        }
        if (parsedQuery instanceof CreateIndexQueryBase q) {
            return executeCreateIndex(q, currentTransaction);
        }
        if (parsedQuery instanceof ExplainQuery q) {
            return executeExplain(q, currentTransaction);
        }
        if (parsedQuery instanceof AnalyzeTableQuery q) {
            return executeAnalyzeTable(q);
        }
        return executeDataQuery(parsedQuery, cleanQuery, currentTransaction);
    }

    /**
     * Reports a query's phase breakdown to the query profiler: parse time is
     * measured in {@link #executeQuery}, while the plan and sort phases come
     * from the {@link SelectQuery} instance that just executed (EXPLAIN is
     * unwrapped to its inner statement). The execute phase is the measured
     * execution wall time minus the plan and sort phases, clamped at zero so
     * a failed early dispatch cannot report negative time.
     */
    private void recordQueryProfiling(Query<?> parsedQuery, String sql, long parseNanos, long execTotalNanos) {
        Query<?> profiled = parsedQuery;
        if (profiled instanceof ExplainQuery eq) {
            profiled = eq.getInnerQuery();
        }
        long planNanos = 0;
        long sortNanos = 0;
        if (profiled instanceof SelectQuery sq) {
            planNanos = sq.getLastPlanNanos();
            sortNanos = sq.getLastSortNanos();
        }
        long executeNanos = Math.max(0, execTotalNanos - planNanos - sortNanos);
        QueryProfiler.getInstance().record(sql, parseNanos, planNanos, executeNanos, sortNanos);
    }

    private Query<?> parse(String query) {
        // EXPLAIN must be handled before the subquery check: SubqueryParser would
        // mistake the inner statement's (SELECT ...) for its own input, whereas
        // QueryParser.parseExplainQuery parses the inner statement with the full
        // subquery-aware pipeline.
        if (QueryParser.isExplainQuery(query)) {
            return new QueryParser().parse(query, this);
        }
        SubqueryParser subqueryParser = new SubqueryParser();
        return subqueryParser.containsSubquery(query)
                ? subqueryParser.parse(query, this)
                : new QueryParser().parse(query, this);
    }

    /** Matches the {@code /* MAX_ROWS=N *&#47;} result-limit override hint. */
    private static final java.util.regex.Pattern MAX_ROWS_HINT_PATTERN =
            java.util.regex.Pattern.compile("(?i)/\\*\\s*MAX_ROWS\\s*=\\s*(\\d+)\\s*\\*/");

    /**
     * Extracts the {@code /* MAX_ROWS=N *&#47;} hint value from a query string,
     * or returns null when the query carries no hint.
     */
    private static Long parseMaxRowsHint(String query) {
        if (query == null) {
            return null;
        }
        java.util.regex.Matcher matcher = MAX_ROWS_HINT_PATTERN.matcher(query);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        return null;
    }

    /**
     * Removes any {@code /* MAX_ROWS=N *&#47;} hint comments from a query string so
     * the parser never sees them.
     */
    private static String stripMaxRowsHint(String query) {
        if (query == null) {
            return query;
        }
        return MAX_ROWS_HINT_PATTERN.matcher(query).replaceAll("");
    }

    /**
     * Applies a parsed {@code /* MAX_ROWS=N *&#47;} hint to the query that executes
     * the SELECT (or, for EXPLAIN, to its inner SELECT).
     */
    private void applyMaxRowsHint(Query<?> parsedQuery, Long maxRowsHint) {
        if (maxRowsHint == null) {
            return;
        }
        if (parsedQuery instanceof SelectQuery sq) {
            sq.setMaxResultRows(maxRowsHint);
        } else if (parsedQuery instanceof ExplainQuery eq) {
            Query<?> inner = eq.getInnerQuery();
            if (inner instanceof SelectQuery isq) {
                isq.setMaxResultRows(maxRowsHint);
            }
        }
    }

    private Object executeSetIsolationLevel(SetIsolationLevelQuery isolationQuery) {
        defaultIsolationLevel = isolationQuery.getIsolationLevel();
        return "Isolation level set to " + defaultIsolationLevel;
    }

    private Object executeSetAutoCommit(SetAutoCommitQuery autoCommitQuery) {
        boolean autoCommit = autoCommitQuery.isAutoCommit();
        setAutoCommit(autoCommit);
        return "AUTOCOMMIT set to " + (autoCommit ? SqlKeywords.ON : "OFF");
    }

    private Object executeBeginTransaction(BeginTransactionQuery beginQuery, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            throw new TransactionException("Another transaction is already active for this client");
        }
        IsolationLevel isolationLevel = Objects.requireNonNullElse(beginQuery.getIsolationLevel(), defaultIsolationLevel);
        Transaction transaction = new Transaction(isolationLevel);
        UUID newTransactionId = transaction.getTransactionId();
        activeTransactions.put(newTransactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        setAutoCommit(false);
        return "Transaction started: " + newTransactionId;
    }

    private Object executeCommit(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to commit");
        }
        persistModifiedTables(currentTransaction.getModifiedTables(), true);
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return "Transaction committed";
    }

    private Object executeRollback(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to rollback");
        }
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return "Transaction rolled back";
    }

    private Object executeCreateTable(CreateTableQuery createQuery) {
        createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes(), createQuery.getPrimaryKeyColumn());
        Table table = getTable(createQuery.getTableName());
        for (Map.Entry<String, Sequence> entry : createQuery.getSequences().entrySet()) {
            table.getSequences().put(entry.getKey(), entry.getValue());
        }
        return "Table created successfully";
    }

    private Object executeCreateIndex(CreateIndexQueryBase indexQuery, Transaction currentTransaction) {
        Table table = getTable(indexQuery.getTableName());
        indexQuery.execute(table);
        queryCache.invalidateAll();
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(indexQuery.getTableName(), table);
        }
        return indexDescription(indexQuery) + " created successfully on "
                + indexQuery.getTableName() + "." + indexQuery.getColumnName();
    }

    private Object executeCreateCompositeIndex(CreateCompositeIndexQuery indexQuery, Transaction currentTransaction) {
        Table table = getTable(indexQuery.getTableName());
        indexQuery.execute(table);
        queryCache.invalidateAll();
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(indexQuery.getTableName(), table);
        }
        return "Composite index created successfully on " + indexQuery.getTableName() + "." + String.join("+", indexQuery.getColumnNames());
    }

    private Object executeCreateCoveringIndex(CreateCoveringIndexQuery indexQuery, Transaction currentTransaction) {
        Table table = getTable(indexQuery.getTableName());
        indexQuery.execute(table);
        queryCache.invalidateAll();
        if (currentTransaction != null && currentTransaction.isActive()) {
            currentTransaction.updateTable(indexQuery.getTableName(), table);
        }
        return "Covering index created successfully on " + indexQuery.getTableName() + "." + indexQuery.getIndexColumn();
    }

    private String indexDescription(CreateIndexQueryBase indexQuery) {
        if (indexQuery instanceof CreateIndexQuery) {
            return "B-tree index";
        }
        if (indexQuery instanceof CreateHashIndexQuery) {
            return "Hash index";
        }
        if (indexQuery instanceof CreateUniqueIndexQuery) {
            return "Unique index";
        }
        if (indexQuery instanceof CreateUniqueClusteredIndexQuery) {
            return "Unique clustered index";
        }
        throw new IllegalArgumentException("Unsupported index query type: " + indexQuery.getClass().getSimpleName());
    }

    /**
     * Executes an EXPLAIN statement: resolves the table the inner statement
     * operates on (a derived-table virtual table when the inner SELECT scans
     * one) and delegates to {@link ExplainQuery#execute}, which renders the
     * plan tree and, for EXPLAIN ANALYZE, runs the statement and appends the
     * actual metrics. ANALYZE runs the statement against the resolved table
     * directly, so the in-memory mutation is not persisted.
     */
    private Object executeExplain(ExplainQuery explainQuery, Transaction currentTransaction) {
        Query<?> inner = explainQuery.getInnerQuery();
        Table table;
        if (inner instanceof SelectQuery sq && sq.getDerivedMainTable() != null) {
            table = sq.getDerivedMainTable();
        } else {
            String tableName = extractTableName(explainQuery.getInnerSql());
            table = getTableForQuery(tableName, currentTransaction);
            if (table == null) {
                throw new TableNotFoundException(ErrorMessages.TABLE_PREFIX + tableName + ErrorMessages.DOES_NOT_EXIST);
            }
        }
        return explainQuery.execute(table);
    }

    /**
     * Executes an {@code ANALYZE TABLE} statement: forces a synchronous
     * recalculation of the table's statistics and returns the status message.
     *
     * @param analyzeQuery the parsed ANALYZE TABLE query
     * @return the status message describing the fresh statistics
     */
    private Object executeAnalyzeTable(AnalyzeTableQuery analyzeQuery) {
        Table table = getTable(analyzeQuery.getTableName());
        Object result = analyzeQuery.execute(table);
        queryCache.invalidateAll();
        return result;
    }

    /**
     * Executes queries that operate on table data (INSERT/UPDATE/DELETE/SELECT),
     * applying the transaction view, auto-commit DML and persistence rules.
     */
    private Object executeDataQuery(Query<?> parsedQuery, String query, Transaction currentTransaction) {
        String tableName = null;
        Table table;
        if (parsedQuery instanceof SelectQuery sq && sq.getDerivedMainTable() != null) {
            table = sq.getDerivedMainTable();
        } else {
            tableName = extractTableName(query);
            table = getTableForQuery(tableName, currentTransaction);
            if (table == null) {
                throw new TableNotFoundException(ErrorMessages.TABLE_PREFIX + tableName + ErrorMessages.DOES_NOT_EXIST);
            }
        }

        boolean isDml = parsedQuery instanceof InsertQuery
                || parsedQuery instanceof UpdateQuery
                || parsedQuery instanceof DeleteQuery;

        // Auto-commit DML runs in a short-lived implicit transaction and is persisted immediately.
        if (autoCommit && isDml && (currentTransaction == null || !currentTransaction.isActive())) {
            Transaction implicitTransaction = new Transaction(defaultIsolationLevel);
            try {
                Object implicitResult = parsedQuery.execute(table);
                implicitTransaction.registerModifiedTable(tableName, table);
                persistModifiedTables(implicitTransaction.getModifiedTables(), false);
                return implicitResult;
            } finally {
                implicitTransaction.setInactive();
            }
        }

        // DML inside an explicit transaction records a copy for the eventual COMMIT.
        if (isDml) {
            Object dmlResult = parsedQuery.execute(table);
            if (currentTransaction != null && currentTransaction.isActive()) {
                currentTransaction.updateTable(tableName, table);
            } else {
                table.saveToFile(tableName);
            }
            return dmlResult;
        }

        return parsedQuery.execute(table);
    }

    /**
     * Publishes transaction-modified tables back into the shared table map and
     * writes them to disk. A null value means the table was dropped in the
     * transaction. Auto-commit DML persists only the CSV, while an explicit
     * COMMIT also writes the serialized table file.
     */
    private void persistModifiedTables(Map<String, Table> modifiedTables, boolean writeSerialized) {
        for (Map.Entry<String, Table> entry : modifiedTables.entrySet()) {
            String tableName = entry.getKey();
            Table modifiedTable = entry.getValue();
            if (modifiedTable != null) {
                tables.put(tableName, modifiedTable);
                modifiedTable.saveToFile(tableName);
                if (writeSerialized) {
                    modifiedTable.saveToSerializedFile(tableName);
                }
            } else {
                tables.remove(tableName);
                deleteTableFiles(tableName);
            }
        }
    }

    /**
     * Returns the table a query operates on, honoring the caller's transaction:
     * an active transaction first sees its own modified copy, then the copies of
     * other READ_UNCOMMITTED transactions, and finally its BEGIN-time snapshot.
     */
    private Table getTableForQuery(String tableName, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            Table modifiedTable = currentTransaction.getModifiedTables().get(tableName);
            if (modifiedTable != null) {
                return modifiedTable;
            }
            if (currentTransaction.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                for (Transaction otherTransaction : activeTransactions.values()) {
                    if (otherTransaction != currentTransaction && otherTransaction.isActive()) {
                        Table otherModifiedTable = otherTransaction.getModifiedTables().get(tableName);
                        if (otherModifiedTable != null) {
                            return otherModifiedTable;
                        }
                    }
                }
            }
            return currentTransaction.getOriginalTables().get(tableName);
        }
        return tables.get(tableName);
    }

    /** Extracts the name of the table a query operates on from the normalized query text. */
    private String extractTableName(String query) {
        String normalized = QueryParser.toUpperCasePreservingQuotedIdentifiers(query.trim());
        if (normalized.startsWith(SqlKeywords.SELECT)) {
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid SELECT format");
            }
            // The first table appears before any INNER JOIN or WHERE clause and may carry an alias.
            return firstIdentifier(parts[1].split("(?i)(INNER JOIN|WHERE)\\s")[0].trim().split("\\s+")[0]);
        }
        if (normalized.startsWith(SqlKeywords.INSERT_INTO)) {
            String[] parts = normalized.split("(?i)INSERT INTO\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid INSERT format");
            }
            return firstIdentifier(parts[1].split("\\s+|\\(")[0]);
        }
        if (normalized.startsWith(SqlKeywords.UPDATE)) {
            String[] parts = normalized.split("(?i)UPDATE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid UPDATE format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        if (normalized.startsWith("DELETE FROM")) {
            String[] parts = normalized.split("(?i)FROM\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid DELETE format");
            }
            return firstIdentifier(parts[1].split("(?i)WHERE\\s*", 2)[0]);
        }
        if (normalized.startsWith(SqlKeywords.CREATE_TABLE)) {
            String[] parts = normalized.split("(?i)CREATE TABLE\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE TABLE format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        if (normalized.startsWith(SqlKeywords.CREATE_INDEX) || normalized.startsWith(SqlKeywords.CREATE_HASH_INDEX)
                || normalized.startsWith(SqlKeywords.CREATE_UNIQUE_INDEX) || normalized.startsWith(SqlKeywords.CREATE_UNIQUE_CLUSTERED_INDEX)) {
            String[] parts = normalized.split("(?i)ON\\s+", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE INDEX format");
            }
            return firstIdentifier(parts[1].split("\\s+")[0]);
        }
        throw new IllegalArgumentException("Cannot extract table name from query: unsupported query type");
    }

    private String firstIdentifier(String token) {
        String tableName = QueryParser.unquoteIdentifier(token.trim());
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("Cannot extract table name from query: table name missing");
        }
        return tableName;
    }

    /**
     * Returns the table registered under the given name.
     *
     * @param tableName the table name
     * @return the registered table
     * @throws IllegalArgumentException if no such table exists
     */
    public Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(ErrorMessages.TABLE_PREFIX + tableName + ErrorMessages.DOES_NOT_EXIST);
        }
        return table;
    }

    /**
     * Removes the table and its CSV and serialized files from disk. Active
     * transactions are notified so their snapshots no longer reference the
     * dropped table.
     *
     * @param tableName the table name
     * @throws IllegalArgumentException if no such table exists
     */
    public void dropTable(String tableName) {
        if (tables.remove(tableName) == null) {
            throw new TableNotFoundException(ErrorMessages.TABLE_PREFIX + tableName + ErrorMessages.DOES_NOT_EXIST);
        }
        queryCache.invalidateAll();
        deleteTableFiles(tableName);
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, null);
            }
        }
    }

    /**
     * Writes every registered table to disk as a serialized {@code .table}
     * file in the data directory.
     *
     * @see Table#saveToSerializedFile
     */
    public void saveTablesToDisk() {
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            entry.getValue().saveToSerializedFile(entry.getKey());
        }
        LOGGER.log(Level.INFO, "Saved {0} tables to disk", tables.size());
    }

    /**
     * Loads every {@code .table} file found in the data directory back into
     * the shared table map, skipping corrupt files with a warning.
     *
     * @see Table#loadFromFile
     */
    public void loadTablesFromDisk() {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(ErrorMessages.TABLE_EXTENSION));
        if (files == null) {
            return;
        }
        for (File file : files) {
            String tableName = file.getName().substring(0, file.getName().length() - ErrorMessages.TABLE_EXTENSION.length());
            Table table = Table.loadFromFile(this, tableName);
            if (table != null) {
                tables.put(tableName, table);
                LOGGER.log(Level.INFO, "Loaded table {0} from disk with {1} rows",
                        new Object[]{tableName, table.getLiveRowCount()});
            }
        }
        queryCache.invalidateAll();
    }

    private void deleteTableFiles(String tableName) {
        try {
            Files.deleteIfExists(Path.of(dataDir, tableName + ".csv"));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete CSV file for table {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
        try {
            Files.deleteIfExists(Path.of(dataDir, tableName + ErrorMessages.TABLE_EXTENSION));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete serialized file for table {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }

    /**
     * Returns whether the given transaction id refers to an active transaction.
     *
     * @param transactionId the transaction id to check
     * @return true when the transaction exists and is still active
     */
    public boolean isInTransaction(UUID transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        return transaction != null && transaction.isActive();
    }

    /**
     * Returns the current auto-commit flag. When it is true, INSERT/UPDATE/
     * DELETE statements outside a transaction are persisted immediately.
     *
     * @return the auto-commit flag
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * Sets the auto-commit flag.
     *
     * @param autoCommit true to persist DML immediately, false to require an
     *                   explicit transaction
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    /**
     * Starts a new transaction at the given isolation level, snapshotting every
     * registered table as the transaction's BEGIN-time view.
     *
     * @param isolationLevel the isolation level for the new transaction
     * @return the id of the started transaction
     * @see Transaction
     */
    public UUID beginTransaction(IsolationLevel isolationLevel) {
        Transaction transaction = new Transaction(isolationLevel);
        UUID transactionId = transaction.getTransactionId();
        activeTransactions.put(transactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        return transactionId;
    }
}
