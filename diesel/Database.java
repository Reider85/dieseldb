package diesel;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import diesel.recovery.CheckpointRecord;
    import java.util.concurrent.CompletableFuture;
    import java.util.concurrent.ConcurrentHashMap;
    import java.util.concurrent.ExecutionException;
    import diesel.AlterTableAddColumnQuery;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import diesel.concurrency.Savepoint;
import diesel.storage.page.CatalogTable;
import diesel.storage.page.CatalogSchema;
import diesel.concurrency.ConflictDetector;
import diesel.concurrency.NestedSavepointStack;
import diesel.concurrency.Savepoint;
import diesel.wal.WALEntry;
import diesel.wal.WALConfig;
import diesel.wal.WALWriter;
import diesel.wal.WALOpcode;
import diesel.wal.AsyncWALWriter;
import diesel.wal.CommitPayload;
import diesel.wal.GroupCommitCoordinator;
import diesel.wal.FsyncPolicy;
import diesel.wal.SavepointPayload;
import diesel.security.AccessDeniedException;
import diesel.security.AuditBackend;
import diesel.security.AuditLogger;
import diesel.security.AuditRecord;
import diesel.security.Authorizer;
import diesel.security.PasswordHasher;
import diesel.security.Privilege;
import diesel.security.Session;
import diesel.security.Tenant;
import diesel.security.TenantStore;
import diesel.security.TenantStoreBackend;
import diesel.security.UserStore;
import diesel.security.UserStoreBackend;

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
    private final TxStatusTracker txStatusTracker = new TxStatusTracker();        // MVCC: tracks transaction status and commit CSNs
    private final VacuumManager vacuumManager = new VacuumManager(this);           // MVCC: dead row version reclamation
    private final ConflictDetector conflictDetector = new ConflictDetector();     // MVCC: SERIALIZABLE SSI conflict detection (prompt4 #5)
    private CatalogTable catalog;                                               // System catalog for table schemas
    private diesel.storage.page.PageManager pageManager;                             // Page manager for catalog storage
    private IsolationLevel defaultIsolationLevel = IsolationLevel.READ_UNCOMMITTED;
    private boolean autoCommit = true;
    private String dataDir = "data";
    
    // WAL subsystem (prompt4.md #15 - group commit coordinator)
    private volatile WALConfig walConfig;
    private volatile WALWriter walWriter;
    private volatile AsyncWALWriter asyncWALWriter;
    private volatile GroupCommitCoordinator groupCoordinator;
    private volatile RecoveryManager recoveryManager;                             // ARIES startup recovery (prompt4 #19)
    private volatile diesel.storage.page.BufferPoolFlusher bufferPoolFlusher;   // Background page flusher (prompt4 #20)
    private volatile diesel.storage.page.CheckpointManager checkpointManager;  // Background checkpoint manager (prompt4 #21)
    private volatile UserStore userStore;                                      // RBAC account store (prompt4 #30), lazy
    private volatile Authorizer authorizer;                                     // Privilege checker (prompt4 #32), lazy
    private volatile AuditLogger auditLogger;                                   // SQL audit logger (prompt4 #33), lazy
    private volatile TenantStore tenantStore;                                   // SNI tenant store (prompt4 #36), lazy
    private volatile boolean auditInitDone;                                     // audit config resolved at most once
    // Online ALTER TABLE ... ALGORITHM=COPY (prompt4 #38): running copies by
    // table name + bounded history of finished ones for ALTER STATUS.
    private final Map<String, AlterTableCopyAlgorithm> activeAlters = new ConcurrentHashMap<>();
    private final java.util.ArrayDeque<Map<String, Object>> alterHistory = new java.util.ArrayDeque<>();
    private static final int ALTER_HISTORY_LIMIT = 32;
    private volatile java.util.concurrent.ExecutorService alterCopyExecutor;     // single daemon thread for background copies
    /** True while an audited top-level statement runs on this thread; subquery/batch re-entries skip their own audit record. */
    private static final ThreadLocal<Boolean> AUDIT_INNER = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final ScheduledExecutorService walScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "diesel-wal-scheduler");
        t.setDaemon(true);
        return t;
    });

    /**
     * Creates an empty in-memory database whose data files live in the
     * "data" subdirectory.
     */
    public Database() {
        this.catalog = new CatalogTable(null);
        try {
            createPageManager();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to create PageManager: " + e.getMessage(), e);
        }
    }

    /**
     * Creates an in-memory database whose data files live in the given
     * directory, creating the directory when it does not exist yet.
     *
     * @param dataDir directory for the CSV and serialized table files, or
     *                null/empty to keep the current working directory
     */
    public Database(String dataDir) {
        this(dataDir, (WALConfig) null);
    }

    /**
     * Creates a database with an explicit WAL configuration (recovery tests,
     * prompt4 #19). Passing a config with {@code enabled=true} turns the WAL
     * subsystem on without touching global system properties, so the test
     * stays safe under parallel class execution.
     *
     * @param dataDir          directory for the table files
     * @param walConfigOverride explicit WAL configuration, or null to resolve
     *                          wal.* from system properties/config.properties
     */
    Database(String dataDir, WALConfig walConfigOverride) {
        if (dataDir != null && !dataDir.isBlank()) {
            this.dataDir = dataDir;
        }
        File dir = new File(this.dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        Table.flushPendingPersistsForDataDir(this.dataDir);
        // Initialize catalog with null page manager (will be set later)
        this.catalog = new CatalogTable(null);
        try {
            createPageManager();
            initializeWAL(walConfigOverride);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to create PageManager: " + e.getMessage(), e);
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
     * Returns this database's catalog for table schemas.
     *
     * @return the catalog, never null
     */
    CatalogTable getCatalog() {
        return catalog;
    }

    /**
     * Initializes the WAL subsystem if enabled.
     *
     * @param override explicit configuration (recovery tests), or null to
     *                 resolve wal.* from system properties/config.properties
     * @throws IOException if initialization fails
     */
    private void initializeWAL(WALConfig override) throws IOException {
        // Resolve WAL config: explicit override (tests) > system properties > config.properties
        this.walConfig = override != null ? override : WALConfig.fromConfig();
        
        if (!walConfig.isEnabled()) {
            LOGGER.info("WAL subsystem disabled (wal.enabled=false)");
            return;
        }
        
        try {
            // Initialize WAL writer with async wrapper and group coordinator
            this.walWriter = WALWriter.open(walConfig);
            this.asyncWALWriter = new AsyncWALWriter(walWriter, walConfig.getFsyncPolicy(),
                walConfig.getGroupWindowMs(), walConfig.getGroupMaxSize(), walScheduler);
            this.groupCoordinator = asyncWALWriter.getCoordinator();
            
            // Load checkpoint record for ARIES recovery
            try {
                CheckpointRecord checkpointRecord = walWriter.getManager().loadCheckpointRecord();
                if (checkpointRecord != null) {
                    LOGGER.info(String.format("Loaded checkpoint from LSN %d with %d active txids at %s",
                        checkpointRecord.getLastLSN(), checkpointRecord.getActiveTxidCount(), 
                        new Date(checkpointRecord.getTimestampEpochMs())));
                } else {
                    LOGGER.info("No checkpoint found, starting with clean state");
                }
            } catch (Exception e) {
                LOGGER.warning("Failed to load checkpoint: " + e.getMessage());
                // Continue without checkpoint - will recover from WAL entries
            }
            
            // ARIES recovery orchestrator (prompt4 #19): applied by
            // DatabaseServer.start() via runRecovery() before clients connect.
            this.recoveryManager = new RecoveryManager(this);
            
            LOGGER.info("WAL subsystem enabled with policy=" + walConfig.getFsyncPolicy() + 
                ", groupWindow=" + walConfig.getGroupWindowMs() + "ms, groupSize=" + walConfig.getGroupMaxSize());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize WAL subsystem: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Returns whether WAL is enabled for this database.
     *
     * @return true if WAL is enabled, false otherwise
     */
    public boolean isWALEnabled() {
        return walConfig != null && walConfig.isEnabled();
    }

    /**
     * Returns the async WAL writer, or null if WAL is disabled.
     *
     * @return the async WAL writer, or null
     */
    public AsyncWALWriter getAsyncWALWriter() {
        return asyncWALWriter;
    }

    /**
     * Returns the group commit coordinator, or null if WAL is disabled.
     *
     * @return the group commit coordinator, or null
     */
    public GroupCommitCoordinator getGroupCoordinator() {
        return groupCoordinator;
    }

    /**
     * Returns the WAL manager behind the writer, or null if WAL is disabled.
     * Used by recovery and by crash-simulation test hooks to flush segments.
     *
     * @return the WAL manager, or null
     */
    public diesel.wal.WALManager getWalManager() {
        WALWriter writer = walWriter;
        return writer != null ? writer.getManager() : null;
    }

    /**
     * Returns the page manager of this database, or null when it could not
     * be created.
     *
     * @return the page manager, or null
     */
    public diesel.storage.page.PageManager getPageManager() {
        return pageManager;
    }

    /**
     * Returns the ARIES recovery manager (prompt4 #19), or null when the WAL
     * is disabled (recovery is skipped entirely).
     *
     * @return the recovery manager, or null
     */
    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    /**
     * Runs ARIES startup recovery (analysis → redo → undo) when the WAL is
     * enabled; a no-op otherwise. Invoked by {@code DatabaseServer.start()}
     * after tables are loaded and strictly before client connections are
     * accepted.
     */
    public void runRecovery() {
        if (!isWALEnabled() || recoveryManager == null) {
            LOGGER.fine("WAL disabled - skipping ARIES startup recovery");
            return;
        }
        try {
            recoveryManager.recover();
        } catch (IOException e) {
            throw new IllegalStateException("ARIES startup recovery failed: " + e.getMessage(), e);
        }
    }

    /**
     * Starts the background page flusher if enabled and not already running.
     * The flusher uses the WAL manager's last flushed LSN to implement the
     * WAL-before-page rule. If WAL is disabled, uses Long.MAX_VALUE as
     * watermark (flush all pages).
     */
    public void startPageFlusher() {
        if (bufferPoolFlusher != null && bufferPoolFlusher.isRunning()) {
            LOGGER.fine("BufferPoolFlusher already running");
            return;
        }

        if (pageManager == null) {
            LOGGER.warning("Cannot start page flusher: PageManager not initialized");
            return;
        }

        // Determine WAL watermark
        LongSupplier walLsnSupplier = () -> {
            if (isWALEnabled() && walWriter != null) {
                return walWriter.getManager().getLastFlushedLsn();
            }
            return Long.MAX_VALUE; // No WAL: flush all pages
        };

        // Create and start flusher
        try {
            Properties props = ConfigLoader.load();
            int flushInterval = Integer.parseInt(
                props.getProperty("bufferpool.flush.interval.ms", "200"));
            boolean enabled = Boolean.parseBoolean(
                props.getProperty("bufferpool.flusher.enabled", "true"));

            if (enabled) {
                bufferPoolFlusher = new diesel.storage.page.BufferPoolFlusher(
                    pageManager.getBufferPool(), walLsnSupplier, flushInterval);
                bufferPoolFlusher.start();
                LOGGER.info("BufferPoolFlusher started with interval " + flushInterval + "ms");
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start BufferPoolFlusher: " + e.getMessage(), e);
        }
    }

    /**
     * Stops the background page flusher after a final flush.
     * Idempotent: does nothing if not running.
     */
    public void stopPageFlusher() {
        if (bufferPoolFlusher != null) {
            try {
                bufferPoolFlusher.stop();
                LOGGER.info("BufferPoolFlusher stopped with final flush");
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "BufferPoolFlusher final flush failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Stops the background page flusher without doing a final flush.
     * Useful for crash simulation: dirty pages are left dirty as expected.
     * Idempotent: does nothing if not running.
     */
    public void stopPageFlusherWithoutFinalFlush() {
        if (bufferPoolFlusher != null) {
            bufferPoolFlusher.stopWithoutFlush();
            LOGGER.info("BufferPoolFlusher stopped without final flush");
        }
    }

    /**
     * Starts the background checkpoint manager if enabled and WAL is enabled.
     * Reads configuration and creates the checkpoint manager with the given
     * dependencies. Idempotent: does nothing if already running.
     */
    public void startCheckpointManager() {
        if (checkpointManager != null && checkpointManager.isRunning()) {
            LOGGER.fine("CheckpointManager already running");
            return;
        }

        if (pageManager == null) {
            LOGGER.warning("Cannot start checkpoint manager: PageManager not initialized");
            return;
        }

        if (!isWALEnabled()) {
            LOGGER.info("CheckpointManager not started: WAL is disabled");
            return;
        }

        try {
            Properties props = ConfigLoader.load();
            boolean enabled = Boolean.parseBoolean(
                props.getProperty("checkpoint.enabled", "true"));
            long intervalMs = Long.parseLong(
                props.getProperty("checkpoint.interval.ms", "300000"));
            long maxWalBytes = Long.parseLong(
                props.getProperty("checkpoint.max.wal.bytes", "268435456"));

            if (enabled) {
                // Create checkpoint manager
                checkpointManager = new diesel.storage.page.CheckpointManager(
                    walWriter.getManager(),
                    bufferPoolFlusher,
                    pageManager.getBufferPool(),
                    txStatusTracker,
                    intervalMs,
                    maxWalBytes,
                    enabled);

                // Set write gate in page manager
                pageManager.setWriteGate(checkpointManager.getWriteGate());

                // Start the checkpoint manager
                checkpointManager.start();
                LOGGER.info("CheckpointManager started with interval " + intervalMs + "ms, maxWalBytes " + maxWalBytes);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start CheckpointManager: " + e.getMessage(), e);
        }
    }

    /**
     * Stops the background checkpoint manager after a final checkpoint.
     * Idempotent: does nothing if not running.
     *
     * @throws IOException if the final checkpoint fails
     */
    public void stopCheckpointManager() throws IOException {
        if (checkpointManager != null) {
            try {
                checkpointManager.stop();
                LOGGER.info("CheckpointManager stopped with final checkpoint");
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "CheckpointManager final checkpoint failed: " + e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * Stops the background checkpoint manager without doing a final checkpoint.
     * Useful for crash simulation: checkpoint state is left incomplete.
     * Idempotent: does nothing if not running.
     */
    public void stopCheckpointManagerWithoutFinalCheckpoint() {
        if (checkpointManager != null) {
            checkpointManager.stopWithoutCheckpoint();
            LOGGER.info("CheckpointManager stopped without final checkpoint");
        }
    }

    /**
     * Returns the checkpoint manager for testing or advanced operations.
     *
     * @return the checkpoint manager, or null if not created
     */
    public diesel.storage.page.CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    /**
     * Appends a logical DML record (INSERT/UPDATE/DELETE) to the WAL when
     * enabled (prompt4 #19). Non-blocking enqueue into the writer's FIFO
     * queue: successive puts from one thread are ordered, so a
     * transaction's records keep their program order in the log (the undo
     * phase replays them in reverse LSN order). The statement does not wait
     * for the write; COMMIT still blocks for durability and thereby waits
     * behind every queued DML record of its transaction.
     *
     * @param txid   the writing transaction id
     * @param op     INSERT, UPDATE or DELETE
     * @param before the before-image payload, or null
     * @param after  the after-image payload, or null
     */
    void appendWalDml(long txid, WALOpcode op, byte[] before, byte[] after) {
        WALWriter writer = walWriter;
        if (writer == null || walConfig == null || !walConfig.isEnabled() || txid <= 0) {
            return;
        }
        try {
            writer.appendAsync(txid, op, before, after);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "WAL DML append failed (statement continues): " + e.getMessage());
        }
    }

    /**
     * Creates a new PageManager for the database.
     *
     * @return the created PageManager
     * @throws IOException if the PageManager cannot be created
     */
    public diesel.storage.page.PageManager createPageManager() throws IOException {
        Path pageFile = Path.of(dataDir, "pages.db");
        this.pageManager = diesel.storage.page.PageManager.open(pageFile, 100);
        return pageManager;
    }

    /**
     * Sets the page manager for the catalog and loads the catalog from disk.
     * This should be called during database initialization.
     *
     * @param pageManager the page manager to use for catalog storage
     */
    public void setCatalogPageManager(diesel.storage.page.PageManager pageManager) {
        // Reinitialize catalog with page manager
        this.catalog = new CatalogTable(pageManager);
        this.catalog.loadCatalog();
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
        
        // Create table
        Table newTable = new Table(this, tableName, columns, columnTypes, primaryKeyColumn, new HashMap<String, Sequence>());
        tables.put(tableName, newTable);
        queryCache.invalidateAll();
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive()) {
                transaction.updateTable(tableName, newTable);
            }
        }
        
        // Update catalog
        try {
            List<CatalogSchema.ColumnSchema> columnSchemas = new ArrayList<>();
            for (String column : columns) {
                Class<?> type = columnTypes.get(column);
                columnSchemas.add(new CatalogSchema.ColumnSchema(
                    column, 
                    type.getSimpleName(), 
                    true, // nullable by default
                    column.equals(primaryKeyColumn) // unique if primary key
                ));
            }
            
            List<String> sequences = new ArrayList<>();
            for (Map.Entry<String, Sequence> entry : newTable.getSequences().entrySet()) {
                sequences.add(entry.getKey());
            }
            
            List<CatalogSchema.IndexSchema> indexSchemas = new ArrayList<>();
            // TODO: Add index information when we have index metadata
            
            catalog.createTableSchema(
                tableName, 
                columnSchemas, 
                primaryKeyColumn, 
                newTable.getStorageType(), 
                sequences, 
                indexSchemas
            );
        } catch (IllegalStateException e) {
            LOGGER.log(Level.WARNING, "Failed to update catalog for table " + tableName, e);
        }
        
        LOGGER.log(Level.INFO, "Created table {0} with primary key {1}", new Object[]{tableName, primaryKeyColumn});
    }

    /**
     * Applies a schema change to a table (internal API for ALTER TABLE ADD COLUMN).
     * 
     * @param tableName Name of the table to modify
     * @param columns Updated column list (must be current + exactly one new column)
     * @param types Updated column type mapping (must be current + exactly one new type)
     * @param description Description of the change
     * @throws diesel.TableNotFoundException if the table doesn't exist
     * @throws IllegalArgumentException if validation fails
     */
    public void applySchemaChange(String tableName, List<String> columns, Map<String, Class<?>> types, String description) {
        // getTable throws TableNotFoundException for unknown tables (engine-wide convention)
        Table table = getTable(tableName);
        
        // Apply schema change under write lock
        table.getTableLock().writeLock().lock();
        try {
            table.applySchemaChange(columns, types, description);
            
            // Update catalog
            List<CatalogSchema.ColumnSchema> columnSchemas = new ArrayList<>();
            for (Map.Entry<String, Class<?>> entry : types.entrySet()) {
                columnSchemas.add(new CatalogSchema.ColumnSchema(
                    entry.getKey(),
                    entry.getValue().getSimpleName(),
                    true, // nullable by default for ADD COLUMN
                    entry.getKey().equals(table.getPrimaryKeyColumn()) // unique if primary key
                ));
            }
            
            CatalogSchema catalogSchema = new CatalogSchema(
                tableName,
                columnSchemas,
                table.getPrimaryKeyColumn(),
                table.getStorageType(),
                new ArrayList<>(table.getSequences().keySet()),
                new ArrayList<>() // indices not supported in step 1
            );
            catalogSchema.setSchemaVersion(table.getSchemaVersions().currentVersion());
            // Update catalog with the new schema version (full CatalogSchema overload
            // so the version is preserved — the field-by-field overload resets it)
            getCatalog().updateTableSchema(catalogSchema);
            
            // Invalidate query cache (DDL contract)
            queryCache.invalidateAll();
        } finally {
            table.getTableLock().writeLock().unlock();
        }
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
        return audited(AuditRecord.USER_EMBEDDED, AuditRecord.USER_EMBEDDED, query,
                () -> executeQueryInternal(query, transactionId));
    }

    /**
     * Executes a fully parsed SQL string (prompt4 #33): the audit-free core
     * shared by both public {@link #executeQuery} overloads so a session
     * query is recorded exactly once with its session identity.
     */
    Object executeQueryInternal(String query, UUID transactionId) {
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
            // Prompt 38 (java:S2259): toUpperCasePreservingQuotedIdentifiers can
            // return null; wrap in Optional to avoid NPE.
            if (Optional.ofNullable(QueryParser.toUpperCasePreservingQuotedIdentifiers(trimmed)).orElse("").startsWith(SqlKeywords.SELECT)) {
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
     * Executes a query with privilege checking (prompt4 #32). This overload
     * is used by the server when a session is available; embedded usage
     * (without session) bypasses all privilege checks.
     *
     * @param query         the SQL to execute
     * @param transactionId the caller's transaction id, or null
     * @param session       the authenticated session (for privilege checking), or null for embedded
     * @return the query result (row list, null, or a status String)
     * @throws RuntimeException when the query cannot be parsed or executed
     */
    public Object executeQuery(String query, UUID transactionId, Session session) {
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        String user = session != null ? session.getUserName() : AuditRecord.USER_EMBEDDED;
        String ip = session != null ? session.getSourceIp() : AuditRecord.USER_EMBEDDED;
        // The authorize() call sits inside the audited body (prompt4 #33) so
        // AccessDeniedException rejections are recorded as ERROR audit events
        // — denied attempts are the most security-relevant records.
        Object result = audited(user, ip, query, () -> {
            if (session != null) {
                authorize(session, query);
            }
            return executeQueryInternal(query, transactionId);
        });

        // A successful own-account password change clears the must-change flag
        // on the live session (prompt4 #32): the flag inside the password hash
        // was already rewritten by UserStore.updatePassword, but Session keeps
        // its own volatile copy that must follow suit.
        if (session != null && session.isMustChangePassword() && AlterUserQuery.isAlterUser(query)) {
            String target = AlterUserQuery.targetUserName(query);
            if (target != null && target.equalsIgnoreCase(session.getUserName())) {
                session.setMustChangePassword(false);
            }
        }
        return result;
    }

    /**
     * Records one SQL operation in the audit log (prompt4 #33) around the
     * given body. When auditing is off (the default) the body runs with no
     * added work beyond a null check; nested re-entries on the same thread
     * (subquery evaluation, batch items) inherit the outer statement's single
     * audit record instead of producing their own.
     *
     * @param userName the acting user, or {@code "embedded"} without a session
     * @param sourceIp the client address, or {@code "embedded"}
     * @param query    the SQL being executed (already null-checked by callers)
     * @param body     the execution to audit
     * @return the body's result
     */
    private <T> T audited(String userName, String sourceIp, String query, Supplier<T> body) {
        AuditLogger audit = getAuditLogger();
        if (audit == null || AUDIT_INNER.get()) {
            return body.get();
        }
        AUDIT_INNER.set(Boolean.TRUE);
        long start = System.nanoTime();
        try {
            T result = body.get();
            audit.record(userName, sourceIp, query, auditStatus(result),
                    (System.nanoTime() - start) / 1_000_000L);
            return result;
        } catch (RuntimeException e) {
            audit.record(userName, sourceIp, query, AuditRecord.STATUS_ERROR,
                    (System.nanoTime() - start) / 1_000_000L);
            throw e;
        } finally {
            AUDIT_INNER.set(Boolean.FALSE);
        }
    }

    /** ERROR when the result carries the engine's error prefix, else OK. */
    private static String auditStatus(Object result) {
        if (result instanceof String s && s.startsWith(ErrorMessages.ERROR_PREFIX)) {
            return AuditRecord.STATUS_ERROR;
        }
        return AuditRecord.STATUS_OK;
    }

    /**
     * Authorizes the given query for the session (prompt4 #32).
     *
     * <p>Order of checks: the must-change password hard block fires before
     * parsing (every statement except {@code ALTER USER} is rejected), then
     * account-targeted and meta operations are resolved, and finally the
     * table-level privileges are checked for each table the statement touches.
     */
    private void authorize(Session session, String query) {
        // Hard must-change password block (prompt4 #32): everything except
        // ALTER USER is rejected before parsing.
        if (session.isMustChangePassword() && !AlterUserQuery.isAlterUser(query)) {
            throw new AccessDeniedException(
                "Password change required: user '" + session.getUserName() + "' must change password first");
        }

        Query<?> parsedQuery = parse(query);
        String cleanQuery = stripMaxRowsHint(query);

        // ALTER USER: self-service is always allowed, cross-account changes
        // require the database-wide ADMIN privilege (prompt4 #32). With the
        // must-change flag still set, the check inside denies cross-account.
        if (parsedQuery instanceof AlterUserQuery aq) {
            if (aq.getUserName() != null && aq.getUserName().equalsIgnoreCase(session.getUserName())) {
                return;
            }
            getAuthorizer().check(session, Privilege.Action.ADMIN, "");
            return;
        }

        // Meta-operations (CREATE USER/ROLE, GRANT/REVOKE) require the
        // database-wide ADMIN privilege and no table-level lookup.
        Privilege.Action action = extractAction(parsedQuery);
        if (action == Privilege.Action.ADMIN) {
            getAuthorizer().check(session, Privilege.Action.ADMIN, "");
            return;
        }

        // Table-level checks
        List<String> tables;
        try {
            tables = extractTables(parsedQuery, cleanQuery);
        } catch (IllegalArgumentException unsupported) {
            // Statements without a table model (BEGIN/COMMIT/ROLLBACK/
            // SAVEPOINT/EXPLAIN/SET/VACUUM/ANALYZE/BATCH ...) carry no
            // table-level privilege requirement.
            return;
        }

        if (tables.size() == 1) {
            // Single table access
            getAuthorizer().check(session, action, tables.get(0));
        } else {
            // Multi-table access (e.g., JOIN) - check each table
            Set<Privilege.Action> actions = Set.of(action);
            for (String table : tables) {
                getAuthorizer().checkAny(session, actions, table);
            }
        }
    }

    /**
     * Extracts the action type from a parsed query.
     */
    private Privilege.Action extractAction(Query<?> parsedQuery) {
        if (parsedQuery instanceof SelectQuery) return Privilege.Action.SELECT;
        if (parsedQuery instanceof InsertQuery) return Privilege.Action.INSERT;
        if (parsedQuery instanceof UpdateQuery) return Privilege.Action.UPDATE;
        if (parsedQuery instanceof DeleteQuery) return Privilege.Action.DELETE;
        if (parsedQuery instanceof CreateTableQuery) return Privilege.Action.CREATE;
        if (parsedQuery instanceof DropTableQuery) return Privilege.Action.DROP;
        if (parsedQuery instanceof CreateIndexQueryBase
                || parsedQuery instanceof CreateCompositeIndexQuery
                || parsedQuery instanceof CreateCoveringIndexQuery) {
            return Privilege.Action.CREATE;
        }
        if (parsedQuery instanceof CreateUserQuery || parsedQuery instanceof CreateRoleQuery ||
            parsedQuery instanceof CreateTenantQuery || parsedQuery instanceof DropTenantQuery ||
            parsedQuery instanceof GrantRoleQuery || parsedQuery instanceof GrantPrivilegeQuery ||
            parsedQuery instanceof RevokePrivilegeQuery || parsedQuery instanceof AlterUserQuery) {
            return Privilege.Action.ADMIN;
        }
        if (parsedQuery instanceof AlterTableAddColumnQuery
                || parsedQuery instanceof AlterTableDropColumnQuery
                || parsedQuery instanceof AlterStatusQuery) {
            return Privilege.Action.ALTER;
        }
        return Privilege.Action.SELECT; // Default for unknown query types
    }

    /**
     * Extracts table names from a query using existing helpers. Statements
     * without a table model (transaction control, EXPLAIN, VACUUM ...) fail
     * the extraction with {@link IllegalArgumentException}, which the caller
     * treats as "no table privileges required".
     */
    private List<String> extractTables(Query<?> parsedQuery, String cleanQuery) {
        List<String> tables = new ArrayList<>();
        
        if (parsedQuery instanceof SelectQuery sq) {
            if (sq.getDerivedMainTable() != null) {
                tables.add(sq.getDerivedMainTable().getName());
            } else {
                // Plain SELECT: authorize the main table and every joined table
                if (sq.getTableName() != null && !sq.getTableName().isEmpty()) {
                    tables.add(sq.getTableName());
                }
                for (QueryParser.JoinInfo join : sq.getJoins()) {
                    if (join.tableName != null && !join.tableName.isEmpty()
                            && !join.tableName.equals(sq.getTableName())) {
                        tables.add(join.tableName);
                    }
                }
            }
        } else {
            String tableName = extractTableName(cleanQuery);
            if (tableName != null) {
                tables.add(tableName);
            }
        }
        
        return tables;
    }

    /**
     * Opens a server-side cursor over a SELECT query (Prompt 81). The query is
     * parsed and executed immediately, and the resulting rows are exposed via
     * the returned {@link Cursor}, which the caller (typically the server's
     * {@code ClientHandler}) uses to hand the client paginated batches of up to
     * {@code fetchSize} rows.
     *
     * <p>Only plain SELECT statements support cursors; any other statement kind
     * fails with an {@link IllegalArgumentException}. Transaction resolution,
     * table lookup and error handling mirror {@link #executeQuery}.
     *
     * @param query         the SQL SELECT to run (cursor semantics apply only
     *                      when the statement is a SELECT)
     * @param fetchSize     the number of rows each {@link Cursor#fetch()} returns
     * @param transactionId the caller's transaction id, or null
     * @return a new open cursor over the query result
     * @throws IllegalArgumentException when the query is null, not a SELECT, or
     *                                  references a missing table
     */
    public Cursor executeCursor(String query, int fetchSize, UUID transactionId) {
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        if (AUDIT_INNER.get()) {
            return executeCursorInternal(query, fetchSize, transactionId);
        }
        return audited(AuditRecord.USER_EMBEDDED, AuditRecord.USER_EMBEDDED, query,
                () -> executeCursorInternal(query, fetchSize, transactionId));
    }

    /** Audit-free cursor core shared by both {@link #executeCursor} overloads (prompt4 #33). */
    private Cursor executeCursorInternal(String query, int fetchSize, UUID transactionId) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("Cursor fetch size must be positive, got " + fetchSize);
        }
        String cleanQuery = stripMaxRowsHint(query);
        // Prompt 38 (java:S2259): toUpperCasePreservingQuotedIdentifiers can
        // return null; wrap in Optional to avoid NPE.
        String normalized = Optional.ofNullable(
                QueryParser.toUpperCasePreservingQuotedIdentifiers(cleanQuery.trim())).orElse("");
        if (!normalized.startsWith(SqlKeywords.SELECT)) {
            throw new IllegalArgumentException(
                    "Cursor can only be opened over a SELECT query, got: " + cleanQuery.trim());
        }
        Query<?> parsedQuery = parse(cleanQuery);
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;

        Table table;
        if (parsedQuery instanceof SelectQuery sq && sq.getDerivedMainTable() != null) {
            table = sq.getDerivedMainTable();
        } else {
            String tableName = extractTableName(cleanQuery);
            table = getTableForQuery(tableName, currentTransaction);
            if (table == null) {
                throw new TableNotFoundException(ErrorMessages.TABLE_PREFIX + tableName + ErrorMessages.DOES_NOT_EXIST);
            }
        }

        if (!(parsedQuery instanceof SelectQuery selectQuery)) {
            throw new IllegalArgumentException(
                    "Cursor can only be opened over a SELECT query, got: " + cleanQuery.trim());
        }
        // MVCC: install the caller's reader context while the cursor query runs.
        refreshReadCommittedSnapshot(currentTransaction);
        MvccReadContext.Context previousContext = MvccReadContext.get();
        MvccReadContext.set(MvccReadContext.contextFor(currentTransaction));
        try {
            Iterator<Map<String, Object>> iterator = selectQuery.executeAsIterator(table);
            return new Cursor(UUID.randomUUID(), cleanQuery.trim(), fetchSize, iterator);
        } finally {
            MvccReadContext.set(previousContext);
        }
    }

    /**
     * Opens a server-side cursor over a SELECT query with privilege checking (prompt4 #32).
     * This overload is used by the server when a session is available.
     *
     * @param query         the SQL SELECT to run
     * @param fetchSize     the number of rows each {@link Cursor#fetch()} returns
     * @param transactionId the caller's transaction id, or null
     * @param session       the authenticated session (for privilege checking), or null for embedded
     * @return a new open cursor over the query result
     * @throws IllegalArgumentException when the query is null, not a SELECT, or
     *                                  references a missing table
     */
    public Cursor executeCursor(String query, int fetchSize, UUID transactionId, Session session) {
        if (query == null) {
            throw new IllegalArgumentException(ErrorMessages.QUERY_NULL);
        }
        String user = session != null ? session.getUserName() : AuditRecord.USER_EMBEDDED;
        String ip = session != null ? session.getSourceIp() : AuditRecord.USER_EMBEDDED;
        // authorize() sits inside the audited body so a denied cursor-open is
        // recorded as an ERROR audit event (prompt4 #33).
        return audited(user, ip, query, () -> {
            // Authorizer check first (prompt4 #32): a cursor must never expose
            // rows the session is not allowed to read.
            if (session != null) {
                authorize(session, query);
            }
            return executeCursor(query, fetchSize, transactionId);
        });
    }

    /**
     * Executes an already-parsed query (from a {@link PreparedStatement})
     * against the database, reusing the caller's derived bindings but skipping
     * the parse phase (Prompt 79). The query string is the concrete SQL the
     * prepared statement rendered from its bound parameters; it is used only
     * for table resolution and profiling, never re-parsed.
     *
     * <p>The query is dispatched exactly like a normal {@link #executeQuery}
     * call: transaction-aware DML, auto-commit persistence and profiling all
     * behave identically, but the parsed AST from the prepared statement's
     * cache is executed directly.
     *
     * @param parsedQuery   the pre-parsed query (never null)
     * @param concreteSql   the concrete SQL rendered from the bound parameters
     * @param transactionId the caller's transaction id, or null
     * @return the query result (row list, null, or a status String)
     */
    public Object executeParsedPrepared(Query<?> parsedQuery, String concreteSql, UUID transactionId) {
        if (parsedQuery == null || concreteSql == null) {
            throw new IllegalArgumentException("Parsed query and SQL must not be null");
        }
        if (AUDIT_INNER.get()) {
            return executeParsedPreparedInternal(parsedQuery, concreteSql, transactionId);
        }
        return audited(AuditRecord.USER_EMBEDDED, AuditRecord.USER_EMBEDDED, concreteSql,
                () -> executeParsedPreparedInternal(parsedQuery, concreteSql, transactionId));
    }

    /** Audit-free prepared core shared by both overloads (prompt4 #33). */
    private Object executeParsedPreparedInternal(Query<?> parsedQuery, String concreteSql, UUID transactionId) {
        LOGGER.log(Level.FINE, "Executing prepared query: {0}", concreteSql);
        Transaction currentTransaction = transactionId != null ? activeTransactions.get(transactionId) : null;
        long execStart = System.nanoTime();
        try {
            Object result = dispatch(parsedQuery, concreteSql, currentTransaction, transactionId);
            recordQueryProfiling(parsedQuery, concreteSql, 0, System.nanoTime() - execStart);
            return result;
        } catch (DieselException e) {
            recordQueryProfiling(parsedQuery, concreteSql, 0, System.nanoTime() - execStart);
            throw e;
        } catch (RuntimeException e) {
            recordQueryProfiling(parsedQuery, concreteSql, 0, System.nanoTime() - execStart);
            throw e;
        }
    }

    /**
     * Executes a prepared statement with privilege checking (prompt4 #32).
     */
    public Object executeParsedPreparedWithSession(Query<?> parsedQuery, String concreteSql, UUID transactionId, Session session) {
        if (concreteSql == null) {
            throw new IllegalArgumentException("Parsed query and SQL must not be null");
        }
        String user = session != null ? session.getUserName() : AuditRecord.USER_EMBEDDED;
        String ip = session != null ? session.getSourceIp() : AuditRecord.USER_EMBEDDED;
        // authorize() inside the audited body so denied prepared executions
        // are recorded as ERROR audit events (prompt4 #33).
        return audited(user, ip, concreteSql, () -> {
            // Authorizer check first (prompt4 #32): never execute before the
            // privilege decision.
            if (session != null) {
                authorize(session, concreteSql);
            }
            return executeParsedPrepared(parsedQuery, concreteSql, transactionId);
        });
    }

    /**
     * Creates a prepared statement from the given SQL template (Prompt 79).
     *
     * @param sql the SQL template with {@code ?} placeholders
     * @return a new prepared statement wrapping the template
     */
    public PreparedStatement prepareStatement(String sql) {
        return new PreparedStatement(sql);
    }

    /**
     * Routes a parsed query to its execution path. Extracted from
     * {@link #executeQuery} so the execution phase can be timed as a unit for
     * the query profiler.
     */
    private Object dispatch(Query<?> parsedQuery, String cleanQuery, Transaction currentTransaction, UUID transactionId) {
        // MVCC (prompt4.md #4): READ COMMITTED refreshes the snapshot at every
        // statement, so the statement sees everything committed before it
        // started; REPEATABLE READ keeps the BEGIN-time snapshot untouched.
        refreshReadCommittedSnapshot(currentTransaction);
        // MVCC: every statement runs with the caller's reader identity so read
        // paths can apply snapshot/dirty-read visibility (prompt 67/68).
        MvccReadContext.Context previousContext = MvccReadContext.get();
        MvccReadContext.set(MvccReadContext.contextFor(currentTransaction));
        try {
            return dispatchInternal(parsedQuery, cleanQuery, currentTransaction, transactionId);
        } finally {
            MvccReadContext.set(previousContext);
        }
    }

    /**
     * Advances an active READ COMMITTED transaction's snapshot to the current
     * commit CSN so each statement observes every transaction committed before
     * it began. Batch transactions and every other isolation level keep their
     * BEGIN-time snapshot.
     *
     * @param transaction the caller's transaction, or null
     */
    private void refreshReadCommittedSnapshot(Transaction transaction) {
        if (transaction == null || !transaction.isActive() || transaction.isBatchMode()
                || transaction.getTxid() <= 0
                || transaction.getIsolationLevel() != IsolationLevel.READ_COMMITTED) {
            return;
        }
        transaction.setSnapshotCsn(txStatusTracker.getCurrentCommitCsn());
    }

    private Object dispatchInternal(Query<?> parsedQuery, String cleanQuery, Transaction currentTransaction, UUID transactionId) {
        if (parsedQuery instanceof SetIsolationLevelQuery q) {
            return executeSetIsolationLevel(q);
        }
        if (parsedQuery instanceof SetAutoCommitQuery q) {
            return executeSetAutoCommit(q);
        }
        if (parsedQuery instanceof BeginTransactionQuery q) {
            return executeBeginTransaction(q, currentTransaction);
        }
        if (parsedQuery instanceof CommitTransactionQuery) {
            return executeCommit(currentTransaction, transactionId);
        }
        if (parsedQuery instanceof SavepointQuery q) {
            return executeSavepoint(q, currentTransaction);
        }
        if (parsedQuery instanceof RollbackToSavepointQuery q) {
            return executeRollbackToSavepoint(q, currentTransaction);
        }
        if (parsedQuery instanceof ReleaseSavepointQuery q) {
            return executeReleaseSavepoint(q, currentTransaction);
        }
        if (parsedQuery instanceof RollbackTransactionQuery) {
            return executeRollback(currentTransaction, transactionId);
        }
        if (parsedQuery instanceof BatchQuery batchQuery) {
            return executeBatchQuery(batchQuery, currentTransaction, transactionId);
        }
        if (parsedQuery instanceof CreateTableQuery q) {
            return executeCreateTable(q);
        }
        if (parsedQuery instanceof DropTableQuery q) {
            dropTable(q.getTableName());
            return "Table dropped successfully";
        }
        if (parsedQuery instanceof AlterTableAddColumnQuery q) {
            return executeAlterTableAddColumn(q);
        }
        if (parsedQuery instanceof AlterTableDropColumnQuery q) {
            return executeAlterTableDropColumn(q);
        }
        if (parsedQuery instanceof AlterStatusQuery) {
            return executeAlterStatus();
        }
        if (parsedQuery instanceof CreateUserQuery q) {
            return executeCreateUser(q);
        }
        if (parsedQuery instanceof ShowTablesQuery q) {
            return executeShowTables(q);
        }
        if (parsedQuery instanceof ShowCreateTableQuery q) {
            return executeShowCreateTable(q);
        }
        if (parsedQuery instanceof ShowCreateIndexesQuery q) {
            return executeShowCreateIndexes(q);
        }
        if (parsedQuery instanceof CreateRoleQuery q) {
            return executeCreateRole(q);
        }
        if (parsedQuery instanceof CreateTenantQuery q) {
            return executeCreateTenant(q);
        }
        if (parsedQuery instanceof DropTenantQuery q) {
            return executeDropTenant(q);
        }
        if (parsedQuery instanceof GrantRoleQuery q) {
            return executeGrantRole(q);
        }
        if (parsedQuery instanceof GrantPrivilegeQuery q) {
            return executeGrantPrivilege(q);
        }
        if (parsedQuery instanceof RevokePrivilegeQuery q) {
            return executeRevokePrivilege(q);
        }
        if (parsedQuery instanceof AlterUserQuery q) {
            return executeAlterUser(q);
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
        if (parsedQuery instanceof VacuumQuery q) {
            return executeVacuum(q);
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
        long indexLookups = 0;
        long indexOnlyScans = 0;
        if (profiled instanceof SelectQuery sq) {
            planNanos = sq.getLastPlanNanos();
            sortNanos = sq.getLastSortNanos();
            indexLookups = sq.getLastIndexLookupCount();
            indexOnlyScans = sq.getLastIndexOnlyScanCount();
        }
        long executeNanos = Math.max(0, execTotalNanos - planNanos - sortNanos);
        QueryProfiler.getInstance().record(sql, parseNanos, planNanos, executeNanos, sortNanos,
                indexLookups, indexOnlyScans);
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
        boolean autoCommitValue = autoCommitQuery.isAutoCommit();
        setAutoCommit(autoCommitValue);
        return "AUTOCOMMIT set to " + (autoCommitValue ? SqlKeywords.ON : "OFF");
    }

    private Object executeBeginTransaction(BeginTransactionQuery beginQuery, Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            throw new TransactionException("Another transaction is already active for this client");
        }
        IsolationLevel isolationLevel = Objects.requireNonNullElse(beginQuery.getIsolationLevel(), defaultIsolationLevel);
        Transaction transaction = new Transaction(this, isolationLevel);
        UUID newTransactionId = transaction.getTransactionId();
        
        // MVCC: allocate txid and register with status tracker
        long txid = txStatusTracker.registerTransaction();
        long snapshotTxid = txid; // Snapshot of txid counter at BEGIN
        long snapshotCsn = txStatusTracker.getCurrentCommitCsn(); // Snapshot of commit CSN counter at BEGIN
        
        // Set MVCC fields using package-private setters
        transaction.setTxid(txid);
        transaction.setSnapshotTxid(snapshotTxid);
        transaction.setSnapshotCsn(snapshotCsn);
        
        // MVCC SSI: begin tracking for SERIALIZABLE transactions
        if (isolationLevel == IsolationLevel.SERIALIZABLE) {
            conflictDetector.beginTracking(txid, snapshotCsn);
        }
        
        activeTransactions.put(newTransactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        // WAL: BEGIN makes the txid visible to the analysis phase (prompt4 #19).
        // Failure here aborts the BEGIN: a transaction without a begin record
        // would be indistinguishable from legacy DML-without-BEGIN on recovery.
        if (isWALEnabled() && asyncWALWriter != null && txid > 0) {
            try {
                asyncWALWriter.appendAsync(txid, WALOpcode.BEGIN, null, null).get();
            } catch (Exception e) {
                activeTransactions.remove(newTransactionId);
                txStatusTracker.markAborted(txid);
                throw new TransactionException("Transaction begin failed due to WAL error: " + e.getMessage());
            }
        }
        setAutoCommit(false);
        return ErrorMessages.TRANSACTION_STARTED + newTransactionId;
    }

    private Object executeCommit(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to commit");
        }
        checkCommitConflicts(currentTransaction);
        
        // MVCC: record the commit CSN BEFORE the table changes are published and
        // persisted so concurrent readers only ever observe fully-formed rows
        long commitCsn = 0;
        if (currentTransaction.getTxid() > 0) {
            commitCsn = txStatusTracker.markCommitted(currentTransaction.getTxid());
        }
        
        // Write COMMIT record to WAL if enabled
        if (isWALEnabled() && groupCoordinator != null) {
            try {
                // Serialize commit payload: txid, commitCsn, modified tables summary
                byte[] payload = serializeCommitPayload(currentTransaction, commitCsn);
                // Submit to group coordinator for durability according to policy
                CompletableFuture<WALEntry> commitFuture = groupCoordinator.submitCommit(
                    currentTransaction.getTxid(), WALOpcode.COMMIT, null, payload);
                
                // Wait for durability (this implements the fsync policy)
                commitFuture.get(); // Blocks until durable per policy
                
                final long txid = currentTransaction.getTxid();
                final long finalCsn = commitCsn;
                LOGGER.fine(() -> String.format("WAL commit recorded for txid=%d, csn=%d", 
                    txid, finalCsn));
            } catch (Exception e) {
                // WAL write failed: abort the commit (no durability = no commit)
                final long txid = currentTransaction.getTxid();
                final Exception finalE = e;
                LOGGER.log(Level.SEVERE, () -> String.format("WAL commit failed for txid=%d, aborting transaction: %s", 
                    txid, finalE.getMessage()));
                // Rollback transaction state
                try {
                    currentTransaction.getUndoLog().clear();
                } catch (IOException ioException) {
                    LOGGER.log(Level.WARNING, "Failed to cleanup undo log during abort", ioException);
                }
                currentTransaction.setInactive();
                activeTransactions.remove(transactionId);
                setAutoCommit(false);
                throw new TransactionException("Transaction commit failed due to WAL error: " + e.getMessage());
            }
        }
        
        // Tombstone deleted rows now: MVCC deletes must not touch the physical
        // row until COMMIT (undo would be impossible afterwards), but the
        // version metadata is transient — without this tombstone the delete
        // would be lost on restart. Readers with a pre-commit snapshot still
        // see the row because isRowVisibleToReader applies the MVCC rules to
        // tombstones carrying an MVCC xmax.
        for (Map.Entry<String, Set<Integer>> entry : currentTransaction.getDeletedRows().entrySet()) {
            Table table = getTableForCommit(entry.getKey(), currentTransaction);
            if (table == null) {
                continue;
            }
            for (Integer rowIndex : entry.getValue()) {
                table.markDeleted(rowIndex);
                // Resolve the row's pending delete: clear the uncommitted
                // flags (only modifiedRows were stamped before, leaving
                // deleted rows permanently "pending" — which froze compaction
                // and unique-slot recycling) and stamp the commit CSN.
                table.markRowCommitted(rowIndex, commitCsn);
            }
        }
        persistModifiedTables(currentTransaction.getModifiedTables(), true);
        // Stamp every changed row with the commit CSN so snapshot readers can
        // tell committed-before-snapshot from committed-after (read-your-writes
        // stays intact: the owning transaction sees current values anyway).
        for (Map.Entry<String, Set<Integer>> entry : currentTransaction.getModifiedRows().entrySet()) {
            Table table = getTableForCommit(entry.getKey(), currentTransaction);
            if (table == null) {
                continue;
            }
            for (Integer rowIndex : entry.getValue()) {
                table.markRowCommitted(rowIndex, commitCsn);
            }
        }
        // The undo log is only needed until COMMIT or ROLLBACK resolves it.
        try {
            currentTransaction.getUndoLog().clear();
        } catch (java.io.IOException e) {
            LOGGER.log(Level.WARNING, "Failed to clean up undo log after COMMIT", e);
        }
        
        // MVCC SSI: check for rw-conflicts at commit for SERIALIZABLE transactions
        if (currentTransaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
            // Build writeSet for conflict detection
            Map<String, Set<Integer>> writeSet = new HashMap<>();
            for (Map.Entry<String, Set<Integer>> entry : currentTransaction.getModifiedRows().entrySet()) {
                writeSet.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
            for (Map.Entry<String, Set<Integer>> entry : currentTransaction.getDeletedRows().entrySet()) {
                writeSet.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue());
            }
            
            conflictDetector.noteCommit(
                currentTransaction.getTxid(), 
                commitCsn, 
                writeSet
            );
        }
        
        // Clear savepoints (they die with the transaction)
        currentTransaction.clearSavepoints();
        
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return ErrorMessages.TRANSACTION_COMMITTED;
    }

    /**
     * Serializes commit payload for WAL entry.
     *
     * <p>The encoding lives in {@link diesel.wal.CommitPayload} so the ARIES
     * redo phase (prompt 4 #18) replays commits with the exact writer format.
     *
     * @param transaction the committing transaction
     * @param commitCsn the commit sequence number
     * @return serialized payload bytes
     */
    private byte[] serializeCommitPayload(Transaction transaction, long commitCsn) {
        try {
            return CommitPayload.serialize(transaction.getTxid(), commitCsn,
                    transaction.getModifiedRows(), transaction.getDeletedRows());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize commit payload", e);
        }
    }

    /**
     * Resolves the live table for a commit-time row update: prefers the shared
     * table registered by the transaction (identical for MVCC paths), falling
     * back to the current registry for tables registered by name only.
     *
     * @param tableName   the table holding the changed rows
     * @param transaction the committing transaction
     * @return the table, or null when it no longer exists
     */
    private Table getTableForCommit(String tableName, Transaction transaction) {
        Table registered = transaction.getModifiedTables().get(tableName);
        if (registered != null) {
            return registered;
        }
        return getTable(tableName);
    }

    private void checkCommitConflicts(Transaction currentTransaction) {
        for (Map.Entry<String, Table> entry : currentTransaction.getModifiedTables().entrySet()) {
            String tableName = entry.getKey();
            Table modifiedTable = entry.getValue();
            if (modifiedTable != null) {
                Table sharedTable = tables.get(tableName);
                // MVCC transactions modify the shared table in place, so a
                // version change is their own doing — only a CoW copy (batch)
                // can still be compared against the BEGIN-time version.
                if (sharedTable != null && modifiedTable != sharedTable) {
                    Long snapshotVersion = currentTransaction.getSnapshotVersions().get(tableName);
                    if (snapshotVersion != null && sharedTable.getVersion() != snapshotVersion) {
                        throw new TransactionException(
                            "Write-write conflict on table " + tableName
                            + ": shared table version changed from " + snapshotVersion
                            + " to " + sharedTable.getVersion() + " since transaction began");
                    }
                }
            }
        }
    }

    private Object executeRollback(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to rollback");
        }
        
        // MVCC: hide the transaction's rows from every reader first, then apply
        // the undo log in reverse so the shared tables return to their
        // pre-BEGIN state (inserts removed, updates restored).
        if (currentTransaction.getTxid() > 0) {
            txStatusTracker.markAborted(currentTransaction.getTxid());
            // WAL: ABORT removes the txid from the recovery active set so a
            // later crash does not undo an already-rolled-back transaction.
            if (isWALEnabled() && asyncWALWriter != null) {
                try {
                    asyncWALWriter.appendAsync(currentTransaction.getTxid(), WALOpcode.ABORT, null, null).get();
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "WAL ABORT record failed (rollback continues): " + e.getMessage());
                }
            }
        }
        currentTransaction.rollback();
        
        // MVCC SSI: cleanup tracking for SERIALIZABLE transactions
        if (currentTransaction.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
            conflictDetector.noteRollback(currentTransaction.getTxid());
        }
        
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return ErrorMessages.TRANSACTION_ROLLED_BACK;
    }

    /**
     * Executes SAVEPOINT name: creates a savepoint marker in the undo log.
     */
    private Object executeSavepoint(SavepointQuery savepointQuery, Transaction currentTransaction) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to create savepoint");
        }
        
        String name = savepointQuery.getSavepointName().toUpperCase();
        // Create bookkeeping snapshot and savepoint
        Transaction.BookkeepingSnapshot snapshot = currentTransaction.snapshotBookkeeping();
        Savepoint savepoint = currentTransaction.getSavepointManager()
                .createSavepoint(name, snapshot.modifiedRows(), snapshot.deletedRows());
        
        // Append savepoint marker to undo log
        currentTransaction.getUndoLog().appendSavepointMarker(name);
        
        // Write WAL SAVEPOINT record
        if (isWALEnabled() && asyncWALWriter != null) {
            try {
                byte[] payload = SavepointPayload.serialize(name, savepoint.getSequence());
                asyncWALWriter.appendAsync(currentTransaction.getTxid(), WALOpcode.SAVEPOINT, null, payload).get();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "WAL SAVEPOINT record failed: " + e.getMessage());
                // Continue: savepoint created in memory and undo log
            }
        }
        
        return ErrorMessages.SAVEPOINT_CREATED + name;
    }

    /**
     * Executes ROLLBACK TO SAVEPOINT name: rolls back to the specified savepoint.
     */
    private Object executeRollbackToSavepoint(RollbackToSavepointQuery rollbackQuery, Transaction currentTransaction) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to rollback");
        }
        
        String name = rollbackQuery.getSavepointName().toUpperCase();
        NestedSavepointStack.RollbackResult result = currentTransaction.getSavepointManager()
                .rollbackToSavepoint(name);
        
        // Apply undo records to the savepoint
        try {
            int appliedCount = currentTransaction.getUndoLog()
                    .rollbackToMarker(this, name);
            LOGGER.log(Level.INFO, "Applied " + appliedCount + " undo records to rollback to savepoint: " + name);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rollback to savepoint: " + name, e);
        }
        
        // Restore bookkeeping from savepoint snapshot
        currentTransaction.restoreBookkeeping(
                new Transaction.BookkeepingSnapshot(
                        result.getTarget().getModifiedSnapshot(), 
                        result.getTarget().getDeletedSnapshot()));
        
        // Write WAL RELEASE records for invalidated savepoints
        if (isWALEnabled() && asyncWALWriter != null) {
            try {
                for (Savepoint invalidated : result.getInvalidated()) {
                    byte[] payload = SavepointPayload.serialize(invalidated.getName());
                    asyncWALWriter.appendAsync(currentTransaction.getTxid(), WALOpcode.RELEASE_SAVEPOINT, null, payload).get();
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "WAL RELEASE_SAVEPOINT records failed: " + e.getMessage());
                // Continue: savepoint stack updated in memory
            }
        }
        
        return ErrorMessages.SAVEPOINT_ROLLED_BACK + name;
    }

    /**
     * Executes RELEASE SAVEPOINT name: releases the savepoint and its descendants.
     */
    private Object executeReleaseSavepoint(ReleaseSavepointQuery releaseQuery, Transaction currentTransaction) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active transaction to release savepoint");
        }
        
        String name = releaseQuery.getSavepointName().toUpperCase();
        List<Savepoint> released = currentTransaction.getSavepointManager().releaseSavepoint(name);
        
        // Write WAL RELEASE records
        if (isWALEnabled() && asyncWALWriter != null) {
            try {
                for (Savepoint savepoint : released) {
                    byte[] payload = SavepointPayload.serialize(savepoint.getName());
                    asyncWALWriter.appendAsync(currentTransaction.getTxid(), WALOpcode.RELEASE_SAVEPOINT, null, payload).get();
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "WAL RELEASE_SAVEPOINT records failed: " + e.getMessage());
                // Continue: savepoint stack updated in memory
            }
        }
        
        return ErrorMessages.SAVEPOINT_RELEASED + name;
    }

    private Object executeBatchQuery(BatchQuery batchQuery, Transaction currentTransaction, UUID transactionId) {
        if (batchQuery.getMode() == BatchQuery.Mode.BEGIN_BATCH) {
            return executeBeginBatch(currentTransaction);
        } else {
            return executeEndBatch(currentTransaction, transactionId);
        }
    }

    private Object executeBeginBatch(Transaction currentTransaction) {
        if (currentTransaction != null && currentTransaction.isActive()) {
            throw new TransactionException("Another transaction is already active for this client");
        }
        Transaction transaction = new Transaction(this, defaultIsolationLevel);
        transaction.setBatchMode(true);
        UUID newTransactionId = transaction.getTransactionId();
        activeTransactions.put(newTransactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        setAutoCommit(false);
        return "Batch started: " + newTransactionId;
    }

    private Object executeEndBatch(Transaction currentTransaction, UUID transactionId) {
        if (currentTransaction == null || !currentTransaction.isActive()) {
            throw new TransactionException("No active batch to end");
        }
        if (!currentTransaction.isBatchMode()) {
            throw new TransactionException("Current transaction is not a batch");
        }
        for (Map.Entry<String, Table> entry : currentTransaction.getModifiedTables().entrySet()) {
            String tableName = entry.getKey();
            Table modifiedTable = entry.getValue();
            if (modifiedTable != null) {
                Table sharedTable = tables.get(tableName);
                if (sharedTable != null) {
                    Long snapshotVersion = currentTransaction.getSnapshotVersions().get(tableName);
                    if (snapshotVersion != null && sharedTable.getVersion() != snapshotVersion) {
                        throw new TransactionException(
                            "Write-write conflict on table " + tableName
                            + ": shared table version changed from " + snapshotVersion
                            + " to " + sharedTable.getVersion() + " since batch began");
                    }
                }
            }
        }
        currentTransaction.getModifiedTables().entrySet().stream()
                .filter(e -> e.getValue() != null)
                .forEach(entry -> {
                    try {
                        entry.getValue().flushDeferredIndexUpdates();
                    } catch (RuntimeException e) {
                        currentTransaction.setInactive();
                        activeTransactions.remove(transactionId);
                        setAutoCommit(false);
                        throw new TransactionException("Batch failed: " + e.getMessage());
                    }
                });
        persistModifiedTables(currentTransaction.getModifiedTables(), true);
        currentTransaction.setInactive();
        activeTransactions.remove(transactionId);
        setAutoCommit(false);
        return "Batch committed";
    }

    private Object executeCreateTable(CreateTableQuery createQuery) {
        createTable(createQuery.getTableName(), createQuery.getColumns(), createQuery.getColumnTypes(), createQuery.getPrimaryKeyColumn());
        Table table = getTable(createQuery.getTableName());
        for (Map.Entry<String, Sequence> entry : createQuery.getSequences().entrySet()) {
            table.getSequences().put(entry.getKey(), entry.getValue());
        }
        return "Table created successfully";
    }

    private Object executeAlterTableAddColumn(AlterTableAddColumnQuery alterQuery) {
        String tableName = alterQuery.getTableName();
        // getTable throws TableNotFoundException for unknown tables (engine-wide convention)
        Table table = getTable(tableName);
        if (activeAlters.containsKey(tableName)) {
            throw new DieselException("ALTER COPY already in progress on table " + tableName);
        }

        if (alterQuery.getAlgorithm() == diesel.schema.AlterAlgorithm.COPY) {
            return executeAlterTableCopy(alterQuery, table);
        }

        // Get current schema
        List<String> currentColumns = table.getColumns();
        Map<String, Class<?>> currentTypes = table.getColumnTypes();
        
        // Prepare new schema (step 1: ADD COLUMN only)
        List<String> newColumns = new ArrayList<>(currentColumns);
        newColumns.add(alterQuery.getColumnName());
        
        Map<String, Class<?>> newTypes = new HashMap<>(currentTypes);
        newTypes.put(alterQuery.getColumnName(), alterQuery.getColumnType());
        
        // LOCK gates (prompt4 #39): SHARED blocks writers, EXCLUSIVE also blocks
        // readers for the duration of the metadata swap (microseconds; the
        // alter.inplace.hold.ms test hook widens the window for observability).
        diesel.schema.AlterLockMode lockMode = alterQuery.getLockMode();
        table.beginAlterGate(lockMode);
        try {
            holdInplaceGateWindow();
            // Apply schema change under write lock
            table.getTableLock().writeLock().lock();
            try {
                table.applySchemaChange(newColumns, newTypes, 
                    "ADD COLUMN " + alterQuery.getColumnName() + " " + alterQuery.getColumnType().getSimpleName());
                
                // Publish the new schema version to the catalog
                publishAlterCatalog(tableName, table);
                
                // Invalidate query cache (DDL contract)
                queryCache.invalidateAll();
                
                return "Table altered successfully: added column " + alterQuery.getColumnName();
            } finally {
                table.getTableLock().writeLock().unlock();
            }
        } finally {
            table.endAlterGate(lockMode);
        }
    }

    /**
     * Test hook (prompt4 #39): when the {@code alter.inplace.hold.ms} system
     * property is set, an INPLACE ALTER holds its LOCK gates that long before
     * the swap - tests observe writer/reader blocking deterministically.
     * Default 0 (no delay).
     */
    private static void holdInplaceGateWindow() {
        int holdMs = Integer.getInteger("alter.inplace.hold.ms", 0);
        if (holdMs > 0) {
            try {
                Thread.sleep(holdMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DieselException("ALTER interrupted while holding LOCK gates");
            }
        }
    }

    /**
     * Starts a background ALGORITHM=COPY rewrite of the table (prompt4 #38).
     * The statement returns as soon as the journal is attached and the
     * baseline captured; poll {@code ALTER STATUS} for completion.
     */
    private Object executeAlterTableCopy(AlterTableAddColumnQuery alterQuery, Table table) {
        String tableName = alterQuery.getTableName();
        List<String> newColumns = new ArrayList<>(table.getColumns());
        newColumns.add(alterQuery.getColumnName());
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.put(alterQuery.getColumnName(), alterQuery.getColumnType());

        AlterTableCopyAlgorithm algorithm = new AlterTableCopyAlgorithm(this, table, newColumns, newTypes,
                "ADD COLUMN " + alterQuery.getColumnName() + " " + alterQuery.getColumnType().getSimpleName());
        activeAlters.put(tableName, algorithm);
        try {
            algorithm.start(getAlterCopyExecutor());
        } catch (RuntimeException e) {
            activeAlters.remove(tableName, algorithm);
            throw e;
        }
        return "Table alter (ALGORITHM=COPY) started: adding column "
                + alterQuery.getColumnName() + " to " + tableName;
    }

    /**
     * Executes {@code ALTER TABLE ... DROP COLUMN} (prompt4 #39, R3-010 step 3).
     *
     * <p>INPLACE (the default for DROP): metadata swap under LOCK gates - the
     * column leaves the schema immediately, its keys stay in row maps until a
     * background strip (compact). COPY: background rewrite via
     * {@link AlterTableCopyAlgorithm} with key stripping and index removal at
     * swap time. Requested INPLACE for an indexed column falls back to COPY
     * with a warning (task 5 of the prompt).
     */
    private Object executeAlterTableDropColumn(AlterTableDropColumnQuery query) {
        String tableName = query.getTableName();
        // getTable throws TableNotFoundException for unknown tables (engine-wide convention)
        Table table = getTable(tableName);
        if (activeAlters.containsKey(tableName)) {
            throw new DieselException("ALTER COPY already in progress on table " + tableName);
        }

        List<String> currentColumns = table.getColumns();
        String column = currentColumns.stream()
                .filter(c -> c.equalsIgnoreCase(query.getColumnName()))
                .findFirst()
                .orElseThrow(() -> new ColumnNotFoundException(
                        ErrorMessages.COLUMN_PREFIX + query.getColumnName() + ErrorMessages.DOES_NOT_EXIST));
        String pk = table.getPrimaryKeyColumn();
        if (pk != null && pk.equalsIgnoreCase(column)) {
            throw new DieselException("Primary key column cannot be dropped: " + column);
        }
        if (table.hasClusteredIndex() && table.getClusteredIndexColumn() != null
                && table.getClusteredIndexColumn().equalsIgnoreCase(column)) {
            throw new DieselException("Clustered index column cannot be dropped: " + column);
        }

        List<String> newColumns = new ArrayList<>(currentColumns);
        newColumns.removeIf(c -> c.equalsIgnoreCase(column));
        Map<String, Class<?>> newTypes = new HashMap<>(table.getColumnTypes());
        newTypes.remove(column);

        boolean fallback = false;
        if (query.getAlgorithm() == diesel.schema.AlterAlgorithm.INPLACE
                && table.hasIndexOnColumn(column)) {
            // INPLACE never rewrites rows, so an index on the removed column
            // could not be rebuilt or kept valid - fall back to COPY (task 5).
            fallback = true;
            LOGGER.log(Level.WARNING,
                    "ALGORITHM=INPLACE not supported for indexed column {0} on {1};"
                    + " falling back to ALGORITHM=COPY",
                    new Object[]{column, tableName});
        }

        if (query.getAlgorithm() == diesel.schema.AlterAlgorithm.COPY || fallback) {
            Object started = executeAlterTableCopyDrop(table, query, column, newColumns, newTypes);
            if (fallback) {
                return "Warning: ALGORITHM=INPLACE is not supported for indexed column "
                        + column + "; fell back to ALGORITHM=COPY. " + started;
            }
            return started;
        }

        // INPLACE path: metadata swap under LOCK gates (microsecond window),
        // then the physical strip runs on the background alter executor.
        diesel.schema.AlterLockMode lockMode = query.getLockMode();
        table.beginAlterGate(lockMode);
        try {
            holdInplaceGateWindow();
            table.getTableLock().writeLock().lock();
            try {
                table.applySchemaChange(newColumns, newTypes, "DROP COLUMN " + column);
                publishAlterCatalog(tableName, table);
                queryCache.invalidateAll();
                table.markAlterPersistDirty();
            } finally {
                table.getTableLock().writeLock().unlock();
            }
        } finally {
            table.endAlterGate(lockMode);
        }
        scheduleDropColumnStrip(table);
        return "Table altered successfully: dropped column " + column;
    }

    /**
     * Starts the background ALGORITHM=COPY rewrite for a DROP COLUMN
     * (prompt4 #39): the shadow strips the column's keys, the column's indexes
     * are removed at swap time (so a rollback restores everything untouched).
     */
    private Object executeAlterTableCopyDrop(Table table, AlterTableDropColumnQuery query,
                                             String column, List<String> newColumns,
                                             Map<String, Class<?>> newTypes) {
        String tableName = query.getTableName();
        AlterTableCopyAlgorithm algorithm = new AlterTableCopyAlgorithm(this, table, newColumns, newTypes,
                "DROP COLUMN " + column, column);
        activeAlters.put(tableName, algorithm);
        try {
            algorithm.start(getAlterCopyExecutor());
        } catch (RuntimeException e) {
            activeAlters.remove(tableName, algorithm);
            throw e;
        }
        return "Table alter (ALGORITHM=COPY) started: dropping column "
                + column + " from " + tableName;
    }

    /**
     * Schedules the physical strip of dropped-column keys after an INPLACE
     * DROP COLUMN (prompt4 #39). compact() skips (deferring to vacuum) while
     * MVCC/tombstones are in flight; a shutdown executor just leaves the strip
     * to the next vacuum pass or saveToFile compaction.
     */
    private void scheduleDropColumnStrip(Table table) {
        try {
            getAlterCopyExecutor().submit(() -> {
                try {
                    table.compact();
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING,
                            "Background DROP COLUMN strip failed for table {0}; vacuum will retry",
                            new Object[]{table.getName(), e});
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            LOGGER.log(Level.FINE,
                    "DROP COLUMN strip not scheduled (executor shut down): {0}", table.getName());
        }
    }

    /** Single daemon worker for background online-ALTER copies. */
    private java.util.concurrent.ExecutorService getAlterCopyExecutor() {
        java.util.concurrent.ExecutorService executor = alterCopyExecutor;
        if (executor == null) {
            synchronized (this) {
                executor = alterCopyExecutor;
                if (executor == null) {
                    executor = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "diesel-alter-copy");
                        t.setDaemon(true);
                        return t;
                    });
                    alterCopyExecutor = executor;
                }
            }
        }
        return executor;
    }

    /**
     * Publishes the table's current schema version to the system catalog
     * (shared by the step-1 metadata-only path and the ALGORITHM=COPY
     * finalize). Caller must hold the table write lock.
     */
    void publishAlterCatalog(String tableName, Table table) {
        List<CatalogSchema.ColumnSchema> columnSchemas = new ArrayList<>();
        for (Map.Entry<String, Class<?>> entry : table.getColumnTypes().entrySet()) {
            columnSchemas.add(new CatalogSchema.ColumnSchema(
                    entry.getKey(),
                    entry.getValue().getSimpleName(),
                    true, // nullable by default for ADD COLUMN
                    entry.getKey().equals(table.getPrimaryKeyColumn()) // unique if primary key
            ));
        }

        CatalogSchema catalogSchema = new CatalogSchema(
                tableName,
                columnSchemas,
                table.getPrimaryKeyColumn(),
                table.getStorageType(),
                new ArrayList<>(table.getSequences().keySet()),
                new ArrayList<>() // indices not supported in step 1
        );
        catalogSchema.setSchemaVersion(table.getSchemaVersions().currentVersion());
        getCatalog().updateTableSchema(catalogSchema);
        // DDL contract: cached plans against the old schema are stale
        queryCache.invalidateAll();
    }

    /**
     * Bookkeeping when a background ALTER COPY reaches a terminal state:
     * removes the entry from the active map and pushes its final
     * {@code ALTER STATUS} row onto the bounded history.
     */
    void onAlterFinished(AlterTableCopyAlgorithm algorithm) {
        activeAlters.remove(algorithm.getTableName(), algorithm);
        Map<String, Object> row = algorithm.statusRow();
        synchronized (alterHistory) {
            alterHistory.addFirst(row);
            while (alterHistory.size() > ALTER_HISTORY_LIMIT) {
                alterHistory.removeLast();
            }
        }
    }

    /** True while an ALGORITHM=COPY rewrite is running (or finalizing) on the table. */
    boolean isAlterCopyActive(String tableName) {
        return activeAlters.containsKey(tableName);
    }

    /**
     * {@code ALTER STATUS}: one row per online ALTER - active copies first,
     * then the recent history (newest first).
     */
    List<Map<String, Object>> executeAlterStatus() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (AlterTableCopyAlgorithm algorithm : activeAlters.values()) {
            rows.add(algorithm.statusRow());
        }
        synchronized (alterHistory) {
            rows.addAll(alterHistory);
        }
        return rows;
    }

    /**
     * Requests cancellation of a running ALTER COPY (rollback semantics: the
     * old schema and data stay authoritative). Returns false when no copy is
     * active for the table.
     */
    public boolean cancelAlter(String tableName) {
        AlterTableCopyAlgorithm algorithm = activeAlters.get(tableName);
        if (algorithm == null) {
            return false;
        }
        algorithm.requestCancel();
        return true;
    }

    /**
     * Returns the RBAC user store, creating the four {@code DIESEL_*} system
     * tables on first use (prompt4 #30). Lazy so non-security workloads never
     * pay for it.
     */
    UserStore getUserStore() {
        UserStore local = userStore;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (userStore == null) {
                ensureSystemTables();
                int iterations = ConfigLoader.getInt("security.pbkdf2.iterations",
                        PasswordHasher.DEFAULT_ITERATIONS);
                userStore = new UserStore(new SystemTableBackend(), new PasswordHasher(iterations));
                userStore.ensureAdminPrivileges();
            }
            return userStore;
        }
    }

    private void ensureSystemTables() {
        ensureSystemTable(UserStoreBackend.USERS,
                List.of("ID", "NAME", "PASSWORD_HASH", "CREATED_AT"), "ID");
        ensureSystemTable(UserStoreBackend.ROLES,
                List.of("ID", "NAME", "CREATED_AT"), "ID");
        ensureSystemTable(UserStoreBackend.USER_ROLES,
                List.of("ID", "USER_ID", "ROLE_ID"), "ID");
        ensureSystemTable(UserStoreBackend.PRIVILEGES,
                List.of("ID", "ROLE_ID", "ACTION", "TABLE_NAME"), "ID");
    }

    private void ensureSystemTable(String name, List<String> columns, String primaryKey) {
        if (tables.containsKey(name)) {
            return;
        }
        Map<String, Class<?>> types = new HashMap<>();
        for (String column : columns) {
            types.put(column, String.class);
        }
        createTable(name, columns, types, primaryKey);
        LOGGER.log(Level.INFO, "Created system table {0}", name);
    }

    /**
     * Returns the SNI tenant store, creating the {@code DIESEL_TENANTS}
     * system table on first use (prompt4 #36). Lazy so deployments without
     * multi-tenant TLS never pay for it.
     */
    TenantStore getTenantStore() {
        TenantStore local = tenantStore;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (tenantStore == null) {
                ensureSystemTable(TenantStoreBackend.TENANTS,
                        List.of("ID", "NAME", "SNI_HOSTNAME", "KEYSTORE_PATH",
                                "KEYSTORE_PASSWORD", "CREATED_AT"), "ID");
                tenantStore = new TenantStore(new TenantTableBackend());
            }
            return tenantStore;
        }
    }

    /** CREATE TENANT — stores the SNI→keystore mapping (prompt4 #36). */
    String executeCreateTenant(CreateTenantQuery query) {
        getTenantStore().createTenant(query.getTenantName(), query.getSniHostname(),
                query.getKeystorePath(), query.getKeystorePassword());
        return "Tenant created: " + query.getTenantName();
    }

    /** DROP TENANT — removes the mapping (prompt4 #36). */
    String executeDropTenant(DropTenantQuery query) {
        getTenantStore().dropTenant(query.getTenantName());
        return "Tenant dropped: " + query.getTenantName();
    }

    /** CREATE USER — hashes the password and stores the account (prompt4 #30). */
    String executeCreateUser(CreateUserQuery query) {
        getUserStore().createUser(query.getUserName(), query.getPassword());
        return "User created: " + query.getUserName();
    }

    /** CREATE ROLE — stores the role (prompt4 #30). */
    String executeCreateRole(CreateRoleQuery query) {
        getUserStore().createRole(query.getRoleName());
        return "Role created: " + query.getRoleName();
    }

    /** GRANT role TO user — links the two (prompt4 #30). */
    String executeGrantRole(GrantRoleQuery query) {
        getUserStore().grantRoleToUser(query.getUserName(), query.getRoleName());
        return "Role granted: " + query.getRoleName() + " -> " + query.getUserName();
    }

    /** ALTER USER — replaces the password hash and clears a forced change (prompt4 #31). */
    String executeAlterUser(AlterUserQuery query) {
        getUserStore().updatePassword(query.getUserName(), query.getPassword());
        return "Password changed: " + query.getUserName();
    }

    /** GRANT privilege TO role — grants one or more privileges (prompt4 #32). */
    String executeGrantPrivilege(GrantPrivilegeQuery query) {
        for (Privilege.Action action : query.getActions()) {
            getUserStore().grantPrivilege(query.getRoleName(), action, query.getTableName());
        }
        return "Privilege granted: " + String.join(", ", actionNames(query.getActions())) + 
               " on " + (query.getTableName().isEmpty() ? "database" : query.getTableName()) + 
               " -> " + query.getRoleName();
    }

    /** REVOKE privilege FROM role — revokes one or more privileges (prompt4 #32). */
    String executeRevokePrivilege(RevokePrivilegeQuery query) {
        for (Privilege.Action action : query.getActions()) {
            getUserStore().revokePrivilege(query.getRoleName(), action, query.getTableName());
        }
        return "Privilege revoked: " + String.join(", ", actionNames(query.getActions())) + 
               " on " + (query.getTableName().isEmpty() ? "database" : query.getTableName()) + 
               " -> " + query.getRoleName();
    }

    private static List<String> actionNames(List<Privilege.Action> actions) {
        List<String> names = new ArrayList<>(actions.size());
        for (Privilege.Action action : actions) {
            names.add(action.name());
        }
        return names;
    }

    /**
     * Returns the authorizer for privilege checking (prompt4 #32). Lazy so
     * non-security workloads never pay for it.
     */
    Authorizer getAuthorizer() {
        Authorizer local = authorizer;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (authorizer == null) {
                authorizer = new Authorizer(getUserStore());
            }
            return authorizer;
        }
    }

    /**
     * Returns the SQL audit logger (prompt4 #33), or {@code null} when
     * {@code audit.enabled} is false (the default). Lazy so development
     * workloads pay nothing: the hot path is a single volatile read.
     * On first use with auditing enabled the {@code DIESEL_AUDIT_LOG}
     * system table is created.
     */
    AuditLogger getAuditLogger() {
        if (auditInitDone) {
            return auditLogger;
        }
        synchronized (this) {
            if (!auditInitDone) {
                AuditLogger created = AuditLogger.create(new AuditTableBackend(),
                        Path.of(dataDir, "audit-archive"));
                if (created.isEnabled()) {
                    ensureSystemTable(AuditBackend.AUDIT,
                            List.of("ID", "EVENT_TIME", "USER_NAME", "SOURCE_IP",
                                    "QUERY", "STATUS", "DURATION_MS"),
                            "ID");
                    auditLogger = created;
                }
                auditInitDone = true;
            }
            return auditLogger;
        }
    }

    /** Bridges AuditLogger onto the DIESEL_AUDIT_LOG table (prompt4 #33). */
    private final class AuditTableBackend implements AuditBackend {
        @Override
        public List<Map<String, Object>> readAll() {
            return getTable(AUDIT).getRows();
        }

        @Override
        public void appendRow(Map<String, Object> row) {
            Table table = getTable(AUDIT);
            table.addRow(row);
            table.saveToFile(AUDIT);
        }

        @Override
        public void appendBatch(List<Map<String, Object>> rows) {
            Table table = getTable(AUDIT);
            for (Map<String, Object> row : rows) {
                table.addRow(row);
            }
            table.saveToFile(AUDIT);
        }

        @Override
        public void deleteRows(List<String> ids) {
            Table table = getTable(AUDIT);
            List<Map<String, Object>> current = table.getRows();
            for (int i = current.size() - 1; i >= 0; i--) {
                if (ids.contains(String.valueOf(current.get(i).get("ID")))) {
                    table.removeRow(i);
                }
            }
            table.saveToFile(AUDIT);
        }
    }

    /** Bridges UserStore onto the DIESEL_* system tables (prompt4 #30). */
    private final class SystemTableBackend implements UserStoreBackend {
        @Override
        public List<Map<String, Object>> readAll(String tableName) {
            return getTable(tableName).getRows();
        }

        @Override
        public void appendRow(String tableName, Map<String, Object> row) {
            Table table = getTable(tableName);
            table.addRow(row);
            table.saveToFile(tableName);
        }

        @Override
        public void updateRow(String tableName, Map<String, Object> row) {
            Table table = getTable(tableName);
            Object id = row.get("ID");
            List<Map<String, Object>> current = table.getRows();
            for (int i = 0; i < current.size(); i++) {
                if (Objects.equals(id, current.get(i).get("ID"))) {
                    table.updateRowInPlace(i, row);
                    table.saveToFile(tableName);
                    return;
                }
            }
            throw new IllegalStateException("No row with ID " + id + " in " + tableName);
        }

        @Override
        public void deleteRow(String tableName, String id) {
            Table table = getTable(tableName);
            List<Map<String, Object>> current = table.getRows();
            for (int i = 0; i < current.size(); i++) {
                if (Objects.equals(id, current.get(i).get("ID"))) {
                    table.removeRow(i);
                    table.saveToFile(tableName);
                    return;
                }
            }
            throw new IllegalStateException("No row with ID " + id + " in " + tableName);
        }
    }

    /** Bridges TenantStore onto the DIESEL_TENANTS system table (prompt4 #36). */
    private final class TenantTableBackend implements TenantStoreBackend {
        @Override
        public List<Map<String, Object>> readAll() {
            return getTable(TENANTS).getRows();
        }

        @Override
        public void appendRow(Map<String, Object> row) {
            Table table = getTable(TENANTS);
            table.addRow(row);
            table.saveToFile(TENANTS);
        }

        @Override
        public void deleteRow(String id) {
            Table table = getTable(TENANTS);
            List<Map<String, Object>> current = table.getRows();
            for (int i = 0; i < current.size(); i++) {
                if (Objects.equals(id, current.get(i).get("ID"))) {
                    table.removeRow(i);
                    table.saveToFile(TENANTS);
                    return;
                }
            }
            throw new IllegalStateException("No row with ID " + id + " in " + TENANTS);
        }
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
     * Executes a {@code VACUUM} statement: reclaims dead row versions on the
     * named table (or on every registered table for a bare {@code VACUUM})
     * and invalidates the query-plan cache, since physical row positions and
     * indexes may have changed.
     *
     * @param vacuumQuery the parsed VACUUM query
     * @return the status message describing the reclaimed tuples
     */
    private Object executeVacuum(VacuumQuery vacuumQuery) {
        String tableName = vacuumQuery.getTableName();
        String result = tableName == null
                ? vacuumManager.vacuumAll()
                : vacuumManager.vacuumTable(getTable(tableName));
        queryCache.invalidateAll();
        return result;
    }

    /**
     * Executes a SHOW TABLES statement and returns the list of table names.
     */
    private Object executeShowTables(ShowTablesQuery showTablesQuery) {
        return showTablesQuery.execute(this, -1);
    }

    /**
     * Executes a SHOW CREATE TABLE statement and returns the DDL.
     */
    private Object executeShowCreateTable(ShowCreateTableQuery showCreateTableQuery) {
        return showCreateTableQuery.execute(this, -1);
    }

    /**
     * Executes a SHOW CREATE INDEXES statement and returns the index DDL.
     */
    private Object executeShowCreateIndexes(ShowCreateIndexesQuery showCreateIndexesQuery) {
        return showCreateIndexesQuery.execute(this, -1);
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

        if (isDml) {
            return executeDmlQuery(parsedQuery, table, tableName, currentTransaction);
        }
        return parsedQuery.execute(table);
    }

    private Object executeDmlQuery(Query<?> parsedQuery, Table table, String tableName, Transaction currentTransaction) {
        if (autoCommit && (currentTransaction == null || !currentTransaction.isActive())) {
            return executeAutoCommitDml(parsedQuery, table, tableName);
        }
        if (currentTransaction != null && currentTransaction.isActive()) {
            return executeTransactionDml(parsedQuery, table, tableName, currentTransaction);
        }
        Object dmlResult = parsedQuery.execute(table);
        Table.flushPendingPersistsForDataDir(this.dataDir);
        table.saveToFile(tableName);
        return dmlResult;
    }

    private Object executeAutoCommitDml(Query<?> parsedQuery, Table table, String tableName) {
        Transaction implicitTransaction = new Transaction(this, defaultIsolationLevel);
        try {
            Object implicitResult = parsedQuery.execute(table);
            implicitTransaction.registerModifiedTable(tableName, table);
            persistModifiedTables(implicitTransaction.getModifiedTables(), false);
            return implicitResult;
        } finally {
            implicitTransaction.setInactive();
        }
    }

    private Object executeTransactionDml(Query<?> parsedQuery, Table table, String tableName, Transaction currentTransaction) {
        if (currentTransaction.isBatchMode()) {
            // Batch keeps copy-on-write: the DML runs against the transaction's
            // private copy, published wholesale at END BATCH. An online ALTER
            // COPY would swap the shared table underneath that publish, so the
            // batch is rejected until the copy finishes.
            if (activeAlters.containsKey(tableName)) {
                throw new DieselException("ALTER COPY is in progress on table " + tableName
                        + ": batch operations are blocked until the copy finishes");
            }
            if (!currentTransaction.getModifiedTables().containsKey(tableName)) {
                currentTransaction.updateTable(tableName, table);
            }
            Table batchCopy = currentTransaction.getModifiedTables().get(tableName);
            return parsedQuery.execute(batchCopy);
        }
        // MVCC (prompt4.md #4): INSERT/UPDATE/DELETE all mutate the shared table
        // in place with undo logging. Pending changes stay invisible to other
        // transactions through RowVersionMeta / TxStatusTracker; optimistic
        // write-write conflicts throw at the writer instead of at COMMIT.
        if (!currentTransaction.getModifiedTables().containsKey(tableName)) {
            currentTransaction.registerModifiedTable(tableName, table);
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
                if (writeSerialized) {
                    modifiedTable.saveToFile(tableName);
                    modifiedTable.saveToSerializedFile(tableName);
                } else {
                    // Auto-commit: use coalesced persistence (write-behind)
                    modifiedTable.saveToFileDebounced(tableName);
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
        // Prompt 38 (java:S2259): toUpperCasePreservingQuotedIdentifiers can
        // return null; wrap in Optional to avoid NPE.
        String normalized = Optional.ofNullable(
                QueryParser.toUpperCasePreservingQuotedIdentifiers(query.trim())).orElse("");
        if (normalized.startsWith(SqlKeywords.SELECT)) {
            return extractTableFromSelect(normalized);
        }
        if (normalized.startsWith(SqlKeywords.INSERT_INTO)) {
            return extractTableFromInsert(normalized);
        }
        if (normalized.startsWith(SqlKeywords.UPDATE)) {
            return extractTableFromUpdate(normalized);
        }
        if (normalized.startsWith(SqlKeywords.DELETE_FROM)) {
            return extractTableFromDelete(normalized);
        }
        if (normalized.startsWith(SqlKeywords.CREATE_TABLE)) {
            return extractTableFromCreateTable(normalized);
        }
        if (normalized.startsWith(SqlKeywords.DROP_TABLE)) {
            return extractTableFromDropTable(normalized);
        }
        if (normalized.startsWith(SqlKeywords.CREATE_INDEX) || normalized.startsWith(SqlKeywords.CREATE_HASH_INDEX)
                || normalized.startsWith(SqlKeywords.CREATE_UNIQUE_INDEX) || normalized.startsWith(SqlKeywords.CREATE_UNIQUE_CLUSTERED_INDEX)) {
            return extractTableFromCreateIndex(normalized);
        }
        if (normalized.startsWith(SqlKeywords.ALTER_USER)) {
            // ALTER USER doesn't operate on a regular table, it's a system operation
            return "DIESEL_USERS";
        }
        if (normalized.startsWith(SqlKeywords.GRANT)) {
            // GRANT doesn't operate on a regular table, it's a system operation
            return "DIESEL_PRIVILEGES";
        }
        if (normalized.startsWith(SqlKeywords.SHOW_TABLES)) {
            // SHOW TABLES doesn't operate on a specific table, it's a system operation
            return "DIESEL_TABLES";
        }
        if (normalized.startsWith(SqlKeywords.SHOW_CREATE_TABLE)) {
            return extractTableFromShowCreateTable(normalized);
        }
        if (normalized.startsWith(SqlKeywords.SHOW_CREATE_INDEXES)) {
            return extractTableFromShowCreateIndexes(normalized);
        }
        throw new IllegalArgumentException("Cannot extract table name from query: unsupported query type");
    }

    private String extractTableFromShowCreateTable(String normalized) {
        String rest = normalized.substring(SqlKeywords.SHOW_CREATE_TABLE.length()).trim();
        if (rest.endsWith(";")) {
            rest = rest.substring(0, rest.length() - 1).trim();
        }
        return firstIdentifier(rest.split("\\s+")[0]);
    }

    private String extractTableFromShowCreateIndexes(String normalized) {
        String rest = normalized.substring(SqlKeywords.SHOW_CREATE_INDEXES.length()).trim();
        if (rest.endsWith(";")) {
            rest = rest.substring(0, rest.length() - 1).trim();
        }
        return firstIdentifier(rest.split("\\s+")[0]);
    }

    private String extractTableFromSelect(String normalized) {
        String[] parts = normalized.split(ErrorMessages.FROM_PATTERN, 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid SELECT format");
        }
        return firstIdentifier(parts[1].split("(?i)(INNER JOIN|WHERE)\\s")[0].trim().split("\\s+")[0]);
    }

    private String extractTableFromInsert(String normalized) {
        String[] parts = normalized.split("(?i)INSERT INTO\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid INSERT format");
        }
        return firstIdentifier(parts[1].split("\\s+|\\(")[0]);
    }

    private String extractTableFromUpdate(String normalized) {
        String[] parts = normalized.split("(?i)UPDATE\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid UPDATE format");
        }
        return firstIdentifier(parts[1].split("\\s+")[0]);
    }

    private String extractTableFromDelete(String normalized) {
        String[] parts = normalized.split(ErrorMessages.FROM_PATTERN, 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid DELETE format");
        }
        return firstIdentifier(parts[1].split("(?i)WHERE\\s*", 2)[0]);
    }

    private String extractTableFromCreateTable(String normalized) {
        String[] parts = normalized.split("(?i)CREATE TABLE\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE TABLE format");
        }
        return firstIdentifier(parts[1].split("\\s+")[0]);
    }

    private String extractTableFromDropTable(String normalized) {
        String[] parts = normalized.split("(?i)DROP TABLE\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid DROP TABLE format");
        }
        String token = parts[1].split("\\s+")[0];
        if (token.endsWith(";")) {
            token = token.substring(0, token.length() - 1);
        }
        return firstIdentifier(token);
    }

    private String extractTableFromCreateIndex(String normalized) {
        String[] parts = normalized.split("(?i)ON\\s+", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Cannot extract table name from query: invalid CREATE INDEX format");
        }
        return firstIdentifier(parts[1].split("\\s+")[0]);
    }

    /**
     * Extracts all table names referenced in a query (simplified implementation).
     * For parallel execution detection, we identify the main tables involved.
     * 
     * @param query the SQL query
     * @return set of table names referenced in the query
     */
    private Set<String> extractAllTableNames(String query) {
        Set<String> tables = new HashSet<>();
        // Prompt 38 (java:S2259): toUpperCasePreservingQuotedIdentifiers can
        // return null; wrap in Optional to avoid NPE.
        String normalized = Optional.ofNullable(
                QueryParser.toUpperCasePreservingQuotedIdentifiers(query.trim())).orElse("");
        
        if (normalized.startsWith(SqlKeywords.SELECT)) {
            extractSelectTables(normalized, tables);
        } else if (normalized.startsWith(SqlKeywords.INSERT_INTO)) {
            extractInsertTables(normalized, tables);
        } else if (normalized.startsWith(SqlKeywords.UPDATE)) {
            extractUpdateTables(normalized, tables);
        } else if (normalized.startsWith(SqlKeywords.DELETE_FROM)) {
            extractDeleteTables(normalized, tables);
        } else if (normalized.startsWith(SqlKeywords.CREATE_TABLE)) {
            extractCreateTableTables(normalized, tables);
        } else if (normalized.startsWith(SqlKeywords.CREATE_INDEX) || 
                 normalized.startsWith(SqlKeywords.CREATE_HASH_INDEX) ||
                 normalized.startsWith(SqlKeywords.CREATE_UNIQUE_INDEX) ||
                 normalized.startsWith(SqlKeywords.CREATE_UNIQUE_CLUSTERED_INDEX)) {
            extractCreateIndexTables(normalized, tables);
        } else {
            fallbackSingleTableExtraction(query, tables);
        }
        
        return tables;
    }

    private void fallbackSingleTableExtraction(String query, Set<String> tables) {
        try {
            String singleTable = extractTableName(query);
            if (singleTable != null && !singleTable.isEmpty()) {
                tables.add(singleTable);
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINER, "Fallback table extraction failed", e);
        }
    }

    private void extractSelectTables(String normalized, Set<String> tables) {
        String[] fromParts = normalized.split(ErrorMessages.FROM_PATTERN, 2);
        if (fromParts.length < 2) {
            return;
        }
        String afterFrom = fromParts[1];
        String[] joinSplit = afterFrom.split("(?i)(INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN|CROSS JOIN|WHERE)\\s+", 2);
        String firstTablePart = joinSplit[0].trim();
        if (!firstTablePart.isEmpty()) {
            String firstTable = firstIdentifier(firstTablePart.split("\\s+")[0]);
            tables.add(firstTable);
        }
        if (joinSplit.length > 1) {
            extractJoinTables(afterFrom, tables);
        }
    }

    private void extractJoinTables(String remaining, Set<String> tables) {
        int pos = 0;
        while ((pos = remaining.toUpperCase().indexOf(" JOIN", pos)) != -1) {
            int joinStart = pos;
            while (joinStart > 0 && !Character.isWhitespace(remaining.charAt(joinStart - 1))) {
                joinStart--;
            }
            int tableEnd = joinStart;
            while (tableEnd < remaining.length() && 
                   !Character.isWhitespace(remaining.charAt(tableEnd)) &&
                   remaining.charAt(tableEnd) != '(' &&
                   remaining.charAt(tableEnd) != ')') {
                tableEnd++;
            }
            String tableName = remaining.substring(joinStart, tableEnd).trim();
            if (!tableName.isEmpty()) {
                String cleanTableName = firstIdentifier(tableName.split("\\s+")[0]);
                tables.add(cleanTableName);
            }
            pos = tableEnd;
        }
    }

    private void extractInsertTables(String normalized, Set<String> tables) {
        String[] parts = normalized.split("(?i)INSERT INTO\\s+", 2);
        if (parts.length >= 2) {
            String tablePart = parts[1].split("\\s+|\\(")[0];
            tables.add(firstIdentifier(tablePart));
        }
    }

    private void extractUpdateTables(String normalized, Set<String> tables) {
        String[] parts = normalized.split("(?i)UPDATE\\s+", 2);
        if (parts.length >= 2) {
            String tablePart = parts[1].split("\\s+")[0];
            tables.add(firstIdentifier(tablePart));
        }
    }

    private void extractDeleteTables(String normalized, Set<String> tables) {
        String[] parts = normalized.split(ErrorMessages.FROM_PATTERN, 2);
        if (parts.length >= 2) {
            String tablePart = parts[1].split("(?i)WHERE\\s*", 2)[0].trim();
            tables.add(firstIdentifier(tablePart.split("\\s+")[0]));
        }
    }

    private void extractCreateTableTables(String normalized, Set<String> tables) {
        String[] parts = normalized.split("(?i)CREATE TABLE\\s+", 2);
        if (parts.length >= 2) {
            String tablePart = parts[1].split("\\s+")[0];
            tables.add(firstIdentifier(tablePart));
        }
    }

    private void extractCreateIndexTables(String normalized, Set<String> tables) {
        String[] parts = normalized.split("(?i)ON\\s+", 2);
        if (parts.length >= 2) {
            String tablePart = parts[1].split("\\s+")[0];
            // Remove potential parentheses or other modifiers
            tablePart = tablePart.split("[\\(\\)]")[0];
            tables.add(firstIdentifier(tablePart));
        }
    }

    /**
     * Executes a batch of queries, running independent queries in parallel.
     * Queries are considered independent if they don't share any tables.
     * 
     * @param queries       the list of SQL queries to execute
     * @param transactionId the caller's transaction id, or null for auto-commit
     * @return list of query results in the same order as input queries
     */
    public List<Object> executeBatch(List<String> queries, UUID transactionId) {
        if (queries == null || queries.isEmpty()) {
            return Collections.emptyList();
        }
        if (queries.size() == 1) {
            return Collections.singletonList(executeQuery(queries.get(0), transactionId));
        }
        if (transactionId != null && isInTransaction(transactionId)) {
            return executeBatchSequential(queries, transactionId);
        }
        return executeBatchParallel(queries, transactionId);
    }

    private List<Object> executeBatchSequential(List<String> queries, UUID transactionId) {
        List<Object> results = new ArrayList<>(queries.size());
        for (String query : queries) {
            results.add(executeQuery(query, transactionId));
        }
        return results;
    }

    private List<Object> executeBatchParallel(List<String> queries, UUID transactionId) {
        List<Set<String>> queryTables = new ArrayList<>(queries.size());
        for (String query : queries) {
            queryTables.add(extractAllTableNames(query));
        }
        boolean[] dependencies = buildDependencyGraph(queries.size(), queryTables);
        List<List<Integer>> batches = groupIntoBatches(queries.size(), dependencies);

        List<Object> results = new ArrayList<>(Collections.nCopies(queries.size(), null));
        executeBatchesInParallel(queries, transactionId, batches, results);

        for (Object result : results) {
            if (result instanceof Exception e) {
                throw new QuerySyntaxException("", e.getMessage());
            }
        }

        @SuppressWarnings("unchecked")
        List<Object> typedResults = (List<Object>) results;
        return typedResults;
    }

    private boolean[] buildDependencyGraph(int size, List<Set<String>> queryTables) {
        boolean[] dependencies = new boolean[size * size];
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                if (shareTables(queryTables.get(i), queryTables.get(j))) {
                    dependencies[i * size + j] = true;
                    dependencies[j * size + i] = true;
                }
            }
        }
        return dependencies;
    }

    private boolean shareTables(Set<String> tables1, Set<String> tables2) {
        for (String table : tables1) {
            if (tables2.contains(table)) {
                return true;
            }
        }
        return false;
    }

    private List<List<Integer>> groupIntoBatches(int size, boolean[] dependencies) {
        List<List<Integer>> batches = new ArrayList<>();
        boolean[] assigned = new boolean[size];
        
        IntStream.range(0, size).filter(i -> !assigned[i]).forEach(i -> {
            List<Integer> currentBatch = new ArrayList<>();
            currentBatch.add(i);
            assigned[i] = true;
            for (int j = i + 1; j < size; j++) {
                if (assigned[j]) continue;
                if (canJoinBatch(currentBatch, j, size, dependencies)) {
                    currentBatch.add(j);
                    assigned[j] = true;
                }
            }
            batches.add(currentBatch);
        });
        return batches;
    }

    private boolean canJoinBatch(List<Integer> currentBatch, int j, int size, boolean[] dependencies) {
        for (Integer batchQueryIdx : currentBatch) {
            if (dependencies[batchQueryIdx * size + j]) {
                return false;
            }
        }
        return true;
    }

    private void executeBatchesInParallel(List<String> queries, UUID transactionId,
                                          List<List<Integer>> batches, List<Object> results) {
        try (ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(batches.size(), Runtime.getRuntime().availableProcessors()))) {
            executeBatchQueries(batches, queries, results, executor, transactionId);
        }
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
     * Package-private: returns the table if present, or null.
     * Used by recovery sinks to skip unknown tables with warnings instead of throwing.
     *
     * @param tableName the table name
     * @return the registered table, or null if not found
     */
    Table findTable(String tableName) {
        return tables.get(tableName);
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
        if (tableName != null && tableName.toUpperCase(Locale.ROOT).startsWith("DIESEL_")) {
            throw new IllegalArgumentException(
                    "Cannot drop system table " + tableName + ": the diesel_ prefix is reserved");
        }
        if (activeAlters.containsKey(tableName)) {
            throw new DieselException(
                    "Cannot drop table " + tableName + ": ALTER COPY is in progress (cancel it first)");
        }
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
        
        // Update catalog
        try {
            catalog.dropTableSchema(tableName);
        } catch (IllegalStateException e) {
            LOGGER.log(Level.WARNING, "Failed to update catalog for dropped table " + tableName, e);
        }
    }

    /**
     * Loads the catalog from page 0.
     * This should be called before loadTablesFromDisk.
     */
    public void loadCatalog() {
        // The catalog will be loaded when setCatalogPageManager is called
        LOGGER.log(Level.INFO, "Catalog loaded from page 0");
    }

    /**
     * Writes every registered table to disk as a serialized {@code .table}
     * file in the data directory.
     *
     * @see Table#saveToSerializedFile
     */
    public void saveTablesToDisk() {
        Table.flushPendingPersistsForDataDir(this.dataDir);

        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            entry.getValue().saveToSerializedFile(entry.getKey());
        }
        LOGGER.log(Level.INFO, "Saved {0} tables to disk", tables.size());

        // Wait for any in-flight persist to complete (e.g., from PersistFlusher daemon)
        // to prevent @TempDir deletion races
        awaitPersistIdle();
    }

    /**
     * Bulk-load rows from a CSV file directly into the table, bypassing the
     * SQL parser (TESTS-9). Format: header row with column names in schema
     * case, then one row per line, no quoting. Used by tests for fast fixture
     * setup.
     *
     * <p>Because the CSV carries explicit values for sequence-based primary
     * key columns, the table's sequences are afterwards seeded to at least
     * {@code maxLoaded + increment}, so subsequent SQL INSERTs continue past
     * the loaded rows instead of colliding with them.
     */
    void bulkLoadCSV(String tableName, Path csvPath) throws IOException {
        Table table = getTable(tableName);
        Map<String, Long> maxBySequenceColumn = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IOException("Fixture file is empty: " + csvPath);
            }
            String[] cols = header.split(",", -1);
            int rowNum = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] values = line.split(",", -1);
                table.insertFromCsv(cols, values, rowNum);
                for (int c = 0; c < cols.length && c < values.length; c++) {
                    if (table.getSequences().containsKey(cols[c])) {
                        maxBySequenceColumn.merge(cols[c], Long.parseLong(values[c]), Math::max);
                    }
                }
                rowNum++;
            }
        }
        for (Map.Entry<String, Sequence> e : table.getSequences().entrySet()) {
            Long max = maxBySequenceColumn.get(e.getKey());
            if (max != null) {
                // seedAtLeast(max + increment) leaves the NEXT nextValue() at
                // max + increment; seeding with the raw max would re-issue it.
                e.getValue().seedAtLeast(max + e.getValue().getIncrement());
            }
        }
        table.rebuildAllIndexes();
    }

    /**
     * Waits for all pending persists to complete; ensures no persist is in-flight.
     */
    private void awaitPersistIdle() {
        for (Table table : tables.values()) {
            // Acquire and release write lock to wait for any in-flight saveToFile
            table.getTableLock().writeLock().lock();
            try {
                // Hold briefly to let any in-flight save complete
            } finally {
                table.getTableLock().writeLock().unlock();
            }
        }
    }

    /**
     * Loads every {@code .table} file found in the data directory back into
     * the shared table map, skipping corrupt files with a warning.
     *
     * @see Table#loadFromFile
     */
    public void loadTablesFromDisk() {
        // Before any disk read, flush pending writes for read-your-writes guarantee
        Table.flushPendingPersistsForDataDir(this.dataDir);

        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(ErrorMessages.TABLE_EXTENSION));
        if (files == null) {
            return;
        }
        for (File file : files) {
            String tableName = file.getName().substring(0, file.getName().length() - ErrorMessages.TABLE_EXTENSION.length());
            try {
                Table table = Table.loadFromFile(this, tableName);
                if (table != null) {
                    Table existing = tables.get(tableName);
                    if (existing != null && table.getSequences().isEmpty() && !existing.getSequences().isEmpty()) {
                        table.getSequences().putAll(existing.getSequences());
                        LOGGER.log(Level.FINE, "Preserved {0} sequences from in-memory table {1}",
                                new Object[]{existing.getSequences().size(), tableName});
                    }
                    tables.put(tableName, table);
                    LOGGER.log(Level.INFO, "Loaded table {0} from disk with {1} rows",
                            new Object[]{tableName, table.getLiveRowCount()});
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to load table {0} from disk, skipping: {1}",
                        new Object[]{tableName, e.getMessage()});
            }
        }
        queryCache.invalidateAll();
    }

    private void deleteTableFiles(String tableName) {
        Table.flushPendingPersistsForDataDir(this.dataDir);

        String[] suffixes = {
                ".csv", ".csv.zst", ".csv.lz4", ".csv.snappy",
                ".tsv", ".tsv.zst", ".tsv.lz4", ".tsv.snappy",
                ".jsonl", ".jsonl.zst", ".jsonl.lz4", ".jsonl.snappy",
                ".idx", ".idx.tmp",
                ".avro", ".avro.tmp", ".avro.bak", ".asi",
                ErrorMessages.TABLE_EXTENSION
        };
        for (String extension : suffixes) {
            try {
                Files.deleteIfExists(Path.of(dataDir, tableName + extension));
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete {0} file for table {1}: {2}",
                        new Object[]{extension, tableName, e.getMessage()});
            }
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
        Transaction transaction = new Transaction(this, isolationLevel);
        UUID transactionId = transaction.getTransactionId();
        activeTransactions.put(transactionId, transaction);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            transaction.snapshotTable(entry.getKey(), entry.getValue());
        }
        return transactionId;
    }

    /**
     * Flushes all pending writes and closes the database.
     * Equivalent to {@code saveTablesToDisk()}. Also stops the auto-vacuum
     * daemon and unregisters its JMX MBean.
     */
    public void close() {
        // Cancel any online ALTER COPY first (prompt4 #38): rollback semantics
        // leave the tables in their pre-ALTER state, so the final persist below
        // writes the old schema/data.
        java.util.concurrent.ExecutorService alters = alterCopyExecutor;
        if (alters != null) {
            for (AlterTableCopyAlgorithm algorithm : activeAlters.values()) {
                algorithm.requestCancel();
            }
            alters.shutdownNow();
            try {
                if (!alters.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.log(Level.WARNING, "ALTER COPY workers did not terminate within 5s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            activeAlters.clear();
        }

        // Stop the SQL audit consumer first (prompt4 #33): its final drain
        // appends any queued records to DIESEL_AUDIT_LOG before the tables
        // themselves are persisted below.
        AuditLogger audit = auditLogger;
        if (audit != null) {
            audit.close();
        }

        // Stop page flusher first (final flush)
        stopPageFlusher();
        
        // Close WAL subsystem to ensure all commits are flushed
        if (recoveryManager != null) {
            recoveryManager.close();
        }
        if (asyncWALWriter != null) {
            try {
                asyncWALWriter.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close WAL writer: " + e.getMessage(), e);
            }
        }
        if (walScheduler != null) {
            walScheduler.shutdown();
            try {
                if (!walScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    walScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                walScheduler.shutdownNow();
            }
        }
        
        vacuumManager.stop();
        saveTablesToDisk();
    }

    /**
     * Crash-simulation close for recovery tests (prompt4 #19): drains and
     * closes the WAL writer (every queued append becomes durable, mirroring a
     * kill after the log writes completed) and discards unflushed page frames,
     * but does <b>not</b> persist tables, stop vacuum, or run any clean
     * shutdown bookkeeping. The successor {@code Database} instance sees
     * exactly what reached the disk before the simulated crash. Mirrors
     * {@code PageManager.closeDiscardingDirty}.
     */
    void closeForCrashSimulation() {
        // Stop page flusher without final flush (crash simulation expects dirty pages)
        stopPageFlusherWithoutFinalFlush();
        
        if (recoveryManager != null) {
            recoveryManager.close();
        }
        // Drain queued DML appends first: shutdown() lets submitted tasks
        // reach the WAL writer, then the writer itself is flushed and closed.
        if (walScheduler != null) {
            walScheduler.shutdown();
            try {
                if (!walScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    walScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                walScheduler.shutdownNow();
            }
        }
        if (asyncWALWriter != null) {
            try {
                asyncWALWriter.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close WAL writer during crash simulation: " + e.getMessage(), e);
            }
        }
        if (pageManager != null) {
            try {
                pageManager.closeDiscardingDirty();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close page manager during crash simulation: " + e.getMessage(), e);
            }
        }
    }
    
    /**
     * Executes a batch of queries in parallel using the given executor.
     */
    private void executeBatchQueries(List<List<Integer>> batches, List<String> queries, 
                                   List<Object> results, ExecutorService executor, UUID transactionId) {
        try {
            List<Future<?>> futures = new ArrayList<>();
            
            for (List<Integer> batch : batches) {
                Future<?> future = executor.submit(() -> {
                    for (Integer queryIdx : batch) {
                        Object result = executeBatchQuery(queries.get(queryIdx), transactionId);
                        results.set(queryIdx, result);
                    }
                });
                futures.add(future);
            }
            
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuerySyntaxException("", "Batch query execution interrupted");
        } catch (ExecutionException e) {
            throw new QuerySyntaxException("", "Error executing batch queries");
        } finally {
            executor.shutdownNow();
        }
    }
    
    /**
     * Executes a single query in a batch, handling exceptions.
     */
    private Object executeBatchQuery(String query, UUID transactionId) {
        try {
            return executeQuery(query, transactionId);
        } catch (Exception e) {
            // Store the exception as the result for this query
            return e;
        }
    }
    
    /**
     * Returns the MVCC transaction status tracker.
     */
    public TxStatusTracker getTxStatusTracker() {
        return txStatusTracker;
    }

    /**
     * Returns this database's vacuum manager (prompt4.md step 3), which
     * reclaims dead row versions on demand and via its auto-vacuum schedule.
     *
     * @return the vacuum manager, never null
     */
    public VacuumManager getVacuumManager() {
        return vacuumManager;
    }

    /**
     * Returns this database's SERIALIZABLE SSI conflict detector (prompt4.md #5),
     * which tracks read/write sets for SERIALIZABLE transactions and detects
     * write-write and read-write conflicts.
     *
     * @return the conflict detector, never null
     */
    public ConflictDetector getConflictDetector() {
        return conflictDetector;
    }

    /**
     * Returns a live view of the registered tables, used by
     * {@link VacuumManager#vacuumAll()}.
     *
     * @return the registered tables
     */
    Collection<Table> getTables() {
        return tables.values();
    }

    /**
     * Computes the vacuum horizon: the minimum snapshot CSN of every active
     * non-batch transaction, or {@link Long#MAX_VALUE} when none is active. A
     * row whose deleting transaction committed at or below this CSN is
     * invisible to all current and future readers and may be reclaimed.
     *
     * <p>Commit CSNs (not transaction ids) are used because commit order does
     * not follow txid order: a transaction with a higher txid may commit first
     * and receive the lower CSN.
     *
     * @return the vacuum horizon CSN
     */
    long computeVacuumHorizonCsn() {
        long horizon = Long.MAX_VALUE;
        for (Transaction transaction : activeTransactions.values()) {
            if (transaction.isActive() && !transaction.isBatchMode()) {
                horizon = Math.min(horizon, transaction.getSnapshotCsn());
            }
        }
        return horizon;
    }

    /**
     * Returns the current transaction (MVCC support).
     */
    public Transaction getCurrentTransaction() {
        if (activeTransactions.size() == 1) {
            return activeTransactions.values().iterator().next();
        }
        return null;
    }

    /**
     * Returns the current transaction id (MVCC support).
     */
    public long getCurrentTxid() {
        Transaction current = getCurrentTransaction();
        return current != null ? current.getTxid() : -1;
    }
}
