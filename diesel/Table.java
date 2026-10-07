package diesel;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.zip.CRC32;
import java.util.stream.IntStream;
import diesel.storage.RowStorage;
import diesel.storage.InMemoryRowStorage;
import diesel.storage.AbstractRowStorage;
import diesel.storage.AtomicFileWriter;
import diesel.storage.StorageFactory;
import diesel.storage.StorageType;

/**
 * Contract implemented by every index (secondary and clustered) that maps
 * column keys to row indexes.
 */
interface Index {
    /**
     * Associates {@code key} with {@code rowIndex}.
     *
     * @param key      the index key, must not be null
     * @param rowIndex the row index to associate with the key
     */
    void insert(Object key, int rowIndex);

    /**
     * Removes the association between {@code key} and {@code rowIndex}.
     *
     * @param key      the index key
     * @param rowIndex the row index to remove
     */
    void remove(Object key, int rowIndex);

    /**
     * Returns every row index that holds the given key.
     *
     * @param key the index key
     * @return the list of matching row indexes, possibly empty
     */
    List<Integer> search(Object key);

    /**
     * Returns the Java type of the indexed keys.
     *
     * @return the key type of the indexed column
     */
    Class<?> getKeyType();

    /**
     * Returns the columns this index covers beyond the indexed column,
     * or an empty list when no extra columns are stored.
     */
    default List<String> getCoversColumns() {
        return Collections.emptyList();
    }

    /**
     * Returns {@code true} when this index stores all of the given columns
     * and can serve as a covering index for them.
     */
    @SuppressWarnings("unused")
    default boolean coversColumns(Set<String> columns) {
        return false;
    }

    /**
     * Returns the covered column values for the given row index, or
     * {@code null} when the index does not store row data.
     */
    @SuppressWarnings("unused")
    @Nullable
    default Map<String, Object> getCoveredValues(int rowIndex) {
        return null;
    }
}

/**
 * An in-memory table: schema (columns, types, primary key), the row storage,
 * secondary indexes, an optional clustered index and the sequences used to
 * auto-generate values. Rows are protected by per-row read/write locks.
 *
 * <p>Tables are serializable: the full state (schema, rows, index definitions
 * and sequences) is persisted to a {@code .table} file and rebuilt on load.
 * A primary key column automatically becomes a unique clustered index.
 *
 * <p>Example:
 * <pre>{@code
 * Table table = new Table(database, "USERS", columns, columnTypes, "ID", sequences);
 * table.addRow(row);
 * table.saveToFile("USERS");
 * }</pre>
 *
 * @see Database
 * @see BTreeIndex
 * @see HashIndex
 * @see UniqueIndex
 * @see BTreeClusteredIndex
 */
class Table implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int CURRENT_FORMAT_VERSION = 3;
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final String name;
    private transient List<String> columns;
    private transient Map<String, Class<?>> columnTypes;
    private final String primaryKeyColumn;
    private transient List<Map<String, Object>> rows;
    private transient RowStorage storage;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, Index> indexes;
    private transient Map<String, Sequence> sequences;
    private transient Map<String, String> indexDefinitions = new ConcurrentHashMap<>();
    private transient Map<String, List<String>> coverColumnDefinitions = new ConcurrentHashMap<>();
    private boolean isFileInitialized;
    private boolean hasClusteredIndex;
    private String clusteredIndexColumn;
    private transient BTreeClusteredIndex clusteredIndex;
    private transient Database database;
    private transient ReentrantReadWriteLock tableLock;
    private int formatVersion = CURRENT_FORMAT_VERSION;

    /**
     * Table statistics used by the optimizer (see {@link #getStatistics()} and
     * {@link #analyze()}). {@code rowCount} is kept exactly in sync with the
     * row list (O(1) maintenance on INSERT/DELETE); {@code avgRowSizeBytes}
     * starts as a schema-only estimate and is refined by the full O(rows)
     * pass that runs asynchronously after mutations and synchronously on
     * {@code ANALYZE TABLE}, together with {@code lastAnalyzedMillis}.
     */
    private long rowCount;
    private long avgRowSizeBytes;
    private long lastAnalyzedMillis;
    /**
     * Monotonically increasing version counter, incremented on every DML
     * mutation. Used for optimistic concurrency control at COMMIT time.
     */
    private transient AtomicLong version = new AtomicLong(0);
    private transient Object statsLock = new Object();
    private transient volatile boolean statsDirty;
    private transient volatile boolean statsRefreshScheduled;

    /** When true, index insert/update operations are skipped (bulk-load mode). */
    private transient volatile boolean indicesDisabled;

    /**
     * Table-level read/write lock. Writers (INSERT, DELETE, CREATE INDEX swap
     * phase) hold the write lock; readers (SELECT) hold the read lock. During
     * clustered index creation, the expensive sort + bulk-load phases run
     * without holding any lock; only the final atomic swap acquires the write
     * lock, minimising read disruption.
     */
    // tableLock is initialized in constructor

    /** Queued index operations when deferred mode is active; null when not deferring. */
    private transient List<Runnable> deferredIndexOps;

    /**
     * True when an auto-commit mutation has been persisted only in memory and the
     * data file is still stale (Prompt: coalesced persistence).
     */
    private transient volatile boolean persistDirty;

    /** Number of auto-commit mutations folded into the pending write. */
    private transient volatile int pendingPersistCount;

    /** True when clustered index was restored from serialized data. */
    private transient boolean restoredClustered;

    /** Tracks positions of tombstoned (logically deleted) rows. Physical removal happens only during {@link #compact()}. */
    private transient BitSet deletedRows;

    /**
     * MVCC metadata per row: maps row index to version information.
     * Null/empty means no MVCC versioning (bootstrap or auto-commit path).
     */
    private transient ConcurrentHashMap<Integer, RowVersionMeta> rowVersions;

    /**
     * Row indexes with a pending (uncommitted) MVCC change. Idempotent
     * add/remove set (a row can accrue several undo records and be rolled back
     * more than once); guards the Avro storage pushdown, which reads only the
     * physical rows and would otherwise miss shadow pre-images.
     */
    private transient java.util.Set<Integer> uncommittedMvccRows = ConcurrentHashMap.newKeySet();

    /** Fraction of rows that must be tombstoned before automatic compaction triggers. */
    private static final double COMPACT_THRESHOLD = 0.3;

    /**
     * Daemon scheduler for the asynchronous statistics refresh. Daemon threads
     * never keep the JVM alive, so the refresh never blocks Maven/JUnit exit.
     * Mutations coalesce into a single refresh because {@code markStatsDirty}
     * only schedules while no refresh is already pending.
     */
    private static final ScheduledExecutorService STATS_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "table-stats-refresh");
                thread.setDaemon(true);
                return thread;
            });

    /** Delay before a dirty statistics refresh runs, coalescing insert bursts. */
    private static final long STATS_REFRESH_DELAY_MILLIS = 150;

    /**
     * Shared ForkJoinPool for parallel index-build phases (sorting, bulk-load).
     * Uses {@link Runtime#availableProcessors()} parallelism. Daemon threads
     * so it does not block JVM exit.
     */
    private static final ForkJoinPool INDEX_BUILD_POOL = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("diesel-index-build-" + t.getPoolIndex());
                return t;
            },
            null, true);

    /** Row count threshold above which parallel sort is used during index creation. */
    private static final int PARALLEL_SORT_THRESHOLD = 10_000;

    private static final DateTimeFormatter STATS_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Creates a table with the given schema. When {@code primaryKeyColumn} is
     * not null it must be part of the schema, and a unique clustered index is
     * built over it.
     *
     * @param database         the owning database (for data-dir resolution)
     * @param name             the table name
     * @param columns          the ordered list of column names
     * @param columnTypes      the column name to type mapping
     * @param primaryKeyColumn the primary key column, or null for none
     * @param sequences        the sequences usable by this table, or null
     * @throws IllegalArgumentException if the schema is invalid or the primary
     *                                  key column is missing
     */
    public Table(Database database, String name, List<String> columns, Map<String, Class<?>> columnTypes, String primaryKeyColumn, Map<String, Sequence> sequences) {
        this.database = database;
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.columnTypes.putAll(columnTypes);
        this.primaryKeyColumn = primaryKeyColumn;
        this.rows = new ArrayList<>();
        this.storage = StorageFactory.create(
                System.getProperty("diesel.storage.type",
                        getConfigProperty("storage.type", "in_memory")),
                name, columns, columnTypes);
        if (this.storage instanceof AbstractRowStorage ars) {
            String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
            ars.setDataDir(dir);
            if (primaryKeyColumn != null) {
                ars.setPrimaryKeyColumn(primaryKeyColumn);
            }
        }
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.sequences = sequences != null ? new ConcurrentHashMap<>(sequences) : new ConcurrentHashMap<>();
        this.tableLock = new ReentrantReadWriteLock();
        this.isFileInitialized = false;
        this.hasClusteredIndex = false;
        this.clusteredIndexColumn = null;
        this.clusteredIndex = null;
        this.deletedRows = new BitSet();
        this.tableLock = new ReentrantReadWriteLock();
        this.rowCount = 0;
        this.avgRowSizeBytes = estimateAverageRowSizeBytes();
        this.lastAnalyzedMillis = 0;

        validateSchema(columns, columnTypes);

        if (primaryKeyColumn != null) {
            if (!this.columnTypes.containsKey(primaryKeyColumn)) {
                throw new IllegalArgumentException("Primary key column " + primaryKeyColumn + ErrorMessages.DOES_NOT_EXIST);
            }
            createUniqueClusteredIndex(primaryKeyColumn);
        }

        LOGGER.log(Level.INFO, "Created table: {0} with columns {1}, types {2}, primary key: {3}, sequences: {4}",
                new Object[]{name, columns, columnTypes, primaryKeyColumn, this.sequences.keySet()});
    }

    /**
     * Returns the owning database, or null when the table was deserialized
     * without one.
     *
     * @return the owning database, possibly null
     */
    public Database getDatabase() {
        return database;
    }

    /** Returns the table-level read-write lock for MVCC operations. */
    public ReentrantReadWriteLock getTableLock() {
        return tableLock;
    }

    /**
     * Attaches a database to this table, used to restore the transient
     * reference after deserialization.
     *
     * @param database the database to attach
     */
    public void attachDatabase(Database database) {
        this.database = database;
    }

    /**
     * Private constructor for {@link #copyForTransaction()} — skips schema
     * validation and clustered index creation (indexes are rebuilt at the end).
     */
    private Table(Database database, String name, List<String> columns,
                  Map<String, Class<?>> columnTypes, String primaryKeyColumn) {
        this.database = database;
        this.name = name;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.primaryKeyColumn = primaryKeyColumn;
        this.rows = new ArrayList<>();
        this.storage = new InMemoryRowStorage(name, columns, columnTypes);
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.sequences = new ConcurrentHashMap<>();
        this.deletedRows = new BitSet();
        this.rowVersions = new ConcurrentHashMap<>();
        this.tableLock = new ReentrantReadWriteLock();
        this.statsLock = new Object();
        this.statsDirty = false;
        this.statsRefreshScheduled = false;
        this.indicesDisabled = false;
        this.deferredIndexOps = null;
        this.persistDirty = false;
        this.pendingPersistCount = 0;
        this.formatVersion = CURRENT_FORMAT_VERSION;
        this.version = new AtomicLong(0);
    }

    /**
     * Creates an independent copy of this table for use inside a transaction.
     * Copies rows and metadata directly, avoiding ObjectOutputStream/ObjectInputStream
     * overhead. Indexes are rebuilt from scratch (same cost as deserialization).
     *
     * <p>Row values are shallow-copied ({@code new HashMap<>(row)}) because
     * DieselDB stores only immutable types (Integer, Long, String, BigDecimal,
     * LocalDate, LocalDateTime, UUID, Boolean).
     */
    public Table copyForTransaction() {
        Table copy = new Table(this.database, this.name,
                new ArrayList<>(this.columns),
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER) {{ putAll(columnTypes); }},
                this.primaryKeyColumn);

        // Deep-copy rows: new list, each row Map shallow-copied.
        for (Map<String, Object> row : this.rows) {
            copy.rows.add(new HashMap<>(row));
        }
        // Mirror the copied rows into the copy's (fresh) storage with a single
        // wholesale replacement instead of per-row insertAt mirroring, so the
        // copy cost stays O(n) and the storage mirror aligns with copy.rows.
        // The copy's storage is a plain in-memory backend without a
        // DelimitedIndexManager, so the bulk window is a no-op there; the
        // engine-level indexes are rebuilt once below via rebuildAllIndexes().
        copy.storage.beginBulkUpdate();
        try {
            copy.storage.setRows(copy.rows);
        } finally {
            copy.storage.endBulkUpdate();
        }

        // Deep-copy deletedRows BitSet.
        copy.deletedRows = this.deletedRows != null ? (BitSet) this.deletedRows.clone() : new BitSet();

        // Share sequences — Sequence.nextValue() is synchronized.
        copy.sequences = new ConcurrentHashMap<>(this.sequences);

        // Copy index metadata.
        copy.indexDefinitions.putAll(this.indexDefinitions);
        copy.coverColumnDefinitions.putAll(this.coverColumnDefinitions);

        // Copy clustered index metadata.
        copy.hasClusteredIndex = this.hasClusteredIndex;
        copy.clusteredIndexColumn = this.clusteredIndexColumn;

        // Copy stats.
        copy.rowCount = this.rowCount;
        copy.avgRowSizeBytes = this.avgRowSizeBytes;
        copy.lastAnalyzedMillis = this.lastAnalyzedMillis;

        copy.isFileInitialized = this.isFileInitialized;
        copy.formatVersion = this.formatVersion;

        // Snapshot the source version at copy time.
        copy.version = new AtomicLong(this.version.get());

        // Rebuild all indexes from the copied rows.
        copy.rebuildAllIndexes();

        return copy;
    }

    private void validateSchema(List<String> columns, Map<String, Class<?>> columnTypes) {
        for (String column : columns) {
            if (!columnTypes.containsKey(column)) {
                LOGGER.log(Level.SEVERE, "Schema validation failed: Column {0} missing in columnTypes {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException(ErrorMessages.COLUMN_PREFIX + column + " missing in columnTypes");
            }
        }
        for (String column : columnTypes.keySet()) {
            if (!columns.contains(column)) {
                LOGGER.log(Level.WARNING, "Column {0} in columnTypes but not in columns list {1}",
                        new Object[]{column, columns});
            }
        }
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the secondary indexes keyed by column name.
     *
     * @return the secondary index map
     */
    public Map<String, Index> getIndexes() {
        return indexes;
    }

    /**
     * Returns whether the table has been written to disk at least once.
     *
     * @return the file-initialized flag
     */
    public boolean isFileInitialized() {
        return isFileInitialized;
    }

    /**
     * Sets the file-initialized flag.
     *
     * @param fileInitialized the new flag value
     */
    public void setFileInitialized(boolean fileInitialized) {
        isFileInitialized = fileInitialized;
    }

    /**
     * Returns the read/write lock guarding the given row, creating it on
     * demand.
     *
     * @param rowIndex the row index
     * @return the row's lock
     */
    public ReentrantReadWriteLock getRowLock(int rowIndex) {
        return rowLocks.computeIfAbsent(rowIndex, k -> new ReentrantReadWriteLock());
    }

    /**
     * Returns the sequences registered for this table.
     *
     * @return the sequence map
     */
    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    /**
     * Builds a B-tree secondary index over the column, failing when the
     * column does not exist.
     *
     * @param columnName the column to index
     * @throws IllegalArgumentException if the column does not exist
     */
    public void createBTreeIndex(String columnName) {
        createSecondaryIndex(columnName, ErrorMessages.INDEX_BTREE, BTreeIndex::new, false);
        LOGGER.log(Level.INFO, "Created B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    /**
     * Builds a hash secondary index over the column, failing when the column
     * does not exist.
     *
     * @param columnName the column to index
     * @throws IllegalArgumentException if the column does not exist
     */
    public void createHashIndex(String columnName) {
        createSecondaryIndex(columnName, ErrorMessages.INDEX_HASH, HashIndex::new, false);
        LOGGER.log(Level.INFO, "Created hash index on column {0} for table {1}", new Object[]{columnName, name});
    }

    /**
     * Builds a unique secondary index over the column, failing on duplicate
     * keys and unknown columns.
     *
     * @param columnName the column to index
     * @throws IllegalArgumentException if the column does not exist
     * @throws IllegalStateException    if the column already holds duplicate keys
     */
    public void createUniqueIndex(String columnName) {
        createSecondaryIndex(columnName, ErrorMessages.INDEX_UNIQUE, UniqueIndex::new, true);
        LOGGER.log(Level.INFO, "Created unique index on column {0} for table {1}", new Object[]{columnName, name});
    }

    /**
     * Builds a composite B-tree index over multiple columns. The composite
     * key is the ordered list of column values.
     *
     * @param columnNames the columns to index (order matters)
     * @throws IllegalArgumentException if any column does not exist
     */
    public void createCompositeBTreeIndex(List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Composite index requires at least one column");
        }
        for (String col : columnNames) {
            if (!columnTypes.containsKey(col)) {
                throw new ColumnNotFoundException(ErrorMessages.COLUMN_PREFIX + col + ErrorMessages.DOES_NOT_EXIST);
            }
        }
        CompositeBTreeIndex index = new CompositeBTreeIndex(columnNames);
        int n = rows.size();
        List<List<Object>> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            List<Object> compositeKey = new ArrayList<>(columnNames.size());
            boolean skip = false;
            for (String col : columnNames) {
                Object val = rows.get(i).get(col);
                if (val == null) {
                    skip = true;
                    break;
                }
                compositeKey.add(val);
            }
            if (!skip) {
                keys.add(compositeKey);
                indices.add(i);
            }
        }
        // Sort by composite key
        List<int[]> pairs = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            pairs.add(new int[]{i});
        }
        pairs.sort((a, b) -> {
            CompositeBTreeIndex.CompositeKey k1 = new CompositeBTreeIndex.CompositeKey(keys.get(a[0]));
            CompositeBTreeIndex.CompositeKey k2 = new CompositeBTreeIndex.CompositeKey(keys.get(b[0]));
            return k1.compareTo(k2);
        });
        List<List<Object>> sortedKeys = new ArrayList<>(keys.size());
        List<Integer> sortedIdx = new ArrayList<>(keys.size());
        for (int[] pair : pairs) {
            sortedKeys.add(keys.get(pair[0]));
            sortedIdx.add(indices.get(pair[0]));
        }
        index.bulkLoad(sortedKeys, sortedIdx);
        String compositeKey = String.join("+", columnNames);
        indexes.put(compositeKey, index);
        indexDefinitions.put(compositeKey, ErrorMessages.INDEX_COMPOSITE_BTREE);
        LOGGER.log(Level.INFO, "Created composite B-tree index on {0} for table {1}",
                new Object[]{compositeKey, name});
    }

    /**
     * Returns the composite index for the given column list, or null.
     *
     * @param columnNames the column names in order
     * @return the index, or null
     */
    public Index getCompositeIndex(List<String> columnNames) {
        return indexes.get(String.join("+", columnNames));
    }

    /**
     * Returns the cover column names for a covering index, or an empty list.
     */
    List<String> getCoverColumnNames(String indexColumn) {
        return coverColumnDefinitions.getOrDefault(indexColumn, Collections.emptyList());
    }

    /**
     * Stores the cover column names for a covering index.
     */
    void setCoverColumnNames(String indexColumn, List<String> coverColumns) {
        coverColumnDefinitions.put(indexColumn, coverColumns);
    }

    /**
     * Builds a covering B-tree index that stores extra column values for
     * each row, enabling index-only scans when all SELECT columns are covered.
     *
     * @param indexColumn  the column to index
     * @param coverColumns additional columns to store in the index
     * @throws IllegalArgumentException if any column does not exist
     */
    public void createCoveringBTreeIndex(String indexColumn, List<String> coverColumns) {
        if (!columnTypes.containsKey(indexColumn)) {
            throw new ColumnNotFoundException(ErrorMessages.COLUMN_PREFIX + indexColumn + ErrorMessages.DOES_NOT_EXIST);
        }
        for (String col : coverColumns) {
            if (!columnTypes.containsKey(col)) {
                throw new ColumnNotFoundException(ErrorMessages.COLUMN_PREFIX + col + ErrorMessages.DOES_NOT_EXIST);
            }
        }
        CoveringBTreeIndex index = new CoveringBTreeIndex(
                columnTypes.get(indexColumn), indexColumn, coverColumns);
        int n = rows.size();
        List<Object> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Object key = rows.get(i).get(indexColumn);
            if (key != null) {
                keys.add(key);
                indices.add(i);
            }
        }
        List<int[]> pairs = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            pairs.add(new int[]{i});
        }
        pairs.sort((a, b) -> {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) keys.get(a[0]);
            return c1.compareTo(keys.get(b[0]));
        });
        List<Object> sortedKeys = new ArrayList<>(keys.size());
        List<Integer> sortedIdx = new ArrayList<>(keys.size());
        for (int[] pair : pairs) {
            sortedKeys.add(keys.get(pair[0]));
            sortedIdx.add(indices.get(pair[0]));
        }
        index.bulkLoadWithCover(sortedKeys, sortedIdx, rows);
        indexes.put(indexColumn, index);
        indexDefinitions.put(indexColumn, ErrorMessages.INDEX_COVERING_BTREE);
        setCoverColumnNames(indexColumn, coverColumns);
        LOGGER.log(Level.INFO, "Created covering B-tree index on {0} (covers {1}) for table {2}",
                new Object[]{indexColumn, coverColumns, name});
    }

    /**
     * Builds a secondary index over the already-present rows and registers it
     * under {@code columnName}. When {@code unique} is set, duplicate keys abort
     * the creation.
     */
    private void createSecondaryIndex(String columnName, String definition, Function<Class<?>, Index> indexFactory, boolean unique) {
        if (!columnTypes.containsKey(columnName)) {
            throw new ColumnNotFoundException(ErrorMessages.COLUMN_PREFIX + columnName + ErrorMessages.DOES_NOT_EXIST);
        }
        Index index = indexFactory.apply(columnTypes.get(columnName));
        if (index instanceof BTreeIndex btree) {
            // Bulk-load: collect all key/rowIndex pairs, sort, then build in O(N).
            int n = rows.size();
            List<Object> keys = new ArrayList<>(n);
            List<Integer> indices = new ArrayList<>(n);
            Set<Object> seenKeys = unique ? new HashSet<>() : null;
            for (int i = 0; i < n; i++) {
                Object key = rows.get(i).get(columnName);
                if (key != null) {
                    if (unique && !seenKeys.add(key)) {
                        throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_QUOTED + key + ErrorMessages.FOUND_IN_COLUMN + columnName + " while creating unique index");
                    }
                    keys.add(key);
                    indices.add(i);
                }
            }
            // Sort by key using the same comparator BTreeIndex uses.
            List<int[]> pairs = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                pairs.add(new int[]{i});
            }
            pairs.sort((a, b) -> {
                @SuppressWarnings("unchecked")
                Comparable<Object> c1 = (Comparable<Object>) keys.get(a[0]);
                return c1.compareTo(keys.get(b[0]));
            });
            List<Object> sortedKeys = new ArrayList<>(keys.size());
            List<Integer> sortedIdx = new ArrayList<>(keys.size());
            for (int[] pair : pairs) {
                sortedKeys.add(keys.get(pair[0]));
                sortedIdx.add(indices.get(pair[0]));
            }
            btree.bulkLoad(sortedKeys, sortedIdx);
        } else {
            // One-by-one insert for Hash/Unique indexes.
            Set<Object> seenKeys = unique ? new HashSet<>() : null;
            for (int i = 0; i < rows.size(); i++) {
                Object key = rows.get(i).get(columnName);
                if (key != null) {
                    if (unique && !seenKeys.add(key)) {
                        throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_QUOTED + key + ErrorMessages.FOUND_IN_COLUMN + columnName + " while creating unique index");
                    }
                    index.insert(key, i);
                }
            }
        }
        indexes.put(columnName, index);
        indexDefinitions.put(columnName, definition);
    }

    /**
     * Builds a unique clustered B-tree index over the column: the rows are
     * sorted by the key and every secondary index is rebuilt on the new row
     * order. Null and duplicate keys abort the creation.
     *
     * @param columnName the column to index
     * @throws IllegalArgumentException if the column does not exist
     * @throws IllegalStateException    if the table already has a clustered
     *                                  index or the column holds null/duplicate keys
     */
    public void createUniqueClusteredIndex(String columnName) {
        if (!columnTypes.containsKey(columnName)) {
            throw new ColumnNotFoundException(ErrorMessages.COLUMN_PREFIX + columnName + ErrorMessages.DOES_NOT_EXIST);
        }
        if (hasClusteredIndex) {
            throw new IllegalStateException("Table already has a clustered index on " + clusteredIndexColumn);
        }

        // Phase 1: Snapshot rows under read lock (readers still see old data).
        List<Map<String, Object>> snapshot;
        tableLock.readLock().lock();
        try {
            snapshot = new ArrayList<>(rows);
        } finally {
            tableLock.readLock().unlock();
        }

        int n = snapshot.size();

        // Phase 2: Extract keys + indices, sort (parallel for large tables).
        Object[] keys = new Object[n];
        Integer[] sortedOrder = new Integer[n];
        for (int i = 0; i < n; i++) {
            Object key = snapshot.get(i).get(columnName);
            if (key == null) {
                throw new IllegalStateException("Null key in column " + columnName + " not allowed for unique clustered index");
            }
            keys[i] = key;
            sortedOrder[i] = i;
        }

        if (n >= PARALLEL_SORT_THRESHOLD) {
            INDEX_BUILD_POOL.submit(() -> Arrays.parallelSort(sortedOrder, (a, b) -> compareKeys(keys[a], keys[b]))).join();
        } else {
            Arrays.sort(sortedOrder, (a, b) -> compareKeys(keys[a], keys[b]));
        }

        // Phase 3: Uniqueness check — O(N) linear scan of sorted data.
        for (int i = 1; i < n; i++) {
            if (compareKeys(keys[sortedOrder[i - 1]], keys[sortedOrder[i]]) == 0) {
                throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_QUOTED + keys[sortedOrder[i]] + ErrorMessages.FOUND_IN_COLUMN + columnName + " while creating unique clustered index");
            }
        }

        // Phase 4: Build sorted row list and key/index lists for bulk-load (no lock held).
        List<Map<String, Object>> sortedRows = new ArrayList<>(n);
        List<Object> sortedKeys = new ArrayList<>(n);
        List<Integer> sortedIndices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            sortedRows.add(snapshot.get(sortedOrder[i]));
            sortedKeys.add(keys[sortedOrder[i]]);
            sortedIndices.add(i);
        }

        BTreeClusteredIndex newIndex = new BTreeClusteredIndex(columnTypes.get(columnName));
        newIndex.bulkLoad(sortedKeys, sortedIndices);

        // Phase 5: Atomic swap under write lock (brief window).
        tableLock.writeLock().lock();
        try {
            rows.clear();
            rows.addAll(sortedRows);
            clusteredIndex = newIndex;
            hasClusteredIndex = true;
            clusteredIndexColumn = columnName;
            // Rebuild secondary indexes on the new row order.
            rebuildSecondaryIndexes();
        } finally {
            tableLock.writeLock().unlock();
        }

        LOGGER.log(Level.INFO, "Created unique clustered B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable ck1 && k2 instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) ck1;
            return c1.compareTo(k2);
        }
        return String.valueOf(k1).compareTo(String.valueOf(k2));
    }

    /**
     * Returns the secondary index built on the column, or null when there is
     * none.
     *
     * @param columnName the column name
     * @return the index, or null
     */
    public Index getIndex(String columnName) {
        return indexes.get(columnName);
    }

    /**
     * Returns whether the table has a clustered index.
     *
     * @return true when a clustered index exists
     */
    public boolean hasClusteredIndex() {
        return hasClusteredIndex;
    }

    /**
     * Returns the column the clustered index is built on, or null.
     *
     * @return the clustered index column, or null
     */
    public String getClusteredIndexColumn() {
        return clusteredIndexColumn;
    }

    /**
     * Returns the clustered index, or null when the table has none.
     *
     * @return the clustered index, or null
     */
    public BTreeClusteredIndex getClusteredIndex() {
        return clusteredIndex;
    }

    /**
     * Returns a copy of the ordered column names.
     *
     * @return the column list
     */
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    /**
     * Returns a copy of the case-insensitive column-to-type mapping.
     *
     * @return the column type map
     */
    public Map<String, Class<?>> getColumnTypes() {
        Map<String, Class<?>> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(columnTypes);
        return copy;
    }

    /**
     * Returns the primary key column name, or null when the table has none.
     *
     * @return the primary key column, or null
     */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /**
     * Returns the current number of rows without copying the row list.
     *
     * @return the row count
     */
    public int rowCount() {
        return getLiveRowCount();
    }

    /**
     * Immutable snapshot of the table statistics used by the optimizer: the
     * exact row count, the average row size in bytes (measured, or a schema
     * estimate before the first analysis) and the timestamp of the last
     * analysis ({@code 0} when never analyzed). See {@link Table#analyze()}.
     */
    public static final class TableStatistics {
        private final long rowCount;
        private final long avgRowSizeBytes;
        private final long lastAnalyzedMillis;

        TableStatistics(long rowCount, long avgRowSizeBytes, long lastAnalyzedMillis) {
            this.rowCount = rowCount;
            this.avgRowSizeBytes = avgRowSizeBytes;
            this.lastAnalyzedMillis = lastAnalyzedMillis;
        }

        /**
         * Returns the exact number of rows in the table.
         *
         * @return the row count
         */
        public long getRowCount() {
            return rowCount;
        }

        /**
         * Returns the average row size in bytes, or a schema-based estimate
         * when the table has never been analyzed.
         *
         * @return the average row size in bytes
         */
        public long getAvgRowSizeBytes() {
            return avgRowSizeBytes;
        }

        /**
         * Returns the epoch-millis timestamp of the last analysis, or {@code 0}
         * when the table has never been analyzed.
         *
         * @return the last-analyzed timestamp in epoch millis
         */
        public long getLastAnalyzedMillis() {
            return lastAnalyzedMillis;
        }
    }

    /**
     * Returns an immutable snapshot of the table statistics: the exact row
     * count, the average row size in bytes and the last-analyzed timestamp.
     * Reads never mutate the table: when the average row size has not been
     * measured yet it is reported as the cheap schema-based estimate.
     *
     * @return the statistics snapshot
     */
    public TableStatistics getStatistics() {
        synchronized (statsLock) {
            long avg = avgRowSizeBytes;
            if (avg <= 0) {
                avg = estimateAverageRowSizeBytes();
            }
            return new TableStatistics(Math.max(0, getLiveRowCount()), avg, lastAnalyzedMillis);
        }
    }

    /** Returns the current version of this table (incremented on every DML mutation). */
    public long getVersion() {
        return version.get();
    }

    /** Increments the version counter. Called after in-place DML (e.g. UPDATE) that bypasses addRow/removeRow. */
    public void bumpVersion() {
        version.incrementAndGet();
    }

    /**
     * Writes a modified row back to the underlying storage so that
     * CSV/TSV backends (which store rows as compact Object[] arrays)
     * persist the changes. For in-memory storage this replaces the
     * internal Map reference.
     *
     * @param rowIndex the zero-based position of the row
     * @param row      the column-to-value map (caller must have already
     *                 applied the mutations)
     */
    public void updateRowInPlace(int rowIndex, Map<String, Object> row) {
        tableLock.writeLock().lock();
        try {
            if (storage != null) {
                storage.update(rowIndex, row);
            }
            // The internal mirror must stay in sync so that reads keep using
            // the shared-map fast path (getLiveRows iterates {@code rows})
            // instead of re-materializing every row from the storage backend.
            rows.set(rowIndex, row);
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /**
     * Synchronously recomputes the statistics (exact row count, measured
     * average row size and the last-analyzed timestamp) and returns them. This
     * is the forced recalculation behind the {@code ANALYZE TABLE} command and
     * the deterministic counterpart of the asynchronous INSERT/DELETE refresh.
     *
     * @return the freshly computed statistics snapshot
     */
    public TableStatistics analyze() {
        synchronized (statsLock) {
            rowCount = getLiveRowCount();
            avgRowSizeBytes = measureAverageRowSizeBytes();
            lastAnalyzedMillis = System.currentTimeMillis();
            statsDirty = false;
        }
        LOGGER.log(Level.INFO, "Table {0} analyzed: {1} rows, avg row size {2} bytes",
                new Object[]{name, rowCount, avgRowSizeBytes});
        return getStatistics();
    }

    /** Marks the statistics dirty and schedules the asynchronous refresh. */
    void markStatsDirty() {
        statsDirty = true;
        if (!statsRefreshScheduled) {
            statsRefreshScheduled = true;
            try {
                STATS_SCHEDULER.schedule(this::refreshStats, STATS_REFRESH_DELAY_MILLIS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // Scheduler shut down (JVM exit): the statistics stay approximate.
                statsRefreshScheduled = false;
            }
        }
    }

    /**
     * Background statistics refresh: recomputes the average row size and the
     * last-analyzed timestamp after a mutation, coalescing the burst into a
     * single pass. When the row list was mutated while measuring, the refresh
     * is rescheduled so the numbers never claim precision they lack.
     */
    private void refreshStats() {
        boolean reschedule;
        synchronized (statsLock) {
            statsRefreshScheduled = false;
            if (!statsDirty) {
                return;
            }
            int rowsAtStart = rows.size();
            long measured = measureAverageRowSizeBytes();
            reschedule = rowsAtStart != rows.size();
            if (!reschedule) {
                rowCount = getLiveRowCount();
                avgRowSizeBytes = measured;
                lastAnalyzedMillis = System.currentTimeMillis();
                statsDirty = false;
            }
        }
        if (reschedule) {
            markStatsDirty();
        }
    }

    /**
     * Measures the actual average row size by summing the estimated byte size
     * of every stored value. O(rows), so it only runs asynchronously after
     * mutations or synchronously on ANALYZE TABLE. Concurrent mutations during
     * the pass are tolerated: on a {@link ConcurrentModificationException} the
     * measurement is retried and finally falls back to the schema estimate.
     */
    private long measureAverageRowSizeBytes() {
        int n = rows.size();
        if (n == 0) {
            return estimateAverageRowSizeBytes();
        }
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                long total = 0;
                for (int i = 0; i < n && i < rows.size(); i++) {
                    Map<String, Object> row = rows.get(i);
                    long rowBytes = 0;
                    for (String column : columns) {
                        rowBytes += estimatedValueBytes(row.get(column), columnTypes.get(column));
                    }
                    total += rowBytes;
                }
                return Math.max(1, total / Math.min(n, rows.size()));
            } catch (ConcurrentModificationException ignored) {
                // Rows changed while measuring; retry.
                LOGGER.log(Level.FINE, "Concurrent modification during row size estimation, retrying");
            }
        }
        return estimateAverageRowSizeBytes();
    }

    /**
     * Schema-only estimate of the average row size, used before the first
     * analysis. Cost: O(columns), never touches the rows.
     */
    private long estimateAverageRowSizeBytes() {
        long perRow = 0;
        for (String column : columns) {
            Class<?> type = columnTypes.get(column);
            if (type != null) {
                perRow += estimatedColumnBaseSize(type);
            }
        }
        return Math.max(1, perRow);
    }

    /** Approximate in-memory byte size of a stored value, used for statistics. */
    private static long estimatedValueBytes(Object value, Class<?> type) {
        if (value == null) {
            return 0;
        }
        if (type == Integer.class || type == Float.class) {
            return 4;
        }
        if (type == Long.class || type == Double.class) {
            return 8;
        }
        if (type == Short.class) {
            return 2;
        }
        if (type == Byte.class) {
            return 1;
        }
        if (type == Character.class) {
            return 2;
        }
        if (type == Boolean.class) {
            return 1;
        }
        if (type == BigDecimal.class) {
            return 48;
        }
        if (type == LocalDate.class) {
            return 16;
        }
        if (type == LocalDateTime.class) {
            return 24;
        }
        if (type == UUID.class) {
            return 16;
        }
        if (type == String.class) {
            String text = value.toString();
            return 40 + 2L * text.length();
        }
        return 32;
    }

    /** Approximate byte size of a column value with the given type, value-free. */
    private static long estimatedColumnBaseSize(Class<?> type) {
        if (type == Integer.class || type == Float.class) {
            return 4;
        }
        if (type == Long.class || type == Double.class) {
            return 8;
        }
        if (type == Short.class) {
            return 2;
        }
        if (type == Byte.class) {
            return 1;
        }
        if (type == Character.class) {
            return 2;
        }
        if (type == Boolean.class) {
            return 1;
        }
        if (type == BigDecimal.class) {
            return 48;
        }
        if (type == LocalDate.class) {
            return 16;
        }
        if (type == LocalDateTime.class) {
            return 24;
        }
        if (type == UUID.class) {
            return 16;
        }
        if (type == String.class) {
            return 64;
        }
        return 32;
    }

    /**
     * Formats an epoch-millis timestamp for the ANALYZE TABLE status message.
     *
     * @param epochMillis the timestamp, or {@code 0} for "never analyzed"
     * @return the formatted local timestamp text
     */
    static String formatTimestamp(long epochMillis) {
        if (epochMillis <= 0) {
            return "never";
        }
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault())
                .format(STATS_TIMESTAMP_FORMAT);
    }

    /**
     * Returns a copy of the table rows.
     *
     * @return the row list
     */
    public List<Map<String, Object>> getRows() {
        tableLock.readLock().lock();
        try {
            if (storage != null) {
                return storage.scan();
            }
            return new ArrayList<>(rows);
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Returns a copy of the table rows with rows hidden from the current reader
     * filtered out: tombstoned rows, rows created by transactions the reader may
     * not see, and rows deleted within the reader's snapshot. Values come from
     * the shared in-memory mirror ({@link #updateRowInPlace} keeps it in sync
     * with the storage backend), so no per-row copies are materialized.
     */
    public List<Map<String, Object>> getLiveRows() {
        tableLock.readLock().lock();
        try {
            List<Map<String, Object>> result = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                if (isRowVisibleToReader(i)) {
                    result.add(getVisibleRowForReader(i));
                }
            }
            return result;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Returns true if the row at {@code rowIndex} must be returned to the reader
     * identified by the {@link MvccReadContext} of the current thread.
     *
     * <p>Combines the tombstone check with MVCC visibility: a row is hidden when
     * it was created by a transaction that is aborted, or by an active
     * transaction whose writes this reader may not observe (no dirty reads), or
     * committed after the reader's snapshot; and when it was deleted by a
     * transaction visible to this reader's snapshot. Without a reader context
     * (query executed outside {@code Database.dispatch}) only the tombstone
     * check and the legacy {@link RowVersionMeta#isAlive()} rule apply.
     *
     * @param rowIndex the raw row index
     * @return true if the row is visible to the current reader
     */
    public boolean isRowVisibleToReader(int rowIndex) {
        // Track read for SERIALIZABLE transactions
        MvccReadContext.Context context = MvccReadContext.get();
        if (context != null && context.getTransaction() != null 
                && context.getTransaction().getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
            long txid = context.getTransaction().getTxid();
            if (txid > 0) {
                database.getConflictDetector().noteRead(txid, name, rowIndex);
            }
        }
        
        if (isDeleted(rowIndex)) {
            // COMMIT-time tombstones for MVCC deletes keep their xmax: a reader
            // whose snapshot predates the delete still has to see the row
            // (only vacuum removes it unconditionally, horizon-aware). Legacy
            // tombstones (no MVCC xmax) stay hidden from everyone.
            RowVersionMeta tombstoneMeta = rowVersions == null ? null : rowVersions.get(rowIndex);
            if (tombstoneMeta == null || tombstoneMeta.getXmax() == 0) {
                return false;
            }
            return MvccReadContext.isRowVisible(database, tombstoneMeta, MvccReadContext.get());
        }
        if (rowVersions == null || rowVersions.isEmpty()) {
            return true;
        }
        RowVersionMeta meta = rowVersions.get(rowIndex);
        if (meta == null) {
            return true;
        }
        return MvccReadContext.isRowVisible(database, meta, MvccReadContext.get());
    }

    /**
     * Returns the row values the current reader must see at {@code rowIndex}:
     * the live row normally, or a copy of the committed values while an
     * uncommitted change from another transaction must stay hidden.
     *
     * @param rowIndex the raw row index
     * @return the reader-visible row values
     */
    public Map<String, Object> getVisibleRowForReader(int rowIndex) {
        return getVisibleRowForReader(rowIndex, rows.get(rowIndex));
    }

    /**
     * Same as {@link #getVisibleRowForReader(int)} but with the current row
     * values supplied by the caller. Callers that already hold the current
     * physical values (e.g. an AS-OF snapshot built from
     * {@code storage.scan()}) pass them here to avoid a mirror re-read; the
     * mirror is kept in sync by {@link #updateRowInPlace}, so the two always
     * agree.
     *
     * <p>Shadow-model rules (prompt4.md #4): the physical row always holds the
     * latest values of its owner; readers see the retained pre-image
     * ({@link RowVersionMeta#getCommittedValues()}) when the physical values
     * are not theirs to see — a pending change by another transaction, or a
     * commit that landed after their snapshot.
     *
     * @param rowIndex   the raw row index
     * @param currentRow the reader's current row values (used when no
     *                   committed pre-image must be returned)
     * @return the reader-visible row values
     */
    public Map<String, Object> getVisibleRowForReader(int rowIndex, Map<String, Object> currentRow) {
        if (rowVersions == null || rowVersions.isEmpty()) {
            return currentRow;
        }
        RowVersionMeta meta = rowVersions.get(rowIndex);
        if (meta == null || meta.getCommittedValues() == null) {
            return currentRow;
        }
        MvccReadContext.Context ctx = MvccReadContext.get();
        if (ctx == null) {
            // No reader context (query outside Database.dispatch): legacy rule —
            // return the retained pre-image whenever one exists.
            return new HashMap<>(meta.getCommittedValues());
        }
        if (meta.getOwnerTxid() != 0 && meta.getOwnerTxid() == ctx.readTxid) {
            return currentRow; // own write: the physical row is authoritative
        }
        if (meta.hasUncommittedChanges()) {
            if (ctx.allowsDirtyReads()) {
                return currentRow; // dirty readers observe the pending change
            }
            return new HashMap<>(meta.getCommittedValues());
        }
        if (meta.getLastCommittedCsn() > ctx.snapshotCsn) {
            return new HashMap<>(meta.getCommittedValues()); // commit after snapshot
        }
        return currentRow;
    }

    /**
     * Executes the given action while holding the table read lock.
     * Use this in query execution paths that access rows or indexes.
     */
    public void withReadLock(Runnable action) {
        tableLock.readLock().lock();
        try {
            action.run();
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Executes the given callable while holding the table read lock.
     */
    public <T> T withReadLock(Callable<T> action) throws Exception {
        tableLock.readLock().lock();
        try {
            return action.call();
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Executes the given action while holding the table write lock.
     * The lock is reentrant, so nested compaction inside the action is allowed.
     */
    public void withWriteLock(Runnable action) {
        tableLock.writeLock().lock();
        try {
            action.run();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /**
     * Executes the given callable while holding the table write lock.
     * The lock is reentrant, so nested compaction inside the callable is allowed.
     */
    public <T> T withWriteLock(Callable<T> action) throws Exception {
        tableLock.writeLock().lock();
        try {
            return action.call();
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /**
     * Enters a deferred bulk-update window on this table's storage (prompt 35).
     * Index-aware backends skip per-operation index rebuilds and position
     * shifting until {@link #endBulkUpdate()} runs the single rebuild. A
     * no-op for in-memory or otherwise non-indexed storages. Intended for
     * mass-change paths such as DELETE WHERE; not thread-safe.
     */
    public void beginBulkUpdate() {
        if (storage != null) {
            storage.beginBulkUpdate();
        }
    }

    /**
     * Leaves a deferred bulk-update window on this table's storage, performing
     * the single index rebuild accumulated while dirty. Must be paired with
     * {@link #beginBulkUpdate()}, even on exceptional paths.
     */
    public void endBulkUpdate() {
        if (storage != null) {
            storage.endBulkUpdate();
        }
    }

    /**
     * Removes the row at the given index and invalidates the locks of this
     * and all following rows, whose indexes shift down by one.
     *
     * @param rowIndex the row index to remove
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public void removeRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException("Row index " + rowIndex + " out of bounds for table " + name);
        }
        if (storage != null) {
            storage.delete(rowIndex);
        }
        rows.remove(rowIndex);
        // Row indexes shift down by one, so the locks of this and all following rows are stale.
        for (int i = rowIndex; i <= rows.size(); i++) {
            rowLocks.remove(i);
        }
        rowCount--;
        version.incrementAndGet();
        markStatsDirty();
    }

    // ─── Tombstone / lazy-deletion support ────────────────────────────

    /**
     * Logically marks the row at {@code rowIndex} as deleted (tombstone).
     * The row stays in the ArrayList but is invisible to all query paths.
     * Does NOT remove index entries — the caller must do that separately.
     */
    public void markDeleted(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            throw new IndexOutOfBoundsException("Row index " + rowIndex + " out of bounds for table " + name);
        }
        deletedRows.set(rowIndex);
        version.incrementAndGet();
        markStatsDirty();
    }

    /** Returns whether the row at the given raw position is tombstoned. */
    public boolean isDeleted(int rowIndex) {
        return deletedRows != null && deletedRows.get(rowIndex);
    }

    /** Returns the number of tombstoned rows. */
    public int getDeletedCount() {
        return deletedRows == null ? 0 : deletedRows.cardinality();
    }

    /** Returns the number of live (non-tombstoned) rows. */
    public int getLiveRowCount() {
        return rows.size() - getDeletedCount();
    }

    /** Returns the raw size of the internal row list, including tombstones. */
    public int getRawRowCount() {
        return rows.size();
    }

    /**
     * Re-synchronises the in-memory row mirror and every engine-level index
     * from the storage backend, treating storage as the source of truth.
     * Required after a storage-level bulk load such as
     * {@link RowStorage#loadFromFile(String)}, which fills only the storage's
     * private row list and bypasses the mirror that {@link #getLiveRows()}
     * scans. Tombstone marks and MVCC version metadata index into the old
     * mirror and are reset along with it. Refuses to run while rows carry
     * uncommitted MVCC changes, since undo records hold raw row indexes.
     */
    public void reloadFromStorage() {
        if (storage == null) {
            return;
        }
        if (hasUncommittedMvccChanges()) {
            LOGGER.log(Level.FINE,
                    "Skipping reloadFromStorage for table {0}: rows carry pending MVCC changes", name);
            return;
        }
        List<Map<String, Object>> loaded = new ArrayList<>(storage.scan());
        tableLock.writeLock().lock();
        try {
            rows.clear();
            rows.addAll(loaded);
            deletedRows = new BitSet();
            rowVersions = new ConcurrentHashMap<>();
            uncommittedMvccRows = ConcurrentHashMap.newKeySet();
            rowLocks = new ConcurrentHashMap<>();
            rebuildAllIndexes();
            rowCount = getLiveRowCount();
            markStatsDirty();
        } finally {
            tableLock.writeLock().unlock();
        }
        LOGGER.log(Level.INFO, "Table {0}: reloaded mirror from storage ({1} rows)",
                new Object[]{name, rows.size()});
    }

    /**
     * Physically removes all tombstoned rows and rebuilds every index.
     * This is the only point where the ArrayList actually shrinks.
     */
    public void compact() {
        tableLock.writeLock().lock();
        try {
            if (hasUncommittedMvccChanges()) {
                // Undo records hold raw row indexes; compacting now would
                // invalidate every pending record. Defer until the open
                // transactions commit or roll back.
                LOGGER.log(Level.FINE,
                        "Skipping compaction of table {0}: rows carry pending MVCC changes", name);
                return;
            }
            if (hasSnapshotDependentTombstones()) {
                // A tombstone committed after the oldest active snapshot must
                // survive physical removal: REPEATABLE READ readers still need
                // the row (and its pre-image) until their transaction ends.
                // Vacuum clears it later once the horizon passes the delete.
                LOGGER.log(Level.FINE,
                        "Skipping compaction of table {0}: tombstones still visible to an open snapshot", name);
                return;
            }
            int deleted = getDeletedCount();
            if (deleted == 0) {
                return;
            }

            List<Map<String, Object>> source = (storage != null) ? storage.scan() : rows;
            List<Map<String, Object>> newRows = new ArrayList<>(source.size() - deleted);
            for (int i = 0; i < source.size(); i++) {
                if (!isDeleted(i)) {
                    newRows.add(source.get(i));
                }
            }

            rows.clear();
            rows.addAll(newRows);
            if (storage != null) {
                // The storage mirror is rebuilt inside a deferred bulk window:
                // one index rebuild total, run by endBulkUpdate().
                storage.beginBulkUpdate();
                try {
                    storage.setRows(newRows);
                } finally {
                    storage.endBulkUpdate();
                }
            }

            // Remap MVCC metadata while the old tombstone marks are still in
            // place: findNewIndexAfterCompact() counts non-deleted rows before
            // the compacted position. Resetting deletedRows first would make
            // every row look alive and leave the metadata keyed by stale
            // pre-compact positions.
            rebuildRowVersionsAfterCompact();

            deletedRows = new BitSet();

            rebuildAllIndexes();

            rowLocks = new ConcurrentHashMap<>();

            rowCount = rows.size();
            markStatsDirty();

            LOGGER.log(Level.INFO, "Compacted table {0}: removed {1} tombstones, {2} live rows remain",
                    new Object[]{name, deleted, rows.size()});
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    // ─── MVCC row version metadata support ─────────────────────────────

    /**
     * Returns the MVCC metadata for the row at the given index.
     * Returns null if no MVCC metadata is available for this row.
     */
    public RowVersionMeta getRowVersionMeta(int rowIndex) {
        return rowVersions != null ? rowVersions.get(rowIndex) : null;
    }

    /**
     * Sets MVCC metadata for the row at the given index.
     */
    public void setRowVersionMeta(int rowIndex, RowVersionMeta meta) {
        if (rowVersions == null) {
            rowVersions = new ConcurrentHashMap<>();
        }
        rowVersions.put(rowIndex, meta);
    }

    /**
     * Removes MVCC metadata for the row at the given index.
     */
    public void removeRowVersionMeta(int rowIndex) {
        if (rowVersions != null) {
            rowVersions.remove(rowIndex);
        }
    }

    /**
     * Updates row metadata for an INSERT operation by the given transaction.
     */
    public void markInsert(int rowIndex, long txid, Map<String, Object> committedValues) {
        RowVersionMeta meta = new RowVersionMeta(txid, committedValues);
        setRowVersionMeta(rowIndex, meta);
        uncommittedMvccRows.add(rowIndex);
    }

    /**
     * Updates row metadata for a DELETE operation by the given transaction,
     * capturing the pre-image values so other readers keep seeing them while
     * the delete is pending. Index entries stay in place until vacuum.
     */
    public void markDelete(int rowIndex, long txid) {
        markDelete(rowIndex, txid, getRows().get(rowIndex));
    }

    /**
     * Updates row metadata for a DELETE operation by the given transaction.
     *
     * @param rowIndex the raw row index
     * @param txid     the deleting transaction
     * @param preImage the row values before the delete (captured by the
     *                 caller while it still holds the row lock)
     */
    public void markDelete(int rowIndex, long txid, Map<String, Object> preImage) {
        RowVersionMeta meta = getRowVersionMeta(rowIndex);
        if (meta == null) {
            // Bootstrap row being deleted
            meta = new RowVersionMeta(0, 0, 0, preImage, false, false, false, txid, 0);
            setRowVersionMeta(rowIndex, meta);
        } else if (preImage != null) {
            // Refresh the retained image with the pre-delete physical values:
            // a delete does not alter data, so readers falling back to the
            // image (pending delete or old snapshot after the delete commits)
            // must see exactly what the row held before it. Undo records keep
            // their own pre-change copies, so chained own-changes are unaffected.
            meta.setCommittedValues(preImage);
        }
        meta.setXmax(txid);
        meta.setUncommittedDelete(true);
        meta.setOwnerTxid(txid);
        uncommittedMvccRows.add(rowIndex);
    }

    /**
     * Updates row metadata for an UPDATE operation by the given transaction.
     * Saves the old values in committedValues for undo (shadow model: the
     * physical row is mutated in place; other readers see the pre-image).
     */
    public void markUpdate(int rowIndex, long txid, Map<String, Object> oldValues) {
        RowVersionMeta meta = getRowVersionMeta(rowIndex);
        if (meta == null) {
            // Bootstrap row being updated
            meta = new RowVersionMeta(0, 0, 0, oldValues, false, false, false, txid, 0);
            setRowVersionMeta(rowIndex, meta);
        } else if (!(meta.isUncommittedInsert()
                || (meta.hasUncommittedChanges() && meta.getOwnerTxid() == txid))) {
            // First pending change by another state: capture this pre-image.
            // Own pending changes keep the original pre-image (first wins), so
            // a rollback of several chained undos restores the oldest values.
            meta.setCommittedValues(oldValues);
        }
        meta.setUncommittedUpdate(true);
        meta.setOwnerTxid(txid);
        uncommittedMvccRows.add(rowIndex);
    }

    /**
     * Fails fast when the row at {@code rowIndex} carries a pending change from
     * another transaction, or was committed after this writer's snapshot.
     * Must be called for <em>every</em> target row before the first
     * {@code markUpdate}/{@code markDelete} of a statement: marking one row
     * and then failing on another would leave stale pending flags with no undo
     * record to clear them.
     *
     * @param rowIndex    the raw row index
     * @param txid        the writing transaction
     * @param snapshotCsn the writer's snapshot commit CSN
     * @throws TransactionException on a write-write conflict
     */
    public void checkWriteWriteConflict(int rowIndex, long txid, long snapshotCsn) {
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta == null) {
            return;
        }
        long owner = meta.getOwnerTxid();
        if (meta.hasUncommittedChanges() && owner != 0 && owner != txid) {
            throw new TransactionException(
                    "Write-write conflict on table " + name + ", row " + rowIndex
                    + ": transaction " + owner + " has an uncommitted change");
        }
        if (meta.getLastCommittedCsn() > snapshotCsn) {
            throw new TransactionException(
                    "Write-write conflict on table " + name + ", row " + rowIndex
                    + ": row committed at CSN " + meta.getLastCommittedCsn()
                    + " after the writer's snapshot " + snapshotCsn);
        }
    }

    /**
     * SERIALIZABLE isolation write conflict check for SSI conflict detection.
     * Same logic as checkWriteWriteConflict but throws SerializationFailureException
     * instead of TransactionException for SERIALIZABLE isolation.
     *
     * @param rowIndex    the raw row index
     * @param txid        the transaction ID of the writer
     * @param snapshotCsn  the writer's snapshot commit CSN
     * @throws SerializationFailureException on a write-write conflict
     */
    public void checkSerializableWriteConflict(int rowIndex, long txid, long snapshotCsn) {
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta == null) {
            return;
        }
        long owner = meta.getOwnerTxid();
        if (meta.hasUncommittedChanges() && owner != 0 && owner != txid) {
            throw new SerializationFailureException(
                    "Serialization failure: transaction " + owner + " has an uncommitted change on table " + name + ", row " + rowIndex);
        }
        if (meta.getLastCommittedCsn() > snapshotCsn) {
            throw new SerializationFailureException(
                    "Serialization failure: row committed at CSN " + meta.getLastCommittedCsn()
                    + " after the writer's snapshot " + snapshotCsn + " on table " + name + ", row " + rowIndex);
        }
    }

    /**
     * Marks every pending change of the row as committed at {@code commitCsn}.
     * The retained pre-image is kept for readers whose snapshot predates the
     * commit; {@code commitCsn} of 0 (batch/legacy paths) keeps the row CSN
     * unchanged.
     *
     * @param rowIndex   the raw row index
     * @param commitCsn  the commit sequence number of the committing transaction
     */
    public void markRowCommitted(int rowIndex, long commitCsn) {
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta != null) {
            meta.markCommitted(commitCsn);
        }
        uncommittedMvccRows.remove(rowIndex);
    }

    /**
     * Resolves a rolled-back row: the pending flags are cleared (the
     * TxStatusTracker entry already hides the row from every reader) and the
     * row leaves the uncommitted set. The meta itself is kept so visibility
     * still sees the ABORTED creator; physical reclamation happens at
     * compaction.
     *
     * @param rowIndex the raw row index
     */
    public void markRowRolledBack(int rowIndex) {
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta != null) {
            meta.markAborted();
        }
        uncommittedMvccRows.remove(rowIndex);
    }

    /**
     * Restores the row's version metadata captured before a change (undo).
     *
     * @param rowIndex the raw row index
     * @param oldMeta  the meta to restore, or null to drop the meta entirely
     */
    public void restoreMetaAfterUndo(int rowIndex, RowVersionMeta oldMeta) {
        if (oldMeta == null) {
            removeRowVersionMeta(rowIndex);
        } else {
            setRowVersionMeta(rowIndex, oldMeta);
            if (oldMeta.hasUncommittedChanges()) {
                uncommittedMvccRows.add(rowIndex);
                return;
            }
        }
        uncommittedMvccRows.remove(rowIndex);
    }

    /**
     * Restores a row mutated by UPDATE (undo): rewrites the physical values,
     * adjusting secondary indexes for the keys that change back. The clustered
     * index keys cannot change through UPDATE, so it is left untouched —
     * mirroring {@code UpdateQuery.applyPerRowUpdate}.
     *
     * @param rowIndex  the raw row index
     * @param oldValues the pre-update values
     * @param oldMeta   the pre-update version metadata, or null
     */
    public void restoreRowAfterUpdateUndo(int rowIndex, Map<String, Object> oldValues,
                                          RowVersionMeta oldMeta) {
        if (oldValues != null) {
            Map<String, Object> current = rows.get(rowIndex);
            for (Map.Entry<String, Object> entry : oldValues.entrySet()) {
                String column = entry.getKey();
                Object oldValue = entry.getValue();
                Object newValue = current.get(column);
                if (Objects.equals(oldValue, newValue)) {
                    continue;
                }
                Index index = indexes == null ? null : indexes.get(column);
                if (index != null) {
                    if (newValue != null) {
                        index.remove(newValue, rowIndex);
                    }
                    if (oldValue != null) {
                        evictDeadUniqueEntries(column, oldValue);
                        index.insert(oldValue, rowIndex);
                    }
                }
            }
            updateRowInPlace(rowIndex, new HashMap<>(oldValues));
        }
        restoreMetaAfterUndo(rowIndex, oldMeta);
    }

    /** Returns true when any row carries a pending (uncommitted) MVCC change. */
    public boolean hasUncommittedMvccChanges() {
        return uncommittedMvccRows != null && !uncommittedMvccRows.isEmpty();
    }

    /**
     * Returns true when a committed MVCC tombstone is newer than the oldest
     * active snapshot, so physically removing it would hide the row from a
     * REPEATABLE READ reader that must still see the pre-image. Legacy
     * tombstones (no version metadata) are always compactable.
     */
    boolean hasSnapshotDependentTombstones() {
        if (deletedRows == null || deletedRows.isEmpty()
                || rowVersions == null || rowVersions.isEmpty()) {
            return false;
        }
        long horizon = database != null ? database.computeVacuumHorizonCsn() : Long.MAX_VALUE;
        for (int i = deletedRows.nextSetBit(0); i >= 0; i = deletedRows.nextSetBit(i + 1)) {
            RowVersionMeta meta = rowVersions.get(i);
            if (meta != null && meta.getXmax() != 0 && meta.getLastCommittedCsn() > horizon) {
                return true;
            }
            if (i == Integer.MAX_VALUE) {
                break;
            }
        }
        return false;
    }

    /**
     * Rebuilds row version metadata after compact() due to row index shifts.
     * This must be called after compact() to maintain correct row index mapping.
     */
    public void rebuildRowVersionsAfterCompact() {
        // The uncommitted-row set holds raw indexes too: remap it alongside
        // the metadata so commit/rollback clears the right entries.
        if (uncommittedMvccRows != null && !uncommittedMvccRows.isEmpty()) {
            java.util.Set<Integer> remapped = ConcurrentHashMap.newKeySet();
            for (Integer oldIndex : uncommittedMvccRows) {
                int newIndex = findNewIndexAfterCompact(oldIndex);
                if (newIndex != -1) {
                    remapped.add(newIndex);
                }
            }
            uncommittedMvccRows = remapped;
        }
        if (rowVersions == null || rowVersions.isEmpty()) {
            return;
        }

        // Create a new map with updated indices
        ConcurrentHashMap<Integer, RowVersionMeta> newRowVersions = new ConcurrentHashMap<>();
        rowVersions.forEach((oldIndex, meta) -> {
            // Find the new index of this row by checking if it's not deleted
            int newIndex = findNewIndexAfterCompact(oldIndex);
            if (newIndex != -1) {
                newRowVersions.put(newIndex, meta);
            }
        });
        rowVersions = newRowVersions;
    }

    /**
     * Helper method to find the new index of a row after compact().
     * Returns -1 if the row was removed during compact.
     */
    private int findNewIndexAfterCompact(int oldIndex) {
        int newIndex = 0;
        for (int i = 0; i < oldIndex; i++) {
            if (!isDeleted(i)) {
                newIndex++;
            }
        }
        return isDeleted(oldIndex) ? -1 : newIndex;
    }

    /**
     * Clears all MVCC metadata (called after transaction commit/rollback).
     */
    public void clearRowVersions() {
        rowVersions = null;
    }

    private static byte[] serializeToBytes(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new ObjectOutputStream(baos).writeObject(obj);
        return baos.toByteArray();
    }

    private static long computeChecksum(Serializable obj) throws IOException {
        return computeChecksumFromBytes(serializeToBytes(obj));
    }

    private static long computeChecksumFromBytes(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        // Serialize the transient fields that were made transient for S1948 compliance
        oos.writeObject(columns);
        oos.writeObject(columnTypes);
        oos.writeObject(rows);
        oos.writeObject(indexDefinitions);
        oos.writeObject(coverColumnDefinitions);
        oos.writeObject(sequences);
        oos.writeObject(deletedRows != null ? deletedRows : new BitSet());
        // Serialize secondary indexes with checksums for integrity validation.
        oos.writeInt(indexes.size());
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            oos.writeUTF(entry.getKey());
            byte[] indexBytes = serializeToBytes((Serializable) entry.getValue());
            oos.writeObject(indexBytes);
            oos.writeLong(computeChecksum((Serializable) entry.getValue()));
        }
        // Serialize clustered index.
        oos.writeBoolean(clusteredIndex != null);
        if (clusteredIndex != null) {
            byte[] clusterBytes = serializeToBytes(clusteredIndex);
            oos.writeObject(clusterBytes);
            oos.writeLong(computeChecksum(clusteredIndex));
        }
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        // Read the transient fields that were made transient for S1948 compliance
        @SuppressWarnings("unchecked")
        List<String> readColumns = (List<String>) ois.readObject();
        @SuppressWarnings("unchecked")
        Map<String, Class<?>> readColumnTypes = (Map<String, Class<?>>) ois.readObject();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> readRows = (List<Map<String, Object>>) ois.readObject();
        @SuppressWarnings("unchecked")
        Map<String, String> readIndexDefinitions = (Map<String, String>) ois.readObject();
        @SuppressWarnings("unchecked")
        Map<String, List<String>> readCoverColumnDefinitions = (Map<String, List<String>>) ois.readObject();
        
        // Update the transient fields
        this.columns = readColumns;
        this.columnTypes = readColumnTypes;
        this.rows = readRows;
        this.indexDefinitions = readIndexDefinitions;
        this.coverColumnDefinitions = readCoverColumnDefinitions;
        
        // Rebuild the case-insensitive type map so lookups stay case-insensitive after load.
        Map<String, Class<?>> tempColumnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tempColumnTypes.putAll(columnTypes);
        this.columnTypes.clear();
        this.columnTypes.putAll(tempColumnTypes);
        this.rowLocks = new ConcurrentHashMap<>();
        this.uncommittedMvccRows = ConcurrentHashMap.newKeySet();
        this.indexes = new ConcurrentHashMap<>();
        this.tableLock = new ReentrantReadWriteLock();
        this.storage = StorageFactory.create(
                System.getProperty("diesel.storage.type",
                        getConfigProperty("storage.type", "in_memory")),
                name, columns, columnTypes);
        if (this.storage instanceof AbstractRowStorage ars) {
            ars.setDataDir(database != null && database.getDataDir() != null ? database.getDataDir() : ".");
            if (primaryKeyColumn != null) {
                ars.setPrimaryKeyColumn(primaryKeyColumn);
            }
        }
        if (this.storage != null && !this.rows.isEmpty()) {
            this.storage.setRows(new ArrayList<>(this.rows));
        }
        // Backward compat: format v1 wrote hasClusteredIndex/clusteredIndexColumn twice
        // (once by defaultWriteObject, once explicitly). v2 removed the redundant write.
        if (formatVersion < 2) {
            this.hasClusteredIndex = (boolean) ois.readObject();
            this.clusteredIndexColumn = (String) ois.readObject();
        }
        this.sequences = (Map<String, Sequence>) ois.readObject();
        try {
            this.deletedRows = (BitSet) ois.readObject();
        } catch (Exception e) {
            this.deletedRows = new BitSet();
        }
        // Format v3+ stores serialized indexes with checksums.
        boolean restoredClustered = false;
        if (formatVersion >= 3) {
            try {
                // Restore secondary indexes from serialized data.
                int indexCount = ois.readInt();
                IntStream.range(0, indexCount).forEach(i -> {
                    restoreSecondaryIndex(ois, name);
                });
                // Restore clustered index from serialized data.
                restoreClusteredIndex(ois, name);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "Failed to read serialized index block in table {0}, will rebuild from rows: {1}",
                        new Object[]{name, e.getMessage()});
            }
        }
        // Fallback: rebuild clustered index from rows if not restored from serialized data.
        if (!restoredClustered && hasClusteredIndex) {
            this.clusteredIndex = new BTreeClusteredIndex(columnTypes.get(clusteredIndexColumn));
            List<Object> keys = new ArrayList<>(rows.size());
            List<Integer> indices = new ArrayList<>(rows.size());
            for (int i = 0; i < rows.size(); i++) {
                Object key = rows.get(i).get(clusteredIndexColumn);
                if (key != null) {
                    keys.add(key);
                    indices.add(i);
                }
            }
            clusteredIndex.bulkLoad(keys, indices);
            LOGGER.log(Level.FINE,
                    "Rebuilt clustered index from rows for table {0}", name);
        }
        // Rebuild only secondary indexes that weren't restored from serialized data.
        if (indexDefinitions != null && !indexDefinitions.isEmpty()) {
            rebuildMissingSecondaryIndexes();
        } else {
            LOGGER.log(Level.FINE,
                    "No secondary index definitions found for table {0} during deserialization",
                    name);
        }
        this.database = null;
        this.statsLock = new Object();
        this.version = new AtomicLong(0);
        // Restore statistics: old serialized files carry zeroed stats fields.
        this.rowCount = rows.size();
        if (this.lastAnalyzedMillis == 0 && this.avgRowSizeBytes == 0) {
            this.avgRowSizeBytes = estimateAverageRowSizeBytes();
        }
        markStatsDirty();
    }

    /**
     * Validates and inserts a row. Missing values are filled from sequences,
     * values are checked against the column types and unique constraints, and
     * the row is either inserted at its clustered position or appended at the
     * end.
     *
     * @param row the column-to-value map
     * @throws IllegalArgumentException if a value is missing, has the wrong
     *                                  type, or the sequence-based primary key is set manually
     * @throws IllegalStateException    if a unique/clustered key is duplicated
     */
    public void addRow(Map<String, Object> row) {
        Map<String, Object> validatedRow = new HashMap<>();
        columns.forEach(col -> {
            Object value;
            Sequence sequence = sequences.get(col);
            if (sequence != null) {
                if (col.equals(primaryKeyColumn) && row.containsKey(col)) {
                    throw new IllegalArgumentException("Cannot manually specify value for sequence-based primary key column: " + col);
                }
                value = row.containsKey(col) ? row.get(col) : sequence.nextValue();
            } else if (!row.containsKey(col)) {
                throw new IllegalArgumentException("Missing value for column: " + col);
            } else {
                value = row.get(col);
            }

            if (deferredIndexOps == null) {
                checkUniqueConstraint(col, value);
            }

            Class<?> expectedType = columnTypes.get(col);
            if (expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (value == null) {
                validatedRow.put(col, null);
            } else {
                validateColumnValueType(col, expectedType, value);
                validatedRow.put(col, value);
            }
        });

        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException("Null key in clustered index column: " + clusteredIndexColumn);
            }
            if (clusteredKeyBlocked(key)) {
                LOGGER.log(Level.WARNING, "Duplicate clustered key detected: key '{0}' in column {1}", new Object[]{key, clusteredIndexColumn});
                throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + key + "' in column " + clusteredIndexColumn);
            }
            if (getDeletedCount() > 0) {
                compact();
            }
            insertIntoClusteredPosition(validatedRow, key);
        } else {
            insertAtEnd(validatedRow);
        }
        rowCount++;
        version.incrementAndGet();
        markStatsDirty();
        LOGGER.log(Level.FINE, "Inserted row into table {0}: {1}", new Object[]{name, validatedRow});
    }

    /**
     * Adds a row to this table with MVCC support.
     *
     * @param row the row to add, keys are column names, values are the values
     * @param txid the transaction id for MVCC tracking
     * @return the index of the added row
     * @throws IllegalStateException if a unique constraint is violated
     */
    public int addRowWithMVCC(Map<String, Object> row, long txid) {
        Map<String, Object> validatedRow = new HashMap<>();
        columns.forEach(col -> {
            Object value;
            Sequence sequence = sequences.get(col);
            if (sequence != null) {
                if (col.equals(primaryKeyColumn) && row.containsKey(col)) {
                    throw new IllegalArgumentException("Cannot manually specify value for sequence-based primary key column: " + col);
                }
                value = row.containsKey(col) ? row.get(col) : sequence.nextValue();
            } else if (!row.containsKey(col)) {
                throw new IllegalArgumentException("Missing value for column: " + col);
            } else {
                value = row.get(col);
            }

            if (deferredIndexOps == null) {
                checkUniqueConstraint(col, value);
            }

            Class<?> expectedType = columnTypes.get(col);
            if (expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (value == null) {
                validatedRow.put(col, null);
            } else {
                validateColumnValueType(col, expectedType, value);
                validatedRow.put(col, value);
            }
        });

        int rowIndex;
        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException("Null key in clustered index column: " + clusteredIndexColumn);
            }
            if (clusteredKeyBlocked(key)) {
                LOGGER.log(Level.WARNING, "Duplicate clustered key detected: key '{0}' in column {1}", new Object[]{key, clusteredIndexColumn});
                throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + key + "' in column " + clusteredIndexColumn);
            }
            if (getDeletedCount() > 0) {
                compact();
            }
            rowIndex = insertIntoClusteredPosition(validatedRow, key);
        } else {
            rowIndex = insertAtEnd(validatedRow);
        }
        rowCount++;
        version.incrementAndGet();
        markStatsDirty();
        
        // Mark as insert in MVCC metadata (no committed values: the row is
        // invisible to every other reader until this transaction commits).
        markInsert(rowIndex, txid, null);
        
        LOGGER.log(Level.FINE, "Inserted row into table {0}: {1}", new Object[]{name, validatedRow});
        return rowIndex;
    }

    void checkUniqueConstraint(String column, Object value) {
        Index index = indexes.get(column);
        if ((index instanceof UniqueIndex || index instanceof BTreeClusteredIndex)
                && value != null) {
            for (int hit : index.search(value)) {
                if (blocksUniqueInsert(hit, column, value)) {
                    LOGGER.log(Level.WARNING, "Duplicate key detected: key '{0}' in column {1}; skipping insertion", new Object[]{value, column});
                    throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + value + ErrorMessages.ALREADY_EXISTS_SUFFIX + ErrorMessages.IN_COLUMN + column);
                }
                if (isIndexEntryDead(hit)) {
                    // Stale unique slot: the holder was deleted by a committed
                    // MVCC transaction (or its creator aborted) and vacuum has
                    // not reclaimed it yet. Free the slot for key reuse.
                    index.remove(value, hit);
                    continue;
                }
                // Hidden only because it committed after this reader's
                // snapshot: a future reader would see both rows.
                throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + value + ErrorMessages.ALREADY_EXISTS_SUFFIX + ErrorMessages.IN_COLUMN + column);
            }
        }
    }

    /**
     * Returns true when the clustered key of the given hit is already taken
     * by a row this insert must respect. Unique slots held by rows no future
     * reader can see are recycled (removed) on the way.
     *
     * @param key the clustered key being inserted
     * @return true when the insert must fail with a duplicate-key error
     */
    private boolean clusteredKeyBlocked(Object key) {
        for (int hit : clusteredIndex.search(key)) {
            if (blocksUniqueInsert(hit, clusteredIndexColumn, key)) {
                return true;
            }
            if (isIndexEntryDead(hit)) {
                clusteredIndex.remove(key, hit);
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     * Returns true when the index entry of {@code rowIndex} can never become
     * visible to any future reader: legacy tombstones, committed MVCC deletes
     * and rows whose creator aborted all hold unique slots that only vacuum
     * reclaims. Pending changes and alive rows keep their entries.
     *
     * @param rowIndex the index-hit row
     * @return true when the unique slot may be recycled
     */
    boolean isIndexEntryDead(int rowIndex) {
        if (isDeleted(rowIndex)) {
            return true;
        }
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta == null || meta.hasUncommittedChanges()) {
            return false;
        }
        TxStatusTracker tracker = database == null ? null : database.getTxStatusTracker();
        if (tracker == null) {
            return false;
        }
        if (meta.getXmin() != 0
                && tracker.getStatus(meta.getXmin()) == TxStatusTracker.TxStatus.ABORTED) {
            return true;
        }
        return meta.getXmax() != 0
                && tracker.getStatus(meta.getXmax()) == TxStatusTracker.TxStatus.COMMITTED;
    }

    /**
     * Frees unique slots of {@code column} held by rows no future reader can
     * see. Index maintenance that re-inserts a key (UPDATE, undo) runs this
     * before {@link Index#insert} so a committed MVCC delete does not look
     * like a duplicate. Alive entries are left for the index to reject.
     *
     * @param column the indexed column
     * @param value  the key being (re)inserted
     */
    void evictDeadUniqueEntries(String column, Object value) {
        Index index = indexes.get(column);
        if (value == null || !(index instanceof UniqueIndex)) {
            return;
        }
        for (int hit : index.search(value)) {
            if (isIndexEntryDead(hit)) {
                index.remove(value, hit);
            }
        }
    }

    /**
     * Decides whether an existing index entry for a unique key blocks an INSERT.
     *
     * <p>Index entries survive MVCC deletes until vacuum, so raw hits are not
     * enough: a pending insert by another transaction is a write-write conflict
     * ({@link TransactionException}); every other entry is judged by reader
     * visibility — aborted rows, rows deleted from this reader's viewpoint and
     * rows created after its snapshot do not block the insert.
     *
     * @param rowIndex the index-hit row
     * @param column   the constrained column (message only)
     * @param value    the rejected key (message only)
     * @return true when the entry must be treated as a duplicate
     * @throws TransactionException when another transaction is inserting the key
     */
    private boolean blocksUniqueInsert(int rowIndex, String column, Object value) {
        RowVersionMeta meta = rowVersions == null ? null : rowVersions.get(rowIndex);
        if (meta != null && meta.isUncommittedInsert() && meta.getOwnerTxid() != 0) {
            MvccReadContext.Context ctx = MvccReadContext.get();
            long readerTxid = ctx == null ? -1L : ctx.readTxid;
            if (meta.getOwnerTxid() != readerTxid) {
                throw new TransactionException(
                        "Write-write conflict: transaction " + meta.getOwnerTxid()
                        + " is inserting duplicate key '" + value
                        + "' into column " + column + " of table " + name);
            }
            return true; // own pending insert of the same key → duplicate
        }
        return isRowVisibleToReader(rowIndex);
    }

    /**
     * Returns the physical row values at a raw index in O(1) by reading the
     * in-memory mirror (kept in sync by {@link #updateRowInPlace}), without
     * materializing the whole table the way {@link #getRows()} does. Index
     * lookups use this to fetch only the matched rows.
     *
     * @param rowIndex the raw row index
     * @return the physical values, or null when the index is out of bounds
     */
    Map<String, Object> readPhysicalRow(int rowIndex) {
        tableLock.readLock().lock();
        try {
            return rowIndex >= 0 && rowIndex < rows.size() ? rows.get(rowIndex) : null;
        } finally {
            tableLock.readLock().unlock();
        }
    }

    /**
     * Returns the physical row values at a raw index, reading through the
     * storage backend when one is attached (undo/rollback paths need the
     * storage representation, which is kept in sync with the internal
     * {@code rows} mirror by {@link #updateRowInPlace}).
     *
     * @param rowIndex the raw row index
     * @return the physical values, or null when the index is out of bounds
     */
    Map<String, Object> getPhysicalRow(int rowIndex) {
        if (storage != null) {
            List<Map<String, Object>> snapshot = storage.scan();
            return rowIndex >= 0 && rowIndex < snapshot.size() ? snapshot.get(rowIndex) : null;
        }
        return rowIndex >= 0 && rowIndex < rows.size() ? rows.get(rowIndex) : null;
    }

    private void validateColumnValueType(String column, Class<?> expectedType, Object value) {
        boolean validType = (expectedType == Integer.class && value instanceof Integer)
                || (expectedType == Long.class && value instanceof Long)
                || (expectedType == Short.class && value instanceof Short)
                || (expectedType == Byte.class && value instanceof Byte)
                || (expectedType == BigDecimal.class && value instanceof BigDecimal)
                || (expectedType == Float.class && value instanceof Float)
                || (expectedType == Double.class && value instanceof Double)
                || (expectedType == Character.class && value instanceof Character)
                || (expectedType == UUID.class && value instanceof UUID)
                || (expectedType == String.class && value instanceof String)
                || (expectedType == Boolean.class && value instanceof Boolean)
                || (expectedType == LocalDate.class && value instanceof LocalDate)
                || (expectedType == LocalDateTime.class && value instanceof LocalDateTime);
        if (!validType) {
            throw new IllegalArgumentException(
                    String.format("Invalid type for column %s: expected %s, got %s",
                            column, expectedType.getSimpleName(), value.getClass().getSimpleName()));
        }
    }

    private int insertIntoClusteredPosition(Map<String, Object> row, Object clusteredKey) {
        int insertIndex = findInsertPosition(clusteredKey);
        ReentrantReadWriteLock lock = getRowLock(insertIndex);
        lock.writeLock().lock();
        try {
            rows.add(insertIndex, row);
            if (storage != null) {
                storage.insertAt(insertIndex, row);
            }
            clusteredIndex.insert(clusteredKey, insertIndex);
            insertRowIntoIndexes(row, insertIndex);
            updateIndicesAfterInsert(insertIndex);
        } finally {
            lock.writeLock().unlock();
        }
        return insertIndex;
    }

    private int insertAtEnd(Map<String, Object> row) {
        int rowIndex = rows.size();
        ReentrantReadWriteLock lock = getRowLock(rowIndex);
        lock.writeLock().lock();
        try {
            if (storage != null) {
                storage.insert(row);
            }
            rows.add(row);
            insertRowIntoIndexes(row, rowIndex);
        } finally {
            lock.writeLock().unlock();
        }
        return rowIndex;
    }

    /** Inserts {@code row} into every secondary index at {@code rowIndex}, skipping NULL keys. */
    private void insertRowIntoIndexes(Map<String, Object> row, int rowIndex) {
        if (indicesDisabled || deferredIndexOps != null) {
            return;
        }
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            if (entry.getKey().contains("+")) {
                // Composite index: extract multi-column key
                String[] cols = entry.getKey().split("\\+");
                List<Object> compositeKey = new ArrayList<>(cols.length);
                boolean skip = false;
                for (String col : cols) {
                    Object val = row.get(col);
                    if (val == null) { skip = true; break; }
                    compositeKey.add(val);
                }
                if (!skip) {
                    entry.getValue().insert(compositeKey, rowIndex);
                }
            } else if (entry.getValue() instanceof CoveringBTreeIndex coverIndex) {
                Object key = row.get(entry.getKey());
                if (key != null) {
                    coverIndex.insertWithRow(key, rowIndex, row);
                }
            } else {
                Object key = row.get(entry.getKey());
                if (key != null) {
                    entry.getValue().insert(key, rowIndex);
                }
            }
        }
    }

    private int findInsertPosition(Object key) {
        if (rows.isEmpty()) {
            return 0;
        }
        int low = 0;
        int high = rows.size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            Object midKey = rows.get(mid).get(clusteredIndexColumn);
            int cmp = compareKeys(key, midKey);
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                // Equal physical key: the holder was already ruled dead by
                // clusteredKeyBlocked() (unique slots are recycled there).
                // Place the new row after it; genuine duplicates never get
                // this far.
                low = mid + 1;
            }
        }
        return low;
    }

    /** Shifts the stored row index of every row after an insert into a clustered table. */
    private void updateIndicesAfterInsert(int insertIndex) {
        if (indicesDisabled || deferredIndexOps != null) {
            return;
        }
        for (int i = insertIndex + 1; i < rows.size(); i++) {
            Map<String, Object> row = rows.get(i);

            Object clusteredKey = row.get(clusteredIndexColumn);
            if (clusteredKey != null) {
                List<Integer> clusteredIndices = clusteredIndex.search(clusteredKey);
                if (clusteredIndices.contains(i - 1)) {
                    clusteredIndex.remove(clusteredKey, i - 1);
                    clusteredIndex.insert(clusteredKey, i);
                }
            }

            for (Map.Entry<String, Index> entry : indexes.entrySet()) {
                if (entry.getKey().contains("+")) {
                    // Composite index: extract multi-column key
                    String[] cols = entry.getKey().split("\\+");
                    List<Object> compositeKey = new ArrayList<>(cols.length);
                    boolean skip = false;
                    for (String col : cols) {
                        Object val = row.get(col);
                        if (val == null) { skip = true; break; }
                        compositeKey.add(val);
                    }
                    if (!skip) {
                        List<Integer> currentIndices = entry.getValue().search(compositeKey);
                        if (currentIndices.contains(i - 1)) {
                            entry.getValue().remove(compositeKey, i - 1);
                            entry.getValue().insert(compositeKey, i);
                        }
                    }
                } else {
                    Object key = row.get(entry.getKey());
                    if (key != null) {
                        List<Integer> currentIndices = entry.getValue().search(key);
                        if (currentIndices.contains(i - 1)) {
                            entry.getValue().remove(key, i - 1);
                            entry.getValue().insert(key, i);
                        }
                    }
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Bulk / deferred / bulk-load index optimisation  (Prompt 51)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Inserts multiple rows in one shot, rebuilding every index exactly once
     * at the end.  For a table with {@code N} existing rows and {@code M}
     * indexes the cost drops from O(N² × M × log N) (N individual inserts)
     * to O((N+K) × M × log(N+K)) where K is the batch size.
     *
     * <p>The rows are validated, sorted by clustered key (if present),
     * duplicate-checked and then merged into the row list.  All indexes are
     * rebuilt from scratch — no per-row shifting is performed.
     *
     * @param incomingRows the rows to insert
     * @throws IllegalArgumentException if a value is missing or has the wrong type
     * @throws IllegalStateException    if a unique/clustered key is duplicated
     */
    public void bulkInsert(List<Map<String, Object>> incomingRows) {
        if (incomingRows == null || incomingRows.isEmpty()) {
            return;
        }

        // Phase 1 – validate every row and collect batch-internal uniqueness keys.
        Set<Object> batchClusteredKeys = hasClusteredIndex ? new HashSet<>() : null;
        Map<String, Set<Object>> batchUniqueKeys = new HashMap<>();
        for (Map.Entry<String, Index> e : indexes.entrySet()) {
            if (e.getValue() instanceof UniqueIndex) {
                batchUniqueKeys.put(e.getKey(), new HashSet<>());
            }
        }

        List<Map<String, Object>> validatedRows = new ArrayList<>(incomingRows.size());
        for (Map<String, Object> row : incomingRows) {
            validatedRows.add(validateRowForBulk(row, batchClusteredKeys, batchUniqueKeys));
        }

        // Phase 2 – sort by clustered key so the final index build is optimal.
        if (hasClusteredIndex) {
            validatedRows.sort((a, b) -> compareKeys(
                    a.get(clusteredIndexColumn),
                    b.get(clusteredIndexColumn)));
        }

        // Phase 3-5 – merge, rebuild indexes, update stats (under write lock).
        tableLock.writeLock().lock();
        try {
            // Phase 3 – merge into the row list.
            rows.addAll(validatedRows);
            if (storage != null) {
                storage.setRows(new ArrayList<>(rows));
            }

            // Phase 4 – rebuild every index in a single pass.
            rebuildAllIndexes();

            // Phase 5 – update statistics.
            rowCount = getLiveRowCount();
            markStatsDirty();
        } finally {
            tableLock.writeLock().unlock();
        }
        LOGGER.log(Level.INFO, "Bulk inserted {0} rows into table {1} (total {2})",
                new Object[]{validatedRows.size(), name, rows.size()});
    }

    /**
     * Validates a single row for bulk insert: type-checks every value, fills
     * sequence defaults, and checks uniqueness against both the live index and
     * the batch-internal key sets.
     */
    private Map<String, Object> validateRowForBulk(Map<String, Object> row,
                                                   Set<Object> batchClusteredKeys,
                                                   Map<String, Set<Object>> batchUniqueKeys) {
        Map<String, Object> validatedRow = new HashMap<>();
        columns.forEach(col -> {
            Object value;
            Sequence sequence = sequences.get(col);
            if (sequence != null) {
                if (col.equals(primaryKeyColumn) && row.containsKey(col)) {
                    throw new IllegalArgumentException(
                            "Cannot manually specify value for sequence-based primary key column: " + col);
                }
                value = row.containsKey(col) ? row.get(col) : sequence.nextValue();
            } else if (!row.containsKey(col)) {
                throw new IllegalArgumentException("Missing value for column: " + col);
            } else {
                value = row.get(col);
            }

            Class<?> expectedType = columnTypes.get(col);
            if (expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (value == null) {
                validatedRow.put(col, null);
            } else {
                validateColumnValueType(col, expectedType, value);
                validatedRow.put(col, value);
            }
        });

        // Clustered uniqueness
        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException(
                        "Null key in clustered index column: " + clusteredIndexColumn);
            }
            if (!clusteredIndex.search(key).isEmpty() || !batchClusteredKeys.add(key)) {
                throw new IllegalStateException(ErrorMessages.DUPLICATE_KEY_PREFIX + key
                        + "' in column " + clusteredIndexColumn);
            }
        }

        // Secondary-unique uniqueness
        for (Map.Entry<String, Set<Object>> e : batchUniqueKeys.entrySet()) {
            String col = e.getKey();
            Set<Object> seen = e.getValue();
            Object value = validatedRow.get(col);
            if (value != null) {
                Index idx = indexes.get(col);
                if (idx != null && !idx.search(value).isEmpty()) {
                    throw new IllegalStateException(
                            ErrorMessages.DUPLICATE_KEY_PREFIX + value + ErrorMessages.ALREADY_EXISTS_SUFFIX
                                    + ErrorMessages.IN_COLUMN + col);
                }
                if (!seen.add(value)) {
                    throw new IllegalStateException(
                            ErrorMessages.DUPLICATE_KEY_PREFIX + value + ErrorMessages.ALREADY_EXISTS_SUFFIX
                                    + ErrorMessages.IN_COLUMN + col + " (within batch)");
                }
            }
        }

        return validatedRow;
    }

    /**
     * Disables all index maintenance.  Subsequent {@link #addRow} calls skip
     * index updates entirely — useful for bulk-load scenarios where indexes
     * will be rebuilt once at the end via {@link #enableAndRebuildIndices()}.
     */
    public void disableIndices() {
        indicesDisabled = true;
        LOGGER.log(Level.INFO, "Index maintenance disabled for table {0}", name);
    }

    /**
     * Re-enables index maintenance and rebuilds every index from the current
     * row data.  Call this after a bulk-load phase that was preceded by
     * {@link #disableIndices()}.
     */
    public void enableAndRebuildIndices() {
        indicesDisabled = false;
        rebuildAllIndexes();
        LOGGER.log(Level.INFO, "Index maintenance re-enabled and indexes rebuilt for table {0}", name);
    }

    /**
     * Starts deferred index mode: subsequent {@link #addRow} calls queue
     * their index operations instead of executing them immediately.  Call
     * {@link #flushDeferredIndexUpdates()} to execute all queued operations
     * and rebuild the final index state.
     */
    public void deferIndexUpdates() {
        deferredIndexOps = new ArrayList<>();
        LOGGER.log(Level.FINE, "Deferred index mode activated for table {0}", name);
    }

    /**
     * Executes all queued index operations and rebuilds every index from
     * the current row data, then exits deferred mode.
     */
    public void flushDeferredIndexUpdates() {
        if (deferredIndexOps == null) {
            return;
        }
        // Execute every queued operation (typically no-ops during bulk inserts,
        // but useful when mixed single-insert + bulk patterns are used).
        for (Runnable op : deferredIndexOps) {
            op.run();
        }
        deferredIndexOps = null;
        rebuildAllIndexes();
        LOGGER.log(Level.FINE, "Deferred index operations flushed and indexes rebuilt for table {0}", name);
    }

    /**
     * Returns whether index maintenance is currently disabled (bulk-load mode).
     *
     * @return true when index updates are skipped
     */
    public boolean isIndicesDisabled() {
        return indicesDisabled;
    }

    /**
     * Rebuilds every index (clustered + secondary) from the current row
     * data. Used by bulk insert, bulk-load rebuild, deferred flush and
     * deserialization.
     */
    private void rebuildAllIndexes() {
        rebuildClusteredIndexFromRows();
        rebuildSecondaryIndexes();
    }

    /**
     * Rebuilds the clustered index from current row data using bulk-load
     * O(N) construction. Rows are expected to already be in sorted order.
     */
    private void rebuildClusteredIndexFromRows() {
        if (!hasClusteredIndex) return;
        int n = rows.size();
        List<Object> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Object key = rows.get(i).get(clusteredIndexColumn);
            if (key != null) {
                keys.add(key);
                indices.add(i);
            }
        }
        clusteredIndex = new BTreeClusteredIndex(columnTypes.get(clusteredIndexColumn));
        clusteredIndex.bulkLoad(keys, indices);
    }

    /**
     * Rebuilds all secondary indexes from the current row data.
     * When multiple secondary indexes exist, each is built in parallel
     * via the shared {@link #INDEX_BUILD_POOL}.
     */
    private void rebuildSecondaryIndexes() {
        if (indexDefinitions.isEmpty()) return;
        int n = rows.size();

        List<Future<Map.Entry<String, Index>>> futures = new ArrayList<>();
        for (Map.Entry<String, String> entry : indexDefinitions.entrySet()) {
            futures.add(INDEX_BUILD_POOL.submit(() -> buildIndex(entry.getKey(), columnTypes.get(entry.getKey()), entry.getValue(), n)));
        }
        collectIndexResults(futures);
    }

    /**
     * Rebuilds only secondary indexes that are missing from the in-memory
     * {@code indexes} map. Used during deserialization when some indexes were
     * restored from serialized data and others need to be rebuilt from rows.
     */
    private void rebuildMissingSecondaryIndexes() {
        if (indexDefinitions.isEmpty()) return;
        int n = rows.size();

        List<Future<Map.Entry<String, Index>>> futures = new ArrayList<>();
        indexDefinitions.entrySet().stream()
                .filter(entry -> !indexes.containsKey(entry.getKey()))
                .forEach(entry -> futures.add(INDEX_BUILD_POOL.submit(
                        () -> buildIndex(entry.getKey(), columnTypes.get(entry.getKey()), entry.getValue(), n))));
        collectIndexResults(futures);
    }

    private Map.Entry<String, Index> buildIndex(String column, Class<?> keyType, String indexType, int n) {
        return switch (indexType) {
            case ErrorMessages.INDEX_BTREE -> buildBTreeIndex(column, keyType, n);
            case ErrorMessages.INDEX_HASH -> buildHashIndex(column, keyType, n);
            case ErrorMessages.INDEX_UNIQUE -> buildUniqueIndex(column, keyType, n);
            case ErrorMessages.INDEX_COMPOSITE_BTREE -> buildCompositeIndex(column, n);
            case ErrorMessages.INDEX_COVERING_BTREE -> buildCoveringIndex(column, keyType, n);
            default -> {
                LOGGER.log(Level.WARNING,
                        "Unknown index type ''{0}'' for column ''{1}'' in table {2}, skipping rebuild",
                        new Object[]{indexType, column, name});
                yield null;
            }
        };
    }

    private Map.Entry<String, Index> buildBTreeIndex(String column, Class<?> keyType, int n) {
        BTreeIndex index = new BTreeIndex(keyType);
        Map.Entry<List<Object>, List<Integer>> sorted = bulkLoadSortedKeys(column, n);
        index.bulkLoad(sorted.getKey(), sorted.getValue());
        return Map.entry(column, index);
    }

    private Map.Entry<String, Index> buildHashIndex(String column, Class<?> keyType, int n) {
        HashIndex index = new HashIndex(keyType);
        insertOneByOne(index, column, n);
        return Map.entry(column, index);
    }

    private Map.Entry<String, Index> buildUniqueIndex(String column, Class<?> keyType, int n) {
        UniqueIndex index = new UniqueIndex(keyType);
        insertOneByOne(index, column, n);
        return Map.entry(column, index);
    }

    private Map.Entry<String, Index> buildCompositeIndex(String column, int n) {
        List<String> cols = List.of(column.split("\\+"));
        CompositeBTreeIndex compIndex = new CompositeBTreeIndex(cols);
        List<List<Object>> compositeKeys = new ArrayList<>(n);
        List<Integer> compositeIndices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            List<Object> ck = new ArrayList<>(cols.size());
            boolean skip = false;
            for (String c : cols) {
                Object val = rows.get(i).get(c);
                if (val == null) { skip = true; break; }
                ck.add(val);
            }
            if (!skip) {
                compositeKeys.add(ck);
                compositeIndices.add(i);
            }
        }
        List<int[]> cpairs = new ArrayList<>(compositeKeys.size());
        for (int i = 0; i < compositeKeys.size(); i++) cpairs.add(new int[]{i});
        cpairs.sort((a, b) -> {
            CompositeBTreeIndex.CompositeKey k1 = new CompositeBTreeIndex.CompositeKey(compositeKeys.get(a[0]));
            CompositeBTreeIndex.CompositeKey k2 = new CompositeBTreeIndex.CompositeKey(compositeKeys.get(b[0]));
            return k1.compareTo(k2);
        });
        List<List<Object>> sortedCK = new ArrayList<>(compositeKeys.size());
        List<Integer> sortedCI = new ArrayList<>(compositeKeys.size());
        for (int[] p : cpairs) {
            sortedCK.add(compositeKeys.get(p[0]));
            sortedCI.add(compositeIndices.get(p[0]));
        }
        compIndex.bulkLoad(sortedCK, sortedCI);
        return Map.entry(column, compIndex);
    }

    private Map.Entry<String, Index> buildCoveringIndex(String column, Class<?> keyType, int n) {
        CoveringBTreeIndex coverIndex = new CoveringBTreeIndex(keyType, column, getCoverColumnNames(column));
        Map.Entry<List<Object>, List<Integer>> sorted = bulkLoadSortedKeys(column, n);
        coverIndex.bulkLoadWithCover(sorted.getKey(), sorted.getValue(), rows);
        return Map.entry(column, coverIndex);
    }

    private Map.Entry<List<Object>, List<Integer>> bulkLoadSortedKeys(String column, int n) {
        List<Object> keys = new ArrayList<>(n);
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Object key = rows.get(i).get(column);
            if (key != null) {
                keys.add(key);
                indices.add(i);
            }
        }
        List<int[]> pairs = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) pairs.add(new int[]{i});
        pairs.sort((a, b) -> {
            @SuppressWarnings("unchecked")
            Comparable<Object> c1 = (Comparable<Object>) keys.get(a[0]);
            return c1.compareTo(keys.get(b[0]));
        });
        List<Object> sortedKeys = new ArrayList<>(keys.size());
        List<Integer> sortedIdx = new ArrayList<>(keys.size());
        for (int[] pair : pairs) {
            sortedKeys.add(keys.get(pair[0]));
            sortedIdx.add(indices.get(pair[0]));
        }
        return Map.entry(sortedKeys, sortedIdx);
    }

    private void insertOneByOne(Index index, String column, int n) {
        for (int i = 0; i < n; i++) {
            Object key = rows.get(i).get(column);
            if (key != null) {
                index.insert(key, i);
            }
        }
    }

    private void collectIndexResults(List<Future<Map.Entry<String, Index>>> futures) {
        for (Future<Map.Entry<String, Index>> future : futures) {
            try {
                Map.Entry<String, Index> result = future.get();
                if (result != null) {
                    indexes.put(result.getKey(), result.getValue());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new QuerySyntaxException("", "Secondary index build interrupted");
            } catch (ExecutionException e) {
                throw new QuerySyntaxException("", "Secondary index build failed");
            }
        }
    }

    private String resolveFilePath(String tableName, String extension) {
        String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
        return dir + File.separator + tableName + extension;
    }

    private static String getConfigProperty(String key, String defaultValue) {
        return ConfigLoader.getString(key, defaultValue);
    }

    /** Returns the underlying {@link RowStorage} used by this table. */
    public RowStorage getStorage() {
        return storage;
    }

    /** Returns the storage type of this table. */
    public StorageType getStorageType() {
        return StorageType.fromString(System.getProperty("diesel.storage.type",
                getConfigProperty("storage.type", "in_memory")));
    }

    /**
     * Writes the table contents (header plus rows) to a CSV file in the data
     * directory. Each row is read under its lock while writing.
     *
     * <p>The save holds the table <em>write</em> lock (prompt 37): two
     * parallel {@code saveToFile} calls on the same table are serialised so
     * they can never interleave writes to the same file (paired with the
     * atomic {@link diesel.storage.AtomicFileWriter} temp+rename pattern).
     *
     * @param tableName the table name, used as the file base name
     * @throws RuntimeException if the file cannot be written
     */
    public void saveToFile(String tableName) {
        persistDirty = false;
        pendingPersistCount = 0;
        tableLock.writeLock().lock();
        try {
            if (storage != null && !(storage instanceof InMemoryRowStorage)) {
                storage.saveToFile(tableName);
                return;
            }
            if (storage == null || inMemoryPersistEnabled()) {
                writeLegacyCsv(tableName);
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Coalesced persistence (auto-commit write-behind)                 */
    /*                                                                   */
    /*  Every storage format rewrites the whole data file on saveToFile,  */
    /*  so an insert burst of N rows used to cost N whole-file writes     */
    /*  (O(N^2) bytes). Auto-commit DML now marks the table dirty and     */
    /*  folds the burst into a single write. Reads are served from the    */
    /*  in-memory rows, so read-your-writes is unaffected; the file is     */
    /*  flushed before anything can observe it from the outside:         */
    /*  explicit saveToFile / COMMIT, DDL, loadTablesFromDisk, the        */
    /*  pending-row threshold and the JVM shutdown hook.                  */
    /* ------------------------------------------------------------------ */

    /**
     * Auto-commit persist that coalesces a burst of mutations into one
     * whole-file write. Writes immediately when
     * {@code diesel.persist.max.pending} mutations are still unwritten, when
     * {@code diesel.persist.deferred} is {@code false}, or when the storage
     * keeps no durable file (in-memory tables without
     * {@code diesel.inmemory.persist}).
     */
    public void saveToFileDebounced(String tableName) {
        if (!isPersistFileBacked()) {
            persistDirty = false;
            pendingPersistCount = 0;
            return;
        }
        boolean flushNow;
        synchronized (this) {
            pendingPersistCount++;
            persistDirty = true;
            flushNow = pendingPersistCount >= persistMaxPending();
        }
        if (flushNow || !persistDeferredEnabled()) {
            flushPendingPersist(tableName);
        } else {
            registerPendingPersist(this);
            LOGGER.log(Level.FINE, "Coalesced persist for table {0} ({1} pending mutations)",
                    new Object[]{name, pendingPersistCount});
        }
    }

    /**
     * Writes the pending whole-file persist, if any. Safe to call when clean.
     */
    public void flushPendingPersist(String tableName) {
        if (!persistDirty) {
            return;
        }
        unregisterPendingPersist(this);
        saveToFile(tableName);
    }

    /**
     * Whether an auto-commit mutation has not reached the data file yet.
     */
    public boolean hasPendingPersist() {
        return persistDirty;
    }

    /** Tables whose last auto-commit mutation is not on disk yet. */
    private static final Map<String, Set<Table>> PENDING_PERSISTS = new ConcurrentHashMap<>();

    /**
     * Flushes the pending persist of every table in this JVM whose file could
     * be observed by an outside reader.
     */
    public static void flushAllPendingPersists() {
        for (Set<Table> tables : PENDING_PERSISTS.values()) {
            for (Table table : tables) {
                try {
                    table.flushPendingPersist(table.name);
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "Failed to flush pending persist for table " + table.name, e);
                }
            }
        }
    }

    /**
     * Flushes the pending persist of every table associated with the given data directory.
     */
    public static void flushPendingPersistsForDataDir(String dataDir) {
        Set<Table> tables = PENDING_PERSISTS.get(dataDir);
        if (tables != null) {
            for (Table table : tables) {
                try {
                    table.flushPendingPersist(table.name);
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "Failed to flush pending persist for table " + table.name, e);
                }
            }
        }
    }

    private static void registerPendingPersist(Table table) {
        PENDING_PERSISTS.computeIfAbsent(table.database.getDataDir(), k -> ConcurrentHashMap.newKeySet()).add(table);
    }

    private static void unregisterPendingPersist(Table table) {
        Set<Table> tables = PENDING_PERSISTS.get(table.database.getDataDir());
        if (tables != null) {
            tables.remove(table);
            if (tables.isEmpty()) {
                PENDING_PERSISTS.remove(table.database.getDataDir());
            }
        }
    }

    /** True when this table has a data file that a save would rewrite. */
    private boolean isPersistFileBacked() {
        return storage != null && !(storage instanceof InMemoryRowStorage);
    }

    private static boolean persistDeferredEnabled() {
        return !"false".equalsIgnoreCase(resolvePersistConfig("diesel.persist.deferred", "true"));
    }

    private static int persistMaxPending() {
        try {
            int configured = Integer.parseInt(resolvePersistConfig("diesel.persist.max.pending", "64").trim());
            return configured > 0 ? configured : 64;
        } catch (NumberFormatException e) {
            return 64;
        }
    }

    /** Sysprop overrides {@code config.properties}, matching the storage-type lookup. */
    private static String resolvePersistConfig(String key, String defaultValue) {
        String sysprop = System.getProperty(key);
        if (sysprop != null && !sysprop.isBlank()) {
            return sysprop;
        }
        return getConfigProperty(key, defaultValue);
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(Table::flushAllPendingPersists, "diesel-persist-flush"));
    }

    /**
     * Whether in-memory tables should be persisted to CSV files on every
     * save. Enabled in server mode ({@code diesel.DatabaseServer}) so that
     * server-terminated tables leave durable CSV files behind; embedded
     * {@link Database} usage keeps the in-memory fast path.
     */
    private static boolean inMemoryPersistEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("diesel.inmemory.persist", "false"));
    }

    private void writeLegacyCsv(String tableName) {
        String fileName = resolveFilePath(tableName, ".csv");
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName))) {
            BufferedWriter writer = afw.bufferedWriter();
            writer.write(String.join(",", columns));
            writer.write('\n');

            IntStream.range(0, rows.size())
                    .filter(i -> !isDeleted(i))
                    .forEach(i -> {
                        ReentrantReadWriteLock lock = getRowLock(i);
                        lock.readLock().lock();
                        try {
                            Map<String, Object> row = rows.get(i);
                            List<String> values = new ArrayList<>();
                            for (String column : columns) {
                                values.add(formatValue(row.get(column)));
                            }
                            try {
                                writer.write(String.join(",", values));
                                writer.write('\n');
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } finally {
                            lock.readLock().unlock();
                        }
                    });

            afw.commit();
            isFileInitialized = true;
            LOGGER.log(Level.INFO, "Table {0} saved to file {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return "\"" + value.toString().replace("\"", "\"\"") + "\"";
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime || value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        return value.toString();
    }

    /**
     * Serializes the whole table (schema, rows, index definitions, sequences)
     * to a {@code .table} file in the data directory.
     *
     * <p>The save holds the table <em>write</em> lock (prompt 37) so parallel
     * serialization calls on the same table cannot interleave file writes.
     *
     * @param tableName the table name, used as the file base name
     * @throws RuntimeException if the file cannot be written
     */
    public void saveToSerializedFile(String tableName) {
        flushPendingPersist(tableName);
        tableLock.writeLock().lock();
        try {
            if (getDeletedCount() > 0) {
                compact();
            }
            String fileName = resolveFilePath(tableName, ErrorMessages.TABLE_EXTENSION);
            try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
                ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
                oos.writeObject(this);
                oos.flush();
                afw.commit();
                isFileInitialized = true;
                LOGGER.log(Level.INFO, "Table {0} saved to file {1} with {2} rows",
                        new Object[]{tableName, fileName, rows.size()});
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
                throw new DieselIOException("Failed to save table to file: " + fileName, e);
            }
        } finally {
            tableLock.writeLock().unlock();
        }
    }

    /**
     * Loads a table from its serialized {@code .table} file in the data
     * directory, restoring the database reference and rebuilding the indexes.
     * When the file is missing, a new empty table with the base structure is
     * created; corrupt files or unsupported format versions yield null.
     *
     * @param database  the database to attach to the loaded table
     * @param tableName the table name, used as the file base name
     * @return the loaded table, a new empty table, or null on failure
     */
    public static Table loadFromFile(Database database, String tableName) {
        String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
        String fileName = dir + File.separator + tableName + ErrorMessages.TABLE_EXTENSION;
        File file = new File(fileName);
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.log(Level.INFO, "Serialized file {0} not found, creating new table {1} with base structure",
                    new Object[]{fileName, tableName});
            Table table = new Table(database, tableName, new ArrayList<>(), new HashMap<>(), null, new HashMap<String, Sequence>());
            table.formatVersion = CURRENT_FORMAT_VERSION;
            table.setFileInitialized(false);
            return table;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            Object obj = ois.readObject();
            if (obj instanceof Table table) {
                if (table.formatVersion > CURRENT_FORMAT_VERSION) {
                    throw new IllegalArgumentException("Unsupported table format version: " + table.formatVersion
                            + ", max supported: " + CURRENT_FORMAT_VERSION);
                }
                table.database = database;
                if (table.storage instanceof AbstractRowStorage ars) {
                    ars.setDataDir(dir);
                    if (table.primaryKeyColumn != null) {
                        ars.setPrimaryKeyColumn(table.primaryKeyColumn);
                    }
                }
                table.setFileInitialized(true);
                LOGGER.log(Level.INFO, "Table {0} loaded from file {1}", new Object[]{tableName, fileName});
                return table;
            }
            if (obj instanceof AbstractRowStorage.SerializedTableData data) {
                LOGGER.log(Level.INFO, "Table {0} .table contains storage mirror (SerializedTableData), "
                        + "loading via storage auto_mtime path", tableName);
                Table table = new Table(database, tableName,
                        new ArrayList<>(data.columns), new TreeMap<>(data.columnTypes),
                        null, new HashMap<String, Sequence>());
                table.formatVersion = CURRENT_FORMAT_VERSION;
                if (table.getStorage() instanceof AbstractRowStorage ars) {
                    ars.setDataDir(dir);
                    ars.loadFromFile(tableName);
                    table.reloadFromStorage();
                }
                table.setFileInitialized(true);
                return table;
            }
            throw new ClassNotFoundException("Unexpected object type in " + fileName + ": " + obj.getClass());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load table {0}, creating empty: {1}",
                    new Object[]{tableName, e.getMessage()});
            Table empty = new Table(database, tableName, new ArrayList<>(), new HashMap<>(),
                    null, new HashMap<String, Sequence>());
            empty.formatVersion = CURRENT_FORMAT_VERSION;
            return empty;
        }
    }
    
    /**
     * Restores a secondary index from serialized data.
     */
    private void restoreSecondaryIndex(ObjectInputStream ois, String tableName) {
        try {
            String key = ois.readUTF();
            byte[] indexBytes = (byte[]) ois.readObject();
            long storedChecksum = ois.readLong();
            long computedChecksum = computeChecksumFromBytes(indexBytes);
            if (storedChecksum != computedChecksum) {
                LOGGER.log(Level.WARNING,
                        "Checksum mismatch for index ''{0}'' in table {1}, will rebuild",
                        new Object[]{key, tableName});
            } else {
                Index idx = (Index) new ObjectInputStream(
                        new ByteArrayInputStream(indexBytes)).readObject();
                indexes.put(key, idx);
                LOGGER.log(Level.FINE,
                        "Restored index ''{0}'' from serialized data for table {1}",
                        new Object[]{key, tableName});
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Failed to deserialize index in table {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }
    
    /**
     * Restores the clustered index from serialized data.
     */
    private void restoreClusteredIndex(ObjectInputStream ois, String tableName) {
        try {
            boolean hasSerializedClustered = ois.readBoolean();
            if (hasSerializedClustered) {
                byte[] clusterBytes = (byte[]) ois.readObject();
                long storedChecksum = ois.readLong();
                long computedChecksum = computeChecksumFromBytes(clusterBytes);
                if (storedChecksum == computedChecksum) {
                    this.clusteredIndex = (BTreeClusteredIndex) new ObjectInputStream(
                            new ByteArrayInputStream(clusterBytes)).readObject();
                    restoredClustered = true;
                    LOGGER.log(Level.FINE,
                            "Restored clustered index from serialized data for table {0}",
                            tableName);
                } else {
                    LOGGER.log(Level.WARNING,
                            "Checksum mismatch for clustered index in table {0}, will rebuild",
                            tableName);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Failed to deserialize clustered index for table {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }
}
