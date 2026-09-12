package diesel.storage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import diesel.ErrorMessages;

/**
 * Format-agnostic index management for delimited storage backends. Maintains
 * a sorted primary-key index and optional per-column secondary indexes, can
 * read delimited files concurrently using a dedicated daemon
 * {@link ForkJoinPool}, and exposes (deprecated) block helpers that return
 * on-demand slices of the in-memory rows.
 *
 * <p>The row format (CSV, TSV, or any future delimited reader) is supplied via
 * a {@link RowReaderFactory}; configuration keys are namespaced by
 * {@code configPrefix}. Adding a new storage type only requires a reader that
 * implements {@link DelimitedRowReader} plus a factory reference.
 *
 * <p>Indexes are order-preserving red-black trees ({@link TreeMap}), so
 * primary-key lookups and range searches run in {@code O(log n)}. All index
 * structures are self-contained within the storage package and do not depend
 * on the engine's package-private B-tree classes.
 */
public abstract class DelimitedIndexManager {

    private static final Logger LOGGER = Logger.getLogger(DelimitedIndexManager.class.getName());

    private static final Comparator<Object> KEY_ORDER = (a, b) -> {
        if (a != null && b != null && a instanceof Comparable && b instanceof Comparable) {
            try {
                @SuppressWarnings("unchecked")
                Comparable<Object> ca = (Comparable<Object>) a;
                return ca.compareTo(b);
            } catch (ClassCastException ignored) {
                return String.valueOf(a).compareTo(String.valueOf(b));
            }
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    };

    /**
     * Shared daemon pool used for parallel file reads and parallel block loads.
     * Daemon threads so the pool does not block JVM exit.
     */
    private static final ForkJoinPool READ_POOL = new ForkJoinPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("diesel-read-" + t.getPoolIndex());
                return t;
            },
            null, true);

    private static final int DEFAULT_BLOCK_SIZE = 1000;
    private static final long DEFAULT_PARALLEL_READ_THRESHOLD = 10000;

    private static final java.util.Properties ROOT_CONFIG = loadRootConfig();

    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final RowReaderFactory rowReaderFactory;
    private final String configPrefix;
    private final boolean multiLineRows;
    private final RowArrays rowColumns;

    /** Rows are kept as compact Object[] arrays (prompt 36) so the index manager
     * shares the very same row objects as the storage instead of a second set of
     * per-row HashMaps. Value lookups go through {@link #rowColumns}. */
    private List<Object[]> rows = new ArrayList<>();
    private String primaryKeyColumn;
    private final NavigableMap<Object, Long> primaryKeyIndex = new TreeMap<>(KEY_ORDER);
    private final Map<String, NavigableMap<Object, List<Long>>> secondaryIndexes =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private long nextRowId = 0;
    private final NavigableMap<Long, Integer> rowIdToPosition = new TreeMap<>();
    private final Set<Long> deletedRowIds = new HashSet<>();
    private static final double COMPACTION_THRESHOLD = 0.25;

    /** Whether a deferred bulk-update window is open (prompt 35). Inside the
     * window per-operation index mutations are replaced by a single rebuild on
     * {@link #endBulkUpdate()}. Not thread-safe: bulk mode must be driven by a
     * single writer context (e.g. under the table write lock). */
    private boolean bulkMode;
    /** Whether the index state is stale since the bulk window opened. */
    private boolean bulkDirty;

    private final int blockSize;
    private final long parallelReadThreshold;

    private final AtomicBoolean deprecationLogged = new AtomicBoolean();

    private volatile LineIndexCache lineIndexCache;

    /**
     * @param tableName       the table name
     * @param columns         the ordered column names of the underlying schema
     * @param columnTypes     column name to type mapping of the underlying schema
     * @param rowReaderFactory factory opening a row reader for the storage format
     * @param configPrefix    namespace for {@code .block.size} and
     *                        {@code .parallel.read.threshold} config keys
     * @param multiLineRows   whether a single logical row can span several physical
     *                        lines (requires a pre-scan before parallel reading)
     */
    protected DelimitedIndexManager(String tableName, List<String> columns, Map<String, Class<?>> columnTypes,
                                    RowReaderFactory rowReaderFactory, String configPrefix, boolean multiLineRows) {
        this.tableName = tableName == null ? "" : tableName;
        this.columns = new ArrayList<>(columns == null ? List.of() : columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnTypes != null) {
            this.columnTypes.putAll(columnTypes);
        }
        this.rowReaderFactory = rowReaderFactory;
        this.configPrefix = configPrefix == null ? "" : configPrefix;
        this.multiLineRows = multiLineRows;
        this.rowColumns = new RowArrays(this.columns);
        this.blockSize = Math.max(1, readIntSetting("block.size", DEFAULT_BLOCK_SIZE));
        this.parallelReadThreshold = Math.max(1, readLongSetting("parallel.read.threshold", DEFAULT_PARALLEL_READ_THRESHOLD));
    }

    /** Returns the table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the ordered column names of the underlying schema. */
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    // ─── Index construction ─────────────────────────────────────────

    /**
     * Adopts the given rows (as column-to-value maps, converted to compact
     * Object[] arrays internally) and builds the primary-key index. Any
     * previously configured indexes and cached blocks are discarded.
     *
     * @param data             the rows to index, or {@code null} for an empty table
     * @param primaryKeyColumn the primary-key column name, or {@code null} for none
     */
    public void buildIndexes(List<Map<String, Object>> data, String primaryKeyColumn) {
        List<Object[]> arrays = new ArrayList<>();
        if (data != null) {
            for (Map<String, Object> row : data) {
                arrays.add(rowColumns.fromMap(row));
            }
        }
        buildIndexesFromArrays(arrays, primaryKeyColumn);
    }

    /**
     * Adopts compact Object[] rows directly, sharing the row objects with the
     * storage instead of copying them into Map form. See
     * {@link #buildIndexes(List, String)}.
     */
    private void buildIndexesFromArrays(List<Object[]> data, String primaryKeyColumn) {
        this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        this.primaryKeyIndex.clear();
        this.secondaryIndexes.clear();
        this.rowIdToPosition.clear();
        this.deletedRowIds.clear();
        this.nextRowId = 0;
        reindex();
    }

    /**
     * Creates (or returns) a secondary index over the given column. The index
     * maps each non-null key to the row indexes holding it.
     *
     * @param column the column to index
     * @return {@code true} if the index is available afterwards
     */
    public boolean createIndex(String column) {
        String canonical = resolveColumn(column);
        if (canonical == null) {
            return false;
        }
        if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(canonical)) {
            return true;
        }
        if (secondaryIndexes.containsKey(canonical)) {
            return true;
        }
        NavigableMap<Object, List<Long>> index = new TreeMap<>(KEY_ORDER);
        for (int i = 0; i < rows.size(); i++) {
            Long rid = positionToRowId(i);
            if (rid == null) {
                continue;
            }
            Object key = rowColumns.get(rows.get(i), canonical);
            if (key != null) {
                index.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
            }
        }
        secondaryIndexes.put(canonical, index);
        return true;
    }

    /** Rebuilds the primary-key index and every secondary index from the current rows. */
    public void reindex() {
        primaryKeyIndex.clear();
        for (NavigableMap<Object, List<Long>> index : secondaryIndexes.values()) {
            index.clear();
        }
        rowIdToPosition.clear();
        deletedRowIds.clear();
        nextRowId = 0;
        for (int i = 0; i < rows.size(); i++) {
            long rid = nextRowId++;
            rowIdToPosition.put(rid, i);
            insertIndexedRow(rows.get(i), rid);
        }
    }

    /**
     * Injects a new row into all maintained indexes.
     *
     * @param row  the compact array row
     * @param rowId the stable row identifier
     */
    public void insertIndexedRow(Object[] row, long rowId) {
        if (primaryKeyColumn != null) {
            Object key = rowColumns.get(row, primaryKeyColumn);
            if (key != null) {
                primaryKeyIndex.put(key, rowId);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = rowColumns.get(row, entry.getKey());
            if (key != null) {
                entry.getValue().computeIfAbsent(key, k -> new ArrayList<>()).add(rowId);
            }
        }
    }

    /**
     * Appends a row at the given position (end of storage). Assigns a new
     * rowId and registers it in the position map. No position shifting.
     *
     * @param row      the row data as a column-to-value map
     * @param rowIndex the position (should be at the end)
     */
    public void appendIndexedRow(Map<String, Object> row, int rowIndex) {
        appendIndexedRowShared(rowColumns.fromMap(row), rowIndex);
    }

    /**
     * Appends a compact Object[] row, keeping the very same array object so the
     * storage and this manager observe one shared representation per row.
     */
    void appendIndexedRowShared(Object[] row, int rowIndex) {
        long rid = nextRowId++;
        rowIdToPosition.put(rid, rowIndex);
        if (rowIndex >= rows.size()) {
            rows.add(row);
        } else {
            rows.add(rowIndex, row);
        }
        insertIndexedRow(row, rid);
    }

    /**
     * Removes a row from all maintained indexes.
     *
     * @param row  the compact array row
     * @param rowId the stable row identifier to disassociate
     */
    public void removeIndexedRow(Object[] row, long rowId) {
        if (primaryKeyColumn != null) {
            Object key = rowColumns.get(row, primaryKeyColumn);
            if (key != null && rowId == primaryKeyIndex.getOrDefault(key, -1L)) {
                primaryKeyIndex.remove(key);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = rowColumns.get(row, entry.getKey());
            if (key == null) {
                continue;
            }
            List<Long> ids = entry.getValue().get(key);
            if (ids != null) {
                ids.remove(rowId);
                if (ids.isEmpty()) {
                    entry.getValue().remove(key);
                }
            }
        }
    }

    /**
     * Updates a row in all maintained indexes at the given physical position.
     * The rowId stays the same; only the key-to-rowId mappings are refreshed.
     *
     * @param oldRow  the previous compact array row
     * @param rowIndex the physical position
     * @param newRow  the new compact array row
     */
    public void updateRow(Object[] oldRow, int rowIndex, Object[] newRow) {
        Long rid = positionToRowId(rowIndex);
        if (rid == null) {
            return;
        }
        removeIndexedRow(oldRow, rid);
        insertIndexedRow(newRow, rid);
    }

    // ─── Stable row-id: insertAt / deleteRow / compact ───────────────

    /**
     * Inserts a row at the given physical position, shifting all later
     * positions up by one. Assigns a new stable rowId. Index key→rowId
     * mappings for existing rows are <em>not</em> touched — only the
     * position map is updated. O(n) in the number of rows after the
     * insertion point.
     *
     * <p>Inside a bulk-update window (see {@link #beginBulkUpdate()}) the row
     * is added to the mirror list and the whole index state is deferred to the
     * single rebuild at {@link #endBulkUpdate()} — no position shifting and no
     * per-row index mutation. Querying the index before the window closes is
     * undefined.
     *
     * @param rowIndex the zero-based position at which to insert
     * @param row      the row data as a column-to-value map
     */
    public void insertAt(int rowIndex, Map<String, Object> row) {
        insertAtShared(rowIndex, rowColumns.fromMap(row));
    }

    /**
     * Inserts a compact Object[] row, keeping the very same array object as the
     * storage so both share one row representation. See
     * {@link #insertAt(int, Map)} for the semantics.
     */
    void insertAtShared(int rowIndex, Object[] row) {
        if (bulkMode) {
            rows.add(rowIndex, row);
            bulkDirty = true;
            return;
        }
        long rid = nextRowId++;
        rowIdToPosition.put(rid, rowIndex);
        if (rowIndex >= rows.size()) {
            rows.add(row);
        } else {
            rows.add(rowIndex, row);
        }
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() >= rowIndex && e.getKey() != rid) {
                e.setValue(e.getValue() + 1);
            }
        }
        insertIndexedRow(row, rid);
    }

    /**
     * Marks the row at the given physical position as deleted (tombstone).
     * Shifts all later positions down by one. Triggers a compaction when
     * the fraction of deleted rowIds exceeds {@link #COMPACTION_THRESHOLD}.
     *
     * <p>Inside a bulk-update window (see {@link #beginBulkUpdate()}) the row
     * is removed from the mirror list and the whole index state is deferred to
     * the single rebuild at {@link #endBulkUpdate()} — no position shifting
     * and no tombstone compaction. Querying the index before the window closes
     * is undefined.
     *
     * @param rowIndex the zero-based position of the row to delete
     */
    public void deleteRow(int rowIndex) {
        if (bulkMode) {
            if (rowIndex < 0 || rowIndex >= rows.size()) {
                return;
            }
            rows.remove(rowIndex);
            bulkDirty = true;
            return;
        }
        Long rid = positionToRowId(rowIndex);
        if (rid == null) {
            return;
        }
        Object[] row = rows.remove(rowIndex);
        removeIndexedRow(row, rid);
        rowIdToPosition.remove(rid);
        deletedRowIds.add(rid);
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() > rowIndex) {
                e.setValue(e.getValue() - 1);
            }
        }
        int liveRows = rowIdToPosition.size();
        if (liveRows > 0 && (double) deletedRowIds.size() / liveRows > COMPACTION_THRESHOLD) {
            compact();
        }
    }

    /**
     * Full rebuild: clears all tombstones, reassigns rowIds sequentially,
     * and rebuilds every index from scratch.
     */
    public void compact() {
        reindex();
    }

    /** Returns the number of tombstoned (deleted) rowIds pending compaction. */
    public int getDeletedCount() {
        return deletedRowIds.size();
    }

    // ─── Deferred bulk updates (prompt 35) ─────────────────────────

    /**
     * Enters a deferred bulk-update window. Until {@link #endBulkUpdate()} is
     * called, {@link #insertAt(int, Map)} and {@link #deleteRow(int)} only
     * mutate the mirror row list and mark the index state dirty, deferring all
     * position shifting and rebuilds to the single rebuild that closes the
     * window. Index state is not query-consistent while the window is open.
     */
    public void beginBulkUpdate() {
        bulkMode = true;
    }

    /**
     * Leaves a deferred bulk-update window and performs the single index
     * rebuild accumulated while dirty, restoring a query-consistent state
     * (rowIds reassigned, positions and tombstones recomputed, every index
     * rebuilt). Must be paired with {@link #beginBulkUpdate()}, even on
     * exceptional paths.
     */
    public void endBulkUpdate() {
        bulkMode = false;
        if (bulkDirty) {
            bulkDirty = false;
            reindex();
        }
    }

    /** Returns whether a deferred bulk-update window is currently open. */
    public boolean isBulkUpdating() {
        return bulkMode;
    }

    /**
     * Adopts a wholesale row replacement for indexing. Outside a bulk-update
     * window this is a full immediate rebuild (the behaviour of
     * {@link #buildIndexes(List, String)}). Inside one the new rows are snapped
     * into the mirror list without rebuilding the indexes, which happens once
     * at {@link #endBulkUpdate()}.
     *
     * @param data             the rows to index, or {@code null} for an empty table
     * @param primaryKeyColumn the primary-key column name, or {@code null} for none
     */
    public void markIndexDirty(List<Map<String, Object>> data, String primaryKeyColumn) {
        if (!bulkMode) {
            buildIndexes(data, primaryKeyColumn);
            return;
        }
        List<Object[]> arrays = new ArrayList<>();
        if (data != null) {
            for (Map<String, Object> row : data) {
                arrays.add(rowColumns.fromMap(row));
            }
        }
        markIndexDirtyFromArrays(arrays, primaryKeyColumn);
    }

    /**
     * Adopts a wholesale replacement of compact Object[] rows. Outside a
     * bulk-update window this is a full immediate rebuild; inside one the new
     * rows are snapped into the mirror list (sharing the storage's arrays) and
     * the indexes are rebuilt once at {@link #endBulkUpdate()}.
     */
    void markIndexDirtyFromArrays(List<Object[]> data, String primaryKeyColumn) {
        if (!bulkMode) {
            buildIndexesFromArrays(data, primaryKeyColumn);
            return;
        }
        this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        bulkDirty = true;
    }

    // ─── Internal row-id helpers ────────────────────────────────────

    /**
     * Returns the rowId at the given physical position, or {@code null}
     * if no mapping exists (e.g. after a raw list mutation without an
     * index sync).
     */
    private Long positionToRowId(int position) {
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() == position) {
                return e.getKey();
            }
        }
        return null;
    }

    /**
     * Translates a collection of rowIds into their current physical
     * positions, sorted ascending, omitting any rowIds whose position
     * is unknown.
     */
    private List<Integer> translateRowIdsToPositions(List<Long> rowIds) {
        List<Integer> result = new ArrayList<>(rowIds.size());
        for (Long rid : rowIds) {
            Integer pos = rowIdToPosition.get(rid);
            if (pos != null) {
                result.add(pos);
            }
        }
        result.sort(Integer::compareTo);
        return result;
    }

    // ─── Index queries ───────────────────────────────────────────────

    /** Returns the configured primary-key column, or {@code null}. */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /** Returns the column names that have a maintained index. */
    public List<String> getIndexColumns() {
        List<String> result = new ArrayList<>();
        if (primaryKeyColumn != null) {
            result.add(primaryKeyColumn);
        }
        result.addAll(secondaryIndexes.keySet());
        return result;
    }

    /**
     * Exact primary-key search, {@code O(log n)}.
     *
     * @param key the primary-key value
     * @return the matching row index, or an empty list when absent
     */
    public List<Integer> searchByPrimaryKey(Object key) {
        if (key == null || primaryKeyColumn == null || primaryKeyIndex.isEmpty()) {
            return List.of();
        }
        Long rowId = primaryKeyIndex.get(key);
        if (rowId == null) {
            return List.of();
        }
        Integer pos = rowIdToPosition.get(rowId);
        return pos == null ? List.of() : List.of(pos);
    }

    /**
     * Exact equality search over the primary-key or a secondary index.
     *
     * @param column the column to search (case-insensitive)
     * @param key    the value to look up
     * @return the matching row indexes, possibly empty
     */
    public List<Integer> search(String column, Object key) {
        if (column == null || key == null) {
            return List.of();
        }
        if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(column)) {
            return searchByPrimaryKey(key);
        }
        NavigableMap<Object, List<Long>> index = secondaryIndexes.get(column);
        if (index == null) {
            return List.of();
        }
        List<Long> ids = index.get(key);
        return ids == null ? List.of() : translateRowIdsToPositions(ids);
    }

    /**
     * Inclusive range search over the primary-key or a secondary index.
     *
     * @param column the column to search (case-insensitive)
     * @param low    the inclusive lower bound, or {@code null} for open-ended
     * @param high   the inclusive upper bound, or {@code null} for open-ended
     * @return the matching row indexes, possibly empty
     */
    public List<Integer> rangeSearch(String column, Object low, Object high) {
        if (column == null) {
            return List.of();
        }
        if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(column)) {
            return collectPositions(subRange(primaryKeyIndex, low, high));
        }
        NavigableMap<Object, List<Long>> index = secondaryIndexes.get(column);
        if (index == null) {
            return List.of();
        }
        return collectPositions(subRange(index, low, high));
    }

    // ─── Block slicing (deprecated cache API, prompt 33) ───────────────

    /** Returns the number of fixed-size blocks the current rows are split into. */
    public int getNumBlocks() {
        int count = rowIdToPosition.size();
        if (count == 0) {
            return 0;
        }
        return (count + blockSize - 1) / blockSize;
    }

    /** Returns the number of rows per block. */
    public int getBlockSize() {
        return blockSize;
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return rowIdToPosition.size();
    }

    /**
     * Deprecated stub (prompt 33): the LRU block cache layer was removed
     * because rows are already fully in memory and the cache never performed
     * real I/O. Returns a fresh on-demand slice of the in-memory rows without
     * caching.
     *
     * @param blockIndex the zero-based block index
     * @return a new block holding the rows of the given slice
     */
    @Deprecated
    public Block getBlock(int blockIndex) {
        int numBlocks = getNumBlocks();
        if (blockIndex < 0 || blockIndex >= numBlocks) {
            throw new IndexOutOfBoundsException("Block index " + blockIndex + " out of range [0, " + numBlocks + ")");
        }
        logDeprecated("getBlock(int)");
        int from = blockIndex * blockSize;
        int to = Math.min(from + blockSize, rows.size());
        List<Map<String, Object>> blockRows = new ArrayList<>();
        List<Object[]> slice = rows.subList(from, Math.max(from, to));
        for (Object[] row : slice) {
            blockRows.add(rowColumns.toMap(row));
        }
        return new Block(blockIndex, blockRows);
    }

    /**
     * Deprecated stub (prompt 33): assembles the blocks sequentially on demand.
     *
     * @return the blocks in ascending block order
     */
    @Deprecated
    public List<Block> loadAllBlocksParallel() {
        logDeprecated("loadAllBlocksParallel()");
        int numBlocks = getNumBlocks();
        List<Block> result = new ArrayList<>(numBlocks);
        for (int i = 0; i < numBlocks; i++) {
            result.add(getBlock(i));
        }
        return result;
    }

    /** Deprecated stub: the block cache no longer exists, so hits are always zero. */
    @Deprecated
    public long getCacheHitCount() {
        logDeprecated("getCacheHitCount()");
        return 0;
    }

    /** Deprecated stub: the block cache no longer exists, so misses are always zero. */
    @Deprecated
    public long getCacheMissCount() {
        logDeprecated("getCacheMissCount()");
        return 0;
    }

    /** Deprecated no-op stub: there is no cache to invalidate. */
    @Deprecated
    public void invalidateCache() {
        logDeprecated("invalidateCache()");
    }

    /** Logs a deprecation warning once per manager instance. */
    private void logDeprecated(String method) {
        if (deprecationLogged.compareAndSet(false, true)) {
            LOGGER.log(Level.WARNING, "DEPRECATED: {0}.{1} is a stub — the LRU block cache layer was removed "
                            + "in prompt 33 because rows are already fully in memory; blocks are sliced on demand",
                    new Object[]{getClass().getSimpleName(), method});
        }
    }

    // ─── Parallel file reading ───────────────────────────────────────

    /**
     * Reads the whole delimited file. Uses parallel chunk readers on the shared
     * daemon pool when the file is large enough, otherwise falls back to a single
     * sequential pass.
     *
     * <p>The parallel path (prompt 34) is driven by a single byte-level
     * pre-scan ({@link #preScan(File)}) that records the file offset of every
     * data line and detects physical lines that continue an unterminated
     * multi-line logical row. Data lines are then partitioned by offset ranges
     * instead of line counts: each task reads only the bytes of its own range
     * once, so the total I/O stays close to the file size and rows are never
     * re-read from the beginning per partition.
     *
     * @param filePath the path to the delimited file
     * @return the decoded rows in file order
     * @throws IOException on I/O errors
     */
    public List<Map<String, Object>> loadFromFileParallel(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return List.of();
        }
        LineIndex lineIndex = preScan(file);
        long totalRows = lineIndex.dataLineCount();
        if (totalRows == 0) {
            return List.of();
        }
        if (totalRows < parallelReadThreshold) {
            return loadFromFileSequential(filePath);
        }
        if (lineIndex.hasMultiLineRows) {
            return loadFromFileSequential(filePath);
        }
        if (totalRows > Integer.MAX_VALUE) {
            return loadFromFileSequential(filePath);
        }
        int[] columnMapping = readHeaderMapping(file);
        long[] offsets = lineIndex.dataLineOffsets;
        int partitions = Math.min(READ_POOL.getParallelism(),
                (int) Math.min((totalRows + blockSize - 1) / blockSize, Integer.MAX_VALUE));
        List<Callable<List<Map<String, Object>>>> tasks = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            long startLine = (totalRows * p) / partitions;
            long endLine = (totalRows * (p + 1)) / partitions;
            if (endLine <= startLine) {
                continue;
            }
            long byteStart = offsets[(int) startLine];
            long byteEnd = endLine < totalRows ? offsets[(int) endLine] : file.length();
            long firstDataLine = startLine + 2;
            tasks.add(new ByteRangeTask(file, byteStart, byteEnd, firstDataLine, columnMapping));
        }
        List<Map<String, Object>> result = new ArrayList<>((int) Math.min(totalRows, Integer.MAX_VALUE));
        try {
            for (Future<List<Map<String, Object>>> future : READ_POOL.invokeAll(tasks)) {
                result.addAll(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("Parallel file read interrupted", e);
        } catch (ExecutionException e) {
            throw new CompletionException("Parallel file read failed", e.getCause());
        }
        return result;
    }

    /** Reads the whole delimited file with a single sequential pass. */
    public List<Map<String, Object>> loadFromFileSequential(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return List.of();
        }
        try (BufferedReader bufferedReader = StorageConfig.newReader(file);
             DelimitedRowReader reader = rowReaderFactory.create(bufferedReader, columns, columnTypes)) {
            reader.readHeader();
            return reader.readAll();
        }
    }

    /**
     * Loads the delimited file (parallel when beneficial) and builds the
     * primary-key index.
     *
     * @param filePath         the path to the delimited file
     * @param primaryKeyColumn the primary-key column, or {@code null} for none
     * @param parallel         whether to allow the parallel read path
     * @return the decoded rows
     * @throws IOException on I/O errors
     */
    public List<Map<String, Object>> loadAndIndex(String filePath, String primaryKeyColumn, boolean parallel)
            throws IOException {
        List<Map<String, Object>> loaded = parallel
                ? loadFromFileParallel(filePath)
                : loadFromFileSequential(filePath);
        buildIndexes(loaded, primaryKeyColumn);
        return loaded;
    }

    // ─── Byte-offset pre-scan (prompt 34) ────────────────────────────

    /**
     * Returns the cached {@link LineIndex} for the file, re-scanning only when
     * the file's mtime or size no longer matches the cached snapshot. The scan
     * reads the file bytes once, records the byte offset of every data line and
     * detects multi-line physical rows, so it replaces the former two full-line
     * scans ({@code countDataLines} + the multi-line probe).
     *
     * @param file the delimited file
     * @return the line index of the file
     * @throws IOException on I/O errors
     */
    private LineIndex preScan(File file) throws IOException {
        LineIndexCache cached = lineIndexCache;
        if (cached != null && cached.matches(file)) {
            return cached.index;
        }
        LineIndex index = scanLines(file);
        try {
            lineIndexCache = new LineIndexCache(file.getAbsolutePath(), file.lastModified(), file.length(), index);
        } catch (SecurityException ignored) {
        }
        return index;
    }

    /**
     * Single byte-level pass over the file: splits physical lines exactly like
     * {@link BufferedReader#readLine()} ({@code \n}, {@code \r\n} and lone
     * {@code \r} terminators) and records the byte offset of each data line
     * start (the header is the first physical line and is skipped). When the
     * format can embed line breaks inside a logical row, every data line is
     * also probed via {@link #lineEndsInsideMultilineRow(String)}.
     */
    private LineIndex scanLines(File file) throws IOException {
        byte[] bytes;
        try (InputStream in = new java.io.BufferedInputStream(new FileInputStream(file))) {
            bytes = in.readAllBytes();
        }
        java.nio.charset.Charset charset = StorageConfig.getCharset();
        long[] offsets = new long[1024];
        int count = 0;
        boolean hasMultiLine = false;
        int lineStart = 0;
        int i = 0;
        while (i < bytes.length) {
            byte b = bytes[i];
            if (b == (byte) '\r' || b == (byte) '\n') {
                if (lineStart > 0) {
                    if (count == offsets.length) {
                        offsets = grow(offsets);
                    }
                    offsets[count++] = lineStart;
                    if (multiLineRows) {
                        String text = new String(bytes, lineStart, i - lineStart, charset);
                        if (lineEndsInsideMultilineRow(text)) {
                            hasMultiLine = true;
                        }
                    }
                }
                if (b == (byte) '\r' && i + 1 < bytes.length && bytes[i + 1] == (byte) '\n') {
                    i += 2;
                } else {
                    i += 1;
                }
                lineStart = i;
            } else {
                i += 1;
            }
        }
        if (lineStart > 0 && lineStart < bytes.length) {
            if (count == offsets.length) {
                offsets = grow(offsets);
            }
            offsets[count++] = lineStart;
            if (multiLineRows) {
                String text = new String(bytes, lineStart, bytes.length - lineStart, charset);
                if (lineEndsInsideMultilineRow(text)) {
                    hasMultiLine = true;
                }
            }
        }
        long[] trimmed = count == offsets.length ? offsets : java.util.Arrays.copyOf(offsets, count);
        return new LineIndex(trimmed, hasMultiLine);
    }

    private static long[] grow(long[] array) {
        return java.util.Arrays.copyOf(array, array.length * 2);
    }

    /**
     * Parses the file header once and returns the header-to-schema column
     * mapping shared by every partition reader. Consumes only the first
     * physical line; the header is not re-read per partition.
     */
    private int[] readHeaderMapping(File file) throws IOException {
        try (BufferedReader bufferedReader = StorageConfig.newReader(file);
             DelimitedRowReader reader = rowReaderFactory.create(bufferedReader, columns, columnTypes)) {
            reader.readHeader();
            int[] mapping = reader.columnMapping();
            if (mapping == null) {
                throw new IOException("Header was not parsed for parallel read");
            }
            return mapping;
        }
    }

    /**
     * Format-specific hook consulted by the byte pre-scan: returns whether the
     * given physical line text ends inside a logical row that continues on the
     * next physical line (e.g. an unterminated CSV quoted field). The default
     * assumes every physical line is a complete row.
     *
     * @param physicalLine the decoded text of one physical line
     * @return {@code true} when the line must not be treated as a row boundary
     */
    protected boolean lineEndsInsideMultilineRow(String physicalLine) {
        return false;
    }

    // ─── Internal helpers ────────────────────────────────────────────

    private String resolveColumn(String column) {
        if (column == null) {
            return null;
        }
        for (String c : columns) {
            if (c.equalsIgnoreCase(column)) {
                return c;
            }
        }
        return column;
    }

    private static Map<Object, ?> subRange(NavigableMap<Object, ?> map, Object low, Object high) {
        if (map.isEmpty()) {
            return map;
        }
        if (low != null && high != null) {
            if (KEY_ORDER.compare(low, high) > 0) {
                return new TreeMap<>(KEY_ORDER);
            }
            return map.subMap(low, true, high, true);
        }
        if (low != null) {
            return map.tailMap(low, true);
        }
        if (high != null) {
            return map.headMap(high, true);
        }
        return map;
    }

    private List<Integer> collectPositions(Map<Object, ?> entries) {
        if (entries.isEmpty()) {
            return List.of();
        }
        List<Long> rowIds = new ArrayList<>();
        for (Object value : entries.values()) {
            if (value instanceof Long rid) {
                rowIds.add(rid);
            } else if (value instanceof List<?> ids) {
                for (Object id : ids) {
                    if (id instanceof Long rid) {
                        rowIds.add(rid);
                    }
                }
            }
        }
        return translateRowIdsToPositions(rowIds);
    }

    private static java.util.Properties loadRootConfig() {
        java.util.Properties props = new java.util.Properties();
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (Exception ignored) {
            LOGGER.log(Level.FINE, "Config error for index manager, using defaults: {0}", ignored.getMessage());
        }
        return props;
    }

    private int readIntSetting(String suffix, int defaultValue) {
        String override = System.getProperty(configPrefix + "." + suffix);
        if (override != null) {
            try {
                return Integer.parseInt(override.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        String configured = ROOT_CONFIG.getProperty(configPrefix + "." + suffix);
        if (configured != null) {
            try {
                return Integer.parseInt(configured.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private long readLongSetting(String suffix, long defaultValue) {
        String override = System.getProperty(configPrefix + "." + suffix);
        if (override != null) {
            try {
                return Long.parseLong(override.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        String configured = ROOT_CONFIG.getProperty(configPrefix + "." + suffix);
        if (configured != null) {
            try {
                return Long.parseLong(configured.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    /** The result of the byte-offset pre-scan: the file offset of every data
     * line start plus a flag for multi-line logical rows. */
    private static final class LineIndex {
        final long[] dataLineOffsets;
        final boolean hasMultiLineRows;

        LineIndex(long[] dataLineOffsets, boolean hasMultiLineRows) {
            this.dataLineOffsets = dataLineOffsets;
            this.hasMultiLineRows = hasMultiLineRows;
        }

        int dataLineCount() {
            return dataLineOffsets.length;
        }
    }

    /** Cached pre-scan keyed by the file identity, mtime and size. */
    private static final class LineIndexCache {
        final String path;
        final long lastModified;
        final long length;
        final LineIndex index;

        LineIndexCache(String path, long lastModified, long length, LineIndex index) {
            this.path = path;
            this.lastModified = lastModified;
            this.length = length;
            this.index = index;
        }

        boolean matches(File file) {
            return path.equals(file.getAbsolutePath())
                    && lastModified == file.lastModified()
                    && length == file.length();
        }
    }

    /**
     * Reads one byte-offset partition of a delimited file. Positions a
     * {@link FileChannel} at the byte offset of the partition's first data line
     * and reads exactly the bytes up to the next line boundary (or EOF), so no
     * line is ever re-read from the file start. The header-to-schema column
     * mapping is parsed once by the main thread and shared by all partitions.
     */
    private final class ByteRangeTask implements Callable<List<Map<String, Object>>> {
        private final File file;
        private final long byteStart;
        private final long byteEnd;
        private final long firstDataLine;
        private final int[] columnMapping;

        ByteRangeTask(File file, long byteStart, long byteEnd, long firstDataLine, int[] columnMapping) {
            this.file = file;
            this.byteStart = byteStart;
            this.byteEnd = byteEnd;
            this.firstDataLine = firstDataLine;
            this.columnMapping = columnMapping;
        }

        @Override
        public List<Map<String, Object>> call() {
            try (FileChannel channel = FileChannel.open(file.toPath())) {
                long span = byteEnd - byteStart;
                if (span > Integer.MAX_VALUE) {
                    throw new IOException("Read partition too large: " + span + " bytes");
                }
                byte[] chunk = new byte[(int) span];
                ByteBuffer buffer = ByteBuffer.wrap(chunk);
                int position = 0;
                while (buffer.hasRemaining()) {
                    int n = channel.read(buffer, byteStart + position);
                    if (n < 0) {
                        break;
                    }
                    position += n;
                }
                try (BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(new ByteArrayInputStream(chunk, 0, position), StorageConfig.getCharset()));
                     DelimitedRowReader reader = rowReaderFactory.create(bufferedReader, columns, columnTypes)) {
                    reader.initPartition(columnMapping, firstDataLine);
                    List<Map<String, Object>> blockRows = new ArrayList<>();
                    while (reader.hasNext()) {
                        Map<String, Object> row = reader.next();
                        if (row != null) {
                            blockRows.add(row);
                        }
                    }
                    return blockRows;
                }
            } catch (IOException e) {
                throw new CompletionException("Failed to read delimited byte range " + byteStart + ".." + byteEnd, e);
            }
        }
    }

    /** A slice of rows produced by the deprecated block API. Holds the block
     * index and the rows within the block, in ascending row order.
     */
    public static final class Block {
        private final int blockIndex;
        private final List<Map<String, Object>> rows;

        Block(int blockIndex, List<Map<String, Object>> rows) {
            this.blockIndex = blockIndex;
            this.rows = rows;
        }

        /** Returns the zero-based block index. */
        public int getBlockIndex() {
            return blockIndex;
        }

        /** Returns the rows contained in this block. */
        public List<Map<String, Object>> getRows() {
            return rows;
        }
    }
}