package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import diesel.ErrorMessages;

/**
 * Format-agnostic index management and cache layer for delimited storage
 * backends. Maintains a sorted primary-key index and optional per-column
 * secondary indexes, exposes an LRU block cache for frequently accessed row
 * ranges, and can read delimited files concurrently using a dedicated daemon
 * {@link ForkJoinPool}.
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
    private static final int DEFAULT_MAX_CACHE_BLOCKS = 64;
    private static final long DEFAULT_PARALLEL_READ_THRESHOLD = 10000;

    private static final java.util.Properties ROOT_CONFIG = loadRootConfig();

    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final RowReaderFactory rowReaderFactory;
    private final String configPrefix;
    private final boolean multiLineRows;

    private List<Map<String, Object>> rows = new ArrayList<>();
    private String primaryKeyColumn;
    private final NavigableMap<Object, Long> primaryKeyIndex = new TreeMap<>(KEY_ORDER);
    private final Map<String, NavigableMap<Object, List<Long>>> secondaryIndexes =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private long nextRowId = 0;
    private final NavigableMap<Long, Integer> rowIdToPosition = new TreeMap<>();
    private final Set<Long> deletedRowIds = new HashSet<>();
    private static final double COMPACTION_THRESHOLD = 0.25;

    private final int blockSize;
    private final int maxCacheBlocks;
    private final long parallelReadThreshold;

    private final Map<Integer, Block> blockCache;
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();

    /**
     * @param tableName       the table name
     * @param columns         the ordered column names of the underlying schema
     * @param columnTypes     column name to type mapping of the underlying schema
     * @param rowReaderFactory factory opening a row reader for the storage format
     * @param configPrefix    namespace for {@code .block.size}, {@code .cache.max.blocks}
     *                        and {@code .parallel.read.threshold} config keys
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
        this.blockSize = Math.max(1, readIntSetting("block.size", DEFAULT_BLOCK_SIZE));
        this.maxCacheBlocks = Math.max(1, readIntSetting("cache.max.blocks", DEFAULT_MAX_CACHE_BLOCKS));
        this.parallelReadThreshold = Math.max(1, readLongSetting("parallel.read.threshold", DEFAULT_PARALLEL_READ_THRESHOLD));
        this.blockCache = Collections.synchronizedMap(
                new LinkedHashMap<Integer, Block>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, Block> eldest) {
                        return size() > DelimitedIndexManager.this.maxCacheBlocks;
                    }
                });
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
     * Adopts the given rows and builds the primary-key index. Any previously
     * configured indexes and cached blocks are discarded.
     *
     * @param data             the rows to index, or {@code null} for an empty table
     * @param primaryKeyColumn the primary-key column name, or {@code null} for none
     */
    public void buildIndexes(List<Map<String, Object>> data, String primaryKeyColumn) {
        this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        this.primaryKeyIndex.clear();
        this.secondaryIndexes.clear();
        this.rowIdToPosition.clear();
        this.deletedRowIds.clear();
        this.nextRowId = 0;
        this.blockCache.clear();
        this.cacheHits.set(0);
        this.cacheMisses.set(0);
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
            Object key = rows.get(i).get(canonical);
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
     * @param row  the row map
     * @param rowId the stable row identifier
     */
    public void insertIndexedRow(Map<String, Object> row, long rowId) {
        if (primaryKeyColumn != null) {
            Object key = row.get(primaryKeyColumn);
            if (key != null) {
                primaryKeyIndex.put(key, rowId);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = row.get(entry.getKey());
            if (key != null) {
                entry.getValue().computeIfAbsent(key, k -> new ArrayList<>()).add(rowId);
            }
        }
    }

    /**
     * Appends a row at the given position (end of storage). Assigns a new
     * rowId and registers it in the position map. No position shifting.
     *
     * @param row      the row data
     * @param rowIndex the position (should be at the end)
     */
    public void appendIndexedRow(Map<String, Object> row, int rowIndex) {
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
     * @param row  the row map
     * @param rowId the stable row identifier to disassociate
     */
    public void removeIndexedRow(Map<String, Object> row, long rowId) {
        if (primaryKeyColumn != null) {
            Object key = row.get(primaryKeyColumn);
            if (key != null && rowId == primaryKeyIndex.getOrDefault(key, -1L)) {
                primaryKeyIndex.remove(key);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = row.get(entry.getKey());
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
     * The rowId stays the same; only the key→rowId mappings are refreshed.
     *
     * @param oldRow  the previous row data
     * @param rowIndex the physical position
     * @param newRow  the new row data
     */
    public void updateRow(Map<String, Object> oldRow, int rowIndex, Map<String, Object> newRow) {
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
     * @param rowIndex the zero-based position at which to insert
     * @param row      the row data
     */
    public void insertAt(int rowIndex, Map<String, Object> row) {
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
     * @param rowIndex the zero-based position of the row to delete
     */
    public void deleteRow(int rowIndex) {
        Long rid = positionToRowId(rowIndex);
        if (rid == null) {
            return;
        }
        Map<String, Object> row = rows.remove(rowIndex);
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

    // ─── Block cache ─────────────────────────────────────────────────

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
     * Returns the block with the given index, loading it (cache-through) if
     * not already present.
     *
     * @param blockIndex the zero-based block index
     * @return the cached block
     */
    public Block getBlock(int blockIndex) {
        int numBlocks = getNumBlocks();
        if (blockIndex < 0 || blockIndex >= numBlocks) {
            throw new IndexOutOfBoundsException("Block index " + blockIndex + " out of range [0, " + numBlocks + ")");
        }
        Block cached = blockCache.get(blockIndex);
        if (cached != null) {
            cacheHits.incrementAndGet();
            return cached;
        }
        cacheMisses.incrementAndGet();
        int from = blockIndex * blockSize;
        int to = Math.min(from + blockSize, rows.size());
        Block block = new Block(blockIndex, new ArrayList<>(rows.subList(from, to)));
        blockCache.put(blockIndex, block);
        return block;
    }

    /**
     * Pro-actively loads every block into the cache. Uses the shared daemon pool
     * when the block count is large enough, otherwise loads sequentially.
     *
     * @return the blocks in ascending block order
     */
    public List<Block> loadAllBlocksParallel() {
        int numBlocks = getNumBlocks();
        if (numBlocks <= 1) {
            return numBlocks == 0 ? List.of() : List.of(getBlock(0));
        }
        if (numBlocks < parallelReadThreshold) {
            List<Block> result = new ArrayList<>(numBlocks);
            for (int i = 0; i < numBlocks; i++) {
                result.add(getBlock(i));
            }
            return result;
        }
        List<Callable<Block>> tasks = new ArrayList<>(numBlocks);
        for (int i = 0; i < numBlocks; i++) {
            int block = i;
            tasks.add(() -> getBlock(block));
        }
        List<Block> result = new ArrayList<>(numBlocks);
        try {
            for (Future<Block> future : READ_POOL.invokeAll(tasks)) {
                result.add(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("Parallel block load interrupted", e);
        } catch (ExecutionException e) {
            throw new CompletionException("Parallel block load failed", e.getCause());
        }
        return result;
    }

    /** Returns the number of block-cache hits. */
    public long getCacheHitCount() {
        return cacheHits.get();
    }

    /** Returns the number of block-cache misses. */
    public long getCacheMissCount() {
        return cacheMisses.get();
    }

    /** Discards all cached blocks. */
    public void invalidateCache() {
        blockCache.clear();
    }

    // ─── Parallel file reading ───────────────────────────────────────

    /**
     * Reads the whole delimited file. Uses parallel chunk readers on the shared
     * daemon pool when the file is large enough, otherwise falls back to a single
     * sequential pass.
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
        long totalRows = countDataLines(file);
        if (totalRows == 0) {
            return List.of();
        }
        if (totalRows < parallelReadThreshold) {
            return loadFromFileSequential(filePath);
        }
        if (multiLineRows && mayContainMultiLineRows(file)) {
            return loadFromFileSequential(filePath);
        }
        int partitions = Math.min(READ_POOL.getParallelism(),
                (int) Math.min((totalRows + blockSize - 1) / blockSize, Integer.MAX_VALUE));
        List<LineRange> ranges = new ArrayList<>(partitions);
        long linesPerPartition = (totalRows + partitions - 1) / partitions;
        long start = 0;
        for (int i = 0; i < partitions && start < totalRows; i++) {
            long end = Math.min(totalRows, start + linesPerPartition);
            ranges.add(new LineRange(start, end));
            start = end;
        }
        List<Callable<List<Map<String, Object>>>> tasks = new ArrayList<>(ranges.size());
        for (LineRange range : ranges) {
            tasks.add(new ReadRangeTask(range, file));
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
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
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

    /**
     * Detects whether the file contains rows spanning multiple physical lines.
     * The default implementation assumes single-line rows; backends whose format
     * can embed line breaks within one logical row override this.
     *
     * @param file the delimited file
     * @return {@code true} when the file must be read sequentially
     * @throws IOException on I/O errors
     */
    protected boolean mayContainMultiLineRows(File file) throws IOException {
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

    private long countDataLines(File file) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            if (bufferedReader.readLine() == null) {
                return 0;
            }
            long lines = 0;
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines++;
            }
            return lines;
        }
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

    /** A contiguous range of data lines (header excluded) within a delimited file. */
    private static final class LineRange {
        final long start;
        final long end;

        LineRange(long start, long end) {
            this.start = start;
            this.end = end;
        }

        long size() {
            return end - start;
        }
    }

    /** Reads a line range from a delimited file by re-opening the file and skipping rows. */
    private final class ReadRangeTask implements Callable<List<Map<String, Object>>> {
        private final LineRange range;
        private final File file;

        ReadRangeTask(LineRange range, File file) {
            this.range = range;
            this.file = file;
        }

        @Override
        public List<Map<String, Object>> call() {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                 DelimitedRowReader reader = rowReaderFactory.create(bufferedReader, columns, columnTypes)) {
                reader.readHeader();
                for (long i = 0; i < range.start && reader.hasNext(); i++) {
                    reader.next();
                }
                List<Map<String, Object>> blockRows = new ArrayList<>((int) range.size());
                for (long i = range.start; i < range.end && reader.hasNext(); i++) {
                    Map<String, Object> row = reader.next();
                    if (row != null) {
                        blockRows.add(row);
                    }
                }
                return blockRows;
            } catch (IOException e) {
                throw new CompletionException("Failed to read delimited range " + range.start + ".." + range.end, e);
            }
        }
    }

    /**
     * A cached block of rows. Holds the block index and the rows within the
     * block, in ascending row order.
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