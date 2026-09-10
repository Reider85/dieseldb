package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
 * Index management and cache layer for TSV-backed tables. Maintains a sorted
 * primary-key index and optional per-column secondary indexes, exposes an LRU
 * block cache for frequently accessed row ranges, and can read TSV files
 * concurrently using a dedicated daemon {@link ForkJoinPool}.
 *
 * <p>Indexes are order-preserving red-black trees ({@link TreeMap}), so
 * primary-key lookups and range searches run in {@code O(log n)}. All index
 * structures are self-contained within the storage package and do not depend
 * on the engine's package-private B-tree classes.
 */
public class TsvIndexManager {

    private static final Logger LOGGER = Logger.getLogger(TsvIndexManager.class.getName());

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
     * Shared daemon pool used for parallel TSV reads and parallel block loads.
     * Daemon threads so the pool does not block JVM exit.
     */
    private static final ForkJoinPool READ_POOL = new ForkJoinPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("diesel-tsv-read-" + t.getPoolIndex());
                return t;
            },
            null, true);

    private static int blockSizeConfig = 1000;
    private static int maxCacheBlocksConfig = 64;
    private static long parallelReadThresholdConfig = 10000;

    static {
        loadConfig();
    }

    private final String tableName;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;

    private List<Map<String, Object>> rows = new ArrayList<>();
    private String primaryKeyColumn;
    private final NavigableMap<Object, Integer> primaryKeyIndex = new TreeMap<>(KEY_ORDER);
    private final Map<String, NavigableMap<Object, List<Integer>>> secondaryIndexes =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final int blockSize;
    private final int maxCacheBlocks;
    private final long parallelReadThreshold;

    private final Map<Integer, TsvBlock> blockCache;
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();

    /**
     * @param tableName   the table name
     * @param columns     the ordered column names of the underlying schema
     * @param columnTypes column name to type mapping of the underlying schema
     */
    public TsvIndexManager(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.tableName = tableName == null ? "" : tableName;
        this.columns = new ArrayList<>(columns == null ? List.of() : columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnTypes != null) {
            this.columnTypes.putAll(columnTypes);
        }
        this.blockSize = Math.max(1, blockSizeConfig);
        this.maxCacheBlocks = Math.max(1, maxCacheBlocksConfig);
        this.parallelReadThreshold = Math.max(1, parallelReadThresholdConfig);
        this.blockCache = Collections.synchronizedMap(
                new LinkedHashMap<Integer, TsvBlock>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, TsvBlock> eldest) {
                        return size() > TsvIndexManager.this.maxCacheBlocks;
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

    // ─── Index construction ──────────────────────────────────────────

    /**
     * Adopts the given rows and builds the primary-key index. Any previously
     * configured indexes and cached blocks are discarded.
     *
     * @param data              the rows to index, or {@code null} for an empty table
     * @param primaryKeyColumn  the primary-key column name, or {@code null} for none
     */
    public void buildIndexes(List<Map<String, Object>> data, String primaryKeyColumn) {
        this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        this.primaryKeyIndex.clear();
        this.secondaryIndexes.clear();
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
        NavigableMap<Object, List<Integer>> index = new TreeMap<>(KEY_ORDER);
        for (int i = 0; i < rows.size(); i++) {
            Object key = rows.get(i).get(canonical);
            if (key != null) {
                index.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
        }
        secondaryIndexes.put(canonical, index);
        return true;
    }

    /** Rebuilds the primary-key index and every secondary index from the current rows. */
    public void reindex() {
        primaryKeyIndex.clear();
        for (NavigableMap<Object, List<Integer>> index : secondaryIndexes.values()) {
            index.clear();
        }
        for (int i = 0; i < rows.size(); i++) {
            insertIndexedRow(rows.get(i), i);
        }
    }

    /**
     * Injects a new row into all maintained indexes.
     *
     * @param row      the row map
     * @param rowIndex the row index associated with the row
     */
    public void insertIndexedRow(Map<String, Object> row, int rowIndex) {
        if (primaryKeyColumn != null) {
            Object key = row.get(primaryKeyColumn);
            if (key != null) {
                primaryKeyIndex.put(key, rowIndex);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Integer>>> entry : secondaryIndexes.entrySet()) {
            Object key = row.get(entry.getKey());
            if (key != null) {
                entry.getValue().computeIfAbsent(key, k -> new ArrayList<>()).add(rowIndex);
            }
        }
    }

    /**
     * Removes a row from all maintained indexes.
     *
     * @param row      the row map
     * @param rowIndex the row index to disassociate
     */
    public void removeIndexedRow(Map<String, Object> row, int rowIndex) {
        if (primaryKeyColumn != null) {
            Object key = row.get(primaryKeyColumn);
            if (key != null && rowIndex == primaryKeyIndex.getOrDefault(key, -1)) {
                primaryKeyIndex.remove(key);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Integer>>> entry : secondaryIndexes.entrySet()) {
            Object key = row.get(entry.getKey());
            if (key == null) {
                continue;
            }
            List<Integer> indexes = entry.getValue().get(key);
            if (indexes != null) {
                indexes.remove(Integer.valueOf(rowIndex));
                if (indexes.isEmpty()) {
                    entry.getValue().remove(key);
                }
            }
        }
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
        Integer rowIndex = primaryKeyIndex.get(key);
        return rowIndex == null ? List.of() : List.of(rowIndex);
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
        NavigableMap<Object, List<Integer>> index = secondaryIndexes.get(column);
        if (index == null) {
            return List.of();
        }
        List<Integer> indexes = index.get(key);
        return indexes == null ? List.of() : new ArrayList<>(indexes);
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
            return collectIndexes(subRange(primaryKeyIndex, low, high));
        }
        NavigableMap<Object, List<Integer>> index = secondaryIndexes.get(column);
        if (index == null) {
            return List.of();
        }
        return collectIndexes(subRange(index, low, high));
    }

    // ─── Block cache ─────────────────────────────────────────────────

    /** Returns the number of fixed-size blocks the current rows are split into. */
    public int getNumBlocks() {
        if (rows.isEmpty()) {
            return 0;
        }
        return (rows.size() + blockSize - 1) / blockSize;
    }

    /** Returns the number of rows per block. */
    public int getBlockSize() {
        return blockSize;
    }

    /** Returns the current row count. */
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Returns the block with the given index, loading it (cache-through) if
     * not already present.
     *
     * @param blockIndex the zero-based block index
     * @return the cached block
     */
    public TsvBlock getBlock(int blockIndex) {
        int numBlocks = getNumBlocks();
        if (blockIndex < 0 || blockIndex >= numBlocks) {
            throw new IndexOutOfBoundsException("Block index " + blockIndex + " out of range [0, " + numBlocks + ")");
        }
        TsvBlock cached = blockCache.get(blockIndex);
        if (cached != null) {
            cacheHits.incrementAndGet();
            return cached;
        }
        cacheMisses.incrementAndGet();
        int from = blockIndex * blockSize;
        int to = Math.min(from + blockSize, rows.size());
        TsvBlock block = new TsvBlock(blockIndex, new ArrayList<>(rows.subList(from, to)));
        blockCache.put(blockIndex, block);
        return block;
    }

    /**
     * Pro-actively loads every block into the cache. Uses the shared daemon pool
     * when the block count is large enough, otherwise loads sequentially.
     *
     * @return the blocks in ascending block order
     */
    public List<TsvBlock> loadAllBlocksParallel() {
        int numBlocks = getNumBlocks();
        if (numBlocks <= 1) {
            return numBlocks == 0 ? List.of() : List.of(getBlock(0));
        }
        if (numBlocks < parallelReadThreshold) {
            List<TsvBlock> result = new ArrayList<>(numBlocks);
            for (int i = 0; i < numBlocks; i++) {
                result.add(getBlock(i));
            }
            return result;
        }
        List<Callable<TsvBlock>> tasks = new ArrayList<>(numBlocks);
        for (int i = 0; i < numBlocks; i++) {
            int block = i;
            tasks.add(() -> getBlock(block));
        }
        List<TsvBlock> result = new ArrayList<>(numBlocks);
        try {
            for (Future<TsvBlock> future : READ_POOL.invokeAll(tasks)) {
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
     * Reads the whole TSV file. Uses parallel chunk readers on the shared daemon
     * pool when the file is large enough, otherwise falls back to a single
     * sequential pass.
     *
     * @param filePath the path to the {@code .tsv} file
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
            throw new CompletionException("Parallel TSV read interrupted", e);
        } catch (ExecutionException e) {
            throw new CompletionException("Parallel TSV read failed", e.getCause());
        }
        return result;
    }

    /** Reads the whole TSV file with a single sequential pass. */
    public List<Map<String, Object>> loadFromFileSequential(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            return List.of();
        }
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
             TsvRowReader reader = new TsvRowReader(bufferedReader, columns, columnTypes)) {
            reader.readHeader();
            return reader.readAll();
        }
    }

    /**
     * Loads the TSV file (parallel when beneficial) and builds the primary-key index.
     *
     * @param filePath          the path to the {@code .tsv} file
     * @param primaryKeyColumn  the primary-key column, or {@code null} for none
     * @param parallel          whether to allow the parallel read path
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

    private static List<Integer> collectIndexes(Map<Object, ?> entries) {
        if (entries.isEmpty()) {
            return List.of();
        }
        List<Integer> result = new ArrayList<>();
        for (Object value : entries.values()) {
            if (value instanceof Integer rowIndex) {
                result.add(rowIndex);
            } else if (value instanceof List<?> indexes) {
                for (Object index : indexes) {
                    result.add((Integer) index);
                }
            }
        }
        return result;
    }

    private static void loadConfig() {
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String blockSize = props.getProperty("tsv.block.size");
                if (blockSize != null) {
                    blockSizeConfig = Math.max(1, Integer.parseInt(blockSize.trim()));
                }
                String maxBlocks = props.getProperty("tsv.cache.max.blocks");
                if (maxBlocks != null) {
                    maxCacheBlocksConfig = Math.max(1, Integer.parseInt(maxBlocks.trim()));
                }
                String threshold = props.getProperty("tsv.parallel.read.threshold");
                if (threshold != null) {
                    parallelReadThresholdConfig = Math.max(1, Long.parseLong(threshold.trim()));
                }
            }
        } catch (Exception ignored) {
            LOGGER.log(Level.FINE, "Config error for TSV index manager, using defaults: {0}",
                    ignored.getMessage());
        }
    }

    /** A contiguous range of data lines (header excluded) within a TSV file. */
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

    /** Reads a line range from a TSV file by re-opening the file and skipping rows. */
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
                 TsvRowReader reader = new TsvRowReader(bufferedReader, columns, columnTypes)) {
                reader.readHeader();
                for (long i = 0; i < range.start && reader.hasNext(); i++) {
                    reader.next();
                }
                List<Map<String, Object>> blockRows = new ArrayList<>((int) range.size());
                for (long i = range.start; i < range.end && reader.hasNext(); i++) {
                    blockRows.add(reader.next());
                }
                return blockRows;
            } catch (IOException e) {
                throw new CompletionException("Failed to read TSV range " + range.start + ".." + range.end, e);
            }
        }
    }

    /**
     * A cached block of rows. Holds the block index and the rows within the
     * block, in ascending row order.
     */
    public static final class TsvBlock {
        private final int blockIndex;
        private final List<Map<String, Object>> rows;

        TsvBlock(int blockIndex, List<Map<String, Object>> rows) {
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