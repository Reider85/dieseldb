package diesel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.logging.Level;

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
    private static final int CURRENT_FORMAT_VERSION = 1;
    private static final Logger LOGGER = Logger.getLogger(Table.class.getName());
    private final String name;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String primaryKeyColumn;
    private final List<Map<String, Object>> rows;
    private transient ConcurrentHashMap<Integer, ReentrantReadWriteLock> rowLocks;
    private transient Map<String, Index> indexes;
    private transient Map<String, Sequence> sequences;
    private final Map<String, String> indexDefinitions = new ConcurrentHashMap<>();
    private boolean isFileInitialized;
    private boolean hasClusteredIndex;
    private String clusteredIndexColumn;
    private transient BTreeClusteredIndex clusteredIndex;
    private transient Database database;
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
    private transient Object statsLock = new Object();
    private transient volatile boolean statsDirty;
    private transient volatile boolean statsRefreshScheduled;

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
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.sequences = sequences != null ? new ConcurrentHashMap<>(sequences) : new ConcurrentHashMap<>();
        this.isFileInitialized = false;
        this.hasClusteredIndex = false;
        this.clusteredIndexColumn = null;
        this.clusteredIndex = null;
        this.rowCount = 0;
        this.avgRowSizeBytes = estimateAverageRowSizeBytes();
        this.lastAnalyzedMillis = 0;

        validateSchema(columns, columnTypes);

        if (primaryKeyColumn != null) {
            if (!this.columnTypes.containsKey(primaryKeyColumn)) {
                throw new IllegalArgumentException("Primary key column " + primaryKeyColumn + " does not exist");
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

    /**
     * Attaches a database to this table, used to restore the transient
     * reference after deserialization.
     *
     * @param database the database to attach
     */
    public void attachDatabase(Database database) {
        this.database = database;
    }

    private void validateSchema(List<String> columns, Map<String, Class<?>> columnTypes) {
        for (String column : columns) {
            if (!columnTypes.containsKey(column)) {
                LOGGER.log(Level.SEVERE, "Schema validation failed: Column {0} missing in columnTypes {1}",
                        new Object[]{column, columnTypes.keySet()});
                throw new IllegalArgumentException("Column " + column + " missing in columnTypes");
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
        createSecondaryIndex(columnName, "BTREE", BTreeIndex::new, false);
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
        createSecondaryIndex(columnName, "HASH", HashIndex::new, false);
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
        createSecondaryIndex(columnName, "UNIQUE", UniqueIndex::new, true);
        LOGGER.log(Level.INFO, "Created unique index on column {0} for table {1}", new Object[]{columnName, name});
    }

    /**
     * Builds a secondary index over the already-present rows and registers it
     * under {@code columnName}. When {@code unique} is set, duplicate keys abort
     * the creation.
     */
    private void createSecondaryIndex(String columnName, String definition, Function<Class<?>, Index> indexFactory, boolean unique) {
        if (!columnTypes.containsKey(columnName)) {
            throw new ColumnNotFoundException("Column " + columnName + " does not exist");
        }
        Index index = indexFactory.apply(columnTypes.get(columnName));
        Set<Object> seenKeys = unique ? new HashSet<>() : null;
        for (int i = 0; i < rows.size(); i++) {
            Object key = rows.get(i).get(columnName);
            if (key != null) {
                if (unique && !seenKeys.add(key)) {
                    throw new IllegalStateException("Duplicate key '" + key + "' found in column " + columnName + " while creating unique index");
                }
                index.insert(key, i);
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
            throw new ColumnNotFoundException("Column " + columnName + " does not exist");
        }
        if (hasClusteredIndex) {
            throw new IllegalStateException("Table already has a clustered index on " + clusteredIndexColumn);
        }

        clusteredIndex = new BTreeClusteredIndex(columnTypes.get(columnName));
        hasClusteredIndex = true;
        clusteredIndexColumn = columnName;

        // Sort the rows by the clustered key and verify uniqueness before reindexing.
        List<Map<String, Object>> sortedRows = new ArrayList<>(rows);
        sortedRows.sort((row1, row2) -> {
            Object key1 = row1.get(columnName);
            Object key2 = row2.get(columnName);
            if (key1 == null || key2 == null) {
                throw new IllegalStateException("Null key in column " + columnName + " not allowed for unique clustered index");
            }
            return compareKeys(key1, key2);
        });

        Set<Object> seenKeys = new HashSet<>();
        for (Map<String, Object> row : sortedRows) {
            Object key = row.get(columnName);
            if (!seenKeys.add(key)) {
                throw new IllegalStateException("Duplicate key '" + key + "' found in column " + columnName + " while creating unique clustered index");
            }
        }

        rows.clear();
        rows.addAll(sortedRows);
        for (int i = 0; i < rows.size(); i++) {
            Object key = rows.get(i).get(columnName);
            clusteredIndex.insert(key, i);
            insertRowIntoIndexes(rows.get(i), i);
        }

        LOGGER.log(Level.INFO, "Created unique clustered B-tree index on column {0} for table {1}", new Object[]{columnName, name});
    }

    private int compareKeys(Object k1, Object k2) {
        if (k1 instanceof Comparable && k2 instanceof Comparable) {
            return ((Comparable<Object>) k1).compareTo(k2);
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
        return rows.size();
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
            return new TableStatistics(Math.max(0, rowCount), avg, lastAnalyzedMillis);
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
            rowCount = rows.size();
            avgRowSizeBytes = measureAverageRowSizeBytes();
            lastAnalyzedMillis = System.currentTimeMillis();
            statsDirty = false;
        }
        LOGGER.log(Level.INFO, "Table {0} analyzed: {1} rows, avg row size {2} bytes",
                new Object[]{name, rowCount, avgRowSizeBytes});
        return getStatistics();
    }

    /** Marks the statistics dirty and schedules the asynchronous refresh. */
    private void markStatsDirty() {
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
                rowCount = rowsAtStart;
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
        return new ArrayList<>(rows);
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
        rows.remove(rowIndex);
        // Row indexes shift down by one, so the locks of this and all following rows are stale.
        for (int i = rowIndex; i <= rows.size(); i++) {
            rowLocks.remove(i);
        }
        rowCount--;
        markStatsDirty();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        oos.writeObject(hasClusteredIndex);
        oos.writeObject(clusteredIndexColumn);
        oos.writeObject(sequences);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        // Rebuild the case-insensitive type map so lookups stay case-insensitive after load.
        Map<String, Class<?>> tempColumnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tempColumnTypes.putAll(columnTypes);
        this.columnTypes.clear();
        this.columnTypes.putAll(tempColumnTypes);
        this.rowLocks = new ConcurrentHashMap<>();
        this.indexes = new ConcurrentHashMap<>();
        this.hasClusteredIndex = (boolean) ois.readObject();
        this.clusteredIndexColumn = (String) ois.readObject();
        this.sequences = (Map<String, Sequence>) ois.readObject();
        if (hasClusteredIndex) {
            this.clusteredIndex = new BTreeClusteredIndex(columnTypes.get(clusteredIndexColumn));
            for (int i = 0; i < rows.size(); i++) {
                Object key = rows.get(i).get(clusteredIndexColumn);
                if (key != null) {
                    clusteredIndex.insert(key, i);
                }
            }
        }
        // Rebuild all secondary indexes from their persisted definitions.
        if (indexDefinitions != null) {
            for (Map.Entry<String, String> entry : indexDefinitions.entrySet()) {
                String column = entry.getKey();
                Class<?> keyType = columnTypes.get(column);
                Index index;
                switch (entry.getValue()) {
                    case "BTREE":
                        index = new BTreeIndex(keyType);
                        break;
                    case "HASH":
                        index = new HashIndex(keyType);
                        break;
                    case "UNIQUE":
                        index = new UniqueIndex(keyType);
                        break;
                    default:
                        continue;
                }
                for (int i = 0; i < rows.size(); i++) {
                    Object key = rows.get(i).get(column);
                    if (key != null) {
                        index.insert(key, i);
                    }
                }
                indexes.put(column, index);
            }
        }
        this.database = null;
        this.statsLock = new Object();
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
        for (String col : columns) {
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

            checkUniqueConstraint(col, value);

            Class<?> expectedType = columnTypes.get(col);
            if (expectedType == null) {
                throw new IllegalArgumentException("Invalid value or type for column: " + col);
            }
            if (value == null) {
                validatedRow.put(col, null);
                continue;
            }
            validateColumnValueType(col, expectedType, value);
            validatedRow.put(col, value);
        }

        if (hasClusteredIndex) {
            Object key = validatedRow.get(clusteredIndexColumn);
            if (key == null) {
                throw new IllegalArgumentException("Null key in clustered index column: " + clusteredIndexColumn);
            }
            List<Integer> existing = clusteredIndex.search(key);
            if (!existing.isEmpty()) {
                LOGGER.log(Level.WARNING, "Duplicate clustered key detected: key '{0}' in column {1}", new Object[]{key, clusteredIndexColumn});
                throw new IllegalStateException("Duplicate key violation: key '" + key + "' in column " + clusteredIndexColumn);
            }
            insertIntoClusteredPosition(validatedRow, key);
        } else {
            insertAtEnd(validatedRow);
        }
        rowCount++;
        markStatsDirty();
        LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{name, validatedRow});
    }

    private void checkUniqueConstraint(String column, Object value) {
        Index index = indexes.get(column);
        if (index instanceof UniqueIndex || index instanceof BTreeClusteredIndex) {
            if (value != null && index.search(value).size() > 0) {
                LOGGER.log(Level.WARNING, "Duplicate key detected: key '{0}' in column {1}; skipping insertion", new Object[]{value, column});
                throw new IllegalStateException("Duplicate key violation: key '" + value + "' already exists in column " + column);
            }
        }
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

    private void insertIntoClusteredPosition(Map<String, Object> row, Object clusteredKey) {
        int insertIndex = findInsertPosition(clusteredKey);
        ReentrantReadWriteLock lock = getRowLock(insertIndex);
        lock.writeLock().lock();
        try {
            rows.add(insertIndex, row);
            clusteredIndex.insert(clusteredKey, insertIndex);
            insertRowIntoIndexes(row, insertIndex);
            updateIndicesAfterInsert(insertIndex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void insertAtEnd(Map<String, Object> row) {
        int rowIndex = rows.size();
        ReentrantReadWriteLock lock = getRowLock(rowIndex);
        lock.writeLock().lock();
        try {
            rows.add(row);
            insertRowIntoIndexes(row, rowIndex);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Inserts {@code row} into every secondary index at {@code rowIndex}, skipping NULL keys. */
    private void insertRowIntoIndexes(Map<String, Object> row, int rowIndex) {
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            Object key = row.get(entry.getKey());
            if (key != null) {
                entry.getValue().insert(key, rowIndex);
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
                throw new IllegalStateException("Duplicate key found: " + key);
            }
        }
        return low;
    }

    /** Shifts the stored row index of every row after an insert into a clustered table. */
    private void updateIndicesAfterInsert(int insertIndex) {
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

    private String resolveFilePath(String tableName, String extension) {
        String dir = database != null && database.getDataDir() != null ? database.getDataDir() : ".";
        return dir + File.separator + tableName + extension;
    }

    /**
     * Writes the table contents (header plus rows) to a CSV file in the data
     * directory. Each row is read under its lock while writing.
     *
     * @param tableName the table name, used as the file base name
     * @throws RuntimeException if the file cannot be written
     */
    public void saveToFile(String tableName) {
        String fileName = resolveFilePath(tableName, ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false))) {
            writer.write(String.join(",", columns));
            writer.newLine();

            for (int i = 0; i < rows.size(); i++) {
                ReentrantReadWriteLock lock = getRowLock(i);
                lock.readLock().lock();
                try {
                    Map<String, Object> row = rows.get(i);
                    List<String> values = new ArrayList<>();
                    for (String column : columns) {
                        values.add(formatValue(row.get(column)));
                    }
                    writer.write(String.join(",", values));
                    writer.newLine();
                } finally {
                    lock.readLock().unlock();
                }
            }

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
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        return value.toString();
    }

    /**
     * Serializes the whole table (schema, rows, index definitions, sequences)
     * to a {@code .table} file in the data directory.
     *
     * @param tableName the table name, used as the file base name
     * @throws RuntimeException if the file cannot be written
     */
    public void saveToSerializedFile(String tableName) {
        String fileName = resolveFilePath(tableName, ".table");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
            oos.writeObject(this);
            oos.flush();
            isFileInitialized = true;
            LOGGER.log(Level.INFO, "Table {0} saved to file {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save table to file: {0}", fileName);
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
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
        String fileName = dir + File.separator + tableName + ".table";
        File file = new File(fileName);
        if (!file.exists()) {
            LOGGER.log(Level.INFO, "Serialized file {0} not found, creating new table {1} with base structure",
                    new Object[]{fileName, tableName});
            Table table = new Table(database, tableName, new ArrayList<>(), new HashMap<>(), null, new HashMap<String, Sequence>());
            table.formatVersion = CURRENT_FORMAT_VERSION;
            table.setFileInitialized(false);
            return table;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            Table table = (Table) ois.readObject();
            if (table.formatVersion != CURRENT_FORMAT_VERSION) {
                throw new IllegalArgumentException("Unsupported table format version: " + table.formatVersion
                        + ", expected: " + CURRENT_FORMAT_VERSION);
            }
            table.database = database;
            table.setFileInitialized(true);
            LOGGER.log(Level.INFO, "Table {0} loaded from file {1}", new Object[]{tableName, fileName});
            return table;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load table {0}: {1}", new Object[]{tableName, e.getMessage()});
            return null;
        }
    }
}
