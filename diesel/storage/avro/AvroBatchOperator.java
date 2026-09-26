package diesel.storage.avro;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import diesel.ConfigKeys;

/**
 * High-throughput batch operations for AVRO storage (Prompt 81).
 *
 * <p>This operator sits on top of {@link AvroRowStorage} and adds four
 * capabilities:
 * <ul>
 *   <li><b>Batch insert</b> — append 1000+ rows in a single call. Rows are
 *       written through {@link AvroRowStorage#insert(Map)} inside a deferred
 *       bulk-update window, so index bookkeeping is coalesced instead of being
 *       paid per row. A compact {@code Object[]} fast path
 *       ({@link #insertBatchRaw(List)}) and an AVRO-to-AVRO streaming import
 *       ({@link #importFromReader(AvroDataFileReader)}) avoid the intermediate
 *       {@code Map} representation entirely.</li>
 *   <li><b>Batch read with size prediction</b> — the caller's predicted row
 *       count is multiplied by {@code avro.batch.read.estimate.factor} and used
 *       to pre-allocate the result list, avoiding repeated {@code ArrayList}
 *       growth. Windowed reads ({@link #readBatchWindow(int, int, int)}) do the
 *       same for paginated access.</li>
 *   <li><b>Transaction batching</b> — {@link #beginBatch()} snapshots the
 *       in-memory rows, {@link #commitBatch()} discards the snapshot and
 *       {@link #rollbackBatch()} restores it. A batch left open when the
 *       operator is {@link #close() closed} is rolled back automatically.</li>
 *   <li><b>Batch statistics</b> — {@link BatchStats} accumulates inserted/read
 *       row counts, estimated bytes written, elapsed wall-clock time, flush
 *       count and transaction count over the operator's lifetime.</li>
 * </ul>
 *
 * <p>Configuration is resolved per operator instance from system properties,
 * then {@code config.properties}, then code defaults:
 * <pre>
 * avro.batch.insert.flush.size    = 1000  (rows between internal flush marks)
 * avro.batch.read.estimate.factor = 1.5   (over-allocation multiplier)
 * avro.batch.validation.mode      = strict
 * </pre>
 * The test hook {@code avro.batch.config.file} redirects the
 * {@code config.properties} lookup to a throwaway file.
 *
 * <p>Thread safety: instances are <b>not</b> thread-safe. Use one operator per
 * thread; the {@link MutableBatchStats} counters themselves are atomic.
 *
 * @since Prompt 81
 */
public final class AvroBatchOperator implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBatchOperator.class);

    /** Test hook redirecting the config.properties lookup. */
    static final String CONFIG_FILE_KEY = "avro.batch.config.file";
    static final String INSERT_FLUSH_SIZE_KEY = "avro.batch.insert.flush.size";
    static final String READ_ESTIMATE_FACTOR_KEY = "avro.batch.read.estimate.factor";
    static final String VALIDATION_MODE_KEY = "avro.batch.validation.mode";

    public static final int DEFAULT_INSERT_FLUSH_SIZE = 1000;
    public static final double DEFAULT_READ_ESTIMATE_FACTOR = 1.5;
    public static final String DEFAULT_VALIDATION_MODE = "strict";

    /** Minimum batch size advertised by the prompt ("1000+ records"). */
    public static final int MIN_BATCH_SIZE = 1000;

    private final AvroRowStorage storage;
    private final int insertFlushSize;
    private final double readEstimateFactor;
    private final String validationMode;

    private final MutableBatchStats stats = new MutableBatchStats();
    private boolean closed;

    private List<Object[]> snapshot;
    private boolean inTransaction;

    // ─── Statistics ─────────────────────────────────────────────────

    /**
     * Immutable snapshot of the accumulated batch operation statistics.
     *
     * @param rowsInserted     total rows appended by batch inserts/imports
     * @param rowsRead         total rows returned by batch reads
     * @param bytesWritten     estimated bytes written by batch inserts
     * @param insertNanos      cumulative wall-clock time spent inserting
     * @param readNanos        cumulative wall-clock time spent reading
     * @param transactionCount number of committed transactional batches
     * @param flushCount       number of internal flush marks crossed
     */
    public record BatchStats(
            long rowsInserted,
            long rowsRead,
            long bytesWritten,
            long insertNanos,
            long readNanos,
            int transactionCount,
            int flushCount
    ) {
        /** All-zero statistics. */
        public static BatchStats empty() {
            return new BatchStats(0, 0, 0, 0, 0, 0, 0);
        }

        /** Average rows inserted per insert operation. */
        public double averageInsertBatchSize(int operations) {
            return operations <= 0 ? 0.0 : (double) rowsInserted / operations;
        }

        @Override
        public String toString() {
            return String.format(
                    "BatchStats{inserted=%d, read=%d, bytes=%d, insertTime=%dms, readTime=%dms, txns=%d, flushes=%d}",
                    rowsInserted, rowsRead, bytesWritten,
                    insertNanos / 1_000_000, readNanos / 1_000_000,
                    transactionCount, flushCount);
        }
    }

    /**
     * Thread-safe mutable counters backing {@link #getStats()}. Exposed via
     * {@link #mutableStats()} so callers can poll progress while a long batch
     * is running.
     */
    public static final class MutableBatchStats {
        private final AtomicLong rowsInserted = new AtomicLong();
        private final AtomicLong rowsRead = new AtomicLong();
        private final AtomicLong bytesWritten = new AtomicLong();
        private final AtomicLong insertNanos = new AtomicLong();
        private final AtomicLong readNanos = new AtomicLong();
        private final AtomicLong transactionCount = new AtomicLong();
        private final AtomicLong flushCount = new AtomicLong();

        public void addInsert(long rows, long bytes, long nanos) {
            rowsInserted.addAndGet(rows);
            bytesWritten.addAndGet(bytes);
            insertNanos.addAndGet(nanos);
        }

        public void addRead(long rows, long nanos) {
            rowsRead.addAndGet(rows);
            readNanos.addAndGet(nanos);
        }

        public void addTransaction() { transactionCount.incrementAndGet(); }

        public void addFlush() { flushCount.incrementAndGet(); }

        public long rowsInserted() { return rowsInserted.get(); }

        public long rowsRead() { return rowsRead.get(); }

        public long bytesWritten() { return bytesWritten.get(); }

        public long insertNanos() { return insertNanos.get(); }

        public long readNanos() { return readNanos.get(); }

        public long transactionCount() { return transactionCount.get(); }

        public long flushCount() { return flushCount.get(); }

        public void reset() {
            rowsInserted.set(0);
            rowsRead.set(0);
            bytesWritten.set(0);
            insertNanos.set(0);
            readNanos.set(0);
            transactionCount.set(0);
            flushCount.set(0);
        }

        public BatchStats snapshot() {
            return new BatchStats(
                    rowsInserted.get(), rowsRead.get(), bytesWritten.get(),
                    insertNanos.get(), readNanos.get(),
                    (int) transactionCount.get(), (int) flushCount.get());
        }

        @Override
        public String toString() {
            return snapshot().toString();
        }
    }

    // ─── Construction ───────────────────────────────────────────────

    /**
     * Creates a batch operator backed by the given storage.
     *
     * @param storage the AVRO row storage to operate on (must not be null)
     */
    public AvroBatchOperator(AvroRowStorage storage) {
        if (storage == null) throw new IllegalArgumentException("storage must not be null");
        this.storage = storage;
        ResolvedConfig cfg = resolveConfig();
        this.insertFlushSize = cfg.insertFlushSize;
        this.readEstimateFactor = cfg.readEstimateFactor;
        this.validationMode = cfg.validationMode;
    }

    /** Returns the backing storage. */
    public AvroRowStorage getStorage() {
        return storage;
    }

    /** Configured rows-between-flush mark. */
    public int getInsertFlushSize() {
        return insertFlushSize;
    }

    /** Configured over-allocation multiplier for predicted reads. */
    public double getReadEstimateFactor() {
        return readEstimateFactor;
    }

    /** Configured validation mode ({@code strict}, {@code permissive}, {@code off}). */
    public String getValidationMode() {
        return validationMode;
    }

    // ─── Batch insert ───────────────────────────────────────────────

    /**
     * Inserts a batch of name-keyed rows in one call. The whole batch runs
     * inside a deferred bulk-update window so per-row index bookkeeping is
     * coalesced; a flush mark is recorded every {@code insertFlushSize} rows.
     *
     * @param rows rows to insert (column name to value); null entries allowed
     * @return the number of rows inserted
     * @throws IllegalStateException    if the operator is closed
     * @throws IllegalArgumentException if {@code rows} is null
     */
    public int insertBatch(List<Map<String, Object>> rows) {
        ensureOpen();
        if (rows == null) throw new IllegalArgumentException("rows must not be null");
        if (rows.isEmpty()) return 0;

        long start = System.nanoTime();
        int inserted = 0;
        storage.beginBulkUpdate();
        try {
            for (Map<String, Object> row : rows) {
                storage.insert(row);
                inserted++;
                if (inserted % insertFlushSize == 0) {
                    stats.addFlush();
                }
            }
        } finally {
            storage.endBulkUpdate();
        }

        long elapsed = System.nanoTime() - start;
        stats.addInsert(inserted, estimateBatchBytes(rows), elapsed);
        LOGGER.debug("Batch insert: {} rows in {}ms", inserted, elapsed / 1_000_000);
        return inserted;
    }

    /**
     * Inserts a batch of positional rows in the internal compact form. This is
     * the fastest path when the caller already holds {@code Object[]} rows in
     * column order — no {@code Map} conversion is performed.
     *
     * @param rows positional rows in column order
     * @return the number of rows inserted
     * @throws IllegalStateException    if the operator is closed
     * @throws IllegalArgumentException if {@code rows} is null
     */
    public int insertBatchRaw(List<Object[]> rows) {
        ensureOpen();
        if (rows == null) throw new IllegalArgumentException("rows must not be null");
        if (rows.isEmpty()) return 0;

        long start = System.nanoTime();
        List<Object[]> internalRows = storage.getInternalRows();
        storage.beginBulkUpdate();
        try {
            for (Object[] row : rows) {
                internalRows.add(row);
            }
        } finally {
            storage.endBulkUpdate();
        }

        long elapsed = System.nanoTime() - start;
        long bytes = 0;
        for (Object[] row : rows) {
            bytes += estimateRowBytes(row);
        }
        stats.addInsert(rows.size(), bytes, elapsed);
        LOGGER.debug("Batch insert raw: {} rows in {}ms", rows.size(), elapsed / 1_000_000);
        return rows.size();
    }

    /**
     * Streams every record from an {@link AvroDataFileReader} directly into the
     * storage. The reader is consumed from its current position to EOF and is
     * <b>not</b> closed by this method.
     *
     * @param reader an open AVRO data reader
     * @return the number of rows imported
     * @throws IllegalStateException    if the operator is closed
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public int importFromReader(AvroDataFileReader reader) {
        ensureOpen();
        if (reader == null) throw new IllegalArgumentException("reader must not be null");

        long start = System.nanoTime();
        int imported = 0;
        List<String> columns = storage.getColumns();
        Map<String, Class<?>> columnTypes = storage.getColumnTypes();
        Class<?>[] targetTypes = AvroRowStorage.resolveColumnTypes(columns, columnTypes);
        List<Object[]> internalRows = storage.getInternalRows();

        storage.beginBulkUpdate();
        try {
            while (reader.hasNext()) {
                GenericRecord record = reader.next();
                internalRows.add(AvroRowStorage.fromRecord(record, columns, targetTypes));
                imported++;
                if (imported % insertFlushSize == 0) {
                    stats.addFlush();
                }
            }
        } finally {
            storage.endBulkUpdate();
        }

        long elapsed = System.nanoTime() - start;
        stats.addInsert(imported, imported * 64L, elapsed);
        LOGGER.debug("Import from reader: {} rows in {}ms", imported, elapsed / 1_000_000);
        return imported;
    }

    // ─── Batch read with size prediction ────────────────────────────

    /**
     * Reads all rows with the result list pre-allocated from a prediction.
     * The prediction is multiplied by {@link #getReadEstimateFactor()} before
     * allocation; an under-estimate simply grows the list, an over-estimate
     * leaves spare capacity that is not trimmed (so the same operator can be
     * reused for similarly sized batches).
     *
     * @param predictedSize caller's estimate of the total row count
     * @return every row as a name-keyed map
     * @throws IllegalStateException if the operator is closed
     */
    public List<Map<String, Object>> readBatchWithPrediction(int predictedSize) {
        ensureOpen();
        if (predictedSize < 0) predictedSize = 0;

        long start = System.nanoTime();
        int allocated = allocateSize(predictedSize);
        List<String> columns = storage.getColumns();
        List<Map<String, Object>> result = new ArrayList<>(allocated);
        for (Object[] row : storage.getInternalRows()) {
            result.add(toMap(row, columns));
        }

        long elapsed = System.nanoTime() - start;
        stats.addRead(result.size(), elapsed);
        LOGGER.debug("Batch read: predicted={} allocated={} actual={} in {}ms",
                predictedSize, allocated, result.size(), elapsed / 1_000_000);
        return result;
    }

    /**
     * Reads a contiguous window of rows with the result list pre-allocated
     * from a prediction. The window is clamped to the available data, so an
     * offset past the end yields an empty list.
     *
     * @param offset        start index (0-based, inclusive)
     * @param limit         maximum rows to return
     * @param predictedSize caller's estimate of the window size
     * @return the requested rows as name-keyed maps
     * @throws IllegalStateException    if the operator is closed
     * @throws IllegalArgumentException if {@code offset} or {@code limit} is negative
     */
    public List<Map<String, Object>> readBatchWindow(int offset, int limit, int predictedSize) {
        ensureOpen();
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        if (limit < 0) throw new IllegalArgumentException("limit must be >= 0");
        if (predictedSize < 0) predictedSize = 0;

        long start = System.nanoTime();
        int allocated = Math.min(allocateSize(predictedSize), limit);
        List<String> columns = storage.getColumns();
        List<Object[]> internalRows = storage.getInternalRows();
        int end = Math.min(offset + limit, internalRows.size());

        List<Map<String, Object>> result = new ArrayList<>(Math.max(allocated, 0));
        for (int i = offset; i < end; i++) {
            result.add(toMap(internalRows.get(i), columns));
        }

        long elapsed = System.nanoTime() - start;
        stats.addRead(result.size(), elapsed);
        LOGGER.debug("Batch read window: offset={} limit={} actual={} in {}ms",
                offset, limit, result.size(), elapsed / 1_000_000);
        return result;
    }

    /**
     * Predicts the row count from the storage's current size, suitable as the
     * argument to {@link #readBatchWithPrediction(int)}.
     */
    public int estimateRowCount() {
        return storage.getInternalRows().size();
    }

    // ─── Transaction batching ───────────────────────────────────────

    /**
     * Begins a transactional batch, snapshotting the current in-memory rows so
     * they can be restored by {@link #rollbackBatch()}.
     *
     * @throws IllegalStateException if the operator is closed or a transaction
     *                               is already in progress
     */
    public void beginBatch() {
        ensureOpen();
        if (inTransaction) {
            throw new IllegalStateException("A transaction is already in progress");
        }
        List<Object[]> internalRows = storage.getInternalRows();
        snapshot = new ArrayList<>(internalRows.size());
        for (Object[] row : internalRows) {
            snapshot.add(row == null ? null : row.clone());
        }
        inTransaction = true;
        LOGGER.debug("Transaction batch begun ({} rows snapshotted)", snapshot.size());
    }

    /**
     * Commits the current transactional batch, releasing the snapshot.
     *
     * @throws IllegalStateException if the operator is closed or no transaction
     *                               is in progress
     */
    public void commitBatch() {
        ensureOpen();
        if (!inTransaction) {
            throw new IllegalStateException("No transaction in progress to commit");
        }
        snapshot = null;
        inTransaction = false;
        stats.addTransaction();
        LOGGER.debug("Transaction batch committed");
    }

    /**
     * Rolls back the current transactional batch, restoring the rows captured
     * by {@link #beginBatch()} and rebuilding the storage's index state.
     *
     * @throws IllegalStateException if the operator is closed or no transaction
     *                               is in progress
     */
    public void rollbackBatch() {
        ensureOpen();
        if (!inTransaction) {
            throw new IllegalStateException("No transaction in progress to rollback");
        }
        List<String> columns = storage.getColumns();
        List<Map<String, Object>> restored = new ArrayList<>(snapshot.size());
        for (Object[] row : snapshot) {
            restored.add(toMap(row, columns));
        }
        storage.setRows(restored);
        snapshot = null;
        inTransaction = false;
        LOGGER.debug("Transaction batch rolled back ({} rows restored)", restored.size());
    }

    /** Whether a transactional batch is currently open. */
    public boolean isInTransaction() {
        return inTransaction;
    }

    // ─── Statistics ─────────────────────────────────────────────────

    /** Returns an immutable snapshot of the accumulated statistics. */
    public BatchStats getStats() {
        return stats.snapshot();
    }

    /** Returns the live atomic counters for progress polling. */
    public MutableBatchStats mutableStats() {
        return stats;
    }

    /** Resets all accumulated statistics to zero. */
    public void resetStats() {
        stats.reset();
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    /**
     * Closes the operator. An open transactional batch is rolled back
     * automatically; closing twice is a no-op.
     */
    @Override
    public void close() {
        if (closed) return;
        if (inTransaction) {
            LOGGER.warn("Closing AvroBatchOperator with an uncommitted transaction — rolling back");
            rollbackBatch();
        }
        closed = true;
        LOGGER.debug("AvroBatchOperator closed: {}", stats.snapshot());
    }

    /** Whether this operator has been closed. */
    public boolean isClosed() {
        return closed;
    }

    // ─── Internal helpers ───────────────────────────────────────────

    private int allocateSize(int predictedSize) {
        long scaled = Math.round(predictedSize * readEstimateFactor);
        if (scaled < 0) return 0;
        if (scaled > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) scaled;
    }

    private static Map<String, Object> toMap(Object[] row, List<String> columns) {
        Map<String, Object> map = new java.util.HashMap<>(Math.max(columns.size() * 2, 4));
        if (row == null) return map;
        for (int i = 0; i < columns.size() && i < row.length; i++) {
            map.put(columns.get(i), row[i]);
        }
        return map;
    }

    private static long estimateBatchBytes(List<Map<String, Object>> rows) {
        long bytes = 0;
        for (Map<String, Object> row : rows) {
            if (row == null) continue;
            for (Object val : row.values()) {
                bytes += estimateValueBytes(val);
            }
        }
        return bytes;
    }

    private static long estimateRowBytes(Object[] row) {
        if (row == null) return 0;
        long bytes = 0;
        for (Object val : row) {
            bytes += estimateValueBytes(val);
        }
        return bytes;
    }

    private static long estimateValueBytes(Object val) {
        if (val == null) return 1;
        if (val instanceof String s) return s.length() * 2L + 16;
        if (val instanceof byte[] b) return b.length + 8L;
        if (val instanceof CharSequence cs) return cs.length() * 2L + 16;
        return 16;
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("AvroBatchOperator is closed");
    }

    // ─── Config resolution ──────────────────────────────────────────

    private record ResolvedConfig(int insertFlushSize, double readEstimateFactor, String validationMode) { }

    static ResolvedConfig resolveConfig() {
        Properties props = loadConfigFile();
        int flushSize = resolveInt(props, INSERT_FLUSH_SIZE_KEY, DEFAULT_INSERT_FLUSH_SIZE, 1, 1_000_000);
        double factor = resolveDouble(props, READ_ESTIMATE_FACTOR_KEY, DEFAULT_READ_ESTIMATE_FACTOR, 1.0, 100.0);
        String mode = resolveString(props, VALIDATION_MODE_KEY, DEFAULT_VALIDATION_MODE);
        return new ResolvedConfig(flushSize, factor, mode);
    }

    private static Properties loadConfigFile() {
        Properties props = new Properties();
        String overridePath = System.getProperty(CONFIG_FILE_KEY);
        File configFile;
        if (overridePath != null && !overridePath.isBlank()) {
            configFile = new File(overridePath);
        } else {
            configFile = new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        }
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException e) {
                LOGGER.warn("Failed to read batch config {}: {}", configFile.getPath(), e.getMessage());
            }
        }
        return props;
    }

    private static int resolveInt(Properties props, String key, int fallback, int min, int max) {
        String val = firstNonBlank(System.getProperty(key), props.getProperty(key));
        if (val == null) return fallback;
        try {
            int parsed = Integer.parseInt(val.trim());
            int clamped = Math.max(min, Math.min(max, parsed));
            if (clamped != parsed) {
                LOGGER.warn("{} value {} out of range [{}, {}] — clamped to {}", key, parsed, min, max, clamped);
            }
            return clamped;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid integer for {}: '{}' — using default {}", key, val, fallback);
            return fallback;
        }
    }

    private static double resolveDouble(Properties props, String key, double fallback, double min, double max) {
        String val = firstNonBlank(System.getProperty(key), props.getProperty(key));
        if (val == null) return fallback;
        try {
            double parsed = Double.parseDouble(val.trim());
            double clamped = Math.max(min, Math.min(max, parsed));
            if (clamped != parsed) {
                LOGGER.warn("{} value {} out of range [{}, {}] — clamped to {}", key, parsed, min, max, clamped);
            }
            return clamped;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid double for {}: '{}' — using default {}", key, val, fallback);
            return fallback;
        }
    }

    private static String resolveString(Properties props, String key, String fallback) {
        String val = firstNonBlank(System.getProperty(key), props.getProperty(key));
        return val == null ? fallback : val.trim().toLowerCase(java.util.Locale.ROOT);
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return null;
    }
}
