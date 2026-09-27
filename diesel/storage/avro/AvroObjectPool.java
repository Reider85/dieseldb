package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import diesel.ConfigKeys;

/**
 * AVRO object pool (Prompt 79).
 *
 * <p>The class reduces GC pressure in the AVRO write/read hot paths by
 * re-using three high-allocation object families instead of allocating them
 * per row / per file:
 * <ul>
 *   <li><b>{@link GenericRecord}</b> — {@link #borrowRecord(Schema)} returns an
 *       already-allocated {@code GenericRecord} for the requested schema (its
 *       backing {@code Object[]} array and {@link Schema} reference are kept);
 *       {@link #returnRecord(GenericRecord)} nulls every field value so the next
 *       borrower sees a clean instance. Keyed by the record schema.</li>
*   <li><b>{@link DatumWriter}</b> — {@link #borrowWriter(Schema)} /
     *       {@link #returnWriter(DatumWriter, Schema)} recycle {@code GenericDatumWriter}
     *       instances keyed by the writer schema. Borrowed writers are owned
     *       exclusively by the borrowing thread until returned.</li>
     *   <li><b>{@link GenericDatumReader}</b> — {@link #borrowReader(Schema, Schema)} /
     *       {@link #returnReader(GenericDatumReader, Schema, Schema)} recycle reader instances keyed
 *       by the {@code (writer schema, reader schema)} pair, covering the
 *       projection-pushdown case where the reader schema differs from the writer
 *       schema.</li>
 * </ul>
 *
 * <p>Capacity is enforced <em>per schema key</em>: a queue never grows past its
 * configured budget ({@code avro.pool.record.capacity} /
 * {@code avro.pool.writer.capacity} / {@code avro.pool.reader.capacity}), so
 * excess returns are simply handed back to the garbage collector. All internal
 * state is thread-safe ({@link ConcurrentHashMap} + {@link ConcurrentLinkedDeque}
 * + {@link AtomicLong}), so the pool can back the parallel reader/writer paths
 * directly.
 *
 * <p><b>Metrics.</b> The pool tracks allocation and reuse counters per object
 * family, active (borrowed-out) counts, pooled counts and two GC-oriented
 * gauges: the <em>allocation rate</em> ({@link MetricsSnapshot#allocationRatePerSec()},
 * total object allocations per elapsed second since the pool epoch) and the
 * <em>GC pause time</em> ({@link MetricsSnapshot#gcPauseTimeMs()}), sampled from
 * the JVM {@link GarbageCollectorMXBean}s. {@link #snapshot()} returns an
 * immutable {@link MetricsSnapshot}.
 *
 * <p><b>Lifecycle / config.</b> {@code avro.pool.enabled} (default
 * {@code true}), {@code avro.pool.record.capacity} (256),
 * {@code avro.pool.writer.capacity} (8) and {@code avro.pool.reader.capacity}
 * (8) are resolved from a system-property override first, then the root
 * {@code config.properties}, then the code defaults (the {@code avro.pool.config.file}
 * system-property hook redirects the properties file for tests). {@link #close()}
 * purges every pool and degrades the instance to always-fresh allocations so
 * long-lived readers/writers that still hold a reference keep working; a new
 * transient pool is created with {@link #newPool()} / {@link #newPool(Config)}.
 *
 * @since Prompt 79
 */
public final class AvroObjectPool implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroObjectPool.class);

    /** Config key: global pool on/off switch. */
    public static final String ENABLED_KEY = "avro.pool.enabled";
    /** Config key: maximum pooled {@link GenericRecord}s per schema. */
    public static final String RECORD_CAPACITY_KEY = "avro.pool.record.capacity";
    /** Config key: maximum pooled {@link DatumWriter}s per schema. */
    public static final String WRITER_CAPACITY_KEY = "avro.pool.writer.capacity";
    /** Config key: maximum pooled {@link GenericDatumReader}s per (writer, reader) pair. */
    public static final String READER_CAPACITY_KEY = "avro.pool.reader.capacity";

    /** Config key: override the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.pool.config.file";

    /** Code-level defaults. */
    public static final boolean DEFAULT_ENABLED = true;
    public static final int DEFAULT_RECORD_CAPACITY = 256;
    public static final int DEFAULT_WRITER_CAPACITY = 8;
    public static final int DEFAULT_READER_CAPACITY = 8;

    private final Config config;
    private final Map<Schema, Deque<GenericRecord>> records = new ConcurrentHashMap<>();
    private final Map<Schema, Deque<DatumWriter<GenericRecord>>> writers = new ConcurrentHashMap<>();
    private final Map<ReaderKey, Deque<GenericDatumReader<GenericRecord>>> readers = new ConcurrentHashMap<>();

    private final AtomicLong recordAllocations = new AtomicLong();
    private final AtomicLong recordHits = new AtomicLong();
    private final AtomicLong recordMisses = new AtomicLong();
    private final AtomicLong recordReturns = new AtomicLong();
    private final AtomicLong writerAllocations = new AtomicLong();
    private final AtomicLong writerHits = new AtomicLong();
    private final AtomicLong writerMisses = new AtomicLong();
    private final AtomicLong writerReturns = new AtomicLong();
    private final AtomicLong readerAllocations = new AtomicLong();
    private final AtomicLong readerHits = new AtomicLong();
    private final AtomicLong readerMisses = new AtomicLong();
    private final AtomicLong readerReturns = new AtomicLong();

    private final AtomicLong activeRecords = new AtomicLong();
    private final AtomicLong activeWriters = new AtomicLong();
    private final AtomicLong activeReaders = new AtomicLong();

    private final long epochMs = System.currentTimeMillis();
    private volatile boolean closed;

    /**
     * Resolved AVRO object-pool configuration (Prompt 79): the global on/off
     * switch and the per-key capacities, resolved sysprop → config.properties
     * → code defaults. Invalid values fall back to the defaults with a WARN.
     */
    public static final class Config {

        private final boolean enabled;
        private final int recordCapacity;
        private final int writerCapacity;
        private final int readerCapacity;

        private Config(boolean enabled, int recordCapacity, int writerCapacity, int readerCapacity) {
            this.enabled = enabled;
            this.recordCapacity = recordCapacity;
            this.writerCapacity = writerCapacity;
            this.readerCapacity = readerCapacity;
        }

        /** Whether pooling is globally enabled. */
        public boolean enabled() {
            return enabled;
        }

        /** Maximum pooled {@link GenericRecord}s per schema key. */
        public int recordCapacity() {
            return recordCapacity;
        }

        /** Maximum pooled {@link DatumWriter}s per schema key. */
        public int writerCapacity() {
            return writerCapacity;
        }

        /** Maximum pooled {@link GenericDatumReader}s per (writer, reader) key. */
        public int readerCapacity() {
            return readerCapacity;
        }

        /**
         * Resolves the configuration: system property override first, then the
         * root {@code config.properties}, then the code defaults. Invalid values
         * fall back to the defaults with a WARN log.
         *
         * @return the resolved configuration (never {@code null})
         */
        public static Config resolve() {
            boolean enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
            int recordCapacity = getInt(RECORD_CAPACITY_KEY, DEFAULT_RECORD_CAPACITY);
            if (recordCapacity < 0) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, RECORD_CAPACITY_KEY, recordCapacity, DEFAULT_RECORD_CAPACITY);
                recordCapacity = DEFAULT_RECORD_CAPACITY;
            }
            int writerCapacity = getInt(WRITER_CAPACITY_KEY, DEFAULT_WRITER_CAPACITY);
            if (writerCapacity < 0) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, WRITER_CAPACITY_KEY, writerCapacity, DEFAULT_WRITER_CAPACITY);
                writerCapacity = DEFAULT_WRITER_CAPACITY;
            }
            int readerCapacity = getInt(READER_CAPACITY_KEY, DEFAULT_READER_CAPACITY);
            if (readerCapacity < 0) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, READER_CAPACITY_KEY, readerCapacity, DEFAULT_READER_CAPACITY);
                readerCapacity = DEFAULT_READER_CAPACITY;
            }
            return new Config(enabled, recordCapacity, writerCapacity, readerCapacity);
        }

        private static boolean getBoolean(String key, boolean defaultValue) {
            String raw = getString(key, String.valueOf(defaultValue));
            if (raw == null) {
                return defaultValue;
            }
            String v = raw.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(v) || "on".equals(v) || "yes".equals(v) || "1".equals(v)) {
                return true;
            }
            if ("false".equals(v) || "off".equals(v) || "no".equals(v) || "0".equals(v)) {
                return false;
            }
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }

        private static String getString(String key, String defaultValue) {
            String systemValue = System.getProperty(key);
            if (systemValue != null) {
                return systemValue;
            }
            String prop = rootProps().getProperty(key);
            return prop == null ? defaultValue : prop;
        }

        private static int getInt(String key, int defaultValue) {
            String raw = getString(key, String.valueOf(defaultValue));
            try {
                return Integer.parseInt(raw.trim());
            } catch (RuntimeException e) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
                return defaultValue;
            }
        }

        private static Properties rootProps() {
            Properties props = new Properties();
            String override = System.getProperty(CONFIG_FILE_KEY);
            File configFile = (override != null && !override.isBlank())
                    ? new File(override)
                    : new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
            if (configFile.exists()) {
                try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                    props.load(in);
                } catch (IOException ignored) {
                    LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
                }
            }
            return props;
        }

        @Override
        public String toString() {
            return "AvroObjectPool.Config{enabled=" + enabled
                    + ", recordCapacity=" + recordCapacity
                    + ", writerCapacity=" + writerCapacity
                    + ", readerCapacity=" + readerCapacity + '}';
        }
    }

    /**
     * Immutable metrics snapshot (Prompt 79). Allocation counters count object
     * creations that could not be served from a pool; pool-hits count borrows
     * satisfied from a pool; allocations are also reflected in
     * {@link #allocationRatePerSec()}.
     */
    public record MetricsSnapshot(
            boolean poolingEnabled,
            long recordCapacity,
            long writerCapacity,
            long readerCapacity,
            long recordAllocations,
            long recordPoolHits,
            long recordPoolMisses,
            long recordReturns,
            long writerAllocations,
            long writerPoolHits,
            long writerPoolMisses,
            long writerReturns,
            long readerAllocations,
            long readerPoolHits,
            long readerPoolMisses,
            long readerReturns,
            long activeRecords,
            long activeWriters,
            long activeReaders,
            long pooledRecords,
            long pooledWriters,
            long pooledReaders,
            long gcPauseTimeMs,
            double allocationRatePerSec,
            long elapsedSeconds,
            long timestampMs) {

        /** Total object allocations (records + writers + readers) since the epoch. */
        public long totalAllocations() {
            return recordAllocations + writerAllocations + readerAllocations;
        }

        /** Total pool hits (records + writers + readers) since the epoch. */
        public long totalPoolHits() {
            return recordPoolHits + writerPoolHits + readerPoolHits;
        }

        /** Total pool misses (records + writers + readers) since the epoch. */
        public long totalPoolMisses() {
            return recordPoolMisses + writerPoolMisses + readerPoolMisses;
        }
    }

    private record ReaderKey(Schema writer, Schema reader) {
        // structural equality on the (content-equal) Schema fields
    }

    private AvroObjectPool(Config config) {
        this.config = config;
    }

    /**
     * Creates a fresh pool from the resolved {@link Config#resolve()}.
     *
     * @return a new, independent pool
     */
    public static AvroObjectPool newPool() {
        return newPool(Config.resolve());
    }

    /**
     * Creates a fresh pool from an explicit configuration.
     *
     * @param config the resolved configuration (may be {@code null} to use
     *               {@link Config#resolve()})
     * @return a new, independent pool
     */
    public static AvroObjectPool newPool(Config config) {
        return new AvroObjectPool(config != null ? config : Config.resolve());
    }

    private static final class InstanceHolder {
        private static final AvroObjectPool INSTANCE = newPool();
    }

    /**
     * Returns the shared process-wide pool. It is resolved lazily from
     * {@link Config#resolve()} on first use.
     *
     * @return the shared pool
     */
    public static AvroObjectPool instance() {
        return InstanceHolder.INSTANCE;
    }

    /** The resolved configuration of this pool. */
    public Config config() {
        return config;
    }

    /** Whether pooling is globally enabled for this instance. */
    public boolean isEnabled() {
        return config.enabled() && !closed;
    }

    // ─── GenericRecord pool ─────────────────────────────────────────

    /**
     * Borrows a {@link GenericRecord} for the given schema: a pooled instance if
     * one is available (all fields {@code null}), otherwise a fresh
     * {@code new GenericData.Record(schema)}. The caller owns the instance
     * exclusively and must hand it back via {@link #returnRecord(GenericRecord)}.
     *
     * @param schema the record schema
     * @return a clean record for the schema
     */
    public GenericRecord borrowRecord(Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("record schema must not be null");
        }
        if (!isEnabled()) {
            recordAllocations.incrementAndGet();
            activeRecords.incrementAndGet();
            return new org.apache.avro.generic.GenericData.Record(schema);
        }
        Deque<GenericRecord> queue = records.get(schema);
        GenericRecord record = queue != null ? queue.poll() : null;
        if (record != null) {
            recordHits.incrementAndGet();
        } else {
            recordMisses.incrementAndGet();
            recordAllocations.incrementAndGet();
            record = new org.apache.avro.generic.GenericData.Record(schema);
        }
        activeRecords.incrementAndGet();
        return record;
    }

    /**
     * Returns a borrowed record to the pool. Every field value is nulled so the
     * next borrower sees a clean instance; the backing array and schema are kept.
     * If the per-schema queue has reached {@code avro.pool.record.capacity}, the
     * record is dropped for the garbage collector.
     *
     * @param record the record to recycle (may be {@code null} = no-op)
     */
    public void returnRecord(GenericRecord record) {
        activeRecords.decrementAndGet();
        if (!isEnabled() || record == null) {
            return;
        }
        clear(record);
        recordReturns.incrementAndGet();
        Deque<GenericRecord> queue = records.computeIfAbsent(record.getSchema(),
                s -> new ConcurrentLinkedDeque<>());
        synchronized (queue) {
            if (queue.size() < config.recordCapacity()) {
                queue.add(record);
            }
        }
    }

    private static void clear(GenericRecord record) {
        List<Schema.Field> fields = record.getSchema().getFields();
        for (int i = 0; i < fields.size(); i++) {
            record.put(i, null);
        }
    }

    // ─── DatumWriter pool ───────────────────────────────────────────

    /**
     * Borrows a {@link DatumWriter} for the given writer schema: a pooled
     * {@code GenericDatumWriter} if one is available, else a fresh one. The
     * borrowed writer is owned by the caller (typically wrapped into a
     * {@link org.apache.avro.file.DataFileWriter}) and must be handed back via
     * {@link #returnWriter(DatumWriter, Schema)}.
     *
     * @param writerSchema the writer schema
     * @return an exclusively owned writer
     */
    public DatumWriter<GenericRecord> borrowWriter(Schema writerSchema) {
        if (writerSchema == null) {
            throw new IllegalArgumentException("writer schema must not be null");
        }
        if (!isEnabled()) {
            writerAllocations.incrementAndGet();
            activeWriters.incrementAndGet();
            return new org.apache.avro.generic.GenericDatumWriter<>(writerSchema);
        }
        Deque<DatumWriter<GenericRecord>> queue = writers.get(writerSchema);
        DatumWriter<GenericRecord> writer = queue != null ? queue.poll() : null;
        if (writer != null) {
            writerHits.incrementAndGet();
        } else {
            writerMisses.incrementAndGet();
            writerAllocations.incrementAndGet();
            writer = new org.apache.avro.generic.GenericDatumWriter<>(writerSchema);
        }
        activeWriters.incrementAndGet();
        return writer;
    }

    /**
     * Returns a borrowed {@link DatumWriter} to the pool, subject to the
     * per-schema {@code avro.pool.writer.capacity} budget.
     *
     * @param writer       the writer to recycle (may be {@code null} = no-op)
     * @param writerSchema the writer schema key used at borrow time (may be
     *                     {@code null} = no-op)
     */
    public void returnWriter(DatumWriter<GenericRecord> writer, Schema writerSchema) {
        activeWriters.decrementAndGet();
        if (!isEnabled() || writer == null || writerSchema == null) {
            return;
        }
        writerReturns.incrementAndGet();
        Deque<DatumWriter<GenericRecord>> queue = writers.computeIfAbsent(writerSchema,
                s -> new ConcurrentLinkedDeque<>());
        synchronized (queue) {
            if (queue.size() < config.writerCapacity()) {
                queue.add(writer);
            }
        }
    }

    // ─── DatumReader pool ───────────────────────────────────────────

    /**
     * Borrows a {@link GenericDatumReader} for the given (writer, reader) schema
     * pair: a pooled instance if one is available, else a fresh
     * {@code new GenericDatumReader<>(writerSchema, readerSchema)}. The reader is
     * owned exclusively by the caller and must be returned via
     * {@link #returnReader(GenericDatumReader, Schema, Schema)}.
     *
     * @param writerSchema the writer schema from the container header
     * @param readerSchema the reader schema (projection or sidecar); may be the
     *                     same object as {@code writerSchema} for a full read
     * @return an exclusively owned reader
     */
    public GenericDatumReader<GenericRecord> borrowReader(Schema writerSchema, Schema readerSchema) {
        if (writerSchema == null) {
            throw new IllegalArgumentException("writer schema must not be null");
        }
        if (readerSchema == null) {
            throw new IllegalArgumentException("reader schema must not be null (pass the writer schema for a full read)");
        }
        if (!isEnabled()) {
            readerAllocations.incrementAndGet();
            activeReaders.incrementAndGet();
            return new GenericDatumReader<>(writerSchema, readerSchema);
        }
        ReaderKey key = new ReaderKey(writerSchema, readerSchema);
        Deque<GenericDatumReader<GenericRecord>> queue = readers.get(key);
        GenericDatumReader<GenericRecord> reader = queue != null ? queue.poll() : null;
        if (reader != null) {
            readerHits.incrementAndGet();
        } else {
            readerMisses.incrementAndGet();
            readerAllocations.incrementAndGet();
            reader = new GenericDatumReader<>(writerSchema, readerSchema);
        }
        activeReaders.incrementAndGet();
        return reader;
    }

    /**
     * Returns a borrowed {@link GenericDatumReader} to the pool, subject to the
     * per-key {@code avro.pool.reader.capacity} budget.
     *
     * @param reader    the reader to recycle (may be {@code null} = no-op)
     * @param writerSchema the writer schema key used at borrow time (may be
     *                     {@code null} = no-op)
     * @param readerSchema the reader schema key used at borrow time (irrelevant
     *                     when {@code writerSchema} is {@code null})
     */
    public void returnReader(GenericDatumReader<GenericRecord> reader, Schema writerSchema, Schema readerSchema) {
        activeReaders.decrementAndGet();
        if (!isEnabled() || reader == null || writerSchema == null) {
            return;
        }
        readerReturns.incrementAndGet();
        ReaderKey key = new ReaderKey(writerSchema, readerSchema);
        Deque<GenericDatumReader<GenericRecord>> queue = readers.computeIfAbsent(key,
                s -> new ConcurrentLinkedDeque<>());
        synchronized (queue) {
            if (queue.size() < config.readerCapacity()) {
                queue.add(reader);
            }
        }
    }

    /** Variant of {@link #returnReader(GenericDatumReader, Schema, Schema)} with a single (full-read) schema. */
    public void returnReader(GenericDatumReader<GenericRecord> reader, Schema schema) {
        returnReader(reader, schema, schema);
    }

    // ─── Metrics ────────────────────────────────────────────────────

    /**
     * Samples the JVM's cumulative garbage-collection collection time in
     * milliseconds across all GarbageCollectorMXBeans. This is a JVM-wide gauge,
     * so it only tells you how much GC the whole process has accrued — use it
     * relative to a baseline for meaningful deltas.
     *
     * @return cumulative GC collection time in milliseconds
     */
    public static long sampleGcTimeMs() {
        long total = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long t = bean.getCollectionTime();
            if (t >= 0) {
                total += t;
            }
        }
        return total;
    }

    /**
     * Returns an immutable metrics snapshot (config, allocation/reuse counters,
     * active and pooled counts, GC pause time, allocation rate).
     */
    public MetricsSnapshot snapshot() {
        long now = System.currentTimeMillis();
        double elapsedSeconds = Math.max(1.0, (now - epochMs) / 1000.0);
        double rate = (recordAllocations.get() + writerAllocations.get() + readerAllocations.get())
                / elapsedSeconds;
        return new MetricsSnapshot(
                isEnabled(),
                config.recordCapacity(),
                config.writerCapacity(),
                config.readerCapacity(),
                recordAllocations.get(), recordHits.get(), recordMisses.get(), recordReturns.get(),
                writerAllocations.get(), writerHits.get(), writerMisses.get(), writerReturns.get(),
                readerAllocations.get(), readerHits.get(), readerMisses.get(), readerReturns.get(),
                activeRecords.get(), activeWriters.get(), activeReaders.get(),
                pooledRecords(), pooledWriters(), pooledReaders(),
                sampleGcTimeMs(), rate, (now - epochMs) / 1000L, now);
    }

    /** Number of records currently pooled across all schemas. */
    public long pooledRecords() {
        return records.values().stream().mapToLong(Deque::size).sum();
    }

    /** Number of writers currently pooled across all schemas. */
    public long pooledWriters() {
        return writers.values().stream().mapToLong(Deque::size).sum();
    }

    /** Number of readers currently pooled across all (writer, reader) keys. */
    public long pooledReaders() {
        return readers.values().stream().mapToLong(Deque::size).sum();
    }

    /** Total object allocations (records + writers + readers) since the epoch. */
    public long totalAllocations() {
        return recordAllocations.get() + writerAllocations.get() + readerAllocations.get();
    }

    /** Total pool hits (records + writers + readers) since the epoch. */
    public long totalPoolHits() {
        return recordHits.get() + writerHits.get() + readerHits.get();
    }

    /** Total pool misses (records + writers + readers) since the epoch. */
    public long totalPoolMisses() {
        return recordMisses.get() + writerMisses.get() + readerMisses.get();
    }

    /** Resets every metrics counter (allocations, hits, misses, returns, actives). Pools are untouched. */
    public void resetMetrics() {
        recordAllocations.set(0);
        recordHits.set(0);
        recordMisses.set(0);
        recordReturns.set(0);
        writerAllocations.set(0);
        writerHits.set(0);
        writerMisses.set(0);
        writerReturns.set(0);
        readerAllocations.set(0);
        readerHits.set(0);
        readerMisses.set(0);
        readerReturns.set(0);
        activeRecords.set(0);
        activeWriters.set(0);
        activeReaders.set(0);
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    /**
     * Purges every pool (records, writers, readers) without closing the pool.
     * The resolved configuration is preserved.
     */
    public void reset() {
        records.clear();
        writers.clear();
        readers.clear();
    }

    /**
     * Purges every pool and marks the pool as closed. After closing, borrow
     * calls degrade gracefully to always-fresh allocations (so code that still
     * holds a reference keeps working), and return calls become no-ops. Closing
     * is idempotent.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        clear(records);
        clear(writers);
        clear(readers);
    }

    private static <T> void clear(Map<?, Deque<T>> pools) {
        pools.values().forEach(Deque::clear);
    }

    @Override
    public String toString() {
        return "AvroObjectPool{" + config + ", pooledRecords=" + pooledRecords()
                + ", pooledWriters=" + pooledWriters() + ", pooledReaders=" + pooledReaders() + '}';
    }
}