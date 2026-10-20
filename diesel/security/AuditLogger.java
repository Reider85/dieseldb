package diesel.security;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Asynchronous SQL audit logger (prompt4 #33, R3-008 step 4/4).
 *
 * <p>Producers (the engine's query entry points) hand records to a bounded
 * queue via a non-blocking {@code offer} - a full queue drops the record and
 * increments the {@link #getDroppedCount() dropped} metric instead of ever
 * blocking the main query. A single daemon thread drains the queue in batches
 * and appends them to the {@code DIESEL_AUDIT_LOG} table through an
 * {@link AuditBackend}.
 *
 * <p>Retention: rows older than {@code audit.retention.days} (default 90) are
 * appended to a gzip cold-archive file under {@code audit.archive.dir} and
 * then deleted from the table - archived, never lost. Runs on the consumer
 * thread at most once per {@code audit.retention.interval.ms}.
 *
 * <p>Config resolution: system property - then {@code config.properties} -
 * then default (same chain as {@code AvroAuditLogger}); {@code audit.enabled}
 * defaults to false so development workloads pay nothing beyond a null check
 * at the call site.
 */
public final class AuditLogger {

    private static final Logger LOGGER = Logger.getLogger(AuditLogger.class.getName());

    public static final String ENABLED_KEY = "audit.enabled";
    public static final String RETENTION_DAYS_KEY = "audit.retention.days";
    public static final String QUEUE_CAPACITY_KEY = "audit.queue.capacity";
    public static final String RETENTION_INTERVAL_MS_KEY = "audit.retention.interval.ms";
    public static final String ARCHIVE_DIR_KEY = "audit.archive.dir";

    public static final boolean DEFAULT_ENABLED = false;
    public static final int DEFAULT_RETENTION_DAYS = 90;
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;
    public static final long DEFAULT_RETENTION_INTERVAL_MS = 86_400_000L;

    private static final String ARCHIVE_FILE_PREFIX = "audit-";
    private static final String ARCHIVE_FILE_SUFFIX = ".log.gz";
    private static final DateTimeFormatter ARCHIVE_TS =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final AuditBackend backend;
    private final boolean enabled;
    private final int retentionDays;
    private final long retentionIntervalMs;
    private final Path archiveDir;
    private final Clock clock;

    private final ArrayBlockingQueue<AuditRecord> queue;
    private final AtomicLong droppedCount = new AtomicLong();
    private final AtomicLong recordedCount = new AtomicLong();

    private volatile boolean running;
    private Thread consumer;
    private long lastRetentionMs;
    private final Object closeLock = new Object();
    /** Serializes every backend write (consumer, test flush, retention) so the table is never mutated concurrently. */
    private final Object writeLock = new Object();

    /**
     * Resolves the configuration (sysprop - then config.properties - then
     * defaults) and builds the logger; when {@code audit.enabled} is false the
     * returned instance records nothing and starts no thread.
     *
     * @param backend    the storage bridge for {@code DIESEL_AUDIT_LOG}
     * @param defaultArchiveDir directory for cold retention archives
     */
    public static AuditLogger create(AuditBackend backend, Path defaultArchiveDir) {
        boolean enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        int retentionDays = getInt(RETENTION_DAYS_KEY, DEFAULT_RETENTION_DAYS);
        int capacity = getInt(QUEUE_CAPACITY_KEY, DEFAULT_QUEUE_CAPACITY);
        long interval = getLong(RETENTION_INTERVAL_MS_KEY, DEFAULT_RETENTION_INTERVAL_MS);
        Path archiveDir = Path.of(getString(ARCHIVE_DIR_KEY, defaultArchiveDir.toString()));
        return new AuditLogger(backend, enabled, retentionDays, capacity, interval,
                archiveDir, Clock.systemDefaultZone());
    }

    /** Full constructor (tests inject a fake backend, clock and small queue). */
    public AuditLogger(AuditBackend backend, boolean enabled, int retentionDays,
                       int queueCapacity, long retentionIntervalMs, Path archiveDir,
                       Clock clock) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.enabled = enabled;
        this.retentionDays = retentionDays;
        this.retentionIntervalMs = retentionIntervalMs;
        this.archiveDir = Objects.requireNonNull(archiveDir, "archiveDir");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        if (enabled) {
            this.running = true;
            this.consumer = new Thread(this::consumeLoop, "diesel-audit-writer");
            this.consumer.setDaemon(true);
            this.consumer.start();
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Number of records dropped because the queue was full. */
    public long getDroppedCount() {
        return droppedCount.get();
    }

    /** Number of records successfully handed to the backend. */
    public long getRecordedCount() {
        return recordedCount.get();
    }

    /**
     * Enqueues an audit record. Never blocks: when the bounded queue is full
     * the record is dropped and the drop metric incremented.
     */
    public void record(String userName, String sourceIp, String query,
                       String status, long durationMs) {
        if (!enabled || !running) {
            return;
        }
        AuditRecord entry = AuditRecord.create(userName, sourceIp, query, status, durationMs);
        if (!queue.offer(entry)) {
            long dropped = droppedCount.incrementAndGet();
            if (dropped == 1 || dropped % 1000 == 0) {
                LOGGER.log(Level.WARNING,
                        "Audit queue full, dropped {0} record(s) so far", dropped);
            }
        }
    }

    /**
     * Stops the consumer with a final drain: remaining queued records are
     * written before the thread dies. Safe to call twice.
     */
    public void close() {
        if (!enabled) {
            return;
        }
        synchronized (closeLock) {
            if (!running) {
                return;
            }
            running = false;
            Thread t = consumer;
            if (t != null) {
                // No eager interrupt: the consumer polls with a short timeout
                // inside writeLock, so it exits on its own within ~100ms and a
                // mid-write interrupt would abort the current batch
                // (ClosedByInterruptException). Interrupt is a last resort.
                try {
                    t.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (t.isAlive()) {
                    t.interrupt();
                    try {
                        t.join(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            synchronized (writeLock) {
                drainOnceLocked();
            }
        }
    }

    /**
     * Test hook: synchronously drains the queue on the caller thread.
     * Takes {@link #writeLock} so it can never miss records the consumer
     * holds mid-flight: the consumer polls and appends under the same lock.
     */
    public void flushForTest() {
        synchronized (writeLock) {
            drainOnceLocked();
        }
    }

    /** Test hook: runs one retention pass now, bypassing the interval gate. */
    public int runRetentionForTest() {
        synchronized (writeLock) {
            return runRetention();
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Consumer                                                           */
    /* ------------------------------------------------------------------ */

    private void consumeLoop() {
        List<AuditRecord> batch = new ArrayList<>(64);
        while (running || !queue.isEmpty()) {
            try {
                synchronized (writeLock) {
                    AuditRecord first = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (first != null) {
                        batch.add(first);
                        queue.drainTo(batch);
                    }
                    if (!batch.isEmpty()) {
                        appendBatchLocked(batch);
                        batch.clear();
                    }
                    maybeRunRetentionLocked();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Audit consumer write failed", e);
                batch.clear();
            }
        }
    }

    private void drainOnceLocked() {
        List<AuditRecord> batch = new ArrayList<>(64);
        queue.drainTo(batch);
        if (!batch.isEmpty()) {
            try {
                appendBatchLocked(batch);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Audit final drain failed", e);
            }
        }
    }

    private void appendBatchLocked(List<AuditRecord> records) {
        List<Map<String, Object>> rows = new ArrayList<>(records.size());
        for (AuditRecord record : records) {
            rows.add(record.toRow());
        }
        backend.appendBatch(rows);
        recordedCount.addAndGet(records.size());
    }

    private void maybeRunRetentionLocked() {
        if (retentionDays <= 0) {
            return;
        }
        long now = clock.millis();
        if (now - lastRetentionMs < retentionIntervalMs) {
            return;
        }
        lastRetentionMs = now;
        try {
            runRetention();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Audit retention failed", e);
        }
    }

    /**
     * Archives rows older than the retention window to a gzip cold-archive
     * file and deletes them from the table.
     *
     * @return the number of rows archived
     */
    int runRetention() {
        if (retentionDays <= 0) {
            return 0;
        }
        LocalDateTime cutoff = LocalDateTime.now(clock).minusDays(retentionDays);
        List<Map<String, Object>> all = backend.readAll();
        List<AuditRecord> expired = new ArrayList<>();
        for (Map<String, Object> row : all) {
            try {
                AuditRecord record = AuditRecord.fromRow(row);
                if (record.getEventTime().isBefore(cutoff)) {
                    expired.add(record);
                }
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Audit row unreadable, skipping: {0}", row);
            }
        }
        if (expired.isEmpty()) {
            return 0;
        }
        writeArchive(expired);
        List<String> ids = new ArrayList<>(expired.size());
        for (AuditRecord record : expired) {
            ids.add(record.getId().toString());
        }
        backend.deleteRows(ids);
        LOGGER.log(Level.INFO, "Audit retention archived {0} row(s) older than {1} day(s)",
                new Object[]{expired.size(), retentionDays});
        return expired.size();
    }

    private void writeArchive(List<AuditRecord> records) {
        try {
            Files.createDirectories(archiveDir);
            String timestamp = LocalDateTime.now(clock).format(ARCHIVE_TS);
            Path target = archiveDir.resolve(ARCHIVE_FILE_PREFIX + timestamp + ARCHIVE_FILE_SUFFIX);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(target));
                 GZIPOutputStream gz = new GZIPOutputStream(out)) {
                for (AuditRecord record : records) {
                    gz.write(encodeArchiveLine(record));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write audit archive: " + e.getMessage(), e);
        }
    }

    /**
     * One pipe-delimited line per record; pipes and newlines inside the SQL
     * are escaped so the archive stays line-oriented (same sanitization rule
     * as the AVRO AuditEntry format).
     */
    static byte[] encodeArchiveLine(AuditRecord record) {
        String line = record.getEventTime() + "|"
                + sanitize(record.getUserName()) + "|"
                + sanitize(record.getSourceIp()) + "|"
                + sanitize(record.getQuery()) + "|"
                + record.getStatus() + "|"
                + record.getDurationMs() + "\n";
        return line.getBytes(StandardCharsets.UTF_8);
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("|", "\\p").replace("\n", "\\n");
    }

    /* ------------------------------------------------------------------ */
    /*  Config: sysprop - then config.properties - then default           */
    /* ------------------------------------------------------------------ */

    static String getString(String key, String defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null) {
            return sys;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop.trim();
    }

    static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(raw);
    }

    static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Invalid {0} = \"{1}\", using default {2}",
                    new Object[]{key, raw, defaultValue});
            return defaultValue;
        }
    }

    static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Invalid {0} = \"{1}\", using default {2}",
                    new Object[]{key, raw, defaultValue});
            return defaultValue;
        }
    }

    private static Properties cachedProps;

    private static synchronized Properties rootProps() {
        if (cachedProps == null) {
            Properties props = new Properties();
            try {
                Path config = Path.of("config.properties");
                if (Files.isReadable(config)) {
                    try (var in = Files.newBufferedReader(config, StandardCharsets.UTF_8)) {
                        props.load(in);
                    }
                }
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "config.properties unreadable, using defaults", e);
            }
            cachedProps = props;
        }
        return cachedProps;
    }

    /** Test hook: clears the cached config.properties snapshot. */
    static void resetConfigCacheForTest() {
        cachedProps = null;
    }
}