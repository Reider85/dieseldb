package diesel.storage.avro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import diesel.ConfigKeys;

/**
 * Structured audit logger for AVRO storage operations (Prompt 95).
 *
 * <p>Provides four capabilities on top of the standard SLF4J logging:
 * (1) <b>structured logging</b> — every operation is written as a pipe-delimited
 * {@link AuditEntry} line to an append-only audit file, with a bounded in-memory
 * window of recent entries for querying; (2) <b>audit log for compliance</b> —
 * immutable entries carrying timestamp, severity {@link AuditLevel}, operation
 * {@link AuditCategory}, operation/table names and a detail payload, so who-did-what
 * can be reconstructed; (3) <b>performance tracing</b> — {@code logRead}/{@code logWrite}/
 * {@code logTrace} accept a duration in nanoseconds and automatically escalate entries
 * slower than {@code avro.audit.tracing.slow.threshold.ms} to WARN while counting them
 * as slow operations; (4) <b>log rotation and archiving</b> — when the active file
 * reaches {@code avro.audit.log.max.size.mb} it is renamed to a timestamped archived
 * name and a fresh file is opened; {@link #pruneOldLogs()} deletes archived files older
 * than {@code avro.audit.log.retention.days}.
 *
 * <p>The logger is a {@link #getInstance() singleton}. Recording is lock-free for the
 * in-memory side ({@link AtomicLong} + {@link ConcurrentLinkedDeque}); the file writer
 * is guarded by an internal monitor so concurrent callers never corrupt the audit file.
 * When {@code avro.audit.enabled=false} every recording method is a no-op and no file is
 * created.
 *
 * <p>Configuration is resolved per call via the standard chain
 * {@code sysprop → config.properties → code defaults}.
 *
 * @since Prompt 95
 */
public class AvroAuditLogger implements AutoCloseable {

    // ─── Config keys ──────────────────────────────────────────────────
    public static final String CONFIG_FILE_KEY = "avro.audit.config.file";
    public static final String ENABLED_KEY = "avro.audit.enabled";
    public static final String LOG_DIR_KEY = "avro.audit.log.dir";
    public static final String LOG_FILE_PREFIX_KEY = "avro.audit.log.file.prefix";
    public static final String LOG_FILE_SUFFIX_KEY = "avro.audit.log.file.suffix";
    public static final String LOG_MAX_SIZE_MB_KEY = "avro.audit.log.max.size.mb";
    public static final String LOG_RETENTION_DAYS_KEY = "avro.audit.log.retention.days";
    public static final String ROTATION_ENABLED_KEY = "avro.audit.log.rotation.enabled";
    public static final String BUFFER_SIZE_KEY = "avro.audit.log.buffer.size";
    public static final String FLUSH_ON_EVERY_WRITE_KEY = "avro.audit.log.flush.on.every.write";
    public static final String TRACING_ENABLED_KEY = "avro.audit.tracing.enabled";
    public static final String SLOW_THRESHOLD_MS_KEY = "avro.audit.tracing.slow.threshold.ms";

    // ─── Defaults ─────────────────────────────────────────────────────
    public static final boolean DEFAULT_ENABLED = true;
    public static final String DEFAULT_LOG_DIR = "data/avro-audit";
    public static final String DEFAULT_LOG_FILE_PREFIX = "audit";
    public static final String DEFAULT_LOG_FILE_SUFFIX = ".log";
    public static final double DEFAULT_LOG_MAX_SIZE_MB = 50;
    public static final int DEFAULT_LOG_RETENTION_DAYS = 30;
    public static final boolean DEFAULT_ROTATION_ENABLED = true;
    public static final int DEFAULT_BUFFER_SIZE = 1024;
    public static final boolean DEFAULT_FLUSH_ON_EVERY_WRITE = false;
    public static final boolean DEFAULT_TRACING_ENABLED = true;
    public static final long DEFAULT_SLOW_THRESHOLD_MS = 1000;
    public static final int MAX_RECENT_ENTRIES = 1000;
    public static final long MILLIS_PER_DAY = 86_400_000L;

    private static final org.slf4j.Logger LOGGER =
            org.slf4j.LoggerFactory.getLogger(AvroAuditLogger.class);

    private static final AvroAuditLogger INSTANCE = new AvroAuditLogger();

    // ─── Counters ─────────────────────────────────────────────────────
    private final AtomicLong totalEntries = new AtomicLong();
    private final AtomicLong totalWarnings = new AtomicLong();
    private final AtomicLong totalErrors = new AtomicLong();
    private final AtomicLong totalSlowOps = new AtomicLong();

    private final long startTimeMs = System.currentTimeMillis();

    // ─── Recent in-memory window ──────────────────────────────────────
    private final ConcurrentLinkedDeque<AuditEntry> recentEntries = new ConcurrentLinkedDeque<>();

    // ─── Config state (volatile) ──────────────────────────────────────
    private volatile boolean enabled;
    private volatile boolean rotationEnabled;
    private volatile boolean flushOnEveryWrite;
    private volatile boolean tracingEnabled;
    private volatile String logDir;
    private volatile String logFilePrefix;
    private volatile String logFileSuffix;
    private volatile double maxSizeMb;
    private volatile long maxSizeBytes;
    private volatile int retentionDays;
    private volatile int bufferSize;
    private volatile long slowThresholdMs;

    // ─── Writer state (guarded by the record monitor) ─────────────────
    private BufferedWriter writer;
    private volatile File currentLogFile;
    private volatile long currentFileBytes;

    private AvroAuditLogger() {
        resolveConfig();
    }

    public static AvroAuditLogger getInstance() {
        return INSTANCE;
    }

    /**
     * Whether structured audit recording is currently enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * The currently active audit file, or {@code null} when nothing has been written yet.
     */
    public File getCurrentLogFile() {
        return currentLogFile;
    }

    // ─── Config resolution ────────────────────────────────────────────

    /**
     * Re-resolves all config keys from {@code sysprop → config.properties → defaults}.
     * If the logger is disabled by the new configuration the open writer is closed so
     * no further file activity happens until the next {@code resolveConfig} or write.
     */
    public void resolveConfig() {
        boolean wasEnabled = enabled;
        enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        logDir = getString(LOG_DIR_KEY, DEFAULT_LOG_DIR);
        logFilePrefix = getString(LOG_FILE_PREFIX_KEY, DEFAULT_LOG_FILE_PREFIX);
        logFileSuffix = getString(LOG_FILE_SUFFIX_KEY, DEFAULT_LOG_FILE_SUFFIX);
        maxSizeMb = getDouble(LOG_MAX_SIZE_MB_KEY, DEFAULT_LOG_MAX_SIZE_MB);
        maxSizeBytes = Math.max(1L, (long) (maxSizeMb * 1024 * 1024));
        retentionDays = getInt(LOG_RETENTION_DAYS_KEY, DEFAULT_LOG_RETENTION_DAYS);
        rotationEnabled = getBoolean(ROTATION_ENABLED_KEY, DEFAULT_ROTATION_ENABLED);
        bufferSize = Math.max(1, getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE));
        flushOnEveryWrite = getBoolean(FLUSH_ON_EVERY_WRITE_KEY, DEFAULT_FLUSH_ON_EVERY_WRITE);
        tracingEnabled = getBoolean(TRACING_ENABLED_KEY, DEFAULT_TRACING_ENABLED);
        slowThresholdMs = getLong(SLOW_THRESHOLD_MS_KEY, DEFAULT_SLOW_THRESHOLD_MS);
        if (wasEnabled && !enabled) {
            closeWriter();
        }
    }

    // ─── Recording API ────────────────────────────────────────────────

    /**
     * Records an arbitrary audit entry with the given level, category, operation and detail.
     */
    public void log(AuditLevel level, AuditCategory category, String operation, String table, String detail) {
        record(new AuditEntry(now(), level, category, operation, table, detail, 0), false);
    }

    /**
     * Structured read log with performance tracing. Entries whose duration exceeds the
     * configured slow threshold are escalated to {@link AuditLevel#WARN} and counted as slow.
     *
     * @param table      logical table name (may be null)
     * @param operation  operation name, e.g. "scan", "pointlookup"
     * @param detail     human-readable detail payload (may be null)
     * @param durationNs elapsed wall-clock time in nanoseconds
     */
    public void logRead(String table, String operation, String detail, long durationNs) {
        boolean slow = tracingEnabled && durationNs > slowThresholdMs * 1_000_000L;
        AuditLevel level = slow ? AuditLevel.WARN : AuditLevel.INFO;
        record(new AuditEntry(now(), level, AuditCategory.READ, operation, table, detail,
                durationNs / 1_000_000), slow);
    }

    /**
     * Structured write log with performance tracing. Same slow-operation escalation as
     * {@link #logRead}.
     */
    public void logWrite(String table, String operation, String detail, long durationNs) {
        boolean slow = tracingEnabled && durationNs > slowThresholdMs * 1_000_000L;
        AuditLevel level = slow ? AuditLevel.WARN : AuditLevel.INFO;
        record(new AuditEntry(now(), level, AuditCategory.WRITE, operation, table, detail,
                durationNs / 1_000_000), slow);
    }

    /**
     * Generic performance trace for any timed operation.
     */
    public void logTrace(String operation, String table, String detail, long durationNs) {
        boolean slow = tracingEnabled && durationNs > slowThresholdMs * 1_000_000L;
        AuditLevel level = slow ? AuditLevel.WARN : AuditLevel.TRACE;
        record(new AuditEntry(now(), level, AuditCategory.PERFORMANCE, operation, table, detail,
                durationNs / 1_000_000), slow);
    }

    /**
     * Structured error log. The throwable (if any) is appended to the detail as a brief cause.
     */
    public void logError(String table, String operation, String detail, Throwable error) {
        String message = detail == null ? "" : detail;
        if (error != null) {
            message = message.isEmpty() ? error.getClass().getSimpleName()
                    : message + " cause=" + error.getClass().getSimpleName() + ": " + msg(error);
        }
        record(new AuditEntry(now(), AuditLevel.ERROR, AuditCategory.ERROR, operation, table,
                message, 0), false);
    }

    /**
     * Structured transaction/audit event for compliance (BEGIN/COMMIT/ROLLBACK and friends).
     */
    public void logTransaction(String table, String operation, String txId, String detail) {
        String d = (detail == null ? "" : detail + " ") + "tx=" + txId;
        record(new AuditEntry(now(), AuditLevel.INFO, AuditCategory.TRANSACTION, operation, table,
                d.trim(), 0), false);
    }

    /**
     * Configuration-change audit trail (compliance requirement).
     */
    public void logConfig(String key, String oldValue, String newValue) {
        record(new AuditEntry(now(), AuditLevel.INFO, AuditCategory.CONFIG, "config_change", null,
                key + "=" + oldValue + " -> " + newValue, 0), false);
    }

    /**
     * Convenience for backup/restore lifecycle events.
     */
    public void logBackup(String table, String operation, String detail) {
        record(new AuditEntry(now(), AuditLevel.INFO, AuditCategory.BACKUP, operation, table,
                detail, 0), false);
    }

    // ─── Reading / snapshotting ───────────────────────────────────────

    /**
     * Returns up to {@code count} most-recent entries (newest first), unmodifiable.
     */
    public List<AuditEntry> getRecentEntries(int count) {
        if (count <= 0) {
            return List.of();
        }
        ArrayList<AuditEntry> list = new ArrayList<>(recentEntries);
        Collections.reverse(list);
        if (list.size() > count) {
            return Collections.unmodifiableList(list.subList(0, count));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * Immutable summary of the audit logger state.
     */
    public AuditSnapshot getSnapshot() {
        return new AuditSnapshot(
                System.currentTimeMillis(),
                totalEntries.get(),
                totalWarnings.get(),
                totalErrors.get(),
                totalSlowOps.get(),
                recentEntries.size(),
                System.currentTimeMillis() - startTimeMs,
                logDir,
                logFilePrefix + logFileSuffix);
    }

    /**
     * Returns a JSON array string of all recent in-memory entries (newest last, file order).
     */
    public String exportJson() {
        StringBuilder sb = new StringBuilder(256);
        sb.append('[');
        int i = 0;
        for (AuditEntry e : recentEntries) {
            if (i++ > 0) {
                sb.append(',');
            }
            sb.append("{\"timestamp\":\"").append(Instant.ofEpochMilli(e.timestampMs())).append('"');
            sb.append(",\"level\":\"").append(e.level()).append('"');
            sb.append(",\"category\":\"").append(e.category()).append('"');
            sb.append(",\"operation\":\"").append(jsonEscape(e.operation())).append('"');
            sb.append(",\"table\":\"").append(jsonEscape(e.table())).append('"');
            sb.append(",\"detail\":\"").append(jsonEscape(e.detail())).append('"');
            sb.append(",\"durationMs\":").append(e.durationMs());
            sb.append('}');
        }
        return sb.append(']').toString();
    }

    /**
     * Clears all counters and the in-memory recent-entry window. Does not touch the file.
     */
    public void resetAudit() {
        totalEntries.set(0);
        totalWarnings.set(0);
        totalErrors.set(0);
        totalSlowOps.set(0);
        recentEntries.clear();
    }

    // ─── File lifecycle: rotation, retention, IO ──────────────────────

    /**
     * Rotates the current audit file immediately (archive + reopen). Returns the number of
     * files archived (0 when nothing was open or rotation is disabled). No-op in disabled mode.
     */
    public synchronized int rotateIfNeeded() {
        if (!enabled || !rotationEnabled) {
            return 0;
        }
        try {
            return rotateFile();
        } catch (IOException e) {
            LOGGER.warn("Avro audit: rotation failed: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Deletes archived audit files older than the configured retention window. The actively
     * written file is never pruned. Returns the number of files deleted.
     */
    public synchronized int pruneOldLogs() {
        if (!enabled) {
            return 0;
        }
        long cutoff = System.currentTimeMillis() - (long) retentionDays * MILLIS_PER_DAY;
        File dir = new File(logDir);
        File[] files = dir.listFiles((d, n) -> n.startsWith(logFilePrefix) && n.endsWith(logFileSuffix));
        if (files == null) {
            return 0;
        }
        int pruned = 0;
        for (File f : files) {
            if (f.equals(currentLogFile)) {
                continue;
            }
            if (f.lastModified() < cutoff) {
                if (f.delete()) {
                    pruned++;
                    LOGGER.info("Avro audit: pruned old log {}", f.getName());
                }
            }
        }
        return pruned;
    }

    /**
     * Flushes buffered entries to disk. No-op when no file is open.
     */
    public synchronized void flush() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                LOGGER.warn("Avro audit: flush failed: {}", e.getMessage());
            }
        }
    }

    /**
     * Flushes and closes the current audit file. The next recorded entry reopens the file in
     * append mode. Idempotent.
     */
    @Override
    public synchronized void close() {
        closeWriter();
    }

    // ─── Recording internals ──────────────────────────────────────────

    private synchronized void record(AuditEntry e, boolean slow) {
        if (!enabled) {
            return;
        }
        totalEntries.incrementAndGet();
        switch (e.level()) {
            case WARN:
                totalWarnings.incrementAndGet();
                break;
            case ERROR:
                totalErrors.incrementAndGet();
                break;
            default:
                break;
        }
        if (slow) {
            totalSlowOps.incrementAndGet();
        }
        recentEntries.addLast(e);
        while (recentEntries.size() > MAX_RECENT_ENTRIES) {
            recentEntries.removeFirst();
        }
        writeEntry(e);
        if (slow) {
            LOGGER.warn("[AVRO-AUDIT] slow {} on {} took {}ms",
                    e.operation(), safeTable(e.table()), e.durationMs());
        }
    }

    private void writeEntry(AuditEntry e) {
        try {
            ensureWriter();
            if (writer == null) {
                return;
            }
            if (rotationEnabled && currentLogFile != null && currentFileBytes >= maxSizeBytes) {
                rotateFile();
            }
            String line = formatLine(e);
            writer.write(line);
            writer.newLine();
            currentFileBytes += line.length() + 1;
            if (flushOnEveryWrite) {
                writer.flush();
            }
        } catch (IOException ex) {
            LOGGER.warn("Avro audit: failed to write audit entry: {}", ex.getMessage());
        }
    }

    private void ensureWriter() throws IOException {
        if (writer != null) {
            return;
        }
        File dir = new File(logDir);
        if (!dir.exists() && !dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("Cannot create audit log dir: " + dir);
        }
        File f = new File(dir, logFilePrefix + logFileSuffix);
        writer = new BufferedWriter(new java.io.FileWriter(f, true), bufferSize);
        currentLogFile = f;
        currentFileBytes = f.length();
        LOGGER.debug("Avro audit: opened audit log {}", f.getName());
    }

    private int rotateFile() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
            writer = null;
        }
        File toArchive = currentLogFile;
        currentLogFile = null;
        currentFileBytes = 0;
        if (toArchive != null && toArchive.exists() && toArchive.length() > 0) {
            File archived = new File(toArchive.getParentFile(),
                    logFilePrefix + "-" + stamp() + logFileSuffix);
            Files.move(toArchive.toPath(), archived.toPath(), StandardCopyOption.REPLACE_EXISTING);
            ensureWriter();
            LOGGER.info("Avro audit: rotated {} to {}", toArchive.getName(), archived.getName());
            return 1;
        }
        ensureWriter();
        return 0;
    }

    private void closeWriter() {
        BufferedWriter w = writer;
        writer = null;
        if (w != null) {
            try {
                w.flush();
                w.close();
            } catch (IOException e) {
                LOGGER.warn("Avro audit: failed to close writer: {}", e.getMessage());
            }
        }
        currentLogFile = null;
        currentFileBytes = 0;
    }

    // ─── Formatting ───────────────────────────────────────────────────

    private static String formatLine(AuditEntry e) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(Instant.ofEpochMilli(e.timestampMs()));
        sb.append('|').append(e.level());
        sb.append('|').append(e.category());
        sb.append('|').append(sanitize(e.operation()));
        sb.append('|').append(sanitize(e.table()));
        sb.append('|').append(e.durationMs());
        sb.append('|').append(sanitize(e.detail()));
        return sb.toString();
    }

    private static String sanitize(String s) {
        if (s == null) {
            return "";
        }
        return s.replace('|', ';').replace('\n', ' ').replace('\r', ' ');
    }

    private static String jsonEscape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String stamp() {
        return new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + "-" + System.nanoTime();
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    private static String msg(Throwable t) {
        return t.getMessage() == null ? "" : t.getMessage();
    }

    private static String safeTable(String table) {
        return table == null ? "<unknown>" : table;
    }

    // ─── Config helpers ───────────────────────────────────────────────

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (InputStream in = Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
            }
        }
        return props;
    }

    private static String getString(String key, String defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null) {
            return sys;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop.trim();
    }

    private static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED_EQUALS, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED_EQUALS, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static double getDouble(String key, double defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Double.parseDouble(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED_EQUALS, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return "true".equalsIgnoreCase(raw.trim()) || "on".equalsIgnoreCase(raw.trim())
                || "yes".equalsIgnoreCase(raw.trim()) || "1".equals(raw.trim());
    }

    // ─── Inner types ──────────────────────────────────────────────────

    /**
     * Severity of an audit entry.
     *
     * @since Prompt 95
     */
    public enum AuditLevel { TRACE, DEBUG, INFO, WARN, ERROR }

    /**
     * Operational category of an audit entry.
     *
     * @since Prompt 95
     */
    public enum AuditCategory {
        READ, WRITE, QUERY, INDEX, COMPRESSION, TRANSACTION, BACKUP, RECOVERY,
        SCHEMA, PARTITION, BLOOM, SYNC_MARKER, POOL, BATCH, BUFFER, METRICS,
        INTEGRITY, PERFORMANCE, CONFIG, ERROR, OTHER
    }

    /**
     * A single immutable audit event.
     *
     * @since Prompt 95
     */
    public record AuditEntry(
            long timestampMs,
            AuditLevel level,
            AuditCategory category,
            String operation,
            String table,
            String detail,
            long durationMs
    ) {
    }

    /**
     * Immutable summary of all audit counters and configuration at a point in time.
     *
     * @since Prompt 95
     */
    public record AuditSnapshot(
            long timestampMs,
            long totalEntries,
            long totalWarnings,
            long totalErrors,
            long totalSlowOps,
            int bufferedEntries,
            long uptimeMs,
            String logDir,
            String logFile
    ) {
    }
}