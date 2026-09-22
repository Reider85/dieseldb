package diesel.storage.avro;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralised AVRO metrics collector with JMX exposure, Prometheus text-format
 * export, and configurable alerting on anomalies (Prompt 94).
 *
 * <p>Collects read/write throughput, compression ratio, active-table counts and
 * error-rate counters. Every mutating method is lock-free ({@link AtomicLong}
 * + {@link ConcurrentLinkedDeque}) so callers never block.
 *
 * <p>JMX: registers a read-only {@link DynamicMBean} on the platform MBean
 * server under the configurable ObjectName (default
 * {@code diesel:type=AvroMetrics}). The DynamicMBean form keeps the class
 * package-private.
 *
 * <p>Prometheus: {@link #renderPrometheus()} returns a standards-compliant
 * text-exposition payload that can be served over HTTP without any external
 * metrics library.
 *
 * <p>Alerting: a list of {@link AlertRule}s is evaluated after every
 * {@code record*} call; fires log lines at the appropriate severity and
 * appends to a bounded in-memory deque (max 1000 alerts).
 *
 * <p>Configuration is resolved per call via the standard chain
 * {@code sysprop → config.properties → code defaults}.
 *
 * @since Prompt 94
 */
public class AvroMetrics implements DynamicMBean {

    // ─── Config keys ──────────────────────────────────────────────────
    public static final String CONFIG_FILE_KEY = "avro.metrics.config.file";
    public static final String ENABLED_KEY = "avro.metrics.enabled";
    public static final String JMX_ENABLED_KEY = "avro.metrics.jmx.enabled";
    public static final String JMX_OBJECT_NAME_KEY = "avro.metrics.jmx.object.name";
    public static final String PROMETHEUS_ENABLED_KEY = "avro.metrics.prometheus.enabled";
    public static final String PROMETHEUS_PATH_KEY = "avro.metrics.prometheus.path";
    public static final String THROUGHPUT_WINDOW_MS_KEY = "avro.metrics.throughput.window.ms";
    public static final String ALERT_SLOW_READ_MS_KEY = "avro.metrics.alert.slow.read.ms";
    public static final String ALERT_SLOW_WRITE_MS_KEY = "avro.metrics.alert.slow.write.ms";
    public static final String ALERT_LOW_COMPRESSION_KEY = "avro.metrics.alert.low.compression.ratio";
    public static final String ALERT_HIGH_ERROR_RATE_KEY = "avro.metrics.alert.high.error.rate";
    public static final String ALERT_MAX_TABLES_KEY = "avro.metrics.alert.max.active.tables";

    // ─── Defaults ─────────────────────────────────────────────────────
    public static final boolean DEFAULT_ENABLED = true;
    public static final boolean DEFAULT_JMX_ENABLED = true;
    public static final String DEFAULT_JMX_OBJECT_NAME = "diesel:type=AvroMetrics";
    public static final boolean DEFAULT_PROMETHEUS_ENABLED = false;
    public static final String DEFAULT_PROMETHEUS_PATH = "/metrics";
    public static final long DEFAULT_THROUGHPUT_WINDOW_MS = 1000;
    public static final long DEFAULT_ALERT_SLOW_READ_MS = 5000;
    public static final long DEFAULT_ALERT_SLOW_WRITE_MS = 5000;
    public static final double DEFAULT_ALERT_LOW_COMPRESSION_RATIO = 0.1;
    public static final double DEFAULT_ALERT_HIGH_ERROR_RATE = 0.1;
    public static final int DEFAULT_ALERT_MAX_TABLES = 100;
    public static final int MAX_ALERT_HISTORY = 1000;

    private static final org.slf4j.Logger LOGGER =
            org.slf4j.LoggerFactory.getLogger(AvroMetrics.class);

    private static final AvroMetrics INSTANCE = new AvroMetrics();

    // ─── Counters ─────────────────────────────────────────────────────
    private final AtomicLong totalReads = new AtomicLong();
    private final AtomicLong totalWrites = new AtomicLong();
    private final AtomicLong totalBytesRead = new AtomicLong();
    private final AtomicLong totalBytesWritten = new AtomicLong();
    private final AtomicLong totalReadNanos = new AtomicLong();
    private final AtomicLong totalWriteNanos = new AtomicLong();
    private final AtomicLong totalOriginalBytes = new AtomicLong();
    private final AtomicLong totalCompressedBytes = new AtomicLong();
    private final AtomicLong activeTables = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong totalOperations = new AtomicLong();
    private final AtomicLong alertCount = new AtomicLong();

    // ─── Volatile last-values ─────────────────────────────────────────
    private volatile long lastReadBytes;
    private volatile long lastReadNanos;
    private volatile long lastWriteBytes;
    private volatile long lastWriteNanos;
    private volatile long lastCompressionRatio100; // ratio * 100 for integer storage
    private volatile long lastAlertTimestampMs;

    private final long startTimeMs = System.currentTimeMillis();

    // ─── Alert history ────────────────────────────────────────────────
    private final ConcurrentLinkedDeque<Alert> alertHistory = new ConcurrentLinkedDeque<>();

    private volatile boolean enabled;
    private volatile boolean jmxEnabled;
    private volatile boolean prometheusEnabled;
    private volatile String jmxObjectName;
    private volatile String prometheusPath;
    private volatile long throughputWindowMs;
    private volatile long alertSlowReadMs;
    private volatile long alertSlowWriteMs;
    private volatile double alertLowCompressionRatio;
    private volatile double alertHighErrorRate;
    private volatile int alertMaxActiveTables;

    private AvroMetrics() {
        resolveConfig();
        if (jmxEnabled) {
            registerMBean();
        }
    }

    public static AvroMetrics getInstance() {
        return INSTANCE;
    }

    // ─── Config resolution ────────────────────────────────────────────

    public void resolveConfig() {
        enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        jmxEnabled = getBoolean(JMX_ENABLED_KEY, DEFAULT_JMX_ENABLED);
        prometheusEnabled = getBoolean(PROMETHEUS_ENABLED_KEY, DEFAULT_PROMETHEUS_ENABLED);
        jmxObjectName = getString(JMX_OBJECT_NAME_KEY, DEFAULT_JMX_OBJECT_NAME);
        prometheusPath = getString(PROMETHEUS_PATH_KEY, DEFAULT_PROMETHEUS_PATH);
        throughputWindowMs = getLong(THROUGHPUT_WINDOW_MS_KEY, DEFAULT_THROUGHPUT_WINDOW_MS);
        alertSlowReadMs = getLong(ALERT_SLOW_READ_MS_KEY, DEFAULT_ALERT_SLOW_READ_MS);
        alertSlowWriteMs = getLong(ALERT_SLOW_WRITE_MS_KEY, DEFAULT_ALERT_SLOW_WRITE_MS);
        alertLowCompressionRatio = getDouble(ALERT_LOW_COMPRESSION_KEY, DEFAULT_ALERT_LOW_COMPRESSION_RATIO);
        alertHighErrorRate = getDouble(ALERT_HIGH_ERROR_RATE_KEY, DEFAULT_ALERT_HIGH_ERROR_RATE);
        alertMaxActiveTables = getInt(ALERT_MAX_TABLES_KEY, DEFAULT_ALERT_MAX_TABLES);
    }

    // ─── Recording API ────────────────────────────────────────────────

    /**
     * Records a completed read operation.
     *
     * @param table logical table name (may be null)
     * @param bytes number of bytes read
     * @param nanos elapsed wall-clock time in nanoseconds
     */
    public void recordRead(String table, long bytes, long nanos) {
        if (!enabled) return;
        totalReads.incrementAndGet();
        totalBytesRead.addAndGet(bytes);
        totalReadNanos.addAndGet(nanos);
        totalOperations.incrementAndGet();
        lastReadBytes = bytes;
        lastReadNanos = nanos;

        long readMs = nanos / 1_000_000;
        if (readMs > alertSlowReadMs) {
            fireAlert(Alert.Severity.WARN, "slow_read", readMs, alertSlowReadMs,
                    "Slow read on " + safeTable(table) + ": " + readMs + "ms (threshold " + alertSlowReadMs + "ms)");
        }
        evaluateAlerts();
    }

    /**
     * Records a completed write operation.
     *
     * @param table logical table name (may be null)
     * @param bytes number of bytes written
     * @param nanos elapsed wall-clock time in nanoseconds
     */
    public void recordWrite(String table, long bytes, long nanos) {
        if (!enabled) return;
        totalWrites.incrementAndGet();
        totalBytesWritten.addAndGet(bytes);
        totalWriteNanos.addAndGet(nanos);
        totalOperations.incrementAndGet();
        lastWriteBytes = bytes;
        lastWriteNanos = nanos;

        long writeMs = nanos / 1_000_000;
        if (writeMs > alertSlowWriteMs) {
            fireAlert(Alert.Severity.WARN, "slow_write", writeMs, alertSlowWriteMs,
                    "Slow write on " + safeTable(table) + ": " + writeMs + "ms (threshold " + alertSlowWriteMs + "ms)");
        }
        evaluateAlerts();
    }

    /**
     * Records compression statistics for a single operation.
     *
     * @param table           logical table name (may be null)
     * @param originalBytes   uncompressed size
     * @param compressedBytes compressed size
     */
    public void recordCompression(String table, long originalBytes, long compressedBytes) {
        if (!enabled) return;
        totalOriginalBytes.addAndGet(originalBytes);
        totalCompressedBytes.addAndGet(compressedBytes);
        double ratio = originalBytes > 0 ? (double) compressedBytes / originalBytes : 1.0;
        lastCompressionRatio100 = Math.round(ratio * 100);
    }

    /**
     * Records an error occurrence.
     *
     * @param table     logical table name (may be null)
     * @param errorType error classification string
     */
    public void recordError(String table, String errorType) {
        if (!enabled) return;
        errorCount.incrementAndGet();
        totalOperations.incrementAndGet();
        evaluateAlerts();
    }

    public void incrementActiveTables() {
        if (!enabled) return;
        activeTables.incrementAndGet();
        if (activeTables.get() > alertMaxActiveTables) {
            fireAlert(Alert.Severity.WARN, "max_active_tables", activeTables.get(), alertMaxActiveTables,
                    "Active tables count " + activeTables.get() + " exceeds threshold " + alertMaxActiveTables);
        }
    }

    public void decrementActiveTables() {
        if (!enabled) return;
        activeTables.accumulateAndGet(1, (cur, dec) -> Math.max(0, cur - dec));
    }

    // ─── Snapshot ─────────────────────────────────────────────────────

    public MetricsSnapshot snapshot() {
        long uptimeMs = System.currentTimeMillis() - startTimeMs;
        long totalOps = totalOperations.get();
        double errorRate = totalOps > 0 ? (double) errorCount.get() / totalOps : 0.0;
        long orig = totalOriginalBytes.get();
        long comp = totalCompressedBytes.get();
        double avgCompressionRatio = orig > 0 ? (double) comp / orig : 1.0;
        double readThroughput = totalReadNanos.get() > 0
                ? (double) totalBytesRead.get() / (totalReadNanos.get() / 1_000_000_000.0)
                : 0.0;
        double writeThroughput = totalWriteNanos.get() > 0
                ? (double) totalBytesWritten.get() / (totalWriteNanos.get() / 1_000_000_000.0)
                : 0.0;

        return new MetricsSnapshot(
                Instant.now().toEpochMilli(),
                totalReads.get(),
                totalWrites.get(),
                totalBytesRead.get(),
                totalBytesWritten.get(),
                readThroughput,
                writeThroughput,
                lastCompressionRatio100 / 100.0,
                avgCompressionRatio,
                activeTables.get(),
                alertCount.get(),
                errorRate,
                uptimeMs
        );
    }

    public void resetMetrics() {
        totalReads.set(0);
        totalWrites.set(0);
        totalBytesRead.set(0);
        totalBytesWritten.set(0);
        totalReadNanos.set(0);
        totalWriteNanos.set(0);
        totalOriginalBytes.set(0);
        totalCompressedBytes.set(0);
        activeTables.set(0);
        errorCount.set(0);
        totalOperations.set(0);
        alertCount.set(0);
        lastReadBytes = 0;
        lastReadNanos = 0;
        lastWriteBytes = 0;
        lastWriteNanos = 0;
        lastCompressionRatio100 = 0;
        lastAlertTimestampMs = 0;
        alertHistory.clear();
    }

    // ─── Alerting ─────────────────────────────────────────────────────

    private void evaluateAlerts() {
        long ops = totalOperations.get();
        if (ops > 0) {
            double errorRate = (double) errorCount.get() / ops;
            if (errorRate > alertHighErrorRate) {
                long now = System.currentTimeMillis();
                if (now - lastAlertTimestampMs >= throughputWindowMs) {
                    lastAlertTimestampMs = now;
                    alertCount.incrementAndGet();
                    Alert alert = new Alert(now, Alert.Severity.CRITICAL, "high_error_rate",
                            errorCount.get(), ops,
                            "Error rate " + String.format("%.4f", errorRate) + " exceeds threshold " + alertHighErrorRate);
                    alertHistory.addLast(alert);
                    while (alertHistory.size() > MAX_ALERT_HISTORY) {
                        alertHistory.pollFirst();
                    }
                    LOGGER.error("[AVRO-METRIC] {}", alert.message());
                }
            }
        }
    }

    private void fireAlert(Alert.Severity severity, String metric, long value, long threshold, String message) {
        long now = System.currentTimeMillis();
        if (now - lastAlertTimestampMs < throughputWindowMs) {
            return; // rate-limit alerts within the window
        }
        lastAlertTimestampMs = now;
        alertCount.incrementAndGet();
        Alert alert = new Alert(now, severity, metric, value, threshold, message);
        alertHistory.addLast(alert);
        while (alertHistory.size() > MAX_ALERT_HISTORY) {
            alertHistory.pollFirst();
        }

        switch (severity) {
            case INFO -> LOGGER.info("[AVRO-METRIC] {}", message);
            case WARN -> LOGGER.warn("[AVRO-METRIC] {}", message);
            case CRITICAL -> LOGGER.error("[AVRO-METRIC] {}", message);
        }
    }

    public List<Alert> getRecentAlerts(int count) {
        List<Alert> all = new ArrayList<>(alertHistory);
        int start = Math.max(0, all.size() - count);
        return Collections.unmodifiableList(all.subList(start, all.size()));
    }

    public void clearAlerts() {
        alertHistory.clear();
        alertCount.set(0);
    }

    // ─── Prometheus text-format export ─────────────────────────────────

    public String renderPrometheus() {
        StringBuilder sb = new StringBuilder(2048);
        long uptimeMs = System.currentTimeMillis() - startTimeMs;

        appendMetric(sb, "avro_total_reads", "counter",
                "Total AVRO read operations", totalReads.get());
        appendMetric(sb, "avro_total_writes", "counter",
                "Total AVRO write operations", totalWrites.get());
        appendMetric(sb, "avro_total_bytes_read", "counter",
                "Total bytes read from AVRO files", totalBytesRead.get());
        appendMetric(sb, "avro_total_bytes_written", "counter",
                "Total bytes written to AVRO files", totalBytesWritten.get());

        long orig = totalOriginalBytes.get();
        long comp = totalCompressedBytes.get();
        double avgRatio = orig > 0 ? (double) comp / orig : 1.0;
        appendMetric(sb, "avro_compression_ratio", "gauge",
                "Last recorded compression ratio (compressed/original)", lastCompressionRatio100 / 100.0);
        appendMetric(sb, "avro_avg_compression_ratio", "gauge",
                "Average compression ratio across all operations", avgRatio);

        appendMetric(sb, "avro_active_tables", "gauge",
                "Number of active AVRO tables", activeTables.get());
        appendMetric(sb, "avro_total_errors", "counter",
                "Total AVRO error count", errorCount.get());
        appendMetric(sb, "avro_alert_count", "counter",
                "Total alerts fired", alertCount.get());
        appendMetric(sb, "avro_uptime_ms", "gauge",
                "Metrics collector uptime in milliseconds", uptimeMs);

        return sb.toString();
    }

    private static void appendMetric(StringBuilder sb, String name, String type, String help, double value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sb.append(name).append(' ').append(formatDouble(value)).append('\n');
    }

    private static void appendMetric(StringBuilder sb, String name, String type, String help, long value) {
        appendMetric(sb, name, type, help, (double) value);
    }

    private static String formatDouble(double v) {
        if (v == (long) v && !Double.isInfinite(v) && !Double.isNaN(v)) {
            return String.valueOf((long) v);
        }
        return String.format("%.6f", v);
    }

    // ─── JMX DynamicMBean ────────────────────────────────────────────

    private void registerMBean() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(this, new ObjectName(jmxObjectName));
        } catch (InstanceAlreadyExistsException ignored) {
            LOGGER.debug("AvroMetrics MBean already registered: {}", ignored.getMessage());
        } catch (Exception e) {
            LOGGER.warn("Failed to register AvroMetrics MBean: {}", e.getMessage());
        }
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case "TotalReads": return totalReads.get();
            case "TotalWrites": return totalWrites.get();
            case "TotalBytesRead": return totalBytesRead.get();
            case "TotalBytesWritten": return totalBytesWritten.get();
            case "CompressionRatio": return lastCompressionRatio100 / 100.0;
            case "ReadThroughput": {
                long rn = totalReadNanos.get();
                return rn > 0 ? (double) totalBytesRead.get() / (rn / 1_000_000_000.0) : 0.0;
            }
            case "WriteThroughput": {
                long wn = totalWriteNanos.get();
                return wn > 0 ? (double) totalBytesWritten.get() / (wn / 1_000_000_000.0) : 0.0;
            }
            case "ActiveTables": return activeTables.get();
            case "AlertCount": return alertCount.get();
            case "UptimeMs": return System.currentTimeMillis() - startTimeMs;
            case "ErrorCount": return errorCount.get();
            default: throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("AvroMetrics attributes are read-only");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList result = new AttributeList();
        for (String attr : attributes) {
            try {
                result.add(new Attribute(attr, getAttribute(attr)));
            } catch (Exception ignored) {
                LOGGER.debug("Skipping unreadable attribute: {}", attr);
            }
        }
        return result;
    }

    @Override
    @SuppressWarnings("unused")
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    @SuppressWarnings("unused")
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        throw new ReflectionException(new UnsupportedOperationException(
                "No operations on AvroMetrics; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
                "TotalReads", "TotalWrites", "TotalBytesRead", "TotalBytesWritten",
                "CompressionRatio", "ReadThroughput", "WriteThroughput",
                "ActiveTables", "AlertCount", "UptimeMs", "ErrorCount"
        };
        String[] types = {
                "long", "long", "long", "long",
                "double", "double", "double",
                "long", "long", "long", "long"
        };
        String[] descriptions = {
                "Total AVRO read operations",
                "Total AVRO write operations",
                "Total bytes read from AVRO files",
                "Total bytes written to AVRO files",
                "Last recorded compression ratio (compressed/original)",
                "Read throughput in bytes/sec",
                "Write throughput in bytes/sec",
                "Number of active AVRO tables",
                "Total alerts fired",
                "Metrics collector uptime in milliseconds",
                "Total AVRO error count"
        };
        MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attrs[i] = new MBeanAttributeInfo(names[i], types[i], descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(AvroMetrics.class.getName(),
                "DieselDB AVRO metrics collector (read/write/compression/alerts)",
                attrs, null, null, null);
    }

    // ─── Config helpers ───────────────────────────────────────────────

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (InputStream in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
            }
        }
        return props;
    }

    private static String getString(String key, String defaultValue) {
        String sys = System.getProperty(key);
        if (sys != null) return sys;
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop.trim();
    }

    private static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} = \"{}\", using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} = \"{}\", using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static double getDouble(String key, double defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Double.parseDouble(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} = \"{}\", using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return "true".equalsIgnoreCase(raw.trim()) || "on".equalsIgnoreCase(raw.trim())
                || "yes".equalsIgnoreCase(raw.trim()) || "1".equals(raw.trim());
    }

    private static String safeTable(String table) {
        return table == null ? "<unknown>" : table;
    }

    // ─── Inner types ──────────────────────────────────────────────────

    /**
     * Immutable snapshot of all AVRO metrics at a point in time.
     *
     * @since Prompt 94
     */
    public record MetricsSnapshot(
            long timestampMs,
            long totalReads,
            long totalWrites,
            long totalBytesRead,
            long totalBytesWritten,
            double readThroughputBytesPerSec,
            double writeThroughputBytesPerSec,
            double lastCompressionRatio,
            double avgCompressionRatio,
            long activeTables,
            long alertCount,
            double errorRate,
            long uptimeMs
    ) { }

    /**
     * A single alert event.
     *
     * @since Prompt 94
     */
    public record Alert(
            long timestampMs,
            Severity severity,
            String metric,
            long value,
            long threshold,
            String message
    ) {
        public enum Severity { INFO, WARN, CRITICAL }
    }
}
