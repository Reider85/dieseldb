package diesel.observability;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;

/**
 * JVM-wide registry of runtime metrics (prompt4 #44, R3-012 step 1/3).
 *
 * <p>Unifies the scattered ad-hoc counters that used to live in
 * {@code QueryProfiler}, {@code BufferPool}, {@code WALWriter} etc. into a
 * single lock-free registry that step 2 ({@code PrometheusExporter}) and
 * step 3 ({@code HealthCheck}) will expose over HTTP.
 *
 * <p>All lookups are lock-free ({@link ConcurrentHashMap#computeIfAbsent});
 * metric objects are created once per unique name+labels and reused for every
 * subsequent increment, so hot-path call sites cost one hash lookup plus a
 * {@code LongAdder} increment.
 *
 * <p><b>Gauge semantics.</b> Gauges are supplier-backed and registered with
 * replace semantics: the latest registration wins. Every {@code Database}
 * instance registers {@code diesel_transactions_active} for its own
 * transaction map, so a freshly created database supersedes a closed one
 * instead of the registry leaking closed instances (tests create hundreds of
 * databases per JVM). The consequence is inherent to JVM-wide metrics with
 * multiple database instances: gauges report the most recently created
 * instance. Counters and histograms are cumulative and unaffected.
 */
public final class MetricsRegistry {

    // ─── Standard metric names (prompt4 #44) ────────────────────────
    /** Query counter, labels: {@code type=SELECT|INSERT|..., status=ok|error}. */
    public static final String QUERIES_TOTAL = "diesel_queries_total";
    /** Query latency histogram in seconds, label: {@code type=...}. */
    public static final String QUERY_DURATION_SECONDS = "diesel_query_duration_seconds";
    /** Currently open transactions gauge. */
    public static final String TRANSACTIONS_ACTIVE = "diesel_transactions_active";
    /** Currently connected clients gauge. */
    public static final String CONNECTIONS_ACTIVE = "diesel_connections_active";
    /** Accepted connections counter. */
    public static final String CONNECTIONS_ACCEPTED = "diesel_connections_accepted";
    /** Rejected connections counter (worker pool full). */
    public static final String CONNECTIONS_REJECTED = "diesel_connections_rejected";
    /** Buffer-pool hit ratio gauge in [0, 1]. */
    public static final String BUFFER_POOL_HIT_RATIO = "diesel_buffer_pool_hit_ratio";
    /** WAL lag gauge: milliseconds since the last successful append. */
    public static final String WAL_LAG_MS = "diesel_wal_lag_ms";
    /** WAL append latency histogram in seconds. */
    public static final String WAL_APPEND_DURATION_SECONDS = "diesel_wal_append_duration_seconds";
    /** Deadlock victim counter. */
    public static final String DEADLOCKS_TOTAL = "diesel_deadlocks_total";
    /** Lock timeout counter. */
    public static final String LOCK_TIMEOUTS_TOTAL = "diesel_lock_timeouts_total";

    private static final MetricsRegistry INSTANCE = new MetricsRegistry();

    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge> gauges = new ConcurrentHashMap<>();

    /**
     * Returns the JVM-wide registry instance.
     *
     * @return the singleton registry
     */
    public static MetricsRegistry getInstance() {
        return INSTANCE;
    }

    // ─── Lookup / registration ──────────────────────────────────────

    /**
     * Returns (creating on first use) a labeled counter.
     *
     * @param name       the metric base name
     * @param labelPairs alternating label keys and values, e.g.
     *                   {@code "type", "SELECT", "status", "ok"}
     * @return the counter for the name+labels combination
     */
    public Counter counter(String name, String... labelPairs) {
        String labels = renderLabels(labelPairs);
        return counters.computeIfAbsent(key(name, labels), k -> new Counter(name, labels));
    }

    /**
     * Returns (creating on first use) a labeled histogram.
     *
     * @param name       the metric base name
     * @param labelPairs alternating label keys and values
     * @return the histogram for the name+labels combination
     */
    public Histogram histogram(String name, String... labelPairs) {
        String labels = renderLabels(labelPairs);
        return histograms.computeIfAbsent(key(name, labels), k -> new Histogram(name, labels));
    }

    /**
     * Registers a supplier-backed gauge (replace semantics: the latest
     * registration wins, so a fresh {@code Database} supersedes a closed one).
     *
     * @param name     the metric base name (unlabeled)
     * @param supplier computes the current value on every read
     * @return the registered gauge
     */
    public Gauge registerGauge(String name, DoubleSupplier supplier) {
        Gauge gauge = new Gauge(name, supplier);
        gauges.put(name, gauge);
        return gauge;
    }

    /**
     * Looks up a previously registered gauge.
     *
     * @param name the metric base name
     * @return the gauge, or null when none is registered
     */
    public Gauge getGauge(String name) {
        return gauges.get(name);
    }

    /**
     * Returns every registered counter (for the /metrics exporter).
     *
     * @return an immutable snapshot view of the counters
     */
    public Collection<Counter> counters() {
        return Collections.unmodifiableCollection(counters.values());
    }

    /**
     * Returns every registered histogram (for the /metrics exporter).
     *
     * @return an immutable snapshot view of the histograms
     */
    public Collection<Histogram> histograms() {
        return Collections.unmodifiableCollection(histograms.values());
    }

    /**
     * Returns every registered gauge (for the /metrics exporter).
     *
     * @return an immutable snapshot view of the gauges
     */
    public Collection<Gauge> gauges() {
        return Collections.unmodifiableCollection(gauges.values());
    }

    // ─── Helpers ────────────────────────────────────────────────────

    /** Renders {@code k,v,k,v,...} into {@code k=v,k=v}; null-safe for values. */
    static String renderLabels(String... pairs) {
        if (pairs == null || pairs.length == 0) {
            return "";
        }
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "label pairs must be alternating keys and values, got " + pairs.length + " entries");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pairs.length; i += 2) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(pairs[i]).append('=').append(pairs[i + 1]);
        }
        return sb.toString();
    }

    private static String key(String name, String labels) {
        return labels.isEmpty() ? name : name + "{" + labels + "}";
    }
}
