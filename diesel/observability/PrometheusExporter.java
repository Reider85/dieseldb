package diesel.observability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Renders the {@link MetricsRegistry} in the Prometheus text exposition
 * format (prompt4 #45, R3-012 step 2/3).
 *
 * <p>Output shape (one {@code # HELP} + {@code # TYPE} pair per metric
 * <i>family</i>, then one sample line per label set):
 *
 * <pre>
 * # HELP diesel_queries_total Total queries executed, by type and status.
 * # TYPE diesel_queries_total counter
 * diesel_queries_total{type="SELECT",status="ok"} 42
 * # HELP diesel_query_duration_seconds Query execution latency in seconds, by type.
 * # TYPE diesel_query_duration_seconds histogram
 * diesel_query_duration_seconds_bucket{type="SELECT",le="1.0e-06"} 0
 * ...
 * diesel_query_duration_seconds_bucket{type="SELECT",le="+Inf"} 42
 * diesel_query_duration_seconds_sum{type="SELECT"} 0.5
 * diesel_query_duration_seconds_count{type="SELECT"} 42
 * </pre>
 *
 * <p>Histogram buckets are emitted <b>cumulative</b> (Prometheus requirement:
 * every {@code le} bucket counts all observations &lt;= its boundary) with a
 * final {@code le="+Inf"} overflow bucket. The per-bucket counts come from
 * {@link Histogram#bucketCounts()} — the same numbers the registry already
 * maintains, so exporting never re-buckets or copies the samples.
 *
 * <p>Family iteration order is deterministic (sorted by name, label sets by
 * rendered labels) so scrapes are byte-stable and tests can assert on exact
 * output even with the JVM-wide registry being mutated concurrently by other
 * threads (a scrape only ever sees a consistent point-in-time sum per series).
 */
public final class PrometheusExporter {

    /**
     * Content type for the Prometheus text exposition format, version 0.0.4
     * (the version of the spec this exporter conforms to).
     */
    public static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    /** HELP text for the standard diesel_* metric families (prompt4 #44). */
    private static final Map<String, String> HELP;
    static {
        Map<String, String> help = new HashMap<>();
        help.put(MetricsRegistry.QUERIES_TOTAL,
                "Total queries executed, by type and status.");
        help.put(MetricsRegistry.QUERY_DURATION_SECONDS,
                "Query execution latency in seconds, by type.");
        help.put(MetricsRegistry.TRANSACTIONS_ACTIVE,
                "Number of currently open transactions.");
        help.put(MetricsRegistry.CONNECTIONS_ACTIVE,
                "Number of currently connected clients.");
        help.put(MetricsRegistry.CONNECTIONS_ACCEPTED,
                "Total accepted client connections.");
        help.put(MetricsRegistry.CONNECTIONS_REJECTED,
                "Total rejected client connections (worker pool full).");
        help.put(MetricsRegistry.BUFFER_POOL_HIT_RATIO,
                "Buffer pool hit ratio in [0, 1].");
        help.put(MetricsRegistry.WAL_LAG_MS,
                "Milliseconds since the last successful WAL append.");
        help.put(MetricsRegistry.WAL_APPEND_DURATION_SECONDS,
                "WAL append latency in seconds.");
        help.put(MetricsRegistry.DEADLOCKS_TOTAL,
                "Total deadlock victims.");
        help.put(MetricsRegistry.LOCK_TIMEOUTS_TOTAL,
                "Total lock wait timeouts.");
        HELP = Collections.unmodifiableMap(help);
    }

    private PrometheusExporter() {
    }

    /**
     * Renders the JVM-wide registry (convenience for the HTTP handler).
     *
     * @return the exposition text, terminated by a newline
     */
    public static String render() {
        return render(MetricsRegistry.getInstance());
    }

    /**
     * Renders the given registry in the Prometheus text exposition format.
     *
     * @param registry the registry to export
     * @return the exposition text, terminated by a newline
     */
    public static String render(MetricsRegistry registry) {
        StringBuilder sb = new StringBuilder(4096);
        renderCounters(registry, sb);
        renderHistograms(registry, sb);
        renderGauges(registry, sb);
        return sb.toString();
    }

    // ─── Families ───────────────────────────────────────────────────

    private static void renderCounters(MetricsRegistry registry, StringBuilder sb) {
        Map<String, List<Counter>> families = new TreeMap<>();
        for (Counter counter : registry.counters()) {
            families.computeIfAbsent(counter.getName(), k -> new ArrayList<>()).add(counter);
        }
        for (Map.Entry<String, List<Counter>> family : families.entrySet()) {
            String name = family.getKey();
            List<Counter> series = sortCounters(family.getValue());
            appendHelpAndType(sb, name, "counter");
            for (Counter counter : series) {
                sb.append(name).append(prometheusLabels(counter.getLabels()))
                        .append(' ').append(counter.get()).append('\n');
            }
        }
    }

    private static void renderHistograms(MetricsRegistry registry, StringBuilder sb) {
        Map<String, List<Histogram>> families = new TreeMap<>();
        for (Histogram histogram : registry.histograms()) {
            families.computeIfAbsent(histogram.getName(), k -> new ArrayList<>()).add(histogram);
        }
        double[] boundaries = Histogram.boundaries();
        for (Map.Entry<String, List<Histogram>> family : families.entrySet()) {
            String name = family.getKey();
            List<Histogram> series = sortHistograms(family.getValue());
            appendHelpAndType(sb, name, "histogram");
            for (Histogram histogram : series) {
                renderHistogramSeries(sb, name, histogram, boundaries);
            }
        }
    }

    private static void renderHistogramSeries(StringBuilder sb, String name,
                                              Histogram histogram, double[] boundaries) {
        String labels = histogram.getLabels();
        long[] counts = histogram.bucketCounts();
        long cumulative = 0;
        for (int i = 0; i < boundaries.length; i++) {
            cumulative += counts[i];
            sb.append(name).append("_bucket").append(prometheusLabels(labels, boundaries[i]))
                    .append(' ').append(cumulative).append('\n');
        }
        // Overflow bucket: everything above the last finite boundary.
        cumulative += counts[counts.length - 1];
        sb.append(name).append("_bucket").append(prometheusLabels(labels, Double.POSITIVE_INFINITY))
                .append(' ').append(cumulative).append('\n');
        sb.append(name).append("_sum").append(prometheusLabels(labels))
                .append(' ').append(formatValue(histogram.sum())).append('\n');
        sb.append(name).append("_count").append(prometheusLabels(labels))
                .append(' ').append(histogram.count()).append('\n');
    }

    private static void renderGauges(MetricsRegistry registry, StringBuilder sb) {
        Map<String, List<Gauge>> families = new TreeMap<>();
        for (Gauge gauge : registry.gauges()) {
            families.computeIfAbsent(gauge.getName(), k -> new ArrayList<>()).add(gauge);
        }
        for (Map.Entry<String, List<Gauge>> family : families.entrySet()) {
            String name = family.getKey();
            appendHelpAndType(sb, name, "gauge");
            // Gauges are keyed by name in the registry (replace semantics), so
            // there is exactly one series per family — sort defensively anyway.
            List<Gauge> series = new ArrayList<>(family.getValue());
            series.sort((a, b) -> a.getName().compareTo(b.getName()));
            for (Gauge gauge : series) {
                sb.append(name).append(" ").append(formatValue(gauge.getValue())).append('\n');
            }
        }
    }

    private static void appendHelpAndType(StringBuilder sb, String name, String type) {
        sb.append("# HELP ").append(name).append(' ').append(help(name)).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    /**
     * Returns the HELP text for a metric family: the curated description for
     * the standard diesel_* metrics, a generic fallback otherwise.
     * Package-private so tests can assert the table directly without having
     * to populate the JVM-wide registry with production metric names.
     *
     * @param name the metric base name
     * @return the HELP description
     */
    static String help(String name) {
        return HELP.getOrDefault(name, "DieselDB metric " + name + ".");
    }

    // ─── Deterministic ordering within a family ─────────────────────

    private static List<Counter> sortCounters(List<Counter> series) {
        List<Counter> sorted = new ArrayList<>(series);
        sorted.sort((a, b) -> a.getLabels().compareTo(b.getLabels()));
        return sorted;
    }

    private static List<Histogram> sortHistograms(List<Histogram> series) {
        List<Histogram> sorted = new ArrayList<>(series);
        sorted.sort((a, b) -> a.getLabels().compareTo(b.getLabels()));
        return sorted;
    }

    // ─── Formatting ─────────────────────────────────────────────────

    /**
     * Converts the registry's internal {@code k=v,k=v} label rendering into
     * the Prometheus {@code {k="v",k="v"}} form, escaping special characters
     * in values. Returns an empty string when unlabeled.
     *
     * @param labels the pre-rendered internal label string
     * @return the Prometheus label block, or empty when unlabeled
     */
    static String prometheusLabels(String labels) {
        return prometheusLabels(labels, null);
    }

    /**
     * Converts the internal label rendering into the Prometheus form and adds
     * the {@code le} bucket-boundary label (histograms only).
     *
     * @param labels   the pre-rendered internal label string, or empty
     * @param boundary the {@code le} boundary, or null to omit it
     * @return the Prometheus label block, or empty when unlabeled and no boundary
     */
    static String prometheusLabels(String labels, Double boundary) {
        boolean hasLabels = labels != null && !labels.isEmpty();
        if (!hasLabels && boundary == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(labels == null ? 32 : labels.length() + 32);
        sb.append('{');
        boolean first = true;
        if (hasLabels) {
            int i = 0;
            while (i < labels.length()) {
                int eq = labels.indexOf('=', i);
                int comma = labels.indexOf(',', eq + 1);
                if (comma < 0) {
                    comma = labels.length();
                }
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(labels, i, eq).append("=\"")
                        .append(escapeLabelValue(labels.substring(eq + 1, comma)))
                        .append('"');
                i = comma + 1;
            }
        }
        if (boundary != null) {
            if (!first) {
                sb.append(',');
            }
            sb.append("le=\"").append(formatBoundary(boundary)).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    /**
     * Escapes a label value per the exposition spec: backslash, double quote
     * and newline.
     */
    static String escapeLabelValue(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Formats a bucket boundary: {@code 1.0e-6} style, {@code +Inf} for infinity. */
    static String formatBoundary(double boundary) {
        if (Double.isInfinite(boundary)) {
            return boundary > 0 ? "+Inf" : "-Inf";
        }
        return Double.toString(boundary).replace('E', 'e');
    }

    /**
     * Formats a sample value. Longs print plainly, integral doubles collapse
     * to their long form, everything else keeps full {@code Double.toString}
     * precision (accepted by the exposition spec's float grammar).
     */
    static String formatValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "+Inf" : "-Inf";
        }
        if (value == Math.floor(value) && Math.abs(value) < 1e15) {
            return Long.toString((long) value);
        }
        return Double.toString(value);
    }
}
