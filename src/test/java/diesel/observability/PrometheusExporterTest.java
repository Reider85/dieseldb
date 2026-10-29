package diesel.observability;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #45: PrometheusExporter — text exposition format (# HELP / # TYPE,
 * cumulative histogram buckets, label escaping).
 * <p>
 * Uses metric names unique to this class: the registry is JVM-wide and the
 * fast profile runs test classes in parallel, so a shared name would let
 * another class' series contaminate exact-output assertions.
 */
@Tag("smoke")
public class PrometheusExporterTest {

    private static final String NAME = "diesel_test_exporter";

    // ─── Counters ───────────────────────────────────────────────────

    @Test
    void counterFamilyEmitsHelpAndTypeOnceAndAllSeries() {
        String name = NAME + "_counter_family";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        registry.counter(name, "type", "SELECT", "status", "ok").add(3);
        registry.counter(name, "type", "INSERT", "status", "ok").add(2);
        registry.counter(name, "type", "SELECT", "status", "error").increment();

        String out = PrometheusExporter.render(registry);

        // # HELP / # TYPE once per family, not per series (spec requirement).
        assertEquals(1, occurrences(out, "# HELP " + name + " "),
                "HELP must appear exactly once per family:\n" + out);
        assertEquals(1, occurrences(out, "# TYPE " + name + " counter"),
                "TYPE must appear exactly once per family:\n" + out);
        assertTrue(out.contains(name + "{type=\"SELECT\",status=\"ok\"} 3"), out);
        assertTrue(out.contains(name + "{type=\"INSERT\",status=\"ok\"} 2"), out);
        assertTrue(out.contains(name + "{type=\"SELECT\",status=\"error\"} 1"), out);
    }

    @Test
    void unlabeledCounterRendersWithoutBraces() {
        String name = NAME + "_unlabeled";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        registry.counter(name).add(7);

        String out = PrometheusExporter.render(registry);

        assertTrue(out.contains(name + " 7\n"), out);
        assertFalse(out.contains(name + "{"), "unlabeled series must have no label block: " + out);
    }

    // ─── Histograms ─────────────────────────────────────────────────

    @Test
    void histogramBucketsAreCumulativeAndCloseWithInf() {
        String name = NAME + "_hist_buckets";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        Histogram histogram = registry.histogram(name, "type", "TEST");
        histogram.record(0.001);
        histogram.record(0.002);
        histogram.record(0.5);

        String out = PrometheusExporter.render(registry);

        assertEquals(1, occurrences(out, "# TYPE " + name + " histogram"), out);

        List<double[]> buckets = parseBuckets(out, name + "_bucket{type=\"TEST\"");
        assertFalse(buckets.isEmpty(), "no bucket series found:\n" + out);

        // Cumulative: counts must be non-decreasing along ascending le.
        long previous = -1;
        for (double[] bucket : buckets) {
            assertTrue(bucket[1] >= previous,
                    "bucket counts must be cumulative (le=" + bucket[0] + " count="
                            + (long) bucket[1] + " < previous " + previous + ")");
            previous = (long) bucket[1];
        }

        // Overflow bucket closes the family at the total sample count.
        double[] last = buckets.get(buckets.size() - 1);
        assertEquals(Double.POSITIVE_INFINITY, last[0], 1e-12, "last bucket must be +Inf");
        assertEquals(3, (long) last[1], "+Inf bucket must hold every observation");

        assertTrue(out.contains(name + "_count{type=\"TEST\"} 3"), out);
        // 0.001 + 0.002 + 0.5 in double arithmetic — compare with a tolerance
        // instead of a literal string, the exact printout may carry rounding.
        assertEquals(0.503, parseSample(out, name + "_sum{type=\"TEST\"}"), 1e-9);
    }

    @Test
    void histogramWithNoLabelsGetsLeOnly() {
        String name = NAME + "_hist_unlabeled";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        Histogram histogram = registry.histogram(name);
        histogram.record(0.01);

        String out = PrometheusExporter.render(registry);

        assertTrue(out.contains(name + "_bucket{le=\""), "le label must exist:\n" + out);
        assertTrue(out.contains(name + "_sum 0.01"), out);
        assertTrue(out.contains(name + "_count 1"), out);
    }

    @Test
    void histogramBoundaryFormattingIsLowercaseExponent() {
        // Spec-conformant float grammar: 1.0e-6 (Go-style), not Java's 1.0E-6.
        assertEquals("1.0e-6", PrometheusExporter.formatBoundary(1e-6));
        assertEquals("100.0", PrometheusExporter.formatBoundary(100.0));
        assertEquals("+Inf", PrometheusExporter.formatBoundary(Double.POSITIVE_INFINITY));
        assertEquals("-Inf", PrometheusExporter.formatBoundary(Double.NEGATIVE_INFINITY));
    }

    // ─── Gauges ─────────────────────────────────────────────────────

    @Test
    void gaugeRendersCurrentSupplierValue() {
        String name = NAME + "_gauge";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        java.util.concurrent.atomic.AtomicInteger value =
                new java.util.concurrent.atomic.AtomicInteger(5);
        registry.registerGauge(name, value::get);

        String out = PrometheusExporter.render(registry);
        assertTrue(out.contains("# TYPE " + name + " gauge"), out);
        assertTrue(out.contains(name + " 5\n"), out);

        // Supplier-backed: the next scrape sees the new value without re-registration.
        value.set(42);
        out = PrometheusExporter.render(registry);
        assertTrue(out.contains(name + " 42\n"), out);
    }

    // ─── HELP / escaping ────────────────────────────────────────────

    @Test
    void standardMetricsCarryHumanReadableHelp() {
        // Assert the curated HELP table directly — the JVM-wide registry may
        // be empty when this class runs in isolation.
        assertEquals("Total queries executed, by type and status.",
                PrometheusExporter.help(MetricsRegistry.QUERIES_TOTAL));
        assertEquals("Buffer pool hit ratio in [0, 1].",
                PrometheusExporter.help(MetricsRegistry.BUFFER_POOL_HIT_RATIO));
        assertEquals("Milliseconds since the last successful WAL append.",
                PrometheusExporter.help(MetricsRegistry.WAL_LAG_MS));
        // Unknown metric names fall back to a generic-but-present HELP line.
        String custom = NAME + "_help_fallback";
        MetricsRegistry.getInstance().counter(custom).increment();
        String out = PrometheusExporter.render();
        assertTrue(out.contains("# HELP " + custom + " DieselDB metric " + custom + "."),
                "fallback HELP missing:\n" + out);
    }

    @Test
    void labelValuesAreQuotedAndEscaped() {
        String name = NAME + "_escape";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        registry.counter(name, "path", "a\"b\\c\nd").increment();

        String out = PrometheusExporter.render(registry);

        assertTrue(out.contains(name + "{path=\"a\\\"b\\\\c\\nd\"} 1"),
                "label value must be quoted and escaped:\n" + out);
    }

    // ─── Output hygiene ─────────────────────────────────────────────

    @Test
    void everyLineIsEitherHelpTypeOrSample() {
        String out = PrometheusExporter.render();
        assertTrue(out.endsWith("\n"), "exposition must end with a newline");
        for (String line : out.split("\n", -1)) {
            if (line.isEmpty()) {
                continue;
            }
            assertTrue(line.startsWith("# HELP ") || line.startsWith("# TYPE ")
                            || line.startsWith("#") || line.matches("[a-zA-Z_:][a-zA-Z0-9_:]*.*"),
                    "invalid exposition line: " + line);
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static int occurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    /**
     * Parses {@code name_bucket{...le="X"} N} lines for one series, returning
     * [le, count] pairs in file order.
     */
    private static List<double[]> parseBuckets(String out, String seriesPrefix) {
        List<double[]> result = new ArrayList<>();
        Pattern pattern = Pattern.compile(
                Pattern.quote(seriesPrefix) + ",le=\"([^\"]+)\"} (\\d+)");
        Matcher matcher = pattern.matcher(out);
        while (matcher.find()) {
            String le = matcher.group(1);
            double bound = "+Inf".equals(le) ? Double.POSITIVE_INFINITY
                    : "-Inf".equals(le) ? Double.NEGATIVE_INFINITY
                    : Double.parseDouble(le);
            result.add(new double[]{bound, Long.parseLong(matcher.group(2))});
        }
        return result;
    }

    /** Parses the numeric sample of an unlabeled-in-suffix series line, e.g. {@code name_sum{type="TEST"} 0.503}. */
    private static double parseSample(String out, String seriesKey) {
        for (String line : out.split("\n")) {
            if (line.startsWith(seriesKey + " ")) {
                return Double.parseDouble(line.substring(seriesKey.length() + 1));
            }
        }
        throw new AssertionError("series not found: " + seriesKey + "\n" + out);
    }
}
