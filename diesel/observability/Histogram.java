package diesel.observability;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * A lock-free bucketed latency histogram (prompt4 #44).
 *
 * <p>Values are recorded in <b>seconds</b> (Prometheus convention for
 * {@code *_duration_seconds} metrics). The bucket layout is exponential: 8
 * sub-buckets per decade from 1µs to ~100s (~64 boundaries), so every
 * percentile is resolved to within ~33% relative error, and the same bucket
 * counts double as native Prometheus histogram buckets for the /metrics
 * exporter (prompt4 #45) without a re-bucketing step.
 *
 * <p>Recording is lock-free ({@link LongAdder} per bucket plus min/max CAS),
 * which keeps the query/WAL hot-path overhead in the tens-of-nanoseconds
 * range — the prompt allows either HDRHistogram or a simple synchronized
 * implementation; this hand-rolled bucketed variant avoids a new dependency
 * (project style: minimal deps) while remaining safe under 10+ concurrent
 * recorder threads.
 *
 * <p>Percentiles ({@link #percentile}) are computed on demand by walking the
 * bucket counts and linearly interpolating inside the resolved bucket.
 */
public final class Histogram {

    /** First bucket boundary: 1 microsecond. */
    static final double BASE = 1e-6;
    /** Sub-buckets per decade → ~33% relative bucket width. */
    static final double GROWTH = Math.pow(10.0, 1.0 / 8.0);
    /** Last finite boundary: ~100 seconds; anything above is the overflow bucket. */
    static final double MAX_BOUNDARY = 100.0;

    /** Static boundary table shared by all histograms (immutable). */
    private static final double[] BOUNDARIES = buildBoundaries();

    /** Prometheus-style base name (e.g. {@code diesel_query_duration_seconds}). */
    private final String name;
    /** Label pairs rendered as {@code k=v,k=v}; empty when unlabeled. */
    private final String labels;
    /** Per-bucket counts; last slot is the overflow (>= last boundary) bucket. */
    private final LongAdder[] buckets;
    private final LongAdder count = new LongAdder();
    private final DoubleAdder sum = new DoubleAdder();
    /** Minimum recorded value in double bits (POSITIVE_INFINITY when empty). */
    private final AtomicLong minBits =
            new AtomicLong(Double.doubleToLongBits(Double.POSITIVE_INFINITY));
    /** Maximum recorded value in double bits (NEGATIVE_INFINITY when empty). */
    private final AtomicLong maxBits =
            new AtomicLong(Double.doubleToLongBits(Double.NEGATIVE_INFINITY));

    /**
     * Creates a histogram with the default 1µs..100s bucket layout.
     *
     * @param name   the metric base name
     * @param labels the pre-rendered label string, or empty for no labels
     */
    public Histogram(String name, String labels) {
        this.name = name;
        this.labels = labels == null ? "" : labels;
        this.buckets = new LongAdder[BOUNDARIES.length + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    private static double[] buildBoundaries() {
        int count = (int) Math.ceil(Math.log(MAX_BOUNDARY / BASE) / Math.log(GROWTH)) + 1;
        double[] boundaries = new double[count];
        double value = BASE;
        for (int i = 0; i < count; i++) {
            boundaries[i] = value;
            value *= GROWTH;
        }
        return boundaries;
    }

    /**
     * Records a latency sample.
     *
     * @param seconds the sample value in seconds; negatives are clamped to zero
     */
    public void record(double seconds) {
        double value = seconds < 0.0 ? 0.0 : seconds;
        buckets[bucketIndex(value)].increment();
        count.increment();
        sum.add(value);
        minBits.accumulateAndGet(Double.doubleToLongBits(value), Histogram::minBits);
        maxBits.accumulateAndGet(Double.doubleToLongBits(value), Histogram::maxBits);
    }

    /**
     * Records a latency sample given in nanoseconds.
     *
     * @param nanos the sample value in nanoseconds; negatives are clamped to zero
     */
    public void recordNanos(long nanos) {
        record(nanos / 1_000_000_000.0);
    }

    /** Returns the bucket index for a non-negative value (binary search). */
    private static int bucketIndex(double value) {
        int i = Arrays.binarySearch(BOUNDARIES, value);
        int index = i >= 0 ? i : -(i + 1);
        return Math.min(index, BOUNDARIES.length);
    }

    private static long minBits(long a, long b) {
        return Double.compare(Double.longBitsToDouble(a), Double.longBitsToDouble(b)) <= 0 ? a : b;
    }

    private static long maxBits(long a, long b) {
        return Double.compare(Double.longBitsToDouble(a), Double.longBitsToDouble(b)) >= 0 ? a : b;
    }

    /**
     * Computes a percentile from the bucket counts with linear interpolation
     * inside the resolved bucket.
     *
     * @param quantile the quantile in (0, 1], e.g. 0.99 for p99
     * @return the estimated value in seconds, or 0 when no samples were recorded
     */
    public double percentile(double quantile) {
        if (quantile <= 0.0 || quantile > 1.0) {
            throw new IllegalArgumentException("quantile must be in (0, 1]: " + quantile);
        }
        long total = count.sum();
        if (total == 0) {
            return 0.0;
        }
        long rank = (long) Math.ceil(quantile * total);
        long cumulative = 0;
        for (int i = 0; i < buckets.length; i++) {
            long bucketCount = buckets[i].sum();
            if (bucketCount == 0) {
                continue;
            }
            cumulative += bucketCount;
            if (cumulative >= rank) {
                double lower = i == 0 ? 0.0 : BOUNDARIES[i - 1];
                double upper = i < BOUNDARIES.length ? BOUNDARIES[i] : BOUNDARIES[BOUNDARIES.length - 1];
                double within = (rank - (cumulative - bucketCount)) / (double) bucketCount;
                return lower + within * (upper - lower);
            }
        }
        // Unreachable: ranks are bounded by total; keep a safe fallback.
        return max();
    }

    /** Returns the p50 estimate in seconds. */
    public double p50() {
        return percentile(0.50);
    }

    /** Returns the p95 estimate in seconds. */
    public double p95() {
        return percentile(0.95);
    }

    /** Returns the p99 estimate in seconds. */
    public double p99() {
        return percentile(0.99);
    }

    /**
     * Returns the number of recorded samples.
     *
     * @return the total sample count
     */
    public long count() {
        return count.sum();
    }

    /**
     * Returns the arithmetic mean of all samples.
     *
     * @return the mean in seconds, or 0 when no samples were recorded
     */
    public double mean() {
        long total = count.sum();
        return total == 0 ? 0.0 : sum.sum() / total;
    }

    /**
     * Returns the smallest recorded sample.
     *
     * @return the minimum in seconds, or 0 when no samples were recorded
     */
    public double min() {
        double value = Double.longBitsToDouble(minBits.get());
        return Double.isFinite(value) ? value : 0.0;
    }

    /**
     * Returns the largest recorded sample.
     *
     * @return the maximum in seconds, or 0 when no samples were recorded
     */
    public double max() {
        double value = Double.longBitsToDouble(maxBits.get());
        return Double.isFinite(value) ? value : 0.0;
    }

    /**
     * Returns the static bucket boundary table (upper bounds, ascending).
     *
     * @return a copy of the boundary table in seconds
     */
    public static double[] boundaries() {
        return Arrays.copyOf(BOUNDARIES, BOUNDARIES.length);
    }

    /**
     * Returns the per-bucket counts aligned with {@link #boundaries()}.
     * The last slot is the overflow bucket (values above the last boundary).
     *
     * @return a copy of the bucket counts
     */
    public long[] bucketCounts() {
        long[] counts = new long[buckets.length];
        for (int i = 0; i < buckets.length; i++) {
            counts[i] = buckets[i].sum();
        }
        return counts;
    }

    /**
     * Returns the metric base name.
     *
     * @return the base name without labels
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the rendered label string.
     *
     * @return labels as {@code k=v,k=v}, or empty when unlabeled
     */
    public String getLabels() {
        return labels;
    }

    /**
     * Returns the full metric key (name plus labels).
     *
     * @return e.g. {@code diesel_query_duration_seconds{type=SELECT}}, or just
     *         the name when unlabeled
     */
    public String key() {
        return labels.isEmpty() ? name : name + "{" + labels + "}";
    }

    @Override
    public String toString() {
        return key() + "{count=" + count() + ", p99=" + p99() + "}";
    }
}
