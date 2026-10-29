package diesel.observability;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #44: Histogram — percentile correctness on 10k samples (acceptance
 * criterion), bucketed and lock-free.
 */
@Tag("smoke")
public class HistogramTest {

    @Test
    void percentilesAreCorrectOnTenThousandUniformSamples() {
        // Acceptance criterion: p99 корректно считается на 10k samples.
        // Linear distribution 0..10s → p50=5.0s, p95=9.5s, p99=9.9s.
        Histogram histogram = new Histogram("diesel_test_hist_uniform", "");
        int samples = 10_000;
        for (int i = 0; i < samples; i++) {
            histogram.record(i / (double) samples * 10.0);
        }

        assertEquals(samples, histogram.count());
        assertRelativeError(5.0, histogram.p50(), 0.05, "p50");
        assertRelativeError(9.5, histogram.p95(), 0.05, "p95");
        assertRelativeError(9.9, histogram.p99(), 0.05, "p99");
        assertRelativeError(5.0, histogram.mean(), 0.05, "mean");
        assertTrue(histogram.max() <= 10.0, "max must not overshoot the recorded range");
        assertTrue(histogram.min() < 0.001, "min must approximate the smallest sample");
    }

    @Test
    void percentilesAreCorrectOnTenThousandLogNormalishSamples() {
        // Skewed latency distribution (most samples fast, a slow tail) — the
        // realistic shape for query latency. p99 must land in the tail, not
        // near the median.
        Histogram histogram = new Histogram("diesel_test_hist_skew", "");
        Random random = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            double latency = Math.abs(random.nextGaussian()) * 0.01 + 0.001; // ~1-80ms
            histogram.record(latency);
        }
        assertTrue(histogram.p99() > histogram.p50(),
                "p99 must exceed p50 on a skewed distribution (" + histogram.p99() + " vs " + histogram.p50() + ")");
        assertTrue(histogram.p50() < 0.03, "p50 of a ~N(1ms,10ms) distribution must stay small");
        assertTrue(histogram.p99() >= histogram.p95(), "p99 >= p95 ordering must hold");
    }

    @Test
    void singleBucketClusterStaysWithinBucketBounds() {
        Histogram histogram = new Histogram("diesel_test_hist_cluster", "");
        for (int i = 0; i < 1_000; i++) {
            histogram.record(0.005); // 5ms
        }
        // 5ms sits in the bucket [~4.38ms, ~5.85ms]; with a uniform cluster
        // the interpolated percentile must stay inside a ±25% band.
        assertRelativeError(0.005, histogram.p99(), 0.25, "p99");
        assertEquals(0.005, histogram.min(), 1e-12, "min is exact (not bucketed)");
        assertEquals(0.005, histogram.max(), 1e-12, "max is exact (not bucketed)");
        assertEquals(0.005, histogram.mean(), 1e-12, "mean is exact (not bucketed)");
    }

    @Test
    void emptyHistogramReportsZeroes() {
        Histogram histogram = new Histogram("diesel_test_hist_empty", "");
        assertEquals(0, histogram.count());
        assertEquals(0.0, histogram.p50(), 0.0);
        assertEquals(0.0, histogram.p95(), 0.0);
        assertEquals(0.0, histogram.p99(), 0.0);
        assertEquals(0.0, histogram.mean(), 0.0);
        assertEquals(0.0, histogram.min(), 0.0);
        assertEquals(0.0, histogram.max(), 0.0);
    }

    @Test
    void negativeSamplesAreClampedToZero() {
        Histogram histogram = new Histogram("diesel_test_hist_negative", "");
        histogram.record(-1.5);
        assertEquals(1, histogram.count());
        assertEquals(0.0, histogram.min(), 0.0, "negatives clamp to zero");
        assertEquals(0.0, histogram.max(), 0.0);
    }

    @Test
    void nanosConvenienceRecordsSeconds() {
        Histogram histogram = new Histogram("diesel_test_hist_nanos", "");
        histogram.recordNanos(5_000_000L); // 5ms
        assertEquals(0.005, histogram.max(), 1e-12);
    }

    @Test
    void quantileValidation() {
        Histogram histogram = new Histogram("diesel_test_hist_quantile", "");
        assertThrows(IllegalArgumentException.class, () -> histogram.percentile(0.0));
        assertThrows(IllegalArgumentException.class, () -> histogram.percentile(1.5));
        assertThrows(IllegalArgumentException.class, () -> histogram.percentile(-0.1));
    }

    @Test
    void bucketCountsAlwaysSumToTotalCount() {
        Histogram histogram = new Histogram("diesel_test_hist_buckets", "");
        Random random = new Random(7);
        int samples = 500;
        for (int i = 0; i < samples; i++) {
            histogram.record(random.nextDouble() * 200.0); // spans the full layout incl. overflow
        }
        long[] counts = histogram.bucketCounts();
        long sum = 0;
        for (long c : counts) {
            sum += c;
        }
        assertEquals(samples, sum, "bucket counts must partition all samples");
        assertEquals(Histogram.boundaries().length + 1, counts.length,
                "one count slot per boundary plus the overflow bucket");
    }

    @Test
    void overflowValuesLandInLastBucket() {
        Histogram histogram = new Histogram("diesel_test_hist_overflow", "");
        histogram.record(10_000.0); // far above the ~100s last boundary
        long[] counts = histogram.bucketCounts();
        assertEquals(1, counts[counts.length - 1], "overflow sample must land in the last bucket");
        assertRelativeError(10_000.0, histogram.max(), 1e-12, "max stays exact for overflow");
    }

    @Test
    void labelsAndKeyFormatting() {
        Histogram histogram = new Histogram("diesel_test_hist_key", "type=SELECT");
        assertEquals("type=SELECT", histogram.getLabels());
        assertEquals("diesel_test_hist_key{type=SELECT}", histogram.key());
        Histogram unlabeled = new Histogram("diesel_test_hist_key2", null);
        assertEquals("", unlabeled.getLabels());
        assertEquals("diesel_test_hist_key2", unlabeled.key());
    }

    private static void assertRelativeError(double expected, double actual, double tolerance, String label) {
        double error = Math.abs(actual - expected) / Math.abs(expected);
        assertTrue(error <= tolerance,
                label + " expected " + expected + " got " + actual + " (relative error " + error + ")");
    }
}
