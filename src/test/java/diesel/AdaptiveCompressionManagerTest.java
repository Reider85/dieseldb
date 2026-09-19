package diesel;

import diesel.storage.avro.AdaptiveCompressionManager;
import diesel.storage.avro.AdaptiveCompressionManager.CodecMetrics;
import diesel.storage.avro.AdaptiveCompressionManager.MetricsSnapshot;
import diesel.storage.avro.AvroCompressionConfig;
import diesel.storage.avro.AvroCodecFactory;
import diesel.storage.avro.BZip2Codec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 67 AdaptiveCompressionManager tests: metrics tracking,
 * sliding window, recommendation engine, pattern analysis,
 * config resolution, and thread safety.
 */
@Tag("storage")
@StorageType("avro")
class AdaptiveCompressionManagerTest {

    private static final String[] PROP_KEYS = {
            AdaptiveCompressionManager.ENABLED_KEY,
            AdaptiveCompressionManager.WINDOW_SIZE_KEY,
            AdaptiveCompressionManager.RATIO_THRESHOLD_KEY,
            AdaptiveCompressionManager.MIN_SAMPLES_KEY
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @TempDir
    Path tempDir;

    @BeforeEach
    void saveConfig() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
        AdaptiveCompressionManager.resetMetrics();
    }

    @AfterEach
    void restoreConfig() {
        for (String key : PROP_KEYS) {
            String prev = prevProps.get(key);
            if (prev != null) {
                System.setProperty(key, prev);
            } else {
                System.clearProperty(key);
            }
        }
        AdaptiveCompressionManager.resetMetrics();
    }

    private static List<Map<String, Object>> textRows(int n) {
        List<Map<String, Object>> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", (long) i);
            row.put("NAME", "User" + i);
            row.put("AGE", (int) (i % 100));
            row.put("ACTIVE", i % 2 == 0);
            rows.add(row);
        }
        return rows;
    }

    // ─── Metrics recording ──────────────────────────────────────────

    @Test
    void recordAndRetrieveMetrics() {
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, 5000);
        CodecMetrics m = AdaptiveCompressionManager.getCodecMetrics("zstd");
        assertNotNull(m);
        assertEquals("zstd", m.codec());
        assertEquals(400, m.compressedSize());
        assertEquals(1000, m.uncompressedSize());
        assertEquals(1, m.operationCount());
        assertEquals(0.4, m.averageRatio(), 0.001);
    }

    @Test
    void compressionRatioTracking() {
        AdaptiveCompressionManager.recordCompression("deflate", 2000, 800, 10000);
        AdaptiveCompressionManager.recordCompression("deflate", 2000, 600, 12000);
        CodecMetrics m = AdaptiveCompressionManager.getCodecMetrics("deflate");
        assertNotNull(m);
        assertEquals(1400, m.compressedSize());
        assertEquals(4000, m.uncompressedSize());
        assertEquals(0.35, m.averageRatio(), 0.001);
        assertEquals(2, m.operationCount());
    }

    @Test
    void decompressionTimeTracking() {
        AdaptiveCompressionManager.recordCompression("snappy", 500, 350, 2000);
        AdaptiveCompressionManager.recordDecompression("snappy", 800, 500);
        AdaptiveCompressionManager.recordDecompression("snappy", 900, 500);
        CodecMetrics m = AdaptiveCompressionManager.getCodecMetrics("snappy");
        assertNotNull(m);
        assertEquals(1700, m.decompressionTimeNs());
    }

    @Test
    void multipleCodecMetrics() {
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 300, 5000);
        AdaptiveCompressionManager.recordCompression("deflate", 1000, 500, 3000);
        AdaptiveCompressionManager.recordCompression("snappy", 1000, 450, 1000);
        assertNotNull(AdaptiveCompressionManager.getCodecMetrics("zstd"));
        assertNotNull(AdaptiveCompressionManager.getCodecMetrics("deflate"));
        assertNotNull(AdaptiveCompressionManager.getCodecMetrics("snappy"));
        assertEquals(3, AdaptiveCompressionManager.getMetricsSnapshot().metrics().size());
    }

    // ─── Sliding window ─────────────────────────────────────────────

    @Test
    void slidingWindowEviction() {
        AdaptiveCompressionManager.resetMetrics();
        int windowSize = AdaptiveCompressionManager.DEFAULT_WINDOW_SIZE;
        for (int i = 0; i < windowSize + 10; i++) {
            AdaptiveCompressionManager.recordCompression("zstd", 100, 50, 1000);
        }
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertEquals(windowSize + 10, snap.totalOperations());
    }

    @Test
    void resetMetricsClearsAll() {
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, 5000);
        AdaptiveCompressionManager.recordDecompression("zstd", 2000, 1000);
        AdaptiveCompressionManager.resetMetrics();
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertEquals(0, snap.totalOperations());
        assertTrue(snap.metrics().isEmpty());
        assertNull(snap.recommendedCodec());
    }

    // ─── Recommendation engine ──────────────────────────────────────

    @Test
    void recommendCodecWithSufficientData() {
        System.setProperty(AdaptiveCompressionManager.ENABLED_KEY, "true");
        // zstd gets better ratio than deflate
        for (int i = 0; i < 15; i++) {
            AdaptiveCompressionManager.recordCompression("zstd", 1000, 200, 5000);
            AdaptiveCompressionManager.recordCompression("deflate", 1000, 600, 3000);
        }
        String rec = AdaptiveCompressionManager.recommendCodec(5000);
        assertEquals("zstd", rec);
    }

    @Test
    void recommendCodecBelowMinSamples() {
        System.setProperty(AdaptiveCompressionManager.ENABLED_KEY, "true");
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 200, 5000);
        // Only 1 op < minSamples (10), so falls back to BZip2 heuristics
        String rec = AdaptiveCompressionManager.recommendCodec(5000);
        assertNotNull(rec);
    }

    @Test
    void recommendCodecAdaptiveDisabled() {
        // enabled is false by default, so always falls back (never null)
        String rec = AdaptiveCompressionManager.recommendCodec(5000);
        assertNotNull(rec);
        assertFalse(rec.isEmpty());
    }

    @Test
    void recommendationWithMixedRatios() {
        System.setProperty(AdaptiveCompressionManager.ENABLED_KEY, "true");
        // snappy: 40% ratio (best), deflate: 60%, zstd: 45%
        for (int i = 0; i < 12; i++) {
            AdaptiveCompressionManager.recordCompression("snappy", 1000, 400, 1000);
            AdaptiveCompressionManager.recordCompression("deflate", 1000, 600, 3000);
            AdaptiveCompressionManager.recordCompression("zstd", 1000, 450, 4000);
        }
        String rec = AdaptiveCompressionManager.recommendCodec(5000);
        assertEquals("snappy", rec);
        assertEquals("snappy", AdaptiveCompressionManager.lastRecommendation());
    }

    // ─── Data pattern analysis ──────────────────────────────────────

    @Test
    void analyzeDataPatternText() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("NAME", "SomeLongTextValue" + i);
            row.put("DESC", "Description text here");
            rows.add(row);
        }
        String pattern = AdaptiveCompressionManager.analyzeDataPattern(rows);
        assertEquals("text", pattern);
    }

    @Test
    void analyzeDataPatternNumeric() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ID", (long) i);
            row.put("AGE", (int) (i % 100));
            row.put("SCORE", (double) i * 1.5);
            rows.add(row);
        }
        String pattern = AdaptiveCompressionManager.analyzeDataPattern(rows);
        assertEquals("numeric", pattern);
    }

    @Test
    void analyzeDataPatternRepetitive() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("STATUS", "ACTIVE");
            row.put("TYPE", "STANDARD");
            rows.add(row);
        }
        String pattern = AdaptiveCompressionManager.analyzeDataPattern(rows);
        assertEquals("repetitive", pattern);
    }

    @Test
    void analyzeDataPatternMixed() {
        // Balanced: 2 text + 2 numeric columns per row -> neither dominates
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("NAME", "User" + i);
            row.put("CITY", "City" + (i % 5));
            row.put("ID", (long) i);
            row.put("AGE", (int) (i % 100));
            rows.add(row);
        }
        String pattern = AdaptiveCompressionManager.analyzeDataPattern(rows);
        assertEquals("mixed", pattern);
    }

    @Test
    void analyzeDataPatternEmptyReturnsMixed() {
        assertEquals("mixed", AdaptiveCompressionManager.analyzeDataPattern(List.of()));
        assertEquals("mixed", AdaptiveCompressionManager.analyzeDataPattern(null));
    }

    // ─── Config resolution ──────────────────────────────────────────

    @Test
    void configResolutionSysprop() {
        System.setProperty(AdaptiveCompressionManager.ENABLED_KEY, "true");
        System.setProperty(AdaptiveCompressionManager.WINDOW_SIZE_KEY, "50");
        System.setProperty(AdaptiveCompressionManager.RATIO_THRESHOLD_KEY, "0.15");
        System.setProperty(AdaptiveCompressionManager.MIN_SAMPLES_KEY, "5");
        assertTrue(AdaptiveCompressionManager.isEnabled());
        assertEquals(50, AdaptiveCompressionManager.windowSize());
        assertEquals(0.15, AdaptiveCompressionManager.ratioThreshold(), 0.001);
        assertEquals(5, AdaptiveCompressionManager.minSamples());
    }

    @Test
    void configResolutionDefault() {
        String summary = AdaptiveCompressionManager.configSummary();
        assertFalse(AdaptiveCompressionManager.isEnabled());
        assertEquals(AdaptiveCompressionManager.DEFAULT_WINDOW_SIZE, AdaptiveCompressionManager.windowSize());
        assertEquals(AdaptiveCompressionManager.DEFAULT_RATIO_THRESHOLD,
                AdaptiveCompressionManager.ratioThreshold(), 0.001);
        assertEquals(AdaptiveCompressionManager.DEFAULT_MIN_SAMPLES, AdaptiveCompressionManager.minSamples());
        assertTrue(summary.contains("AdaptiveCompressionConfig"));
    }

    // ─── Thread safety ──────────────────────────────────────────────

    @Test
    void threadSafetyConcurrentUpdates() throws InterruptedException {
        int threads = 8;
        int opsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerThread; i++) {
                        String codec = threadId % 2 == 0 ? "zstd" : "deflate";
                        AdaptiveCompressionManager.recordCompression(codec, 1000, 400 + threadId, 5000);
                        AdaptiveCompressionManager.recordDecompression(codec, 2000, 1000);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertEquals(threads * opsPerThread, snap.totalOperations());
        assertNotNull(snap.metrics().get("zstd"));
        assertNotNull(snap.metrics().get("deflate"));
    }

    // ─── Snapshot and metrics ───────────────────────────────────────

    @Test
    void metricsSnapshotTimestamp() {
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, 5000);
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertTrue(snap.timestamp() > 0);
        assertNotNull(snap.metrics());
        assertEquals(1, snap.metrics().size());
    }

    @Test
    void codecMetricsZeroInitial() {
        CodecMetrics m = new CodecMetrics("test");
        assertEquals(0, m.compressedSize());
        assertEquals(0, m.uncompressedSize());
        assertEquals(0, m.compressionTimeNs());
        assertEquals(0, m.decompressionTimeNs());
        assertEquals(0, m.operationCount());
        assertEquals(0.0, m.averageRatio(), 0.0);
        assertEquals(0.0, m.averageCompressionSpeed(), 0.0);
        assertEquals(0.0, m.averageDecompressionSpeed(), 0.0);
    }

    @Test
    void codecMetricsReset() {
        CodecMetrics m = new CodecMetrics("test");
        m.record(1000, 400, 5000);
        m.recordDecompression(2000);
        assertEquals(1, m.operationCount());
        m.reset();
        assertEquals(0, m.operationCount());
        assertEquals(0, m.compressedSize());
    }

    @Test
    void configSummaryOutput() {
        String summary = AdaptiveCompressionManager.configSummary();
        assertNotNull(summary);
        assertFalse(summary.isEmpty());
        assertTrue(summary.contains("AdaptiveCompressionConfig"));
    }

    @Test
    void invalidCompressionRecordIgnored() {
        AdaptiveCompressionManager.recordCompression(null, 1000, 400, 5000);
        AdaptiveCompressionManager.recordCompression("zstd", -1, 400, 5000);
        AdaptiveCompressionManager.recordCompression("zstd", 1000, -1, 5000);
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, -1);
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertEquals(0, snap.totalOperations());
    }

    @Test
    void invalidDecompressionRecordIgnored() {
        AdaptiveCompressionManager.recordDecompression(null, 2000, 1000);
        AdaptiveCompressionManager.recordDecompression("zstd", -1, 1000);
        AdaptiveCompressionManager.recordDecompression("zstd", 2000, -1);
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertEquals(0, snap.totalOperations());
    }

    @Test
    void snapshotHasSufficientData() {
        AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, 5000);
        MetricsSnapshot snap = AdaptiveCompressionManager.getMetricsSnapshot();
        assertFalse(snap.hasSufficientData(AdaptiveCompressionManager.DEFAULT_MIN_SAMPLES));
        for (int i = 1; i < AdaptiveCompressionManager.DEFAULT_MIN_SAMPLES; i++) {
            AdaptiveCompressionManager.recordCompression("zstd", 1000, 400, 5000);
        }
        MetricsSnapshot snap2 = AdaptiveCompressionManager.getMetricsSnapshot();
        assertTrue(snap2.hasSufficientData(AdaptiveCompressionManager.DEFAULT_MIN_SAMPLES));
    }
}