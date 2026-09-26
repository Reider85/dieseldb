package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import diesel.ConfigKeys;
import diesel.storage.StorageMessageConstants;

/**
 * Adaptive AVRO compression manager — runtime monitoring, metrics collection,
 * and codec recommendation engine (Prompt 67).
 *
 * <p>Tracks per-codec compression ratios, compression/decompression times,
 * and data-pattern characteristics in a sliding window. When adaptive mode
 * is enabled and sufficient samples are collected, recommends the best codec
 * for the current data pattern and estimated payload size.
 *
 * <p>All methods are thread-safe; metrics are stored in concurrent maps
 * and the sliding window uses a lock-free queue.
 *
 * <p>Config keys (sysprop → config.properties → default, resolved at call time
 * so test overrides take effect — mirroring {@link BZip2Codec}):
 * <ul>
 *   <li>{@code avro.adaptive.enabled} — enable adaptive switching (default: false)</li>
 *   <li>{@code avro.adaptive.window.size} — sliding window capacity (default: 100)</li>
 *   <li>{@code avro.adaptive.ratio.threshold} — min ratio diff to switch codec (default: 0.20)</li>
 *   <li>{@code avro.adaptive.min.samples} — minimum ops before recommendations (default: 10)</li>
 * </ul>
 *
 * @since Prompt 67
 */
public final class AdaptiveCompressionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveCompressionManager.class);

    // ─── Config keys ───────────────────────────────────────────────────────

    /** Config key: enable adaptive compression switching. */
    public static final String ENABLED_KEY = "avro.adaptive.enabled";
    /** Config key: sliding window capacity. */
    public static final String WINDOW_SIZE_KEY = "avro.adaptive.window.size";
    /** Config key: minimum ratio difference to recommend a switch. */
    public static final String RATIO_THRESHOLD_KEY = "avro.adaptive.ratio.threshold";
    /** Config key: minimum sample count before recommendations are made. */
    public static final String MIN_SAMPLES_KEY = "avro.adaptive.min.samples";

    // ─── Defaults ──────────────────────────────────────────────────────────

    /** Default: adaptive mode disabled (opt-in). */
    public static final boolean DEFAULT_ENABLED = false;
    /** Default sliding window capacity. */
    public static final int DEFAULT_WINDOW_SIZE = 100;
    /** Default minimum ratio difference to recommend a switch (20%). */
    public static final double DEFAULT_RATIO_THRESHOLD = 0.20;
    /** Default minimum sample count before recommendations are made. */
    public static final int DEFAULT_MIN_SAMPLES = 10;

    // ─── Per-codec metrics accumulator ─────────────────────────────────────

    /** Per-codec running metrics (thread-safe). */
    public static final class CodecMetrics {
        private final String codec;
        private final AtomicLong compressedSize = new AtomicLong(0);
        private final AtomicLong uncompressedSize = new AtomicLong(0);
        private final AtomicLong compressionTimeNs = new AtomicLong(0);
        private final AtomicLong decompressionTimeNs = new AtomicLong(0);
        private final AtomicLong operationCount = new AtomicLong(0);

        /** Creates an empty metrics accumulator for the given codec. */
        public CodecMetrics(String codec) {
            this.codec = codec;
        }

        /** The codec name. */
        public String codec() {
            return codec;
        }

        /** Total compressed bytes across all recorded operations. */
        public long compressedSize() {
            return compressedSize.get();
        }

        /** Total uncompressed bytes across all recorded operations. */
        public long uncompressedSize() {
            return uncompressedSize.get();
        }

        /** Total compression time in nanoseconds. */
        public long compressionTimeNs() {
            return compressionTimeNs.get();
        }

        /** Total decompression time in nanoseconds. */
        public long decompressionTimeNs() {
            return decompressionTimeNs.get();
        }

        /** Total number of recorded operations. */
        public long operationCount() {
            return operationCount.get();
        }

        /** Average compression ratio (compressed / uncompressed); 0 if no data. */
        public double averageRatio() {
            long uncompressed = uncompressedSize.get();
            if (uncompressed == 0) {
                return 0.0;
            }
            return (double) compressedSize.get() / uncompressed;
        }

        /** Average compression speed (uncompressed bytes per nanosecond). */
        public double averageCompressionSpeed() {
            long time = compressionTimeNs.get();
            if (time == 0) {
                return 0.0;
            }
            return (double) uncompressedSize.get() / time;
        }

        /** Average decompression speed (decompressed bytes per nanosecond). */
        public double averageDecompressionSpeed() {
            long time = decompressionTimeNs.get();
            if (time == 0) {
                return 0.0;
            }
            return (double) uncompressedSize.get() / time;
        }

        /** Records a compression event. */
        public void record(long uncompressed, long compressed, long compressTimeNs) {
            uncompressedSize.addAndGet(uncompressed);
            compressedSize.addAndGet(compressed);
            compressionTimeNs.addAndGet(compressTimeNs);
            operationCount.incrementAndGet();
        }

        /** Records a decompression event. */
        public void recordDecompression(long decompressTimeNs) {
            decompressionTimeNs.addAndGet(decompressTimeNs);
        }

        /** Resets all counters to zero. */
        public void reset() {
            compressedSize.set(0);
            uncompressedSize.set(0);
            compressionTimeNs.set(0);
            decompressionTimeNs.set(0);
            operationCount.set(0);
        }

        @Override
        public String toString() {
            return String.format("CodecMetrics{codec='%s', ops=%d, ratio=%.3f, compressSpeed=%.1f B/ns, decompressSpeed=%.1f B/ns}",
                    codec, operationCount.get(), averageRatio(), averageCompressionSpeed(), averageDecompressionSpeed());
        }
    }

    // ─── Sliding window entry ──────────────────────────────────────────────

    /** One compression event stored in the sliding window. */
    public static final class CompressionRecord {
        final String codec;
        final long uncompressedSize;
        final long compressedSize;
        final long compressionTimeNs;
        final long timestamp;

        CompressionRecord(String codec, long uncompressedSize, long compressedSize, long compressionTimeNs) {
            this.codec = codec;
            this.uncompressedSize = uncompressedSize;
            this.compressedSize = compressedSize;
            this.compressionTimeNs = compressionTimeNs;
            this.timestamp = System.nanoTime();
        }
    }

    // ─── Metrics snapshot ──────────────────────────────────────────────────

    /** Point-in-time snapshot of all codec metrics. */
    public static final class MetricsSnapshot {
        private final Map<String, CodecMetrics> metrics;
        private final String recommendedCodec;
        private final long timestamp;
        private final int windowSize;
        private final int totalOperations;

        MetricsSnapshot(Map<String, CodecMetrics> metrics, String recommendedCodec,
                        long timestamp, int windowSize, int totalOperations) {
            this.metrics = Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
            this.recommendedCodec = recommendedCodec;
            this.timestamp = timestamp;
            this.windowSize = windowSize;
            this.totalOperations = totalOperations;
        }

        /** Per-codec metrics map (codec name → metrics). */
        public Map<String, CodecMetrics> metrics() {
            return metrics;
        }

        /** The recommended codec for the current data pattern, or {@code null} if insufficient data. */
        public String recommendedCodec() {
            return recommendedCodec;
        }

        /** Snapshot timestamp (System.nanoTime() at creation). */
        public long timestamp() {
            return timestamp;
        }

        /** The configured sliding window size. */
        public int windowSize() {
            return windowSize;
        }

        /** Total number of recorded operations across all codecs. */
        public int totalOperations() {
            return totalOperations;
        }

        /** Whether enough samples have been collected for reliable recommendations. */
        public boolean hasSufficientData(int minSamples) {
            return totalOperations >= minSamples;
        }

        @Override
        public String toString() {
            return String.format("MetricsSnapshot{recommended='%s', ops=%d, windowSize=%d, timestamp=%d, codecs=%s}",
                    recommendedCodec, totalOperations, windowSize, timestamp, metrics.keySet());
        }
    }

    // ─── Instance state ────────────────────────────────────────────────────

    private static final ConcurrentMap<String, CodecMetrics> METRICS_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<CompressionRecord> WINDOW = new ConcurrentLinkedQueue<>();

    private static volatile String lastRecommendation = null;

    private AdaptiveCompressionManager() {
        throw new AssertionError("No instances");
    }

    // ─── Public API ────────────────────────────────────────────────────────

    /**
     * Records a compression event for the given codec.
     *
     * @param codec             the codec name used
     * @param uncompressedSize  original data size in bytes
     * @param compressedSize    compressed data size in bytes
     * @param compressionTimeNs time spent compressing, in nanoseconds
     */
    public static void recordCompression(String codec, long uncompressedSize,
                                         long compressedSize, long compressionTimeNs) {
        if (codec == null || uncompressedSize < 0 || compressedSize < 0 || compressionTimeNs < 0) {
            LOGGER.warn("Invalid compression record: codec={}, uncompressed={}, compressed={}, time={}",
                    codec, uncompressedSize, compressedSize, compressionTimeNs);
            return;
        }
        getOrCreateMetrics(codec).record(uncompressedSize, compressedSize, compressionTimeNs);
        addToWindow(new CompressionRecord(codec, uncompressedSize, compressedSize, compressionTimeNs));
        LOGGER.debug("Recorded compression: codec={}, ratio={}, time={} ns",
                codec, (double) compressedSize / Math.max(uncompressedSize, 1), compressionTimeNs);
    }

    /**
     * Records a decompression event for the given codec.
     *
     * @param codec              the codec name used
     * @param decompressionTimeNs time spent decompressing, in nanoseconds
     * @param decompressedSize   size of the decompressed data in bytes
     */
    public static void recordDecompression(String codec, long decompressionTimeNs, long decompressedSize) {
        if (codec == null || decompressionTimeNs < 0 || decompressedSize < 0) {
            LOGGER.warn("Invalid decompression record: codec={}, time={}, size={}",
                    codec, decompressionTimeNs, decompressedSize);
            return;
        }
        getOrCreateMetrics(codec).recordDecompression(decompressionTimeNs);
    }

    /**
     * Recommends the best codec for the given estimated payload size.
     *
     * <p>If adaptive mode is disabled or insufficient samples have been collected,
     * falls back to {@link BZip2Codec#getRecommendedCodec(long, String)}.
     * Otherwise, picks the codec with the best average compression ratio from
     * the collected metrics.
     *
     * @param estimatedBytes estimated uncompressed payload size in bytes
     * @return recommended codec name (never {@code null})
     */
    public static String recommendCodec(long estimatedBytes) {
        if (!isEnabled() || totalOperations() < minSamples()) {
            String fallback = BZip2Codec.getRecommendedCodec(estimatedBytes, null);
            LOGGER.debug("Adaptive disabled or insufficient data ({} < {}), fallback to {}",
                    totalOperations(), minSamples(), fallback);
            return fallback;
        }
        String best = pickBestCodec(estimatedBytes);
        if (best == null) {
            best = BZip2Codec.getRecommendedCodec(estimatedBytes, null);
        }
        lastRecommendation = best;
        LOGGER.info("Adaptive recommendation: {} (estimated {} bytes, {} ops recorded)",
                best, estimatedBytes, totalOperations());
        return best;
    }

    /**
     * Returns a point-in-time snapshot of all collected metrics.
     *
     * @return a non-null snapshot
     */
    public static MetricsSnapshot getMetricsSnapshot() {
        Map<String, CodecMetrics> copy = new LinkedHashMap<>();
        for (Map.Entry<String, CodecMetrics> e : METRICS_MAP.entrySet()) {
            copy.put(e.getKey(), e.getValue());
        }
        String recommended = null;
        if (isEnabled() && totalOperations() >= minSamples()) {
            recommended = pickBestCodec(-1L);
        }
        return new MetricsSnapshot(copy, recommended, System.nanoTime(), windowSize(), totalOperations());
    }

    /**
     * Returns the metrics for a specific codec, or {@code null} if no
     * operations have been recorded for that codec.
     *
     * @param codec the codec name
     * @return the metrics, or {@code null}
     */
    public static CodecMetrics getCodecMetrics(String codec) {
        return METRICS_MAP.get(codec);
    }

    /** Resets all collected metrics and clears the sliding window. */
    public static void resetMetrics() {
        METRICS_MAP.clear();
        WINDOW.clear();
        lastRecommendation = null;
        LOGGER.info("All adaptive compression metrics reset");
    }

    /**
     * Analyzes the data pattern of the given rows to help codec selection.
     *
     * <p>Heuristic: examines a sample of rows and classifies the dominant
     * data shape as {@code repetitive}, {@code text}, {@code numeric}, or
     * {@code mixed}. Repetitive wins when most string values are constants
     * across rows; then the dominant value type (text vs numeric) is chosen.
     *
     * @param rows list of rows (Object[] or Map)
     * @return the detected data pattern string
     */
    public static String analyzeDataPattern(List<?> rows) {
        if (rows == null || rows.isEmpty()) {
            return StorageMessageConstants.CONTENT_MIXED;
        }
        int sample = Math.min(rows.size(), 100);
        long textValues = 0;
        long numericValues = 0;
        long repeatedValues = 0;
        long knownValues = 0;

        // Pre-collect the global set of distinct string values to detect constants.
        Set<String> distinctStrings = new HashSet<>();
        for (int i = 0; i < sample; i++) {
            for (Object v : extractValues(rows.get(i))) {
                if (v instanceof String s) {
                    distinctStrings.add(s);
                }
            }
        }

        for (int i = 0; i < sample; i++) {
            for (Object v : extractValues(rows.get(i))) {
                if (v == null) {
                    continue;
                }
                if (v instanceof String s && !s.isEmpty()) {
                    textValues++;
                    knownValues++;
                    // A value is "repeated" when it is one of few distinct strings
                    // seen many times in the sample.
                    if (distinctStrings.size() <= 8 && countMatches(rows, sample, s) >= 2) {
                        repeatedValues++;
                    }
                } else if (v instanceof Number || v instanceof Boolean) {
                    numericValues++;
                    knownValues++;
                }
            }
        }

        if (knownValues == 0) {
            return StorageMessageConstants.CONTENT_MIXED;
        }
        double textRatio = (double) textValues / knownValues;
        double numericRatio = (double) numericValues / knownValues;
        double repeatedRatio = (double) repeatedValues / Math.max(textValues, 1);

        if (repeatedRatio > 0.6 && textValues > 0) {
            return "repetitive";
        }
        if (numericRatio > 0.7) {
            return "numeric";
        }
        if (textRatio > 0.7) {
            return "text";
        }
        return StorageMessageConstants.CONTENT_MIXED;
    }

    /** Whether adaptive mode is currently enabled. */
    public static boolean isEnabled() {
        return resolveEnabled();
    }

    /** The effective sliding window size. */
    public static int windowSize() {
        return resolveWindowSize();
    }

    /** The effective ratio threshold. */
    public static double ratioThreshold() {
        return resolveRatioThreshold();
    }

    /** The effective minimum sample count. */
    public static int minSamples() {
        return resolveMinSamples();
    }

    /** The last recommendation produced by {@link #recommendCodec(long)}, or {@code null}. */
    public static String lastRecommendation() {
        return lastRecommendation;
    }

    /** Returns a human-readable configuration summary. */
    public static String configSummary() {
        return String.format("AdaptiveCompressionConfig{enabled=%s, windowSize=%d, ratioThreshold=%.2f, minSamples=%d}",
                isEnabled(), windowSize(), ratioThreshold(), minSamples());
    }

    // ─── Config resolution (call-time, sysprop → config.properties → default) ──

    private static boolean resolveEnabled() {
        String raw = getString(ENABLED_KEY);
        if (raw != null) {
            switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "on":
                case "true":
                case "yes":
                    return true;
                case "off":
                case "false":
                case "no":
                    return false;
                default:
                    LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, ENABLED_KEY, raw, DEFAULT_ENABLED);
            }
        }
        return DEFAULT_ENABLED;
    }

    private static int resolveWindowSize() {
        String raw = getString(WINDOW_SIZE_KEY);
        if (raw != null) {
            try {
                int v = Integer.parseInt(raw.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, WINDOW_SIZE_KEY, raw, DEFAULT_WINDOW_SIZE);
            }
        }
        return DEFAULT_WINDOW_SIZE;
    }

    private static double resolveRatioThreshold() {
        String raw = getString(RATIO_THRESHOLD_KEY);
        if (raw != null) {
            try {
                double v = Double.parseDouble(raw.trim());
                if (v >= 0.0 && v <= 1.0) {
                    return v;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, RATIO_THRESHOLD_KEY, raw, DEFAULT_RATIO_THRESHOLD);
            }
        }
        return DEFAULT_RATIO_THRESHOLD;
    }

    private static int resolveMinSamples() {
        String raw = getString(MIN_SAMPLES_KEY);
        if (raw != null) {
            try {
                int v = Integer.parseInt(raw.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, MIN_SAMPLES_KEY, raw, DEFAULT_MIN_SAMPLES);
            }
        }
        return DEFAULT_MIN_SAMPLES;
    }

    // ─── Internal helpers ──────────────────────────────────────────────────

    private static String getString(String key) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        return readProperty(key);
    }

    private static CodecMetrics getOrCreateMetrics(String codec) {
        return METRICS_MAP.computeIfAbsent(codec, CodecMetrics::new);
    }

    private static void addToWindow(CompressionRecord record) {
        WINDOW.add(record);
        while (WINDOW.size() > windowSize()) {
            WINDOW.poll();
        }
    }

    private static int totalOperations() {
        int total = 0;
        for (CodecMetrics m : METRICS_MAP.values()) {
            total += (int) m.operationCount();
        }
        return total;
    }

    private static String pickBestCodec(long estimatedBytes) {
        String bestCodec = null;
        double bestRatio = Double.MAX_VALUE;
        for (CodecMetrics m : METRICS_MAP.values()) {
            double ratio = m.averageRatio();
            if (ratio > 0 && ratio < bestRatio) {
                bestRatio = ratio;
                bestCodec = m.codec();
            }
        }
        return bestCodec;
    }

    private static Object[] extractValues(Object row) {
        if (row instanceof Object[] a) {
            return a;
        }
        if (row instanceof Map<?, ?> m) {
            return m.values().toArray();
        }
        return new Object[0];
    }

    private static int countMatches(List<?> rows, int sample, String value) {
        int matches = 0;
        for (int i = 0; i < sample && i < rows.size(); i++) {
            for (Object v : extractValues(rows.get(i))) {
                if (value.equals(v)) {
                    matches++;
                }
            }
        }
        return matches;
    }

    private static String readProperty(String key) {
        Properties props = new Properties();
        String userDir = System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, ".");
        File configFile = new File(userDir, ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (var in = Files.newInputStream(configFile.toPath())) {
                props.load(in);
                return props.getProperty(key);
            } catch (IOException ignored) {
                LOGGER.debug("Could not read config.properties for key {}", key);
            }
        }
        return null;
    }
}