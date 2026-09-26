package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import diesel.ConfigKeys;

/**
 * Deflate compression level configuration for AVRO data files with adaptive
 * level selection and compressor/decompressor caching (Prompt 65).
 *
 * <p>Deflate is a versatile compression codec that offers a trade-off between
 * compression speed and ratio. This configuration provides intelligent level
 * selection based on data characteristics and caches {@link Deflater}/
 * {@link Inflater} instances so the (expensive) native-object construction is
 * amortised over many small write/read operations.
 *
 * <p>Level selection strategy:
 * <ul>
 *   <li>Level 1 (fastest): Best for small files or real-time compression</li>
 *   <li>Level 3 (default): Balanced speed/ratio for general use</li>
 *   <li>Level 5: Better ratio with moderate speed impact</li>
 *   <li>Level 7: High ratio for archival data</li>
 *   <li>Level 9 (slowest): Maximum ratio, best compression</li>
 * </ul>
 *
 * <p>Adaptive selection ({@link #selectOptimalLevel(long, String, boolean)})
 * picks the level from the payload size, the dominant data type and whether the
 * write is streaming:
 * <ul>
 *   <li>Small data (&lt;1KB): level 1 for speed</li>
 *   <li>Streaming write: level 1-3 to minimize latency</li>
 *   <li>Text data: level 3-5 for good compression</li>
 *   <li>Repetitive data: level 5-7 for better ratio</li>
 *   <li>Archive data (&gt;1MB): level 7 for maximum compression</li>
 * </ul>
 *
 * <p>Compressor/decompressor caching is backed by a bounded per-level pool of
 * reusable {@link Deflater}/{@link Inflater} instances ({@code borrow}/
 * {@code return} semantics) and is enabled by
 * {@code avro.deflate.cache.compressors = on} (default) with a configurable cap
 * {@code avro.deflate.cache.max.size} (default 10) — resolved sysprop →
 * config.properties → code default, mirroring {@link AvroCompressionConfig}.
 *
 * @since Prompt 65
 */
public final class DeflateLevelConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeflateLevelConfig.class);

    /** Default compression level (balanced speed/ratio). */
    public static final int DEFAULT_LEVEL = 3;

    /** Minimum compression level (fastest). */
    public static final int MIN_LEVEL = 1;

    /** Maximum compression level (best compression). */
    public static final int MAX_LEVEL = 9;

    /** Predefined levels for benchmarking and testing. */
    public static final int[] BENCHMARK_LEVELS = {1, 3, 5, 7, 9};

    /** Config key: enable compressor/decompressor caching. */
    public static final String CACHE_KEY = "avro.deflate.cache.compressors";
    /** Config key: max cached compressors per level. */
    public static final String CACHE_MAX_KEY = "avro.deflate.cache.max.size";

    /** Default: caching enabled. */
    public static final boolean DEFAULT_CACHE_ENABLED = true;
    /** Default: 10 cached objects per level. */
    public static final int DEFAULT_CACHE_MAX = 10;

    /** Threshold for small data (bytes) - use faster compression. */
    private static final int SMALL_DATA_THRESHOLD = 1024;

    /** Threshold for large data (bytes) - use better compression. */
    private static final int LARGE_DATA_THRESHOLD = 1024 * 1024; // 1MB

    /** Bounded pool of cached {@link Deflater}s per level. */
    private static final ConcurrentMap<Integer, Deque<Deflater>> DEFLATER_POOL = new ConcurrentHashMap<>();

    /** Bounded pool of cached {@link Inflater}s (level-independent, shared). */
    private static final Deque<Inflater> INFLATER_POOL = new ArrayDeque<>();

    private static volatile boolean cacheEnabled = DEFAULT_CACHE_ENABLED;
    private static volatile int cacheMax = DEFAULT_CACHE_MAX;

    static {
        resolveCacheConfig();
    }

    private DeflateLevelConfig() {
        throw new AssertionError("No instances");
    }

    /**
     * Resolves the caching settings from sysprop → config.properties → defaults.
     * Public so tests can force a re-read after changing a system property.
     */
    public static void resolveCacheConfig() {
        String enabled = System.getProperty(CACHE_KEY);
        if (enabled == null) {
            String fromFile = readProperty(CACHE_KEY);
            if (fromFile != null) {
                enabled = fromFile;
            }
        }
        if (enabled != null) {
            cacheEnabled = parseBool(enabled, DEFAULT_CACHE_ENABLED);
        }

        int max = DEFAULT_CACHE_MAX;
        String rawMax = System.getProperty(CACHE_MAX_KEY);
        if (rawMax == null) {
            String fromFile = readProperty(CACHE_MAX_KEY);
            if (fromFile != null) {
                rawMax = fromFile;
            }
        }
        if (rawMax != null) {
            try {
                int parsed = Integer.parseInt(rawMax.trim());
                if (parsed < 0) {
                    LOGGER.warn("{} must be >= 0, got {}; using default {}", CACHE_MAX_KEY, rawMax, DEFAULT_CACHE_MAX);
                } else {
                    max = parsed;
                }
            } catch (NumberFormatException e) {
                LOGGER.warn("{} is not a valid integer '{}'; using default {}", CACHE_MAX_KEY, rawMax, DEFAULT_CACHE_MAX);
            }
        }
        cacheMax = max;
    }

    private static String readProperty(String key) {
        try {
            Properties props = new Properties();
            try (java.io.InputStream in = new java.io.FileInputStream(ConfigKeys.CONFIG_FILE)) {
                props.load(in);
                return props.getProperty(key);
            }
        } catch (java.io.IOException e) {
            return null;
        }
    }

    private static boolean parseBool(String value, boolean defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "on", "1", "yes" -> true;
            case "false", "off", "0", "no" -> false;
            default -> defaultValue;
        };
    }

    /**
     * Resolves a deflate level to a valid compressible range: {@code -1} maps to
     * the default level, out-of-range values are clamped to {@code [1, 9]}.
     *
     * @param level the configured level
     * @return the effective level
     */
    public static int resolveLevel(int level) {
        if (level == AvroCompressionConfig.DEFAULT_LEVEL) {
            return DEFAULT_LEVEL;
        }
        if (level < MIN_LEVEL) {
            LOGGER.warn("Deflate level {} below minimum {}, clamping to {}", level, MIN_LEVEL, MIN_LEVEL);
            return MIN_LEVEL;
        }
        if (level > MAX_LEVEL) {
            LOGGER.warn("Deflate level {} above maximum {}, clamping to {}", level, MAX_LEVEL, MAX_LEVEL);
            return MAX_LEVEL;
        }
        return level;
    }

    /**
     * Creates a new Avro {@link CodecFactory} for Deflate compression with the
     * specified level.
     *
     * @param level the compression level (1-9, -1 for default)
     * @return the Avro codec factory for writing
     */
    public static CodecFactory newCodec(int level) {
        int effectiveLevel = resolveLevel(level);
        LOGGER.debug("Creating Deflate codec with level {}", effectiveLevel);
        return CodecFactory.deflateCodec(effectiveLevel);
    }

    /**
     * Selects the optimal deflate level based on data characteristics.
     *
     * @param dataSize    the size of the data to be compressed in bytes
     * @param dataType    the type of data (text, numeric, binary, repetitive)
     * @param isStreaming whether this is for real-time streaming
     * @return the recommended compression level in {@code [1, 9]}
     */
    public static int selectOptimalLevel(long dataSize, String dataType, boolean isStreaming) {
        if (isStreaming) {
            return Math.min(MIN_LEVEL + 2, DEFAULT_LEVEL);
        }
        if (dataSize < SMALL_DATA_THRESHOLD) {
            return MIN_LEVEL;
        }
        if (dataSize > LARGE_DATA_THRESHOLD) {
            return 7;
        }
        if (dataType != null) {
            return switch (dataType.toLowerCase(java.util.Locale.ROOT)) {
                case "text" -> 3;
                case "numeric" -> 2;
                case "binary" -> 5;
                case "repetitive" -> 6;
                default -> DEFAULT_LEVEL;
            };
        }
        return DEFAULT_LEVEL;
    }

    /**
     * Analyzes the compression trade-off for a given level.
     *
     * @param level the compression level to analyze
     * @return a human-readable description of the speed/ratio trade-off
     */
    public static String analyzeTradeoff(int level) {
        return switch (level) {
            case 1 -> "Level 1: Fastest compression, lowest ratio (best for real-time)";
            case 2 -> "Level 2: Very fast compression, low ratio";
            case 3 -> "Level 3: Fast compression, good ratio (balanced, default)";
            case 4 -> "Level 4: Moderate speed, better ratio";
            case 5 -> "Level 5: Good speed, better ratio (recommended for text)";
            case 6 -> "Level 6: Moderate speed, high ratio (good for repetitive data)";
            case 7 -> "Level 7: Slow compression, high ratio (good for large files)";
            case 8 -> "Level 8: Very slow compression, very high ratio";
            case 9 -> "Level 9: Slowest compression, highest ratio (best for archival)";
            default -> "Level " + level + ": Unknown trade-off";
        };
    }

    /**
     * Borrows a reusable {@link Deflater} for the given level, or allocates a
     * fresh one when the pool is empty or caching is disabled.
     *
     * @param level the compression level (1-9)
     * @return a Deflater configured for the level; call {@link #returnDeflater}
     *         when done
     */
    public static Deflater borrowDeflater(int level) {
        int effectiveLevel = resolveLevel(level);
        if (!cacheEnabled) {
            return new Deflater(effectiveLevel);
        }
        Deque<Deflater> pool = DEFLATER_POOL.get(effectiveLevel);
        if (pool == null) {
            return new Deflater(effectiveLevel);
        }
        Deflater d = pool.pollLast();
        return d != null ? d : new Deflater(effectiveLevel);
    }

    /**
     * Returns a {@link Deflater} to the bounded per-level pool (resetting its
     * state so it can be reused). The instance is discarded silently when the
     * pool is full or caching was disabled mid-run.
     *
     * @param level the compression level the Deflater was created with
     * @param d     the Deflater to release
     */
    public static void returnDeflater(int level, Deflater d) {
        if (!cacheEnabled || cacheMax <= 0) {
            d.end();
            return;
        }
        int effectiveLevel = resolveLevel(level);
        d.reset();
        DEFLATER_POOL.computeIfAbsent(effectiveLevel, k -> new ArrayDeque<>());
        Deque<Deflater> pool = DEFLATER_POOL.get(effectiveLevel);
        synchronized (pool) {
            if (pool.size() < cacheMax) {
                pool.addLast(d);
            } else {
                d.end();
            }
        }
    }

    /**
     * Borrows a reusable {@link Inflater}. ZLIB decoding is level-independent,
     * so a single shared pool is used.
     *
     * @return an Inflater; call {@link #returnInflater} when done
     */
    public static Inflater borrowInflater() {
        if (!cacheEnabled) {
            return new Inflater();
        }
        Inflater inflater;
        synchronized (INFLATER_POOL) {
            inflater = INFLATER_POOL.pollLast();
        }
        return inflater != null ? inflater : new Inflater();
    }

    /**
     * Returns an {@link Inflater} to the bounded shared pool.
     *
     * @param i the Inflater to release
     */
    public static void returnInflater(Inflater i) {
        if (!cacheEnabled || cacheMax <= 0) {
            i.end();
            return;
        }
        i.reset();
        synchronized (INFLATER_POOL) {
            if (INFLATER_POOL.size() < cacheMax) {
                INFLATER_POOL.addLast(i);
            } else {
                i.end();
            }
        }
    }

    /**
     * Clears the compressor and decompressor pools, releasing all native
     * resources. Safe to call at any time; borrowed objects are unaffected.
     */
    public static void clearCache() {
        synchronized (INFLATER_POOL) {
            for (Inflater i : INFLATER_POOL) {
                i.end();
            }
            INFLATER_POOL.clear();
        }
        for (Deque<Deflater> pool : DEFLATER_POOL.values()) {
            synchronized (pool) {
                for (Deflater d : pool) {
                    d.end();
                }
                pool.clear();
            }
        }
        DEFLATER_POOL.clear();
        LOGGER.debug("Cleared deflate compressor/decompressor pools");
    }

    /**
     * Returns cache statistics as a readable string.
     *
     * @return e.g. {@code Deflate cache: enabled, 2 deflaters, 1 inflater, max 10}
     */
    public static String getCacheStats() {
        int deflaters = 0;
        for (Deque<Deflater> pool : DEFLATER_POOL.values()) {
            deflaters += pool.size();
        }
        int inflaters;
        synchronized (INFLATER_POOL) {
            inflaters = INFLATER_POOL.size();
        }
        return String.format("Deflate cache: %s, %d deflaters, %d inflaters, max %d",
                cacheEnabled ? "enabled" : "disabled", deflaters, inflaters, cacheMax);
    }

    /**
     * Estimates the compression ratio for the given level and data type.
     * Returns a value in {@code (0, 1]} where smaller means better compression.
     *
     * @param level    the compression level (1-9)
     * @param dataType the type of data (text, numeric, binary, repetitive)
     * @return estimated compressed/uncompressed size ratio
     */
    public static double estimateCompressionRatio(int level, String dataType) {
        double baseRatio = switch (resolveLevel(level)) {
            case 1 -> 0.5;
            case 2 -> 0.4;
            case 3 -> 0.35;
            case 4 -> 0.3;
            case 5 -> 0.25;
            case 6 -> 0.2;
            case 7 -> 0.15;
            case 8 -> 0.12;
            case 9 -> 0.1;
            default -> 0.35;
        };
        if (dataType != null) {
            baseRatio *= switch (dataType.toLowerCase(java.util.Locale.ROOT)) {
                case "text" -> 0.7;
                case "numeric" -> 0.85;
                case "binary" -> 0.9;
                case "repetitive" -> 0.6;
                default -> 1.0;
            };
        }
        return Math.max(0.05, Math.min(1.0, baseRatio));
    }

    /**
     * Gets benchmark information for different compression levels.
     *
     * @return array of level description strings
     */
    public static String[] getBenchmarkInfo() {
        return new String[]{
                "Level 1: ~100% speed, ~50% ratio",
                "Level 3: ~75% speed, ~65% ratio (default)",
                "Level 5: ~50% speed, ~75% ratio",
                "Level 7: ~25% speed, ~85% ratio",
                "Level 9: ~10% speed, ~90% ratio"
        };
    }
}