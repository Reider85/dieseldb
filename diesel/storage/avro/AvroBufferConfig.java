package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * Resolved AVRO read/write buffer configuration (Prompt 78).
 *
 * <p>Write buffer size ({@code avro.buffer.write.size}), read buffer size
 * ({@code avro.buffer.read.size}), the flush strategy
 * ({@code avro.buffer.flush.strategy}), the time-flush interval
 * ({@code avro.buffer.flush.interval.ms}) and the zero-copy switch
 * ({@code avro.buffer.zero.copy}) are resolved from a system-property
 * override first, then the root {@code config.properties}, then the
 * code-level defaults.
 *
 * <p>Flush strategies control how {@link AvroBufferManager} drains its
 * managed write buffers:
 * <ul>
 *   <li>{@code size} — flush a buffer as soon as the buffered payload reaches
 *       the configured {@code avro.buffer.write.size} byte budget</li>
 *   <li>{@code time} — flush every {@code avro.buffer.flush.interval.ms}
 *       milliseconds via a background scheduler</li>
 *   <li>{@code forced} — only explicit {@code flush}/{@code flushAll} calls
 *       drain a buffer (largest batches, highest latency)</li>
 * </ul>
 *
 * <p>Zero-copy ({@code avro.buffer.zero.copy}) gates the memory-mapped
 * ({@code FileChannel.map}) and copy-free ({@code ByteBuffer.wrap}) read
 * paths exposed by {@link AvroBufferManager}.
 *
 * @since Prompt 78
 */
public final class AvroBufferConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBufferConfig.class);

    /** Config key: write buffer size in bytes. */
    public static final String WRITE_SIZE_KEY = "avro.buffer.write.size";
    /** Config key: read buffer size in bytes. */
    public static final String READ_SIZE_KEY = "avro.buffer.read.size";
    /** Config key: flush strategy (size | time | forced). */
    public static final String FLUSH_STRATEGY_KEY = "avro.buffer.flush.strategy";
    /** Config key: time-flush interval in milliseconds. */
    public static final String FLUSH_INTERVAL_KEY = "avro.buffer.flush.interval.ms";
    /** Config key: enable zero-copy read paths. */
    public static final String ZERO_COPY_KEY = "avro.buffer.zero.copy";

    /** Code-level defaults. */
    public static final int DEFAULT_WRITE_SIZE = 65536;          // 64 KB
    public static final int DEFAULT_READ_SIZE = 65536;           // 64 KB
    public static final String DEFAULT_FLUSH_STRATEGY = "size";
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;
    public static final boolean DEFAULT_ZERO_COPY = true;

    /** Flush strategy names. */
    public static final String STRATEGY_SIZE = "size";
    public static final String STRATEGY_TIME = "time";
    public static final String STRATEGY_FORCED = "forced";

    private static final java.util.Set<String> VALID_STRATEGIES =
            java.util.Set.of(STRATEGY_SIZE, STRATEGY_TIME, STRATEGY_FORCED);

    private final int writeBufferSize;
    private final int readBufferSize;
    private final FlushStrategy flushStrategy;
    private final long flushIntervalMs;
    private final boolean zeroCopyEnabled;

    /**
     * The flush policy applied by {@link AvroBufferManager} to its managed
     * write buffers.
     */
    public enum FlushStrategy {
        /** Flush when the buffered byte budget is reached. */
        SIZE,
        /** Flush periodically on a background scheduler. */
        TIME,
        /** Flush only on an explicit {@code flush}/{@code flushAll} call. */
        FORCED
    }

    private AvroBufferConfig(int writeBufferSize, int readBufferSize,
                             FlushStrategy flushStrategy, long flushIntervalMs,
                             boolean zeroCopyEnabled) {
        this.writeBufferSize = writeBufferSize;
        this.readBufferSize = readBufferSize;
        this.flushStrategy = flushStrategy;
        this.flushIntervalMs = flushIntervalMs;
        this.zeroCopyEnabled = zeroCopyEnabled;
    }

    /** The configured write buffer size in bytes. */
    public int writeBufferSize() {
        return writeBufferSize;
    }

    /** The configured read buffer size in bytes. */
    public int readBufferSize() {
        return readBufferSize;
    }

    /** The configured flush strategy. */
    public FlushStrategy flushStrategy() {
        return flushStrategy;
    }

    /** The time-flush interval in milliseconds. */
    public long flushIntervalMs() {
        return flushIntervalMs;
    }

    /** Whether zero-copy read paths are enabled. */
    public boolean zeroCopyEnabled() {
        return zeroCopyEnabled;
    }

    /**
     * Resolves the AVRO buffer configuration:
     * system property override, then {@code config.properties}, then defaults.
     *
     * @return the resolved configuration (never {@code null})
     */
    public static AvroBufferConfig resolve() {
        int writeSize = getInt(WRITE_SIZE_KEY, DEFAULT_WRITE_SIZE);
        if (writeSize <= 0) {
            LOGGER.warn("Invalid {} value {}, using default {}", WRITE_SIZE_KEY, writeSize, DEFAULT_WRITE_SIZE);
            writeSize = DEFAULT_WRITE_SIZE;
        }
        int readSize = getInt(READ_SIZE_KEY, DEFAULT_READ_SIZE);
        if (readSize <= 0) {
            LOGGER.warn("Invalid {} value {}, using default {}", READ_SIZE_KEY, readSize, DEFAULT_READ_SIZE);
            readSize = DEFAULT_READ_SIZE;
        }
        FlushStrategy strategy = normalizeStrategy(getString(FLUSH_STRATEGY_KEY, DEFAULT_FLUSH_STRATEGY));
        long interval = getLong(FLUSH_INTERVAL_KEY, DEFAULT_FLUSH_INTERVAL_MS);
        if (interval <= 0) {
            LOGGER.warn("Invalid {} value {}, using default {}", FLUSH_INTERVAL_KEY, interval, DEFAULT_FLUSH_INTERVAL_MS);
            interval = DEFAULT_FLUSH_INTERVAL_MS;
        }
        boolean zeroCopy = getBoolean(ZERO_COPY_KEY, DEFAULT_ZERO_COPY);
        return new AvroBufferConfig(writeSize, readSize, strategy, interval, zeroCopy);
    }

    private static FlushStrategy normalizeStrategy(String raw) {
        if (raw == null || raw.isBlank()) {
            return FlushStrategy.SIZE;
        }
        String s = raw.trim().toLowerCase(Locale.ROOT);
        return switch (s) {
            case STRATEGY_TIME -> FlushStrategy.TIME;
            case STRATEGY_FORCED -> FlushStrategy.FORCED;
            case STRATEGY_SIZE -> FlushStrategy.SIZE;
            default -> {
                LOGGER.warn("Unknown flush strategy '{}', using default '{}'", raw, DEFAULT_FLUSH_STRATEGY);
                yield FlushStrategy.SIZE;
            }
        };
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        if (raw == null) {
            return defaultValue;
        }
        String v = raw.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(v) || "on".equals(v) || "yes".equals(v) || "1".equals(v)) {
            return true;
        }
        if ("false".equals(v) || "off".equals(v) || "no".equals(v) || "0".equals(v)) {
            return false;
        }
        LOGGER.warn("Invalid {} value '{}', using default {}", key, raw, defaultValue);
        return defaultValue;
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} value '{}', using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn("Invalid {} value '{}', using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.buffer.config.file";

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug("Could not read config.properties, using defaults: {}", ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroBufferConfig{writeBufferSize=" + writeBufferSize
                + ", readBufferSize=" + readBufferSize
                + ", flushStrategy=" + flushStrategy
                + ", flushIntervalMs=" + flushIntervalMs
                + ", zeroCopyEnabled=" + zeroCopyEnabled + '}';
    }
}