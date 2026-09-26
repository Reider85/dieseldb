package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * Resolved AVRO block configuration (Prompt 68).
 *
 * <p>The block size ({@code avro.block.size}), workload preset
 * ({@code avro.block.workload}) and sync marker interval
 * ({@code avro.block.sync.interval}) are resolved from a system-property
 * override first, then the root {@code config.properties}, then the
 * code-level defaults.
 *
 * <p>Workload presets control the block size independently of the
 * explicit {@code avro.block.size} setting:
 * <ul>
 *   <li>{@code streaming} — 128 MB blocks, fewer sync markers,
 *       best sequential throughput</li>
 *   <li>{@code random_access} — 16 MB blocks, finer granularity
 *       for block-level seeking</li>
 *   <li>{@code batch} — 256 MB blocks, bulk-load optimization</li>
 *   <li>{@code default} — uses the explicit {@code avro.block.size}
 *       value (64 MB)</li>
 * </ul>
 *
 * @since Prompt 68
 */
public final class AvroBlockConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBlockConfig.class);

    /** Config key: block size in bytes. */
    public static final String BLOCK_SIZE_KEY = "avro.block.size";
    /** Config key: workload preset. */
    public static final String WORKLOAD_KEY = "avro.block.workload";
    /** Config key: sync marker interval in bytes. */
    public static final String SYNC_INTERVAL_KEY = "avro.block.sync.interval";

    /** Code-level defaults. */
    public static final long DEFAULT_BLOCK_SIZE = 67108864L;       // 64 MB
    public static final String DEFAULT_WORKLOAD = "default";
    public static final long DEFAULT_SYNC_INTERVAL = 67108864L;   // 64 MB

    /** Workload presets (name → block size in bytes). */
    public static final String WORKLOAD_STREAMING = "streaming";
    public static final String WORKLOAD_RANDOM_ACCESS = "random_access";
    public static final String WORKLOAD_BATCH = "batch";

    public static final long STREAMING_BLOCK_SIZE = 134217728L;    // 128 MB
    public static final long RANDOM_ACCESS_BLOCK_SIZE = 16777216L; // 16 MB
    public static final long BATCH_BLOCK_SIZE = 268435456L;        // 256 MB

    private static final java.util.Set<String> VALID_WORKLOADS =
            java.util.Set.of(DEFAULT_WORKLOAD, WORKLOAD_STREAMING, WORKLOAD_RANDOM_ACCESS, WORKLOAD_BATCH);

    private final long blockSize;
    private final String workload;
    private final long syncInterval;

    private AvroBlockConfig(long blockSize, String workload, long syncInterval) {
        this.blockSize = blockSize;
        this.workload = workload;
        this.syncInterval = syncInterval;
    }

    /** The configured block size in bytes. */
    public long blockSize() {
        return blockSize;
    }

    /** The configured workload preset. */
    public String workload() {
        return workload;
    }

    /** The sync marker interval in bytes. */
    public long syncInterval() {
        return syncInterval;
    }

    /**
     * Returns the effective block size for the configured workload.
     * When the workload is {@code default}, the explicit
     * {@code avro.block.size} value is returned; otherwise the
     * workload preset size takes precedence.
     */
    public long effectiveBlockSize() {
        return switch (workload) {
            case WORKLOAD_STREAMING -> STREAMING_BLOCK_SIZE;
            case WORKLOAD_RANDOM_ACCESS -> RANDOM_ACCESS_BLOCK_SIZE;
            case WORKLOAD_BATCH -> BATCH_BLOCK_SIZE;
            default -> blockSize;
        };
    }

    /**
     * Resolves the AVRO block configuration:
     * system property override, then {@code config.properties}, then defaults.
     *
     * @return the resolved configuration (never {@code null})
     */
    public static AvroBlockConfig resolve() {
        long blockSize = getLong(BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
        if (blockSize <= 0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, BLOCK_SIZE_KEY, blockSize, DEFAULT_BLOCK_SIZE);
            blockSize = DEFAULT_BLOCK_SIZE;
        }
        String workload = normalizeWorkload(getString(WORKLOAD_KEY, DEFAULT_WORKLOAD));
        long syncInterval = getLong(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL);
        if (syncInterval <= 0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, SYNC_INTERVAL_KEY, syncInterval, DEFAULT_SYNC_INTERVAL);
            syncInterval = DEFAULT_SYNC_INTERVAL;
        }
        return new AvroBlockConfig(blockSize, workload, syncInterval);
    }

    private static String normalizeWorkload(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_WORKLOAD;
        }
        String w = raw.trim().toLowerCase(Locale.ROOT);
        if (!VALID_WORKLOADS.contains(w)) {
            LOGGER.warn("Unknown workload '{}', using default '{}'", raw, DEFAULT_WORKLOAD);
            return DEFAULT_WORKLOAD;
        }
        return w;
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.block.config.file";

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
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroBlockConfig{blockSize=" + blockSize
                + ", workload='" + workload + '\''
                + ", effectiveBlockSize=" + effectiveBlockSize()
                + ", syncInterval=" + syncInterval + '}';
    }
}
