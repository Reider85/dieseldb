package diesel.wal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diesel.ErrorMessages;

/**
 * Configuration for WAL (Write-Ahead Log) (prompt4.md step 12, R3-003 step 2/5).
 *
 * <p>Resolution order for properties:
 * <ol>
 *   <li>System property (e.g., -Ddiesel.wal.dir=...)</li>
 *   <li>config.properties file (in working directory)</li>
 *   <li>Default values (see constants)</li>
 * </ol>
 *
 * <p>Package-private: accessed via WALManager constructor.
 */
public final class WALConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALConfig.class);

    /** Config key for WAL directory. */
    public static final String DIR_KEY = "wal.dir";
    /** Config key for WAL segment max size in megabytes. */
    public static final String SEGMENT_MAX_SIZE_MB_KEY = "wal.segment.max.size.mb";
    /** Default WAL directory (relative to working directory). */
    public static final String DEFAULT_DIR = "./wal";
    /** Default segment max size in megabytes. */
    public static final int DEFAULT_SEGMENT_MAX_SIZE_MB = 64;
    /** Minimum segment size in bytes (1MB). */
    public static final long MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;

    private final Path walDir;
    private final long maxSegmentSizeBytes;

    private WALConfig(Path walDir, long maxSegmentSizeBytes) {
        this.walDir = walDir;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
    }

    /**
     * Returns the WAL directory path.
     *
     * @return the WAL directory
     */
    public Path getWalDir() {
        return walDir;
    }

    /**
     * Returns the maximum segment size in bytes.
     *
     * @return max segment size in bytes
     */
    public long getMaxSegmentSizeBytes() {
        return maxSegmentSizeBytes;
    }

    /**
     * Creates a WALConfig from system properties and config.properties.
     *
     * @return the resolved configuration
     */
    public static WALConfig fromConfig() {
        // Resolve wal.dir
        String dirPath = System.getProperty(DIR_KEY);
        if (dirPath == null) {
            dirPath = loadRootProps().getProperty(DIR_KEY, DEFAULT_DIR);
        }
        Path walDir = Paths.get(dirPath).toAbsolutePath().normalize();

        // Resolve maxSegmentSizeBytes
        String rawSize = System.getProperty(SEGMENT_MAX_SIZE_MB_KEY);
        if (rawSize == null) {
            rawSize = loadRootProps().getProperty(SEGMENT_MAX_SIZE_MB_KEY, String.valueOf(DEFAULT_SEGMENT_MAX_SIZE_MB));
        }
        long sizeBytes;
        try {
            long sizeMB = Long.parseLong(rawSize.trim());
            if (sizeMB < 1) {
                LOGGER.warn("Invalid wal.segment.max.size.mb '{}': must be >= 1, using default {}", 
                        rawSize, DEFAULT_SEGMENT_MAX_SIZE_MB);
                sizeBytes = DEFAULT_SEGMENT_MAX_SIZE_MB * 1024L * 1024L;
            } else {
                sizeBytes = sizeMB * 1024L * 1024L;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.segment.max.size.mb format '{}': {}, using default {}", 
                    rawSize, e.getMessage(), DEFAULT_SEGMENT_MAX_SIZE_MB);
            sizeBytes = DEFAULT_SEGMENT_MAX_SIZE_MB * 1024L * 1024L;
        }

        return new WALConfig(walDir, sizeBytes);
    }

    /**
     * Creates a WALConfig with explicit values (for tests).
     *
     * @param walDir WAL directory
     * @param maxSegmentSizeBytes max segment size in bytes
     * @return the configuration
     */
    public static WALConfig of(Path walDir, long maxSegmentSizeBytes) {
        if (maxSegmentSizeBytes < MIN_SEGMENT_SIZE_BYTES) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be >= " + MIN_SEGMENT_SIZE_BYTES);
        }
        return new WALConfig(walDir, maxSegmentSizeBytes);
    }

    /**
     * Loads root properties from config.properties (same pattern as PageConfig).
     * Package-private: shared with other config classes.
     */
    static Properties loadRootProps() {
        Properties props = new Properties();
        try {
            java.io.File configFile = new File(diesel.ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                try (java.io.FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (IOException ignored) {
            LOGGER.debug("Config error, using defaults: {}", ignored.getMessage());
        }
        return props;
    }
}