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
    /** Config key for WAL queue max size. */
    public static final String QUEUE_MAX_SIZE_KEY = "wal.queue.max.size";
    /** Config key for WAL segment max age in milliseconds. */
    public static final String SEGMENT_MAX_AGE_MS_KEY = "wal.segment.max.age.ms";
    /** Config key for WAL archive directory. */
    public static final String ARCHIVE_DIR_KEY = "wal.archive.dir";
    /** Config key for WAL archive retention in days. */
    public static final String ARCHIVE_RETENTION_DAYS_KEY = "wal.archive.retention.days";
    /** Config key for WAL archive interval in milliseconds. */
    public static final String ARCHIVE_INTERVAL_MS_KEY = "wal.archive.interval.ms";
    /** Config key for WAL enablement. */
    public static final String ENABLED_KEY = "wal.enabled";
    /** Config key for WAL fsync policy. */
    public static final String FSYNC_POLICY_KEY = "wal.fsync.policy";
    /** Config key for group commit window in milliseconds. */
    public static final String GROUP_WINDOW_MS_KEY = "wal.groupcommit.window.ms";
    /** Config key for maximum group size. */
    public static final String GROUP_MAX_SIZE_KEY = "wal.groupcommit.max.size";
    /** Default WAL directory (relative to working directory). */
    public static final String DEFAULT_DIR = "./wal";
    /** Default segment max size in megabytes. */
    public static final int DEFAULT_SEGMENT_MAX_SIZE_MB = 64;
    /** Default queue max size. */
    public static final long DEFAULT_QUEUE_MAX_SIZE = 100_000;
    /** Default segment max age in milliseconds (5 minutes). */
    public static final long DEFAULT_SEGMENT_MAX_AGE_MS = 300_000;
    /** Default archive directory (relative to WAL directory). */
    public static final String DEFAULT_ARCHIVE_DIR = "archive";
    /** Default archive retention in days. */
    public static final int DEFAULT_ARCHIVE_RETENTION_DAYS = 7;
    /** Default archive interval in milliseconds (1 minute). */
    public static final long DEFAULT_ARCHIVE_INTERVAL_MS = 60_000;
    /** Minimum segment size in bytes (1MB). */
    public static final long MIN_SEGMENT_SIZE_BYTES = 1024 * 1024;
    /** Minimum queue max size. */
    public static final long MIN_QUEUE_MAX_SIZE = 1;
    /** Disabled value for age/retention/interval (<=0). */
    public static final long DISABLED = 0;
    /** Default fsync policy. */
    public static final FsyncPolicy DEFAULT_FSYNC_POLICY = FsyncPolicy.GROUP;
    /** Default group commit window in milliseconds. */
    public static final long DEFAULT_GROUP_WINDOW_MS = 5;
    /** Default maximum group size. */
    public static final int DEFAULT_GROUP_MAX_SIZE = 64;
    /** Default WAL enablement. */
    public static final boolean DEFAULT_ENABLED = false;

    private final Path walDir;
    private final long maxSegmentSizeBytes;
    private final long queueMaxSize;
    private final long maxSegmentAgeMs;
    private final Path archiveDir;
    private final int archiveRetentionDays;
    private final long archiveIntervalMs;
    private final boolean enabled;
    private final FsyncPolicy fsyncPolicy;
    private final long groupWindowMs;
    private final int groupMaxSize;

    private WALConfig(Path walDir, long maxSegmentSizeBytes, long queueMaxSize, 
                     long maxSegmentAgeMs, Path archiveDir, int archiveRetentionDays, long archiveIntervalMs,
                     boolean enabled, FsyncPolicy fsyncPolicy, long groupWindowMs, int groupMaxSize) {
        this.walDir = walDir;
        this.maxSegmentSizeBytes = maxSegmentSizeBytes;
        this.queueMaxSize = queueMaxSize;
        this.maxSegmentAgeMs = maxSegmentAgeMs;
        this.archiveDir = archiveDir;
        this.archiveRetentionDays = archiveRetentionDays;
        this.archiveIntervalMs = archiveIntervalMs;
        this.enabled = enabled;
        this.fsyncPolicy = fsyncPolicy;
        this.groupWindowMs = groupWindowMs;
        this.groupMaxSize = groupMaxSize;
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
     * Returns the maximum queue size.
     *
     * @return max queue size
     */
    public long getQueueMaxSize() {
        return queueMaxSize;
    }

    /**
     * Returns the maximum segment age in milliseconds.
     *
     * @return max segment age in milliseconds, or DISABLED if age rotation is disabled
     */
    public long getMaxSegmentAgeMs() {
        return maxSegmentAgeMs;
    }

    /**
     * Returns the archive directory path.
     *
     * @return the archive directory
     */
    public Path getArchiveDir() {
        return archiveDir;
    }

    /**
     * Returns the archive retention period in days.
     *
     * @return retention period in days, or DISABLED if retention is disabled
     */
    public int getArchiveRetentionDays() {
        return archiveRetentionDays;
    }

    /**
     * Returns the archive interval in milliseconds.
     *
     * @return archive interval in milliseconds, or DISABLED if daemon is disabled
     */
    public long getArchiveIntervalMs() {
        return archiveIntervalMs;
    }

    /**
     * Returns whether WAL is enabled.
     *
     * @return true if WAL is enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the fsync policy.
     *
     * @return the fsync policy
     */
    public FsyncPolicy getFsyncPolicy() {
        return fsyncPolicy;
    }

    /**
     * Returns the group commit window in milliseconds.
     *
     * @return group window in milliseconds
     */
    public long getGroupWindowMs() {
        return groupWindowMs;
    }

    /**
     * Returns the maximum group size.
     *
     * @return maximum group size
     */
    public int getGroupMaxSize() {
        return groupMaxSize;
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

        // Resolve queueMaxSize
        String rawQueueSize = System.getProperty(QUEUE_MAX_SIZE_KEY);
        if (rawQueueSize == null) {
            rawQueueSize = loadRootProps().getProperty(QUEUE_MAX_SIZE_KEY, String.valueOf(DEFAULT_QUEUE_MAX_SIZE));
        }
        long queueSize;
        try {
            queueSize = Long.parseLong(rawQueueSize.trim());
            if (queueSize < MIN_QUEUE_MAX_SIZE) {
                LOGGER.warn("Invalid wal.queue.max.size '{}': must be >= {}, using default {}", 
                        rawQueueSize, MIN_QUEUE_MAX_SIZE, DEFAULT_QUEUE_MAX_SIZE);
                queueSize = DEFAULT_QUEUE_MAX_SIZE;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.queue.max.size format '{}': {}, using default {}", 
                    rawQueueSize, e.getMessage(), DEFAULT_QUEUE_MAX_SIZE);
            queueSize = DEFAULT_QUEUE_MAX_SIZE;
        }

        // Resolve maxSegmentAgeMs
        String rawAge = System.getProperty(SEGMENT_MAX_AGE_MS_KEY);
        if (rawAge == null) {
            rawAge = loadRootProps().getProperty(SEGMENT_MAX_AGE_MS_KEY, String.valueOf(DEFAULT_SEGMENT_MAX_AGE_MS));
        }
        long ageMs;
        try {
            ageMs = Long.parseLong(rawAge.trim());
            if (ageMs < DISABLED) {
                LOGGER.warn("Invalid wal.segment.max.age.ms '{}': must be >= {}, using default {}", 
                        rawAge, DISABLED, DEFAULT_SEGMENT_MAX_AGE_MS);
                ageMs = DEFAULT_SEGMENT_MAX_AGE_MS;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.segment.max.age.ms format '{}': {}, using default {}", 
                    rawAge, e.getMessage(), DEFAULT_SEGMENT_MAX_AGE_MS);
            ageMs = DEFAULT_SEGMENT_MAX_AGE_MS;
        }

        // Resolve archiveDir
        String archiveDirPath = System.getProperty(ARCHIVE_DIR_KEY);
        if (archiveDirPath == null) {
            archiveDirPath = loadRootProps().getProperty(ARCHIVE_DIR_KEY, DEFAULT_ARCHIVE_DIR);
        }
        Path resolvedArchiveDir = walDir.resolve(archiveDirPath).toAbsolutePath().normalize();

        // Resolve archiveRetentionDays
        String rawRetention = System.getProperty(ARCHIVE_RETENTION_DAYS_KEY);
        if (rawRetention == null) {
            rawRetention = loadRootProps().getProperty(ARCHIVE_RETENTION_DAYS_KEY, String.valueOf(DEFAULT_ARCHIVE_RETENTION_DAYS));
        }
        int retentionDays;
        try {
            retentionDays = Integer.parseInt(rawRetention.trim());
            if (retentionDays < DISABLED) {
                LOGGER.warn("Invalid wal.archive.retention.days '{}': must be >= {}, using default {}", 
                        rawRetention, DISABLED, DEFAULT_ARCHIVE_RETENTION_DAYS);
                retentionDays = DEFAULT_ARCHIVE_RETENTION_DAYS;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.archive.retention.days format '{}': {}, using default {}", 
                    rawRetention, e.getMessage(), DEFAULT_ARCHIVE_RETENTION_DAYS);
            retentionDays = DEFAULT_ARCHIVE_RETENTION_DAYS;
        }

        // Resolve archiveIntervalMs
        String rawInterval = System.getProperty(ARCHIVE_INTERVAL_MS_KEY);
        if (rawInterval == null) {
            rawInterval = loadRootProps().getProperty(ARCHIVE_INTERVAL_MS_KEY, String.valueOf(DEFAULT_ARCHIVE_INTERVAL_MS));
        }
        long intervalMs;
        try {
            intervalMs = Long.parseLong(rawInterval.trim());
            if (intervalMs < DISABLED) {
                LOGGER.warn("Invalid wal.archive.interval.ms '{}': must be >= {}, using default {}", 
                        rawInterval, DISABLED, DEFAULT_ARCHIVE_INTERVAL_MS);
                intervalMs = DEFAULT_ARCHIVE_INTERVAL_MS;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.archive.interval.ms format '{}': {}, using default {}", 
                    rawInterval, e.getMessage(), DEFAULT_ARCHIVE_INTERVAL_MS);
            intervalMs = DEFAULT_ARCHIVE_INTERVAL_MS;
        }

        // Resolve fsync policy
        String rawPolicy = System.getProperty(FSYNC_POLICY_KEY);
        if (rawPolicy == null) {
            rawPolicy = loadRootProps().getProperty(FSYNC_POLICY_KEY, DEFAULT_FSYNC_POLICY.name());
        }
        FsyncPolicy policy;
        try {
            policy = FsyncPolicy.valueOf(rawPolicy.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid wal.fsync.policy '{}': {}, using default {}", 
                    rawPolicy, e.getMessage(), DEFAULT_FSYNC_POLICY);
            policy = DEFAULT_FSYNC_POLICY;
        }

        // Resolve group window
        String rawWindow = System.getProperty(GROUP_WINDOW_MS_KEY);
        if (rawWindow == null) {
            rawWindow = loadRootProps().getProperty(GROUP_WINDOW_MS_KEY, String.valueOf(DEFAULT_GROUP_WINDOW_MS));
        }
        long windowMs;
        try {
            windowMs = Long.parseLong(rawWindow.trim());
            if (windowMs < 1) {
                LOGGER.warn("Invalid wal.groupcommit.window.ms '{}': must be >= 1, using default {}", 
                        rawWindow, DEFAULT_GROUP_WINDOW_MS);
                windowMs = DEFAULT_GROUP_WINDOW_MS;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.groupcommit.window.ms format '{}': {}, using default {}", 
                    rawWindow, e.getMessage(), DEFAULT_GROUP_WINDOW_MS);
            windowMs = DEFAULT_GROUP_WINDOW_MS;
        }

        // Resolve group max size
        String rawGroupSize = System.getProperty(GROUP_MAX_SIZE_KEY);
        if (rawGroupSize == null) {
            rawGroupSize = loadRootProps().getProperty(GROUP_MAX_SIZE_KEY, String.valueOf(DEFAULT_GROUP_MAX_SIZE));
        }
        int groupSize;
        try {
            groupSize = Integer.parseInt(rawGroupSize.trim());
            if (groupSize < 1) {
                LOGGER.warn("Invalid wal.groupcommit.max.size '{}': must be >= 1, using default {}", 
                        rawGroupSize, DEFAULT_GROUP_MAX_SIZE);
                groupSize = DEFAULT_GROUP_MAX_SIZE;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid wal.groupcommit.max.size format '{}': {}, using default {}", 
                    rawGroupSize, e.getMessage(), DEFAULT_GROUP_MAX_SIZE);
            groupSize = DEFAULT_GROUP_MAX_SIZE;
        }

        // Resolve enablement
        String rawEnabled = System.getProperty(ENABLED_KEY);
        if (rawEnabled == null) {
            rawEnabled = loadRootProps().getProperty(ENABLED_KEY, String.valueOf(DEFAULT_ENABLED));
        }
        boolean enabled;
        try {
            enabled = Boolean.parseBoolean(rawEnabled.trim());
        } catch (Exception e) {
            LOGGER.warn("Invalid wal.enabled '{}': {}, using default {}", 
                    rawEnabled, e.getMessage(), DEFAULT_ENABLED);
            enabled = DEFAULT_ENABLED;
        }

        return new WALConfig(walDir, sizeBytes, queueSize, ageMs, resolvedArchiveDir, retentionDays, intervalMs,
                           enabled, policy, windowMs, groupSize);
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
        return new WALConfig(walDir, maxSegmentSizeBytes, DEFAULT_QUEUE_MAX_SIZE,
                           DEFAULT_SEGMENT_MAX_AGE_MS, walDir.resolve(DEFAULT_ARCHIVE_DIR), 
                           DEFAULT_ARCHIVE_RETENTION_DAYS, DEFAULT_ARCHIVE_INTERVAL_MS,
                           DEFAULT_ENABLED, DEFAULT_FSYNC_POLICY, DEFAULT_GROUP_WINDOW_MS, DEFAULT_GROUP_MAX_SIZE);
    }

    /**
     * Creates a WALConfig with explicit values (for tests).
     *
     * @param walDir WAL directory
     * @param maxSegmentSizeBytes max segment size in bytes
     * @param queueMaxSize max queue size
     * @return the configuration
     */
    public static WALConfig of(Path walDir, long maxSegmentSizeBytes, long queueMaxSize) {
        if (maxSegmentSizeBytes < MIN_SEGMENT_SIZE_BYTES) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be >= " + MIN_SEGMENT_SIZE_BYTES);
        }
        if (queueMaxSize < MIN_QUEUE_MAX_SIZE) {
            throw new IllegalArgumentException("queueMaxSize must be >= " + MIN_QUEUE_MAX_SIZE);
        }
        return new WALConfig(walDir, maxSegmentSizeBytes, queueMaxSize, 
                           DEFAULT_SEGMENT_MAX_AGE_MS, walDir.resolve(DEFAULT_ARCHIVE_DIR), 
                           DEFAULT_ARCHIVE_RETENTION_DAYS, DEFAULT_ARCHIVE_INTERVAL_MS,
                           DEFAULT_ENABLED, DEFAULT_FSYNC_POLICY, DEFAULT_GROUP_WINDOW_MS, DEFAULT_GROUP_MAX_SIZE);
    }

    /**
     * Creates a WALConfig with explicit values (for tests).
     *
     * @param walDir WAL directory
     * @param maxSegmentSizeBytes max segment size in bytes
     * @param queueMaxSize max queue size
     * @param maxSegmentAgeMs max segment age in milliseconds (DISABLED to disable)
     * @param archiveDir archive directory
     * @param archiveRetentionDays archive retention in days (DISABLED to disable)
     * @param archiveIntervalMs archive interval in milliseconds (DISABLED to disable daemon)
     * @return the configuration
     */
    public static WALConfig of(Path walDir, long maxSegmentSizeBytes, long queueMaxSize,
                               long maxSegmentAgeMs, Path archiveDir, int archiveRetentionDays, long archiveIntervalMs) {
        if (maxSegmentSizeBytes < MIN_SEGMENT_SIZE_BYTES) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be >= " + MIN_SEGMENT_SIZE_BYTES);
        }
        if (queueMaxSize < MIN_QUEUE_MAX_SIZE) {
            throw new IllegalArgumentException("queueMaxSize must be >= " + MIN_QUEUE_MAX_SIZE);
        }
        return new WALConfig(walDir, maxSegmentSizeBytes, queueMaxSize, 
                            maxSegmentAgeMs, archiveDir, archiveRetentionDays, archiveIntervalMs,
                            DEFAULT_ENABLED, DEFAULT_FSYNC_POLICY, DEFAULT_GROUP_WINDOW_MS, DEFAULT_GROUP_MAX_SIZE);
    }

    /**
     * Creates a WALConfig with explicit values (for tests).
     *
     * @param walDir WAL directory
     * @param maxSegmentSizeBytes max segment size in bytes
     * @param queueMaxSize max queue size
     * @param maxSegmentAgeMs max segment age in milliseconds (DISABLED to disable)
     * @param archiveDir archive directory
     * @param archiveRetentionDays archive retention in days (DISABLED to disable)
     * @param archiveIntervalMs archive interval in milliseconds (DISABLED to disable daemon)
     * @param enabled whether WAL is enabled
     * @param fsyncPolicy the fsync policy
     * @param groupWindowMs group commit window in milliseconds
     * @param groupMaxSize maximum group size
     * @return the configuration
     */
    public static WALConfig of(Path walDir, long maxSegmentSizeBytes, long queueMaxSize,
                               long maxSegmentAgeMs, Path archiveDir, int archiveRetentionDays, long archiveIntervalMs,
                               boolean enabled, FsyncPolicy fsyncPolicy, long groupWindowMs, int groupMaxSize) {
        if (maxSegmentSizeBytes < MIN_SEGMENT_SIZE_BYTES) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be >= " + MIN_SEGMENT_SIZE_BYTES);
        }
        if (queueMaxSize < MIN_QUEUE_MAX_SIZE) {
            throw new IllegalArgumentException("queueMaxSize must be >= " + MIN_QUEUE_MAX_SIZE);
        }
        return new WALConfig(walDir, maxSegmentSizeBytes, queueMaxSize, 
                           maxSegmentAgeMs, archiveDir, archiveRetentionDays, archiveIntervalMs,
                           enabled, fsyncPolicy, groupWindowMs, groupMaxSize);
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