package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * AVRO online backup and incremental backup (Prompt 84).
 *
 * <p>Provides non-blocking online backup of AVRO data directories without
 * locking writes: a snapshot is taken via file-level copy, so concurrent
 * inserts to the source directory proceed unimpeded while the backup runs.</p>
 *
 * <p>Two backup modes are supported:</p>
 * <ul>
 *   <li><b>Full backup</b> — every {@code .avro} file in the source directory
 *       is copied to a timestamped backup subdirectory with an optional CRC32
 *       integrity check.</li>
 *   <li><b>Incremental backup</b> — only files whose size or last-modified
 *       timestamp has changed since the previous backup are copied. A
 *       {@code manifest.txt} sidecar tracks which files were included.</li>
 * </ul>
 *
 * <p>Configuration is resolved per call from a system property, then the root
 * {@code config.properties}, then the code defaults:</p>
 * <ul>
 *   <li>{@code avro.backup.dir} (default {@code data/avro-backups})</li>
 *   <li>{@code avro.backup.retention.days} (default {@code 30})</li>
 *   <li>{@code avro.backup.incremental.enabled} (default {@code true})</li>
 *   <li>{@code avro.backup.validate.on.create} (default {@code true})</li>
 *   <li>{@code avro.backup.config.file} — test-support hook</li>
 * </ul>
 *
 * <p>The class is thread-safe: it keeps no mutable state.</p>
 *
 * @since Prompt 84
 */
public final class AvroBackupManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBackupManager.class);

    /** Config key: root backup directory. */
    public static final String BACKUP_DIR_KEY = "avro.backup.dir";
    /** Config key: retention period in days. */
    public static final String RETENTION_DAYS_KEY = "avro.backup.retention.days";
    /** Config key: enable incremental backup. */
    public static final String INCREMENTAL_ENABLED_KEY = "avro.backup.incremental.enabled";
    /** Config key: validate CRC32 after copy. */
    public static final String VALIDATE_ON_CREATE_KEY = "avro.backup.validate.on.create";

    /** Code-level default for backup directory. */
    public static final String DEFAULT_BACKUP_DIR = "data/avro-backups";
    /** Code-level default for retention days. */
    public static final int DEFAULT_RETENTION_DAYS = 30;
    /** Code-level default for incremental backup. */
    public static final boolean DEFAULT_INCREMENTAL_ENABLED = true;
    /** Code-level default for validate-on-create. */
    public static final boolean DEFAULT_VALIDATE_ON_CREATE = true;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.backup.config.file";

    private AvroBackupManager() {
    }

    // ─── Configuration ──────────────────────────────────────────────

    /**
     * Resolved configuration for a backup operation.
     */
    public record BackupConfig(
            String backupDir,
            int retentionDays,
            boolean incrementalEnabled,
            boolean validateOnCreate
    ) {
    }

    /**
     * Resolves configuration from system property → config.properties → defaults.
     *
     * @return the resolved configuration
     */
    public static BackupConfig resolve() {
        Properties props = loadConfig();
        String backupDir = getString(props, BACKUP_DIR_KEY, DEFAULT_BACKUP_DIR);
        int retentionDays = getInt(props, RETENTION_DAYS_KEY, DEFAULT_RETENTION_DAYS);
        boolean incrementalEnabled = getBoolean(props, INCREMENTAL_ENABLED_KEY, DEFAULT_INCREMENTAL_ENABLED);
        boolean validateOnCreate = getBoolean(props, VALIDATE_ON_CREATE_KEY, DEFAULT_VALIDATE_ON_CREATE);
        return new BackupConfig(backupDir, retentionDays, incrementalEnabled, validateOnCreate);
    }

    // ─── Full backup ────────────────────────────────────────────────

    /**
     * Creates a full backup of the source directory.
     *
     * @param sourceDir the AVRO data directory to back up
     * @return the backup report
     * @throws IOException if the backup fails
     */
    public static BackupReport backupFull(File sourceDir) throws IOException {
        return backupFull(sourceDir, resolve());
    }

    /**
     * Creates a full backup with explicit configuration.
     *
     * @param sourceDir the AVRO data directory to back up
     * @param config    resolved backup configuration
     * @return the backup report
     * @throws IOException if the backup fails
     */
    public static BackupReport backupFull(File sourceDir, BackupConfig config) throws IOException {
        Instant startedAt = Instant.now();
        long t0 = System.nanoTime();
        validateSourceDir(sourceDir);

        String backupName = formatTimestamp(startedAt);
        File backupDir = resolveBackupDir(config, backupName);
        Files.createDirectories(backupDir.toPath());

        LOGGER.info("Avro backup: full backup of {} -> {}", sourceDir.getPath(), backupDir.getPath());

        List<AvroFileEntry> entries = collectAvroFiles(sourceDir);
        List<BackedUpFile> backed = new ArrayList<>();
        int filesFailed = 0;

        for (AvroFileEntry entry : entries) {
            Path relativePath = sourceDir.toPath().relativize(entry.path().toPath());
            Path target = backupDir.toPath().resolve(relativePath);
            Files.createDirectories(target.getParent());
            try {
                long crc = copyWithCrc(entry.path(), target, config.validateOnCreate());
                backed.add(new BackedUpFile(relativePath.toString(), entry.size(), entry.lastModifiedMs(), crc, true));
            } catch (IOException e) {
                LOGGER.error("Avro backup: failed to copy {}: {}", entry.path().getName(), e.getMessage());
                backed.add(new BackedUpFile(relativePath.toString(), entry.size(), entry.lastModifiedMs(), -1, false));
                filesFailed++;
            }
        }

        long totalBytes = backed.stream().filter(BackedUpFile::success).mapToLong(BackedUpFile::sizeBytes).sum();
        long durationNanos = System.nanoTime() - t0;
        Instant completedAt = Instant.now();

        BackupManifest manifest = new BackupManifest("full", startedAt, completedAt,
                sourceDir.getAbsolutePath(), backupDir.getAbsolutePath(),
                backed, totalBytes, durationNanos, filesFailed);
        writeManifest(backupDir, manifest);

        LOGGER.info("Avro backup: full backup complete — {} files, {} bytes, {} failures, {} ms",
                backed.size(), totalBytes, filesFailed, durationNanos / 1_000_000);

        return new BackupReport(manifest, backupDir, backed.size(),
                backed.stream().filter(BackedUpFile::success).count(), filesFailed,
                totalBytes, startedAt, completedAt, durationNanos);
    }

    // ─── Incremental backup ─────────────────────────────────────────

    /**
     * Creates an incremental backup: only files changed since the last backup.
     *
     * @param sourceDir the AVRO data directory to back up
     * @return the backup report
     * @throws IOException if the backup fails
     */
    public static BackupReport backupIncremental(File sourceDir) throws IOException {
        return backupIncremental(sourceDir, resolve());
    }

    /**
     * Creates an incremental backup with explicit configuration.
     *
     * @param sourceDir the AVRO data directory to back up
     * @param config    resolved backup configuration
     * @return the backup report
     * @throws IOException if the backup fails
     */
    public static BackupReport backupIncremental(File sourceDir, BackupConfig config) throws IOException {
        Instant startedAt = Instant.now();
        long t0 = System.nanoTime();
        validateSourceDir(sourceDir);

        if (!config.incrementalEnabled()) {
            LOGGER.info("Avro backup: incremental mode disabled; falling back to full backup");
            return backupFull(sourceDir, config);
        }

        BackupManifest previousManifest = findLatestManifest(config);
        Map<String, BackedUpFile> previousFiles = previousManifest != null
                ? indexFiles(previousManifest)
                : Collections.emptyMap();

        String backupName = formatTimestamp(startedAt);
        File backupDir = resolveBackupDir(config, backupName);
        Files.createDirectories(backupDir.toPath());

        LOGGER.info("Avro backup: incremental backup of {} -> {} (previous: {})",
                sourceDir.getPath(), backupDir.getPath(),
                previousManifest != null ? previousManifest.startedAt() : "none");

        List<AvroFileEntry> entries = collectAvroFiles(sourceDir);
        List<BackedUpFile> backed = new ArrayList<>();
        int filesFailed = 0;

        for (AvroFileEntry entry : entries) {
            Path relativePath = sourceDir.toPath().relativize(entry.path().toPath());
            String relKey = relativePath.toString();
            BackedUpFile prev = previousFiles.get(relKey);

            if (prev != null && prev.sizeBytes() == entry.size()
                    && prev.lastModifiedMs() == entry.lastModifiedMs()) {
                continue;
            }

            Path target = backupDir.toPath().resolve(relativePath);
            Files.createDirectories(target.getParent());
            try {
                long crc = copyWithCrc(entry.path(), target, config.validateOnCreate());
                backed.add(new BackedUpFile(relKey, entry.size(), entry.lastModifiedMs(), crc, true));
            } catch (IOException e) {
                LOGGER.error("Avro backup: failed to copy {}: {}", entry.path().getName(), e.getMessage());
                backed.add(new BackedUpFile(relKey, entry.size(), entry.lastModifiedMs(), -1, false));
                filesFailed++;
            }
        }

        long totalBytes = backed.stream().filter(BackedUpFile::success).mapToLong(BackedUpFile::sizeBytes).sum();
        long durationNanos = System.nanoTime() - t0;
        Instant completedAt = Instant.now();

        BackupManifest manifest = new BackupManifest("incremental", startedAt, completedAt,
                sourceDir.getAbsolutePath(), backupDir.getAbsolutePath(),
                backed, totalBytes, durationNanos, filesFailed);
        writeManifest(backupDir, manifest);

        LOGGER.info("Avro backup: incremental backup complete — {} files changed, {} bytes, {} failures, {} ms",
                backed.size(), totalBytes, filesFailed, durationNanos / 1_000_000);

        return new BackupReport(manifest, backupDir, backed.size(),
                backed.stream().filter(BackedUpFile::success).count(), filesFailed,
                totalBytes, startedAt, completedAt, durationNanos);
    }

    // ─── Backup pruning ─────────────────────────────────────────────

    /**
     * Removes backup directories older than the configured retention period.
     *
     * @return number of backup directories pruned
     * @throws IOException if deletion fails
     */
    public static int pruneOldBackups() throws IOException {
        return pruneOldBackups(resolve());
    }

    /**
     * Removes backup directories older than the configured retention period.
     *
     * @param config resolved backup configuration
     * @return number of backup directories pruned
     * @throws IOException if deletion fails
     */
    public static int pruneOldBackups(BackupConfig config) throws IOException {
        File root = new File(config.backupDir());
        if (!root.isDirectory()) {
            return 0;
        }
        Instant cutoff = Instant.now().minusSeconds((long) config.retentionDays() * 86400);
        int pruned = 0;
        File[] children = root.listFiles();
        if (children == null) {
            return 0;
        }
        for (File child : children) {
            if (!child.isDirectory()) {
                continue;
            }
            Instant dirTime = Instant.ofEpochMilli(child.lastModified());
            if (dirTime.isBefore(cutoff)) {
                deleteRecursively(child.toPath());
                LOGGER.info("Avro backup: pruned old backup {}", child.getName());
                pruned++;
            }
        }
        return pruned;
    }

    // ─── Manifest operations ────────────────────────────────────────

    /**
     * Reads the latest backup manifest from the backup directory.
     *
     * @param config resolved backup configuration
     * @return the latest manifest, or {@code null} if no backups exist
     * @throws IOException if reading fails
     */
    public static BackupManifest readLatestManifest(BackupConfig config) throws IOException {
        return findLatestManifest(config);
    }

    // ─── Internal helpers ───────────────────────────────────────────

    private static void validateSourceDir(File sourceDir) {
        if (sourceDir == null) {
            throw new IllegalArgumentException("Source directory must not be null");
        }
        if (!sourceDir.isDirectory()) {
            throw new IllegalArgumentException("Source directory does not exist or is not a directory: "
                    + sourceDir.getPath());
        }
    }

    private static File resolveBackupDir(BackupConfig config, String backupName) {
        return new File(config.backupDir(), backupName);
    }

    private static String formatTimestamp(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")
                .withZone(ZoneOffset.UTC)
                .format(instant);
    }

    private static List<AvroFileEntry> collectAvroFiles(File sourceDir) throws IOException {
        List<AvroFileEntry> result = new ArrayList<>();
        Files.walkFileTree(sourceDir.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (name.endsWith(".avro") && !name.endsWith(".tmp") && !name.endsWith(".bak")) {
                    result.add(new AvroFileEntry(file.toFile(), attrs.size(),
                            attrs.lastModifiedTime().toMillis()));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        Collections.sort(result);
        return result;
    }

    private static long copyWithCrc(File source, Path target, boolean validate) throws IOException {
        CRC32 crc = new CRC32();
        byte[] buf = new byte[65536];
        try (InputStream in = Files.newInputStream(source.toPath());
             OutputStream out = Files.newOutputStream(target)) {
            int n;
            while ((n = in.read(buf)) != -1) {
                crc.update(buf, 0, n);
                out.write(buf, 0, n);
            }
        }
        if (validate) {
            long computedCrc = crc.getValue();
            CRC32 verify = new CRC32();
            try (InputStream in = Files.newInputStream(target)) {
                int n;
                while ((n = in.read(buf)) != -1) {
                    verify.update(buf, 0, n);
                }
            }
            if (verify.getValue() != computedCrc) {
                Files.delete(target);
                throw new IOException("CRC32 mismatch after copy: " + source.getName());
            }
        }
        return crc.getValue();
    }

    private static BackupManifest findLatestManifest(BackupConfig config) throws IOException {
        File root = new File(config.backupDir());
        if (!root.isDirectory()) {
            return null;
        }
        File[] children = root.listFiles();
        if (children == null) {
            return null;
        }
        File latest = null;
        for (File child : children) {
            if (child.isDirectory()) {
                File manifest = new File(child, "manifest.txt");
                if (manifest.isFile()) {
                    if (latest == null || child.lastModified() > latest.lastModified()) {
                        latest = child;
                    }
                }
            }
        }
        if (latest == null) {
            return null;
        }
        return readManifest(new File(latest, "manifest.txt"));
    }

    private static void writeManifest(File backupDir, BackupManifest manifest) throws IOException {
        Path manifestPath = backupDir.toPath().resolve("manifest.txt");
        StringBuilder sb = new StringBuilder();
        sb.append("backup_type=").append(manifest.backupType()).append('\n');
        sb.append("source_dir=").append(manifest.sourceDir()).append('\n');
        sb.append("backup_dir=").append(manifest.backupDir()).append('\n');
        sb.append("started_at=").append(manifest.startedAt()).append('\n');
        sb.append("completed_at=").append(manifest.completedAt()).append('\n');
        sb.append("total_files=").append(manifest.totalFiles()).append('\n');
        sb.append("total_bytes=").append(manifest.totalBytes()).append('\n');
        sb.append("duration_nanos=").append(manifest.durationNanos()).append('\n');
        sb.append("files_failed=").append(manifest.filesFailed()).append('\n');
        for (BackedUpFile f : manifest.files()) {
            sb.append("file=").append(f.path())
                    .append('|').append(f.sizeBytes())
                    .append('|').append(f.lastModifiedMs())
                    .append('|').append(f.crc32())
                    .append('|').append(f.success())
                    .append('\n');
        }
        Files.writeString(manifestPath, sb.toString());
    }

    static BackupManifest readManifest(File manifestFile) throws IOException {
        String content = Files.readString(manifestFile.toPath());
        String[] lines = content.split("\n");
        String type = "", source = "", backup = "", started = "", completed = "";
        long totalBytes = 0, durationNanos = 0;
        int totalFiles = 0, filesFailed = 0;
        List<BackedUpFile> files = new ArrayList<>();

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;
            if (line.startsWith("backup_type=")) {
                type = line.substring("backup_type=".length());
            } else if (line.startsWith("source_dir=")) {
                source = line.substring("source_dir=".length());
            } else if (line.startsWith("backup_dir=")) {
                backup = line.substring("backup_dir=".length());
            } else if (line.startsWith("started_at=")) {
                started = line.substring("started_at=".length());
            } else if (line.startsWith("completed_at=")) {
                completed = line.substring("completed_at=".length());
            } else if (line.startsWith("total_files=")) {
                totalFiles = Integer.parseInt(line.substring("total_files=".length()));
            } else if (line.startsWith("total_bytes=")) {
                totalBytes = Long.parseLong(line.substring("total_bytes=".length()));
            } else if (line.startsWith("duration_nanos=")) {
                durationNanos = Long.parseLong(line.substring("duration_nanos=".length()));
            } else if (line.startsWith("files_failed=")) {
                filesFailed = Integer.parseInt(line.substring("files_failed=".length()));
            } else if (line.startsWith("file=")) {
                String[] parts = line.substring("file=".length()).split("\\|", -1);
                if (parts.length >= 5) {
                    files.add(new BackedUpFile(
                            parts[0],
                            Long.parseLong(parts[1]),
                            Long.parseLong(parts[2]),
                            Long.parseLong(parts[3]),
                            Boolean.parseBoolean(parts[4])));
                }
            }
        }
        return new BackupManifest(type,
                Instant.parse(started),
                Instant.parse(completed),
                source, backup, files, totalBytes, durationNanos, filesFailed);
    }

    private static Map<String, BackedUpFile> indexFiles(BackupManifest manifest) {
        Map<String, BackedUpFile> map = new TreeMap<>();
        for (BackedUpFile f : manifest.files()) {
            map.put(f.path(), f);
        }
        return map;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    // ─── Config helpers ─────────────────────────────────────────────

    private static Properties loadConfig() {
        String overrideFile = System.getProperty(CONFIG_FILE_KEY);
        Properties props = new Properties();
        Path configPath = overrideFile != null
                ? Path.of(overrideFile)
                : Path.of(System.getProperty("user.dir"), "config.properties");
        if (Files.exists(configPath)) {
            try (var in = Files.newInputStream(configPath)) {
                props.load(in);
            } catch (IOException e) {
                LOGGER.warn("Avro backup: failed to load {}: {}", configPath, e.getMessage());
            }
        }
        return props;
    }

    private static String getString(Properties props, String key, String defaultValue) {
        String sysVal = System.getProperty(key);
        if (sysVal != null) return sysVal;
        String propVal = props.getProperty(key);
        return propVal != null ? propVal : defaultValue;
    }

    private static int getInt(Properties props, String key, int defaultValue) {
        String raw = getString(props, key, String.valueOf(defaultValue));
        try {
            int val = Integer.parseInt(raw);
            if (val < 0) {
                LOGGER.warn("Avro backup: negative value for {} ({}), using default {}", key, raw, defaultValue);
                return defaultValue;
            }
            return val;
        } catch (NumberFormatException e) {
            LOGGER.warn("Avro backup: invalid value for {} ({}), using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String raw = getString(props, key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(raw);
    }

    // ─── Public records ─────────────────────────────────────────────

    /**
     * Represents a single AVRO file collected from the source directory.
     */
    public record AvroFileEntry(File path, long size, long lastModifiedMs)
            implements Comparable<AvroFileEntry> {
        @Override
        public int compareTo(AvroFileEntry o) {
            return path().compareTo(o.path());
        }
    }

    /**
     * Represents a single backed-up file in a backup manifest.
     *
     * @param path           relative path within the backup
     * @param sizeBytes      original file size in bytes
     * @param lastModifiedMs source file last-modified timestamp (0 for full backups)
     * @param crc32          CRC32 checksum of the copied file, or -1 on failure
     * @param success        whether the file was copied successfully
     */
    public record BackedUpFile(String path, long sizeBytes, long lastModifiedMs,
                               long crc32, boolean success) {

        /** Convenience constructor for full backups (lastModifiedMs = 0). */
        public BackedUpFile(String path, long sizeBytes, long crc32, boolean success) {
            this(path, sizeBytes, 0, crc32, success);
        }
    }

    /**
     * Backup manifest recording all metadata about a backup operation.
     *
     * @param backupType    "full" or "incremental"
     * @param startedAt     when the backup started
     * @param completedAt   when the backup completed
     * @param sourceDir     the source directory path
     * @param backupDir     the backup directory path
     * @param files         list of backed-up files
     * @param totalBytes    total bytes copied
     * @param durationNanos wall-clock duration in nanoseconds
     * @param filesFailed   number of files that failed to copy
     */
    public record BackupManifest(
            String backupType,
            Instant startedAt,
            Instant completedAt,
            String sourceDir,
            String backupDir,
            List<BackedUpFile> files,
            long totalBytes,
            long durationNanos,
            int filesFailed
    ) {
        /** Whether every file was copied successfully. */
        public boolean allSuccessful() {
            return filesFailed == 0;
        }

        /** Number of files processed. */
        public int totalFiles() {
            return files.size();
        }
    }

    /**
     * Report produced by a backup operation.
     *
     * @param manifest        the written manifest
     * @param backupDirectory the created backup directory
     * @param totalFiles      total files processed
     * @param filesCopied     files copied successfully
     * @param filesFailed     files that failed to copy
     * @param totalBytes      total bytes copied
     * @param startedAt       when the backup started
     * @param completedAt     when the backup completed
     * @param durationNanos   wall-clock duration in nanoseconds
     */
    public record BackupReport(
            BackupManifest manifest,
            File backupDirectory,
            long totalFiles,
            long filesCopied,
            int filesFailed,
            long totalBytes,
            Instant startedAt,
            Instant completedAt,
            long durationNanos
    ) {
        /** Whether every file was copied successfully. */
        public boolean allSuccessful() {
            return filesFailed == 0;
        }
    }
}
