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
import java.util.Properties;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * AVRO restore from backup with validation and point-in-time recovery
 * (Prompt 84).
 *
 * <p>Restores AVRO data files from a backup directory created by
 * {@link AvroBackupManager}. Two restore modes are supported:</p>
 * <ul>
 *   <li><b>Full restore</b> — copies every file from the specified backup
 *       directory to the target data directory.</li>
 *   <li><b>Point-in-time restore</b> — finds the latest backup taken at or
 *       before the requested timestamp and restores from it.</li>
 * </ul>
 *
 * <p>Before restoring, each file's CRC32 checksum is verified against the
 * manifest to detect corruption in the backup. A pre-restore validation pass
 * can be enabled/disabled via config.</p>
 *
 * <p>Configuration is resolved per call from a system property, then the root
 * {@code config.properties}, then the code defaults:</p>
 * <ul>
 *   <li>{@code avro.restore.validate.before.restore} (default {@code true})
 *       — verify CRC32 of each file before copying;</li>
 *   <li>{@code avro.restore.point.in.time.enabled} (default {@code true})
 *       — allow timestamp-based restore selection;</li>
 *   <li>{@code avro.restore.config.file} — test-support hook.</li>
 * </ul>
 *
 * <p>The class is thread-safe: it keeps no mutable state.</p>
 *
 * @since Prompt 84
 */
public final class AvroRestoreManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRestoreManager.class);

    /** Config key: verify CRC32 before restoring each file. */
    public static final String VALIDATE_BEFORE_RESTORE_KEY = "avro.restore.validate.before.restore";
    /** Config key: enable point-in-time restore. */
    public static final String POINT_IN_TIME_ENABLED_KEY = "avro.restore.point.in.time.enabled";

    /** Code-level default for pre-restore validation. */
    public static final boolean DEFAULT_VALIDATE_BEFORE_RESTORE = true;
    /** Code-level default for point-in-time restore. */
    /** Code-level default for point-in-time restore. */
    public static final boolean DEFAULT_POINT_IN_TIME_ENABLED = true;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.restore.config.file";

    private AvroRestoreManager() {
    }

    // ─── Configuration ──────────────────────────────────────────────

    /**
     * Resolved configuration for a restore operation.
     */
    public record RestoreConfig(
            boolean validateBeforeRestore,
            boolean pointInTimeEnabled
    ) {
    }

    /**
     * Resolves configuration from system property → config.properties → defaults.
     *
     * @return the resolved configuration
     */
    public static RestoreConfig resolve() {
        Properties props = loadConfig();
        boolean validate = getBoolean(props, VALIDATE_BEFORE_RESTORE_KEY, DEFAULT_VALIDATE_BEFORE_RESTORE);
        boolean pit = getBoolean(props, POINT_IN_TIME_ENABLED_KEY, DEFAULT_POINT_IN_TIME_ENABLED);
        return new RestoreConfig(validate, pit);
    }

    // ─── Full restore ───────────────────────────────────────────────

    /**
     * Restores all files from the specified backup directory to the target.
     *
     * @param backupDir  the backup directory (contains manifest.txt + files)
     * @param targetDir  the target data directory to restore into
     * @return the restore report
     * @throws IOException if the restore fails
     */
    public static RestoreReport restoreFull(File backupDir, File targetDir) throws IOException {
        return restoreFull(backupDir, targetDir, resolve());
    }

    /**
     * Restores all files with explicit configuration.
     *
     * @param backupDir  the backup directory
     * @param targetDir  the target data directory
     * @param config     resolved restore configuration
     * @return the restore report
     * @throws IOException if the restore fails
     */
    public static RestoreReport restoreFull(File backupDir, File targetDir, RestoreConfig config)
            throws IOException {
        Instant startedAt = Instant.now();
        long t0 = System.nanoTime();
        validateDir(backupDir, "backup");
        validateDir(targetDir, "target");

        AvroBackupManager.BackupManifest manifest = readManifest(backupDir);
        LOGGER.info("Avro restore: full restore from {} -> {} (backup type={}, {} files)",
                backupDir.getPath(), targetDir.getPath(),
                manifest.backupType(), manifest.files().size());

        if (config.validateBeforeRestore()) {
            validateManifest(backupDir, manifest);
        }

        List<RestoredFile> restored = new ArrayList<>();
        int filesFailed = 0;

        for (AvroBackupManager.BackedUpFile entry : manifest.files()) {
            if (!entry.success()) {
                LOGGER.warn("Avro restore: skipping failed backup entry {}", entry.path());
                continue;
            }
            Path source = backupDir.toPath().resolve(entry.path());
            Path target = targetDir.toPath().resolve(entry.path());
            Files.createDirectories(target.getParent());
            try {
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                restored.add(new RestoredFile(entry.path(), entry.sizeBytes(), entry.crc32(), true));
            } catch (IOException e) {
                LOGGER.error("Avro restore: failed to restore {}: {}", entry.path(), e.getMessage());
                restored.add(new RestoredFile(entry.path(), entry.sizeBytes(), -1, false));
                filesFailed++;
            }
        }

        long durationNanos = System.nanoTime() - t0;
        Instant completedAt = Instant.now();
        long totalBytes = restored.stream().filter(RestoredFile::success).mapToLong(RestoredFile::sizeBytes).sum();

        LOGGER.info("Avro restore: full restore complete — {} files, {} bytes, {} failures, {} ms",
                restored.size(), totalBytes, filesFailed, durationNanos / 1_000_000);

        return new RestoreReport("full", targetDir, restored.size(),
                restored.stream().filter(RestoredFile::success).count(), filesFailed,
                totalBytes, startedAt, completedAt, durationNanos);
    }

    // ─── Point-in-time restore ──────────────────────────────────────

    /**
     * Restores from the latest backup taken at or before the given timestamp.
     *
     * @param backupRoot the root backup directory containing timestamped backups
     * @param targetDir  the target data directory
     * @param pointInTime the cutoff timestamp
     * @return the restore report
     * @throws IOException if the restore fails
     */
    public static RestoreReport restorePointInTime(File backupRoot, File targetDir,
                                                    Instant pointInTime) throws IOException {
        return restorePointInTime(backupRoot, targetDir, pointInTime, resolve());
    }

    /**
     * Restores from the latest backup taken at or before the given timestamp.
     *
     * @param backupRoot the root backup directory containing timestamped backups
     * @param targetDir  the target data directory
     * @param pointInTime the cutoff timestamp
     * @param config     resolved restore configuration
     * @return the restore report
     * @throws IOException if the restore fails
     */
    public static RestoreReport restorePointInTime(File backupRoot, File targetDir,
                                                    Instant pointInTime, RestoreConfig config)
            throws IOException {
        if (!config.pointInTimeEnabled()) {
            throw new IOException("Point-in-time restore is disabled (avro.restore.point.in.time.enabled=false)");
        }
        validateDir(backupRoot, "backup root");
        validateDir(targetDir, "target");

        File bestBackup = findBestBackup(backupRoot, pointInTime);
        if (bestBackup == null) {
            throw new IOException("No backup found at or before " + pointInTime);
        }
        LOGGER.info("Avro restore: point-in-time restore from {} (cutoff={})",
                bestBackup.getName(), pointInTime);

        return restoreFull(bestBackup, targetDir, config);
    }

    // ─── Backup listing ─────────────────────────────────────────────

    /**
     * Lists all available backups in the root directory, sorted by timestamp.
     *
     * @param backupRoot the root backup directory
     * @return list of backup directories with their manifests
     * @throws IOException if reading fails
     */
    public static List<AvailableBackup> listBackups(File backupRoot) throws IOException {
        validateDir(backupRoot, "backup root");
        List<AvailableBackup> result = new ArrayList<>();
        File[] children = backupRoot.listFiles();
        if (children == null) {
            return result;
        }
        for (File child : children) {
            if (!child.isDirectory()) continue;
            File manifestFile = new File(child, "manifest.txt");
            if (!manifestFile.isFile()) continue;
            try {
                AvroBackupManager.BackupManifest manifest = AvroBackupManager.readManifest(manifestFile);
                result.add(new AvailableBackup(child, manifest));
            } catch (Exception e) {
                LOGGER.warn("Avro restore: unreadable manifest in {}: {}", child.getName(), e.getMessage());
            }
        }
        Collections.sort(result, (a, b) -> a.manifest().startedAt().compareTo(b.manifest().startedAt()));
        return result;
    }

    // ─── Internal helpers ───────────────────────────────────────────

    private static void validateDir(File dir, String label) {
        if (dir == null) {
            throw new IllegalArgumentException(label + " directory must not be null");
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException(label + " directory does not exist or is not a directory: "
                    + dir.getPath());
        }
    }

    private static AvroBackupManager.BackupManifest readManifest(File backupDir) throws IOException {
        File manifestFile = new File(backupDir, "manifest.txt");
        if (!manifestFile.isFile()) {
            throw new IOException("No manifest.txt found in " + backupDir.getPath());
        }
        return AvroBackupManager.readManifest(manifestFile);
    }

    private static void validateManifest(File backupDir, AvroBackupManager.BackupManifest manifest)
            throws IOException {
        LOGGER.info("Avro restore: validating {} files in backup...", manifest.files().size());
        int invalid = 0;
        for (AvroBackupManager.BackedUpFile entry : manifest.files()) {
            if (!entry.success()) continue;
            Path filePath = backupDir.toPath().resolve(entry.path());
            if (!Files.exists(filePath)) {
                LOGGER.error("Avro restore: backup file missing: {}", entry.path());
                invalid++;
                continue;
            }
            if (entry.crc32() >= 0) {
                long actualCrc = computeCrc(filePath);
                if (actualCrc != entry.crc32()) {
                    LOGGER.error("Avro restore: CRC mismatch for {} (expected={}, actual={})",
                            entry.path(),
                            Long.toHexString(entry.crc32()),
                            Long.toHexString(actualCrc));
                    invalid++;
                }
            }
        }
        if (invalid > 0) {
            throw new IOException("Backup validation failed: " + invalid + " file(s) invalid");
        }
        LOGGER.info("Avro restore: validation passed — all files OK");
    }

    private static long computeCrc(Path file) throws IOException {
        CRC32 crc = new CRC32();
        byte[] buf = new byte[65536];
        try (InputStream in = Files.newInputStream(file)) {
            int n;
            while ((n = in.read(buf)) != -1) {
                crc.update(buf, 0, n);
            }
        }
        return crc.getValue();
    }

    private static File findBestBackup(File backupRoot, Instant pointInTime) {
        File[] children = backupRoot.listFiles();
        if (children == null) return null;

        File best = null;
        for (File child : children) {
            if (!child.isDirectory()) continue;
            File manifestFile = new File(child, "manifest.txt");
            if (!manifestFile.isFile()) continue;
            try {
                AvroBackupManager.BackupManifest manifest = AvroBackupManager.readManifest(manifestFile);
                if (!manifest.startedAt().isAfter(pointInTime)) {
                    if (best == null || manifest.startedAt().isAfter(
                            AvroBackupManager.readManifest(new File(best, "manifest.txt")).startedAt())) {
                        best = child;
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Avro restore: unreadable manifest in {}: {}", child.getName(), e.getMessage());
            }
        }
        return best;
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
                LOGGER.warn("Avro restore: failed to load {}: {}", configPath, e.getMessage());
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

    private static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        String raw = getString(props, key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(raw);
    }

    // ─── Public records ─────────────────────────────────────────────

    /**
     * Represents a single restored file.
     *
     * @param path       relative path within the target directory
     * @param sizeBytes  file size in bytes
     * @param crc32      CRC32 checksum from the manifest
     * @param success    whether the restore succeeded
     */
    public record RestoredFile(String path, long sizeBytes, long crc32, boolean success) {
    }

    /**
     * Report produced by a restore operation.
     *
     * @param restoreType  "full" or "point_in_time"
     * @param targetDir    the target directory
     * @param totalFiles   total files processed
     * @param filesRestored files restored successfully
     * @param filesFailed  files that failed to restore
     * @param totalBytes   total bytes restored
     * @param startedAt    when the restore started
     * @param completedAt  when the restore completed
     * @param durationNanos wall-clock duration in nanoseconds
     */
    public record RestoreReport(
            String restoreType,
            File targetDir,
            long totalFiles,
            long filesRestored,
            int filesFailed,
            long totalBytes,
            Instant startedAt,
            Instant completedAt,
            long durationNanos
    ) {
        /** Whether every file was restored successfully. */
        public boolean allSuccessful() {
            return filesFailed == 0;
        }
    }

    /**
     * Represents an available backup that can be restored from.
     *
     * @param directory the backup directory
     * @param manifest  the parsed manifest
     */
    public record AvailableBackup(File directory, AvroBackupManager.BackupManifest manifest) {
    }
}
