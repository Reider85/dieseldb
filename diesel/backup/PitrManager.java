package diesel.backup;

import diesel.PitrSupport;
import diesel.recovery.PitrResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Point-in-time recovery orchestrator (prompt 4 #43 / R3-011 step 4/4).
 *
 * <p>Runs the full PITR pipeline over an existing backup chain:
 * <ol>
 *   <li><b>Physical chain restore</b> — {@link IncrementalBackup#restoreChain}
 *       copies the full backup and every incremental into the target directory
 *       (the base state at backup time);</li>
 *   <li><b>WAL staging</b> — the source WAL directory is copied read-only into
 *       a sibling staging directory so the live log is never mutated during
 *       replay;</li>
 *   <li><b>Logical replay</b> — {@link PitrSupport#recoverToTimestamp} opens the
 *       restored database, replays the staged WAL up to the target timestamp
 *       and aborts every transaction that was still active at that instant.</li>
 * </ol>
 *
 * <p>The last backup LSN (the replay floor) is read from the newest
 * incremental's {@code maxLsn} manifest metadata, falling back to the full
 * backup's manifest, then to {@code 0} (replay the whole staged log). Legacy
 * full backups written before {@code maxLsn} existed replay from the start of
 * the log — safe, just slower.
 */
public final class PitrManager {

    /** Manifest metadata key carrying the backup high-water LSN (incremental + full). */
    static final String META_MAX_LSN = "maxLsn";

    private PitrManager() {
        // utility class
    }

    /**
     * Restores the backup chain into {@code targetDir} and replays the staged
     * WAL up to {@code targetTimestampMs}.
     *
     * @param fullBackupDir     the full (step-2) backup directory
     * @param incrementalDirs   incremental backups in chain order (oldest first), may be empty
     * @param walSourceDir      the directory holding the WAL segments to replay
     * @param targetDir         the restore target (must not already contain a database)
     * @param targetTimestampMs the restore target in epoch millis
     * @return the PITR pass statistics
     * @throws IOException on restore, staging or replay failure
     */
    public static PitrResult recoverToTimestamp(Path fullBackupDir, List<Path> incrementalDirs,
                                                Path walSourceDir, Path targetDir,
                                                long targetTimestampMs)
            throws IOException {
        Objects.requireNonNull(fullBackupDir, "fullBackupDir");
        Objects.requireNonNull(incrementalDirs, "incrementalDirs");
        Objects.requireNonNull(walSourceDir, "walSourceDir");
        Objects.requireNonNull(targetDir, "targetDir");

        if (!PhysicalRestore.validateBackupDirectory(fullBackupDir)) {
            throw new IOException("Invalid full backup directory: " + fullBackupDir);
        }
        if (!Files.isDirectory(walSourceDir)) {
            throw new IOException("WAL source directory not found: " + walSourceDir);
        }

        // Step 1: physical restore of the whole chain (base state at backup time).
        IncrementalBackup.restoreChain(fullBackupDir, incrementalDirs, targetDir);

        // Step 2: the replay floor — newest incremental wins, then the full backup.
        long lastBackupLsn = resolveLastBackupLsn(fullBackupDir, incrementalDirs);

        // Step 3: stage the WAL read-only so replay never mutates the live log.
        Path stagedWal = Files.createTempDirectory(targetDir.getParent(), "pitr-wal-");
        try {
            stageWalSegments(walSourceDir, stagedWal);
            return PitrSupport.recoverToTimestamp(targetDir, stagedWal,
                    lastBackupLsn, targetTimestampMs);
        } finally {
            deleteRecursively(stagedWal);
        }
    }

    /**
     * Parses a PITR target timestamp in {@code yyyy-MM-dd HH:mm:ss} form using
     * the system default time zone (the CLI form mandated by prompt 4 #43).
     *
     * @param timestamp the timestamp text, e.g. {@code 2026-10-05 14:30:00}
     * @return the parsed instant in epoch millis
     * @throws IOException if the text cannot be parsed
     */
    public static long parseTimestamp(String timestamp) throws IOException {
        Objects.requireNonNull(timestamp, "timestamp");
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        try {
            LocalDateTime local = LocalDateTime.parse(timestamp.trim(), formatter);
            return local.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IOException("Invalid PITR timestamp '" + timestamp
                    + "' (expected 'yyyy-MM-dd HH:mm:ss'): " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the backup high-water LSN from the newest incremental's
     * manifest, falling back to the full backup's manifest, then {@code 0}.
     */
    static long resolveLastBackupLsn(Path fullBackupDir, List<Path> incrementalDirs)
            throws IOException {
        if (!incrementalDirs.isEmpty()) {
            Path newest = incrementalDirs.get(incrementalDirs.size() - 1);
            Long lsn = readMaxLsn(newest);
            if (lsn != null) {
                return lsn;
            }
        }
        Long fullLsn = readMaxLsn(fullBackupDir);
        return fullLsn != null ? fullLsn : 0L;
    }

    /** Reads {@code maxLsn} from a backup manifest; {@code null} when absent/unreadable. */
    private static Long readMaxLsn(Path backupDir) {
        try {
            Path manifestPath = backupDir.resolve(IncrementalBackup.MANIFEST_FILE);
            if (!Files.exists(manifestPath)) {
                return null;
            }
            BackupManifest manifest = BackupManifest.load(manifestPath);
            String value = manifest.getMetadata().get(META_MAX_LSN);
            return value == null ? null : Long.parseLong(value.trim());
        } catch (Exception e) {
            return null; // legacy or malformed manifest: fall back to full replay
        }
    }

    /** Copies every WAL segment file ({@code .log}/{@code .wal}) into the staging dir. */
    private static void stageWalSegments(Path walSourceDir, Path stagedWal) throws IOException {
        try (Stream<Path> files = Files.walk(walSourceDir)) {
            files.filter(Files::isRegularFile)
                    .filter(PitrManager::isWalSegment)
                    .forEach(source -> {
                        try {
                            Files.copy(source, stagedWal.resolve(source.getFileName().toString()),
                                    StandardCopyOption.COPY_ATTRIBUTES);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to stage WAL segment: " + source, e);
                        }
                    });
        }
    }

    private static boolean isWalSegment(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".log") || name.endsWith(".wal");
    }

    /** Deletes a directory tree, best-effort (staging cleanup must not fail a successful PITR). */
    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup of the temp staging dir
                }
            });
        } catch (IOException ignored) {
            // best-effort cleanup of the temp staging dir
        }
    }
}
