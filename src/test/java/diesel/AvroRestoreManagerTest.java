package diesel;

import diesel.storage.avro.AvroBackupManager;
import diesel.storage.avro.AvroBackupManager.BackupConfig;
import diesel.storage.avro.AvroBackupManager.BackupReport;
import diesel.storage.avro.AvroRestoreManager;
import diesel.storage.avro.AvroRestoreManager.AvailableBackup;
import diesel.storage.avro.AvroRestoreManager.RestoreConfig;
import diesel.storage.avro.AvroRestoreManager.RestoreReport;
import diesel.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AvroRestoreManager} (Prompt 84).
 *
 * <p>Covers: config resolution, full restore, point-in-time restore,
 * CRC validation, listing backups, edge cases.</p>
 */
@Tag("storage")
@StorageType("avro")
class AvroRestoreManagerTest {

    @TempDir
    Path tempDir;

    private static final String[] PROP_KEYS = {
            AvroRestoreManager.VALIDATE_BEFORE_RESTORE_KEY,
            AvroRestoreManager.POINT_IN_TIME_ENABLED_KEY,
            "avro.restore.config.file"
    };

    private final Map<String, String> prevProps = new LinkedHashMap<>();

    @BeforeEach
    void saveProps() {
        for (String key : PROP_KEYS) {
            prevProps.put(key, System.getProperty(key));
        }
    }

    @AfterEach
    void restoreProps() {
        for (String key : PROP_KEYS) {
            String value = prevProps.get(key);
            if (value != null) {
                System.setProperty(key, value);
            } else {
                System.clearProperty(key);
            }
        }
    }

    // ─── Config resolution tests ────────────────────────────────────

    @Test
    void configDefaults() {
        RestoreConfig cfg = new RestoreConfig(true, true);
        assertTrue(cfg.validateBeforeRestore());
        assertTrue(cfg.pointInTimeEnabled());
    }

    @Test
    void configSyspropOverride() {
        System.setProperty(AvroRestoreManager.VALIDATE_BEFORE_RESTORE_KEY, "false");
        System.setProperty(AvroRestoreManager.POINT_IN_TIME_ENABLED_KEY, "false");

        RestoreConfig cfg = AvroRestoreManager.resolve();
        assertFalse(cfg.validateBeforeRestore());
        assertFalse(cfg.pointInTimeEnabled());
    }

    // ─── Full restore tests ─────────────────────────────────────────

    @Test
    void fullRestoreSingleFile() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        byte[] data = "restore test data".getBytes();
        Files.write(new File(sourceDir, "test.avro").toPath(), data);

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig bCfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);
        BackupReport backup = AvroBackupManager.backupFull(sourceDir, bCfg);

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        RestoreConfig rCfg = new RestoreConfig(true, true);

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, rCfg);
        assertNotNull(report);
        assertEquals(1, report.filesRestored());
        assertEquals(0, report.filesFailed());
        assertTrue(report.allSuccessful());
        assertTrue(new File(targetDir, "test.avro").isFile());
        assertEquals(data.length, new File(targetDir, "test.avro").length());
    }

    @Test
    void fullRestoreMultipleFiles() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        for (int i = 0; i < 3; i++) {
            Files.write(new File(sourceDir, "table" + i + ".avro").toPath(),
                    ("data" + i).getBytes());
        }

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig bCfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);
        BackupReport backup = AvroBackupManager.backupFull(sourceDir, bCfg);

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        RestoreConfig rCfg = new RestoreConfig(false, true);

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, rCfg);
        assertEquals(3, report.filesRestored());
        assertTrue(report.allSuccessful());
    }

    @Test
    void fullRestoreWithValidation() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "validated.avro").toPath(), "content".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig bCfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);
        BackupReport backup = AvroBackupManager.backupFull(sourceDir, bCfg);

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        RestoreConfig rCfg = new RestoreConfig(true, true);

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, rCfg);
        assertTrue(report.allSuccessful());
    }

    @Test
    void fullRestoreWithoutValidation() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "fast.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig bCfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);
        BackupReport backup = AvroBackupManager.backupFull(sourceDir, bCfg);

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        RestoreConfig rCfg = new RestoreConfig(false, true);

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, rCfg);
        assertTrue(report.allSuccessful());
    }

    @Test
    void fullRestoreOverwritesExistingFiles() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "new_data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig bCfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);
        BackupReport backup = AvroBackupManager.backupFull(sourceDir, bCfg);

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();
        Files.write(new File(targetDir, "test.avro").toPath(), "old_data".getBytes());

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, new RestoreConfig(false, true));
        assertTrue(report.allSuccessful());
        assertEquals("new_data", Files.readString(new File(targetDir, "test.avro").toPath()));
    }

    @Test
    void fullRestoreBackupDirNull() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroRestoreManager.restoreFull(null, new File("/tmp")));
    }

    @Test
    void fullRestoreTargetDirNull() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        assertThrows(IllegalArgumentException.class,
                () -> AvroRestoreManager.restoreFull(backup.backupDirectory(), null));
    }

    @Test
    void fullRestoreNoManifest() throws IOException {
        File emptyDir = tempDir.resolve("empty_backup").toFile();
        emptyDir.mkdirs();
        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        assertThrows(IOException.class,
                () -> AvroRestoreManager.restoreFull(emptyDir, targetDir));
    }

    @Test
    void fullRestoreTimestamps() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "t.avro").toPath(), "d".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        Instant before = Instant.now();
        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, new RestoreConfig(false, true));
        Instant after = Instant.now();

        assertFalse(report.startedAt().isBefore(before));
        assertFalse(report.completedAt().isAfter(after));
        assertTrue(report.durationNanos() >= 0);
    }

    // ─── Point-in-time restore tests ────────────────────────────────

    @Test
    void pointInTimeRestoreExactMatch() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "pit.avro").toPath(), "pit_data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        RestoreReport report = AvroRestoreManager.restorePointInTime(
                backupDir, targetDir, backup.startedAt(), new RestoreConfig(false, true));
        assertTrue(report.allSuccessful());
        assertEquals(1, report.filesRestored());
    }

    @Test
    void pointInTimeRestoreBeforeBackups() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        Instant beforeAll = Instant.now().minusSeconds(3600);
        assertThrows(IOException.class,
                () -> AvroRestoreManager.restorePointInTime(backupDir, targetDir, beforeAll));
    }

    @Test
    void pointInTimeRestoreDisabled() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        assertThrows(IOException.class,
                () -> AvroRestoreManager.restorePointInTime(
                        backupDir, targetDir, Instant.now(), new RestoreConfig(false, false)));
    }

    @Test
    void pointInTimeRestoreNullTimestamp() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        assertThrows(IOException.class,
                () -> AvroRestoreManager.restorePointInTime(backupDir, targetDir, null));
    }

    @Test
    void pointInTimeRestoreMultipleBackups() throws Exception {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "old.avro").toPath(), "old".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport first = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        Thread.sleep(50);
        Files.write(new File(sourceDir, "new.avro").toPath(), "new".getBytes());
        AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        RestoreReport report = AvroRestoreManager.restorePointInTime(
                backupDir, targetDir, first.startedAt(), new RestoreConfig(false, true));
        assertEquals(1, report.filesRestored());
    }

    // ─── List backups tests ─────────────────────────────────────────

    @Test
    void listBackupsEmpty() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();

        List<AvailableBackup> backups = AvroRestoreManager.listBackups(backupDir);
        assertNotNull(backups);
        assertTrue(backups.isEmpty());
    }

    @Test
    void listBackupsSingle() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, false));

        List<AvailableBackup> backups = AvroRestoreManager.listBackups(backupDir);
        assertEquals(1, backups.size());
        assertEquals("full", backups.get(0).manifest().backupType());
    }

    @Test
    void listBackupsSorted() throws Exception {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);
        AvroBackupManager.backupFull(sourceDir, cfg);
        Thread.sleep(50);
        AvroBackupManager.backupFull(sourceDir, cfg);

        List<AvailableBackup> backups = AvroRestoreManager.listBackups(backupDir);
        assertEquals(2, backups.size());
        assertTrue(backups.get(0).manifest().startedAt()
                .isBefore(backups.get(1).manifest().startedAt()));
    }

    @Test
    void listBackupsNonexistentDir() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroRestoreManager.listBackups(new File("/nonexistent/path")));
    }

    @Test
    void listBackupsSkipsNonDirs() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();
        Files.write(new File(backupDir, "loose_file.txt").toPath(), "text".getBytes());

        List<AvailableBackup> backups = AvroRestoreManager.listBackups(backupDir);
        assertTrue(backups.isEmpty());
    }

    @Test
    void listBackupsSkipsDirsWithoutManifest() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();
        new File(backupDir, "no_manifest").mkdirs();

        List<AvailableBackup> backups = AvroRestoreManager.listBackups(backupDir);
        assertTrue(backups.isEmpty());
    }

    // ─── RestoreReport tests ────────────────────────────────────────

    @Test
    void restoreReportAllSuccessful() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, true));

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, new RestoreConfig(true, true));
        assertTrue(report.allSuccessful());
        assertEquals(0, report.filesFailed());
    }

    // ─── CRC validation edge cases ──────────────────────────────────

    @Test
    void restoreDetectsCorruptBackupFile() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "original".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, true));

        File backed = new File(backup.backupDirectory(), "test.avro");
        Files.write(backed.toPath(), "CORRUPTED".getBytes());

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        assertThrows(IOException.class,
                () -> AvroRestoreManager.restoreFull(
                        backup.backupDirectory(), targetDir, new RestoreConfig(true, true)));
    }

    @Test
    void restoreSkipsCorruptFileWithoutValidation() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "original".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupReport backup = AvroBackupManager.backupFull(sourceDir,
                new BackupConfig(backupDir.getAbsolutePath(), 30, true, true));

        File backed = new File(backup.backupDirectory(), "test.avro");
        Files.write(backed.toPath(), "CORRUPTED".getBytes());

        File targetDir = tempDir.resolve("target").toFile();
        targetDir.mkdirs();

        RestoreReport report = AvroRestoreManager.restoreFull(
                backup.backupDirectory(), targetDir, new RestoreConfig(false, true));
        assertTrue(report.allSuccessful());
        assertEquals("CORRUPTED", Files.readString(new File(targetDir, "test.avro").toPath()));
    }
}
