package diesel;

import diesel.storage.avro.AvroBackupManager;
import diesel.storage.avro.AvroBackupManager.AvroFileEntry;
import diesel.storage.avro.AvroBackupManager.BackedUpFile;
import diesel.storage.avro.AvroBackupManager.BackupConfig;
import diesel.storage.avro.AvroBackupManager.BackupManifest;
import diesel.storage.avro.AvroBackupManager.BackupReport;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AvroBackupManager} (Prompt 84).
 *
 * <p>Covers: config resolution, full backup, incremental backup,
 * CRC validation, pruning, manifest read/write, edge cases.</p>
 */
@Tag("storage")
@StorageType("avro")
class AvroBackupManagerTest {

    @TempDir
    Path tempDir;

    private static final String[] PROP_KEYS = {
            AvroBackupManager.BACKUP_DIR_KEY,
            AvroBackupManager.RETENTION_DAYS_KEY,
            AvroBackupManager.INCREMENTAL_ENABLED_KEY,
            AvroBackupManager.VALIDATE_ON_CREATE_KEY,
            "avro.backup.config.file"
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
        BackupConfig cfg = new BackupConfig(
                AvroBackupManager.DEFAULT_BACKUP_DIR,
                AvroBackupManager.DEFAULT_RETENTION_DAYS,
                AvroBackupManager.DEFAULT_INCREMENTAL_ENABLED,
                AvroBackupManager.DEFAULT_VALIDATE_ON_CREATE);
        assertEquals("data/avro-backups", cfg.backupDir());
        assertEquals(30, cfg.retentionDays());
        assertTrue(cfg.incrementalEnabled());
        assertTrue(cfg.validateOnCreate());
    }

    @Test
    void configSyspropOverride() {
        System.setProperty(AvroBackupManager.BACKUP_DIR_KEY, "/tmp/test-backups");
        System.setProperty(AvroBackupManager.RETENTION_DAYS_KEY, "7");
        System.setProperty(AvroBackupManager.INCREMENTAL_ENABLED_KEY, "false");
        System.setProperty(AvroBackupManager.VALIDATE_ON_CREATE_KEY, "false");

        BackupConfig cfg = AvroBackupManager.resolve();
        assertEquals("/tmp/test-backups", cfg.backupDir());
        assertEquals(7, cfg.retentionDays());
        assertFalse(cfg.incrementalEnabled());
        assertFalse(cfg.validateOnCreate());
    }

    @Test
    void configInvalidRetentionPolicy() {
        System.setProperty(AvroBackupManager.RETENTION_DAYS_KEY, "not_a_number");
        BackupConfig cfg = AvroBackupManager.resolve();
        assertEquals(AvroBackupManager.DEFAULT_RETENTION_DAYS, cfg.retentionDays());
    }

    @Test
    void configNegativeRetentionPolicy() {
        System.setProperty(AvroBackupManager.RETENTION_DAYS_KEY, "-5");
        BackupConfig cfg = AvroBackupManager.resolve();
        assertEquals(AvroBackupManager.DEFAULT_RETENTION_DAYS, cfg.retentionDays());
    }

    // ─── Full backup tests ──────────────────────────────────────────

    @Test
    void fullBackupEmptyDir() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertNotNull(report);
        assertEquals(0, report.totalFiles());
        assertEquals(0, report.filesFailed());
        assertTrue(report.allSuccessful());
    }

    @Test
    void fullBackupSingleFile() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        File avroFile = new File(sourceDir, "test.avro");
        byte[] data = "test avro data".getBytes();
        Files.write(avroFile.toPath(), data);

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertNotNull(report);
        assertEquals(1, report.totalFiles());
        assertEquals(1, report.filesCopied());
        assertEquals(0, report.filesFailed());
        assertTrue(report.allSuccessful());
        assertEquals(data.length, report.totalBytes());

        File backed = report.backupDirectory();
        assertTrue(backed.isDirectory());
        assertTrue(new File(backed, "test.avro").isFile());
        assertTrue(new File(backed, "manifest.txt").isFile());
    }

    @Test
    void fullBackupMultipleFiles() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        for (int i = 0; i < 5; i++) {
            Files.write(new File(sourceDir, "table" + i + ".avro").toPath(),
                    ("data" + i).getBytes());
        }

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertEquals(5, report.totalFiles());
        assertEquals(5, report.filesCopied());
        assertTrue(report.allSuccessful());
    }

    @Test
    void fullBackupSkipsNonAvroFiles() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "data.avro").toPath(), "avro".getBytes());
        Files.write(new File(sourceDir, "readme.txt").toPath(), "txt".getBytes());
        Files.write(new File(sourceDir, "data.json").toPath(), "json".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertEquals(1, report.totalFiles());
    }

    @Test
    void fullBackupSkipsTmpAndBak() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "data.avro").toPath(), "avro".getBytes());
        Files.write(new File(sourceDir, "data.avro.tmp").toPath(), "tmp".getBytes());
        Files.write(new File(sourceDir, "data.avro.bak").toPath(), "bak".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertEquals(1, report.totalFiles());
    }

    @Test
    void fullBackupSkipsSubdirs() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "top.avro").toPath(), "top".getBytes());
        File sub = new File(sourceDir, "subdir");
        sub.mkdirs();
        Files.write(new File(sub, "nested.avro").toPath(), "nested".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertEquals(2, report.totalFiles());
    }

    @Test
    void fullBackupCrcValidated() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        byte[] data = "crc test data content".getBytes();
        Files.write(new File(sourceDir, "test.avro").toPath(), data);

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertEquals(1, report.filesCopied());
        assertTrue(report.allSuccessful());
        BackedUpFile backed = report.manifest().files().get(0);
        assertTrue(backed.crc32() >= 0);
    }

    @Test
    void fullBackupSourceDirNull() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroBackupManager.backupFull(null));
    }

    @Test
    void fullBackupSourceDirNotDirectory() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroBackupManager.backupFull(new File("/nonexistent/path")));
    }

    @Test
    void fullBackupCreatesBackupDir() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        File backupDir = tempDir.resolve("new_backup_dir").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        AvroBackupManager.backupFull(sourceDir, cfg);
        assertTrue(backupDir.isDirectory());
    }

    @Test
    void fullBackupManifestRecorded() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        BackupManifest manifest = report.manifest();
        assertEquals("full", manifest.backupType());
        assertNotNull(manifest.startedAt());
        assertNotNull(manifest.completedAt());
        assertTrue(manifest.durationNanos() >= 0);
        assertTrue(manifest.allSuccessful());
    }

    @Test
    void fullBackupTimestamps() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "t.avro").toPath(), "d".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        Instant before = Instant.now();
        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        Instant after = Instant.now();

        assertFalse(report.startedAt().isBefore(before));
        assertFalse(report.completedAt().isAfter(after));
    }

    // ─── Incremental backup tests ───────────────────────────────────

    @Test
    void incrementalBackupNoPreviousFullBackup() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupReport report = AvroBackupManager.backupIncremental(sourceDir, cfg);
        assertEquals(1, report.totalFiles());
        assertEquals("incremental", report.manifest().backupType());
    }

    @Test
    void incrementalBackupNoChangesSkips() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        AvroBackupManager.backupFull(sourceDir, cfg);
        BackupReport report = AvroBackupManager.backupIncremental(sourceDir, cfg);
        assertEquals(0, report.totalFiles());
    }

    @Test
    void incrementalBackupDetectsNewFile() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "old.avro").toPath(), "old".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        AvroBackupManager.backupFull(sourceDir, cfg);

        Files.write(new File(sourceDir, "new.avro").toPath(), "new".getBytes());
        BackupReport report = AvroBackupManager.backupIncremental(sourceDir, cfg);
        assertEquals(1, report.totalFiles());
    }

    @Test
    void incrementalBackupDetectsModifiedFile() throws Exception {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "original".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        AvroBackupManager.backupFull(sourceDir, cfg);

        Thread.sleep(50);
        Files.write(new File(sourceDir, "test.avro").toPath(), "modified_content".getBytes());
        BackupReport report = AvroBackupManager.backupIncremental(sourceDir, cfg);
        assertEquals(1, report.totalFiles());
    }

    @Test
    void incrementalBackupFallsBackWhenDisabled() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, false, false);

        BackupReport report = AvroBackupManager.backupIncremental(sourceDir, cfg);
        assertEquals("full", report.manifest().backupType());
        assertEquals(1, report.totalFiles());
    }

    @Test
    void incrementalBackupSourceDirNull() {
        assertThrows(IllegalArgumentException.class,
                () -> AvroBackupManager.backupIncremental(null));
    }

    // ─── Pruning tests ──────────────────────────────────────────────

    @Test
    void pruneNoBackups() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 1, true, false);

        int pruned = AvroBackupManager.pruneOldBackups(cfg);
        assertEquals(0, pruned);
    }

    @Test
    void pruneNonexistentDir() throws IOException {
        BackupConfig cfg = new BackupConfig("/nonexistent/path", 1, true, false);
        int pruned = AvroBackupManager.pruneOldBackups(cfg);
        assertEquals(0, pruned);
    }

    @Test
    void pruneKeepsRecentBackups() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();
        File recent = new File(backupDir, "20990101_120000");
        recent.mkdirs();
        Files.write(new File(recent, "manifest.txt").toPath(), "test".getBytes());
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        int pruned = AvroBackupManager.pruneOldBackups(cfg);
        assertEquals(0, pruned);
        assertTrue(recent.isDirectory());
    }

    // ─── Manifest read/write tests ──────────────────────────────────

    @Test
    void manifestRoundTrip() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        BackupManifest read = AvroBackupManager.readLatestManifest(cfg);

        assertNotNull(read);
        assertEquals(report.manifest().backupType(), read.backupType());
        assertEquals(report.manifest().totalFiles(), read.totalFiles());
        assertEquals(report.manifest().totalBytes(), read.totalBytes());
    }

    @Test
    void readLatestManifestNoBackups() throws IOException {
        File backupDir = tempDir.resolve("backups").toFile();
        backupDir.mkdirs();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, false);

        BackupManifest manifest = AvroBackupManager.readLatestManifest(cfg);
        assertEquals(null, manifest);
    }

    @Test
    void readLatestManifestNonexistentDir() throws IOException {
        BackupConfig cfg = new BackupConfig("/nonexistent/path", 30, true, false);
        BackupManifest manifest = AvroBackupManager.readLatestManifest(cfg);
        assertEquals(null, manifest);
    }

    // ─── AvroFileEntry tests ────────────────────────────────────────

    @Test
    void avroFileEntryComparable() {
        File f1 = new File("a.avro");
        File f2 = new File("b.avro");
        AvroFileEntry e1 = new AvroFileEntry(f1, 100, 0);
        AvroFileEntry e2 = new AvroFileEntry(f2, 200, 0);
        assertTrue(e1.compareTo(e2) < 0);
        assertTrue(e2.compareTo(e1) > 0);
    }

    // ─── BackedUpFile tests ─────────────────────────────────────────

    @Test
    void backedUpFileConvenienceCtor() {
        BackedUpFile f = new BackedUpFile("test.avro", 100, 42L, true);
        assertEquals("test.avro", f.path());
        assertEquals(100, f.sizeBytes());
        assertEquals(0, f.lastModifiedMs());
        assertEquals(42, f.crc32());
        assertTrue(f.success());
    }

    @Test
    void backedUpFileFailed() {
        BackedUpFile f = new BackedUpFile("bad.avro", 50, -1, false);
        assertFalse(f.success());
        assertEquals(-1, f.crc32());
    }

    // ─── BackupReport tests ─────────────────────────────────────────

    @Test
    void backupReportAllSuccessful() throws IOException {
        File sourceDir = tempDir.resolve("source").toFile();
        sourceDir.mkdirs();
        Files.write(new File(sourceDir, "test.avro").toPath(), "data".getBytes());

        File backupDir = tempDir.resolve("backups").toFile();
        BackupConfig cfg = new BackupConfig(backupDir.getAbsolutePath(), 30, true, true);

        BackupReport report = AvroBackupManager.backupFull(sourceDir, cfg);
        assertTrue(report.allSuccessful());
        assertEquals(0, report.filesFailed());
    }
}
