package diesel;

import diesel.storage.avro.AvroCrashDetector;
import diesel.storage.avro.AvroCrashDetector.BakFile;
import diesel.storage.avro.AvroCrashDetector.CorruptedFile;
import diesel.storage.avro.AvroCrashDetector.CrashDetectionReport;
import diesel.storage.avro.AvroCrashDetector.TempArtifact;
import diesel.storage.avro.AvroDataFileReader;
import diesel.storage.avro.AvroDataFileWriter;
import diesel.storage.avro.AvroRecoveryManager;
import diesel.storage.avro.AvroRecoveryManager.FileRecovery;
import diesel.storage.avro.AvroRecoveryManager.RecoveryAction;
import diesel.storage.avro.AvroRecoveryManager.RecoveryReport;
import diesel.storage.avro.AvroReadIterator;
import diesel.storage.avro.AvroSyncMarkerManager;
import diesel.storage.avro.AvroSyncMarkerManager.IntegrityResult;
import diesel.storage.avro.AvroSyncMarkerManager.SyncMarkerInfo;
import diesel.storage.avro.AvroTypeMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 82 AVRO crash recovery tests: crash artifact detection and the
 * recovery orchestration (roll-forward of complete temps, roll-back of
 * unfinished transactions, truncation to the last consistent block, and
 * process logging).
 */
@Tag("storage")
@StorageType("avro")
class AvroRecoveryManagerTest {

    private static final String[] PROP_KEYS = {
            "avro.recovery.detect.on.startup",
            "avro.recovery.cleanup.temps",
            "avro.recovery.config.file",
            "avro.syncmarker.recovery.backup",
            "avro.block.sync.interval"
    };

    @TempDir
    Path tempDir;

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

    // ─── Test data helpers ─────────────────────────────────────────

    private static List<String> cols() {
        return List.of("ID", "NAME", "AGE");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        m.put("ID", Long.class);
        m.put("NAME", String.class);
        m.put("AGE", Integer.class);
        return m;
    }

    private static Map<String, Object> row(long id) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ID", id);
        m.put("NAME", "User-" + id);
        m.put("AGE", (int) (id % 100));
        return m;
    }

    /** Writes a multi-block file (small sync interval) and returns it. */
    private File writeMultiBlock(String name, int rows) throws IOException {
        System.setProperty("avro.block.sync.interval", "256");
        File f = new File(tempDir.toFile(), name);
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), f);
        try {
            for (int i = 0; i < rows; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        return f;
    }

    private static Map<String, Class<?>> types(Schema schema) {
        Map<String, Class<?>> m = new LinkedHashMap<>();
        for (Schema.Field f : schema.getFields()) {
            m.put(f.name(), AvroTypeMapper.toJavaType(f.schema()));
        }
        return m;
    }

    private static long countRows(File f) throws IOException {
        try (AvroDataFileReader reader = new AvroDataFileReader(f);
             AvroReadIterator it = new AvroReadIterator(reader, cols(), types(reader.getSchema()))) {
            long n = 0;
            while (it.hasNext()) {
                it.next();
                n++;
            }
            return n;
        }
    }

    /** Flips bit 0 of the byte at {@code offset}. */
    private static void corruptByte(File f, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 1);
        }
    }

    private static void truncate(File f, long length) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.setLength(length);
        }
    }

    private void writeText(File f, String text) throws IOException {
        try (OutputStream out = new FileOutputStream(f)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static SyncMarkerInfo lastBlockMarker(List<SyncMarkerInfo> markers) {
        return markers.stream()
                .filter(m -> m.blockIndex() >= 0)
                .reduce((a, b) -> b)
                .orElseThrow();
    }

    /** Returns the sync-marker offset of block {@code index}. */
    private long blockSyncOffset(File f, int index) throws IOException {
        return AvroSyncMarkerManager.resolve().scanAllMarkers(f).stream()
                .filter(m -> m.blockIndex() == index)
                .map(SyncMarkerInfo::fileOffset)
                .findFirst().orElseThrow();
    }

    // ─── Detector: clean / empty scans ─────────────────────────────

    @Test
    void detectCleanDirHasNoIssues() throws IOException {
        writeMultiBlock("users.avro", 50);
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertFalse(r.hasIssues());
        assertEquals(0, r.issueCount());
        assertTrue(r.orphanedTemps().isEmpty());
        assertTrue(r.corruptedFiles().isEmpty());
        assertTrue(r.emptyFiles().isEmpty());
    }

    @Test
    void detectMissingDirReturnsEmptyReport() throws IOException {
        File missing = tempDir.resolve("does-not-exist").toFile();
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(missing);
        assertFalse(r.hasIssues());
        assertEquals(0, r.issueCount());
    }

    @Test
    void detectEmptyDirHasNoIssues() throws IOException {
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertFalse(r.hasIssues());
        assertEquals(0, r.issueCount());
    }

    // ─── Detector: artifacts ───────────────────────────────────────

    @Test
    void detectOrphanedTmpFile() throws IOException {
        File tmp = new File(tempDir.toFile(), "users.avro.tmp");
        writeText(tmp, "not a complete write");
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.orphanedTemps().size());
        TempArtifact a = r.orphanedTemps().get(0);
        assertEquals("users.avro", a.targetName());
        assertEquals(tmp, a.file());
        assertEquals(tmp.length(), a.size());
        assertNotNull(a.lastModified());
    }

    @Test
    void detectMultipleOrphanedTmpFiles() throws IOException {
        writeText(new File(tempDir.toFile(), "a.avro.tmp"), "x");
        writeText(new File(tempDir.toFile(), "b.avro.tmp"), "y");
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(2, r.orphanedTemps().size());
    }

    @Test
    void detectTruncatedAvroFileNeedsRecovery() throws IOException {
        File f = writeMultiBlock("trunc.avro", 2000);
        long lastSync = blockSyncOffset(f, healthyBlockCount(f) - 1);
        truncate(f, lastSync + 3);
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.corruptedFiles().size());
        CorruptedFile c = r.corruptedFiles().get(0);
        assertTrue(c.needsRecovery(), "truncated file must need recovery");
        assertNotNull(c.integrityResult());
        assertTrue(c.integrityResult().truncationOffset() >= 0);
    }

    @Test
    void detectCorruptedSyncMarkerFlagsFile() throws IOException {
        File f = writeMultiBlock("corr.avro", 2000);
        corruptByte(f, blockSyncOffset(f, 0));
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.corruptedFiles().size());
        assertTrue(r.corruptedFiles().get(0).needsRecovery());
    }

    @Test
    void detectUnreadableHeaderFlagsFile() throws IOException {
        File f = new File(tempDir.toFile(), "garbage.avro");
        writeText(f, "this is not an avro file at all");
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.corruptedFiles().size());
        CorruptedFile c = r.corruptedFiles().get(0);
        assertTrue(c.needsRecovery());
        assertTrue(c.integrityResult() == null, "no integrity scan possible without a header");
    }

    @Test
    void detectBakFileReportedInformational() throws IOException {
        writeMultiBlock("users.avro", 50);
        File bak = new File(tempDir.toFile(), "users.avro.bak");
        writeText(bak, "backup contents");
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.backupFiles().size());
        BakFile b = r.backupFiles().get(0);
        assertEquals("users.avro", b.originalName());
        assertEquals(bak, b.file());
    }

    @Test
    void detectEmptyAvroFileReported() throws IOException {
        File f = new File(tempDir.toFile(), "empty.avro");
        assertTrue(f.createNewFile());
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.emptyFiles().size());
        assertEquals(f, r.emptyFiles().get(0).file());
    }

    @Test
    void detectMixedIssuesCountsTogether() throws IOException {
        writeText(new File(tempDir.toFile(), "orphan.avro.tmp"), "x");
        File truncated = writeMultiBlock("truncated.avro", 2000);
        truncate(truncated, blockSyncOffset(truncated, healthyBlockCount(truncated) - 1) + 3);
        File empty = new File(tempDir.toFile(), "nothing.avro");
        assertTrue(empty.createNewFile());
        CrashDetectionReport r = AvroCrashDetector.resolve().detectCrash(tempDir.toFile());
        assertEquals(1, r.orphanedTemps().size());
        assertEquals(1, r.corruptedFiles().size());
        assertEquals(1, r.emptyFiles().size());
        assertEquals(3, r.issueCount());
        assertTrue(r.hasIssues());
    }

    private int healthyBlockCount(File f) throws IOException {
        return AvroSyncMarkerManager.resolve().countBlocks(f);
    }

    // ─── Recovery: healthy files ───────────────────────────────────

    @Test
    void recoverHealthyFileIsNoOp() throws IOException {
        File f = writeMultiBlock("users.avro", 500);
        long before = f.length();
        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(r.allSuccessful());
        assertEquals(before, f.length(), "healthy file must be untouched");
        assertEquals(500, countRows(f));
        assertEquals(0, r.filesRecovered());
    }

    @Test
    void recoverFileIntactNoOp() throws IOException {
        File f = writeMultiBlock("users.avro", 50);
        FileRecovery fr = AvroRecoveryManager.resolve().recoverFile(f);
        assertEquals(RecoveryAction.NONE, fr.action());
        assertTrue(fr.success());
        assertEquals(50, countRows(f));
    }

    // ─── Recovery: truncated / corrupt files ───────────────────────

    @Test
    void recoverTruncatedFileRestoresLastConsistentState() throws IOException {
        File f = writeMultiBlock("users.avro", 2000);
        IntegrityResult healthy = AvroSyncMarkerManager.resolve().validateIntegrity(f);
        long lastSync = lastBlockMarker(healthy.markers()).fileOffset();
        long secondLast = blockSyncOffset(f, (int) (healthy.totalBlocks() - 2));
        long expectedEnd = secondLast + AvroSyncMarkerManager.SYNC_SIZE;
        truncate(f, lastSync + 3);

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(r.allSuccessful());
        assertEquals(expectedEnd, f.length(), "file must end at the last consistent block boundary");
        long rows = countRows(f);
        assertTrue(rows > 0, "recovered records are preserved");
        assertTrue(rows < 2000, "crashed block rows are dropped");
        assertEquals(1, r.filesRecovered());
    }

    @Test
    void recoverCorruptedSyncMarkerDropsBlock() throws IOException {
        File f = writeMultiBlock("users.avro", 2000);
        corruptByte(f, blockSyncOffset(f, 2));

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(r.allSuccessful());
        assertEquals(1, r.filesRecovered());
        FileRecovery fr = r.recoveries().stream().filter(x -> x.file().equals(f))
                .findFirst().orElseThrow();
        assertTrue(fr.action() == RecoveryAction.TRUNCATED || fr.action() == RecoveryAction.BACKUP_CREATED);
        long rows = countRows(f);
        assertTrue(rows > 0, "blocks 0 and 1 must survive");
        assertTrue(rows < 2000, "corrupted block's rows are dropped");
    }

    @Test
    void recoveryCreatesBackupOnTruncation() throws IOException {
        File f = writeMultiBlock("users.avro", 500);
        long lastSync = blockSyncOffset(f, healthyBlockCount(f) - 1);
        long crashedLen = lastSync + 3;
        truncate(f, crashedLen);

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        FileRecovery fr = r.recoveries().stream().filter(x -> x.file().equals(f))
                .findFirst().orElseThrow();
        assertEquals(RecoveryAction.BACKUP_CREATED, fr.action());
        File bak = new File(tempDir.toFile(), "users.avro.bak");
        assertTrue(bak.isFile(), "backup file must be written on truncation");
        assertEquals(crashedLen, bak.length(), "backup holds the crashed bytes");
        assertTrue(f.length() < bak.length());
    }

    @Test
    void unreadableHeaderFileFailsAndIsLeftUntouched() throws IOException {
        File f = new File(tempDir.toFile(), "garbage.avro");
        String content = "not an avro file";
        writeText(f, content);

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(1, r.filesFailed());
        assertEquals(1, r.recoveries().size());
        assertEquals(RecoveryAction.FAILED, r.recoveries().get(0).action());
        assertEquals(content, new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8),
                "file must be left untouched");
    }

    // ─── Recovery: orphaned temps (transaction rollback) ───────────

    @Test
    void promoteCompleteTempOverTarget() throws IOException {
        File tmp = new File(tempDir.toFile(), "users.avro.tmp");
        System.setProperty("avro.block.sync.interval", "256");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), tmp);
        try {
            for (int i = 0; i < 100; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }
        File target = new File(tempDir.toFile(), "users.avro");
        assertFalse(target.exists());

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(1, r.filesRecovered());
        assertEquals(RecoveryAction.PROMOTED_TMP, r.recoveries().get(0).action());
        assertFalse(tmp.exists(), "temp must be renamed (promoted)");
        assertTrue(target.isFile(), "target must be created from the temp");
        assertEquals(100, countRows(target), "promoted file keeps its records");
    }

    @Test
    void promoteCompleteTempReplacesStaleTarget() throws IOException {
        writeMultiBlock("users.avro", 10);
        File tmp = new File(tempDir.toFile(), "users.avro.tmp");
        System.setProperty("avro.block.sync.interval", "256");
        AvroDataFileWriter w = new AvroDataFileWriter(cols(), types(), tmp);
        try {
            for (int i = 0; i < 250; i++) {
                w.writeRow(row(i));
            }
            w.flush();
        } finally {
            w.close();
        }

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(RecoveryAction.PROMOTED_TMP, r.recoveries().get(0).action());
        assertEquals(250, countRows(new File(tempDir.toFile(), "users.avro")),
                "the most recent complete write wins over the stale target");
    }

    @Test
    void discardUnusableTemp() throws IOException {
        File tmp = new File(tempDir.toFile(), "users.avro.tmp");
        writeText(tmp, "half-written garbage");
        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertEquals(1, r.filesRecovered());
        assertEquals(RecoveryAction.DISCARDED_TMP, r.recoveries().get(0).action());
        assertFalse(tmp.exists(), "unusable temp must be discarded");
        assertFalse(new File(tempDir.toFile(), "users.avro").exists(),
                "no target may be fabricated from garbage");
    }

    @Test
    void cleanupDisabledLeavesTempInPlace() throws IOException {
        File tmp = new File(tempDir.toFile(), "users.avro.tmp");
        writeText(tmp, "half-written garbage");
        AvroRecoveryManager mgr = new AvroRecoveryManager(true, false);
        RecoveryReport r = mgr.recoverAll(tempDir.toFile());
        assertEquals(RecoveryAction.NONE, r.recoveries().get(0).action());
        assertTrue(tmp.exists(), "temp must be kept when cleanup is disabled");
        assertEquals(0, r.filesRecovered());
    }

    // ─── Recovery: reports and startup hook ────────────────────────

    @Test
    void recoverAllMixedDirectoryCountsCorrectly() throws IOException {
        writeMultiBlock("healthy.avro", 30);
        File truncated = writeMultiBlock("truncated.avro", 2000);
        truncate(truncated, blockSyncOffset(truncated, healthyBlockCount(truncated) - 1) + 3);
        writeText(new File(tempDir.toFile(), "orphan.avro.tmp"), "garbage");
        File empty = new File(tempDir.toFile(), "empty.avro");
        assertTrue(empty.createNewFile());

        RecoveryReport r = AvroRecoveryManager.resolve().recoverAll(tempDir.toFile());
        assertTrue(r.allSuccessful());
        assertEquals(3, r.recoveries().size(), "truncated + temp + empty (healthy file is not an issue)");
        assertEquals(2, r.filesRecovered(), "truncated + discarded temp");
        assertEquals(1, r.filesSkipped(), "empty file left in place");
        assertEquals(0, r.filesFailed());
        assertTrue(r.totalFilesScanned() >= r.recoveries().size());
        assertTrue(r.totalRecoveryTimeNanos() >= 0);
        assertNotNull(r.startedAt());
        assertNotNull(r.completedAt());
    }

    @Test
    void recoverOnStartupRunsFullPass() throws IOException {
        File f = writeMultiBlock("users.avro", 2000);
        truncate(f, blockSyncOffset(f, healthyBlockCount(f) - 1) + 3);
        RecoveryReport r = AvroRecoveryManager.resolve().recoverOnStartup(tempDir.toFile());
        assertTrue(r.allSuccessful());
        assertEquals(1, r.filesRecovered());
        assertTrue(countRows(f) > 0);
    }

    @Test
    void recoverOnStartupDisabledIsNoOp() throws IOException {
        System.setProperty("avro.recovery.detect.on.startup", "false");
        AvroRecoveryManager mgr = AvroRecoveryManager.resolve();
        assertFalse(mgr.detector().detectOnStartup());
        RecoveryReport r = mgr.recoverOnStartup(tempDir.toFile());
        assertEquals(0, r.totalFilesScanned());
        assertEquals(0, r.filesRecovered());
        assertEquals(0, r.filesSkipped());
    }

    @Test
    void recoverFileOnTruncatedFile() throws IOException {
        File f = writeMultiBlock("users.avro", 1000);
        truncate(f, blockSyncOffset(f, healthyBlockCount(f) - 1) + 3);
        FileRecovery fr = AvroRecoveryManager.resolve().recoverFile(f);
        assertTrue(fr.success());
        assertTrue(fr.action() == RecoveryAction.TRUNCATED || fr.action() == RecoveryAction.BACKUP_CREATED);
        assertTrue(countRows(f) > 0);
    }

    // ─── Config resolution ─────────────────────────────────────────

    @Test
    void configDefaults() {
        AvroCrashDetector d = AvroCrashDetector.resolve();
        assertEquals(AvroCrashDetector.DEFAULT_DETECT_ON_STARTUP, d.detectOnStartup());
        assertEquals(AvroCrashDetector.DEFAULT_CLEANUP_TEMPS, d.cleanupTemps());
        assertEquals("avro.recovery.detect.on.startup", AvroCrashDetector.DETECT_ON_STARTUP_KEY);
        assertEquals("avro.recovery.cleanup.temps", AvroCrashDetector.CLEANUP_TEMPS_KEY);
    }

    @Test
    void configSyspropOverridesDefault() {
        System.setProperty("avro.recovery.detect.on.startup", "false");
        System.setProperty("avro.recovery.cleanup.temps", "false");
        AvroCrashDetector d = AvroCrashDetector.resolve();
        assertFalse(d.detectOnStartup());
        assertFalse(d.cleanupTemps());
    }

    @Test
    void configFileOverride() throws Exception {
        Path config = tempDir.resolve("recovery-test.properties");
        Properties props = new Properties();
        props.setProperty("avro.recovery.detect.on.startup", "true");
        props.setProperty("avro.recovery.cleanup.temps", "false");
        try (OutputStream out = new FileOutputStream(config.toFile())) {
            props.store(out, "test");
        }
        System.setProperty("avro.recovery.config.file", config.toString());
        AvroCrashDetector d = AvroCrashDetector.resolve();
        assertTrue(d.detectOnStartup());
        assertFalse(d.cleanupTemps());
    }

    @Test
    void configFileGivesWayToSysprop() throws Exception {
        Path config = tempDir.resolve("recovery-test.properties");
        Properties props = new Properties();
        props.setProperty("avro.recovery.detect.on.startup", "false");
        try (OutputStream out = new FileOutputStream(config.toFile())) {
            props.store(out, "test");
        }
        System.setProperty("avro.recovery.config.file", config.toString());
        System.setProperty("avro.recovery.detect.on.startup", "true");
        AvroCrashDetector d = AvroCrashDetector.resolve();
        assertTrue(d.detectOnStartup(), "sysprop must win over the config file");
    }
}