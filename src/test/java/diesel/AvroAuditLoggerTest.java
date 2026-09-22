package diesel;

import diesel.storage.avro.AvroAuditLogger;
import diesel.storage.avro.AvroAuditLogger.AuditCategory;
import diesel.storage.avro.AvroAuditLogger.AuditEntry;
import diesel.storage.avro.AvroAuditLogger.AuditLevel;
import diesel.storage.avro.AvroAuditLogger.AuditSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt 95 AVRO audit logger tests: config resolution (sysprop → config.properties → defaults),
 * structured logging for read/write/error/transaction/config/backup, performance tracing with
 * slow-operation escalation, file rotation and archiving, retention pruning, JSON export,
 * snapshot/reset lifecycle, disabled mode, and concurrent recording.
 */
@Tag("storage")
@StorageType("avro")
class AvroAuditLoggerTest {

    private static final String[] SYS_PROPS = {
            AvroAuditLogger.ENABLED_KEY,
            AvroAuditLogger.LOG_DIR_KEY,
            AvroAuditLogger.LOG_FILE_PREFIX_KEY,
            AvroAuditLogger.LOG_FILE_SUFFIX_KEY,
            AvroAuditLogger.LOG_MAX_SIZE_MB_KEY,
            AvroAuditLogger.LOG_RETENTION_DAYS_KEY,
            AvroAuditLogger.ROTATION_ENABLED_KEY,
            AvroAuditLogger.BUFFER_SIZE_KEY,
            AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY,
            AvroAuditLogger.TRACING_ENABLED_KEY,
            AvroAuditLogger.SLOW_THRESHOLD_MS_KEY,
            AvroAuditLogger.CONFIG_FILE_KEY,
    };

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        AvroAuditLogger.getInstance().close();
        for (String key : SYS_PROPS) {
            System.clearProperty(key);
        }
        System.setProperty(AvroAuditLogger.LOG_DIR_KEY, tempDir.toString());
        AvroAuditLogger.getInstance().resolveConfig();
        AvroAuditLogger.getInstance().resetAudit();
    }

    @AfterEach
    void cleanUp() {
        for (String key : SYS_PROPS) {
            System.clearProperty(key);
        }
        AvroAuditLogger.getInstance().resetAudit();
        AvroAuditLogger.getInstance().close();
    }

    private int archivedCount() {
        File dir = tempDir.toFile();
        File[] files = dir.listFiles((d, n) -> n.startsWith("audit-") && n.endsWith(".log"));
        return files == null ? 0 : files.length;
    }

    private boolean activeFileExists() {
        return Files.exists(tempDir.resolve("audit.log"));
    }

    // ─── Config resolution ────────────────────────────────────────────

    @Test
    void configDefaults() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        AuditSnapshot s = l.getSnapshot();
        assertNotNull(s);
        assertEquals(0, s.totalEntries());
        assertTrue(l.isEnabled());
    }

    @Test
    void configSyspropOverrides() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.LOG_FILE_PREFIX_KEY, "myapp");
        System.setProperty(AvroAuditLogger.LOG_FILE_SUFFIX_KEY, ".audit");
        l.resolveConfig();
        l.logRead("t", "scan", "x", 0);
        l.flush();
        assertTrue(Files.exists(tempDir.resolve("myapp.audit")),
                "Custom prefix/suffix should drive the active file name");
    }

    @Test
    void configInvalidValueFallsBackToDefault() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.LOG_MAX_SIZE_MB_KEY, "not_a_number");
        System.setProperty(AvroAuditLogger.SLOW_THRESHOLD_MS_KEY, "abc");
        l.resolveConfig();
        l.logRead("t", "scan", "still works", 0);
        AuditSnapshot s = l.getSnapshot();
        assertEquals(1, s.totalEntries(), "Invalid values should fall back without breaking recording");
    }

    @Test
    void configFileOverrideIsHonored() throws Exception {
        Properties p = new Properties();
        p.setProperty(AvroAuditLogger.ENABLED_KEY, "false");
        File cfg = tempDir.resolve("audit-test.properties").toFile();
        try (var out = Files.newOutputStream(cfg.toPath())) {
            p.store(out, "test");
        }
        System.setProperty(AvroAuditLogger.CONFIG_FILE_KEY, cfg.getAbsolutePath());
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.resolveConfig();
        l.resetAudit();
        l.logRead("t", "scan", "x", 0);
        assertEquals(0, l.getSnapshot().totalEntries(),
                "Disabled via config-file should suppress recording");
        assertFalse(activeFileExists(), "No audit file should be created while disabled");
    }

    @Test
    void configResolutionPriority_syspropOverConfigFile() throws Exception {
        Properties p = new Properties();
        p.setProperty(AvroAuditLogger.ENABLED_KEY, "false");
        File cfg = tempDir.resolve("audit-test.properties").toFile();
        try (var out = Files.newOutputStream(cfg.toPath())) {
            p.store(out, "test");
        }
        System.setProperty(AvroAuditLogger.ENABLED_KEY, "true");
        System.setProperty(AvroAuditLogger.CONFIG_FILE_KEY, cfg.getAbsolutePath());
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.resolveConfig();
        l.resetAudit();
        l.logRead("t", "scan", "x", 0);
        assertEquals(1, l.getSnapshot().totalEntries(),
                "System property should win over the config file");
    }

    @Test
    void disabledModeSuppressesEverything() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.ENABLED_KEY, "false");
        l.resolveConfig();
        l.resetAudit();
        l.logRead("t", "scan", "x", 5_000_000L);
        l.logError("t", "op", "boom", null);
        l.logBackup("t", "backup", "detail");
        AuditSnapshot s = l.getSnapshot();
        assertEquals(0, s.totalEntries());
        assertEquals(0, s.totalErrors());
        assertEquals(0, s.totalSlowOps());
        assertFalse(activeFileExists(), "Disabled logger must not create files");
    }

    // ─── Structured logging API ───────────────────────────────────────

    @Test
    void logCreatesEntry() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.log(AuditLevel.WARN, AuditCategory.QUERY, "custom", "T", "payload");
        AuditSnapshot s = l.getSnapshot();
        assertEquals(1, s.totalEntries());
        assertEquals(1, s.totalWarnings());
        List<AuditEntry> recent = l.getRecentEntries(10);
        assertEquals(1, recent.size());
        AuditEntry e = recent.get(0);
        assertEquals(AuditLevel.WARN, e.level());
        assertEquals(AuditCategory.QUERY, e.category());
        assertEquals("custom", e.operation());
        assertEquals("T", e.table());
        assertEquals("payload", e.detail());
    }

    @Test
    void logReadLogsStructuredEntry() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("USERS", "scan", "rows=100", 1_000_000L);
        List<AuditEntry> recent = l.getRecentEntries(5);
        assertEquals(1, recent.size());
        AuditEntry e = recent.get(0);
        assertEquals(AuditLevel.INFO, e.level());
        assertEquals(AuditCategory.READ, e.category());
        assertEquals("scan", e.operation());
        assertEquals("USERS", e.table());
        assertEquals(1, e.durationMs());
    }

    @Test
    void logWriteLogsStructuredEntry() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logWrite("ORDERS", "flush", "bytes=2048", 2_000_000L);
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditCategory.WRITE, e.category());
        assertEquals(2, e.durationMs());
    }

    @Test
    void logErrorIncrementsErrorCounter() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logError("T", "op", "boom", new IllegalArgumentException("nope"));
        AuditSnapshot s = l.getSnapshot();
        assertEquals(1, s.totalEntries());
        assertEquals(1, s.totalErrors());
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditLevel.ERROR, e.level());
        assertEquals(AuditCategory.ERROR, e.category());
        assertTrue(e.detail().contains("IllegalArgumentException"), e.detail());
        assertTrue(e.detail().contains("nope"), e.detail());
    }

    @Test
    void logErrorWithNullThrowable() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logError("T", "op", "plain", null);
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditLevel.ERROR, e.level());
        assertEquals("plain", e.detail());
    }

    @Test
    void logTransactionCarriesTxId() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logTransaction("ACCOUNTS", "COMMIT", "tx-42", null);
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditCategory.TRANSACTION, e.category());
        assertTrue(e.detail().contains("tx-42"), e.detail());
    }

    @Test
    void logConfigRecordsChange() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logConfig("avro.audit.enabled", "1", "2");
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditCategory.CONFIG, e.category());
        assertTrue(e.detail().contains("avro.audit.enabled=1 -> 2"), e.detail());
    }

    @Test
    void logBackupRecordsEvent() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logBackup("T", "full_backup", "scheduled");
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditCategory.BACKUP, e.category());
        assertEquals("full_backup", e.operation());
    }

    @Test
    void entriesAccumulate() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("a", "s1", null, 0);
        l.logRead("b", "s2", null, 0);
        l.logRead("c", "s3", null, 0);
        assertEquals(3, l.getSnapshot().totalEntries());
        assertEquals(3, l.getRecentEntries(10).size());
    }

    @Test
    void recentEntriesNewestFirst() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("t1", "a", null, 0);
        l.logRead("t2", "b", null, 0);
        l.logRead("t3", "c", null, 0);
        List<AuditEntry> two = l.getRecentEntries(2);
        assertEquals(2, two.size());
        assertEquals("t3", two.get(0).table());
        assertEquals("t2", two.get(1).table());
        List<AuditEntry> all = l.getRecentEntries(10);
        assertEquals("t1", all.get(2).table());
    }

    @Test
    void recentEntriesCutoffHandled() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("t", "a", null, 0);
        assertEquals(0, l.getRecentEntries(0).size());
    }

    @Test
    void recentEntriesBoundedWindow() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        for (int i = 0; i < 1010; i++) {
            l.logRead("t", "op", null, 0);
        }
        assertEquals(1010, l.getSnapshot().totalEntries());
        assertEquals(AvroAuditLogger.MAX_RECENT_ENTRIES, l.getRecentEntries(5000).size());
    }

    // ─── Performance tracing ──────────────────────────────────────────

    @Test
    void slowReadEscalatesToWarn() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.SLOW_THRESHOLD_MS_KEY, "1");
        l.resolveConfig();
        l.resetAudit();
        l.logRead("T", "scan", "slow", 10_000_000L); // 10ms > 1ms
        AuditSnapshot s = l.getSnapshot();
        assertEquals(1, s.totalSlowOps());
        assertEquals(1, s.totalWarnings());
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditLevel.WARN, e.level());
        assertEquals(10, e.durationMs());
    }

    @Test
    void fastReadStaysInfo() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.SLOW_THRESHOLD_MS_KEY, "1000");
        l.resolveConfig();
        l.resetAudit();
        l.logRead("T", "scan", "fast", 1_000_000L); // 1ms < 1000ms
        AuditSnapshot s = l.getSnapshot();
        assertEquals(0, s.totalSlowOps());
        assertEquals(0, s.totalWarnings());
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditLevel.INFO, e.level());
    }

    @Test
    void tracingDisabledDisablesSlowFlag() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.TRACING_ENABLED_KEY, "false");
        System.setProperty(AvroAuditLogger.SLOW_THRESHOLD_MS_KEY, "1");
        l.resolveConfig();
        l.resetAudit();
        l.logWrite("T", "flush", "slow-but-untracked", 50_000_000L); // 50ms
        AuditSnapshot s = l.getSnapshot();
        assertEquals(0, s.totalSlowOps(), "Tracing disabled must not count slow ops");
        assertEquals(0, s.totalWarnings());
        AuditEntry e = l.getRecentEntries(5).get(0);
        assertEquals(AuditLevel.INFO, e.level());
    }

    @Test
    void slowTraceEscalates() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.SLOW_THRESHOLD_MS_KEY, "1");
        l.resolveConfig();
        l.resetAudit();
        l.logTrace("repartition", "T", "heavy", 5_000_000L);
        assertEquals(1, l.getSnapshot().totalSlowOps());
        assertEquals(AuditLevel.WARN, l.getRecentEntries(5).get(0).level());
    }

    // ─── File output and rotation ─────────────────────────────────────

    @Test
    void firstWriteCreatesLogFile() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("T", "scan", "rows=1", 0);
        l.flush();
        assertTrue(activeFileExists(), "Writing an entry should create the active log file");
    }

    @Test
    void contentWrittenWithStructuredFormat() throws Exception {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("USERS", "scan", "rows=100", 0);
        l.flush();
        String text = Files.readString(tempDir.resolve("audit.log"), StandardCharsets.UTF_8);
        assertTrue(text.contains("|INFO|READ|scan|USERS|0|rows=100"),
                "Expected structured line, got: " + text.trim());
        assertTrue(text.contains("|INFO|"), text);
    }

    @Test
    void detailPipesSanitizedOnDisk() throws Exception {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("T", "scan", "a|b|c", 0);
        l.flush();
        String text = Files.readString(tempDir.resolve("audit.log"), StandardCharsets.UTF_8);
        assertTrue(text.contains("|a;b;c"), "Pipe in detail should be sanitized on disk");
    }

    @Test
    void rotationTriggersArchive() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.LOG_MAX_SIZE_MB_KEY, "0.001"); // ~1 KB
        System.setProperty(AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY, "true");
        l.resolveConfig();
        l.resetAudit();
        for (int i = 0; i < 40; i++) {
            l.logRead("T", "scan", "row-" + i, 0);
        }
        assertTrue(archivedCount() >= 1, "Expected at least one archived audit file");
        assertTrue(activeFileExists(), "Active file should be re-opened after rotation");
        assertEquals(40, l.getSnapshot().totalEntries());
    }

    @Test
    void rotationDisabledNeverArchives() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.ROTATION_ENABLED_KEY, "false");
        System.setProperty(AvroAuditLogger.LOG_MAX_SIZE_MB_KEY, "0.001");
        System.setProperty(AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY, "true");
        l.resolveConfig();
        l.resetAudit();
        for (int i = 0; i < 20; i++) {
            l.logRead("T", "scan", "row-" + i, 0);
        }
        assertEquals(0, archivedCount(), "Rotation disabled must not produce archives");
        assertTrue(activeFileExists());
        assertEquals(20, l.getSnapshot().totalEntries());
    }

    @Test
    void manualRotateArchivesCurrentFile() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY, "true");
        l.resolveConfig();
        l.logRead("T", "scan", "one", 0);
        int archived = l.rotateIfNeeded();
        assertEquals(1, archived, "Manual rotation should archive the active file");
        assertTrue(archivedCount() >= 1);
        l.logRead("T", "scan", "two", 0);
        assertEquals(2, l.getSnapshot().totalEntries());
    }

    @Test
    void rotationEmptyActiveFileNoArchive() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY, "true");
        l.resolveConfig();
        assertEquals(0, l.rotateIfNeeded(), "Nothing written -> nothing to archive");
    }

    // ─── Retention / pruning ──────────────────────────────────────────

    @Test
    void pruneOldLogsRemovesOldKeepsRecent() throws Exception {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.LOG_RETENTION_DAYS_KEY, "365");
        l.resolveConfig();
        l.logRead("T", "scan", "seed the active file", 0);
        l.flush();

        File old = tempDir.resolve("audit-old.log").toFile();
        Files.writeString(old.toPath(), "old", StandardCharsets.UTF_8);
        long oldTime = System.currentTimeMillis() - 400L * 24 * 60 * 60 * 1000; // 400 days ago
        assertTrue(old.setLastModified(oldTime), "Should set old timestamp");

        File recent = tempDir.resolve("audit-recent.log").toFile();
        Files.writeString(recent.toPath(), "recent", StandardCharsets.UTF_8);

        int pruned = l.pruneOldLogs();
        assertEquals(1, pruned, "Only the 400-day-old file should be pruned");
        assertFalse(old.exists(), "Old archived log should be deleted");
        assertTrue(recent.exists(), "Recent archived log should be kept");
        assertTrue(activeFileExists(), "Active file must never be pruned");
    }

    @Test
    void pruneWithNoDirIsNoOp() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.LOG_DIR_KEY,
                tempDir.resolve("does-not-exist").toString());
        l.resolveConfig();
        assertEquals(0, l.pruneOldLogs());
    }

    // ─── JSON export ──────────────────────────────────────────────────

    @Test
    void exportJsonEmptyInitially() {
        assertEquals("[]", AvroAuditLogger.getInstance().exportJson());
    }

    @Test
    void exportJsonContainsEntries() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("USERS", "scan", "rows=10", 1_000_000L);
        l.logError("USERS", "save", "io", null);
        String json = l.exportJson();
        assertTrue(json.startsWith("["), json);
        assertTrue(json.endsWith("]"), json);
        assertTrue(json.contains("\"operation\":\"scan\""), json);
        assertTrue(json.contains("\"table\":\"USERS\""), json);
        assertTrue(json.contains("\"level\":\"INFO\""), json);
        assertTrue(json.contains("\"level\":\"ERROR\""), json);
        assertTrue(json.contains("\"durationMs\":1"), json);
    }

    // ─── Snapshot / reset ─────────────────────────────────────────────

    @Test
    void snapshotReturnsCounters() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("t", "s", null, 0);
        l.logRead("t", "s", null, 0);
        l.logError("t", "e", "x", null);
        l.logWrite("t", "w", null, 0);
        AuditSnapshot s = l.getSnapshot();
        assertEquals(4, s.totalEntries());
        assertEquals(1, s.totalErrors());
        assertEquals(4, s.bufferedEntries());
        assertEquals(tempDir.toString(), s.logDir());
        assertEquals("audit.log", s.logFile());
        assertTrue(s.uptimeMs() >= 0);
    }

    @Test
    void resetClearsAll() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("t", "s", null, 0);
        l.logWrite("t", "w", null, 50_000_000L);
        l.resetAudit();
        AuditSnapshot s = l.getSnapshot();
        assertEquals(0, s.totalEntries());
        assertEquals(0, s.totalErrors());
        assertEquals(0, s.totalSlowOps());
        assertEquals(0, s.bufferedEntries());
    }

    // ─── Concurrency ──────────────────────────────────────────────────

    @Test
    void concurrentLoggingDoesNotLoseEntries() throws Exception {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        System.setProperty(AvroAuditLogger.FLUSH_ON_EVERY_WRITE_KEY, "true");
        l.resolveConfig();
        l.resetAudit();
        int threads = 8;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            final int id = t;
            pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    l.logRead("T" + id, "scan", "row-" + i, 0);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "Pool should finish");
        assertEquals(threads * perThread, l.getSnapshot().totalEntries(),
                "All entries must be recorded under concurrency");
        assertTrue(activeFileExists());
    }

    // ─── Close lifecycle ──────────────────────────────────────────────

    @Test
    void closeFlushesAndIsIdempotent() throws Exception {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("T", "scan", "before close", 0);
        l.close();
        l.close();
        String text = Files.readString(tempDir.resolve("audit.log"), StandardCharsets.UTF_8);
        assertTrue(text.contains("before close"), "Close should flush pending entries");
    }

    @Test
    void logAfterCloseReopensFile() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        l.logRead("T", "one", null, 0);
        l.close();
        l.logRead("T", "two", null, 0);
        l.flush();
        assertEquals(2, l.getSnapshot().totalEntries());
        assertTrue(activeFileExists());
    }

    @Test
    void getCurrentLogFileReportsActive() {
        AvroAuditLogger l = AvroAuditLogger.getInstance();
        assertNull(l.getCurrentLogFile(), "No file before first write");
        l.logRead("T", "scan", null, 0);
        assertNotNull(l.getCurrentLogFile());
        assertEquals("audit.log", l.getCurrentLogFile().getName());
    }
}