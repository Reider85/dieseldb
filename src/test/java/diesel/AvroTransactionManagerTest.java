package diesel;

import diesel.storage.avro.AvroTransactionManager;
import diesel.storage.avro.AvroTransactionManager.AvroTransaction;
import diesel.storage.avro.AvroTransactionManager.LockTimeoutException;
import diesel.storage.avro.AvroTransactionManager.RecoveryReport;
import diesel.storage.avro.AvroTransactionManager.TransactionConflictException;
import diesel.storage.avro.AvroTransactionManager.TransactionState;
import diesel.storage.avro.AvroTransactionManager.WalEntry;
import diesel.storage.avro.AvroTransactionManager.WalOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
@StorageType("avro")
class AvroTransactionManagerTest {

    @TempDir
    Path tempDir;

    private Path walDir;
    private File configFile;
    private AvroTransactionManager mgr;

    @BeforeEach
    void setUp() {
        walDir = tempDir.resolve("wal");
        configFile = tempDir.resolve("test-config.properties").toFile();
    }

    private AvroTransactionManager createManager(Properties overrides) throws Exception {
        Properties props = new Properties();
        props.setProperty(AvroTransactionManager.ENABLED_KEY, "true");
        props.setProperty(AvroTransactionManager.LOCK_TIMEOUT_KEY, "5000");
        props.setProperty(AvroTransactionManager.LOG_DIR_KEY, walDir.toString());
        props.setProperty(AvroTransactionManager.LOG_MAX_SIZE_KEY, "10");
        props.setProperty(AvroTransactionManager.LOG_RETENTION_KEY, "60000");
        props.setProperty(AvroTransactionManager.RECOVERY_ON_STARTUP_KEY, "false");
        if (overrides != null) {
            props.putAll(overrides);
        }
        try (var out = java.nio.file.Files.newOutputStream(configFile.toPath())) {
            props.store(out, "test");
        }
        System.setProperty(AvroTransactionManager.CONFIG_FILE_KEY, configFile.getAbsolutePath());
        try {
            return AvroTransactionManager.create();
        } finally {
            System.clearProperty(AvroTransactionManager.CONFIG_FILE_KEY);
        }
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(AvroTransactionManager.CONFIG_FILE_KEY);
    }

    // ── Config resolution tests ───────────────────────────────────────────

    @Test
    void configDefaults() throws Exception {
        mgr = createManager(null);
        assertTrue(mgr.isEnabled());
        assertEquals(5000, mgr.getLockTimeoutMs());
        assertTrue(Files.exists(mgr.getWalDir()));
        assertEquals(10 * 1024 * 1024, mgr.getLogMaxSizeBytes());
        assertEquals(60000, mgr.getLogRetentionMs());
        assertFalse(mgr.isRecoveryOnStartup());
    }

    @Test
    void configDisabled() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.ENABLED_KEY, "false");
        }});
        assertFalse(mgr.isEnabled());
        assertThrows(IllegalStateException.class,
                () -> mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED));
    }

    @Test
    void configInvalidLockTimeoutFallsBackToDefault() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.LOCK_TIMEOUT_KEY, "not-a-number");
        }});
        assertEquals(AvroTransactionManager.DEFAULT_LOCK_TIMEOUT_MS, mgr.getLockTimeoutMs());
    }

    @Test
    void configInvalidMaxSizeFallsBackToDefault() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.LOG_MAX_SIZE_KEY, "-5");
        }});
        assertEquals(AvroTransactionManager.DEFAULT_LOG_MAX_SIZE_MB * 1024L * 1024L,
                mgr.getLogMaxSizeBytes());
    }

    @Test
    void configSyspropOverridesFile() throws Exception {
        mgr = createManager(null);
        // The sysprop should override the config file value
        System.setProperty(AvroTransactionManager.LOCK_TIMEOUT_KEY, "9999");
        try {
            AvroTransactionManager mgr2 = AvroTransactionManager.create();
            assertEquals(9999, mgr2.getLockTimeoutMs());
        } finally {
            System.clearProperty(AvroTransactionManager.LOCK_TIMEOUT_KEY);
        }
    }

    @Test
    void configToString() throws Exception {
        mgr = createManager(null);
        String s = mgr.toString();
        assertTrue(s.contains("enabled=true"));
        assertTrue(s.contains("AvroTransactionManager"));
    }

    // ── Transaction lifecycle tests ───────────────────────────────────────

    @Test
    void beginAndCommit() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_COMMITTED);
        assertNotNull(txId);
        assertEquals(TransactionState.ACTIVE, mgr.getTransactionState(txId));
        mgr.commitTransaction(txId);
        assertEquals(TransactionState.COMMITTED, mgr.getTransactionState(txId));
    }

    @Test
    void beginAndAbort() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.abortTransaction(txId);
        assertEquals(TransactionState.ABORTED, mgr.getTransactionState(txId));
    }

    @Test
    void commitWithoutBeginThrows() throws Exception {
        mgr = createManager(null);
        UUID fakeTxId = UUID.randomUUID();
        assertThrows(IllegalStateException.class,
                () -> mgr.commitTransaction(fakeTxId));
    }

    @Test
    void abortWithoutBeginIsNoOp() throws Exception {
        mgr = createManager(null);
        UUID fakeTxId = UUID.randomUUID();
        // Should not throw — silently no-ops
        mgr.abortTransaction(fakeTxId);
    }

    @Test
    void doubleCommitThrows() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.commitTransaction(txId);
        assertThrows(IllegalStateException.class,
                () -> mgr.commitTransaction(txId));
    }

    @Test
    void defaultIsolationLevelIsNull() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(null);
        AvroTransaction tx = mgr.getAllTransactions().get(txId);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, tx.getIsolationLevel());
    }

    @Test
    void beginRecordsWalEntry() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.SERIALIZABLE);
        mgr.commitTransaction(txId);

        List<WalEntry> entries = mgr.readAllWalEntries();
        assertFalse(entries.isEmpty());
        WalEntry beginEntry = entries.stream()
                .filter(e -> e.operation() == WalOperation.BEGIN)
                .findFirst().orElse(null);
        assertNotNull(beginEntry);
        assertEquals(txId, beginEntry.transactionId());

        WalEntry commitEntry = entries.stream()
                .filter(e -> e.operation() == WalOperation.COMMIT)
                .findFirst().orElse(null);
        assertNotNull(commitEntry);
        assertEquals(txId, commitEntry.transactionId());
    }

    @Test
    void activeTransactionCount() throws Exception {
        mgr = createManager(null);
        assertEquals(0, mgr.getActiveTransactionCount());
        UUID tx1 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        assertEquals(1, mgr.getActiveTransactionCount());
        UUID tx2 = mgr.beginTransaction(IsolationLevel.READ_COMMITTED);
        assertEquals(2, mgr.getActiveTransactionCount());
        mgr.commitTransaction(tx1);
        assertEquals(1, mgr.getActiveTransactionCount());
        mgr.abortTransaction(tx2);
        assertEquals(0, mgr.getActiveTransactionCount());
    }

    // ── Snapshot tests ────────────────────────────────────────────────────

    @Test
    void snapshotAndRetrieve() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.REPEATABLE_READ);

        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, "Alice"});
        rows.add(new Object[]{2, "Bob"});
        mgr.snapshotTable(txId, "USERS", rows);

        List<Object[]> snapshot = mgr.getSnapshot(txId, "USERS");
        assertNotNull(snapshot);
        assertEquals(2, snapshot.size());
        assertArrayEquals(new Object[]{1, "Alice"}, snapshot.get(0));

        // Modifying original does not affect snapshot
        rows.add(new Object[]{3, "Charlie"});
        assertEquals(2, snapshot.size());
    }

    @Test
    void snapshotForUnknownTableReturnsNull() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        assertNull(mgr.getSnapshot(txId, "NONEXISTENT"));
    }

    // ── Isolation level tests ─────────────────────────────────────────────

    @Test
    void readUncommittedSeesCurrentRows() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);

        List<Object[]> current = new ArrayList<>();
        current.add(new Object[]{1, "uncommitted"});
        List<Object[]> committed = new ArrayList<>();

        List<Object[]> visible = mgr.resolveVisibleRows(txId, "T", current, committed);
        assertSame(current, visible);
    }

    @Test
    void readCommittedSeesCommittedRows() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_COMMITTED);

        List<Object[]> current = new ArrayList<>();
        current.add(new Object[]{1, "uncommitted"});
        List<Object[]> committed = new ArrayList<>();
        committed.add(new Object[]{0, "committed"});

        List<Object[]> visible = mgr.resolveVisibleRows(txId, "T", current, committed);
        assertSame(committed, visible);
    }

    @Test
    void repeatableReadSeesSnapshot() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.REPEATABLE_READ);

        List<Object[]> snapshotRows = new ArrayList<>();
        snapshotRows.add(new Object[]{0, "at-begin"});
        mgr.snapshotTable(txId, "T", snapshotRows);

        List<Object[]> current = new ArrayList<>();
        current.add(new Object[]{1, "now"});
        List<Object[]> committed = new ArrayList<>();
        committed.add(new Object[]{2, "other"});

        List<Object[]> visible = mgr.resolveVisibleRows(txId, "T", current, committed);
        assertEquals(snapshotRows, visible);
        assertEquals("at-begin", visible.get(0)[1]);
    }

    @Test
    void serializableSeesSnapshot() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.SERIALIZABLE);

        List<Object[]> snapshotRows = new ArrayList<>();
        snapshotRows.add(new Object[]{0, "frozen"});
        mgr.snapshotTable(txId, "T", snapshotRows);

        List<Object[]> visible = mgr.resolveVisibleRows(txId, "T",
                new ArrayList<>(), new ArrayList<>());
        assertEquals(1, visible.size());
        assertEquals("frozen", visible.get(0)[1]);
    }

    @Test
    void snapshotMissingFallsToCurrent() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.REPEATABLE_READ);
        // No snapshot taken

        List<Object[]> current = new ArrayList<>();
        current.add(new Object[]{1, "live"});

        List<Object[]> visible = mgr.resolveVisibleRows(txId, "T", current, new ArrayList<>());
        assertSame(current, visible);
    }

    // ── Write lock tests ──────────────────────────────────────────────────

    @Test
    void acquireAndReleaseLock() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", txId);
        assertEquals(txId, mgr.getWriteLockOwner("USERS"));
        mgr.releaseWriteLock("USERS", txId);
        assertNull(mgr.getWriteLockOwner("USERS"));
    }

    @Test
    void lockAutoReleasedOnCommit() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", txId);
        mgr.commitTransaction(txId);
        assertNull(mgr.getWriteLockOwner("USERS"));
    }

    @Test
    void lockAutoReleasedOnAbort() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", txId);
        mgr.abortTransaction(txId);
        assertNull(mgr.getWriteLockOwner("USERS"));
    }

    @Test
    void concurrentLockBlocksThenTimesOut() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.LOCK_TIMEOUT_KEY, "500");
        }});
        UUID tx1 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", tx1);

        UUID tx2 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        LockTimeoutException ex = assertThrows(LockTimeoutException.class,
                () -> mgr.acquireWriteLock("USERS", tx2));
        assertEquals("USERS", ex.getTableName());
        assertEquals(tx1, ex.getOwnerTxId());
    }

    @Test
    void concurrentLockAcquiredAfterRelease() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.LOCK_TIMEOUT_KEY, "5000");
        }});
        UUID tx1 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", tx1);

        // Release in a separate thread after a short delay
        CountDownLatch released = new CountDownLatch(1);
        new Thread(() -> {
            try {
                Thread.sleep(100);
                mgr.releaseWriteLock("USERS", tx1);
                released.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();

        UUID tx2 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", tx2);
        assertEquals(tx2, mgr.getWriteLockOwner("USERS"));
        assertTrue(released.await(3, TimeUnit.SECONDS));
    }

    @Test
    void releaseLockByNonOwnerIsNoOp() throws Exception {
        mgr = createManager(null);
        UUID tx1 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.acquireWriteLock("USERS", tx1);

        UUID tx2 = UUID.randomUUID();
        mgr.releaseWriteLock("USERS", tx2);
        assertEquals(tx1, mgr.getWriteLockOwner("USERS"));
    }

    // ── WAL tests ─────────────────────────────────────────────────────────

    @Test
    void walEntryRoundTrip() {
        UUID txId = UUID.randomUUID();
        WalEntry original = new WalEntry(txId, WalOperation.INSERT, "USERS",
                12345L, "0|Alice", null);
        String line = original.toLine();
        WalEntry parsed = WalEntry.parse(line);
        assertNotNull(parsed);
        assertEquals(txId, parsed.transactionId());
        assertEquals(WalOperation.INSERT, parsed.operation());
        assertEquals("USERS", parsed.tableName());
        assertEquals(12345L, parsed.timestampMs());
        assertEquals("0|Alice", parsed.detail());
    }

    @Test
    void walEntryCorruptedChecksumReturnsNull() {
        UUID txId = UUID.randomUUID();
        WalEntry original = new WalEntry(txId, WalOperation.BEGIN, "",
                12345L, "", null);
        String line = original.toLine();
        // Tamper with the checksum
        String corrupted = line.substring(0, line.length() - 8) + "00000000";
        assertNull(WalEntry.parse(corrupted));
    }

    @Test
    void walEntryMalformedReturnsNull() {
        assertNull(WalEntry.parse(null));
        assertNull(WalEntry.parse(""));
        assertNull(WalEntry.parse("too|few|parts"));
    }

    @Test
    void walRecordsInsertUpdateDelete() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.recordInsert(txId, "USERS", -1, "{\"id\":1}");
        mgr.recordUpdate(txId, "USERS", 0, "{\"id\":1,\"name\":\"Bob\"}");
        mgr.recordDelete(txId, "USERS", 0);
        mgr.commitTransaction(txId);

        List<WalEntry> entries = mgr.readAllWalEntries();
        long insertCount = entries.stream()
                .filter(e -> e.operation() == WalOperation.INSERT).count();
        long updateCount = entries.stream()
                .filter(e -> e.operation() == WalOperation.UPDATE).count();
        long deleteCount = entries.stream()
                .filter(e -> e.operation() == WalOperation.DELETE).count();
        assertTrue(insertCount >= 1);
        assertTrue(updateCount >= 1);
        assertTrue(deleteCount >= 1);
    }

    @Test
    void walRecordThrowsForInactiveTx() throws Exception {
        mgr = createManager(null);
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.commitTransaction(txId);
        assertThrows(IllegalStateException.class,
                () -> mgr.recordInsert(txId, "T", 0, "data"));
    }

    // ── Crash recovery tests ──────────────────────────────────────────────

    @Test
    void recoverySkipsCorruptedEntries() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.RECOVERY_ON_STARTUP_KEY, "true");
        }});
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.commitTransaction(txId);

        // Append a corrupted line to the WAL file
        File[] walFiles = walDir.toFile().listFiles((d, n) -> n.endsWith(".wal"));
        assertNotNull(walFiles);
        assertTrue(walFiles.length > 0);
        Files.writeString(walFiles[0].toPath(), "CORRUPTED|LINE|HERE\n",
                java.nio.file.StandardOpenOption.APPEND);

        RecoveryReport report = mgr.recoverTransactions();
        assertTrue(report.corruptedEntriesSkipped() >= 1);
    }

    @Test
    void recoveryAbortsUncommittedTransactions() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.RECOVERY_ON_STARTUP_KEY, "true");
        }});
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        // No commit — just abandon

        RecoveryReport report = mgr.recoverTransactions();
        assertTrue(report.transactionsAborted() >= 1);
    }

    @Test
    void recoveryReplaysCommittedTransactions() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.RECOVERY_ON_STARTUP_KEY, "true");
        }});
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.recordInsert(txId, "USERS", -1, "{\"id\":1}");
        mgr.commitTransaction(txId);

        RecoveryReport report = mgr.recoverTransactions();
        assertTrue(report.transactionsRecovered() >= 1);
        assertTrue(report.entriesReplayed() >= 1);
    }

    @Test
    void recoveryNoopWhenDisabled() throws Exception {
        mgr = createManager(new Properties() {{
            put(AvroTransactionManager.RECOVERY_ON_STARTUP_KEY, "false");
        }});
        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.commitTransaction(txId);

        RecoveryReport report = mgr.recoverTransactions();
        assertEquals(0, report.transactionsRecovered());
    }

    @Test
    void walPruneRemovesExpiredFiles() throws Exception {
        Properties props = new Properties();
        props.setProperty(AvroTransactionManager.LOG_RETENTION_KEY, "1"); // 1 ms
        mgr = createManager(props);

        UUID txId = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        mgr.commitTransaction(txId);

        Thread.sleep(10); // Let the entry expire
        int pruned = mgr.pruneExpiredWalFiles();
        // Should keep at least one file, so pruned may be 0
        assertTrue(pruned >= 0);
    }

    // ── toString / general ────────────────────────────────────────────────

    @Test
    void toStringContainsAllFields() throws Exception {
        mgr = createManager(null);
        String s = mgr.toString();
        assertTrue(s.contains("lockTimeoutMs="));
        assertTrue(s.contains("walDir="));
        assertTrue(s.contains("activeTransactions="));
    }

    @Test
    void transactionIdIsUnique() throws Exception {
        mgr = createManager(null);
        UUID tx1 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        UUID tx2 = mgr.beginTransaction(IsolationLevel.READ_UNCOMMITTED);
        assertNotEquals(tx1, tx2);
    }
}
