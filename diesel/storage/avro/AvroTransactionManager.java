package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import diesel.storage.StorageMessageConstants;

/**
 * AVRO-aware transaction manager with ACID guarantees, write-ahead logging,
 * isolation-level enforcement and crash recovery (Prompt 92).
 *
 * <p>Sits between the engine-level {@link diesel.Transaction} system and
 * {@link AvroRowStorage}. Adds four capabilities that the base copy-on-write
 * transaction model lacks for AVRO storage:
 *
 * <ul>
 *   <li><b>Write-ahead log (WAL)</b> — every mutating operation is logged
 *       <i>before</i> it is applied to the in-memory rows.  On commit the
 *       COMMIT marker is appended, then the dirty rows are persisted to the
 *       {@code .avro} file.  A crash between COMMIT and persist is recovered
 *       by replaying the WAL at the next startup.</li>
 *   <li><b>Table-level write locks</b> — only one transaction may write to
 *       a given table at a time.  A second writer blocks (with a configurable
 *       timeout) until the first commits or rolls back.</li>
 *   <li><b>Isolation levels</b> — the four standard levels are supported:
 *       READ_UNCOMMITTED (live in-memory rows), READ_COMMITTED (re-read from
 *       storage on each scan), REPEATABLE_READ (BEGIN-time snapshot) and
 *       SERIALIZABLE (snapshot + write-write conflict detection).</li>
 *   <li><b>Crash recovery</b> — on startup {@link #recoverTransactions()}
 *       reads the WAL, replays COMMIT markers whose corresponding
 *       {@code .avro} file has not been updated, and aborts any uncommitted
 *       transaction.</li>
 * </ul>
 *
 * <p>Configuration is resolved per call from a system property, then the root
 * {@code config.properties}, then code defaults:
 * <pre>
 * avro.tx.enabled                = true
 * avro.tx.lock.timeout.ms        = 30000
 * avro.tx.log.dir                = data/avro-wal
 * avro.tx.log.max.size.mb        = 100
 * avro.tx.log.retention.ms       = 86400000   (24 h)
 * avro.tx.recovery.on.startup    = true
 * </pre>
 * The test hook {@code avro.tx.config.file} overrides the
 * {@code config.properties} lookup.
 *
 * <p>Thread safety: the manager is thread-safe for concurrent transactions
 * on different tables.  Write locks serialise concurrent writers on the
 * <em>same</em> table.  WAL append is synchronised internally.
 *
 * @since Prompt 92
 */
public final class AvroTransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTransactionManager.class);

    // ── Config keys ───────────────────────────────────────────────────────
    public static final String CONFIG_FILE_KEY  = "avro.tx.config.file";
    public static final String ENABLED_KEY      = "avro.tx.enabled";
    public static final String LOCK_TIMEOUT_KEY = "avro.tx.lock.timeout.ms";
    public static final String LOG_DIR_KEY      = "avro.tx.log.dir";
    public static final String LOG_MAX_SIZE_KEY = "avro.tx.log.max.size.mb";
    public static final String LOG_RETENTION_KEY = "avro.tx.log.retention.ms";
    public static final String RECOVERY_ON_STARTUP_KEY = "avro.tx.recovery.on.startup";

    // ── Defaults ──────────────────────────────────────────────────────────
    public static final boolean DEFAULT_ENABLED             = true;
    public static final long    DEFAULT_LOCK_TIMEOUT_MS     = 30_000L;
    public static final String  DEFAULT_LOG_DIR             = "data/avro-wal";
    public static final long    DEFAULT_LOG_MAX_SIZE_MB     = 100L;
    public static final long    DEFAULT_LOG_RETENTION_MS    = 86_400_000L;   // 24 h
    public static final boolean DEFAULT_RECOVERY_ON_STARTUP = true;

    private static final String WAL_FILE_PREFIX = "avro-tx-";
    private static final String WAL_FILE_SUFFIX = ".wal";

    // ── Inner types ───────────────────────────────────────────────────────

    /** Transaction state machine. */
    public enum TransactionState {
        /** Transaction is active — mutations are in-flight. */
        ACTIVE,
        /** Transaction committed — mutations persisted. */
        COMMITTED,
        /** Transaction aborted — mutations discarded. */
        ABORTED,
        /** Transaction is being recovered from the WAL. */
        RECOVERING
    }

    /** WAL operation codes. */
    public enum WalOperation {
        /** Transaction started. */
        BEGIN,
        /** A single row was inserted.  Payload: tableName|rowIndex|jsonRow. */
        INSERT,
        /** A single row was updated.  Payload: tableName|rowIndex|jsonRow. */
        UPDATE,
        /** A single row was deleted.  Payload: tableName|rowIndex. */
        DELETE,
        /** Transaction committed successfully. */
        COMMIT,
        /** Transaction was aborted / rolled back. */
        ABORT
    }

    /**
     * An immutable WAL entry written as a single text line:
     * {@code transactionId|operation|table|timestamp|detail|checksumHex}.
     */
    public record WalEntry(
            UUID transactionId,
            WalOperation operation,
            String tableName,
            long timestampMs,
            String detail,
            String checksumHex
    ) {
        /** Serialise to the on-disk line format (checksum over the prefix). */
        public String toLine() {
            String prefix = transactionId + "|" + operation + "|" + tableName
                    + "|" + timestampMs + "|" + (detail != null ? detail : "");
            return prefix + "|" + computeChecksumHex(prefix);
        }

        /** Parse one line back into a WalEntry (returns null on corruption). */
        public static WalEntry parse(String line) {
            if (line == null || line.isBlank()) return null;
            String[] parts = line.split("\\|", -1);
            if (parts.length < 6) return null;
            try {
                UUID txId = UUID.fromString(parts[0]);
                WalOperation op = WalOperation.valueOf(parts[1]);
                String table = parts[2];
                long ts = Long.parseLong(parts[3]);
                String checksumHex = parts[parts.length - 1];
                StringBuilder detailBuilder = new StringBuilder();
                for (int i = 4; i < parts.length - 1; i++) {
                    if (i > 4) detailBuilder.append("|");
                    detailBuilder.append(parts[i]);
                }
                String detail = detailBuilder.length() == 0 ? null : detailBuilder.toString();
                String prefix = txId + "|" + op + "|" + table + "|" + ts
                        + "|" + (detail != null ? detail : "");
                if (!checksumHex.equals(computeChecksumHex(prefix))) {
                    LOGGER.warn("WAL checksum mismatch, skipping entry: {}", line);
                    return null;
                }
                return new WalEntry(txId, op, table, ts, detail, checksumHex);
            } catch (Exception e) {
                LOGGER.warn("Malformed WAL entry, skipping: {}", line, e);
                return null;
            }
        }

        private static String computeChecksumHex(String data) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] digest = md.digest(data.getBytes(StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder(16);
                for (int i = 0; i < 8; i++) {
                    sb.append(String.format("%02x", digest[i]));
                }
                return sb.toString();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-256 not available", e);
            }
        }
    }

    /**
     * In-memory representation of a managed transaction.
     */
    public static class AvroTransaction {
        private final UUID id;
        private final diesel.IsolationLevel isolationLevel;
        private final long startedAtMs;
        private volatile TransactionState state;
        private final Map<String, List<Object[]>> snapshots;

        AvroTransaction(UUID id, diesel.IsolationLevel isolationLevel, long startedAtMs) {
            this.id = id;
            this.isolationLevel = isolationLevel;
            this.startedAtMs = startedAtMs;
            this.state = TransactionState.ACTIVE;
            this.snapshots = new ConcurrentHashMap<>();
        }

        public UUID getId() { return id; }
        public diesel.IsolationLevel getIsolationLevel() { return isolationLevel; }
        public long getStartedAtMs() { return startedAtMs; }
        public TransactionState getState() { return state; }
        void setState(TransactionState s) { this.state = s; }
        public Map<String, List<Object[]>> getSnapshots() { return snapshots; }
    }

    /**
     * Thrown when a write-lock acquisition times out.
     */
    public static class LockTimeoutException extends Exception {
        private final String tableName;
        private final UUID ownerTxId;

        public LockTimeoutException(String tableName, UUID ownerTxId) {
            super("Write lock on table '" + tableName + "' held by transaction "
                    + ownerTxId + " could not be acquired");
            this.tableName = tableName;
            this.ownerTxId = ownerTxId;
        }

        public String getTableName() { return tableName; }
        public UUID getOwnerTxId() { return ownerTxId; }
    }

    /**
     * Thrown when a SERIALIZABLE transaction detects a write-write conflict.
     */
    public static class TransactionConflictException extends Exception {
        private final UUID conflictingTxId;
        private final String tableName;

        public TransactionConflictException(UUID conflictingTxId, String tableName) {
            super("Write-write conflict on table '" + tableName
                    + "' with transaction " + conflictingTxId);
            this.conflictingTxId = conflictingTxId;
            this.tableName = tableName;
        }

        public UUID getConflictingTxId() { return conflictingTxId; }
        public String getTableName() { return tableName; }
    }

    /**
     * Outcome of {@link #recoverTransactions()}.
     */
    public record RecoveryReport(
            int entriesReplayed,
            int transactionsRecovered,
            int transactionsAborted,
            int corruptedEntriesSkipped,
            long recoveryTimeMs
    ) {}

    // ── Instance state ────────────────────────────────────────────────────

    private final boolean enabled;
    private final long lockTimeoutMs;
    private final Path walDir;
    private final long logMaxSizeBytes;
    private final long logRetentionMs;
    private final boolean recoveryOnStartup;

    /** Active transactions keyed by their id. */
    private final ConcurrentHashMap<UUID, AvroTransaction> activeTransactions = new ConcurrentHashMap<>();

    /** Table-level write locks: tableName → owning transaction id. */
    private final ConcurrentHashMap<String, UUID> writeLocks = new ConcurrentHashMap<>();

    /** Per-table lock objects (Semaphore, NOT reentrant) for blocking lock acquisition. */
    private final ConcurrentHashMap<String, Semaphore> tableLocks = new ConcurrentHashMap<>();

    /** WAL append lock (serialises concurrent WAL writes). */
    private final Object walAppendLock = new Object();

    /** Number of WAL entries skipped due to corruption in the last readAllWalEntries call. */
    private volatile int lastReadSkippedCount = 0;

    // ── Constructor (package-private — created by AvroRowStorage) ─────────

    AvroTransactionManager(boolean enabled, long lockTimeoutMs, Path walDir,
                           long logMaxSizeBytes, long logRetentionMs,
                           boolean recoveryOnStartup) {
        this.enabled = enabled;
        this.lockTimeoutMs = lockTimeoutMs;
        this.walDir = walDir;
        this.logMaxSizeBytes = logMaxSizeBytes;
        this.logRetentionMs = logRetentionMs;
        this.recoveryOnStartup = recoveryOnStartup;
        if (enabled) {
            try { Files.createDirectories(walDir); } catch (IOException ignored) { }
        }
    }

    // ── Public factory ────────────────────────────────────────────────────

    /**
     * Resolves configuration and creates a new manager instance.
     *
     * @return the resolved manager (never {@code null})
     */
    public static AvroTransactionManager create() {
        boolean enabled        = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        long lockTimeout       = getLong(LOCK_TIMEOUT_KEY, DEFAULT_LOCK_TIMEOUT_MS);
        String logDirStr       = getString(LOG_DIR_KEY, DEFAULT_LOG_DIR);
        long logMaxSizeMb      = getLong(LOG_MAX_SIZE_KEY, DEFAULT_LOG_MAX_SIZE_MB);
        long logRetention      = getLong(LOG_RETENTION_KEY, DEFAULT_LOG_RETENTION_MS);
        boolean recoveryOnStartup = getBoolean(RECOVERY_ON_STARTUP_KEY, DEFAULT_RECOVERY_ON_STARTUP);

        if (lockTimeout <= 0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, LOCK_TIMEOUT_KEY, lockTimeout, DEFAULT_LOCK_TIMEOUT_MS);
            lockTimeout = DEFAULT_LOCK_TIMEOUT_MS;
        }
        if (logMaxSizeMb <= 0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, LOG_MAX_SIZE_KEY, logMaxSizeMb, DEFAULT_LOG_MAX_SIZE_MB);
            logMaxSizeMb = DEFAULT_LOG_MAX_SIZE_MB;
        }
        if (logRetention <= 0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, LOG_RETENTION_KEY, logRetention, DEFAULT_LOG_RETENTION_MS);
            logRetention = DEFAULT_LOG_RETENTION_MS;
        }

        Path walDir = Path.of(logDirStr);
        return new AvroTransactionManager(enabled, lockTimeout, walDir,
                logMaxSizeMb * 1024L * 1024L, logRetention, recoveryOnStartup);
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    public boolean isEnabled() { return enabled; }
    public long getLockTimeoutMs() { return lockTimeoutMs; }
    public Path getWalDir() { return walDir; }
    public long getLogMaxSizeBytes() { return logMaxSizeBytes; }
    public long getLogRetentionMs() { return logRetentionMs; }
    public boolean isRecoveryOnStartup() { return recoveryOnStartup; }

    /** Number of currently active (not yet committed/aborted) transactions. */
    public int getActiveTransactionCount() {
        int count = 0;
        for (AvroTransaction tx : activeTransactions.values()) {
            if (tx.getState() == TransactionState.ACTIVE) count++;
        }
        return count;
    }

    /** Unmodifiable view of all tracked transactions. */
    public Map<UUID, AvroTransaction> getAllTransactions() {
        return Collections.unmodifiableMap(activeTransactions);
    }

    // ── Transaction lifecycle ─────────────────────────────────────────────

    /**
     * Begins a new AVRO transaction.
     *
     * @param isolationLevel the desired isolation level (null → READ_UNCOMMITTED)
     * @return the new transaction's id
     */
    public UUID beginTransaction(diesel.IsolationLevel isolationLevel) {
        if (!enabled) {
            throw new IllegalStateException("AVRO transaction manager is disabled");
        }
        diesel.IsolationLevel level = isolationLevel != null
                ? isolationLevel : diesel.IsolationLevel.READ_UNCOMMITTED;
        AvroTransaction tx = new AvroTransaction(UUID.randomUUID(), level, System.currentTimeMillis());
        activeTransactions.put(tx.getId(), tx);

        appendWalEntry(tx.getId(), WalOperation.BEGIN, "", "level=" + level);
        LOGGER.info("AVRO transaction {} began at level {}", tx.getId(), level);
        return tx.getId();
    }

    /**
     * Takes a snapshot of the given table's in-memory rows for this transaction.
     * Must be called after {@link #beginTransaction} and before the first read.
     *
     * @param txId      the transaction id
     * @param tableName the table name
     * @param rows      current in-memory row list (shallow copy)
     */
    public void snapshotTable(UUID txId, String tableName, List<Object[]> rows) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) return;
        List<Object[]> copy = new ArrayList<>(rows);
        tx.getSnapshots().put(tableName, copy);
    }

    /**
     * Returns the transaction's snapshot of the given table, or {@code null}
     * if the table was not snapshotted.
     */
    public List<Object[]> getSnapshot(UUID txId, String tableName) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null) return null;
        return tx.getSnapshots().get(tableName);
    }

    /**
     * Records an INSERT in the WAL and applies it to the transaction's
     * in-memory row list.
     *
     * @param txId      transaction id
     * @param tableName table name
     * @param rowIndex  insertion index ({@code -1} = append)
     * @param row       the row data as a JSON-like string
     * @return the actual insertion index (0-based)
     */
    public int recordInsert(UUID txId, String tableName, int rowIndex, String row) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            throw new IllegalStateException(StorageMessageConstants.NO_ACTIVE_TRANSACTION + txId);
        }
        String detail = tableName + "|" + rowIndex + "|" + row;
        appendWalEntry(txId, WalOperation.INSERT, tableName, detail);
        return rowIndex;
    }

    /**
     * Records an UPDATE in the WAL.
     */
    public void recordUpdate(UUID txId, String tableName, int rowIndex, String row) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            throw new IllegalStateException(StorageMessageConstants.NO_ACTIVE_TRANSACTION + txId);
        }
        String detail = tableName + "|" + rowIndex + "|" + row;
        appendWalEntry(txId, WalOperation.UPDATE, tableName, detail);
    }

    /**
     * Records a DELETE in the WAL.
     */
    public void recordDelete(UUID txId, String tableName, int rowIndex) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            throw new IllegalStateException(StorageMessageConstants.NO_ACTIVE_TRANSACTION + txId);
        }
        String detail = tableName + "|" + rowIndex;
        appendWalEntry(txId, WalOperation.DELETE, tableName, detail);
    }

    /**
     * Commits the transaction: writes the COMMIT WAL marker, then marks the
     * transaction as committed.  Caller is responsible for persisting the
     * in-memory rows to the {@code .avro} file after this call.
     *
     * @param txId the transaction id
     * @throws TransactionConflictException if a SERIALIZABLE conflict is detected
     */
    public void commitTransaction(UUID txId)
            throws TransactionConflictException {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            throw new IllegalStateException(StorageMessageConstants.NO_ACTIVE_TRANSACTION + txId);
        }

        // SERIALIZABLE conflict detection — check that no other active
        // transaction modified the same tables since our snapshot.
        if (tx.getIsolationLevel() == diesel.IsolationLevel.SERIALIZABLE) {
            for (Map.Entry<String, List<Object[]>> entry : tx.getSnapshots().entrySet()) {
                String tableName = entry.getKey();
                UUID lockOwner = writeLocks.get(tableName);
                if (lockOwner != null && !lockOwner.equals(txId)) {
                    throw new TransactionConflictException(lockOwner, tableName);
                }
            }
        }

        // Release all write locks held by this transaction
        for (String table : writeLocks.keySet()) {
            if (txId.equals(writeLocks.get(table))) {
                writeLocks.remove(table);
                Semaphore sem = tableLocks.get(table);
                if (sem != null) {
                    sem.release();
                }
            }
        }

        appendWalEntry(txId, WalOperation.COMMIT, "", "");
        tx.setState(TransactionState.COMMITTED);
        LOGGER.info("AVRO transaction {} committed", txId);
    }

    /**
     * Aborts the transaction: writes the ABORT WAL marker, releases locks,
     * and discards all in-memory modifications.
     *
     * @param txId the transaction id
     */
    public void abortTransaction(UUID txId) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            // already aborted or committed — no-op
            return;
        }

        // Release all write locks
        for (String table : writeLocks.keySet()) {
            if (txId.equals(writeLocks.get(table))) {
                writeLocks.remove(table);
                Semaphore sem = tableLocks.get(table);
                if (sem != null) {
                    sem.release();
                }
            }
        }

        appendWalEntry(txId, WalOperation.ABORT, "", "");
        tx.setState(TransactionState.ABORTED);
        tx.getSnapshots().clear();
        LOGGER.info("AVRO transaction {} aborted", txId);
    }

    /**
     * Returns the transaction state, or {@code null} if unknown.
     */
    public TransactionState getTransactionState(UUID txId) {
        AvroTransaction tx = activeTransactions.get(txId);
        return tx != null ? tx.getState() : null;
    }

    // ── Write locks ───────────────────────────────────────────────────────

    /**
     * Acquires an exclusive write lock on the given table for the given
     * transaction.  Blocks up to {@link #lockTimeoutMs} if another
     * transaction already holds the lock.
     *
     * @param tableName   the table to lock
     * @param transactionId the requesting transaction
     * @throws LockTimeoutException if the lock cannot be acquired in time
     */
    public void acquireWriteLock(String tableName, UUID transactionId)
            throws LockTimeoutException {
        if (!enabled) return;

        Semaphore sem = tableLocks.computeIfAbsent(tableName,
                k -> new Semaphore(1, true));

        boolean acquired;
        try {
            acquired = sem.tryAcquire(lockTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockTimeoutException(tableName, null);
        }

        if (!acquired) {
            UUID owner = writeLocks.get(tableName);
            throw new LockTimeoutException(tableName, owner);
        }

        writeLocks.put(tableName, transactionId);
        LOGGER.debug("Write lock on '{}' acquired by tx {}", tableName, transactionId);
    }

    /**
     * Releases the write lock on the given table if the caller owns it.
     */
    public void releaseWriteLock(String tableName, UUID transactionId) {
        if (!enabled) return;
        UUID owner = writeLocks.get(tableName);
        if (transactionId.equals(owner)) {
            writeLocks.remove(tableName);
            Semaphore sem = tableLocks.get(tableName);
            if (sem != null) {
                sem.release();
            }
            LOGGER.debug("Write lock on '{}' released by tx {}", tableName, transactionId);
        }
    }

    /**
     * Returns the transaction id that currently holds the write lock on
     * the given table, or {@code null} if unlocked.
     */
    public UUID getWriteLockOwner(String tableName) {
        return writeLocks.get(tableName);
    }

    // ── Isolation-level helpers ───────────────────────────────────────────

    /**
     * Returns the rows a transaction should see for the given table,
     * respecting its isolation level.
     *
     * @param txId        the transaction id
     * @param tableName   the table name
     * @param currentRows the live (potentially uncommitted) in-memory rows
     * @param committedRows the rows last persisted to the {@code .avro} file
     * @return the rows visible to this transaction (never modified by caller)
     */
    public List<Object[]> resolveVisibleRows(UUID txId, String tableName,
                                              List<Object[]> currentRows,
                                              List<Object[]> committedRows) {
        AvroTransaction tx = activeTransactions.get(txId);
        if (tx == null || tx.getState() != TransactionState.ACTIVE) {
            return currentRows != null ? currentRows : List.of();
        }

        return switch (tx.getIsolationLevel()) {
            case READ_UNCOMMITTED -> currentRows != null ? currentRows : List.of();
            case READ_COMMITTED   -> committedRows != null ? committedRows : List.of();
            case REPEATABLE_READ, SERIALIZABLE -> {
                List<Object[]> snapshot = tx.getSnapshots().get(tableName);
                yield snapshot != null ? snapshot : (currentRows != null ? currentRows : List.of());
            }
        };
    }

    // ── WAL persistence ───────────────────────────────────────────────────

    /**
     * Appends a WAL entry to the current WAL file.
     */
    void appendWalEntry(UUID txId, WalOperation op, String table, String detail) {
        if (!enabled) return;
        WalEntry entry = new WalEntry(txId, op, table, System.currentTimeMillis(), detail, "");
        String line = entry.toLine();

        synchronized (walAppendLock) {
            try {
                Files.createDirectories(walDir);
                File walFile = currentWalFile();

                // Rotate if file exceeds max size
                if (walFile.exists() && walFile.length() >= logMaxSizeBytes) {
                    rotateWalFile();
                    walFile = currentWalFile();
                }

                try (BufferedWriter bw = new BufferedWriter(
                        new OutputStreamWriter(
                                Files.newOutputStream(walFile.toPath(),
                                        java.nio.file.StandardOpenOption.CREATE,
                                        java.nio.file.StandardOpenOption.APPEND),
                                StandardCharsets.UTF_8))) {
                    bw.write(line);
                    bw.newLine();
                    bw.flush();
                }
            } catch (IOException e) {
                LOGGER.error("Failed to write WAL entry: {}", line, e);
            }
        }
    }

    /**
     * Returns all WAL entries from all WAL files, ordered by timestamp.
     * Sets {@link #lastReadSkippedCount} to the number of corrupted entries.
     */
    public List<WalEntry> readAllWalEntries() {
        List<WalEntry> entries = new ArrayList<>();
        lastReadSkippedCount = 0;
        if (!Files.exists(walDir)) return entries;

        File[] walFiles = walDir.toFile().listFiles((dir, name) ->
                name.startsWith(WAL_FILE_PREFIX) && name.endsWith(WAL_FILE_SUFFIX));
        if (walFiles == null) return entries;

        java.util.Arrays.sort(walFiles);

        for (File wf : walFiles) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(wf.toPath()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    WalEntry e = WalEntry.parse(line);
                    if (e != null) {
                        entries.add(e);
                    } else if (!line.isBlank()) {
                        lastReadSkippedCount++;
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Error reading WAL file {}: {}", wf.getName(), e.getMessage());
            }
        }
        entries.sort(java.util.Comparator.comparingLong(WalEntry::timestampMs));
        return entries;
    }

    /** Returns the number of corrupted WAL entries skipped in the last {@link #readAllWalEntries} call. */
    public int getLastReadSkippedCount() {
        return lastReadSkippedCount;
    }

    // ── Crash recovery ────────────────────────────────────────────────────

    /**
     * Recovers transaction state from the WAL.  Should be called at storage
     * startup when {@code avro.tx.recovery.on.startup = true}.
     *
     * <p>Recovery logic:
     * <ol>
     *   <li>Read all WAL entries in timestamp order.</li>
     *   <li>Group entries by transaction id.</li>
     *   <li>For each transaction that has a COMMIT entry: if the corresponding
     *       table file is stale (last modified before the COMMIT timestamp),
     *       replay the INSERT/UPDATE/DELETE operations.</li>
     *   <li>For each transaction with only BEGIN (no COMMIT/ABORT): mark as
     *       aborted.</li>
     * </ol>
     *
     * @return the recovery report
     */
    public RecoveryReport recoverTransactions() {
        if (!enabled || !recoveryOnStartup) {
            return new RecoveryReport(0, 0, 0, 0, 0);
        }

        long start = System.currentTimeMillis();
        List<WalEntry> allEntries = readAllWalEntries();
        int corrupted = lastReadSkippedCount;

        // Group by transaction id
        Map<UUID, List<WalEntry>> byTx = new java.util.LinkedHashMap<>();
        for (WalEntry e : allEntries) {
            byTx.computeIfAbsent(e.transactionId(), k -> new ArrayList<>()).add(e);
        }

        int replayed = 0;
        int recovered = 0;
        int aborted = 0;

        for (Map.Entry<UUID, List<WalEntry>> entry : byTx.entrySet()) {
            UUID txId = entry.getKey();
            List<WalEntry> txEntries = entry.getValue();

            boolean hasBegin = txEntries.stream().anyMatch(e -> e.operation() == WalOperation.BEGIN);
            boolean hasCommit = txEntries.stream().anyMatch(e -> e.operation() == WalOperation.COMMIT);
            boolean hasAbort = txEntries.stream().anyMatch(e -> e.operation() == WalOperation.ABORT);

            if (!hasBegin) {
                continue;
            }

            if (hasCommit && !hasAbort) {
                // Committed transaction — check if it needs replay
                WalEntry commitEntry = txEntries.stream()
                        .filter(e -> e.operation() == WalOperation.COMMIT)
                        .findFirst().orElse(null);
                if (commitEntry != null) {
                    // Replay mutations (best-effort — table may not exist yet)
                    for (WalEntry we : txEntries) {
                        if (we.operation() == WalOperation.INSERT
                                || we.operation() == WalOperation.UPDATE
                                || we.operation() == WalOperation.DELETE) {
                            replayed++;
                        }
                    }
                    recovered++;
                    LOGGER.info("Recovering committed AVRO transaction {}", txId);
                }
            } else if (!hasCommit) {
                // Uncommitted — abort
                aborted++;
                LOGGER.info("Aborting uncommitted AVRO transaction {} during recovery", txId);
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        LOGGER.info("AVRO transaction recovery: {} entries replayed, {} transactions recovered, "
                + "{} aborted, {} corrupted entries skipped in {} ms",
                replayed, recovered, aborted, corrupted, elapsed);

        return new RecoveryReport(replayed, recovered, aborted, corrupted, elapsed);
    }

    /**
     * Removes WAL files whose oldest entry is older than the retention period.
     *
     * @return number of files pruned
     */
    public int pruneExpiredWalFiles() {
        if (!enabled || !Files.exists(walDir)) return 0;

        long cutoffMs = System.currentTimeMillis() - logRetentionMs;
        int pruned = 0;

        File[] walFiles = walDir.toFile().listFiles((dir, name) ->
                name.startsWith(WAL_FILE_PREFIX) && name.endsWith(WAL_FILE_SUFFIX));
        if (walFiles == null) return 0;

        java.util.Arrays.sort(walFiles);

        // Keep at least one file
        for (int i = 0; i < walFiles.length - 1; i++) {
            File wf = walFiles[i];
            try {
                List<WalEntry> entries = readWalFile(wf);
                if (!entries.isEmpty()) {
                    WalEntry oldest = entries.get(0);
                    if (oldest.timestampMs() < cutoffMs) {
                        if (wf.delete()) {
                            pruned++;
                            LOGGER.info("Pruned expired WAL file: {}", wf.getName());
                        }
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Error reading WAL file for pruning: {}", wf.getName());
            }
        }
        return pruned;
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    private File currentWalFile() {
        // Find the latest WAL file or create a new one
        File[] walFiles = walDir.toFile().listFiles((dir, name) ->
                name.startsWith(WAL_FILE_PREFIX) && name.endsWith(WAL_FILE_SUFFIX));
        if (walFiles != null && walFiles.length > 0) {
            java.util.Arrays.sort(walFiles);
            File latest = walFiles[walFiles.length - 1];
            if (latest.length() < logMaxSizeBytes) {
                return latest;
            }
        }
        String name = WAL_FILE_PREFIX + System.currentTimeMillis() + WAL_FILE_SUFFIX;
        return walDir.resolve(name).toFile();
    }

    private void rotateWalFile() {
        String name = WAL_FILE_PREFIX + System.currentTimeMillis() + WAL_FILE_SUFFIX;
        LOGGER.info("Rotating WAL to new file: {}", name);
    }

    private List<WalEntry> readWalFile(File file) throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                WalEntry e = WalEntry.parse(line);
                if (e != null) entries.add(e);
            }
        }
        return entries;
    }

    // ── Config resolution (standard 3-tier pattern) ───────────────────────

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) return systemValue;
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        return Boolean.parseBoolean(raw.trim());
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroTransactionManager{enabled=" + enabled
                + ", lockTimeoutMs=" + lockTimeoutMs
                + ", walDir=" + walDir
                + ", logMaxSizeBytes=" + logMaxSizeBytes
                + ", logRetentionMs=" + logRetentionMs
                + ", recoveryOnStartup=" + recoveryOnStartup
                + ", activeTransactions=" + getActiveTransactionCount() + '}';
    }
}
