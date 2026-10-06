package diesel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents one client transaction session with its own isolation level.
 *
 * <p>The transaction uses Copy-on-Write semantics:
 * <ul>
 *   <li>{@link #originalTables} - stores direct references to shared tables at
 *       BEGIN time (lazy snapshot — no copy until first read/write).</li>
 *   <li>{@link #modifiedTables} - the transaction's own private copies, created
 *       on first DML per table via {@link Table#copyForTransaction()}. On COMMIT
 *       these copies are published back into the shared database.</li>
 * </ul>
 *
 * @see Database
 * @see IsolationLevel
 */
class Transaction {
    private final UUID transactionId;
    private final Database database; // Reference to the database
    private long txid; // MVCC transaction id
    private long snapshotTxid; // Snapshot of txid counter at BEGIN
    private long snapshotCsn; // Snapshot of commitCsn counter at BEGIN
    private final IsolationLevel isolationLevel;
    private final Map<String, Table> originalTables;
    private final Map<String, Table> modifiedTables;
    private final Map<String, Long> snapshotVersions;
    private boolean active;
    private boolean batchMode;
    private UndoLog undoLog; // MVCC: undo log for rollback support
    /**
     * Raw row indexes this transaction changed, per table (prompt4.md #4).
     * Idempotent sets: a row updated twice still appears once, so COMMIT
     * resolves each row exactly once.
     */
    private final Map<String, Set<Integer>> modifiedRows = new HashMap<>();
    /**
     * Raw row indexes this transaction deleted, per table. COMMIT tombstones
     * them so the delete survives a restart (MVCC metadata is transient).
     */
    private final Map<String, Set<Integer>> deletedRows = new HashMap<>();

    /**
     * Starts a transaction at the given isolation level, defaulting to
     * {@link IsolationLevel#READ_UNCOMMITTED} when the level is null.
     *
     * @param isolationLevel the isolation level, or null for the default
     */
    public Transaction(Database database, IsolationLevel isolationLevel) {
        this.database = database;
        this.transactionId = UUID.randomUUID();
        this.txid = 0; // Will be set by Database.executeBeginTransaction
        this.snapshotTxid = 0; // Will be set by Database.executeBeginTransaction  
        this.snapshotCsn = 0; // Will be set by Database.executeBeginTransaction
        this.isolationLevel = isolationLevel != null ? isolationLevel : IsolationLevel.READ_UNCOMMITTED;
        this.originalTables = new HashMap<>();
        this.modifiedTables = new HashMap<>();
        this.snapshotVersions = new HashMap<>();
        // MVCC undo log: spills to a temp file once undo.spill.threshold.mb is exceeded
        long spillThresholdBytes = Long.getLong("undo.spill.threshold.mb", 1L) * 1024L * 1024L;
        this.undoLog = new UndoLog(spillThresholdBytes);
        this.active = true;
        this.batchMode = false;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    /** Returns the MVCC transaction id (long). */
    public long getTxid() {
        return txid;
    }

    /** Returns the snapshot txid counter value at BEGIN time. */
    public long getSnapshotTxid() {
        return snapshotTxid;
    }

    /** Returns the snapshot commit CSN counter value at BEGIN time. */
    public long getSnapshotCsn() {
        return snapshotCsn;
    }

    /** Sets the MVCC transaction id (package-private for MVCC implementation). */
    void setTxid(long txid) {
        this.txid = txid;
    }

    /** Sets the snapshot txid counter value (package-private for MVCC implementation). */
    void setSnapshotTxid(long snapshotTxid) {
        this.snapshotTxid = snapshotTxid;
    }

    /** Sets the snapshot commit CSN counter value (package-private for MVCC implementation). */
    void setSnapshotCsn(long snapshotCsn) {
        this.snapshotCsn = snapshotCsn;
    }

    /** Returns the transaction's undo log for MVCC rollback support. */
    public UndoLog getUndoLog() {
        return undoLog;
    }

    /**
     * Rolls back the transaction by applying undo records in reverse order.
     * This restores the database to its state before the transaction began.
     */
    public void rollback() {
        if (!active) {
            throw new IllegalStateException("Transaction is not active");
        }
        
        try {
            // Apply undo records in reverse order to rollback changes
            undoLog.rollback(database);
            // The log has served its purpose: free records and the spill file
            undoLog.clear();
            
            // Clear modified tables since all changes are rolled back
            modifiedTables.clear();
            modifiedRows.clear();
            deletedRows.clear();
            
            // Mark transaction as inactive
            active = false;
        } catch (Exception e) {
            throw new RuntimeException("Failed to rollback transaction", e);
        }
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public boolean isActive() {
        return active;
    }

    public void setInactive() {
        this.active = false;
    }

    /** Records a reference to {@code table} as the BEGIN-time snapshot (lazy — no copy). */
    public void snapshotTable(String tableName, Table table) {
        originalTables.put(tableName, table);
        if (table != null) {
            snapshotVersions.put(tableName, table.getVersion());
        }
    }

    /** Records a deep copy of {@code table} as the transaction's own modified state. */
    public void updateTable(String tableName, Table table) {
        Table copy = table != null ? table.copyForTransaction() : null;
        if (copy != null && batchMode) {
            copy.deferIndexUpdates();
        }
        modifiedTables.put(tableName, copy);
    }

    /**
     * Stores the live table reference itself (no copy). Used by short-lived
     * auto-commit DML transactions that publish and persist the table right away.
     */
    public void registerModifiedTable(String tableName, Table table) {
        modifiedTables.put(tableName, table);
    }

    /**
     * Records that this transaction changed a row, so COMMIT can mark the
     * change committed ({@link Table#markRowCommitted}) with the commit CSN.
     *
     * @param tableName the table holding the row
     * @param rowIndex  the raw row index
     */
    public void noteModifiedRow(String tableName, int rowIndex) {
        modifiedRows.computeIfAbsent(tableName, key -> ConcurrentHashMap.newKeySet())
                .add(rowIndex);
    }

    /**
     * Records that this transaction deleted a row. Also counts as a
     * modification; COMMIT additionally tombstones the row so the delete
     * persists across restarts.
     *
     * @param tableName the table holding the row
     * @param rowIndex  the raw row index
     */
    public void noteDeletedRow(String tableName, int rowIndex) {
        noteModifiedRow(tableName, rowIndex);
        deletedRows.computeIfAbsent(tableName, key -> ConcurrentHashMap.newKeySet())
                .add(rowIndex);
    }

    /** Returns per-table sets of raw row indexes changed by this transaction. */
    public Map<String, Set<Integer>> getModifiedRows() {
        return modifiedRows;
    }

    /** Returns per-table sets of raw row indexes deleted by this transaction. */
    public Map<String, Set<Integer>> getDeletedRows() {
        return deletedRows;
    }

    public Map<String, Table> getOriginalTables() {
        return originalTables;
    }

    public Map<String, Table> getModifiedTables() {
        return modifiedTables;
    }

    /** Returns the version of each table at snapshot time. */
    public Map<String, Long> getSnapshotVersions() {
        return snapshotVersions;
    }

    /** Returns the database this transaction belongs to. */
    public Database getDatabase() {
        return database;
    }

    /** Returns a table from the database (for MVCC undo log access). */
    public Table getTableForNameFromDatabase(String tableName) {
        return database.getTable(tableName);
    }

    /**
     * Returns an MVCC snapshot view of the given table for this transaction.
     * The snapshot provides filtered rows based on transaction visibility rules.
     * 
     * @param tableName the name of the table
     * @return transaction table snapshot
     * @throws IllegalArgumentException if table not found
     */
    public TransactionTableSnapshot getSnapshot(String tableName) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        
        // Use the transaction's own metadata for visibility
        return new TransactionTableSnapshot(
            tableName,
            table,
            txid,
            snapshotTxid,
            snapshotCsn,
            isolationLevel,
            database.getTxStatusTracker()
        );
    }

    /**
     * Returns whether this transaction is in batch mode.
     *
     * @return true if in batch mode
     */
    public boolean isBatchMode() {
        return batchMode;
    }

    /**
     * Sets the batch mode flag.
     *
     * @param batchMode true to enable batch mode
     */
    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }
}
