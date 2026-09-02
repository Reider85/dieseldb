package diesel;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The isolation level of a transaction, in increasing order of strictness:
 * <ul>
 *   <li>{@link #READ_UNCOMMITTED} - dirty reads are allowed; a transaction can
 *       see another transaction's uncommitted modifications.</li>
 *   <li>{@link #READ_COMMITTED} - only committed data is visible (currently
 *       treated the same as {@link #REPEATABLE_READ}).</li>
 *   <li>{@link #REPEATABLE_READ} - the BEGIN-time snapshot is used for reads.</li>
 *   <li>{@link #SERIALIZABLE} - strongest isolation (currently treated the
 *       same as {@link #REPEATABLE_READ}).</li>
 * </ul>
 */
enum IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}

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
    private final IsolationLevel isolationLevel;
    private final Map<String, Table> originalTables;
    private final Map<String, Table> modifiedTables;
    private final Map<String, Long> snapshotVersions;
    private boolean active;
    private boolean batchMode;

    /**
     * Starts a transaction at the given isolation level, defaulting to
     * {@link IsolationLevel#READ_UNCOMMITTED} when the level is null.
     *
     * @param isolationLevel the isolation level, or null for the default
     */
    public Transaction(IsolationLevel isolationLevel) {
        this.transactionId = UUID.randomUUID();
        this.isolationLevel = isolationLevel != null ? isolationLevel : IsolationLevel.READ_UNCOMMITTED;
        this.originalTables = new HashMap<>();
        this.modifiedTables = new HashMap<>();
        this.snapshotVersions = new HashMap<>();
        this.active = true;
        this.batchMode = false;
    }

    public UUID getTransactionId() {
        return transactionId;
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
