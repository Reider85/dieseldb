package diesel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
 * <p>The transaction keeps two independent views of every table it touches:
 * <ul>
 *   <li>{@link #originalTables} - a deep copy of each table as it was when the
 *       transaction started (BEGIN time). It backs the snapshot semantics for
 *       reads inside the transaction.</li>
 *   <li>{@link #modifiedTables} - the transaction's own changes, deep-copied on
 *       every DML statement. On COMMIT these copies are published back into the
 *       shared database and persisted to disk.</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * Transaction txn = new Transaction(IsolationLevel.REPEATABLE_READ);
 * txn.snapshotTable("USERS", table);
 * txn.updateTable("USERS", modifiedTable);
 * }</pre>
 *
 * @see Database
 * @see IsolationLevel
 */
class Transaction {
    private final UUID transactionId;
    private final IsolationLevel isolationLevel;
    private final Map<String, Table> originalTables;
    private final Map<String, Table> modifiedTables;
    private boolean active;

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
        this.active = true;
    }

    /**
     * Returns the unique id of this transaction, used by the client to refer
     * to the transaction in subsequent queries.
     *
     * @return the transaction id
     */
    public UUID getTransactionId() {
        return transactionId;
    }

    /**
     * Returns the isolation level of this transaction.
     *
     * @return the isolation level, never null
     */
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Returns whether the transaction is still active.
     *
     * @return true while the transaction has neither been committed nor rolled back
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Marks the transaction as no longer active.
     */
    public void setInactive() {
        this.active = false;
    }

    /** Records a deep copy of {@code table} as the BEGIN-time snapshot. */
    public void snapshotTable(String tableName, Table table) {
        originalTables.put(tableName, cloneForTransaction(table, tableName));
    }

    /** Records a deep copy of {@code table} as the transaction's own modified state. */
    public void updateTable(String tableName, Table table) {
        modifiedTables.put(tableName, cloneForTransaction(table, tableName));
    }

    /**
     * Stores the live table reference itself (no copy). Used by short-lived
     * auto-commit DML transactions that publish and persist the table right away.
     */
    public void registerModifiedTable(String tableName, Table table) {
        modifiedTables.put(tableName, table);
    }

    /**
     * Returns the BEGIN-time snapshot views of the tables.
     *
     * @return the original table map
     */
    public Map<String, Table> getOriginalTables() {
        return originalTables;
    }

    /**
     * Returns the transaction's own modified table views.
     *
     * @return the modified table map
     */
    public Map<String, Table> getModifiedTables() {
        return modifiedTables;
    }

    private Table cloneForTransaction(Table table, String tableName) {
        try {
            return table != null ? cloneTable(table) : null;
        } catch (IOException | ClassNotFoundException e) {
            throw new DieselException("Failed to snapshot/update transaction table: " + tableName, e);
        }
    }

    /** Deep-copies a table through serialization so transaction views stay independent. */
    private static Table cloneTable(Table table) throws IOException, ClassNotFoundException {
        Database databaseRef = table.getDatabase();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(table);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            Table clonedTable = (Table) ois.readObject();
            // The database field is transient, so restore it on the clone.
            if (databaseRef != null) {
                clonedTable.attachDatabase(databaseRef);
            }
            return clonedTable;
        }
    }
}
