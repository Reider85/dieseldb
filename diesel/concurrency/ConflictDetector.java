package diesel.concurrency;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import diesel.SerializationFailureException;

/**
 * Simplified Serializable Snapshot Isolation (SSI) conflict detector for SERIALIZABLE transactions.
 * Tracks read and write sets per transaction and detects rw-conflicts at commit time.
 * Victim policy: writer detection loser — the transaction that commits/writes last (typically higher txid)
 * gets aborted when a conflict is detected.
 */
public final class ConflictDetector {

    /**
     * Reference to a row in a table for conflict tracking.
     * Records table name and row index — sufficient for read/write set intersection.
     */
    public record RowRef(String table, int rowIndex) {}

    /**
     * Tracking state for a single SERIALIZABLE transaction.
     * Contains readSet (rows read), writeSet (rows written), and snapshotCsn.
     */
    private static final class TxnTracking {
        final long txid;
        final long snapshotCsn;
        final Set<RowRef> readSet;
        final Set<RowRef> writeSet;

        TxnTracking(long txid, long snapshotCsn) {
            this.txid = txid;
            this.snapshotCsn = snapshotCsn;
            this.readSet = ConcurrentHashMap.newKeySet();
            this.writeSet = ConcurrentHashMap.newKeySet();
        }

        void addRead(String table, int rowIndex) {
            readSet.add(new RowRef(table, rowIndex));
        }

        void addWrite(String table, int rowIndex) {
            writeSet.add(new RowRef(table, rowIndex));
        }
    }

    // Active SERIALIZABLE transactions: txid -> tracking state
    private final ConcurrentMap<Long, TxnTracking> activeTxns = new ConcurrentHashMap<>();

    /**
     * Begin tracking a SERIALIZABLE transaction.
     * Called when a SERIALIZABLE transaction starts (after txid and snapshotCsn are assigned).
     */
    public void beginTracking(long txid, long snapshotCsn) {
        if (activeTxns.putIfAbsent(txid, new TxnTracking(txid, snapshotCsn)) != null) {
            throw new IllegalStateException("Transaction " + txid + " already being tracked");
        }
    }

    /**
     * Record that a SERIALIZABLE transaction read a row.
     * Called during SELECT scans or when rows are identified for UPDATE/DELETE.
     */
    public void noteRead(long txid, String table, int rowIndex) {
        TxnTracking tracking = activeTxns.get(txid);
        if (tracking == null) {
            throw new IllegalStateException("Transaction " + txid + " not being tracked");
        }
        tracking.addRead(table, rowIndex);
    }

    /**
     * Record that a SERIALIZABLE transaction wrote to a row.
     * Called during INSERT/UPDATE/DELETE when a row is marked as pending.
     */
    public void noteWrite(long txid, String table, int rowIndex) {
        TxnTracking tracking = activeTxns.get(txid);
        if (tracking == null) {
            throw new IllegalStateException("Transaction " + txid + " not being tracked");
        }
        tracking.addWrite(table, rowIndex);
    }

    /**
     * Check for write-side conflicts for a SERIALIZABLE transaction targeting a specific row.
     * Throws SerializationFailureException if:
     * 1. Another transaction has an uncommitted change on this row (pending foreign change)
     * 2. The row was committed by another transaction after this transaction's snapshot (stale snapshot)
     *
     * @param table         table name
     * @param rowIndex      row index being targeted for write
     * @param txid          current transaction ID
     * @param snapshotCsn    current transaction's snapshot commit sequence number
     * @param pendingOwnerTxid  txid of transaction with pending change on this row, or 0 if none
     * @param lastCommittedCsn commit CSN of last committed change to this row, or 0 if unknown/bootstrap
     * @throws SerializationFailureException if a write conflict is detected
     */
    public void checkSerializableWriteConflict(
            String table, int rowIndex, long txid, long snapshotCsn,
            long pendingOwnerTxid, long lastCommittedCsn) {
        
        // Condition 1: Pending foreign change (another transaction owns this row)
        if (pendingOwnerTxid != 0 && pendingOwnerTxid != txid) {
            throw new SerializationFailureException(
                    "Serialization failure: transaction " + pendingOwnerTxid + " has an uncommitted change on table " + table + " row " + rowIndex);
        }

        // Condition 2: Stale snapshot (row committed after our snapshot)
        if (lastCommittedCsn > snapshotCsn) {
            throw new SerializationFailureException(
                    "Serialization failure: row committed at CSN " + lastCommittedCsn + " after the writer's snapshot " + snapshotCsn + " on table " + table + " row " + rowIndex);
        }
    }

    /**
     * Record that a SERIALIZABLE transaction is committing.
     * Checks for read-write conflicts with any active SERIALIZABLE transactions.
     * A rw-conflict occurs if:
     * - This transaction wrote to a row that was read by an active SERIALIZABLE transaction
     *   whose snapshot predates this transaction's commit
     * - Or this transaction read a row that was written by a now-committed transaction
     *   after this transaction's snapshot (handled by write-time check above)
     *
     * @param txid        current transaction ID
     * @param commitCsn   commit sequence number of this transaction
     * @param writeSet    map of table name -> set of row indices written by this transaction
     * @throws SerializationFailureException if a rw-conflict is detected; victim is the committing transaction (writer detection loser)
     */
    public void noteCommit(long txid, long commitCsn, java.util.Map<String, java.util.Set<Integer>> writeSet) {
        TxnTracking thisTxn = activeTxns.get(txid);
        if (thisTxn == null) {
            throw new IllegalStateException("Transaction " + txid + " not being tracked during commit");
        }

        // Check if our writeSet intersects with any active SERIALIZABLE transaction's readSet
        // where the active transaction's snapshot predates our commit (rw-antidependency)
        for (TxnTracking otherTxn : activeTxns.values()) {
            if (otherTxn.txid == txid) continue; // Skip self

            // Check if otherTxn read any row that we are now writing
            for (RowRef ourWrite : thisTxn.writeSet) {
                for (RowRef otherRead : otherTxn.readSet) {
                    if (ourWrite.table().equals(otherRead.table()) && ourWrite.rowIndex() == otherRead.rowIndex()) {
                        // rw-conflict: otherTxn read row R; we are writing R after otherTxn's snapshot
                        // Victim policy: writer detection loser — this transaction (committer) loses
                        throw new SerializationFailureException(
                                "Serialization failure: rw-conflict detected at commit. Transaction " + otherTxn.txid + " read table " + otherRead.table() + " row " + otherRead.rowIndex() + " before this transaction's commit. Committing transaction is the victim.");
                    }
                }
            }
        }
    }

    /**
     * Record that a SERIALIZABLE transaction is rolling back.
     * Cleanup tracking state for the transaction.
     */
    public void noteRollback(long txid) {
        TxnTracking removed = activeTxns.remove(txid);
        if (removed == null) {
            throw new IllegalStateException("Transaction " + txid + " not being tracked during rollback");
        }
    }

    /**
     * Record that a SERIALIZABLE transaction has ended (committed successfully).
     * Cleanup tracking state.
     */
    public void noteEnd(long txid) {
        TxnTracking removed = activeTxns.remove(txid);
        if (removed == null) {
            throw new IllegalStateException("Transaction " + txid + " not being tracked during end");
        }
    }

    /**
     * Cleanup all tracking state.
     * Useful for testing and database close.
     */
    public void clear() {
        activeTxns.clear();
    }

    /**
     * Get the number of currently active SERIALIZABLE transactions being tracked.
     * Useful for testing heap stability (e.g., ChainOf1000TxTest).
     */
    public int activeSerializableCount() {
        return activeTxns.size();
    }
}