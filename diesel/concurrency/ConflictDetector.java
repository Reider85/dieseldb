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

        void addRead(RowRef ref) {
            readSet.add(ref);
        }

        void addWrite(String table, int rowIndex) {
            writeSet.add(new RowRef(table, rowIndex));
        }
    }

    // Active SERIALIZABLE transactions: txid -> tracking state
    private final ConcurrentMap<Long, TxnTracking> activeTxns = new ConcurrentHashMap<>();

    // Global index of which transactions are reading each row: RowRef -> set of txids
    private final ConcurrentMap<RowRef, Set<Long>> rowReaders = new ConcurrentHashMap<>();

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
        RowRef ref = new RowRef(table, rowIndex);
        tracking.addRead(ref);
        rowReaders.computeIfAbsent(ref, k -> ConcurrentHashMap.newKeySet()).add(txid);
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
      * Cleanup all reader registrations for a transaction's read set.
      * Removes the transaction from rowReaders index for all rows it read.
      */
    private void cleanupReaderEntries(TxnTracking txn) {
        for (RowRef ref : txn.readSet) {
            rowReaders.computeIfPresent(ref, (k, readers) -> {
                readers.remove(txn.txid);
                return readers.isEmpty() ? null : readers;  // Remove empty sets to prevent unbounded growth
            });
        }
    }

    /**
      * Record that a SERIALIZABLE transaction is committing.
      * Checks for read-write conflicts using global rowReaders index.
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

        // Union of writeSet parameter and thisTxn.writeSet (covers both Database-provided writeSet and noteWrite-tracked writes)
        Set<RowRef> writes = new HashSet<>(thisTxn.writeSet);
        if (writeSet != null) {
            writeSet.forEach((table, indices) -> {
                for (int rowIndex : indices) {
                    writes.add(new RowRef(table, rowIndex));
                }
            });
        }

        // Check if any of our writes intersect with active transactions' reads using rowReaders index
        for (RowRef writeRef : writes) {
            Set<Long> readers = rowReaders.get(writeRef);
            if (readers != null) {
                for (long readerTxid : readers) {
                    if (readerTxid != txid) {
                        // rw-conflict: readerTxid read row that we are writing
                        // Victim policy: writer detection loser — this transaction (committer) loses
                        throw new SerializationFailureException(
                                "Serialization failure: rw-conflict detected at commit. Transaction " + readerTxid + " read table " + writeRef.table() + " row " + writeRef.rowIndex() + " before this transaction's commit. Committing transaction is the victim.");
                    }
                }
            }
        }

        // Success: cleanup this transaction's reader registrations and remove from active transactions
        cleanupReaderEntries(thisTxn);
        activeTxns.remove(txid);
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
        cleanupReaderEntries(removed);
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
        cleanupReaderEntries(removed);
    }

/**
      * Cleanup all tracking state.
      * Useful for testing and database close.
      */
    public void clear() {
        activeTxns.clear();
        rowReaders.clear();
    }

/**
      * Get the number of currently active SERIALIZABLE transactions being tracked.
      * Useful for testing heap stability (e.g., ChainOf1000TxTest).
      */
    public int activeSerializableCount() {
        return activeTxns.size();
    }

    /**
      * Get the number of row readers currently tracked in the global index.
      * Useful for testing that rowReaders is properly cleaned up after commits/aborts.
      */
    public int trackedReaderRowCount() {
        return rowReaders.size();
    }
}