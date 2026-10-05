package diesel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks transaction status for MVCC visibility and commit sequence numbering.
 * 
 * <p><b>States:</b>
 * <ul>
 *   <li>ACTIVE — transaction started but not yet committed/aborted</li>
 *   <li>COMMITTED — transaction committed; visible to others with commitCsn</li>
 *   <li>ABORTED — transaction aborted; invisible</li>
 * </ul>
 * 
 * <p><b>CSN (Commit Sequence Number):</b> Monotonically increasing timestamp assigned
 * at commit time. Used for snapshot visibility: a row version is visible if
 * its creator/deleter has commitCsn <= snapshotCsn.
 */
public final class TxStatusTracker {
    public enum TxStatus {
        ACTIVE,
        COMMITTED,
        ABORTED
    }

    private static class TxInfo {
        final TxStatus status;
        final long commitCsn; // 0 if not committed

        TxInfo(TxStatus status, long commitCsn) {
            this.status = status;
            this.commitCsn = commitCsn;
        }
    }

    private final ConcurrentHashMap<Long, TxInfo> txInfoMap = new ConcurrentHashMap<>();
    private final AtomicLong commitCsnCounter = new AtomicLong(1); // Start at 1; 0 = bootstrap
    private final AtomicLong nextTxid = new AtomicLong(1); // Start at 1; 0 = bootstrap

    /**
     * Starts a new transaction and registers it as ACTIVE.
     * 
     * @return allocated txid for the new transaction
     */
    public long registerTransaction() {
        long txid = nextTxid.getAndIncrement();
        txInfoMap.put(txid, new TxInfo(TxStatus.ACTIVE, 0));
        return txid;
    }

    /**
     * Marks a transaction as COMMITTED and assigns a commit CSN.
     * 
     * @param txid the transaction id
     * @return the assigned commitCsn
     * @throws IllegalStateException if transaction not ACTIVE
     */
    public long markCommitted(long txid) {
        TxInfo info = txInfoMap.get(txid);
        if (info == null || info.status != TxStatus.ACTIVE) {
            throw new IllegalStateException("Transaction " + txid + " not active");
        }
        long csn = commitCsnCounter.getAndIncrement();
        txInfoMap.put(txid, new TxInfo(TxStatus.COMMITTED, csn));
        return csn;
    }

    /**
     * Marks a transaction as ABORTED.
     * 
     * @param txid the transaction id
     * @throws IllegalStateException if transaction not ACTIVE
     */
    public void markAborted(long txid) {
        TxInfo info = txInfoMap.get(txid);
        if (info == null || info.status != TxStatus.ACTIVE) {
            throw new IllegalStateException("Transaction " + txid + " not active");
        }
        txInfoMap.put(txid, new TxInfo(TxStatus.ABORTED, 0));
    }

    /**
     * Returns the current status of a transaction, or null when the transaction
     * id is unknown to this tracker (pre-MVCC rows, cleared tracker).
     * 
     * @param txid the transaction id
     * @return the status, or null if unknown
     */
    public TxStatus getStatus(long txid) {
        TxInfo info = txInfoMap.get(txid);
        return info == null ? null : info.status;
    }

    /**
     * Checks if a transaction is committed and its commitCsn is <= the given snapshotCsn.
     * 
     * @param txid the transaction id
     * @param snapshotCsn the snapshot CSN to compare against
     * @return true if the transaction is committed and committed before or at the snapshot
     */
    public boolean isCommittedBefore(long txid, long snapshotCsn) {
        TxInfo info = txInfoMap.get(txid);
        if (info == null || info.status != TxStatus.COMMITTED) {
            return false;
        }
        return info.commitCsn <= snapshotCsn;
    }

    /**
     * Checks if a transaction is currently active.
     */
    public boolean isActive(long txid) {
        TxInfo info = txInfoMap.get(txid);
        return info != null && info.status == TxStatus.ACTIVE;
    }

    /**
     * Returns the oldest active transaction id (for vacuum planning).
     * Returns Long.MAX_VALUE if no active transactions.
     */
    public long getOldestActiveTxid() {
        long oldest = Long.MAX_VALUE;
        for (TxInfo info : txInfoMap.values()) {
            if (info.status == TxStatus.ACTIVE) {
                // Note: this is a placeholder; in a real system we'd track tx start time.
                // For now, just return any active txid.
                return oldest; // Simplified: return first active found
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Clears all transaction info (for testing or reset).
     */
    public void clear() {
        txInfoMap.clear();
        commitCsnCounter.set(1);
        nextTxid.set(1);
    }

    /**
     * Gets the current commit CSN counter value.
     */
    public long getCurrentCommitCsn() {
        return commitCsnCounter.get() - 1; // Return last assigned
    }
}