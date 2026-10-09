package diesel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
     * Returns the lowest transaction id currently in the ACTIVE state, or
     * {@link Long.MAX_VALUE} when no transaction is active. Used for vacuum
     * planning: every row version written by a txid at or above the returned
     * value may still change, so it must not be reclaimed yet.
     *
     * @return the oldest active transaction id, or {@link Long.MAX_VALUE}
     */
    public long getOldestActiveTxid() {
        long oldest = Long.MAX_VALUE;
        for (java.util.Map.Entry<Long, TxInfo> entry : txInfoMap.entrySet()) {
            if (entry.getValue().status == TxStatus.ACTIVE) {
                oldest = Math.min(oldest, entry.getKey());
            }
        }
        return oldest;
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
     * Registers a transaction recovered from the WAL as COMMITTED with its
     * original commit CSN (prompt 4 #19). Unlike {@link #markCommitted} this
     * does not allocate a fresh CSN — the WAL COMMIT payload carries the one
     * the transaction had before the crash. Also floors the txid and CSN
     * counters so post-restart allocations never collide with recovered ids.
     *
     * @param txid      the recovered transaction id
     * @param commitCsn the commit CSN from the WAL COMMIT payload
     */
    public void registerRecoveredCommitted(long txid, long commitCsn) {
        txInfoMap.put(txid, new TxInfo(TxStatus.COMMITTED, commitCsn));
        advanceNextTxidBeyond(txid);
        advanceCommitCsnAtLeast(commitCsn);
    }

    /**
     * Registers a transaction recovered from the WAL as ABORTED (prompt 4
     * #19): it was still active when the crash hit and the undo phase rolled
     * its changes back. Floors the txid counter past the recovered id.
     *
     * @param txid the recovered transaction id
     */
    public void registerRecoveredAbort(long txid) {
        txInfoMap.put(txid, new TxInfo(TxStatus.ABORTED, 0));
        advanceNextTxidBeyond(txid);
    }

    /**
     * Moves the txid allocator past {@code txid} so a future
     * {@link #registerTransaction} can never reuse a recovered id.
     *
     * @param txid the highest recovered transaction id
     */
    public void advanceNextTxidBeyond(long txid) {
        nextTxid.accumulateAndGet(txid + 1, Math::max);
    }

    /**
     * Moves the commit-CSN allocator past {@code csn} so a future
     * {@link #markCommitted} can never assign a CSN at or below a recovered
     * one.
     *
     * @param csn the highest recovered commit CSN
     */
    public void advanceCommitCsnAtLeast(long csn) {
        commitCsnCounter.accumulateAndGet(csn + 1, Math::max);
    }

    /**
     * Gets the current commit CSN counter value.
     */
    public long getCurrentCommitCsn() {
        return commitCsnCounter.get() - 1; // Return last assigned
    }

    /**
     * Returns a list of all currently active transaction IDs.
     * Used by ARIES checkpoint mechanism to record active txids at checkpoint time.
     *
     * @return list of active transaction IDs (may be empty)
     */
    public List<Long> getActiveTxids() {
        List<Long> activeTxids = new ArrayList<>();
        for (java.util.Map.Entry<Long, TxInfo> entry : txInfoMap.entrySet()) {
            if (entry.getValue().status == TxStatus.ACTIVE) {
                activeTxids.add(entry.getKey());
            }
        }
        return Collections.unmodifiableList(activeTxids);
    }
}