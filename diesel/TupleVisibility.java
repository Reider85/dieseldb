package diesel;

import java.util.function.LongPredicate;
import java.util.Objects;

/**
 * MVCC tuple visibility logic per isolation level.
 * 
 * <p><b>Step 1 Scope:</b> Foundation-only. No integration with Transaction/SelectQuery yet.
 * 
 * <p><b>Visibility Contract:</b>
 * <ul>
 *   <li>Own insert (xmin == currentTxid) → visible (self-write visibility)
 *   <li>Own delete (xmax == currentTxid) → invisible (row logically gone for its deleter)
 *   <li>Created after snapshot (xmin > snapshotTxid) → invisible
 *   <li>Deleted at/before snapshot by committed tx (xmax != 0 && xmax <= snapshotTxid && txCommitted.test(xmax)) → invisible
 *   <li>Insert by uncommitted tx (xmin != currentTxid && !txCommitted.test(xmin)) → invisible
 *   <li>Otherwise → visible
 * </ul>
 * 
 * <p><b>Isolation Levels:</b>
 * <ul>
 *   <li>{@link IsolationLevel#READ_UNCOMMITTED} — always visible (dirty reads allowed)
 *   <li>{@link IsolationLevel#READ_COMMITTED} — sees data committed before statement start
 *   <li>{@link IsolationLevel#REPEATABLE_READ} — sees data committed before transaction start
 *   <li>{@link IsolationLevel#SERIALIZABLE} — same visibility as REPEATABLE_READ (conflict detection in step 5)
 * </ul>
 * 
 * <p><b>Snapshot Semantics:</b>
 * <ul>
 *   <li>READ_COMMITTED: snapshotTxid = statement-start snapshot
 *   <li>REPEATABLE_READ/SERIALIZABLE: snapshotTxid = transaction-start snapshot
 * </ul>
 * 
 * <p><b>CommandId:</b> Stored and round-trips through serialization; not used in step 1 visibility
 * (reserved for cursor/statement semantics in later steps).
 */
public final class TupleVisibility {
    
    private TupleVisibility() {
        throw new AssertionError("Utility class, do not instantiate");
    }

    /**
     * Simple contract overload — assumes every non-zero txid ≤ snapshot is committed.
     * Use when the caller knows all relevant txids are committed (e.g., bootstrap data).
     * 
     * <p>Contract: xmin > snapshot → invisible; xmax <= snapshot && xmax != 0 → invisible; else visible.
     */
    public static boolean visible(Row row, long currentTxid, long snapshotTxid, IsolationLevel level) {
        Objects.requireNonNull(row);
        Objects.requireNonNull(level);
        
        // For simple contract, assume every non-zero txid ≤ snapshot is committed
        return visible(row, currentTxid, snapshotTxid, level, txid -> {
            if (txid == 0) return false; // 0 = bootstrap, never "committed"
            return txid <= snapshotTxid;
        });
    }

    /**
     * Full contract — caller supplies predicate to check if a txid is committed.
     * This is the canonical API; UndoLog and TransactionTableSnapshot (steps 2–4) will provide
     * predicates that track active/committed txids correctly.
     */
    public static boolean visible(Row row, long currentTxid, long snapshotTxid,
                                 IsolationLevel level, LongPredicate txCommitted) {
        Objects.requireNonNull(row);
        Objects.requireNonNull(level);
        Objects.requireNonNull(txCommitted);
        
        // READ_UNCOMMITTED: always visible (dirty reads allowed)
        if (level == IsolationLevel.READ_UNCOMMITTED) {
            return true;
        }
        
        long xmin = row.getXmin();
        long xmax = row.getXmax();
        
        // 1. Own insert — visible to the creating transaction
        if (xmin == currentTxid) {
            return true;
        }
        
        // 2. Own delete — invisible to the deleting transaction
        if (xmax == currentTxid) {
            return false;
        }
        
        // 3. Created after snapshot — invisible
        if (xmin > snapshotTxid) {
            return false;
        }
        
        // 4. Insert by uncommitted tx — invisible
        if (xmin != 0 && !txCommitted.test(xmin)) {
            return false;
        }
        
        // 5. Deleted at/before snapshot by committed tx — invisible
        if (xmax != 0 && xmax <= snapshotTxid && txCommitted.test(xmax)) {
            return false;
        }
        
        // 6. Otherwise — visible
        return true;
    }

    /**
     * Status-based canonical contract used by the production read path
     * ({@link MvccReadContext} → {@link Table#isRowVisibleToReader(int)} → SELECT/DML scans).
     *
     * <p>Unlike the overload above, this variant never compares txids against a
     * snapshot counter directly: creator/deleter txids live in txid space while
     * snapshots live in commit-CSN space, so membership is decided by the
     * three-state status of each participant:
     * <ul>
     *   <li>{@code xmin == 0} (bootstrap) → visible; {@code xmin == currentTxid} → visible (own write)</li>
     *   <li>creator ABORTED → invisible; creator ACTIVE → visible only to dirty readers;
     *       creator COMMITTED → visible iff its commit is at/before the reader's snapshot</li>
     *   <li>{@code xmax == currentTxid} → invisible (own delete); {@code xmax == 0} → visible</li>
     *   <li>deleter ABORTED → visible; deleter COMMITTED → hidden iff its commit is at/before
     *       the reader's snapshot; deleter ACTIVE → hidden from dirty readers (they observe the
     *       pending delete), visible to everyone else</li>
     *   <li>unknown participant (pre-MVCC row, cleared tracker) → treated as committed/alive</li>
     * </ul>
     *
     * @param xmin                     creator txid (0 = bootstrap)
     * @param xmax                     deleter txid (0 = alive)
     * @param currentTxid              the reader's own txid (-1 for auto-commit readers)
     * @param dirtyReadsAllowed        true at READ UNCOMMITTED
     * @param committedBeforeSnapshot  true when the txid committed at/before the reader's snapshot CSN
     * @param statusOf                 status lookup for a txid, or null for unknown
     * @return true if the row must be returned to this reader
     */
    public static boolean visibleByStatus(long xmin, long xmax, long currentTxid,
                                          boolean dirtyReadsAllowed,
                                          LongPredicate committedBeforeSnapshot,
                                          java.util.function.LongFunction<TxStatusTracker.TxStatus> statusOf) {
        Objects.requireNonNull(committedBeforeSnapshot);
        Objects.requireNonNull(statusOf);

        // ── Creator rules ────────────────────────────────────────────────
        if (xmin != 0 && xmin != currentTxid) {
            TxStatusTracker.TxStatus creator = statusOf.apply(xmin);
            if (creator == TxStatusTracker.TxStatus.ABORTED) {
                return false;
            }
            if (creator == TxStatusTracker.TxStatus.ACTIVE && !dirtyReadsAllowed) {
                return false;
            }
            if (creator == TxStatusTracker.TxStatus.COMMITTED && !committedBeforeSnapshot.test(xmin)) {
                return false;
            }
            // UNKNOWN creator (pre-MVCC row): assume committed — fall through.
        }

        // ── Deleter rules ────────────────────────────────────────────────
        if (xmax != 0) {
            if (xmax == currentTxid) {
                return false; // own delete — the row is gone for its deleter
            }
            TxStatusTracker.TxStatus deleter = statusOf.apply(xmax);
            if (deleter == TxStatusTracker.TxStatus.ABORTED) {
                return true; // delete rolled back — row alive again
            }
            if (deleter == TxStatusTracker.TxStatus.COMMITTED) {
                return !committedBeforeSnapshot.test(xmax);
            }
            if (deleter == TxStatusTracker.TxStatus.ACTIVE) {
                return !dirtyReadsAllowed; // dirty readers observe the pending delete
            }
            // UNKNOWN deleter: assume the delete is not in effect yet.
        }
        return true;
    }

    /**
     * SSI (Serializable Snapshot Isolation) conflict detection predicate for write operations.
     * Encapsulates the write-side conflict check for SERIALIZABLE transactions:
     * - A row has a write conflict if another transaction has an uncommitted change (pending foreign change)
     * - Or if the row was committed by another transaction after the writer's snapshot (stale snapshot)
     * 
     * <p>This is the same logic as {@code Table.checkWriteWriteConflict} but as a standalone
     * predicate for use in SSI conflict detection (prompt4.md #5).
     * 
     * @param pendingOwnerTxid  txid of transaction with pending change on the row, or 0 if none
     * @param lastCommittedCsn  commit CSN of the last committed change to the row, or 0 if unknown/bootstrap
     * @param writerTxid        the transaction ID of the writer (to exclude self from pending check)
     * @param snapshotCsn       the writer's snapshot commit sequence number (BEGIN-time for SERIALIZABLE)
     * @return true if a write conflict is detected (row cannot be safely written by this transaction)
     */
    public static boolean hasSerializableWriteConflict(
            long pendingOwnerTxid, long lastCommittedCsn, long writerTxid, long snapshotCsn) {
        
        // Condition 1: Pending foreign change (another transaction owns this row)
        if (pendingOwnerTxid != 0 && pendingOwnerTxid != writerTxid) {
            return true;
        }
        
        // Condition 2: Stale snapshot (row committed after our snapshot)
        if (lastCommittedCsn > snapshotCsn) {
            return true;
        }
        
        return false;
    }
}