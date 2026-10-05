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
}