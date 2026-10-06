package diesel;

import java.io.Serializable;
import java.util.Map;

/**
 * MVCC metadata for a single row in a table.
 * 
 * <p>Tracks the transaction that created this version (xmin), the transaction that
 * deleted it (xmax), and provides access to the committed values for visibility.
 * 
 * <p><b>Fields:</b>
 * <ul>
 *   <li>{@code xmin} — transaction ID that created this version; 0 = bootstrap/initial state</li>
 *   <li>{@code xmax} — transaction ID that deleted this version; 0 = row is alive</li>
 *   <li>{@code commandId} — statement ordinal within the creating transaction; 0 = unused</li>
 *   <li>{@code committedValues} — pre-image of the last change (for readers whose snapshot predates it); null if the row was never changed</li>
 *   <li>{@code uncommittedInsert} — true if this row was inserted by an uncommitted transaction</li>
 *   <li>{@code uncommittedDelete} — true if this row was deleted by an uncommitted transaction</li>
 *   <li>{@code uncommittedUpdate} — true if this row was updated by an uncommitted transaction (shadow model)</li>
 *   <li>{@code ownerTxid} — transaction owning the uncommitted change; 0 = no pending change</li>
 *   <li>{@code lastCommittedCsn} — commit CSN of the last commit that changed this row; 0 = unknown/bootstrap</li>
 * </ul>
 */
public class RowVersionMeta implements Serializable {
    private static final long serialVersionUID = 1L;

    private long xmin; // creator txid; 0 = bootstrap
    private long xmax; // deleter txid; 0 = alive
    private long commandId; // statement ordinal; 0 = unused
    private Map<String, Object> committedValues; // pre-image of the last change (null = no retained pre-image)
    private boolean uncommittedInsert;
    private boolean uncommittedDelete;
    private boolean uncommittedUpdate;
    private long ownerTxid; // txn owning the pending change; 0 = none
    private long lastCommittedCsn; // CSN of the last commit touching this row; 0 = unknown

    /**
     * Creates metadata for a bootstrap row (xmin=0, xmax=0).
     */
    public RowVersionMeta() {
        this.xmin = 0;
        this.xmax = 0;
        this.commandId = 0;
        this.committedValues = null;
        this.uncommittedInsert = false;
        this.uncommittedDelete = false;
        this.uncommittedUpdate = false;
        this.ownerTxid = 0;
        this.lastCommittedCsn = 0;
    }

    /**
     * Creates metadata for a row inserted by a transaction.
     */
    public RowVersionMeta(long xmin, Map<String, Object> committedValues) {
        this.xmin = xmin;
        this.xmax = 0;
        this.commandId = 0;
        this.committedValues = committedValues;
        this.uncommittedInsert = true;
        this.uncommittedDelete = false;
        this.uncommittedUpdate = false;
        this.ownerTxid = xmin;
        this.lastCommittedCsn = 0;
    }

    /**
     * Creates metadata for a row with explicit version info.
     */
    public RowVersionMeta(long xmin, long xmax, long commandId, 
                         Map<String, Object> committedValues,
                         boolean uncommittedInsert, boolean uncommittedDelete) {
        this(xmin, xmax, commandId, committedValues, uncommittedInsert, uncommittedDelete, false, 0, 0);
    }

    /**
     * Creates metadata for a row with the full version info.
     */
    public RowVersionMeta(long xmin, long xmax, long commandId,
                         Map<String, Object> committedValues,
                         boolean uncommittedInsert, boolean uncommittedDelete,
                         boolean uncommittedUpdate, long ownerTxid, long lastCommittedCsn) {
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandId = commandId;
        this.committedValues = committedValues;
        this.uncommittedInsert = uncommittedInsert;
        this.uncommittedDelete = uncommittedDelete;
        this.uncommittedUpdate = uncommittedUpdate;
        this.ownerTxid = ownerTxid;
        this.lastCommittedCsn = lastCommittedCsn;
    }

    /**
     * Returns an independent copy of this metadata. Used by undo records to
     * snapshot the pre-change state before a mutating mark (update/delete)
     * alters the live meta in place.
     *
     * @return a deep copy of the values map with identical flags
     */
    public RowVersionMeta copy() {
        Map<String, Object> copiedValues =
                committedValues == null ? null : new java.util.HashMap<>(committedValues);
        return new RowVersionMeta(xmin, xmax, commandId, copiedValues,
                uncommittedInsert, uncommittedDelete, uncommittedUpdate, ownerTxid, lastCommittedCsn);
    }

    // Getters and setters
    public long getXmin() {
        return xmin;
    }

    public void setXmin(long xmin) {
        this.xmin = xmin;
    }

    public long getXmax() {
        return xmax;
    }

    public void setXmax(long xmax) {
        this.xmax = xmax;
    }

    public long getCommandId() {
        return commandId;
    }

    public void setCommandId(long commandId) {
        this.commandId = commandId;
    }

    public Map<String, Object> getCommittedValues() {
        return committedValues;
    }

    public void setCommittedValues(Map<String, Object> committedValues) {
        this.committedValues = committedValues;
    }

    public boolean isUncommittedInsert() {
        return uncommittedInsert;
    }

    public void setUncommittedInsert(boolean uncommittedInsert) {
        this.uncommittedInsert = uncommittedInsert;
    }

    public boolean isUncommittedDelete() {
        return uncommittedDelete;
    }

    public void setUncommittedDelete(boolean uncommittedDelete) {
        this.uncommittedDelete = uncommittedDelete;
    }

    public boolean isUncommittedUpdate() {
        return uncommittedUpdate;
    }

    public void setUncommittedUpdate(boolean uncommittedUpdate) {
        this.uncommittedUpdate = uncommittedUpdate;
    }

    public long getOwnerTxid() {
        return ownerTxid;
    }

    public void setOwnerTxid(long ownerTxid) {
        this.ownerTxid = ownerTxid;
    }

    public long getLastCommittedCsn() {
        return lastCommittedCsn;
    }

    public void setLastCommittedCsn(long lastCommittedCsn) {
        this.lastCommittedCsn = lastCommittedCsn;
    }

    /**
     * Marks this row's pending change as committed (clears uncommitted flags
     * and the owner). The retained pre-image ({@link #getCommittedValues()})
     * is intentionally kept: readers whose snapshot predates this commit must
     * still see the previous values. The commit CSN is only advanced when a
     * positive one is supplied.
     */
    public void markCommitted() {
        markCommitted(0);
    }

    /**
     * Marks this row's pending change as committed at the given commit CSN.
     *
     * @param commitCsn the commit sequence number, or 0 to keep the current one
     */
    public void markCommitted(long commitCsn) {
        this.uncommittedInsert = false;
        this.uncommittedDelete = false;
        this.uncommittedUpdate = false;
        this.ownerTxid = 0;
        if (commitCsn > 0) {
            this.lastCommittedCsn = commitCsn;
        }
    }

    /**
     * Marks this row as aborted (clears uncommitted flags, resets xmax for delete).
     */
    public void markAborted() {
        if (uncommittedDelete) {
            this.xmax = 0; // Restore row to alive state
        }
        this.uncommittedInsert = false;
        this.uncommittedDelete = false;
        this.uncommittedUpdate = false;
        this.ownerTxid = 0;
    }

    /**
     * Returns true if this row is currently alive (not deleted).
     */
    public boolean isAlive() {
        return xmax == 0;
    }

    /**
     * Returns true if this row has uncommitted changes.
     */
    public boolean hasUncommittedChanges() {
        return uncommittedInsert || uncommittedDelete || uncommittedUpdate;
    }
}