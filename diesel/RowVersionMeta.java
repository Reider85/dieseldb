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
 *   <li>{@code committedValues} — last committed values of the row; null if never committed or same as current</li>
 *   <li>{@code uncommittedInsert} — true if this row was inserted by an uncommitted transaction</li>
 *   <li>{@code uncommittedDelete} — true if this row was deleted by an uncommitted transaction</li>
 * </ul>
 */
public class RowVersionMeta implements Serializable {
    private static final long serialVersionUID = 1L;

    private long xmin; // creator txid; 0 = bootstrap
    private long xmax; // deleter txid; 0 = alive
    private long commandId; // statement ordinal; 0 = unused
    private Map<String, Object> committedValues; // last committed values (null = no uncommitted changes)
    private boolean uncommittedInsert;
    private boolean uncommittedDelete;

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
    }

    /**
     * Creates metadata for a row with explicit version info.
     */
    public RowVersionMeta(long xmin, long xmax, long commandId, 
                         Map<String, Object> committedValues,
                         boolean uncommittedInsert, boolean uncommittedDelete) {
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandId = commandId;
        this.committedValues = committedValues;
        this.uncommittedInsert = uncommittedInsert;
        this.uncommittedDelete = uncommittedDelete;
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

    /**
     * Marks this row as committed (clears uncommitted flags).
     */
    public void markCommitted() {
        this.uncommittedInsert = false;
        this.uncommittedDelete = false;
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
        return uncommittedInsert || uncommittedDelete;
    }
}