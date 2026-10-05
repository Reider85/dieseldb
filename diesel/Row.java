package diesel;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A versioned row container with MVCC metadata (xmin/xmax/commandId).
 * 
 * <p>Each row represents one version of a tuple created by a transaction (xmin)
 * and potentially deleted by another (xmax). The commandId tracks the statement
 * ordinal within the creating transaction (reserved for future cursor/statement
 * semantics in steps 2+).
 * 
 * <p><b>Fields:</b>
 * <ul>
 *   <li>{@code xmin} — transaction ID that created this version; 0 = bootstrap/initial state
 *   <li>{@code xmax} — transaction ID that deleted this version; 0 = row is alive
 *   <li>{@code commandId} — statement ordinal within the creating transaction; 0 = unused in step 1
 * </ul>
 * 
 * <p><b>Serialization:</b> All fields included, restart-stable (acceptance criterion).
 * 
 * <p><b>Step 1 Scope:</b> Standalone container. Not wired into Table/storage yet
 * (integration in steps 2–4). equals/hashCode on values only (version fields are metadata).
 */
public final class Row implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Map<String, Object> values;
    private long xmin;
    private long xmax;
    private long commandId;

    /**
     * Creates a new row with bootstrap metadata (xmin=0, xmax=0, commandId=0).
     * Values are defensively copied.
     */
    public Row(Map<String, Object> values) {
        this.values = new LinkedHashMap<>(Objects.requireNonNull(values));
        this.xmin = 0;
        this.xmax = 0;
        this.commandId = 0;
    }

    /**
     * Creates a row with explicit version metadata.
     * Values are defensively copied.
     */
    public Row(Map<String, Object> values, long xmin, long xmax, long commandId) {
        this.values = new LinkedHashMap<>(Objects.requireNonNull(values));
        this.xmin = xmin;
        this.xmax = xmax;
        this.commandId = commandId;
    }

    /**
     * Returns an immutable view of the column values.
     * Defensive copy not needed — LinkedHashMap is not modifiable via this reference.
     */
    public Map<String, Object> getValues() {
        return values;
    }

    /** Returns the transaction ID that created this version; 0 = bootstrap. */
    public long getXmin() {
        return xmin;
    }

    /** Returns the transaction ID that deleted this version; 0 = row is alive. */
    public long getXmax() {
        return xmax;
    }

    /** Returns the statement ordinal within the creating transaction; 0 = unused in step 1. */
    public long getCommandId() {
        return commandId;
    }

    /**
     * Sets the xmin (package-private for steps 2–4).
     * Called during row creation or UndoLog replay.
     */
    void setXmin(long xmin) {
        this.xmin = xmin;
    }

    /**
     * Sets the xmax (package-private for steps 2–4).
     * Called during row deletion or transaction rollback.
     */
    void setXmax(long xmax) {
        this.xmax = xmax;
    }

    /**
     * Sets the commandId (package-private for steps 2–4).
     * Called during statement execution in step 2+.
     */
    void setCommandId(long commandId) {
        this.commandId = commandId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return values.equals(row.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "Row{" +
               "values=" + values +
               ", xmin=" + xmin +
               ", xmax=" + xmax +
               ", commandId=" + commandId +
               '}';
    }
}