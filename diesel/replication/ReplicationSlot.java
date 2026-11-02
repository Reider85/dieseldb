package diesel.replication;

import java.io.Serializable;
import java.util.Objects;

/**
 * A replication slot that tracks the progress of standby databases.
 * Slots prevent WAL segments from being deleted until standbys have confirmed receipt.
 */
public class ReplicationSlot implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String name;
    private long confirmedFlushLsn;
    private long restartLsn;
    private final long createdAt;
    
    /**
     * Creates a new replication slot.
     * 
     * @param name the unique name of the slot
     * @param initialLsn the initial LSN (typically the current WAL end)
     */
    public ReplicationSlot(String name, long initialLsn) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Slot name cannot be null or empty");
        }
        this.name = name;
        this.confirmedFlushLsn = initialLsn;
        this.restartLsn = initialLsn;
        this.createdAt = System.currentTimeMillis();
    }
    
    /**
     * Gets the slot name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the highest LSN confirmed by the standby.
     */
    public long getConfirmedFlushLsn() {
        return confirmedFlushLsn;
    }
    
    /**
     * Updates the confirmed flush LSN.
     * 
     * @param lsn the new confirmed LSN
     */
    public void setConfirmedFlushLsn(long lsn) {
        if (lsn < 0) {
            throw new IllegalArgumentException("LSN cannot be negative: " + lsn);
        }
        this.confirmedFlushLsn = Math.max(this.confirmedFlushLsn, lsn);
    }
    
    /**
     * Gets the LSN from which the standby can restart if disconnected.
     */
    public long getRestartLsn() {
        return restartLsn;
    }
    
    /**
     * Updates the restart LSN.
     * 
     * @param lsn the new restart LSN
     */
    public void setRestartLsn(long lsn) {
        if (lsn < 0) {
            throw new IllegalArgumentException("LSN cannot be negative: " + lsn);
        }
        this.restartLsn = lsn;
    }
    
    /**
     * Gets the creation timestamp.
     */
    public long getCreatedAt() {
        return createdAt;
    }
    
    /**
     * Validates that this slot can be safely deleted.
     * A slot can be deleted if its confirmed LSN is beyond the given threshold.
     * 
     * @param lsnThreshold the LSN threshold
     * @return true if the slot can be deleted, false otherwise
     */
    public boolean canBeDeleted(long lsnThreshold) {
        return confirmedFlushLsn >= lsnThreshold;
    }
    
    /**
     * Checks if the slot is stale (not updated for a long time).
     * 
     * @param staleThresholdMs the stale threshold in milliseconds
     * @return true if the slot is stale, false otherwise
     */
    public boolean isStale(long staleThresholdMs) {
        return System.currentTimeMillis() - getCreatedAt() > staleThresholdMs;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationSlot that = (ReplicationSlot) o;
        return Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
    
    @Override
    public String toString() {
        return String.format("ReplicationSlot{name='%s', confirmedFlushLsn=%d, restartLsn=%d, createdAt=%d}",
                name, confirmedFlushLsn, restartLsn, createdAt);
    }
}