package diesel;

/**
 * Synchronous replication acknowledgment (prompt4 #60, R3-016 step 3/5).
 * 
 * <p>An ACK represents a standby's confirmation that it has applied a WAL record
 * up to a specific LSN. The coordinator collects ACKs from configured standbys
 * before allowing a COMMIT to proceed to the client.
 */
public final class SyncAck {
    
    private final String standbyName;
    private final long appliedLsn;
    private final long receivedTimestamp;
    
    /**
     * Creates a new synchronous replication acknowledgment.
     * 
     * @param standbyName the name of the standby that sent this ACK
     * @param appliedLsn the highest LSN the standby has confirmed applying
     */
    public SyncAck(String standbyName, long appliedLsn) {
        if (standbyName == null || standbyName.trim().isEmpty()) {
            throw new IllegalArgumentException("Standby name cannot be null or empty");
        }
        if (appliedLsn < 0) {
            throw new IllegalArgumentException("Applied LSN cannot be negative: " + appliedLsn);
        }
        this.standbyName = standbyName.trim();
        this.appliedLsn = appliedLsn;
        this.receivedTimestamp = System.currentTimeMillis();
    }
    
    /**
     * Gets the name of the standby that sent this ACK.
     */
    public String getStandbyName() {
        return standbyName;
    }
    
    /**
     * Gets the highest LSN the standby has confirmed applying.
     */
    public long getAppliedLsn() {
        return appliedLsn;
    }
    
    /**
     * Gets the timestamp when this ACK was received.
     */
    public long getReceivedTimestamp() {
        return receivedTimestamp;
    }
    
    /**
     * Checks if this ACK is up-to-date with the given commit LSN.
     * 
     * @param commitLsn the LSN that needs to be confirmed
     * @return true if the standby has applied at least up to the commit LSN, false otherwise
     */
    public boolean isUpToDate(long commitLsn) {
        return appliedLsn >= commitLsn;
    }
    
    @Override
    public String toString() {
        return String.format("SyncAck{standby='%s', appliedLsn=%d, receivedTime=%d}",
                standbyName, appliedLsn, receivedTimestamp);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyncAck syncAck = (SyncAck) o;
        return appliedLsn == syncAck.appliedLsn &&
               standbyName.equals(syncAck.standbyName);
    }
    
    @Override
    public int hashCode() {
        return standbyName.hashCode() + 31 * Long.hashCode(appliedLsn);
    }
}