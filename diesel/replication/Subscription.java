package diesel.replication;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a subscription to a publication for logical replication.
 * Manages the replication state and synchronization with the master.
 */
public class Subscription {
    
    private final String name;
    private final String publicationName;
    private final String masterHost;
    private final int masterPort;
    private final UUID subscriptionId;
    private final long createdTime;
    private final boolean enabled;
    private final ConflictResolutionStrategy conflictResolution;
    
    // Runtime state
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final ConcurrentMap<String, Long> tableLsnMap = new ConcurrentHashMap<>();
    private volatile long confirmedLsn = 0;
    private volatile long lastSyncTime;
    private volatile long lastErrorTime;
    private volatile String lastError;
    
    public Subscription(String name, String publicationName, String masterHost, int masterPort,
                       ConflictResolutionStrategy conflictResolution, boolean enabled) {
        this.name = name;
        this.publicationName = publicationName;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.conflictResolution = conflictResolution;
        this.enabled = enabled;
        this.subscriptionId = UUID.randomUUID();
        this.createdTime = System.currentTimeMillis();
        this.lastSyncTime = System.currentTimeMillis();
        this.lastErrorTime = 0;
        this.lastError = null;
    }
    
    public String getName() {
        return name;
    }
    
    public String getPublicationName() {
        return publicationName;
    }
    
    public String getMasterHost() {
        return masterHost;
    }
    
    public int getMasterPort() {
        return masterPort;
    }
    
    public UUID getSubscriptionId() {
        return subscriptionId;
    }
    
    public long getCreatedTime() {
        return createdTime;
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public ConflictResolutionStrategy getConflictResolution() {
        return conflictResolution;
    }
    
    public boolean isConnected() {
        return connected.get();
    }
    
    public void setConnected(boolean connected) {
        this.connected.set(connected);
    }
    
    public ConcurrentMap<String, Long> getTableLsnMap() {
        return tableLsnMap;
    }
    
    public long getConfirmedLsn() {
        return confirmedLsn;
    }
    
    public void setConfirmedLsn(long confirmedLsn) {
        this.confirmedLsn = confirmedLsn;
    }
    
    public long getLastSyncTime() {
        return lastSyncTime;
    }
    
    public void setLastSyncTime(long lastSyncTime) {
        this.lastSyncTime = lastSyncTime;
    }
    
    public long getLastErrorTime() {
        return lastErrorTime;
    }
    
    public String getLastError() {
        return lastError;
    }
    
    public void setLastError(String error) {
        this.lastError = error;
        this.lastErrorTime = System.currentTimeMillis();
    }
    
    public void clearError() {
        this.lastError = null;
        this.lastErrorTime = 0;
    }
    
    public void updateTableLsn(String tableName, long lsn) {
        tableLsnMap.put(tableName, lsn);
    }
    
    public Long getTableLsn(String tableName) {
        return tableLsnMap.get(tableName);
    }
    
    public void resetState() {
        connected.set(false);
        tableLsnMap.clear();
        lastSyncTime = System.currentTimeMillis();
        clearError();
    }
    
    public boolean hasSyncedData() {
        return !tableLsnMap.isEmpty();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
    
    @Override
    public String toString() {
        return String.format("Subscription{name='%s', publication='%s', master=%s:%d, enabled=%s, connected=%s, conflictResolution=%s}",
                           name, publicationName, masterHost, masterPort, enabled, connected.get(), conflictResolution);
    }
    
    /**
     * Conflict resolution strategies for logical replication.
     */
    public enum ConflictResolutionStrategy {
        ERROR,    // Throw error on conflict (default)
        SKIP,     // Skip conflicting changes
        REPLACE   // Replace with master data
    }
}