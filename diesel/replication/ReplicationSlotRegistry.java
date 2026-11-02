package diesel.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe registry for managing replication slots.
 * Provides CRUD operations and thread-safe access to replication slots.
 */
public class ReplicationSlotRegistry {
    private final ConcurrentMap<String, ReplicationSlot> slots = new ConcurrentHashMap<>();
    
    /**
     * Creates a new replication slot.
     * 
     * @param name the slot name
     * @param initialLsn the initial LSN
     * @return the created slot
     * @throws IllegalArgumentException if slot name is invalid
     * @throws IllegalStateException if slot already exists
     */
    public ReplicationSlot createSlot(String name, long initialLsn) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Slot name cannot be null or empty");
        }
        
        ReplicationSlot slot = new ReplicationSlot(name, initialLsn);
        ReplicationSlot existing = slots.putIfAbsent(name, slot);
        
        if (existing != null) {
            throw new IllegalStateException("Replication slot already exists: " + name);
        }
        
        return slot;
    }
    
    /**
     * Gets a replication slot by name.
     * 
     * @param name the slot name
     * @return the slot, or null if not found
     */
    public ReplicationSlot getSlot(String name) {
        return slots.get(name);
    }
    
    /**
     * Updates the confirmed flush LSN for a slot.
     * 
     * @param name the slot name
     * @param lsn the new confirmed LSN
     * @return true if updated successfully, false if slot not found
     */
    public boolean updateConfirmedFlushLsn(String name, long lsn) {
        ReplicationSlot slot = slots.get(name);
        if (slot != null) {
            slot.setConfirmedFlushLsn(lsn);
            return true;
        }
        return false;
    }
    
    /**
     * Updates the restart LSN for a slot.
     * 
     * @param name the slot name
     * @param lsn the new restart LSN
     * @return true if updated successfully, false if slot not found
     */
    public boolean updateRestartLsn(String name, long lsn) {
        ReplicationSlot slot = slots.get(name);
        if (slot != null) {
            slot.setRestartLsn(lsn);
            return true;
        }
        return false;
    }
    
    /**
     * Drops a replication slot.
     * 
     * @param name the slot name
     * @return true if slot was dropped, false if not found
     */
    public boolean dropSlot(String name) {
        return slots.remove(name) != null;
    }
    
    /**
     * Checks if a slot exists.
     * 
     * @param name the slot name
     * @return true if slot exists, false otherwise
     */
    public boolean slotExists(String name) {
        return slots.containsKey(name);
    }
    
    /**
     * Gets all replication slots.
     * 
     * @return unmodifiable map of all slots
     */
    public Map<String, ReplicationSlot> getAllSlots() {
        return Map.copyOf(slots);
    }
    
    /**
     * Gets the number of replication slots.
     * 
     * @return the slot count
     */
    public int getSlotCount() {
        return slots.size();
    }
    
    /**
     * Clears all replication slots.
     */
    public void clear() {
        slots.clear();
    }
    
    /**
     * Checks if any slot prevents WAL segments from being deleted up to the given LSN.
     * 
     * @param lsn the LSN to check
     * @return true if any slot needs WAL segments beyond this LSN, false otherwise
     */
    public boolean needsWALSegmentsBeyond(long lsn) {
        return slots.values().stream()
                .anyMatch(slot -> slot.getConfirmedFlushLsn() > lsn);
    }
    
    /**
     * Finds the minimum LSN that all slots have confirmed.
     * This is the safe LSN for WAL segment deletion.
     * 
     * @return the minimum confirmed LSN, or -1 if no slots exist
     */
    public long getMinimumConfirmedLsn() {
        return slots.values().stream()
                .mapToLong(ReplicationSlot::getConfirmedFlushLsn)
                .min()
                .orElse(-1);
    }
    
    /**
     * Removes stale slots (older than the given threshold).
     * 
     * @param staleThresholdMs the stale threshold in milliseconds
     * @return number of stale slots removed
     */
    public int removeStaleSlots(long staleThresholdMs) {
        int originalSize = slots.size();
        slots.entrySet().removeIf(entry -> 
                entry.getValue().isStale(staleThresholdMs));
        return originalSize - slots.size();
    }
}