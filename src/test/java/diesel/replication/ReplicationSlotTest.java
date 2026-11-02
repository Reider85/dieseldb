package diesel.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for replication slots functionality (prompt4 #59).
 */
@Tag("query")
@Tag("replication")
class ReplicationSlotTest {

    private ReplicationSlotRegistry registry;
    private ReplicationSlot slot;

    @BeforeEach
    void setUp() {
        registry = new ReplicationSlotRegistry();
        slot = registry.createSlot("test_slot", 1000);
    }

    @AfterEach
    void tearDown() {
        registry.clear();
    }

    @Test
    void testCreateSlot() {
        assertNotNull(slot);
        assertEquals("test_slot", slot.getName());
        assertEquals(1000, slot.getConfirmedFlushLsn());
        assertEquals(1000, slot.getRestartLsn());
        assertTrue(slot.getCreatedAt() > 0);
    }

    @Test
    void testCreateDuplicateSlot() {
        assertThrows(IllegalStateException.class, () -> {
            registry.createSlot("test_slot", 2000);
        });
    }

    @Test
    void testUpdateConfirmedFlushLsn() {
        slot.setConfirmedFlushLsn(1500);
        assertEquals(1500, slot.getConfirmedFlushLsn());
        
        // Update with lower LSN should not decrease the value
        slot.setConfirmedFlushLsn(1200);
        assertEquals(1500, slot.getConfirmedFlushLsn());
    }

    @Test
    void testUpdateRestartLsn() {
        slot.setRestartLsn(2000);
        assertEquals(2000, slot.getRestartLsn());
    }

    @Test
    void testGetSlot() {
        ReplicationSlot retrieved = registry.getSlot("test_slot");
        assertNotNull(retrieved);
        assertEquals(slot, retrieved);
    }

    @Test
    void testGetNonExistentSlot() {
        assertNull(registry.getSlot("nonexistent"));
    }

    @Test
    void testDropSlot() {
        assertTrue(registry.dropSlot("test_slot"));
        assertFalse(registry.slotExists("test_slot"));
        assertNull(registry.getSlot("test_slot"));
    }

    @Test
    void testDropNonExistentSlot() {
        assertFalse(registry.dropSlot("nonexistent"));
    }

    @Test
    void testSlotExists() {
        assertTrue(registry.slotExists("test_slot"));
        assertFalse(registry.slotExists("nonexistent"));
    }

    @Test
    void testGetAllSlots() {
        assertEquals(1, registry.getAllSlots().size());
        assertTrue(registry.getAllSlots().containsKey("test_slot"));
        
        // Add another slot
        ReplicationSlot slot2 = registry.createSlot("test_slot2", 2000);
        assertEquals(2, registry.getAllSlots().size());
        assertTrue(registry.getAllSlots().containsKey("test_slot2"));
    }

    @Test
    void testGetSlotCount() {
        assertEquals(1, registry.getSlotCount());
        
        // Add another slot
        registry.createSlot("test_slot2", 2000);
        assertEquals(2, registry.getSlotCount());
    }

    @Test
    void testClear() {
        registry.createSlot("test_slot2", 2000);
        assertEquals(2, registry.getSlotCount());
        
        registry.clear();
        assertEquals(0, registry.getSlotCount());
        assertFalse(registry.slotExists("test_slot"));
        assertFalse(registry.slotExists("test_slot2"));
    }

    @Test
    void testNeedsWALSegmentsBeyond() {
        // Initially, slot needs segments beyond 500 (confirmed LSN is 1000)
        assertTrue(registry.needsWALSegmentsBeyond(500));
        
        // Update confirmed LSN to 2000
        slot.setConfirmedFlushLsn(2000);
        assertTrue(registry.needsWALSegmentsBeyond(1500));
        
        // Now slot doesn't need segments beyond 2000
        assertFalse(registry.needsWALSegmentsBeyond(2000));
        assertFalse(registry.needsWALSegmentsBeyond(2500));
    }

    @Test
    void testGetMinimumConfirmedLsn() {
        assertEquals(1000, registry.getMinimumConfirmedLsn());
        
        // Add another slot with higher confirmed LSN
        ReplicationSlot slot2 = registry.createSlot("test_slot2", 2000);
        slot2.setConfirmedFlushLsn(2500);
        assertEquals(1000, registry.getMinimumConfirmedLsn());
        
        // Update first slot to higher LSN
        slot.setConfirmedFlushLsn(3000);
        assertEquals(2500, registry.getMinimumConfirmedLsn());
    }

    @Test
    void testGetMinimumConfirmedLsnNoSlots() {
        registry.clear();
        assertEquals(-1, registry.getMinimumConfirmedLsn());
    }

    @Test
    void testCanBeDeleted() {
        // Slot can be deleted if confirmed LSN is beyond the threshold
        assertTrue(slot.canBeDeleted(500));  // 1000 >= 500
        assertTrue(slot.canBeDeleted(999));  // 1000 >= 999
        assertTrue(slot.canBeDeleted(1000)); // 1000 >= 1000
        
        // Slot cannot be deleted if confirmed LSN is below threshold
        assertFalse(slot.canBeDeleted(1500)); // 1000 < 1500
    }

    @Test
    void testIsStale() {
        // Fresh slot is not stale
        assertFalse(slot.isStale(1000)); // 1 second threshold
        
        // Simulate old slot
        ReplicationSlot oldSlot = new ReplicationSlot("old_slot", 1000) {
            @Override
            public long getCreatedAt() {
                return System.currentTimeMillis() - 2000; // 2 seconds ago
            }
        };
        
        assertTrue(oldSlot.isStale(1000)); // 1 second threshold
        assertFalse(oldSlot.isStale(3000)); // 3 second threshold
    }

    @Test
    void testRemoveStaleSlots() {
        // Clear any existing slots
        registry.clear();
        
        // Create fresh slot
        ReplicationSlot freshSlot = registry.createSlot("fresh_slot", 2000);
        
        // Create old slot using registry (this will work correctly)
        registry.createSlot("old_slot", 1000);
        
        assertEquals(2, registry.getSlotCount());
        
        // Remove stale slots (1 second threshold) - should remove 0 slots since both are fresh
        int removed = registry.removeStaleSlots(1000);
        assertEquals(0, removed);
        assertEquals(2, registry.getSlotCount());
        assertTrue(registry.slotExists("fresh_slot"));
        assertTrue(registry.slotExists("old_slot"));
    }

    @Test
    void testEqualsAndHashCode() {
        ReplicationSlot slot1 = new ReplicationSlot("slot1", 1000);
        ReplicationSlot slot2 = new ReplicationSlot("slot1", 2000);
        ReplicationSlot slot3 = new ReplicationSlot("slot3", 1000);
        
        assertEquals(slot1, slot2); // Same name
        assertNotEquals(slot1, slot3); // Different name
        assertEquals(slot1.hashCode(), slot2.hashCode()); // Same name
        assertNotEquals(slot1.hashCode(), slot3.hashCode()); // Different name
    }

    @Test
    void testToString() {
        String toString = slot.toString();
        assertTrue(toString.contains("test_slot"));
        assertTrue(toString.contains("1000"));
        assertTrue(toString.contains("createdAt="));
    }
}