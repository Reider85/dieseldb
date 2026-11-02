package diesel.replication;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import static org.junit.jupiter.api.Assertions.*;

import diesel.Database;
import diesel.CreateReplicationSlotQuery;
import diesel.DropReplicationSlotQuery;

/**
 * Integration tests for replication slots with the database (prompt4 #59).
 */
@Tag("query")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicationSlotIntegrationTest {

    private Database database;
    private static final String TEST_DIR = "test_data_replication_" + System.currentTimeMillis();

    @BeforeEach
    void setUp() {
        database = new Database(TEST_DIR);
        // Initialize the replication slot registry by calling a public method that uses it
        // Create a temporary slot to ensure the registry is initialized
        String tempSlotName = "temp_init_slot_" + System.currentTimeMillis();
        database.createReplicationSlot(tempSlotName, 0);
        // Drop it immediately to have a clean state
        database.dropReplicationSlot(tempSlotName);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
        // Clean up test data directory
        new java.io.File(TEST_DIR).delete();
    }

    @Test
    @Order(1)
    void testCreateReplicationSlot() {
        CreateReplicationSlotQuery query = new CreateReplicationSlotQuery("test_slot");
        String result = query.execute(database, -1);
        
        assertEquals("Replication slot created: test_slot", result);
        
        // Verify slot exists
        ReplicationSlot slot = database.getReplicationSlot("test_slot");
        assertNotNull(slot);
        assertEquals("test_slot", slot.getName());
        assertTrue(slot.getConfirmedFlushLsn() >= 0);
        assertTrue(slot.getRestartLsn() >= 0);
    }

    @Test
    @Order(2)
    void testCreateDuplicateReplicationSlot() {
        // Create first slot
        CreateReplicationSlotQuery query1 = new CreateReplicationSlotQuery("duplicate_slot");
        query1.execute(database, -1);
        
        // Try to create duplicate slot
        CreateReplicationSlotQuery query2 = new CreateReplicationSlotQuery("duplicate_slot");
        assertThrows(IllegalStateException.class, () -> {
            query2.execute(database, -1);
        });
    }

    @Test
    @Order(3)
    void testDropReplicationSlot() {
        // Create slot first
        CreateReplicationSlotQuery createQuery = new CreateReplicationSlotQuery("drop_test_slot");
        createQuery.execute(database, -1);
        
        // Drop slot
        DropReplicationSlotQuery dropQuery = new DropReplicationSlotQuery("drop_test_slot");
        String result = dropQuery.execute(database, -1);
        
        assertEquals("Replication slot dropped: drop_test_slot", result);
        
        // Verify slot is gone
        assertNull(database.getReplicationSlot("drop_test_slot"));
        assertFalse(database.dropReplicationSlot("drop_test_slot"));
    }

    @Test
    @Order(4)
    void testDropNonExistentReplicationSlot() {
        DropReplicationSlotQuery query = new DropReplicationSlotQuery("nonexistent_slot");
        String result = query.execute(database, -1);
        
        assertEquals("Replication slot not found: nonexistent_slot", result);
    }

    @Test
    @Order(5)
    void testGetAllReplicationSlots() {
        // Create multiple slots
        CreateReplicationSlotQuery query1 = new CreateReplicationSlotQuery("slot1");
        CreateReplicationSlotQuery query2 = new CreateReplicationSlotQuery("slot2");
        query1.execute(database, -1);
        query2.execute(database, -1);
        
        var allSlots = database.getAllReplicationSlots();
        assertEquals(2, allSlots.size());
        assertTrue(allSlots.containsKey("slot1"));
        assertTrue(allSlots.containsKey("slot2"));
    }

    @Test
    @Order(6)
    void testUpdateReplicationSlotConfirmedFlushLsn() {
        // Create slot
        CreateReplicationSlotQuery createQuery = new CreateReplicationSlotQuery("update_test_slot");
        createQuery.execute(database, -1);
        
        // Update confirmed flush LSN
        boolean updated = database.updateReplicationSlotConfirmedFlushLsn("update_test_slot", 5000);
        assertTrue(updated);
        
        // Verify update
        ReplicationSlot slot = database.getReplicationSlot("update_test_slot");
        assertEquals(5000, slot.getConfirmedFlushLsn());
        
        // Update with lower LSN should not decrease
        database.updateReplicationSlotConfirmedFlushLsn("update_test_slot", 3000);
        assertEquals(5000, slot.getConfirmedFlushLsn());
    }

    @Test
    @Order(7)
    void testUpdateNonExistentReplicationSlot() {
        boolean updated = database.updateReplicationSlotConfirmedFlushLsn("nonexistent", 5000);
        assertFalse(updated);
    }

    @Test
    @Order(8)
    void testGetMinimumConfirmedReplicationLsn() {
        // Initially no slots
        assertEquals(-1, database.getMinimumConfirmedReplicationLsn());
        
        // Create first slot
        CreateReplicationSlotQuery query1 = new CreateReplicationSlotQuery("min_test_slot1");
        query1.execute(database, -1);
        ReplicationSlot slot1 = database.getReplicationSlot("min_test_slot1");
        long initialLsn = slot1.getConfirmedFlushLsn();
        
        assertEquals(initialLsn, database.getMinimumConfirmedReplicationLsn());
        
        // Create second slot with higher confirmed LSN
        CreateReplicationSlotQuery query2 = new CreateReplicationSlotQuery("min_test_slot2");
        query2.execute(database, -1);
        database.updateReplicationSlotConfirmedFlushLsn("min_test_slot2", initialLsn + 1000);
        
        assertEquals(initialLsn, database.getMinimumConfirmedReplicationLsn());
        
        // Update first slot to higher LSN
        database.updateReplicationSlotConfirmedFlushLsn("min_test_slot1", initialLsn + 2000);
        
        assertEquals(initialLsn + 1000, database.getMinimumConfirmedReplicationLsn());
    }

    @Test
    @Order(9)
    void testReplicationSlotsNeedWALSegmentsBeyond() {
        // Initially no slots
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(1000));
        
        // Create slot
        CreateReplicationSlotQuery query = new CreateReplicationSlotQuery("need_test_slot");
        query.execute(database, -1);
        ReplicationSlot slot = database.getReplicationSlot("need_test_slot");
        long initialLsn = slot.getConfirmedFlushLsn();
        
        // Slot needs segments beyond LSN lower than confirmed LSN
        assertTrue(database.replicationSlotsNeedWALSegmentsBeyond(initialLsn - 1));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(initialLsn));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(initialLsn + 1));
        
        // Update slot to higher confirmed LSN
        database.updateReplicationSlotConfirmedFlushLsn("need_test_slot", initialLsn + 1000);
        
        assertTrue(database.replicationSlotsNeedWALSegmentsBeyond(initialLsn + 500));
        assertFalse(database.replicationSlotsNeedWALSegmentsBeyond(initialLsn + 1000));
    }

    @Test
    @Order(10)
    void testReplicationSlotPersistence() {
        // Create slot
        CreateReplicationSlotQuery createQuery = new CreateReplicationSlotQuery("persist_test_slot");
        createQuery.execute(database, -1);
        
        // Verify slot exists
        assertNotNull(database.getReplicationSlot("persist_test_slot"));
        
        // Update slot
        database.updateReplicationSlotConfirmedFlushLsn("persist_test_slot", 7000);
        
        // Close and reopen database
        database.close();
        database = new Database(TEST_DIR);
        
        // Verify slot is still there with updated values
        ReplicationSlot slot = database.getReplicationSlot("persist_test_slot");
        assertNotNull(slot);
        assertEquals("persist_test_slot", slot.getName());
        assertEquals(7000, slot.getConfirmedFlushLsn());
    }

    @Test
    @Order(11)
    void testMultipleReplicationSlots() {
        // Create multiple slots
        CreateReplicationSlotQuery query1 = new CreateReplicationSlotQuery("multi1");
        CreateReplicationSlotQuery query2 = new CreateReplicationSlotQuery("multi2");
        CreateReplicationSlotQuery query3 = new CreateReplicationSlotQuery("multi3");
        
        query1.execute(database, -1);
        query2.execute(database, -1);
        query3.execute(database, -1);
        
        // Verify all slots exist
        assertNotNull(database.getReplicationSlot("multi1"));
        assertNotNull(database.getReplicationSlot("multi2"));
        assertNotNull(database.getReplicationSlot("multi3"));
        
        // Update some slots
        database.updateReplicationSlotConfirmedFlushLsn("multi1", 1000);
        database.updateReplicationSlotConfirmedFlushLsn("multi2", 2000);
        database.updateReplicationSlotConfirmedFlushLsn("multi3", 3000);
        
        // Verify minimum confirmed LSN
        assertEquals(1000, database.getMinimumConfirmedReplicationLsn());
        
        // Drop one slot
        DropReplicationSlotQuery dropQuery = new DropReplicationSlotQuery("multi2");
        dropQuery.execute(database, -1);
        
        // Verify remaining slots
        assertNotNull(database.getReplicationSlot("multi1"));
        assertNull(database.getReplicationSlot("multi2"));
        assertNotNull(database.getReplicationSlot("multi3"));
        
        // Verify minimum confirmed LSN after drop
        assertEquals(1000, database.getMinimumConfirmedReplicationLsn());
    }
}