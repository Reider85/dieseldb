package diesel.replication;

import diesel.Database;
import diesel.CreateReplicationSlotQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to understand replication slot persistence issues
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReplicationSlotDebugTest {
    
    private static final String TEST_DIR = "test_data_debug_" + System.currentTimeMillis();
    private Database database;
    
    @BeforeEach
    void setUp() {
        // Clean up any existing test directory
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
        
        // Create new database instance
        database = new Database(TEST_DIR);
    }
    
    @Test
    @Order(1)
    void testCreateAndDebugSlot() {
        System.out.println("=== Creating slot ===");
        
        // Create slot
        CreateReplicationSlotQuery createQuery = new CreateReplicationSlotQuery("debug_slot");
        String result = createQuery.execute(database, -1);
        System.out.println("Create result: " + result);
        
        // Verify slot exists
        ReplicationSlot slot = database.getReplicationSlot("debug_slot");
        assertNotNull(slot);
        System.out.println("Initial slot confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        System.out.println("Initial slot restartLsn: " + slot.getRestartLsn());
        
        // Update slot
        System.out.println("=== Updating slot ===");
        boolean updated = database.updateReplicationSlotConfirmedFlushLsn("debug_slot", 7000);
        System.out.println("Update result: " + updated);
        
        // Check slot after update
        slot = database.getReplicationSlot("debug_slot");
        System.out.println("Updated slot confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        
        // Close and reopen database
        System.out.println("=== Closing and reopening database ===");
        database.close();
        
        database = new Database(TEST_DIR);
        
        // Check slot after reload
        slot = database.getReplicationSlot("debug_slot");
        System.out.println("Reloaded slot confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        
        // Verify the final value
        assertEquals(7000, slot.getConfirmedFlushLsn());
    }
    
    private void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}