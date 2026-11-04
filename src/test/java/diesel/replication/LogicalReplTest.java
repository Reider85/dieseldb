package diesel.replication;

import diesel.Database;
import diesel.wal.WALManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for logical replication INSERT synchronization (prompt4 #61).
 * 
 * <p>Acceptance criteria: INSERT on master → INSERT on subscriber < 1 sec
 */
@Tag("replication")
@Tag("logical")
@Tag("storage")
public class LogicalReplTest extends BaseReplicationTest {

    private LogicalDecoder masterDecoder;
    private LogicalDecoder subscriberDecoder;
    
    @BeforeEach
    void setUpDecoders() {
        masterDecoder = new LogicalDecoder(master.getWalManager());
        subscriberDecoder = new LogicalDecoder(subscriber.getWalManager());
    }



    @Test
    void testInsertSynchronization() {
        // Create fresh database instances for this test to ensure clean state
        Database freshMaster = createFreshDatabase("test_master_fresh_" + System.currentTimeMillis());
        Database freshSubscriber = createFreshDatabase("test_subscriber_fresh_" + System.currentTimeMillis());
        
        // Clean up WAL completely
        resetWal(freshMaster);
        clearWalDirectory(freshMaster);
        resetWal(freshSubscriber);
        clearWalDirectory(freshSubscriber);
        
        // Create table in fresh databases
        freshMaster.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        freshSubscriber.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create publication and subscription in fresh databases
        freshMaster.executeQuery("CREATE PUBLICATION test_pub FOR TABLE test_table", null);
        freshSubscriber.executeQuery("CREATE PUBLICATION test_pub FOR TABLE test_table", null);
        freshSubscriber.executeQuery("CREATE SUBSCRIPTION test_sub TO PUBLICATION test_pub CONNECTION 'localhost:5432'", null);
        
        // Get fresh subscription
        Subscription freshSubscription = freshSubscriber.getReplicationRegistry().getSubscription("test_sub");
        
        // Set confirmed LSN to current WAL end to avoid re-applying historical data
        try {
            // Get the current maximum LSN from the WAL
            long maxLsn = freshMaster.getWalManager().getSegments().values().stream()
                .mapToLong(segment -> segment.getLastLsn())
                .max()
                .orElse(0L);
            freshSubscription.setConfirmedLsn(maxLsn);
            System.out.println("DEBUG: Set subscription confirmed LSN to: " + maxLsn);
        } catch (Exception e) {
            System.err.println("Warning: Could not set confirmed LSN: " + e.getMessage());
        }
        
        // Start replication pipeline
        WALManager masterWalManager = freshMaster.getWalManager();
        freshSubscriber.startReplicationPipeline("test_sub", masterWalManager);
        
        try {
            // Insert data on master with a unique ID to avoid conflicts
            insertTestDataInDatabase(freshMaster, 999, "John Doe", 100);
            
            // Wait for replication to apply
            try {
                Thread.sleep(500); // Give more time for replication to apply
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Verify the data was replicated to subscriber
            List<Map<String, Object>> results = (List<Map<String, Object>>) freshSubscriber.executeQuery("SELECT * FROM test_table WHERE id = 999", null);
            assertEquals(1, results.size(), "Subscriber should have 1 row with id=999");
            
            Map<String, Object> row = results.get(0);
            assertEquals(999, row.get("id"), "ID should match");
            assertEquals("John Doe", row.get("name"), "Name should match");
            assertEquals(100, row.get("value"), "Value should match");
        } finally {
            // Stop replication pipeline
            freshSubscriber.stopReplicationPipeline("test_sub");
            
            // Clean up fresh databases
            freshMaster.close();
            freshSubscriber.close();
        }
    }
    
    
    
    private void insertTestDataInDatabase(Database db, int id, String name, int value) {
        // Start transaction
        String beginResult = (String) db.executeQuery("BEGIN", null);
        UUID txId = UUID.fromString(beginResult.split(": ")[1]);
        
        // Execute INSERT within the transaction
        db.executeQuery("INSERT INTO test_table (id, name, value) VALUES (" + id + ", '" + name + "', " + value + ")", txId);
        
        // Commit the transaction
        db.executeQuery("COMMIT", txId);
        
        // Wait for replication
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private void cleanupWal(Database db) {
        try {
            // Force a checkpoint to clean up old WAL entries
            db.getWalManager().writeCheckpoint(java.util.Collections.emptyList());
            
            // Clear the replication registry to ensure fresh state
            db.getReplicationRegistry().clear();
            
            // Reset the subscription's confirmed LSN
            for (Subscription subscription : db.getReplicationRegistry().getAllSubscriptions()) {
                subscription.setConfirmedLsn(0);
            }
            
            System.out.println("DEBUG: Cleaned up WAL for database");
        } catch (Exception e) {
            System.err.println("Warning: Could not clean up WAL: " + e.getMessage());
        }
    }

    @Test
    void testMultipleInsertSynchronization() {
        // Create fresh database instances for this test to ensure clean state
        Database freshMaster = createFreshDatabase("test_master_fresh_multi_" + System.currentTimeMillis());
        Database freshSubscriber = createFreshDatabase("test_subscriber_fresh_multi_" + System.currentTimeMillis());
        
        // Clean up WAL completely
        resetWal(freshMaster);
        clearWalDirectory(freshMaster);
        resetWal(freshSubscriber);
        clearWalDirectory(freshSubscriber);
        
        // Create table in fresh databases
        freshMaster.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        freshSubscriber.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create publication and subscription in fresh databases
        freshMaster.executeQuery("CREATE PUBLICATION test_pub FOR TABLE test_table", null);
        freshSubscriber.executeQuery("CREATE PUBLICATION test_pub FOR TABLE test_table", null);
        freshSubscriber.executeQuery("CREATE SUBSCRIPTION test_sub TO PUBLICATION test_pub CONNECTION 'localhost:5432'", null);
        
        // Get fresh subscription
        Subscription freshSubscription = freshSubscriber.getReplicationRegistry().getSubscription("test_sub");
        
        // Set confirmed LSN to current WAL end to avoid re-applying historical data
        try {
            // Get the current maximum LSN from the WAL
            long maxLsn = freshMaster.getWalManager().getSegments().values().stream()
                .mapToLong(segment -> segment.getLastLsn())
                .max()
                .orElse(0L);
            freshSubscription.setConfirmedLsn(maxLsn);
            System.out.println("DEBUG: Set subscription confirmed LSN to: " + maxLsn);
        } catch (Exception e) {
            System.err.println("Warning: Could not set confirmed LSN: " + e.getMessage());
        }
        
        // Start replication pipeline
        WALManager masterWalManager = freshMaster.getWalManager();
        freshSubscriber.startReplicationPipeline("test_sub", masterWalManager);
        
        try {
            // Start transaction
            String beginResult = (String) freshMaster.executeQuery("BEGIN", null);
            UUID txId1 = UUID.fromString(beginResult.split(": ")[1]);
            
            // Insert multiple rows on master within the transaction
            freshMaster.executeQuery("INSERT INTO test_table (id, name, value) VALUES (1, 'Alice', 100)", txId1);
            freshMaster.executeQuery("INSERT INTO test_table (id, name, value) VALUES (2, 'Bob', 200)", txId1);
            
            // Commit the transaction
            freshMaster.executeQuery("COMMIT", txId1);
            
            // Wait for replication to apply
            try {
                Thread.sleep(500); // Give more time for replication to apply
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Verify all data was replicated to subscriber
            List<Map<String, Object>> results = (List<Map<String, Object>>) freshSubscriber.executeQuery("SELECT * FROM test_table ORDER BY id", null);
            assertEquals(2, results.size(), "Subscriber should have 2 rows");
            
            Map<String, Object> row1 = results.get(0);
            assertEquals(1, row1.get("id"), "First row ID should match");
            assertEquals("Alice", row1.get("name"), "First row name should match");
            
            Map<String, Object> row2 = results.get(1);
            assertEquals(2, row2.get("id"), "Second row ID should match");
            assertEquals("Bob", row2.get("name"), "Second row name should match");
        } finally {
            // Stop replication pipeline
            freshSubscriber.stopReplicationPipeline("test_sub");
            
            // Clean up fresh databases
            freshMaster.close();
            freshSubscriber.close();
        }
    }

    @Test
    void testLogicalDecoderCanReadChanges() {
        // Create fresh database instances for this test to ensure clean state
        Database freshMaster = createFreshDatabase("test_master_fresh_decoder_" + System.currentTimeMillis());
        
        // Clean up WAL completely
        resetWal(freshMaster);
        clearWalDirectory(freshMaster);
        
        // Create table in fresh database
        freshMaster.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create fresh decoder
        LogicalDecoder freshDecoder = new LogicalDecoder(freshMaster.getWalManager());
        
        // Insert data on master
        insertTestDataInDatabase(freshMaster, 1, "John Doe", 100);
        
        // Decode changes from master - start from LSN before current to capture the INSERT
        long currentLsn = freshMaster.getWalManager().getLastLsn();
        System.out.println("DEBUG: Starting decode from LSN: " + currentLsn);
        // Start from currentLsn - 1 to capture the INSERT that happened before the COMMIT
        List<LogicalChangeRecord> changes = freshDecoder.decode(Math.max(0, currentLsn - 1), 10);
        
        // Verify we got the change
        assertEquals(1, changes.size(), "Should have 1 change record");
        
        LogicalChangeRecord change = changes.get(0);
        assertEquals("test_table", change.getTableName(), "Table name should match");
        assertEquals(LogicalChangeRecord.Operation.INSERT, change.getOperation(), "Operation should be INSERT");
        assertNotNull(change.getAfterValues(), "After values should not be null");
        assertNull(change.getBeforeValues(), "Before values should be null for INSERT");
        
        // Verify the decoded data
        Map<String, Object> afterValues = change.getAfterValues();
        assertEquals(1, afterValues.get("id"), "ID should match");
        assertEquals("John Doe", afterValues.get("name"), "Name should match");
        assertEquals(100, afterValues.get("value"), "Value should match");
        
        // Clean up fresh database
        freshMaster.close();
    }

    @Test
    void testSubscriptionStatusTracking() {
        // Initially subscription should not be connected
        assertFalse(subscription.isConnected(), "Subscription should start as not connected");
        
        // Simulate connection (in real implementation, this would happen via network)
        subscription.setConnected(true);
        assertTrue(subscription.isConnected(), "Subscription should be connected");
        
        // Test subscription state
        assertEquals("test_pub", subscription.getPublicationName(), "Publication name should match");
        assertEquals("localhost", subscription.getMasterHost(), "Master host should match");
        assertEquals(5432, subscription.getMasterPort(), "Master port should match");
        assertEquals(Subscription.ConflictResolutionStrategy.ERROR, subscription.getConflictResolution(), "Conflict resolution should match");
    }

    @Test
    void testPublicationContainsTable() {
        assertTrue(publication.containsTable("test_table"), "Publication should contain test_table");
        assertFalse(publication.containsTable("orders"), "Publication should not contain orders table");
    }

    @Test
    void testEmptyDatabaseReplication() {
        // Create fresh database instances for this test to ensure clean state
        Database freshMaster = createFreshDatabase("test_master_fresh_empty_" + System.currentTimeMillis());
        Database freshSubscriber = createFreshDatabase("test_subscriber_fresh_empty_" + System.currentTimeMillis());
        
        // Clean up WAL completely
        resetWal(freshMaster);
        clearWalDirectory(freshMaster);
        resetWal(freshSubscriber);
        clearWalDirectory(freshSubscriber);
        
        // Create table in fresh databases
        freshMaster.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        freshSubscriber.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create fresh decoder
        LogicalDecoder freshDecoder = new LogicalDecoder(freshMaster.getWalManager());
        
        // Both databases should be empty initially
        List<Map<String, Object>> masterResults = (List<Map<String, Object>>) freshMaster.executeQuery("SELECT * FROM test_table", null);
        List<Map<String, Object>> subscriberResults = (List<Map<String, Object>>) freshSubscriber.executeQuery("SELECT * FROM test_table", null);
        
        assertEquals(0, masterResults.size(), "Master should be empty");
        assertEquals(0, subscriberResults.size(), "Subscriber should be empty");
        
        // No changes to decode - start from current WAL end to avoid historical data
        long currentLsn = freshMaster.getWalManager().getLastLsn();
        List<LogicalChangeRecord> changes = freshDecoder.decode(currentLsn, (int)(currentLsn + 10));
        assertEquals(0, changes.size(), "Should have no change records");
        
        // Clean up fresh databases
        freshMaster.close();
        freshSubscriber.close();
    }
}