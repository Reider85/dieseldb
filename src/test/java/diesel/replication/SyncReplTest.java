package diesel.replication;

import diesel.Database;
import diesel.IsolationLevel;
import diesel.SyncReplicationCoordinator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.UUID;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for synchronous replication (prompt4 #60, R3-016 step 3/5).
 * 
 * <p>Verifies that commits wait for standby acknowledgments when
 * synchronous replication is enabled.
 */
@Tag("replication")
@Tag("storage")
public class SyncReplTest {

    private Database database;

    @BeforeEach
    void setUp() {
        // Enable synchronous replication for testing
        System.setProperty("synchronous_commit", "on");
        System.setProperty("synchronous_standby_names", "standby-1,standby-2");
        
        database = new Database();
        database.setAutoCommit(false);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
        // Reset system properties
        System.clearProperty("synchronous_commit");
        System.clearProperty("synchronous_standby_names");
    }

    @Test
    void testSyncReplOneStandby() {
        // Set synchronous mode and create a new database instance
        System.setProperty("synchronous_commit", "on");
        System.setProperty("synchronous_standby_names", "standby1");
        Database newDatabase = new Database();
        newDatabase.setAutoCommit(false);
        
        try {
            // Create table (auto-commits)
            newDatabase.executeQuery("CREATE TABLE test_sync (id INTEGER PRIMARY KEY, name STRING)", null);
            
            // Start a transaction
            UUID txId = newDatabase.beginTransaction(IsolationLevel.READ_COMMITTED);
            
            // Insert some data
            newDatabase.executeQuery("INSERT INTO test_sync (id, name) VALUES (1, 'test')", txId);
            
            // The commit should wait for standby acknowledgment
            // Since we don't have actual standbys running, this should timeout
            assertThrows(Exception.class, () -> {
                newDatabase.executeQuery("COMMIT",txId);
            });
        } finally {
            newDatabase.close();
        }
    }

    @Test
    void testSyncReplOffMode() {
        // Test with synchronous replication disabled
        System.setProperty("synchronous_commit", "off");
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_off (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_off (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed immediately without waiting for standbys
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",txId);
        });
    }

    @Test
    void testSyncReplLocalMode() {
        // Test with local-only synchronous replication
        System.setProperty("synchronous_commit", "local");
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_local (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_local (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed after local write (no standby wait)
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",txId);
        });
    }

    @Test
    void testSyncReplNoStandbysConfigured() {
        // Test with synchronous replication enabled but no standbys configured
        System.setProperty("synchronous_commit", "on");
        System.setProperty("synchronous_standby_names", "");
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_nostandby (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_nostandby (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed immediately since no standbys are configured
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",txId);
        });
    }

    @Test
    void testSyncReplCoordinatorCreation() {
        // Test that the synchronous replication coordinator is created when needed
        System.setProperty("synchronous_commit", "on");
        
        // Create a new database instance to pick up the new configuration
        Database newDatabase = new Database();
        
        // The coordinator should be lazily created when first accessed
        SyncReplicationCoordinator coordinator = newDatabase.getSyncReplicationCoordinator();
        
        assertNotNull(coordinator, "Synchronous replication coordinator should be created");
        
        // Verify it's the same instance when accessed again
        SyncReplicationCoordinator coordinator2 = newDatabase.getSyncReplicationCoordinator();
        assertSame(coordinator, coordinator2, "Should return the same coordinator instance");
    }

    @Test
    void testSyncReplCoordinatorNotCreatedWhenOff() {
        // Test that the coordinator is not created when synchronous replication is off
        System.setProperty("synchronous_commit", "off");
        
        // The coordinator should not be created
        SyncReplicationCoordinator coordinator = database.getSyncReplicationCoordinator();
        
        assertNull(coordinator, "Synchronous replication coordinator should not be created when mode is off");
    }
}