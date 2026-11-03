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
 * Tests for synchronous replication off mode (prompt4 #60, R3-016 step 3/5).
 * 
 * <p>Verifies that commits proceed immediately without waiting for
 * standby acknowledgments when synchronous replication is disabled.
 */
@Tag("replication")
@Tag("storage")
public class SyncReplOffTest {

    private Database database;

    @BeforeEach
    void setUp() {
        // Ensure synchronous replication is disabled
        System.setProperty("synchronous_commit", "off");
        
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
    }

@Test
    void testOffModeCommitsImmediately() {
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_off_mode (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_off_mode (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed immediately without waiting for standbys
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT", txId);
        });
        
        // Verify the data was committed
        UUID readTxId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = (List<Map<String, Object>>) database.executeQuery("SELECT COUNT(*) AS count FROM test_off_mode", readTxId);
        assertEquals(1L, result.get(0).get("count"));
        database.executeQuery("COMMIT", readTxId);
    }

    @Test
    void testOffModeWithMultipleOperations() {
        // Create a table
        database.executeQuery("CREATE TABLE test_off_multiple (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert multiple rows
        database.executeQuery("INSERT INTO test_off_multiple (id, name, value) VALUES (1, 'test1', 100)", txId);
        database.executeQuery("INSERT INTO test_off_multiple (id, name, value) VALUES (2, 'test2', 200)", txId);
        database.executeQuery("UPDATE test_off_multiple SET value = 150 WHERE id = 1", txId);
        database.executeQuery("DELETE FROM test_off_multiple WHERE id = 2", txId);
        
        // The commit should proceed immediately
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT", txId);
        });
        
        // Verify the final state
        UUID readTxId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        @SuppressWarnings("unchecked")
        var result = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM test_off_multiple", readTxId);
        int count = 0;
        for (var row : result) {
            count++;
            assertEquals(1, row.get("id"));
            assertEquals("test1", row.get("name"));
            assertEquals(150, row.get("value"));
        }
        assertEquals(1, count, "Should have only one row remaining");
        database.executeQuery("COMMIT", readTxId);
    }

    @Test
    void testOffModePerformance() {
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_off_perf (id INTEGER PRIMARY KEY, data STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert many rows to test performance
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            database.executeQuery(String.format("INSERT INTO test_off_perf (id, data) VALUES (%d, 'data%d')", i, i), txId);
        }
        
        // Commit should be fast since no standby wait
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT", txId);
        });
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Should complete in reasonable time (much faster than with standby wait)
        assertTrue(duration < 5000, "Commit should be fast without standby wait, took " + duration + "ms");
        
        // Verify all data was committed
        UUID readTxId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        @SuppressWarnings("unchecked")
        var result = (List<Map<String, Object>>) database.executeQuery("SELECT COUNT(*) as count FROM test_off_perf", readTxId);
        assertEquals(1, result.size());
        var countRow = result.get(0);
        assertEquals(1000L, countRow.get("count"));
        database.executeQuery("COMMIT", readTxId);
    }

    @Test
    void testOffModeWithDifferentIsolationLevels() {
        // Test different isolation levels with off mode
        database.executeQuery("CREATE TABLE test_off_isolation (id INTEGER PRIMARY KEY, value INTEGER)", null);
        
        // Test READ COMMITTED
        UUID tx1 = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        database.executeQuery("INSERT INTO test_off_isolation (id, value) VALUES (1, 100)", tx1);
        database.executeQuery("COMMIT",tx1);
        
        // Test REPEATABLE READ
        UUID tx2 = database.beginTransaction(IsolationLevel.REPEATABLE_READ);
        database.executeQuery("UPDATE test_off_isolation SET value = 200 WHERE id = 1", tx2);
        database.executeQuery("COMMIT",tx2);
        
        // Test SERIALIZABLE
        UUID tx3 = database.beginTransaction(IsolationLevel.SERIALIZABLE);
        database.executeQuery("UPDATE test_off_isolation SET value = 300 WHERE id = 1", tx3);
        database.executeQuery("COMMIT",tx3);
        
        // Verify final state
        UUID tx4 = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        @SuppressWarnings("unchecked")
        var result = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM test_off_isolation", tx4);
        assertEquals(1, result.size());
        var row = result.get(0);
        assertEquals(1, row.get("id"));
        assertEquals(300, row.get("value"));
        database.executeQuery("COMMIT", tx4);
    }

    @Test
    void testOffModeCoordinatorNotCreated() {
        // Verify that the synchronous replication coordinator is not created in off mode
        SyncReplicationCoordinator coordinator = database.getSyncReplicationCoordinator();
        
        assertNull(coordinator, "Synchronous replication coordinator should not be created in off mode");
    }

    @Test
    void testOffModeWithEmptyStandbyNames() {
        // Test with explicit empty standby names
        System.setProperty("synchronous_standby_names", "");
        
        // Create a table
        database.executeQuery("CREATE TABLE test_off_empty (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID tx = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_off_empty (id, name) VALUES (1, 'test')", tx);
        
        // The commit should proceed immediately
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",tx);
        });
        
        // Verify the data was committed
        UUID readTx = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        @SuppressWarnings("unchecked")
        var result = (List<Map<String, Object>>) database.executeQuery("SELECT * FROM test_off_empty WHERE id = 1", readTx);
        assertEquals(1, result.size(), "Data should be committed and visible");
        database.executeQuery("COMMIT",readTx);
    }
}