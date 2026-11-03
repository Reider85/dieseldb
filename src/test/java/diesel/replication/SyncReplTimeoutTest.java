package diesel.replication;

import diesel.Database;
import diesel.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for synchronous replication timeout handling (prompt4 #60, R3-016 step 3/5).
 * 
 * <p>Verifies that commits timeout when standbys don't respond within
 * the configured timeout period.
 */
@Tag("replication")
@Tag("storage")
public class SyncReplTimeoutTest {

    private Database database;

    @BeforeEach
    void setUp() {
        // Enable synchronous replication with short timeout for testing
        System.setProperty("synchronous_commit", "on");
        System.setProperty("synchronous_standby_names", "standby-1,standby-2");
        System.setProperty("synchronous_commit.timeout.ms", "1000"); // 1 second timeout
        
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
        System.clearProperty("synchronous_commit.timeout.ms");
    }

    @Test
    void testCommitTimeoutWhenStandbysUnavailable() {
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_timeout (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_timeout (id, name) VALUES (1, 'test')", txId);
        
        // The commit should timeout waiting for standby acknowledgments
        // Since we don't have actual standbys running, this will timeout
        Exception exception = assertThrows(Exception.class, () -> {
            database.executeQuery("COMMIT",txId);
        });
        
        // Verify it's a timeout-related exception
        assertTrue(exception.getMessage().contains("timeout") || 
                   exception.getMessage().contains("synchronous replication"),
                   "Exception should indicate timeout or synchronous replication failure");
    }

    @Test
    void testShortTimeoutConfiguration() {
        // Test with very short timeout
        System.setProperty("synchronous_commit.timeout.ms", "100"); // 100ms
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_short_timeout (id INTEGER PRIMARY KEY, name STRING)", null);
        
// Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_short_timeout (id, name) VALUES (1, 'test')", txId);
        
        // The commit should timeout waiting for standbys
assertThrows(Exception.class, () -> {
            database.executeQuery("COMMIT",txId);
        }, "Should timeout even with longer timeout");
    }

    @Test
    void testLongTimeoutConfiguration() {
        // Test with longer timeout
        System.setProperty("synchronous_commit.timeout.ms", "5000"); // 5 seconds
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_long_timeout (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_long_timeout (id, name) VALUES (1, 'test')", txId);
        
        // The commit should still timeout since no standbys are running,
        // but it should take longer than the short timeout test
        assertThrows(Exception.class, () -> {
            database.executeQuery("COMMIT",txId);
        });
    }

    @Test
    void testTimeoutDoesNotAffectNonSyncMode() {
        // Test that timeout doesn't affect non-synchronous commits
        System.setProperty("synchronous_commit", "off");
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_no_timeout (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_no_timeout (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed immediately without timeout
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",txId);
        });
    }

    @Test
    void testTimeoutWithLocalMode() {
        // Test that timeout doesn't affect local mode
        System.setProperty("synchronous_commit", "local");
        
        // Create table (auto-commits)
        database.executeQuery("CREATE TABLE test_local_timeout (id INTEGER PRIMARY KEY, name STRING)", null);
        
        // Start a transaction
        UUID txId = database.beginTransaction(IsolationLevel.READ_COMMITTED);
        
        // Insert some data
        database.executeQuery("INSERT INTO test_local_timeout (id, name) VALUES (1, 'test')", txId);
        
        // The commit should proceed immediately without waiting for standbys
        assertDoesNotThrow(() -> {
            database.executeQuery("COMMIT",txId);
        });
    }
}