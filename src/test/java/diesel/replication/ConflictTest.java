package diesel.replication;

import diesel.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for logical replication conflict resolution (prompt4 #61).
 *
 * <p>Acceptance criteria: same PK on master and subscriber → on_conflict=error/skip/replace works
 */
@Tag("replication")
@Tag("logical")
@Tag("storage")
public class ConflictTest extends BaseReplicationTest {

    private Publication errorPublication;
    private Publication skipPublication;
    private Publication replacePublication;
    private Subscription errorSubscription;
    private Subscription skipSubscription;
    private Subscription replaceSubscription;

    @BeforeEach
    void setUp() throws Exception {
        // Base class: enables WAL, cleans data dirs, creates master/subscriber with test_table
        super.setUp();

        dropIfPresent(master, "orders");
        dropIfPresent(subscriber, "orders");
        master.executeQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer STRING, amount INTEGER)", null);
        subscriber.executeQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer STRING, amount INTEGER)", null);

        // Publications must exist in both registries (subscription creation resolves locally)
        errorPublication = new Publication("orders_error_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.ERROR);
        skipPublication = new Publication("orders_skip_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.SKIP);
        replacePublication = new Publication("orders_replace_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.REPLACE);

        master.getLogicalReplicationRegistry().createPublication(errorPublication);
        master.getLogicalReplicationRegistry().createPublication(skipPublication);
        master.getLogicalReplicationRegistry().createPublication(replacePublication);

        subscriber.getLogicalReplicationRegistry().createPublication(
                new Publication("orders_error_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.ERROR));
        subscriber.getLogicalReplicationRegistry().createPublication(
                new Publication("orders_skip_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.SKIP));
        subscriber.getLogicalReplicationRegistry().createPublication(
                new Publication("orders_replace_pub", Set.of("orders"), Publication.ConflictResolutionStrategy.REPLACE));

        // Subscriptions with different conflict resolution strategies
        errorSubscription = new Subscription("orders_error_sub", "orders_error_pub", "localhost", 5432,
                                          Subscription.ConflictResolutionStrategy.ERROR, true);
        skipSubscription = new Subscription("orders_skip_sub", "orders_skip_pub", "localhost", 5432,
                                        Subscription.ConflictResolutionStrategy.SKIP, true);
        replaceSubscription = new Subscription("orders_replace_sub", "orders_replace_pub", "localhost", 5432,
                                            Subscription.ConflictResolutionStrategy.REPLACE, true);

        subscriber.getLogicalReplicationRegistry().createSubscription(errorSubscription);
        subscriber.getLogicalReplicationRegistry().createSubscription(skipSubscription);
        subscriber.getLogicalReplicationRegistry().createSubscription(replaceSubscription);
    }

    @AfterEach
    void stopPipelines() {
        if (subscriber != null) {
            for (String sub : new String[]{"orders_error_sub", "orders_skip_sub", "orders_replace_sub"}) {
                try {
                    subscriber.stopReplicationPipeline(sub);
                } catch (Exception ignored) {
                    // not running
                }
            }
        }
    }

    private UUID beginTx(Database db) {
        String beginResult = (String) db.executeQuery("BEGIN", null);
        return UUID.fromString(beginResult.split(": ")[1]);
    }

    private void seedSubscriber(int id, String customer, int amount) {
        UUID txId = beginTx(subscriber);
        subscriber.executeQuery("INSERT INTO orders (id, customer, amount) VALUES (" + id + ", '" + customer + "', " + amount + ")", txId);
        subscriber.executeQuery("COMMIT", txId);
    }

    private void insertOnMaster(int id, String customer, int amount) {
        UUID txId = beginTx(master);
        master.executeQuery("INSERT INTO orders (id, customer, amount) VALUES (" + id + ", '" + customer + "', " + amount + ")", txId);
        master.executeQuery("COMMIT", txId);
    }

    private void startPipelineFromNow(Subscription subscription) {
        subscription.setConfirmedLsn(master.getWalManager().getLastLsn());
        subscriber.startReplicationPipeline(subscription.getName(), master.getWalManager());
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testConflictResolutionError() {
        // Existing row on subscriber, conflicting insert on master
        seedSubscriber(1, "Existing Customer", 100);
        startPipelineFromNow(errorSubscription);

        insertOnMaster(1, "New Customer", 200);
        sleep(300);

        // ERROR strategy: conflicting change must NOT be applied
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders", null);
        assertEquals(1, results.size(), "Should still have 1 row");

        Map<String, Object> row = results.get(0);
        assertEquals("Existing Customer", row.get("customer"), "Original customer must be preserved under ERROR");
        assertEquals(100, row.get("amount"), "Original amount must be preserved under ERROR");
    }

    @Test
    void testConflictResolutionSkip() {
        seedSubscriber(1, "Existing Customer", 100);
        startPipelineFromNow(skipSubscription);

        insertOnMaster(1, "New Customer", 200);
        sleep(300);

        // Verify the original data was preserved (skip strategy)
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders", null);
        assertEquals(1, results.size(), "Should have 1 row due to skip strategy");

        Map<String, Object> row = results.get(0);
        assertEquals(1, row.get("id"), "ID should match");
        assertEquals("Existing Customer", row.get("customer"), "Customer should be original");
        assertEquals(100, row.get("amount"), "Amount should be original");
    }

    @Test
    void testConflictResolutionReplace() {
        seedSubscriber(1, "Existing Customer", 100);
        startPipelineFromNow(replaceSubscription);

        insertOnMaster(1, "New Customer", 200);
        sleep(300);

        // Verify the new data replaced the old data (replace strategy)
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders", null);
        assertEquals(1, results.size(), "Should have 1 row due to replace strategy");

        Map<String, Object> row = results.get(0);
        assertEquals(1, row.get("id"), "ID should match");
        assertEquals("New Customer", row.get("customer"), "Customer should be new");
        assertEquals(200, row.get("amount"), "Amount should be new");
    }

    @Test
    void testNoConflictWhenDifferentKeys() {
        seedSubscriber(1, "Customer 1", 100);
        startPipelineFromNow(skipSubscription);

        insertOnMaster(2, "Customer 2", 200);
        sleep(300);

        // Verify both records exist
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders ORDER BY id", null);
        assertEquals(2, results.size(), "Should have 2 rows");

        Map<String, Object> row1 = results.get(0);
        assertEquals(1, row1.get("id"), "First row ID should match");
        assertEquals("Customer 1", row1.get("customer"), "First row customer should match");

        Map<String, Object> row2 = results.get(1);
        assertEquals(2, row2.get("id"), "Second row ID should match");
        assertEquals("Customer 2", row2.get("customer"), "Second row customer should match");
    }

    @Test
    void testUpdateConflictResolution() {
        // Same row exists on both sides before replication starts
        insertOnMaster(1, "Existing Customer", 100);
        seedSubscriber(1, "Existing Customer", 100);
        startPipelineFromNow(skipSubscription);

        // Update the data on master
        UUID txId = beginTx(master);
        master.executeQuery("UPDATE orders SET amount = 200 WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);
        sleep(300);

        // Verify the update was applied
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders", null);
        assertEquals(1, results.size(), "Should have 1 row");

        Map<String, Object> row = results.get(0);
        assertEquals(1, row.get("id"), "ID should match");
        assertEquals("Existing Customer", row.get("customer"), "Customer should match");
        assertEquals(200, row.get("amount"), "Amount should be updated");
    }

    @Test
    void testDeleteConflictResolution() {
        insertOnMaster(1, "Existing Customer", 100);
        seedSubscriber(1, "Existing Customer", 100);
        startPipelineFromNow(skipSubscription);

        // Delete the data on master
        UUID txId = beginTx(master);
        master.executeQuery("DELETE FROM orders WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);
        sleep(300);

        // Verify the delete was applied
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM orders", null);
        assertEquals(0, results.size(), "Should have 0 rows");
    }

    @Test
    void testConflictResolutionStrategyEnum() {
        // Test the enum values
        assertEquals(Publication.ConflictResolutionStrategy.ERROR, Publication.ConflictResolutionStrategy.valueOf("ERROR"));
        assertEquals(Publication.ConflictResolutionStrategy.SKIP, Publication.ConflictResolutionStrategy.valueOf("SKIP"));
        assertEquals(Publication.ConflictResolutionStrategy.REPLACE, Publication.ConflictResolutionStrategy.valueOf("REPLACE"));

        assertEquals(Subscription.ConflictResolutionStrategy.ERROR, Subscription.ConflictResolutionStrategy.valueOf("ERROR"));
        assertEquals(Subscription.ConflictResolutionStrategy.SKIP, Subscription.ConflictResolutionStrategy.valueOf("SKIP"));
        assertEquals(Subscription.ConflictResolutionStrategy.REPLACE, Subscription.ConflictResolutionStrategy.valueOf("REPLACE"));
    }

    @Test
    void testSubscriptionConflictResolutionConfiguration() {
        // Test that subscriptions have the correct conflict resolution strategies
        assertEquals(Subscription.ConflictResolutionStrategy.ERROR, errorSubscription.getConflictResolution());
        assertEquals(Subscription.ConflictResolutionStrategy.SKIP, skipSubscription.getConflictResolution());
        assertEquals(Subscription.ConflictResolutionStrategy.REPLACE, replaceSubscription.getConflictResolution());
    }
}
