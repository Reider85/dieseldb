package diesel.replication;

import diesel.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for logical replication UPDATE/DELETE synchronization (prompt4 #61).
 *
 * <p>Acceptance criteria: UPDATE/DELETE synchronize correctly
 */
@Tag("replication")
@Tag("logical")
@Tag("storage")
public class UpdateDeleteTest extends BaseReplicationTest {

    private LogicalDecoder masterDecoder;

    @BeforeEach
    void setUp() throws Exception {
        // Call base class setup first
        super.setUp();

        // Override the table to use products instead of test_table
        dropIfPresent(master, "products");
        dropIfPresent(subscriber, "products");
        master.executeQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, name STRING, price INTEGER, stock INTEGER)", null);
        subscriber.executeQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, name STRING, price INTEGER, stock INTEGER)", null);

        // Create publication in both registries (subscription creation resolves it locally)
        publication = new Publication("products_pub", Set.of("products"), Publication.ConflictResolutionStrategy.ERROR);
        master.getLogicalReplicationRegistry().createPublication(publication);
        subscriber.getLogicalReplicationRegistry().createPublication(
                new Publication("products_pub", Set.of("products"), Publication.ConflictResolutionStrategy.ERROR));

        // Create subscription
        subscription = new Subscription("products_sub", "products_pub", "localhost", 5432,
                                      Subscription.ConflictResolutionStrategy.ERROR, true);
        subscriber.getLogicalReplicationRegistry().createSubscription(subscription);

        // Create decoder
        masterDecoder = new LogicalDecoder(master.getWalManager(), publication);

        // Insert initial data on master
        UUID txId = beginTx(master);
        master.executeQuery("INSERT INTO products (id, name, price, stock) VALUES (1, 'Laptop', 1000, 10)", txId);
        master.executeQuery("COMMIT", txId);

        // Seed subscriber with the same row directly, then start replication from now on
        UUID subTxId = beginTx(subscriber);
        subscriber.executeQuery("INSERT INTO products (id, name, price, stock) VALUES (1, 'Laptop', 1000, 10)", subTxId);
        subscriber.executeQuery("COMMIT", subTxId);

        subscription.setConfirmedLsn(master.getWalManager().getLastLsn());
        subscriber.startReplicationPipeline("products_sub", master.getWalManager());

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @org.junit.jupiter.api.AfterEach
    void stopPipeline() {
        if (subscriber != null) {
            try {
                subscriber.stopReplicationPipeline("products_sub");
            } catch (Exception ignored) {
                // pipeline may not be running
            }
        }
    }

    private UUID beginTx(Database db) {
        String beginResult = (String) db.executeQuery("BEGIN", null);
        return UUID.fromString(beginResult.split(": ")[1]);
    }

    private UUID beginTx() {
        return beginTx(master);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testUpdateSynchronization() {
        // Update data on master
        UUID txId = beginTx();
        master.executeQuery("UPDATE products SET price = 900, stock = 15 WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the update was replicated to subscriber
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM products", null);
        assertEquals(1, results.size(), "Subscriber should have 1 row");

        Map<String, Object> row = results.get(0);
        assertEquals(1, row.get("id"), "ID should match");
        assertEquals("Laptop", row.get("name"), "Name should match");
        assertEquals(900, row.get("price"), "Price should be updated");
        assertEquals(15, row.get("stock"), "Stock should be updated");
    }

    @Test
    void testDeleteSynchronization() {
        // Delete data on master
        UUID txId = beginTx();
        master.executeQuery("DELETE FROM products WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the delete was replicated to subscriber
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM products", null);
        assertEquals(0, results.size(), "Subscriber should have 0 rows");
    }

    @Test
    void testUpdateDecoderCanReadChanges() {
        // Capture LSN before the update
        long startLsn = master.getWalManager().getLastLsn() + 1;

        // Update data on master
        UUID txId = beginTx();
        master.executeQuery("UPDATE products SET price = 900 WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);

        sleep(200);

        // Decode changes from master starting at the captured LSN
        List<LogicalChangeRecord> changes = masterDecoder.decode(startLsn, 10);

        assertEquals(1, changes.size(), "Should have 1 change record");

        LogicalChangeRecord change = changes.get(0);
        assertEquals("products", change.getTableName(), "Table name should match");
        assertEquals(LogicalChangeRecord.Operation.UPDATE, change.getOperation(), "Operation should be UPDATE");
        assertNotNull(change.getBeforeValues(), "Before values should not be null for UPDATE");
        assertNotNull(change.getAfterValues(), "After values should not be null for UPDATE");

        // Verify the decoded data
        Map<String, Object> beforeValues = change.getBeforeValues();
        Map<String, Object> afterValues = change.getAfterValues();

        assertEquals(1, beforeValues.get("id"), "Before ID should match");
        assertEquals(1000, beforeValues.get("price"), "Before price should match");

        assertEquals(1, afterValues.get("id"), "After ID should match");
        assertEquals(900, afterValues.get("price"), "After price should match");
    }

    @Test
    void testDeleteDecoderCanReadChanges() {
        // Capture LSN before the delete
        long startLsn = master.getWalManager().getLastLsn() + 1;

        // Delete data on master
        UUID txId = beginTx();
        master.executeQuery("DELETE FROM products WHERE id = 1", txId);
        master.executeQuery("COMMIT", txId);

        sleep(200);

        // Decode changes from master starting at the captured LSN
        List<LogicalChangeRecord> changes = masterDecoder.decode(startLsn, 10);

        assertEquals(1, changes.size(), "Should have 1 change record");

        LogicalChangeRecord change = changes.get(0);
        assertEquals("products", change.getTableName(), "Table name should match");
        assertEquals(LogicalChangeRecord.Operation.DELETE, change.getOperation(), "Operation should be DELETE");
        assertNotNull(change.getBeforeValues(), "Before values should not be null for DELETE");
        assertNull(change.getAfterValues(), "After values should be null for DELETE");

        // Verify the decoded data
        Map<String, Object> beforeValues = change.getBeforeValues();
        assertEquals(1, beforeValues.get("id"), "Before ID should match");
        assertEquals("Laptop", beforeValues.get("name"), "Before name should match");
    }

    @Test
    void testComplexUpdateSynchronization() {
        // Insert an additional product (id=1 already exists from setUp)
        UUID txId1 = beginTx();
        master.executeQuery("INSERT INTO products (id, name, price, stock) VALUES (2, 'Phone', 500, 20)", txId1);
        master.executeQuery("COMMIT", txId1);

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Update multiple products
        UUID txId2 = beginTx();
        master.executeQuery("UPDATE products SET price = 900 WHERE id = 1", txId2);
        master.executeQuery("UPDATE products SET stock = 25 WHERE id = 2", txId2);
        master.executeQuery("COMMIT", txId2);

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify all updates were replicated to subscriber
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM products ORDER BY id", null);
        assertEquals(2, results.size(), "Subscriber should have 2 rows");

        Map<String, Object> row1 = results.get(0);
        assertEquals(1, row1.get("id"), "First row ID should match");
        assertEquals("Laptop", row1.get("name"), "First row name should match");
        assertEquals(900, row1.get("price"), "First row price should be updated");
        assertEquals(10, row1.get("stock"), "First row stock should be unchanged");

        Map<String, Object> row2 = results.get(1);
        assertEquals(2, row2.get("id"), "Second row ID should match");
        assertEquals("Phone", row2.get("name"), "Second row name should match");
        assertEquals(500, row2.get("price"), "Second row price should be unchanged");
        assertEquals(25, row2.get("stock"), "Second row stock should be updated");
    }

    @Test
    void testDeleteNonExistentRecord() {
        // Try to delete non-existent record
        UUID txId = beginTx();
        master.executeQuery("DELETE FROM products WHERE id = 999", txId);
        master.executeQuery("COMMIT", txId);

        // Wait for replication
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify subscriber still has the original data
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM products", null);
        assertEquals(1, results.size(), "Subscriber should still have 1 row");
    }
}
