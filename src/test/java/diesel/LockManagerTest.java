package diesel;

import diesel.concurrency.LockManager;
import diesel.concurrency.LockMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LockManager basic functionality.
 * <p>
 * Tests lock acquisition, compatibility, conflicts, and upgrades.
 */
@Tag("concurrency")
class LockManagerTest {

    private static final long TEST_TXID = 12345L;
    private static final String TEST_RESOURCE = "table:USERS";

    @Test
    void sSCompatibility() throws InterruptedException {
        LockManager manager = new LockManager();
        
        // Both S locks should be granted
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        
        // Should hold two S locks (merged)
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(TEST_RESOURCE));
    }

    @Test
    void sXUpgrade() throws InterruptedException {
        LockManager manager = new LockManager();
        
        // Grant S lock
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        
        // X lock should be allowed as upgrade for same transaction
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.X, TEST_TXID));
        assertEquals(Collections.singleton(LockMode.X), manager.heldModes(TEST_RESOURCE));
    }

    @Test
    @Timeout(5)
    void xXConflictWithWait() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        
        // Thread 1 acquires X lock
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean tx2GotLock = new AtomicBoolean(false);
        
        // First let tx1 acquire the lock
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.X, tx1));
        
        Future<?> future = executor.submit(() -> {
            try {
                // This should block until tx1 releases
                manager.acquire(TEST_RESOURCE, LockMode.X, tx2);
                tx2GotLock.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        // Give tx2 time to start waiting
        Thread.sleep(500);
        
        // Verify tx2 is waiting (before releasing tx1's lock)
        assertEquals(1, manager.waiterCount(TEST_RESOURCE));
        
        // Release tx1's lock to wake up tx2
        manager.release(tx1);
        
        // Wait for tx2 to acquire the lock
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Test failed: tx2 did not acquire lock", e);
        }
        
        assertTrue(tx2GotLock.get());
        assertEquals(1, manager.resourceCount());
        
        // Verify tx2 now holds the X lock
        assertEquals(Collections.singleton(LockMode.X), manager.heldModes(TEST_RESOURCE));
        
        executor.shutdown();
    }

    @Test
    void upgradeSToX() throws InterruptedException {
        LockManager manager = new LockManager();
        
        // Acquire S lock
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(TEST_RESOURCE));
        
        // Upgrade to X lock (should succeed when no other holders)
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.X, TEST_TXID));
        assertEquals(Collections.singleton(LockMode.X), manager.heldModes(TEST_RESOURCE));
    }

    @Test
    void upgradeConflict() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        
        // Tx1 acquires S lock
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, tx1));
        
        // Tx2 tries to upgrade S to X while tx1 holds S - should conflict
        assertFalse(manager.tryAcquire(TEST_RESOURCE, LockMode.X, tx2));
    }

    @Test
    void idempotentAcquire() throws InterruptedException {
        LockManager manager = new LockManager();
        
        // Acquire S lock multiple times - should be idempotent
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        assertTrue(manager.tryAcquire(TEST_RESOURCE, LockMode.S, TEST_TXID));
        
        // Should still hold only one S lock
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(TEST_RESOURCE));
    }

    @Test
    void multipleResources() throws InterruptedException {
        LockManager manager = new LockManager();
        final String resource1 = "table:USERS";
        final String resource2 = "table:PROFILES";
        
        // Acquire locks on different resources
        assertTrue(manager.tryAcquire(resource1, LockMode.S, TEST_TXID));
        assertTrue(manager.tryAcquire(resource2, LockMode.X, TEST_TXID));
        
        // Should hold locks on both resources
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(resource1));
        assertEquals(Collections.singleton(LockMode.X), manager.heldModes(resource2));
        
        // Release should clean up both
        manager.release(TEST_TXID);
        assertEquals(0, manager.resourceCount());
    }

    @Test
    void releaseTxid() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        
        // Tx1 acquires lock on multiple resources
        assertTrue(manager.tryAcquire("table:USERS", LockMode.S, tx1));
        assertTrue(manager.tryAcquire("table:PROFILES", LockMode.X, tx1));
        
        // Tx2 acquires a lock on another resource
        assertTrue(manager.tryAcquire("table:TRANSACTIONS", LockMode.S, tx2));
        
        // Release tx1 - should clean up its locks but not tx2's
        manager.release(tx1);
        
        assertEquals(1, manager.resourceCount()); // Only TRANSACTIONS left
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes("table:TRANSACTIONS"));
    }
}