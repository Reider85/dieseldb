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
 * Unit tests for LockManager release functionality.
 * <p>
 * Tests transaction-based release across multiple resources and
 * proper notification of waiting transactions.
 */
@Tag("concurrency")
class LockReleaseTest {

    @Test
    void releaseTxidFreesAllResources() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final String resource1 = "table:USERS";
        final String resource2 = "table:PROFILES";
        final String resource3 = "table:TRANSACTIONS";
        
        // Tx1 acquires locks on multiple resources
        assertTrue(manager.tryAcquire(resource1, LockMode.S, tx1));
        assertTrue(manager.tryAcquire(resource2, LockMode.X, tx1));
        assertTrue(manager.tryAcquire(resource3, LockMode.IS, tx1));
        
        // Should hold locks on all three resources
        assertEquals(3, manager.resourceCount());
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(resource1));
        assertEquals(Collections.singleton(LockMode.X), manager.heldModes(resource2));
        assertEquals(Collections.singleton(LockMode.IS), manager.heldModes(resource3));
        
        // Release tx1 - should clean up all resources
        manager.release(tx1);
        
        assertEquals(0, manager.resourceCount());
    }

    @Test
    @Timeout(5)
    void releaseNotifiesWaiters() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        final String resource = "table:USERS";
        
        // Tx1 acquires X lock
        assertTrue(manager.tryAcquire(resource, LockMode.X, tx1));
        
        // Tx2 tries to acquire S lock - should wait
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean tx2GotLock = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        
        Future<?> future = executor.submit(() -> {
            try {
                started.countDown();
                manager.acquire(resource, LockMode.S, tx2);
                tx2GotLock.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        // Wait for tx2 to start waiting
        started.await(1, TimeUnit.SECONDS);
        Thread.sleep(100); // Give time to enter wait state
        
        // Verify tx2 is waiting
        assertEquals(1, manager.waiterCount(resource));
        
        // Release tx1's lock - should notify tx2
        manager.release(tx1);
        
        // Wait for tx2 to acquire the lock
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Test failed: tx2 did not acquire lock", e);
        }
        
        assertTrue(tx2GotLock.get());
        assertEquals(1, manager.resourceCount());
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(resource));
        
        executor.shutdown();
    }

    @Test
    @Timeout(5)
    void releaseMultipleWaiters() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        final long tx3 = 3L;
        final String resource = "table:USERS";
        
        // Tx1 acquires X lock
        assertTrue(manager.tryAcquire(resource, LockMode.X, tx1));
        
        // Multiple transactions try to acquire S lock - all should wait
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicBoolean tx2GotLock = new AtomicBoolean(false);
        AtomicBoolean tx3GotLock = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(2);
        
        Future<?> future2 = executor.submit(() -> {
            try {
                started.countDown();
                manager.acquire(resource, LockMode.S, tx2);
                tx2GotLock.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        Future<?> future3 = executor.submit(() -> {
            try {
                started.countDown();
                manager.acquire(resource, LockMode.S, tx3);
                tx3GotLock.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        // Wait for both to start waiting
        started.await(1, TimeUnit.SECONDS);
        Thread.sleep(100);
        
        // Verify both are waiting
        assertEquals(2, manager.waiterCount(resource));
        
        // Release tx1 - should notify both waiters
        manager.release(tx1);
        
        // Wait for both to acquire locks
        try {
            future2.get(2, TimeUnit.SECONDS);
            future3.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Test failed: transactions did not acquire locks", e);
        }
        
        assertTrue(tx2GotLock.get());
        assertTrue(tx3GotLock.get());
        assertEquals(1, manager.resourceCount());
        assertEquals(Collections.singleton(LockMode.S), manager.heldModes(resource));
        
        executor.shutdown();
    }

    @Test
    void releaseNonexistentTxid() {
        LockManager manager = new LockManager();
        
        // Release nonexistent txid should not throw
        assertDoesNotThrow(() -> manager.release(99999L));
        assertEquals(0, manager.resourceCount());
    }

    @Test
    void releaseEmptyTxid() {
        LockManager manager = new LockManager();
        
        // Release txid with no locks should not throw
        assertDoesNotThrow(() -> manager.release(0L));
        assertEquals(0, manager.resourceCount());
    }

    @Test
    void releaseAfterWait() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        final String resource = "table:USERS";
        
        // Tx1 acquires S lock
        assertTrue(manager.tryAcquire(resource, LockMode.S, tx1));
        
        // Tx2 tries to acquire X lock - should wait
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean tx2GotLock = new AtomicBoolean(false);
        
        Future<?> future = executor.submit(() -> {
            try {
                manager.acquire(resource, LockMode.X, tx2);
                tx2GotLock.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        // Give tx2 time to start waiting
        Thread.sleep(100);
        assertEquals(1, manager.waiterCount(resource));
        
        // Tx1 releases - tx2 should get the lock
        manager.release(tx1);
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Test failed: tx2 did not acquire lock", e);
        }
        
        assertTrue(tx2GotLock.get());
        assertEquals(1, manager.resourceCount());
        
        executor.shutdown();
    }

    @Test
    void releaseMultipleResourcesForTxid() throws InterruptedException {
        LockManager manager = new LockManager();
        final long tx1 = 1L;
        final String[] resources = {"table:USERS", "table:PROFILES", "table:TRANSACTIONS"};
        
        // Tx1 acquires different lock types on different resources
        assertTrue(manager.tryAcquire(resources[0], LockMode.S, tx1));
        assertTrue(manager.tryAcquire(resources[1], LockMode.X, tx1));
        assertTrue(manager.tryAcquire(resources[2], LockMode.IS, tx1));
        
        // Should hold locks on all three resources
        assertEquals(3, manager.resourceCount());
        
        // Release tx1 - should clean up all resources
        manager.release(tx1);
        
        assertEquals(0, manager.resourceCount());
        
        // Verify no locks remain
        for (String resource : resources) {
            assertEquals(0, manager.waiterCount(resource));
        }
    }
}