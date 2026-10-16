package diesel;

import diesel.concurrency.DeadlockDetector;
import diesel.concurrency.DeadlockVictimException;
import diesel.concurrency.LockManager;
import diesel.concurrency.LockMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for deadlock detection and resolution.
 * <p>
 * Tests 2-transaction deadlock cycle detection, victim selection, and integration
 * with the background deadlock detector.
 */
@Tag("concurrency")
class DeadlockTest {

    private static final long TX1 = 1L;
    private static final long TX2 = 2L;
    private static final String RESOURCE1 = "table:USERS";
    private static final String RESOURCE2 = "table:PROFILES";

    /**
     * Waits until the given exception reference is populated (victim was killed).
     */
    private static DeadlockVictimException awaitVictim(AtomicReference<Exception> ref, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (ref.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        Exception e = ref.get();
        assertNotNull(e, "Victim transaction should have been killed within " + timeoutMs + "ms");
        assertTrue(e instanceof DeadlockVictimException, "Expected DeadlockVictimException but got: " + e);
        return (DeadlockVictimException) e;
    }

    @Test
    @Timeout(15)
    void twoTransactionDeadlockDetectorKillsMinTxid() throws Exception {
        LockManager manager = new LockManager();
        DeadlockDetector detector = new DeadlockDetector(manager);

        // Setup: tx1 holds r1, tx2 holds r2
        assertTrue(manager.tryAcquire(RESOURCE1, LockMode.X, TX1));
        assertTrue(manager.tryAcquire(RESOURCE2, LockMode.X, TX2));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicReference<Exception> tx1Exception = new AtomicReference<>();
        AtomicReference<Exception> tx2Exception = new AtomicReference<>();
        Future<?> tx1Future;
        Future<?> tx2Future;

        try {
            // Both tx block: tx1 waits for r2, tx2 waits for r1 -> cycle 1->2->1
            tx1Future = executor.submit(() -> {
                try {
                    manager.acquire(RESOURCE2, LockMode.X, TX1);
                } catch (DeadlockVictimException e) {
                    tx1Exception.set(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tx1Exception.set(e);
                } finally {
                    manager.release(TX1);
                }
            });
            tx2Future = executor.submit(() -> {
                try {
                    manager.acquire(RESOURCE1, LockMode.X, TX2);
                } catch (DeadlockVictimException e) {
                    tx2Exception.set(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tx2Exception.set(e);
                } finally {
                    manager.release(TX2);
                }
            });

            // Wait until both waiters are registered in the lock manager
            long deadline = System.currentTimeMillis() + 5000;
            while ((manager.waiterCount(RESOURCE1) == 0 || manager.waiterCount(RESOURCE2) == 0)
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, manager.waiterCount(RESOURCE1));
            assertEquals(1, manager.waiterCount(RESOURCE2));

            // One detection pass resolves the cycle
            assertTrue(detector.detectAndResolveOnce());
            assertEquals(1, detector.getDeadlocksDetected());

            // Victim = min txid = TX1
            DeadlockVictimException victimEx = awaitVictim(tx1Exception, 5000);
            assertEquals(TX1, victimEx.getVictimTxid());
            assertTrue(victimEx.getCycle().contains(TX1) && victimEx.getCycle().contains(TX2),
                    "Cycle should contain both txids: " + victimEx.getCycle());
            assertTrue(victimEx.getMessage().contains("deadlock"));

            // TX1 released its locks in finally -> TX2 acquires and completes
            tx2Future.get(5, TimeUnit.SECONDS);
            assertNull(tx2Exception.get(), "TX2 (non-victim) should not be killed");
            tx1Future.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            detector.close();
        }
    }

    @Test
    @Timeout(15)
    void twoTransactionDeadlockWithBackgroundDetector() throws InterruptedException {
        LockManager manager = new LockManager();
        DeadlockDetector detector = new DeadlockDetector(manager, 50); // Fast check for test

        try {
            detector.start();

            // Setup: tx1 holds r1, tx2 holds r2
            assertTrue(manager.tryAcquire(RESOURCE1, LockMode.X, TX1));
            assertTrue(manager.tryAcquire(RESOURCE2, LockMode.X, TX2));

            ExecutorService executor = Executors.newFixedThreadPool(2);
            AtomicReference<Exception> tx1Exception = new AtomicReference<>();
            AtomicReference<Exception> tx2Exception = new AtomicReference<>();

            // Cycle: tx1 waits for r2, tx2 waits for r1
            executor.submit(() -> {
                try {
                    manager.acquire(RESOURCE2, LockMode.X, TX1);
                } catch (DeadlockVictimException e) {
                    tx1Exception.set(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tx1Exception.set(e);
                } finally {
                    manager.release(TX1);
                }
            });
            executor.submit(() -> {
                try {
                    manager.acquire(RESOURCE1, LockMode.X, TX2);
                } catch (DeadlockVictimException e) {
                    tx2Exception.set(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tx2Exception.set(e);
                } finally {
                    manager.release(TX2);
                }
            });

            // Background detector resolves the deadlock on its own
            DeadlockVictimException victimEx = awaitVictim(tx1Exception, 5000);
            assertEquals(TX1, victimEx.getVictimTxid());

            // Verify detector statistics
            assertEquals(1, detector.getDeadlocksDetected());
            assertTrue(detector.getChecksPerformed() >= 1);

            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            assertNull(tx2Exception.get(), "TX2 (non-victim) should not be killed");
        } finally {
            detector.stop();
        }
    }

    @Test
    @Timeout(5)
    void noDeadlockWithConcurrentLocks() throws InterruptedException {
        LockManager manager = new LockManager();

        // Both tx acquire different resources - no conflict
        assertTrue(manager.tryAcquire(RESOURCE1, LockMode.S, TX1));
        assertTrue(manager.tryAcquire(RESOURCE2, LockMode.S, TX2));

        // Both can acquire each other's resources in S mode (compatible)
        assertTrue(manager.tryAcquire(RESOURCE2, LockMode.S, TX1));
        assertTrue(manager.tryAcquire(RESOURCE1, LockMode.S, TX2));

        // No deadlock should occur
        assertEquals(0, manager.waiterCount(RESOURCE1));
        assertEquals(0, manager.waiterCount(RESOURCE2));
    }

    @Test
    @Timeout(5)
    void waitForEdgesReflectBlockedWaiters() throws Exception {
        LockManager manager = new LockManager();

        // tx1 holds r1 X, tx2 holds r2 X
        assertTrue(manager.tryAcquire(RESOURCE1, LockMode.X, TX1));
        assertTrue(manager.tryAcquire(RESOURCE2, LockMode.X, TX2));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            try {
                manager.acquire(RESOURCE1, LockMode.X, TX2);
            } catch (DeadlockVictimException e) {
                // expected if a detector kills us - not started here
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                manager.release(TX2);
            }
        });

        // Wait until tx2 is registered as a waiter on r1
        long deadline = System.currentTimeMillis() + 5000;
        while (manager.waiterCount(RESOURCE1) == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        // Wait-for edge: tx2 -> tx1 (tx2 waits for a lock held by tx1)
        var edges = manager.getWaitForEdges();
        assertTrue(edges.containsKey(TX2), "tx2 should be waiting: " + edges);
        assertTrue(edges.get(TX2).contains(TX1), "tx2 waits for tx1: " + edges);

        // Release r1 so tx2 can finish
        manager.release(TX1);
        future.get(5, TimeUnit.SECONDS);
        executor.shutdown();
    }
}
