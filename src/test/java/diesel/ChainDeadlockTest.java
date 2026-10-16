package diesel;

import diesel.concurrency.DeadlockDetector;
import diesel.concurrency.DeadlockVictimException;
import diesel.concurrency.LockManager;
import diesel.concurrency.LockMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for chain deadlock scenarios (multiple transactions in a cycle).
 * <p>
 * Tests 5-transaction deadlock cycle where one victim is killed and the
 * remaining transactions continue after the victim releases its locks.
 */
@Tag("concurrency")
class ChainDeadlockTest {

    private static final int CHAIN_SIZE = 5;
    private static final List<String> RESOURCES = List.of(
        "table:USERS", "table:PROFILES", "table:ORDERS", "table:PRODUCTS", "table:CUSTOMERS"
    );

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

    /**
     * Waits until all CHAIN_SIZE waiters are registered in the lock manager.
     */
    private static void awaitAllWaiting(LockManager manager) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            int total = 0;
            for (String r : RESOURCES) {
                total += manager.waiterCount(r);
            }
            if (total == CHAIN_SIZE) {
                return;
            }
            Thread.sleep(10);
        }
        fail("Expected " + CHAIN_SIZE + " waiters to be registered");
    }

    @Test
    @Timeout(20)
    void fiveTransactionChainDeadlockOneVictim() throws Exception {
        LockManager manager = new LockManager();
        List<Long> txids = List.of(1L, 2L, 3L, 4L, 5L);

        // Phase 1: Each tx acquires X lock on its own resource
        for (int i = 0; i < CHAIN_SIZE; i++) {
            assertTrue(manager.tryAcquire(RESOURCES.get(i), LockMode.X, txids.get(i)));
        }

        // Phase 2: Each tx waits for the next resource -> cycle 1->2->3->4->5->1
        ExecutorService executor = Executors.newFixedThreadPool(CHAIN_SIZE);
        List<AtomicReference<Exception>> exceptions = new ArrayList<>(CHAIN_SIZE);
        List<AtomicBoolean> completed = new ArrayList<>(CHAIN_SIZE);
        for (int i = 0; i < CHAIN_SIZE; i++) {
            exceptions.add(new AtomicReference<>());
            completed.add(new AtomicBoolean(false));
        }

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < CHAIN_SIZE; i++) {
            final int idx = i;
            long txid = txids.get(i);
            String nextResource = RESOURCES.get((i + 1) % CHAIN_SIZE);

            futures.add(executor.submit(() -> {
                try {
                    manager.acquire(nextResource, LockMode.X, txid);
                    completed.get(idx).set(true);
                } catch (DeadlockVictimException e) {
                    exceptions.get(idx).set(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exceptions.get(idx).set(e);
                } finally {
                    manager.release(txid);
                }
            }));
        }

        awaitAllWaiting(manager);

        // Phase 3: Detector resolves the cycle - victim = min txid = 1
        DeadlockDetector detector = new DeadlockDetector(manager);
        boolean deadlockFound = detector.detectAndResolveOnce();
        assertTrue(deadlockFound);
        assertEquals(1, detector.getDeadlocksDetected());

        // Phase 4: Verify victim (tx1) was killed
        DeadlockVictimException victimEx = awaitVictim(exceptions.get(0), 5000);
        assertEquals(1L, victimEx.getVictimTxid());
        assertEquals(6, victimEx.getCycle().size()); // 5-node cycle: 1->2->3->4->5->1
        assertTrue(victimEx.getCycle().containsAll(txids), "Cycle should contain all txids");

        // Phase 5+6: Victim released its locks in finally -> chain unwinds
        for (int i = 1; i < CHAIN_SIZE; i++) {
            futures.get(i).get(10, TimeUnit.SECONDS);
            assertTrue(completed.get(i).get(), "Transaction " + txids.get(i) + " should complete");
        }
        futures.get(0).get(5, TimeUnit.SECONDS);

        // Verify only the victim was killed
        for (int i = 1; i < CHAIN_SIZE; i++) {
            assertNull(exceptions.get(i).get(), "Transaction " + txids.get(i) + " should not be killed");
        }

        executor.shutdown();
    }

    @Test
    @Timeout(10)
    void chainWithoutCycleHasNoDeadlock() throws InterruptedException {
        LockManager manager = new LockManager();
        DeadlockDetector detector = new DeadlockDetector(manager);

        // Partial chain: 1->2->3->4->5 (no closing edge, no cycle)
        for (int i = 0; i < CHAIN_SIZE; i++) {
            assertTrue(manager.tryAcquire(RESOURCES.get(i), LockMode.X, (long) (i + 1)));
        }
        ExecutorService executor = Executors.newFixedThreadPool(CHAIN_SIZE - 1);
        try {
            // tx1..tx4 wait for the next resource; tx5 waits for nothing
            for (int i = 0; i < CHAIN_SIZE - 1; i++) {
                final long txid = i + 1;
                String next = RESOURCES.get(i + 1);
                executor.submit(() -> {
                    try {
                        manager.acquire(next, LockMode.X, txid);
                    } catch (DeadlockVictimException | InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        manager.release(txid);
                    }
                });
            }

            long deadline = System.currentTimeMillis() + 5000;
            while (manager.waiterCount(RESOURCES.get(1)) == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }

            // Chain is not a cycle -> no deadlock
            assertFalse(detector.detectAndResolveOnce());
            assertEquals(0, detector.getDeadlocksDetected());

            // Unblock the chain: release tx5's resource
            manager.release(5L);
        } finally {
            executor.shutdownNow();
            detector.close();
        }
    }

    @Test
    @Timeout(20)
    void victimSelectionIsMinimumTxid() throws Exception {
        LockManager manager = new LockManager();
        // Cycle txids in order: 10->5->8->2->15->10; minimum is 2
        List<Long> txids = List.of(10L, 5L, 8L, 2L, 15L);

        for (int i = 0; i < txids.size(); i++) {
            assertTrue(manager.tryAcquire(RESOURCES.get(i), LockMode.X, txids.get(i)));
        }

        ExecutorService executor = Executors.newFixedThreadPool(txids.size());
        List<AtomicReference<Exception>> exceptions = new ArrayList<>(txids.size());
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < txids.size(); i++) {
                exceptions.add(new AtomicReference<>());
                final int idx = i;
                long txid = txids.get(i);
                String next = RESOURCES.get((i + 1) % txids.size());
                futures.add(executor.submit(() -> {
                    try {
                        manager.acquire(next, LockMode.X, txid);
                    } catch (DeadlockVictimException e) {
                        exceptions.get(idx).set(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        exceptions.get(idx).set(e);
                    } finally {
                        manager.release(txid);
                    }
                }));
            }

            awaitAllWaiting(manager);

            DeadlockDetector detector = new DeadlockDetector(manager);
            try {
                assertTrue(detector.detectAndResolveOnce());
                assertEquals(1, detector.getDeadlocksDetected());

                // Victim must be the minimum txid in the cycle: 2
                DeadlockVictimException victimEx = awaitVictim(exceptions.get(3), 5000);
                assertEquals(2L, victimEx.getVictimTxid());
            } finally {
                detector.close();
            }

            // Everyone else completes after the chain unwinds
            for (int i = 0; i < txids.size(); i++) {
                futures.get(i).get(10, TimeUnit.SECONDS);
            }
            for (int i = 0; i < txids.size(); i++) {
                if (txids.get(i) != 2L) {
                    assertNull(exceptions.get(i).get(),
                            "Transaction " + txids.get(i) + " should not be killed");
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
