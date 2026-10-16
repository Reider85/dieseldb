package diesel;

import diesel.concurrency.LockManager;
import diesel.concurrency.LockMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for LockTimeoutManager: timer cancellation when lock is granted before timeout.
 * 
 * @tag concurrency
 */
@Tag("concurrency")
public class CancelTimerTest {

    private static final long TX1 = 1L;
    private static final long TX2 = 2L;
    private static final String RESOURCE = "table:USERS:row=100";

    private LockManager manager;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        // Use longer timeout (5000ms) to allow cancellation
        manager = new LockManager(5000);
        manager.getTimeoutManager().start();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        manager.getTimeoutManager().close();
    }

    @Test
    @Timeout(10)
    void tx1HoldsX_tx2Waits_tx1Releases_tx2GetsLock() throws InterruptedException {
        // tx1 acquires X lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.X, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        AtomicBoolean tx2Success = new AtomicBoolean(false);
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to acquire X lock (conflict) - should wait
        executor.submit(() -> {
            tx2Started.set(true);
            try {
                manager.acquire(RESOURCE, LockMode.X, TX2);
                tx2Success.set(true); // If we get here, lock was granted
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Wait for tx2 to start waiting
        while (!tx2Started.get()) {
            Thread.sleep(10);
        }

        // Verify tx2 is waiting
        assertEquals(1, manager.waiterCount(RESOURCE));
        assertEquals(1, manager.getTimeoutManager().activeWaitCount());

        // tx1 releases lock
        manager.release(TX1);

        // Wait for tx2 to get lock (should happen quickly after release)
        awaitSuccess(tx2Success, 2000);

        // Verify tx2 got the lock
        assertTrue(tx2Success.get());
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        // Verify no timeout occurred
        assertEquals(0, manager.getTimeoutManager().getTimeoutCount());
        assertEquals(0, manager.getTimeoutManager().activeWaitCount());

        // tx2 should release the lock
        manager.release(TX2);
    }

    @Test
    @Timeout(10)
    void tx1HoldsS_tx2WaitsForX_tx1Releases_tx2GetsLock() throws InterruptedException {
        // tx1 acquires S lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.S, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.S, manager.heldModes(RESOURCE).iterator().next());

        AtomicBoolean tx2Success = new AtomicBoolean(false);
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to acquire X lock (conflict) - should wait
        executor.submit(() -> {
            tx2Started.set(true);
            try {
                manager.acquire(RESOURCE, LockMode.X, TX2);
                tx2Success.set(true); // If we get here, lock was granted
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Wait for tx2 to start waiting
        while (!tx2Started.get()) {
            Thread.sleep(10);
        }

        // Verify tx2 is waiting
        assertEquals(1, manager.waiterCount(RESOURCE));
        assertEquals(1, manager.getTimeoutManager().activeWaitCount());

        // tx1 releases lock
        manager.release(TX1);

        // Wait for tx2 to get lock
        awaitSuccess(tx2Success, 2000);

        // Verify tx2 got the lock
        assertTrue(tx2Success.get());
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        // Verify no timeout occurred
        assertEquals(0, manager.getTimeoutManager().getTimeoutCount());
        assertEquals(0, manager.getTimeoutManager().activeWaitCount());

        // tx2 should release the lock
        manager.release(TX2);
    }

    @Test
    @Timeout(10)
    void tx1HoldsX_tx2Waits_tx1Interrupts_tx2GetsLock() throws InterruptedException {
        // tx1 acquires X lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.X, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        Thread tx2Thread = null;
        AtomicBoolean tx2Success = new AtomicBoolean(false);
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to acquire X lock (conflict) - should wait
        tx2Thread = new Thread(() -> {
            tx2Started.set(true);
            try {
                manager.acquire(RESOURCE, LockMode.X, TX2);
                tx2Success.set(true); // If we get here, lock was granted
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        tx2Thread.start();

        // Wait for tx2 to start waiting
        while (!tx2Started.get()) {
            Thread.sleep(10);
        }

        // Verify tx2 is waiting
        assertEquals(1, manager.waiterCount(RESOURCE));
        assertEquals(1, manager.getTimeoutManager().activeWaitCount());

        // tx1 releases lock
        manager.release(TX1);

        // tx2 should get lock (no interruption needed, but test that it works)
        awaitSuccess(tx2Success, 2000);

        // Verify tx2 got the lock
        assertTrue(tx2Success.get());
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        // Verify no timeout occurred
        assertEquals(0, manager.getTimeoutManager().getTimeoutCount());
        assertEquals(0, manager.getTimeoutManager().activeWaitCount());

        // tx2 should release the lock
        manager.release(TX2);

        tx2Thread.join();
    }

    /**
     * Helper to wait for tx2 to succeed in getting the lock.
     * Polls periodically to avoid flaky tests due to thread scheduling.
     */
    private void awaitSuccess(AtomicBoolean successFlag, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (successFlag.get()) {
                return;
            }
            Thread.sleep(10);
        }
        // If we get here, tx2 did not get the lock within timeout
        fail("tx2 did not get the lock within " + timeoutMs + "ms");
    }
}