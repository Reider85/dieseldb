package diesel;

import diesel.concurrency.LockManager;
import diesel.concurrency.LockMode;
import diesel.concurrency.LockTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for LockTimeoutManager: transaction waiting for lock exceeds timeout period.
 * 
 * @tag concurrency
 */
@Tag("concurrency")
public class LockTimeoutTest {

    private static final long TX1 = 1L;
    private static final long TX2 = 2L;
    private static final String RESOURCE = "table:USERS:row=100";

    private LockManager manager;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        // Use short timeout (300ms) for fast test suite
        manager = new LockManager(300);
        manager.getTimeoutManager().start();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        manager.getTimeoutManager().close();
    }

    @Test
    @Timeout(5)
    void tx1HoldsX_tx2TimesOut() throws InterruptedException {
        // tx1 acquires X lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.X, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to acquire X lock (conflict) - should timeout
        executor.submit(() -> {
            tx2Started.set(true);
            try {
                manager.acquire(RESOURCE, LockMode.X, TX2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        // Wait for tx2 to start waiting
        while (!tx2Started.get()) {
            Thread.sleep(10);
        }

        // Wait for tx2 to timeout (should happen within 500ms)
        awaitThrow(exceptionRef, 1000);

        // Verify exception is LockTimeoutException with correct fields
        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get() instanceof LockTimeoutException);
        LockTimeoutException e = (LockTimeoutException) exceptionRef.get();
        assertEquals(TX2, e.getTxid());
        assertEquals(RESOURCE, e.getResource());
        assertEquals(300, e.getTimeoutMs());

        // Verify timeout count
        assertEquals(1, manager.getTimeoutManager().getTimeoutCount());

        // tx1 still holds lock
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());
    }

    @Test
    @Timeout(5)
    void tx1HoldsS_tx2TimesOutForX() throws InterruptedException {
        // tx1 acquires S lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.S, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.S, manager.heldModes(RESOURCE).iterator().next());

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to upgrade to X lock (conflict) - should timeout
        executor.submit(() -> {
            tx2Started.set(true);
            try {
                manager.acquire(RESOURCE, LockMode.X, TX2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        // Wait for tx2 to start waiting
        while (!tx2Started.get()) {
            Thread.sleep(10);
        }

        // Wait for tx2 to timeout
        awaitThrow(exceptionRef, 1000);

        // Verify exception is LockTimeoutException
        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get() instanceof LockTimeoutException);
        LockTimeoutException e = (LockTimeoutException) exceptionRef.get();
        assertEquals(TX2, e.getTxid());
        assertEquals(RESOURCE, e.getResource());
        assertEquals(300, e.getTimeoutMs());

        // Verify timeout count
        assertEquals(1, manager.getTimeoutManager().getTimeoutCount());

        // tx1 still holds lock
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.S, manager.heldModes(RESOURCE).iterator().next());
    }

    /**
     * Helper to wait for an exception to be thrown in the AtomicReference.
     * Polls periodically to avoid flaky tests due to thread scheduling.
     */
    private void awaitThrow(AtomicReference<Exception> exceptionRef, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (exceptionRef.get() != null) {
                return;
            }
            Thread.sleep(10);
        }
        // If we get here, no exception was thrown within timeout
        fail("Expected LockTimeoutException was not thrown within " + timeoutMs + "ms");
    }
}