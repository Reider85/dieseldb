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
 * Test for LockTimeoutManager configuration: lock.timeout.ms system property.
 * Verifies that the timeout period matches the configured value.
 * 
 * @tag concurrency
 */
@Tag("concurrency")
public class LockTimeoutConfigTest {

    private static final long TX1 = 1L;
    private static final long TX2 = 2L;
    private static final String RESOURCE = "table:USERS:row=100";
    private static final long CONFIGURED_TIMEOUT_MS = 5000L;

    private LockManager manager;
    private ExecutorService executor;
    private String originalTimeoutProperty;

    @BeforeEach
    void setUp() {
        // Save original property value
        originalTimeoutProperty = System.getProperty("lock.timeout.ms");
        
        // Set custom timeout via system property
        System.setProperty("lock.timeout.ms", String.valueOf(CONFIGURED_TIMEOUT_MS));
        
        // Create manager with default constructor (reads sysprop)
        manager = new LockManager();
        manager.getTimeoutManager().start();
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        manager.getTimeoutManager().close();
        
        // Restore original property value
        if (originalTimeoutProperty != null) {
            System.setProperty("lock.timeout.ms", originalTimeoutProperty);
        } else {
            System.clearProperty("lock.timeout.ms");
        }
    }

    @Test
    @Timeout(15) // Allow up to 15s for the 5s timeout test
    void configuredTimeoutUsed() throws InterruptedException {
        // Verify the manager is using the configured timeout
        assertEquals(CONFIGURED_TIMEOUT_MS, manager.getTimeoutManager().getTimeoutMs());

        // tx1 acquires X lock
        assertTrue(manager.tryAcquire(RESOURCE, LockMode.X, TX1));
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());

        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        AtomicBoolean tx2Started = new AtomicBoolean(false);

        // tx2 tries to acquire X lock (conflict) - should timeout after configured period
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

        // Measure elapsed time until timeout
        long startTime = System.currentTimeMillis();
        awaitThrow(exceptionRef, CONFIGURED_TIMEOUT_MS + 2000); // Allow 2s buffer
        long elapsedMs = System.currentTimeMillis() - startTime;

        // Verify exception is LockTimeoutException with correct timeout value
        assertNotNull(exceptionRef.get());
        assertTrue(exceptionRef.get() instanceof LockTimeoutException);
        LockTimeoutException e = (LockTimeoutException) exceptionRef.get();
        assertEquals(TX2, e.getTxid());
        assertEquals(RESOURCE, e.getResource());
        assertEquals(CONFIGURED_TIMEOUT_MS, e.getTimeoutMs());

        // Verify timeout count
        assertEquals(1, manager.getTimeoutManager().getTimeoutCount());

        // Verify elapsed time is within reasonable bounds (not exact due to scheduler precision)
        // Allow 10% tolerance + 100ms buffer for Windows scheduler noise
        long lowerBound = (long) (CONFIGURED_TIMEOUT_MS * 0.9);
        long upperBound = CONFIGURED_TIMEOUT_MS + 1000; // 1s buffer for test environment
        assertTrue(elapsedMs >= lowerBound, 
            "Timeout elapsed too early: " + elapsedMs + "ms < " + lowerBound + "ms");
        assertTrue(elapsedMs <= upperBound, 
            "Timeout took too long: " + elapsedMs + "ms > " + upperBound + "ms");

        // tx1 still holds lock
        assertEquals(1, manager.heldModes(RESOURCE).size());
        assertEquals(LockMode.X, manager.heldModes(RESOURCE).iterator().next());
    }

    @Test
    @Timeout(10)
    void defaultTimeoutWhenPropertyNotSet() throws InterruptedException {
        // Save original property
        String original = System.getProperty("lock.timeout.ms");
        System.clearProperty("lock.timeout.ms");

        try {
            // Create manager with default constructor (should use 30000ms default)
            LockManager defaultManager = new LockManager();
            defaultManager.getTimeoutManager().start();

            try {
                assertEquals(30000L, defaultManager.getTimeoutManager().getTimeoutMs());
            } finally {
                defaultManager.getTimeoutManager().close();
            }
        } finally {
            // Restore original property
            if (original != null) {
                System.setProperty("lock.timeout.ms", original);
            }
        }
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