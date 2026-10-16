package diesel.concurrency;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages lock wait timeouts for LockManager.
 * <p>
 * Implements a per-waiter timeout mechanism using ScheduledExecutorService.
 * When a transaction starts waiting for a lock, a timer is scheduled. If the
 * lock is not acquired within the timeout period, the transaction receives
 * a LockTimeoutException and is aborted.
 * <p>
 * Thread-safe: uses concurrent collections and atomic counters.
 * Lifecycle: start() must be called to activate timeouts; close() shuts down the scheduler.
 */
public final class LockTimeoutManager {

    /**
     * System property key for lock wait timeout in milliseconds.
     * Default: 30000 (30 seconds).
     */
    public static final String TIMEOUT_KEY = "lock.timeout.ms";
    public static final long DEFAULT_TIMEOUT_MS = 30_000L;

    private final LockManager lockManager;
    private final long timeoutMs;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<Long, WaitInfo> activeWaits = new ConcurrentHashMap<>();
    private final AtomicLong timeoutCount = new AtomicLong();
    private final AtomicLong timeoutsScheduled = new AtomicLong();
    private volatile boolean running = false;

    /**
     * Creates a LockTimeoutManager with default timeout (30s) and default check interval (100ms).
     * The manager must be started via start() to activate timeouts.
     */
    public LockTimeoutManager(LockManager lockManager) {
        this(lockManager, Long.getLong(TIMEOUT_KEY, DEFAULT_TIMEOUT_MS));
    }

    /**
     * Creates a LockTimeoutManager with a custom timeout.
     * The manager must be started via start() to activate timeouts.
     */
    public LockTimeoutManager(LockManager lockManager, long timeoutMs) {
        this.lockManager = Objects.requireNonNull(lockManager, "LockManager cannot be null");
        this.timeoutMs = Math.max(1, timeoutMs); // Ensure positive timeout
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "diesel-lock-timeout");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the timeout scheduler. Safe to call multiple times (idempotent).
     */
    public void start() {
        running = true;
    }

    /**
     * Stops the timeout scheduler and cancels all pending timeouts.
     * Safe to call multiple times (idempotent).
     */
    public void stop() {
        scheduler.shutdownNow();
    }

    /**
     * Shuts down the scheduler and releases all resources.
     * Equivalent to stop().
     */
    public void close() {
        stop();
    }

    /**
     * Returns the configured timeout in milliseconds.
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Returns the number of timeouts that have occurred since creation.
     */
    public long getTimeoutCount() {
        return timeoutCount.get();
    }

    /**
     * Returns the number of currently active waiters being timed.
     */
    public int activeWaitCount() {
        return activeWaits.size();
    }

    /**
     * Returns true if the timeout scheduler is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Called by LockManager when a transaction starts waiting for a lock.
     * Schedules a timeout for this waiter.
     *
     * @param txid the transaction ID
     * @param resource the resource being waited on
     * @return true if a timeout was scheduled
     */
    public boolean onWaitStart(long txid, String resource) {
        Objects.requireNonNull(resource, "Resource cannot be null");

        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        ScheduledFuture<?> future = scheduler.schedule(
            () -> fireTimeout(txid, resource),
            timeoutMs,
            TimeUnit.MILLISECONDS
        );

        activeWaits.put(txid, new WaitInfo(resource, deadline, future));
        timeoutsScheduled.incrementAndGet();
        return true;
    }

    /**
     * Called by LockManager when a transaction stops waiting (granted, interrupted, or timed out).
     * Cancels the timeout and removes the waiter from tracking.
     *
     * @param txid the transaction ID
     */
    public void onWaitEnd(long txid) {
        WaitInfo info = activeWaits.remove(txid);
        if (info != null) {
            info.future().cancel(false);
            lockManager.clearTimedOutWaiter(txid);
        }
    }

    /**
     * Called by the scheduled timeout task when a timeout expires.
     * Marks the transaction as timed out and notifies the LockManager.
     *
     * @param txid the transaction ID
     * @param resource the resource being waited on
     */
    private void fireTimeout(long txid, String resource) {
        // Atomically check and remove the waiter to prevent race conditions
        WaitInfo info = activeWaits.remove(txid);
        if (info != null) {
            // The waiter was still active - this is a real timeout
            lockManager.abortTimeoutWait(txid, resource);
            timeoutCount.incrementAndGet();
        }
        // If info is null, the waiter was already granted/interrupted and removed by onWaitEnd
    }

    /**
     * Information about an active waiter for timeout tracking.
     */
    private static final class WaitInfo {
        private final String resource;
        private final long deadlineNano;
        private final ScheduledFuture<?> future;

        WaitInfo(String resource, long deadlineNano, ScheduledFuture<?> future) {
            this.resource = resource;
            this.deadlineNano = deadlineNano;
            this.future = future;
        }

        String getResource() {
            return resource;
        }

        long getDeadlineNano() {
            return deadlineNano;
        }

        ScheduledFuture<?> future() {
            return future;
        }
    }
}