package diesel.concurrency;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Background deadlock detector for DieselDB's lock manager.
 * <p>
 * Periodically checks for deadlocks in the wait-for graph and aborts victim transactions.
 * Runs as a daemon thread with configurable check interval.
 */
public final class DeadlockDetector implements AutoCloseable {

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread thread = new Thread(r, "diesel-deadlock-detector");
        thread.setDaemon(true);
        return thread;
    };

    private final LockManager lockManager;
    private final long checkIntervalMs;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // Statistics
    private final AtomicLong deadlocksDetected = new AtomicLong(0);
    private final AtomicLong checksPerformed = new AtomicLong(0);
    
    // Graph for cycle detection
    private final WaitForGraph graph = new WaitForGraph();

    /**
     * Creates a deadlock detector with default check interval (500ms).
     * Reads interval from system property "deadlock.check.interval.ms" or uses default.
     * 
     * @param lockManager the lock manager to monitor
     */
    public DeadlockDetector(LockManager lockManager) {
        this(lockManager, Long.getLong("deadlock.check.interval.ms", 500L));
    }

    /**
     * Creates a deadlock detector with specified check interval.
     * 
     * @param lockManager the lock manager to monitor
     * @param checkIntervalMs deadlock check interval in milliseconds
     */
    public DeadlockDetector(LockManager lockManager, long checkIntervalMs) {
        this.lockManager = lockManager;
        this.checkIntervalMs = checkIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(DAEMON_THREAD_FACTORY);
    }

    /**
     * Starts the deadlock detector background thread.
     * Idempotent: safe to call multiple times.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleAtFixedRate(
                this::detectAndResolve,
                checkIntervalMs,
                checkIntervalMs,
                TimeUnit.MILLISECONDS
            );
        }
    }

    /**
     * Stops the deadlock detector background thread.
     * Idempotent: safe to call multiple times.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Performs a single deadlock detection and resolution pass.
     * Useful for testing and deterministic deadlock resolution.
     * 
     * @return true if a deadlock was found and resolved, false otherwise
     */
    public boolean detectAndResolveOnce() {
        return detectAndResolve();
    }

    /**
     * Main deadlock detection and resolution logic.
     * Runs periodically when the detector is active.
     */
    private boolean detectAndResolve() {
        checksPerformed.incrementAndGet();
        
        try {
            // Get current wait-for edges from lock manager
            Map<Long, Set<Long>> edges = lockManager.getWaitForEdges();
            if (edges == null) {
                System.err.println("Warning: getWaitForEdges() returned null");
                return false;
            }
            
            // Rebuild the graph with current edges
            graph.rebuild(edges);
            
            // Check for cycles
            List<Long> cycle = graph.findCycle();
            if (cycle != null && !cycle.isEmpty()) {
                // Found a deadlock - select and abort victim
                long victim = WaitForGraph.selectVictim(cycle);
                boolean killed = lockManager.abortWait(victim, cycle);
                if (killed) {
                    deadlocksDetected.incrementAndGet();
                }
                
                // Log warning
                System.err.println("Deadlock detected and resolved: cycle " + cycle + 
                                 ", victim txid=" + victim);
                
                return true;
            }
            
            return false;
        } catch (Exception e) {
            // Log error but continue running
            System.err.println("Error in deadlock detection: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Returns the number of deadlocks detected since startup.
     */
    public long getDeadlocksDetected() {
        return deadlocksDetected.get();
    }

    /**
     * Returns the number of detection passes performed since startup.
     */
    public long getChecksPerformed() {
        return checksPerformed.get();
    }

    /**
     * Returns the check interval in milliseconds.
     */
    public long getCheckIntervalMs() {
        return checkIntervalMs;
    }

    /**
     * Returns true if the detector is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Closes the deadlock detector (alias for stop()).
     */
    @Override
    public void close() {
        stop();
    }
}