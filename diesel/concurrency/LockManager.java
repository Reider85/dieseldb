package diesel.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
     * Lock manager for DieselDB's granular locking system.
     * <p>
     * Manages locks on resources (e.g., "table:USERS", "row:USERS#42") with support for
     * multiple lock modes: IS, IX, S, X, SIX. Provides blocking and non-blocking acquisition,
     * transaction-based release, and compatibility checking.
     * <p>
     * <b>Note:</b> This is a standalone subsystem. Deadlock detection (step 2/4),
     * lock timeouts (step 3/4), and integration with the DML engine (step 4/4)
     * are implemented in R3-006.
     */
public final class LockManager {

    // State for each resource: holders + waiters
    private final ConcurrentMap<String, ResourceState> resourceStates = new ConcurrentHashMap<>();

    // Global index: txid -> set of resources it holds locks on
    private final ConcurrentMap<Long, Set<String>> txResources = new ConcurrentHashMap<>();

    // Index: txid -> ResourceState where it's currently waiting (for abortWait)
    private final ConcurrentMap<Long, ResourceState> waitingTx = new ConcurrentHashMap<>();

    // Killed waiters: txid -> cycle path (for exception message)
    private final ConcurrentMap<Long, List<Long>> killedWaiters = new ConcurrentHashMap<>();

    // Timeout waiters: txid -> resource (for LockTimeoutException)
    private final ConcurrentMap<Long, String> timedOutWaiters = new ConcurrentHashMap<>();

    // Lock timeout manager
    private final LockTimeoutManager timeoutManager;

        /**
     * Creates a LockManager with default timeout (30 seconds).
     */
    public LockManager() {
        this.timeoutManager = new LockTimeoutManager(this);
    }

    /**
     * Creates a LockManager with a custom timeout.
     *
     * @param lockTimeoutMs the lock timeout in milliseconds
     */
    public LockManager(long lockTimeoutMs) {
        this.timeoutManager = new LockTimeoutManager(this, lockTimeoutMs);
    }

    /**
     * Returns the LockTimeoutManager for this LockManager.
     * Must be started via start() to activate timeouts.
     */
    public LockTimeoutManager getTimeoutManager() {
        return timeoutManager;
    }

    /**
     * Attempts to acquire a lock on a resource. Blocks until the lock can be granted
     * or the thread is interrupted.
     *
     * @param resource the resource to lock (e.g., "table:USERS", "row:USERS#42")
     * @param mode the lock mode to acquire
     * @param txid the transaction ID requesting the lock
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void acquire(String resource, LockMode mode, long txid) throws InterruptedException {
        Objects.requireNonNull(resource, "Resource cannot be null");
        Objects.requireNonNull(mode, "Lock mode cannot be null");

        while (true) {
            ResourceState state = resourceStates.computeIfAbsent(resource, k -> new ResourceState());
            boolean waited = false;

            synchronized (state) {
                // Check if this state is still mapped (not orphaned)
                if (resourceStates.get(resource) != state) {
                    continue; // State was removed - retry with fresh state
                }
                while (true) {
                // Check if this tx was killed by deadlock detector
                if (killedWaiters.containsKey(txid)) {
                    // Clean up waiting state and throw exception
                    removeWaiterForTx(txid, state);
                    List<Long> cycle = killedWaiters.remove(txid);
                    throw new DeadlockVictimException(txid, cycle);
                }

                // Check if we already hold a compatible lock (or can upgrade)
                Lock existingLock = state.getLockForTx(txid);
                if (existingLock != null) {
                    Lock merged = existingLock.merge(new Lock(resource, mode, txid));
                    if (merged.getMode() == mode) {
                        // Already hold the requested mode (idempotent)
                        return;
                    }
                    // Check if upgrade is possible
                    if (canUpgrade(existingLock.getMode(), mode, state.holders, txid)) {
                        state.replaceLock(txid, merged);
                        return;
                    }
                    // Need to wait for upgrade
                } else if (LockCompatibility.isCompatibleWithAll(mode, state.getHeldModes())) {
                    // Grant the lock immediately
                    state.addLock(new Lock(resource, mode, txid));
                    txResources.computeIfAbsent(txid, k -> ConcurrentHashMap.newKeySet()).add(resource);
                    // Clean up timeout manager state if we were waiting
                    if (waited) {
                        timeoutManager.onWaitEnd(txid);
                    }
                    return;
                }

                // Check if this tx timed out
                String timedOutResource = timedOutWaiters.remove(txid);
                if (timedOutResource != null) {
                    // Clean up waiter already done by loop
                    throw new LockTimeoutException(txid, timedOutResource, timeoutManager.getTimeoutMs());
                }

                // Need to wait: register with timeout manager if first wait
                if (!waited) {
                    timeoutManager.onWaitStart(txid, resource);
                    waited = true;
                }

                // Add to waiters and wait for notification
                Waiter waiter = new Waiter(txid, mode, Thread.currentThread());
                state.addWaiter(waiter);
                waitingTx.put(txid, state); // Track where this tx is waiting
                try {
                    state.wait();
                } catch (InterruptedException e) {
                    state.removeWaiter(waiter);
                    waitingTx.remove(txid);
                    throw e;
                }
                // Woken up: leave the wait queue before re-checking,
                // otherwise a granted waiter stays in the graph as a phantom edge
                state.removeWaiter(waiter);
                waitingTx.remove(txid);
                }
            }
        }
    }

    /**
     * Attempts to acquire a lock without blocking.
     *
     * @param resource the resource to lock
     * @param mode the lock mode to acquire
     * @param txid the transaction ID requesting the lock
     * @return true if the lock was granted, false if there would be a conflict
     */
    public boolean tryAcquire(String resource, LockMode mode, long txid) {
        Objects.requireNonNull(resource, "Resource cannot be null");
        Objects.requireNonNull(mode, "Lock mode cannot be null");

        while (true) {
            ResourceState state = resourceStates.computeIfAbsent(resource, k -> new ResourceState());

            synchronized (state) {
                // Check if this state is still mapped (not orphaned)
                if (resourceStates.get(resource) != state) {
                    continue; // State was removed - retry with fresh state
                }
            // Check if we already hold a compatible lock
            Lock existingLock = state.getLockForTx(txid);
            if (existingLock != null) {
                Lock merged = existingLock.merge(new Lock(resource, mode, txid));
                
                // Check if upgrade is possible
                if (canUpgrade(existingLock.getMode(), mode, state.holders, txid)) {
                    state.replaceLock(txid, merged);
                    return true;
                }
                return false;
            }

            // Check compatibility with other holders
            if (LockCompatibility.isCompatibleWithAll(mode, state.getHeldModes())) {
                state.addLock(new Lock(resource, mode, txid));
                txResources.computeIfAbsent(txid, k -> ConcurrentHashMap.newKeySet()).add(resource);
                    return true;
                }

                return false;
            }
        }
    }

    /**
     * Releases all locks held by a transaction across all resources.
     *
     * @param txid the transaction ID whose locks should be released
     */
    public void release(long txid) {
        // Clean up waiting state
        waitingTx.remove(txid);
        killedWaiters.remove(txid);
        timedOutWaiters.remove(txid);
        timeoutManager.onWaitEnd(txid);
        
        Set<String> resources = txResources.get(txid);
        if (resources != null) {
            for (String resource : resources) {
                ResourceState state = resourceStates.get(resource);
                if (state != null) {
                    synchronized (state) {
                        state.removeLocksForTx(txid);
                        if (state.isEmpty()) {
                            resourceStates.remove(resource, state);
                        }
                        state.notifyAll(); // Wake up all waiters
                    }
                }
            }
            txResources.remove(txid);
        }
    }

    /**
     * Checks if a lock upgrade is possible.
     * An upgrade is allowed only if the transaction can acquire the stronger mode
     * without conflicting with other holders.
     */
    private boolean canUpgrade(LockMode current, LockMode requested, Iterable<Lock> otherHolders, long txid) {
        // For upgrade, we need to check compatibility with OTHER holders only
        // (the transaction can always upgrade its own lock)
        for (Lock other : otherHolders) {
            if (other.getTxid() != txid) {
                if (!LockCompatibility.isCompatible(other.getMode(), requested)) {
                    return false;
                }
            }
        }
        return true;
    }

    // Package-private test hooks
    /**
     * Returns the lock modes currently held on a resource.
     */
    public Set<LockMode> heldModes(String resource) {
        ResourceState state = resourceStates.get(resource);
        if (state == null) {
            return Collections.emptySet();
        }
        synchronized (state) {
            Set<LockMode> modes = new HashSet<>();
            for (Lock lock : state.holders) {
                modes.add(lock.getMode());
            }
            return modes;
        }
    }

    /**
     * Returns the number of waiters for a resource.
     */
    public int waiterCount(String resource) {
        ResourceState state = resourceStates.get(resource);
        if (state == null) {
            return 0;
        }
        synchronized (state) {
            return state.waiters.size();
        }
    }

    /**
     * Returns the number of resources currently being tracked.
     */
    public int resourceCount() {
        return resourceStates.size();
    }

    /**
     * Returns a snapshot of the current wait-for edges.
     * Each edge waiter→holder represents a transaction waiting for a lock held by another transaction.
     * 
     * @return map where keys are waiting txids and values are sets of txids holding locks they wait for
     */
    public Map<Long, Set<Long>> getWaitForEdges() {
        Map<Long, Set<Long>> edges = new ConcurrentHashMap<>();
        
        for (Map.Entry<String, ResourceState> entry : resourceStates.entrySet()) {
            ResourceState state = entry.getValue();
            if (state == null) continue;
            
            synchronized (state) {
                for (Waiter waiter : state.waiters) {
                    if (waiter == null) continue;
                    
                    // Find all holders that conflict with this waiter
                    for (Lock holder : state.holders) {
                        if (holder == null) continue;
                        
                        if (holder.getTxid() != waiter.txid && 
                            !LockCompatibility.isCompatible(holder.getMode(), waiter.mode)) {
                            // Waiter is blocked by holder
                            edges.computeIfAbsent(waiter.txid, k -> ConcurrentHashMap.newKeySet())
                                 .add(holder.getTxid());
                        }
                    }
                }
            }
        }
        return edges;
    }

    /**
     * Aborts a transaction that is waiting due to deadlock.
     * Marks the transaction's waiters as killed and notifies them to throw DeadlockVictimException.
     * 
     * @param txid the transaction ID to abort
     * @param cycle the wait-for cycle that caused the deadlock (for exception message)
     * @return true if the transaction was marked as killed for the first time, false if already killed or not waiting
     */
    public boolean abortWait(long txid, List<Long> cycle) {
        ResourceState state = waitingTx.get(txid);
        if (state != null) {
            synchronized (state) {
                // Mark the waiter as killed only if not already killed (fresh kill)
                if (killedWaiters.putIfAbsent(txid, cycle) == null) {
                    state.notifyAll(); // Wake up the waiter to check killed flag
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Aborts a transaction that is waiting due to timeout.
     * Marks the transaction's waiter as timed out and notifies them to throw LockTimeoutException.
     * Called by LockTimeoutManager when a timeout expires.
     * 
     * @param txid the transaction ID to abort
     * @param resource the resource that timed out
     */
    void abortTimeoutWait(long txid, String resource) {
        ResourceState state = waitingTx.get(txid);
        if (state != null) {
            synchronized (state) {
                // Mark the waiter as timed out
                timedOutWaiters.put(txid, resource);
                state.notifyAll(); // Wake up the waiter to check timeout flag
            }
        }
    }

    /**
     * Clears a timed out waiter entry.
     * Called by LockTimeoutManager when a waiter finishes (granted or interrupted).
     * 
     * @param txid the transaction ID
     */
    void clearTimedOutWaiter(long txid) {
        timedOutWaiters.remove(txid);
    }

    /**
     * Helper to remove a waiter for a transaction from a resource state.
     * Called when the waiter is killed or when the transaction completes.
     */
    private void removeWaiterForTx(long txid, ResourceState state) {
        synchronized (state) {
            state.waiters.removeIf(waiter -> waiter.txid == txid);
        }
        waitingTx.remove(txid);
    }

    /**
     * State for a single resource: lock holders and waiters.
     */
    private static class ResourceState {
        final List<Lock> holders = new ArrayList<>();
        final Deque<Waiter> waiters = new LinkedBlockingDeque<>();

        synchronized void addLock(Lock lock) {
            holders.add(lock);
        }

        synchronized void replaceLock(long txid, Lock newLock) {
            for (int i = 0; i < holders.size(); i++) {
                if (holders.get(i).getTxid() == txid) {
                    holders.set(i, newLock);
                    return;
                }
            }
            throw new IllegalStateException("Lock not found for txid: " + txid);
        }

        synchronized void removeLocksForTx(long txid) {
            holders.removeIf(lock -> lock.getTxid() == txid);
        }

        synchronized Lock getLockForTx(long txid) {
            for (Lock lock : holders) {
                if (lock.getTxid() == txid) {
                    return lock;
                }
            }
            return null;
        }

        synchronized Iterable<LockMode> getHeldModes() {
            List<LockMode> modes = new ArrayList<>();
            for (Lock lock : holders) {
                modes.add(lock.getMode());
            }
            return modes;
        }

synchronized void addWaiter(Waiter waiter) {
        Objects.requireNonNull(waiter, "Waiter cannot be null");
        waiters.addLast(waiter);
    }

        synchronized void removeWaiter(Waiter waiter) {
            Objects.requireNonNull(waiter, "Waiter cannot be null");
            waiters.remove(waiter);
        }

        synchronized boolean isEmpty() {
            return holders.isEmpty() && waiters.isEmpty();
        }
    }

    /**
     * A transaction waiting for a lock.
     */
    private static class Waiter {
        final long txid;
        final LockMode mode;
        final Thread thread;

        Waiter(long txid, LockMode mode, Thread thread) {
            this.txid = txid;
            this.mode = mode;
            this.thread = thread;
        }
    }
}