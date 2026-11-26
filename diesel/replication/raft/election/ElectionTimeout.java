package diesel.replication.raft.election;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diesel.replication.raft.RaftConfig;

/**
 * Election timeout management for Raft leader election (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Manages randomized election timeouts to split votes in case of split votes.
 * Uses jitter to prevent synchronized elections across the cluster.
 */
public class ElectionTimeout {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionTimeout.class);
    
    // Configuration
    private final RaftConfig config;
    private final Runnable onTimeout;
    
    // State
    private final AtomicReference<Long> scheduledTimeout = new AtomicReference<>(null);
    private final AtomicLong lastResetTime = new AtomicLong(0);
    private final AtomicLong currentTimeout = new AtomicLong(0);
    
    /**
     * Create a new election timeout.
     */
    public ElectionTimeout(RaftConfig config) {
        this.config = config;
        this.onTimeout = () -> {
            LOGGER.debug("Election timeout triggered");
            if (scheduledTimeout.compareAndSet(currentTimeout.get(), null)) {
                // Timeout occurred - in a real implementation, this would trigger the timeout
                LOGGER.info("Election timeout expired for node");
            }
        };
    }
    
    /**
     * Reset the election timeout with randomized jitter.
     */
    public void reset() {
        long newTimeout = generateRandomizedTimeout();
        currentTimeout.set(newTimeout);
        lastResetTime.set(System.currentTimeMillis());
        
        LOGGER.debug("Election timeout reset to {}ms", newTimeout);
    }
    
    /**
     * Schedule the next election timeout.
     */
    public void schedule(Runnable timeoutExecutor) {
        long timeout = currentTimeout.get();
        if (timeout > 0) {
            long scheduledTime = System.currentTimeMillis() + timeout;
            scheduledTimeout.set(scheduledTime);
            
            // In a real implementation, this would use a scheduler
            // For now, we'll just log the scheduled time
            LOGGER.debug("Election timeout scheduled for {}ms from now", timeout);
        }
    }
    
    /**
     * Check if the election timeout has expired.
     */
    public boolean isExpired() {
        long lastReset = lastResetTime.get();
        long timeout = currentTimeout.get();
        
        if (timeout <= 0 || lastReset == 0) {
            return false;
        }
        
        long elapsed = System.currentTimeMillis() - lastReset;
        return elapsed >= timeout;
    }
    
    /**
     * Get the current timeout value.
     */
    public long getTimeout() {
        return currentTimeout.get();
    }
    
    /**
     * Get the time since last reset.
     */
    public long getTimeSinceReset() {
        long lastReset = lastResetTime.get();
        if (lastReset == 0) {
            return 0;
        }
        return System.currentTimeMillis() - lastReset;
    }
    
    /**
     * Generate a randomized timeout with jitter.
     */
    private long generateRandomizedTimeout() {
        // Base timeout with jitter: [electionTimeoutMs/2, 3*electionTimeoutMs/2]
        long baseTimeout = config.getElectionTimeoutMs();
        long minTimeout = baseTimeout / 2;
        long maxTimeout = (long) (baseTimeout * 1.5);
        
        long randomTimeout = minTimeout + ThreadLocalRandom.current().nextLong(maxTimeout - minTimeout);
        
        // Ensure timeout is at least 100ms to prevent too frequent elections
        return Math.max(randomTimeout, 100);
    }
    
    /**
     * Get election timeout statistics.
     */
    public TimeoutStats getStats() {
        return new TimeoutStats(
            currentTimeout.get(),
            lastResetTime.get(),
            getTimeSinceReset(),
            isExpired()
        );
    }
    
    /**
     * Election timeout statistics.
     */
    public static class TimeoutStats {
        private final long timeoutMs;
        private final long lastResetTime;
        private final long timeSinceReset;
        private final boolean expired;
        
        public TimeoutStats(long timeoutMs, long lastResetTime, long timeSinceReset, boolean expired) {
            this.timeoutMs = timeoutMs;
            this.lastResetTime = lastResetTime;
            this.timeSinceReset = timeSinceReset;
            this.expired = expired;
        }
        
        // Getters
        public long getTimeoutMs() { return timeoutMs; }
        public long getLastResetTime() { return lastResetTime; }
        public long getTimeSinceReset() { return timeSinceReset; }
        public boolean isExpired() { return expired; }
        
        @Override
        public String toString() {
            return String.format("TimeoutStats{timeout=%dms, elapsed=%dms, expired=%s}", 
                timeoutMs, timeSinceReset, expired);
        }
    }
}