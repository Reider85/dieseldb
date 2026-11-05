package diesel.storage.page;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.wal.WALManager;
import diesel.wal.WALArchiver;
import diesel.TxStatusTracker;
import diesel.recovery.CheckpointStrategy;

/**
 * Background checkpoint manager (prompt4.md #21, R3-005 step 2/3).
 *
 * <p>Periodically checkpoints the WAL and flushes dirty pages. Triggers on:
 * - time: every `checkpoint.interval.ms` (default 300000 = 5 min)
 * - WAL size: when WAL bytes since last checkpoint >= `checkpoint.max.wal.bytes` (default 268435456 = 256MB)
 *
 * <p>Thread-safe lifecycle: start() / stop() / stopWithoutCheckpoint() are idempotent.
 * AutoCloseable for try-with-resources support.
 */
public final class CheckpointManager implements AutoCloseable, CheckpointManagerMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String OBJECT_NAME_PREFIX = "diesel:type=CheckpointManager";
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final WALManager walManager;
    private final BufferPoolFlusher bufferPoolFlusher;
    private final BufferPool bufferPool;
    private final TxStatusTracker txStatusTracker;
    private final long intervalMs;
    private final long maxWalBytes;
    private final boolean enabled;
    private final ReadWriteLock writeGate = new ReentrantReadWriteLock();
    private final CheckpointStrategy strategy;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicLong checkpointCount = new AtomicLong(0);
    private final AtomicLong totalCheckpoints = new AtomicLong(0);
    private final AtomicLong lastCheckpointLsn = new AtomicLong(0);
    private final AtomicLong lastCheckpointDurationMs = new AtomicLong(0);
    private final AtomicLong totalFlushedPages = new AtomicLong(0);
    private final AtomicLong totalFlushDurationMs = new AtomicLong(0);
    private volatile ObjectName registeredName;

    private Thread checkpointThread;
    private volatile long bytesAtLastCheckpoint;

    /**
     * Creates a checkpoint manager with the given dependencies and strategy.
     *
     * @param walManager the WAL manager (must not be null)
     * @param bufferPoolFlusher the background flusher (nullable, direct pool flush used if null)
     * @param bufferPool the buffer pool (must not be null)
     * @param txStatusTracker transaction status tracker (nullable, empty list used if null)
     * @param intervalMs checkpoint interval in milliseconds (must be positive)
     * @param maxWalBytes maximum WAL bytes before triggering checkpoint (must be positive)
     * @param enabled whether checkpointing is enabled
     * @param strategy the checkpoint strategy (nullable, defaults to sharp for backward compatibility)
     */
    public CheckpointManager(WALManager walManager,
                             BufferPoolFlusher bufferPoolFlusher,
                             BufferPool bufferPool,
                             TxStatusTracker txStatusTracker,
                             long intervalMs,
                             long maxWalBytes,
                             boolean enabled,
                             CheckpointStrategy strategy) {
        if (walManager == null) {
            throw new IllegalArgumentException("WAL manager must not be null");
        }
        if (bufferPool == null) {
            throw new IllegalArgumentException("Buffer pool must not be null");
        }
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got " + intervalMs);
        }
        if (maxWalBytes <= 0) {
            throw new IllegalArgumentException("Max WAL bytes must be positive, got " + maxWalBytes);
        }
        
        this.walManager = walManager;
        this.bufferPoolFlusher = bufferPoolFlusher;
        this.bufferPool = bufferPool;
        this.txStatusTracker = txStatusTracker;
        this.intervalMs = intervalMs;
        this.maxWalBytes = maxWalBytes;
        this.enabled = enabled;
        this.strategy = strategy != null ? strategy : createDefaultStrategy();
        this.bytesAtLastCheckpoint = walManager.getTotalAppendedBytes();
    }
    
    /**
     * Creates a checkpoint manager with legacy sharp checkpoint behavior.
     * For backward compatibility with existing tests and code.
     *
     * @param walManager the WAL manager (must not be null)
     * @param bufferPoolFlusher the background flusher (nullable, direct pool flush used if null)
     * @param bufferPool the buffer pool (must not be null)
     * @param txStatusTracker transaction status tracker (nullable, empty list used if null)
     * @param intervalMs checkpoint interval in milliseconds (must be positive)
     * @param maxWalBytes maximum WAL bytes before triggering checkpoint (must be positive)
     * @param enabled whether checkpointing is enabled
     */
    public CheckpointManager(WALManager walManager,
                             BufferPoolFlusher bufferPoolFlusher,
                             BufferPool bufferPool,
                             TxStatusTracker txStatusTracker,
                             long intervalMs,
                             long maxWalBytes,
                             boolean enabled) {
        this(walManager, bufferPoolFlusher, bufferPool, txStatusTracker, 
             intervalMs, maxWalBytes, enabled, null); // null = default sharp strategy
    }
    
    /**
     * Creates a default sharp checkpoint strategy for backward compatibility.
     */
    private CheckpointStrategy createDefaultStrategy() {
        return new diesel.recovery.SharpCheckpoint(walManager, bufferPoolFlusher, 
                                                  bufferPool, txStatusTracker, writeGate);
    }

    /**
     * Starts the checkpoint daemon thread.
     * Idempotent: does nothing if already running.
     */
    public void start() {
        if (!enabled) {
            LOGGER.info("CheckpointManager disabled (checkpoint.enabled=false)");
            return;
        }

        if (running.get()) {
            LOGGER.debug("CheckpointManager already running");
            return;
        }

        shutdownRequested.set(false);
        running.set(true);
        registerMBean();
        checkpointThread = new Thread(this::checkpointLoop, "diesel-checkpoint");
        checkpointThread.setDaemon(true);
        checkpointThread.start();
        LOGGER.info("CheckpointManager started with interval {}ms, maxWalBytes {}",
                    intervalMs, maxWalBytes);
    }

    /**
     * Stops the checkpoint daemon after a final checkpoint.
     * Idempotent: does nothing if not running.
     */
    @Override
    public void close() throws IOException {
        stop();
    }

    /**
     * Stops the checkpoint daemon after a final checkpoint.
     * Idempotent: does nothing if not running.
     *
     * @throws IOException if the final checkpoint fails
     */
    public void stop() throws IOException {
        stopWithCheckpoint(true);
    }

    /**
     * Stops the checkpoint daemon without doing a final checkpoint.
     * Useful for crash simulation: checkpoint state is left incomplete.
     * Idempotent: does nothing if not running.
     */
    public void stopWithoutCheckpoint() {
        try {
            stopWithCheckpoint(false);
        } catch (IOException e) {
            // This shouldn't happen since we're not doing a final checkpoint
            LOGGER.warn("Unexpected IOException during stopWithoutCheckpoint: " + e.getMessage());
        }
    }

    private void stopWithCheckpoint(boolean doCheckpoint) throws IOException {
        if (!running.get()) {
            LOGGER.debug("CheckpointManager not running");
            return;
        }

        shutdownRequested.set(true);
        running.set(false);

        // Do final checkpoint if requested
        if (doCheckpoint) {
            LOGGER.debug("CheckpointManager doing final checkpoint");
            checkpointNow();
        }

        // Wait for thread to finish (with timeout)
        if (checkpointThread != null) {
            try {
                checkpointThread.join(5000); // 5s timeout
                if (checkpointThread.isAlive()) {
                    LOGGER.warn("CheckpointManager thread did not stop cleanly");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Interrupted while waiting for CheckpointManager to stop");
            }
            checkpointThread = null;
        }

        unregisterMBean();
        LOGGER.info("CheckpointManager stopped (final checkpoint: {})", doCheckpoint);
    }

    /**
     * Returns whether the checkpoint manager is currently running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Performs a checkpoint immediately using the configured strategy.
     * Delegates to the checkpoint strategy for flush and record writing logic.
     *
     * @return LSN of the checkpoint, or 0 if checkpointing is disabled
     * @throws IOException if checkpoint fails
     */
    public synchronized long checkpointNow() throws IOException {
        if (!enabled) {
            LOGGER.debug("CheckpointManager disabled - skipping checkpoint");
            return 0;
        }

        long start = System.currentTimeMillis();
        
        // Delegate to checkpoint strategy
        long checkpointLsn;
        if (strategy != null) {
            checkpointLsn = strategy.checkpoint();
        } else {
            // Fallback to legacy sharp behavior (should not happen with new constructor)
            checkpointLsn = legacyCheckpointNow();
        }
        
        if (checkpointLsn > 0) {
            lastCheckpointLsn.set(checkpointLsn);
            totalCheckpoints.incrementAndGet();
            checkpointCount.incrementAndGet();

            // Update metrics
            long duration = System.currentTimeMillis() - start;
            lastCheckpointDurationMs.set(duration);
            totalFlushDurationMs.addAndGet(duration);

            // Trigger WAL archiver for segments before checkpoint
            try {
                WALArchiver archiver = walManager.getArchiver();
                if (archiver != null) {
                    archiver.runOnce();
                }
            } catch (Exception e) {
                LOGGER.warn("WAL archiver runOnce failed after checkpoint: " + e.getMessage());
            }

            // Reset WAL byte counter
            bytesAtLastCheckpoint = walManager.getTotalAppendedBytes();

            LOGGER.info("Checkpoint completed at LSN {} in {} ms using {} strategy",
                       checkpointLsn, duration, strategy != null ? strategy.name() : "legacy");
        }

        return checkpointLsn;
    }
    
    /**
     * Legacy checkpoint implementation for backward compatibility.
     * Used only when strategy is null (should not happen with new constructor).
     */
    private long legacyCheckpointNow() throws IOException {
        writeGate.writeLock().lock();
        try {
            // Flush all dirty pages
            int flushed;
            if (bufferPoolFlusher != null) {
                flushed = bufferPoolFlusher.flushNow();
            } else {
                // Fallback: direct pool flush (all pages eligible)
                bufferPool.flushDirty();
                flushed = bufferPool.getDirtyPageCount(); // Approximate count
            }
            if (flushed > 0) {
                totalFlushedPages.addAndGet(flushed);
                LOGGER.debug("Checkpoint flushed {} dirty pages", flushed);
            }

            // Get active transaction IDs
            List<Long> activeTxids = txStatusTracker != null ? 
                txStatusTracker.getActiveTxids() : List.of();

            // Write checkpoint record
            walManager.writeCheckpoint(activeTxids);
            long checkpointLsn = walManager.getLastCheckpointLsn();
            
            if (flushed > 0) {
                LOGGER.debug("Legacy checkpoint flushed {} dirty pages", flushed);
            }

            return checkpointLsn;
        } finally {
            writeGate.writeLock().unlock();
        }
    }

    /**
     * Returns the write gate for page-level synchronization.
     * PageManager.writePage and allocatePage should acquire the read lock
     * when this gate is not null.
     *
     * @return the write gate, never null
     */
    public ReadWriteLock getWriteGate() {
        return writeGate;
    }
    
    /**
     * Returns the name of the current checkpoint strategy.
     * Useful for testing and monitoring.
     *
     * @return strategy name, or "legacy" if no strategy is set
     */
    public String getCheckpointStrategyName() {
        return strategy != null ? strategy.name() : "legacy";
    }

    // ─── JMX MXBean methods ────────────────────────────────────────────────

    @Override
    public long getCheckpointCount() {
        return checkpointCount.get();
    }

    @Override
    public long getLastCheckpointLsn() {
        return lastCheckpointLsn.get();
    }

    @Override
    public long getLastCheckpointDurationMs() {
        return lastCheckpointDurationMs.get();
    }

    @Override
    public long getTotalCheckpoints() {
        return totalCheckpoints.get();
    }

    @Override
    public int getCurrentIntervalMs() {
        return (int) intervalMs;
    }

    @Override
    public int getBaseIntervalMs() {
        return (int) intervalMs;
    }

    @Override
    public long getTotalFlushedPages() {
        return totalFlushedPages.get();
    }

    @Override
    public long getTotalFlushDurationMs() {
        return totalFlushDurationMs.get();
    }

    // ─── Private methods ─────────────────────────────────────────────────────

    private void checkpointLoop() {
        LOGGER.debug("CheckpointManager loop started");

        try {
            while (running.get() && !shutdownRequested.get()) {
                long cycleStart = System.currentTimeMillis();

                // Check if we need to trigger checkpoint
                boolean shouldCheckpoint = shouldTriggerCheckpoint();
                if (shouldCheckpoint) {
                    checkpointNow();
                }

                // Sleep for the remaining interval
                long cycleDuration = System.currentTimeMillis() - cycleStart;
                long remainingSleep = intervalMs - cycleDuration;
                if (remainingSleep > 0 && running.get() && !shutdownRequested.get()) {
                    try {
                        Thread.sleep(remainingSleep);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.debug("CheckpointManager sleep interrupted");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("CheckpointManager loop error", e);
        } finally {
            running.set(false);
            LOGGER.debug("CheckpointManager loop ended");
        }
    }

    private boolean shouldTriggerCheckpoint() {
        long currentBytes = walManager.getTotalAppendedBytes();
        long bytesSinceLast = currentBytes - bytesAtLastCheckpoint;
        
        boolean timeTriggered = System.currentTimeMillis() >= 
            (lastCheckpointDurationMs.get() + intervalMs);
        boolean walTriggered = bytesSinceLast >= maxWalBytes;
        
        if (timeTriggered || walTriggered) {
            LOGGER.debug("Checkpoint triggered: time={}, walSince={} (threshold {})",
                        timeTriggered, bytesSinceLast, maxWalBytes);
            return true;
        }
        return false;
    }

    private void registerMBean() {
        try {
            StandardMBean mbean = new StandardMBean((CheckpointManagerMXBean) this, 
                CheckpointManagerMXBean.class) {};
            ObjectName name = new ObjectName(
                    OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, name);
            registeredName = name;
        } catch (Exception e) {
            LOGGER.warn("Failed to register CheckpointManager MBean: {}", e.toString());
        }
    }

    private void unregisterMBean() {
        ObjectName name = registeredName;
        registeredName = null;
        if (name != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
            } catch (Exception ignored) {
                // Already gone; nothing to clean up.
            }
        }
    }
}