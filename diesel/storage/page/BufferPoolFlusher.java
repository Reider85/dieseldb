package diesel.storage.page;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background flusher for BufferPool (prompt4.md #20, R3-005 step 1/3).
 *
 * <p>Periodically flushes dirty pages from the buffer pool using an adaptive strategy.
 * Flushes only pages with LSN <= last WAL flushed LSN (WAL-before-page rule).
 * Skips pinned pages to avoid torn writes.
 *
 * <p>Thread-safe lifecycle: start() / stop() / stopWithoutFlush() are idempotent.
 * AutoCloseable for try-with-resources support.
 */
public final class BufferPoolFlusher implements AutoCloseable, BufferPoolFlusherMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(BufferPoolFlusher.class);
    private static final String OBJECT_NAME_PREFIX = "diesel:type=BufferPoolFlusher";
    private static final AtomicInteger MBEAN_SEQUENCE = new AtomicInteger();

    private final BufferPool pool;
    private final LongSupplier lastWalFlushLsnSupplier;
    private final AdaptiveFlushStrategy adaptiveStrategy;
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final AtomicLong flushCount = new AtomicLong(0);
    private final AtomicLong totalFlushedPages = new AtomicLong(0);
    private final AtomicLong totalFlushDurationMs = new AtomicLong(0);
    private final AtomicLong lastFlushDurationMs = new AtomicLong(0);
    private volatile ObjectName registeredName;

    private Thread flusherThread;
    private volatile int currentIntervalMs;

    /**
     * Creates a flusher with the given pool and WAL LSN supplier.
     *
     * @param pool the buffer pool to flush
     * @param lastWalFlushLsnSupplier supplier of last flushed WAL LSN, or null if WAL disabled
     * @param baseIntervalMs base flush interval in milliseconds (must be positive)
     */
    public BufferPoolFlusher(BufferPool pool, LongSupplier lastWalFlushLsnSupplier, int baseIntervalMs) {
        if (pool == null) {
            throw new IllegalArgumentException("Buffer pool must not be null");
        }
        if (baseIntervalMs <= 0) {
            throw new IllegalArgumentException("Base interval must be positive, got " + baseIntervalMs);
        }
        this.pool = pool;
        this.lastWalFlushLsnSupplier = lastWalFlushLsnSupplier != null ? lastWalFlushLsnSupplier : () -> Long.MAX_VALUE;
        this.adaptiveStrategy = new AdaptiveFlushStrategy(baseIntervalMs);
        this.currentIntervalMs = baseIntervalMs;
    }

    /**
     * Starts the flusher daemon thread.
     * Idempotent: does nothing if already running.
     *
     * @throws IllegalStateException if the pool is closed
     */
    public void start() {
        lifecycleLock.lock();
        try {
            if (running.get()) {
                LOGGER.debug("BufferPoolFlusher already running");
                return;
            }

            if (pool.getObjectName() == null) {
                throw new IllegalStateException("BufferPool is closed");
            }

            shutdownRequested.set(false);
            running.set(true);
            registerMBean();
            flusherThread = new Thread(this::flusherLoop, "diesel-bufferpool-flusher");
            flusherThread.setDaemon(true);
            flusherThread.start();
            LOGGER.info("BufferPoolFlusher started with base interval {}ms", currentIntervalMs);
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Stops the flusher after a final flush cycle.
     * Idempotent: does nothing if not running.
     *
     * @throws IOException if the final flush fails
     */
    @Override
    public void close() throws IOException {
        stop();
    }

    /**
     * Stops the flusher after a final flush cycle.
     * Idempotent: does nothing if not running.
     *
     * @throws IOException if the final flush fails
     */
    public void stop() throws IOException {
        stopWithFlush(true);
    }

    /**
     * Stops the flusher without doing a final flush.
     * Useful for crash simulation: dirty pages are left dirty as expected.
     * Idempotent: does nothing if not running.
     */
    public void stopWithoutFlush() {
        try {
            stopWithFlush(false);
        } catch (IOException e) {
            // This shouldn't happen since we're not doing a final flush
            LOGGER.warn("Unexpected IOException during stopWithoutFlush: " + e.getMessage());
        }
    }

    private void stopWithFlush(boolean doFinalFlush) throws IOException {
        lifecycleLock.lock();
        try {
            if (!running.get()) {
                LOGGER.debug("BufferPoolFlusher not running");
                return;
            }

            shutdownRequested.set(true);
            running.set(false);

            // Wait for thread to finish (with timeout)
            if (flusherThread != null) {
                try {
                    flusherThread.join(5000); // 5s timeout
                    if (flusherThread.isAlive()) {
                        LOGGER.warn("BufferPoolFlusher thread did not stop cleanly");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted while waiting for BufferPoolFlusher to stop");
                }
                flusherThread = null;
            }

            // Do final flush if requested (shutdownRequested is already set
            // above to break the loop, so it must not gate the final flush)
            if (doFinalFlush) {
                LOGGER.debug("BufferPoolFlusher doing final flush");
                flushDirtyPages();
            }

            unregisterMBean();
            LOGGER.info("BufferPoolFlusher stopped (final flush: {})", doFinalFlush);
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Returns whether the flusher is currently running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Returns the current adaptive interval in milliseconds.
     *
     * @return current interval
     */
    public int getCurrentIntervalMs() {
        return adaptiveStrategy.getCurrentIntervalMs();
    }

    /**
     * Returns the base interval configured at construction.
     *
     * @return base interval in milliseconds
     */
    public int getBaseIntervalMs() {
        return adaptiveStrategy.getBaseIntervalMs();
    }

    /**
     * Returns the number of flush cycles executed.
     *
     * @return flush count
     */
    public long getFlushCount() {
        return flushCount.get();
    }

    /**
     * Returns the total number of pages flushed.
     *
     * @return total flushed pages
     */
    public long getTotalFlushedPages() {
        return totalFlushedPages.get();
    }

    /**
     * Returns the duration of the last flush cycle in milliseconds.
     *
     * @return last flush duration in milliseconds
     */
    public long getLastFlushDurationMs() {
        return lastFlushDurationMs.get();
    }

    /**
     * Returns the total time spent flushing in milliseconds.
     *
     * @return total flush duration in milliseconds
     */
    public long getTotalFlushDurationMs() {
        return totalFlushDurationMs.get();
    }

    /**
     * Returns the current dirty page count from the pool.
     *
     * @return dirty page count
     */
    public long getDirtyPageCount() {
        return pool.getDirtyPageCount();
    }

    /**
     * Returns the pool capacity in pages.
     *
     * @return pool capacity
     */
    public int getCapacityPages() {
        return pool.getCapacityPages();
    }

    /**
     * Returns the JMX object name this flusher is registered under.
     *
     * @return the registered object name, or null if registration failed
     */
    public ObjectName getObjectName() {
        return registeredName;
    }

    /**
     * Main flusher loop: sleeps the adaptive interval, then flushes dirty pages.
     */
    private void flusherLoop() {
        LOGGER.debug("BufferPoolFlusher loop started");

        try {
            while (running.get() && !shutdownRequested.get()) {
                long cycleStart = System.currentTimeMillis();

                // Update adaptive interval based on current dirty state
                int dirtyPages = pool.getDirtyPageCount();
                int capacityPages = pool.getCapacityPages();
                currentIntervalMs = adaptiveStrategy.updateAndGetCurrentInterval(dirtyPages, capacityPages);
                
                LOGGER.debug("Flush cycle: dirty={}, capacity={}, interval={}ms", 
                    dirtyPages, capacityPages, currentIntervalMs);

                // Flush dirty pages
                if (dirtyPages > 0) {
                    flushDirtyPages();
                }

                // Record cycle metrics
                long cycleDuration = System.currentTimeMillis() - cycleStart;
                lastFlushDurationMs.set(cycleDuration);
                totalFlushDurationMs.addAndGet(cycleDuration);

                // Sleep for the adaptive interval (with shutdown check)
                long remainingSleep = currentIntervalMs - cycleDuration;
                if (remainingSleep > 0 && running.get() && !shutdownRequested.get()) {
                    try {
                        Thread.sleep(remainingSleep);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.debug("BufferPoolFlusher sleep interrupted");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("BufferPoolFlusher loop error", e);
        } finally {
            running.set(false);
            LOGGER.debug("BufferPoolFlusher loop ended");
        }
    }

    /**
     * Flushes dirty pages matching the WAL LSN predicate.
     */
    private void flushDirtyPages() throws IOException {
        long watermark = lastWalFlushLsnSupplier.getAsLong();
        Predicate<Page> eligible = page -> {
            // Pages with LSN 0 (normal writes) are always flushable
            long pageLsn = page.getLsn();
            return pageLsn == 0 || pageLsn <= watermark;
        };

        try {
            int flushed = pool.flushDirtyMatching(eligible);
            if (flushed > 0) {
                flushCount.incrementAndGet();
                totalFlushedPages.addAndGet(flushed);
                LOGGER.debug("Flushed {} dirty pages (WAL watermark: {})", flushed, watermark);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to flush dirty pages", e);
            throw e;
        }
    }

    /**
     * Registers JMX MBean for monitoring.
     */
    private void registerMBean() {
        try {
            StandardMBean mbean = new StandardMBean((BufferPoolFlusherMXBean) this, BufferPoolFlusherMXBean.class) {};
            ObjectName name = new ObjectName(
                    OBJECT_NAME_PREFIX + ",id=" + MBEAN_SEQUENCE.incrementAndGet());
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, name);
            registeredName = name;
        } catch (Exception e) {
            LOGGER.warn("Failed to register BufferPoolFlusher MBean: {}", e.toString());
        }
    }

    /**
     * Unregisters the JMX MBean.
     */
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