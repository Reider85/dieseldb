package diesel.storage.page;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive flush strategy for BufferPoolFlusher (prompt4.md #20, R3-005 step 1/3).
 *
 * <p>Adjusts flush interval based on dirty page ratio:
 * - dirty ratio > {@link #AGGRESSIVE_RATIO} (0.25): interval = base × 0.5 (more aggressive)
 * - dirty ratio < {@link #LAZY_RATIO} (0.05): interval = base × 2 (less aggressive)
 * - otherwise: use base interval
 * 
 * <p>Clamps the adaptive interval to [MIN_INTERVAL_MS, MAX_INTERVAL_MS] to prevent
 * excessive frequency or long delays.
 */
public final class AdaptiveFlushStrategy {

    /** Aggressive threshold: if dirty pages > 25% of capacity, shrink interval */
    public static final double AGGRESSIVE_RATIO = 0.25;
    /** Lazy threshold: if dirty pages < 5% of capacity, grow interval */
    public static final double LAZY_RATIO = 0.05;
    /** Minimum adaptive interval (10ms) to prevent excessive frequency */
    public static final int MIN_INTERVAL_MS = 10;
    /** Maximum adaptive interval (60s) to prevent long delays */
    public static final int MAX_INTERVAL_MS = 60_000;

    private final int baseIntervalMs;
    private final AtomicInteger currentIntervalMs = new AtomicInteger();
    private final AtomicLong lastUpdateMillis = new AtomicLong();
    private final AtomicLong dirtyPageCount = new AtomicLong();
    private final AtomicLong capacityPages = new AtomicLong();

    /**
     * Creates a strategy with the given base interval.
     *
     * @param baseIntervalMs the base interval in milliseconds (must be positive)
     */
    public AdaptiveFlushStrategy(int baseIntervalMs) {
        if (baseIntervalMs <= 0) {
            throw new IllegalArgumentException("Base interval must be positive, got " + baseIntervalMs);
        }
        this.baseIntervalMs = baseIntervalMs;
        this.currentIntervalMs.set(baseIntervalMs);
        this.lastUpdateMillis.set(System.currentTimeMillis());
    }

    /**
     * Updates the dirty page count and recomputes the adaptive interval if needed.
     *
     * @param dirtyPages current number of dirty pages
     * @param capacityPages total capacity in pages
     * @return the new interval in milliseconds
     */
    public synchronized int updateAndGetCurrentInterval(int dirtyPages, int capacityPages) {
        if (capacityPages <= 0) {
            return baseIntervalMs;
        }

        // Update state
        this.dirtyPageCount.set(dirtyPages);
        this.capacityPages.set(capacityPages);
        this.lastUpdateMillis.set(System.currentTimeMillis());

        // Compute adaptive interval
        double ratio = getCurrentDirtyRatio();
        int newInterval;

        if (ratio > AGGRESSIVE_RATIO) {
            // More than 25% dirty: be more aggressive (shorter interval)
            newInterval = (int) (baseIntervalMs * 0.5);
        } else if (ratio < LAZY_RATIO) {
            // Less than 5% dirty: be lazy (longer interval)
            newInterval = (int) (baseIntervalMs * 2);
        } else {
            // Within thresholds: use base interval
            newInterval = baseIntervalMs;
        }

        // Clamp to reasonable bounds
        newInterval = Math.max(MIN_INTERVAL_MS, Math.min(MAX_INTERVAL_MS, newInterval));

        // Update current interval
        currentIntervalMs.set(newInterval);
        return newInterval;
    }

    /**
     * Returns the current adaptive interval in milliseconds.
     *
     * @return the current interval
     */
    public int getCurrentIntervalMs() {
        return currentIntervalMs.get();
    }

    /**
     * Returns the base interval configured at construction.
     *
     * @return the base interval in milliseconds
     */
    public int getBaseIntervalMs() {
        return baseIntervalMs;
    }

    /**
     * Returns the last dirty page count used for interval computation.
     *
     * @return dirty page count, or 0 if never updated
     */
    public long getDirtyPageCount() {
        return dirtyPageCount.get();
    }

    /**
     * Returns the last capacity used for interval computation.
     *
     * @return capacity in pages, or 0 if never updated
     */
    public long getCapacityPages() {
        return capacityPages.get();
    }

    /**
     * Returns the timestamp of the last interval update.
     *
     * @return last update timestamp in milliseconds since epoch
     */
    public long getLastUpdateMillis() {
        return lastUpdateMillis.get();
    }

    /**
     * Returns the current dirty ratio (dirty pages / capacity), clamped to [0.0, 1.0].
     *
     * @return dirty ratio, or 0 if never updated
     */
    public double getCurrentDirtyRatio() {
        long dirty = dirtyPageCount.get();
        long capacity = capacityPages.get();
        if (capacity <= 0) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, (double) dirty / capacity));
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.ROOT,
                "AdaptiveFlushStrategy{base=%dms, current=%dms, dirty=%d/%d=%.2f, lastUpdate=%d}",
                baseIntervalMs,
                currentIntervalMs.get(),
                dirtyPageCount.get(),
                capacityPages.get(),
                getCurrentDirtyRatio(),
                lastUpdateMillis.get());
    }
}