package diesel.observability;

import java.util.concurrent.atomic.LongAdder;

/**
 * A monotonically increasing counter (prompt4 #44).
 *
 * <p>Backed by a {@link LongAdder}: increments are lock-free and scale to the
 * number of cores under contention, at the cost of {@link #get()} being an
 * eventually-consistent sum across threads (a thread always immediately sees
 * its own increments). This is the standard trade-off for hot-path metrics
 * (query executor, WAL writer, lock manager).
 */
public final class Counter {

    /** Prometheus-style base name (e.g. {@code diesel_queries_total}). */
    private final String name;
    /** Label pairs rendered as {@code k=v,k=v}; empty when unlabeled. */
    private final String labels;
    private final LongAdder value = new LongAdder();

    /**
     * Creates a counter. Used by {@link MetricsRegistry}; tests may construct
     * counters directly for isolation.
     *
     * @param name   the metric base name
     * @param labels the pre-rendered label string, or empty for no labels
     */
    public Counter(String name, String labels) {
        this.name = name;
        this.labels = labels == null ? "" : labels;
    }

    /** Increments the counter by one. */
    public void increment() {
        value.increment();
    }

    /**
     * Adds the given delta (must not be negative — counters are monotonic).
     *
     * @param delta the non-negative amount to add
     */
    public void add(long delta) {
        if (delta < 0) {
            throw new IllegalArgumentException("counters are monotonic; cannot add " + delta);
        }
        value.add(delta);
    }

    /**
     * Returns the current counter value.
     *
     * @return the sum of all increments (eventually consistent across threads)
     */
    public long get() {
        return value.sum();
    }

    /**
     * Returns the metric base name.
     *
     * @return the base name without labels
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the rendered label string.
     *
     * @return labels as {@code k=v,k=v}, or empty when unlabeled
     */
    public String getLabels() {
        return labels;
    }

    /**
     * Returns the full metric key (name plus labels).
     *
     * @return e.g. {@code diesel_queries_total{type=SELECT,status=ok}}, or just
     *         the name when unlabeled
     */
    public String key() {
        return labels.isEmpty() ? name : name + "{" + labels + "}";
    }

    @Override
    public String toString() {
        return key() + "=" + get();
    }
}
