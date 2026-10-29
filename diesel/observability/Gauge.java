package diesel.observability;

import java.util.function.DoubleSupplier;

/**
 * A point-in-time value metric (prompt4 #44), e.g. active transactions,
 * buffer-pool hit ratio or WAL lag.
 *
 * <p>Supplier-backed: the value is computed lazily on {@link #getValue()},
 * so the gauge never goes stale between scrapes and no explicit "set" call is
 * needed on the hot path. The supplier must be cheap and lock-free (typical
 * implementation: read an {@code AtomicInteger}/{@code AtomicLong} or a cached
 * counter).
 */
public final class Gauge {

    /** Prometheus-style base name (e.g. {@code diesel_transactions_active}). */
    private final String name;
    private final DoubleSupplier supplier;

    /**
     * Creates a gauge.
     *
     * @param name     the metric base name
     * @param supplier computes the current value on every read
     */
    public Gauge(String name, DoubleSupplier supplier) {
        this.name = name;
        this.supplier = supplier;
    }

    /**
     * Samples the current value.
     *
     * @return the value produced by the supplier
     */
    public double getValue() {
        return supplier.getAsDouble();
    }

    /**
     * Returns the metric base name.
     *
     * @return the base name
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name + "=" + getValue();
    }
}
