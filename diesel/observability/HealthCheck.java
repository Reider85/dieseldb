package diesel.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;

/**
 * JSON health endpoint orchestrator (prompt4 #46, R3-012 step 3/3).
 *
 * <p>Combines a {@link LivenessCheck} and a {@link ReadinessCheck} with the
 * runtime figures Kubernetes operators expect from a database probe:
 *
 * <pre>{@code
 * {"status":"UP","uptime":3600,"activeTx":5,"diskFree":1024,"walLag":50,
 *  "checks":{"live":"UP","ready":"UP"},"reasons":[]}
 * }</pre>
 *
 * <p>Three views are served (see {@code MetricsHttpServer}):
 * <ul>
 *   <li>{@code /health} — combined verdict + runtime figures</li>
 *   <li>{@code /health/live} — liveness only</li>
 *   <li>{@code /health/ready} — readiness only</li>
 * </ul>
 *
 * <p>The HTTP status is {@code 200} when the view is UP and {@code 503} when it
 * is DOWN, which is what the native Kubernetes probes consume; the JSON body
 * carries the diagnostic detail.
 *
 * <p>All values are supplier-backed so a probe never serves stale data and the
 * probe cost stays O(1). The {@code diesel.Database}-wiring factory lives in
 * {@code diesel.HealthSupport} because {@code Database} is package-private.
 */
public final class HealthCheck {

    /** Content type of every health response. */
    public static final String CONTENT_TYPE = "application/json";
    /** Base path of the health endpoint. */
    public static final String HEALTH_PATH = "/health";
    /** Sub-path of the liveness view. */
    public static final String LIVE_PATH = HEALTH_PATH + "/live";
    /** Sub-path of the readiness view. */
    public static final String READY_PATH = HEALTH_PATH + "/ready";

    /** Default free-heap floor (percent of max heap) for the OOM guard. */
    public static final double DEFAULT_OOM_FREE_HEAP_PERCENT = 5.0;
    /** Default minimum resident pages for a warm buffer pool. */
    public static final int DEFAULT_COLDSTART_MIN_PAGES = 0;

    private final LivenessCheck liveness;
    private final ReadinessCheck readiness;
    private final DoubleSupplier activeTransactions;
    private final DoubleSupplier diskFreeMb;
    private final DoubleSupplier walLagMs;
    private final long startNanos;

    /**
     * Creates a health orchestrator.
     *
     * @param liveness           liveness probe
     * @param readiness          readiness probe
     * @param activeTransactions currently open transactions
     * @param diskFreeMb         free disk space on the data volume, in MB
     * @param walLagMs           milliseconds since the last successful WAL append
     * @param startNanos         {@link System#nanoTime()} captured at server start
     */
    public HealthCheck(LivenessCheck liveness, ReadinessCheck readiness,
                       DoubleSupplier activeTransactions, DoubleSupplier diskFreeMb,
                       DoubleSupplier walLagMs, long startNanos) {
        this.liveness = liveness;
        this.readiness = readiness;
        this.activeTransactions = activeTransactions;
        this.diskFreeMb = diskFreeMb;
        this.walLagMs = walLagMs;
        this.startNanos = startNanos;
    }

    /** Returns the liveness view. */
    public HealthResult live() {
        return liveness.check();
    }

    /** Returns the readiness view. */
    public HealthResult ready() {
        return readiness.check();
    }

    /**
     * Returns the combined view: UP only when both sub-probes are UP.
     *
     * @return the combined verdict and JSON document
     */
    public HealthResult combined() {
        HealthResult live = liveness.check();
        HealthResult ready = readiness.check();
        boolean up = live.up() && ready.up();
        List<String> reasons = new ArrayList<>(live.reasons().size() + ready.reasons().size());
        reasons.addAll(live.reasons());
        reasons.addAll(ready.reasons());

        String body = "{\"status\":\"" + (up ? "UP" : "DOWN") + "\","
                + "\"uptime\":" + uptimeSeconds() + ","
                + "\"activeTx\":" + Math.round(activeTransactions.getAsDouble()) + ","
                + "\"diskFree\":" + Math.round(diskFreeMb.getAsDouble()) + ","
                + "\"walLag\":" + Math.round(walLagMs.getAsDouble()) + ","
                + "\"checks\":{\"live\":\"" + (live.up() ? "UP" : "DOWN") + "\","
                + "\"ready\":\"" + (ready.up() ? "UP" : "DOWN") + "\"},"
                + "\"reasons\":" + HealthJson.array(reasons) + "}";
        return new HealthResult(up, body, reasons);
    }

    /** Seconds since the server started. */
    private long uptimeSeconds() {
        return (System.nanoTime() - startNanos) / 1_000_000_000L;
    }
}
