package diesel.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * Readiness probe (prompt4 #46, R3-012 step 3/3): answers "can this instance
 * serve traffic right now?".
 *
 * <p>Two conditions keep a healthy process out of the load-balancer rotation:
 * <ul>
 *   <li><b>Recovery in progress</b> — ARIES analysis/redo/undo has not finished.
 *       Serving clients against a half-recovered database would expose
 *       uncommitted rows or miss committed ones.</li>
 *   <li><b>Buffer pool cold</b> — no page has been loaded yet, so the first
 *       queries would pay a full cold-cache penalty (or the page manager is not
 *       initialised). Once the first page is read or written the pool is warm
 *       and the probe turns UP.</li>
 * </ul>
 *
 * <p>Both inputs are suppliers so the probe is cheap on every poll and can be
 * unit-tested without a real database.
 */
public final class ReadinessCheck {

    /** Reason emitted while startup recovery is still running. */
    public static final String REASON_RECOVERY_IN_PROGRESS = "recovery-in-progress";
    /** Reason emitted while the buffer pool has not loaded a single page. */
    public static final String REASON_BUFFER_POOL_COLD = "buffer-pool-cold";

    private final BooleanSupplier recoveryInProgress;
    private final IntSupplier residentPages;
    private final int coldStartMinPages;

    /**
     * Creates a readiness check.
     *
     * @param recoveryInProgress true while ARIES startup recovery is running
     * @param residentPages      pages currently resident in the buffer pool
     * @param coldStartMinPages  minimum resident pages for the pool to count as
     *                           warm; {@code 0} means "at least one page"
     */
    public ReadinessCheck(BooleanSupplier recoveryInProgress, IntSupplier residentPages,
                          int coldStartMinPages) {
        this.recoveryInProgress = recoveryInProgress;
        this.residentPages = residentPages;
        this.coldStartMinPages = coldStartMinPages;
    }

    /**
     * Runs the readiness probe.
     *
     * @return UP when recovery is done and the buffer pool is warm
     */
    public HealthResult check() {
        List<String> reasons = new ArrayList<>(2);
        boolean recoveryOk = !recoveryInProgress.getAsBoolean();
        if (!recoveryOk) {
            reasons.add(REASON_RECOVERY_IN_PROGRESS);
        }
        boolean warm = residentPages.getAsInt() > coldStartMinPages;
        if (!warm) {
            reasons.add(REASON_BUFFER_POOL_COLD);
        }
        boolean up = reasons.isEmpty();
        String body = "{\"status\":\"" + (up ? "UP" : "DOWN") + "\","
                + "\"checks\":{\"recovery\":\"" + (recoveryOk ? "DONE" : "IN_PROGRESS") + "\","
                + "\"bufferPool\":\"" + (warm ? "WARM" : "COLD") + "\"},"
                + "\"reasons\":" + HealthJson.array(reasons) + "}";
        return new HealthResult(up, body, reasons);
    }
}
