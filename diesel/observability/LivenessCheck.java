package diesel.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

/**
 * Liveness probe (prompt4 #46, R3-012 step 3/3): answers "is this process still
 * able to make progress, or should Kubernetes restart it?".
 *
 * <p>Two failure modes are detected:
 * <ul>
 *   <li><b>Dead WAL writer</b> — the background thread that drains the WAL queue
 *       has exited (crash or an unrecoverable I/O error). Without it every
 *       commit stalls forever, so the only remedy is a restart.</li>
 *   <li><b>OOM imminent</b> — free heap fell below the configured percentage of
 *       {@code -Xmx}. A JVM that is nearly out of heap is about to throw
 *       {@link OutOfMemoryError}; restarting is safer than serving failures.</li>
 * </ul>
 *
 * <p>Deliberately independent of recovery progress and buffer-pool warmth:
 * those are readiness concerns ({@link ReadinessCheck}), not liveness ones — a
 * process that is merely still warming up must not be killed.
 *
 * <p>All inputs are suppliers so the check is cheap and lock-free on every
 * probe (Kubernetes may poll it every few seconds) and trivially testable
 * without a real database.
 */
public final class LivenessCheck {

    /** Reason emitted when the WAL writer thread is dead. */
    public static final String REASON_WAL_WRITER_DEAD = "wal-writer-dead";
    /** Reason emitted when free heap is below the OOM threshold. */
    public static final String REASON_OOM_IMMINENT = "oom-imminent";

    private final BooleanSupplier walWriterAlive;
    private final boolean walWriterApplicable;
    private final DoubleSupplier freeHeapPercent;
    private final double oomFreeHeapPercent;

    /**
     * Creates a liveness check.
     *
     * @param walWriterAlive      true while the WAL writer thread is alive
     * @param walWriterApplicable false when the WAL is disabled (the check is
     *                            then skipped instead of reporting DOWN)
     * @param freeHeapPercent     current free heap as a percentage of max heap
     * @param oomFreeHeapPercent  free-heap floor; below it the JVM is treated
     *                            as OOM-imminent
     */
    public LivenessCheck(BooleanSupplier walWriterAlive, boolean walWriterApplicable,
                         DoubleSupplier freeHeapPercent, double oomFreeHeapPercent) {
        this.walWriterAlive = walWriterAlive;
        this.walWriterApplicable = walWriterApplicable;
        this.freeHeapPercent = freeHeapPercent;
        this.oomFreeHeapPercent = oomFreeHeapPercent;
    }

    /**
     * Runs the liveness probe.
     *
     * @return UP unless the WAL writer is dead or the JVM is OOM-imminent
     */
    public HealthResult check() {
        List<String> reasons = new ArrayList<>(2);
        boolean walOk = !walWriterApplicable || walWriterAlive.getAsBoolean();
        if (!walOk) {
            reasons.add(REASON_WAL_WRITER_DEAD);
        }
        boolean heapOk = freeHeapPercent.getAsDouble() >= oomFreeHeapPercent;
        if (!heapOk) {
            reasons.add(REASON_OOM_IMMINENT);
        }
        boolean up = reasons.isEmpty();
        String body = "{\"status\":\"" + (up ? "UP" : "DOWN") + "\","
                + "\"checks\":{\"walWriter\":\"" + (walOk ? "UP" : "DOWN") + "\","
                + "\"heap\":\"" + (heapOk ? "UP" : "DOWN") + "\"},"
                + "\"reasons\":" + HealthJson.array(reasons) + "}";
        return new HealthResult(up, body, reasons);
    }
}
