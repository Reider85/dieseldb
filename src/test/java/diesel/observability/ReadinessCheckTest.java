package diesel.observability;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #46: ReadinessCheck — recovery-in-progress or cold buffer pool keeps
 * the instance out of the load-balancer rotation (DOWN).
 * <p>
 * Supplier-based (no real Database needed): "during recovery startup →
 * /health/ready=DOWN, after recovery → UP" is modelled by the
 * recoveryInProgress supplier.
 */
@Tag("smoke")
public class ReadinessCheckTest {

    @Test
    void recoveryDoneAndWarmPoolReportsUp() {
        ReadinessCheck check = new ReadinessCheck(() -> false, () -> 5, 0);
        HealthResult result = check.check();
        assertTrue(result.up(), "recovery done + warm pool must be UP");
        assertTrue(result.reasons().isEmpty());
        assertTrue(result.body().contains("\"recovery\":\"DONE\""));
        assertTrue(result.body().contains("\"bufferPool\":\"WARM\""));
    }

    @Test
    void recoveryInProgressReportsDown() {
        // Acceptance: during recovery startup → /health/ready=DOWN.
        ReadinessCheck check = new ReadinessCheck(() -> true, () -> 5, 0);
        HealthResult result = check.check();
        assertFalse(result.up(), "recovery in progress must be DOWN");
        assertTrue(result.reasons().contains(ReadinessCheck.REASON_RECOVERY_IN_PROGRESS),
                "reason must be recovery-in-progress: " + result.reasons());
        assertTrue(result.body().contains("\"recovery\":\"IN_PROGRESS\""));
        assertTrue(result.body().contains("\"status\":\"DOWN\""));
    }

    @Test
    void coldBufferPoolReportsDown() {
        ReadinessCheck check = new ReadinessCheck(() -> false, () -> 0, 0);
        HealthResult result = check.check();
        assertFalse(result.up(), "zero resident pages must be DOWN (cold)");
        assertTrue(result.reasons().contains(ReadinessCheck.REASON_BUFFER_POOL_COLD),
                "reason must be buffer-pool-cold: " + result.reasons());
        assertTrue(result.body().contains("\"bufferPool\":\"COLD\""));
    }

    @Test
    void coldStartMinPagesThresholdIsRespected() {
        // 5 resident pages, but the operator demands 10 → still cold.
        ReadinessCheck check = new ReadinessCheck(() -> false, () -> 5, 10);
        assertFalse(check.check().up(), "resident < min must be cold");

        // 15 resident pages with the same threshold → warm.
        ReadinessCheck warm = new ReadinessCheck(() -> false, () -> 15, 10);
        assertTrue(warm.check().up(), "resident >= min must be warm");
    }

    @Test
    void bothFailuresReportBothReasons() {
        ReadinessCheck check = new ReadinessCheck(() -> true, () -> 0, 0);
        HealthResult result = check.check();
        assertFalse(result.up());
        assertTrue(result.reasons().contains(ReadinessCheck.REASON_RECOVERY_IN_PROGRESS));
        assertTrue(result.reasons().contains(ReadinessCheck.REASON_BUFFER_POOL_COLD));
    }
}
