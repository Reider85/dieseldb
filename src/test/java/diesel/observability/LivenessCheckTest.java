package diesel.observability;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #46: LivenessCheck — dead WAL writer or OOM-imminent heap reports DOWN.
 * <p>
 * Supplier-based (no real Database/WALWriter needed): the acceptance scenario
 * "kill WAL writer thread → status=DOWN" is modelled by the walWriterAlive
 * supplier returning false, which is exactly what
 * {@code WALWriter.isWriterAlive()} returns after the thread exits.
 */
@Tag("smoke")
public class LivenessCheckTest {

    @Test
    void allHealthyReportsUp() {
        LivenessCheck check = new LivenessCheck(() -> true, true, () -> 50.0, 5.0);
        HealthResult result = check.check();
        assertTrue(result.up(), "alive writer + healthy heap must be UP");
        assertTrue(result.reasons().isEmpty());
        assertTrue(result.body().contains("\"status\":\"UP\""));
    }

    @Test
    void deadWalWriterReportsDown() {
        // Acceptance: kill WAL writer thread → status=DOWN.
        LivenessCheck check = new LivenessCheck(() -> false, true, () -> 50.0, 5.0);
        HealthResult result = check.check();
        assertFalse(result.up(), "dead WAL writer must be DOWN");
        assertTrue(result.reasons().contains(LivenessCheck.REASON_WAL_WRITER_DEAD),
                "reason must be wal-writer-dead: " + result.reasons());
        assertTrue(result.body().contains("\"walWriter\":\"DOWN\""));
        assertTrue(result.body().contains("\"status\":\"DOWN\""));
    }

    @Test
    void lowFreeHeapReportsOomImminent() {
        LivenessCheck check = new LivenessCheck(() -> true, true, () -> 1.0, 5.0);
        HealthResult result = check.check();
        assertFalse(result.up(), "free heap below floor must be DOWN");
        assertTrue(result.reasons().contains(LivenessCheck.REASON_OOM_IMMINENT),
                "reason must be oom-imminent: " + result.reasons());
        assertTrue(result.body().contains("\"heap\":\"DOWN\""));
    }

    @Test
    void walDisabledSkipsWriterCheck() {
        // A missing/dead writer while WAL is disabled is not evidence of death.
        LivenessCheck check = new LivenessCheck(() -> false, false, () -> 50.0, 5.0);
        HealthResult result = check.check();
        assertTrue(result.up(), "WAL-disabled must skip the writer check");
        assertTrue(result.reasons().isEmpty());
        assertTrue(result.body().contains("\"walWriter\":\"UP\""));
    }

    @Test
    void bothFailuresReportBothReasons() {
        LivenessCheck check = new LivenessCheck(() -> false, true, () -> 0.5, 5.0);
        HealthResult result = check.check();
        assertFalse(result.up());
        assertTrue(result.reasons().contains(LivenessCheck.REASON_WAL_WRITER_DEAD));
        assertTrue(result.reasons().contains(LivenessCheck.REASON_OOM_IMMINENT));
    }
}
