package diesel.observability;

import java.util.List;

/**
 * Outcome of a single health probe (prompt4 #46, R3-012 step 3/3).
 *
 * <p>Carries both the verdict that drives the HTTP status code ({@code 200} when
 * up, {@code 503} when down) and the pre-rendered JSON body, so the HTTP layer
 * stays a thin transport shim with no knowledge of individual checks. The
 * {@link #reasons()} list is kept alongside the body so the combined
 * {@code /health} document can merge the sub-probe failures without parsing
 * JSON.
 *
 * @param up      true when the probe is healthy
 * @param body    the JSON document describing this probe's outcome
 * @param reasons machine-readable failure reasons; empty when {@code up}
 */
public record HealthResult(boolean up, String body, List<String> reasons) {

    /**
     * Normalises the reason list to an immutable snapshot.
     *
     * @param up      true when healthy
     * @param body    the JSON document
     * @param reasons failure reasons (null treated as empty)
     */
    public HealthResult {
        reasons = reasons == null ? List.of() : List.copyOf(reasons);
    }
}
