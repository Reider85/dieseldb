package diesel;

import diesel.storage.avro.AdaptiveCompressionManager;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * O2 Avro-4 acceptance: {@code AdaptiveCompressionManager.analyzeDataPattern}
 * must run in O(sample x row_size), not O(sample^2 x row_size).
 *
 * <p>The old implementation re-scanned the whole sample with
 * {@code countMatches} for every string value occurrence (only gated by the
 * {@code <= 8 distinct strings} check), giving quadratic behaviour; the fix
 * builds one frequency table in a single pass and reads counts from it in O(1).
 *
 * <p>The fixture keeps the distinct-string gate open (4 constants) so the
 * quadratic path would actually engage: at sample=100, row_size=100 the old
 * shape performs ~10^8 value comparisons per call (identity-fast String.equals
 * over shared constants: hundreds of milliseconds per call), while the fixed
 * shape does 2 x 10^4 visits. Twenty measured repetitions with a 1000ms bound
 * separate the two shapes by more than an order of magnitude in both
 * directions.
 */
@Tag("storage")
@StorageType("avro")
class AnalyzeDataPatternIsOSampleTest {

    private static final int SAMPLE = 100;
    private static final int ROW_SIZE = 100;
    private static final int REPS = 20;

    /** 100 rows x 100 cells drawn from 4 shared string constants. */
    private static List<Object[]> constantRows() {
        String[] constants = {"alpha", "beta", "gamma", "delta"};
        List<Object[]> rows = new ArrayList<>(SAMPLE);
        for (int i = 0; i < SAMPLE; i++) {
            Object[] row = new Object[ROW_SIZE];
            for (int j = 0; j < ROW_SIZE; j++) {
                row[j] = constants[j % constants.length];
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Spec shape (sample=100, row_size=100): every repetition must classify
     * the constant-heavy fixture as {@code repetitive} (correctness) and the
     * whole measured batch must stay inside the linear-time bound
     * (regression guard for the removed countMatches re-scan).
     */
    @Test
    @Timeout(60)
    void constantStringSampleStaysLinearAcrossRepetitions() {
        List<Object[]> rows = constantRows();

        // Unmeasured warmup (JIT, HashMap resize behaviour).
        assertEquals("repetitive", AdaptiveCompressionManager.analyzeDataPattern(rows));

        long start = System.nanoTime();
        for (int rep = 0; rep < REPS; rep++) {
            assertEquals("repetitive",
                    AdaptiveCompressionManager.analyzeDataPattern(rows),
                    "repetition " + rep + " must classify constants as repetitive");
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(elapsedMillis < 1_000,
                REPS + " x analyzeDataPattern(sample=" + SAMPLE + ", row_size="
                        + ROW_SIZE + ") took " + elapsedMillis
                        + " ms, expected < 1000 ms (O(sample x row_size), o2 Avro-4)");
    }
}
