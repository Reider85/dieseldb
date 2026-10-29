package diesel;

import diesel.observability.Counter;
import diesel.observability.Histogram;
import diesel.observability.MetricsRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #45 acceptance: metric-recording overhead must stay below 1% of
 * query throughput ("Latency overhead metrics &lt; 1% throughput, verified via
 * benchmark").
 *
 * <p>Method: the recording path is measured in isolation with exactly the
 * calls {@link Database#recordQueryMetrics} performs (counter lookup by
 * name+labels + increment, histogram lookup + recordNanos), then compared
 * against the wall time of a representative cached SELECT going through the
 * real {@code executeQuery} pipeline. The query figure includes its own
 * metric recording (recording is unconditionally on the hot path), so the
 * measured ratio is a faithful upper bound of what recording costs on
 * throughput.
 *
 * <p>Both measurements take the minimum of several rounds: the minimum is the
 * least noisy statistic on a shared machine (precedent: min-of-3 microbench
 * style used by the WAL readAll benchmark), and the perf profile already runs
 * forked and sequential. Numbers are printed so a red run is diagnosable
 * without a re-run.
 */
@Tag("perf")
public class MetricsRecordingOverheadTest {

    private static final int METRICS_OPS = 100_000;
    private static final int QUERY_OPS = 1_000;
    private static final int ROUNDS = 5;
    /** Isolated recording cost allowed per query, as 1/100 of the query time (the <1% criterion). */
    private static final double MAX_OVERHEAD_RATIO = 0.01;

    @TempDir
    Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery(
                "CREATE TABLE OVERHEAD_BENCH (ID LONG PRIMARY KEY, NAME STRING)", null);
        database.executeQuery("INSERT INTO OVERHEAD_BENCH (ID, NAME) VALUES (1, 'row1')", null);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
    }

    @Test
    void metricRecordingCostIsBelowOnePercentOfQueryCost() {
        warmUp();

        double metricsNanosPerOp = bestOfRounds(ROUNDS, METRICS_OPS, this::recordMetricsOnce);
        double queryNanosPerOp = bestOfRounds(ROUNDS, QUERY_OPS, this::runQueryOnce);

        double ratio = metricsNanosPerOp / queryNanosPerOp;
        System.out.printf(java.util.Locale.ROOT,
                "[overhead] metrics recording %.0f ns/op, query %.0f ns/op, ratio %.4f%%%n",
                metricsNanosPerOp, queryNanosPerOp, ratio * 100);

        assertTrue(ratio < MAX_OVERHEAD_RATIO,
                String.format(java.util.Locale.ROOT,
                        "metric recording overhead %.4f%% >= %.0f%% "
                                + "(metrics=%.0f ns/op, query=%.0f ns/op)",
                        ratio * 100, MAX_OVERHEAD_RATIO * 100,
                        metricsNanosPerOp, queryNanosPerOp));
    }

    // ─── Workload ───────────────────────────────────────────────────

    /** Mirrors {@code Database.recordQueryMetrics}: label building, CHM lookup, increment, record. */
    private void recordMetricsOnce() {
        MetricsRegistry registry = MetricsRegistry.getInstance();
        Counter counter = registry.counter(
                MetricsRegistry.QUERIES_TOTAL, "type", "SELECT", "status", "ok");
        counter.increment();
        Histogram histogram = registry.histogram(
                MetricsRegistry.QUERY_DURATION_SECONDS, "type", "SELECT");
        histogram.recordNanos(12_345L);
    }

    private void runQueryOnce() {
        database.executeQuery("SELECT * FROM OVERHEAD_BENCH WHERE ID = 1", null);
    }

    private void warmUp() {
        for (int i = 0; i < 20_000; i++) {
            recordMetricsOnce();
        }
        for (int i = 0; i < 200; i++) {
            runQueryOnce();
        }
    }

    /**
     * Runs {@code rounds} rounds of {@code ops} iterations of {@code workload},
     * warming the JIT with one unrecorded round first, and returns the best
     * (lowest) nanoseconds-per-operation across recorded rounds.
     */
    private static double bestOfRounds(int rounds, int ops, Runnable workload) {
        for (int i = 0; i < ops; i++) {
            workload.run();
        }
        double best = Double.MAX_VALUE;
        for (int round = 0; round < rounds; round++) {
            long start = System.nanoTime();
            for (int i = 0; i < ops; i++) {
                workload.run();
            }
            double perOp = (System.nanoTime() - start) / (double) ops;
            if (perOp < best) {
                best = perOp;
            }
        }
        return best;
    }
}
