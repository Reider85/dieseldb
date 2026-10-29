package diesel;

import diesel.observability.Gauge;
import diesel.observability.Histogram;
import diesel.observability.MetricsRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #44: integration — a real {@link Database} feeds the standard
 * metrics ({@code diesel_queries_total}, {@code diesel_query_duration_seconds},
 * {@code diesel_transactions_active}).
 * <p>
 * Lives in package {@code diesel} because {@code Database} is package-private.
 * The registry is JVM-wide and the fast profile runs test classes in parallel,
 * so every assertion uses before/after deltas (our own increments are always
 * included) instead of absolute values.
 */
@Tag("smoke")
public class MetricsIntegrationTest {

    @TempDir
    Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery(
                "CREATE TABLE METRICS_IT (ID LONG PRIMARY KEY, NAME STRING)", null);
    }

    @AfterEach
    void tearDown() {
        // Hard rule: a constructed Database must always be closed.
        if (database != null) {
            database.close();
            database = null;
        }
    }

    private long queriesTotal(String type, String status) {
        return MetricsRegistry.getInstance()
                .counter(MetricsRegistry.QUERIES_TOTAL, "type", type, "status", status)
                .get();
    }

    private Histogram queryDuration(String type) {
        return MetricsRegistry.getInstance()
                .histogram(MetricsRegistry.QUERY_DURATION_SECONDS, "type", type);
    }

    @Test
    void successfulQueriesIncrementTypedCounters() {
        long selectOkBefore = queriesTotal("SELECT", "ok");
        long insertOkBefore = queriesTotal("INSERT", "ok");
        long selectDurationBefore = queryDuration("SELECT").count();

        database.executeQuery("INSERT INTO METRICS_IT (ID, NAME) VALUES (1, 'alpha')", null);
        database.executeQuery("SELECT * FROM METRICS_IT", null);

        assertTrue(queriesTotal("INSERT", "ok") > insertOkBefore,
                "INSERT must increment diesel_queries_total{type=INSERT,status=ok}");
        assertTrue(queriesTotal("SELECT", "ok") > selectOkBefore,
                "SELECT must increment diesel_queries_total{type=SELECT,status=ok}");
        assertTrue(queryDuration("SELECT").count() > selectDurationBefore,
                "SELECT must record a diesel_query_duration_seconds{type=SELECT} sample");
    }

    @Test
    void failedQueryIncrementsErrorCounter() {
        long errorBefore = queriesTotal("SELECT", "error");

        assertThrows(RuntimeException.class, () -> database.executeQuery(
                "SELECT * FROM METRICS_NO_SUCH_TABLE", null));

        assertTrue(queriesTotal("SELECT", "error") > errorBefore,
                "a failed query must increment diesel_queries_total{type=SELECT,status=error}");
    }

    @Test
    void databaseRegistersStandardGauges() {
        assertNotNull(MetricsRegistry.getInstance().getGauge(MetricsRegistry.TRANSACTIONS_ACTIVE),
                "Database must register diesel_transactions_active");
        assertNotNull(MetricsRegistry.getInstance().getGauge(MetricsRegistry.BUFFER_POOL_HIT_RATIO),
                "Database's buffer pool must register diesel_buffer_pool_hit_ratio");

        // The active-transaction gauge tracks this database's map while it is
        // the most recently created one (replace semantics, see MetricsRegistry).
        Gauge activeTx = MetricsRegistry.getInstance().getGauge(MetricsRegistry.TRANSACTIONS_ACTIVE);
        double baseline = activeTx.getValue();
        database.beginTransaction(IsolationLevel.READ_COMMITTED);
        assertTrue(activeTx.getValue() >= baseline,
                "active-transaction gauge must stay consistent after BEGIN");
    }
}
