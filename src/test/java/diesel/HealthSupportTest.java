package diesel;

import diesel.observability.HealthCheck;
import diesel.observability.HealthResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #46: HealthSupport — the {@code diesel}-package facade that wires a
 * real {@link Database} into {@link HealthCheck}.
 * <p>
 * Lives in package {@code diesel} because {@code Database} is package-private
 * (same pattern as MetricsIntegrationTest / PitrSupport). Asserts structural
 * correctness of the JSON document rather than UP/DOWN, because a freshly
 * constructed Database may legitimately be buffer-pool-cold (readiness DOWN).
 */
@Tag("smoke")
public class HealthSupportTest {

    @TempDir
    Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database();
        database.setDataDir(tempDir.toString());
    }

    @AfterEach
    void tearDown() {
        // Hard rule: a constructed Database must always be closed.
        if (database != null) {
            database.close();
            database = null;
        }
    }

    @Test
    void forDatabaseProducesWellFormedCombinedDocument() {
        HealthCheck health = HealthSupport.forDatabase(database);
        HealthResult combined = health.combined();

        String body = combined.body();
        assertTrue(body.contains("\"status\":"), "must carry status: " + body);
        assertTrue(body.contains("\"uptime\":"), "must carry uptime: " + body);
        assertTrue(body.contains("\"activeTx\":"), "must carry activeTx: " + body);
        assertTrue(body.contains("\"diskFree\":"), "must carry diskFree: " + body);
        assertTrue(body.contains("\"walLag\":"), "must carry walLag: " + body);
        assertTrue(body.contains("\"checks\":"), "must carry sub-checks: " + body);
        assertTrue(body.contains("\"reasons\":"), "must carry reasons: " + body);
        assertNotNull(combined.reasons());
    }

    @Test
    void liveAndReadyViewsAreWellFormed() {
        HealthCheck health = HealthSupport.forDatabase(database);

        HealthResult live = health.live();
        assertTrue(live.body().contains("\"walWriter\":"),
                "liveness body must carry walWriter check: " + live.body());
        assertTrue(live.body().contains("\"heap\":"),
                "liveness body must carry heap check: " + live.body());

        HealthResult ready = health.ready();
        assertTrue(ready.body().contains("\"recovery\":"),
                "readiness body must carry recovery check: " + ready.body());
        assertTrue(ready.body().contains("\"bufferPool\":"),
                "readiness body must carry bufferPool check: " + ready.body());
    }

    @Test
    void afterCatalogPersistThePoolIsWarmAndLiveIsUp() {
        // A fresh Database has a cold pool (0 resident pages). Allocating a
        // page through the PageManager inserts a frame into the buffer pool,
        // so the cold-start guard must clear and readiness must go UP.
        HealthCheck health = HealthSupport.forDatabase(database);
        assertTrue(!health.ready().up(), "fresh database starts cold: " + health.ready().body());

        try {
            database.getPageManager().allocatePage(0);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

        HealthResult ready = health.ready();
        assertTrue(ready.up(), "after a page load the pool must be warm: " + ready.body());

        HealthResult live = health.live();
        assertTrue(live.up(), "a running database must be live: " + live.body());
    }
}
