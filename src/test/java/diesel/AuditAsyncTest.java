package diesel;

import static org.junit.jupiter.api.Assertions.*;

import diesel.security.Authenticator;
import diesel.security.Session;
import diesel.security.UserStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * Async acceptance for the SQL audit log (prompt4 #33): enabling the audit
 * logger never blocks the producing query path, and a large burst of
 * statements completes promptly with every record eventually durable.
 *
 * <p>Queue-overflow drop semantics (graceful drop + metric) are covered at
 * unit level by {@code AuditLoggerTest#queueOverflowDropsGracefullyWithMetric}
 * and {@code AuditLoggerTest#recordNeverBlocksOnFullQueue} where the queue
 * capacity and the backend can be controlled directly.
 */
@Tag("smoke")
@ResourceLock("diesel.audit.enabled")  // shares JVM-global AuditLogger.ENABLED_KEY system property
public class AuditAsyncTest {

    @TempDir
    Path tempDir;
    private Database database;
    private Authenticator authenticator;
    private UserStore userStore;

    @BeforeEach
    void setUp() {
        System.setProperty(AuditTestSupport.ENABLED_KEY, "true");
        database = new Database();
        database.setDataDir(tempDir.toString());
        userStore = database.getUserStore();
        authenticator = new Authenticator(userStore);
    }

    @AfterEach
    void tearDown() {
        database.close();
        System.clearProperty(AuditTestSupport.ENABLED_KEY);
    }

    private Session loginAdmin() {
        userStore.resetAdminPassword();
        Session tempSession = authenticator.authenticate("admin", UserStore.DEFAULT_ADMIN_PASSWORD);
        database.executeQuery("ALTER USER admin PASSWORD 'adminpass'", null, tempSession);
        return authenticator.authenticate("admin", "adminpass");
    }

    @Test
    void auditDoesNotBlockTheMainQueryPath() {
        Session admin = loginAdmin();
        database.executeQuery("CREATE TABLE ASYNC_T (ID INTEGER)", null, admin);
        AuditTestSupport.flush(database);
        int baseline = database.getTable(diesel.security.AuditBackend.AUDIT).getRows().size();

        // A burst of 500 statements must complete fast: the producer only
        // offers to a bounded queue, it never waits for the consumer's
        // table append (which rewrites the whole audit file per batch).
        long start = System.nanoTime();
        for (int i = 0; i < 500; i++) {
            database.executeQuery("INSERT INTO ASYNC_T (ID) VALUES (" + i + ")", null, admin);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < 30_000,
                "500 audited statements must not stall on the audit writer, took " + elapsedMs + "ms");

        // close() performs the final drain; every record must be durable.
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.loadTablesFromDisk();
        List<Map<String, Object>> rows = database
                .getTable(diesel.security.AuditBackend.AUDIT).getRows();
        assertEquals(500, rows.size() - baseline,
                "all 500 records must survive the final drain, got " + (rows.size() - baseline));
    }
}