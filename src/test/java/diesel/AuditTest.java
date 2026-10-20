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

/**
 * Acceptance test for the SQL audit log (prompt4 #33): 100 executed
 * statements produce exactly 100 DIESEL_AUDIT_LOG rows with the correct
 * user, query text, status and duration.
 *
 * <p>Runs against an embedded database with {@code audit.enabled=true}
 * injected as a system property before the logger is first resolved.
 */
@Tag("smoke")
public class AuditTest {

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
    void hundredQueriesProduceHundredAuditRows() {
        Session admin = loginAdmin();
        database.executeQuery("CREATE TABLE AUDIT_T (ID INTEGER, NAME STRING)", null, admin);
        // Baseline: setup statements (ALTER USER, CREATE TABLE) are already
        // audited; the acceptance criterion counts the next 100 statements.
        AuditTestSupport.flush(database);
        int baseline = AuditTestSupport.auditRows(database).size();

        // 100 statements: 90 INSERTs + 9 SELECTs + 1 failing statement.
        for (int i = 0; i < 90; i++) {
            database.executeQuery("INSERT INTO AUDIT_T (ID, NAME) VALUES (" + i + ", 'n" + i + "')", null, admin);
        }
        for (int i = 0; i < 9; i++) {
            database.executeQuery("SELECT ID FROM AUDIT_T WHERE ID = " + i, null, admin);
        }
        assertThrows(RuntimeException.class,
                () -> database.executeQuery("SELECT NOPE FROM MISSING_TABLE", null, admin));

        AuditTestSupport.flush(database);

        List<Map<String, Object>> rows = AuditTestSupport.auditRows(database);
        assertEquals(100, rows.size() - baseline,
                "100 executed statements must produce 100 audit rows, got " + (rows.size() - baseline));

        long okCount = 0;
        long errorCount = 0;
        for (Map<String, Object> row : rows) {
            assertEquals("admin", row.get("USER_NAME"), "user must be recorded");
            assertNotNull(row.get("EVENT_TIME"), "event time must be recorded");
            assertNotNull(row.get("QUERY"), "query must be recorded");
            assertNotNull(row.get("SOURCE_IP"), "source ip must be recorded");
            long duration = Long.parseLong(String.valueOf(row.get("DURATION_MS")));
            assertTrue(duration >= 0, "duration must be >= 0, was " + duration);
            String status = String.valueOf(row.get("STATUS"));
            if (AuditTestSupport.STATUS_OK.equals(status)) {
                okCount++;
            } else if (AuditTestSupport.STATUS_ERROR.equals(status)) {
                errorCount++;
            } else {
                fail("unexpected status: " + status);
            }
        }
        assertEquals(1, errorCount, "exactly one failing statement among the 100");
        assertTrue(okCount >= 99, "at least 99 OK statements, got " + okCount);
    }

    @Test
    void deniedQueryIsAuditedAsError() {
        userStore.createUser("bob", "secret");
        userStore.createRole("reader");
        userStore.grantRoleToUser("bob", "reader");
        Session admin = loginAdmin();
        database.executeQuery("CREATE TABLE SEC_T (ID INTEGER)", null, admin);
        Session bob = authenticator.authenticate("bob", "secret");

        assertThrows(diesel.security.AccessDeniedException.class,
                () -> database.executeQuery("SELECT ID FROM SEC_T", null, bob));

        AuditTestSupport.flush(database);
        List<Map<String, Object>> rows = AuditTestSupport.auditRows(database);
        boolean denied = rows.stream().anyMatch(r ->
                "bob".equals(r.get("USER_NAME"))
                        && AuditTestSupport.STATUS_ERROR.equals(r.get("STATUS"))
                        && String.valueOf(r.get("QUERY")).contains("SEC_T"));
        assertTrue(denied, "a denied query must be recorded as an ERROR audit event");
    }

    @Test
    void sessionSourceIpIsRecorded() {
        Session admin = loginAdmin();
        admin.setSourceIp("203.0.113.7");
        database.executeQuery("CREATE TABLE IP_T (ID INTEGER)", null, admin);

        AuditTestSupport.flush(database);
        List<Map<String, Object>> rows = AuditTestSupport.auditRows(database);
        boolean found = rows.stream().anyMatch(r ->
                "203.0.113.7".equals(r.get("SOURCE_IP"))
                        && String.valueOf(r.get("QUERY")).contains("IP_T"));
        assertTrue(found, "the session's source IP must land in SOURCE_IP");
    }

    @Test
    void embeddedQueriesAreAuditedAsEmbeddedUser() {
        database.close();
        database = new Database();
        database.setDataDir(tempDir.toString());
        database.executeQuery("CREATE TABLE EMB_T (ID INTEGER)", null);

        AuditTestSupport.flush(database);
        List<Map<String, Object>> rows = AuditTestSupport.auditRows(database);
        boolean found = rows.stream().anyMatch(r ->
                "embedded".equals(r.get("USER_NAME"))
                        && String.valueOf(r.get("QUERY")).contains("EMB_T"));
        assertTrue(found, "embedded queries must be audited with user 'embedded'");
    }

    @Test
    void subqueryDoesNotProduceSeparateAuditRow() {
        Session admin = loginAdmin();
        database.executeQuery("CREATE TABLE OUTER_T (ID INTEGER)", null, admin);
        database.executeQuery("INSERT INTO OUTER_T (ID) VALUES (1)", null, admin);
        database.executeQuery("INSERT INTO OUTER_T (ID) VALUES (2)", null, admin);

        AuditTestSupport.flush(database);
        int before = AuditTestSupport.auditRows(database).size();
        database.executeQuery(
                "SELECT ID FROM OUTER_T WHERE ID IN (SELECT ID FROM OUTER_T)", null, admin);
        AuditTestSupport.flush(database);
        List<Map<String, Object>> rows = AuditTestSupport.auditRows(database);
        // Exactly one new row for the outer statement; the inner subquery
        // re-entry is suppressed by the AUDIT_INNER depth guard.
        assertEquals(before + 1, rows.size(),
                "a subquery must not produce a separate audit record");
    }
}