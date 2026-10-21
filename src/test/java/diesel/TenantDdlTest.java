package diesel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import diesel.security.Tenant;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Prompt4 #36 acceptance: CREATE TENANT / DROP TENANT SQL end to end through
 * {@link Database#executeQuery}, including system-table DROP protection and
 * restart persistence of the DIESEL_TENANTS mappings.
 */
@Tag("smoke")
class TenantDdlTest {

    private Database database;

    @TempDir
    Path tempDir;

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
            database = null;
        }
    }

    private void open() {
        database = new Database(tempDir.toString());
    }

    @Test
    void createTenantStoresMapping() {
        open();
        Object result = database.executeQuery(
                "CREATE TENANT tenant1 SNI 'tenant1.example.com' "
                + "KEYSTORE 'data/tenant1.p12' PASSWORD 'changeit'", null);
        assertEquals("Tenant created: tenant1", result);
        List<Tenant> tenants = database.getTenantStore().loadAllTenants();
        assertEquals(1, tenants.size());
        Tenant tenant = tenants.get(0);
        assertEquals("tenant1", tenant.name());
        assertEquals("tenant1.example.com", tenant.sniHostname());
        assertEquals("data/tenant1.p12", tenant.keystorePath());
        assertEquals("changeit", tenant.keystorePassword());
    }

    @Test
    void createTenantWithoutPasswordDefaultsToEmpty() {
        open();
        database.executeQuery(
                "CREATE TENANT t2 SNI 't2.example.com' KEYSTORE 'data/t2.p12'", null);
        Tenant tenant = database.getTenantStore().loadAllTenants().get(0);
        assertEquals("", tenant.keystorePassword());
    }

    @Test
    void duplicateTenantNameRejected() {
        open();
        database.executeQuery(
                "CREATE TENANT t1 SNI 'a.example.com' KEYSTORE 'data/a.p12'", null);
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(
                "CREATE TENANT t1 SNI 'b.example.com' KEYSTORE 'data/b.p12'", null));
    }

    @Test
    void duplicateSniHostnameRejected() {
        open();
        database.executeQuery(
                "CREATE TENANT t1 SNI 'same.example.com' KEYSTORE 'data/a.p12'", null);
        assertThrows(IllegalArgumentException.class, () -> database.executeQuery(
                "CREATE TENANT t2 SNI 'same.example.com' KEYSTORE 'data/b.p12'", null));
    }

    @Test
    void malformedSyntaxRejected() {
        open();
        assertThrows(SyntaxErrorException.class, () -> database.executeQuery(
                "CREATE TENANT missing-clauses", null));
        assertThrows(SyntaxErrorException.class, () -> database.executeQuery(
                "DROP TENANT", null));
    }

    @Test
    void dropTenantRemovesMapping() {
        open();
        database.executeQuery(
                "CREATE TENANT t1 SNI 'a.example.com' KEYSTORE 'data/a.p12'", null);
        Object result = database.executeQuery("DROP TENANT t1", null);
        assertEquals("Tenant dropped: t1", result);
        assertFalse(database.getTenantStore().tenantExists("t1"));
        assertThrows(IllegalArgumentException.class,
                () -> database.executeQuery("DROP TENANT t1", null));
    }

    @Test
    void tenantsSurviveRestart() {
        open();
        database.executeQuery(
                "CREATE TENANT tenant1 SNI 'tenant1.example.com' "
                + "KEYSTORE 'data/tenant1.p12' PASSWORD 'pw1'", null);
        database.executeQuery(
                "CREATE TENANT tenant2 SNI 'tenant2.example.com' KEYSTORE 'data/tenant2.p12'", null);
        database.saveTablesToDisk();
        database.close();

        database = new Database(tempDir.toString());
        database.loadTablesFromDisk();
        List<Tenant> tenants = database.getTenantStore().loadAllTenants();
        assertEquals(2, tenants.size());
        assertTrue(database.getTenantStore().tenantExists("tenant1"));
        assertTrue(database.getTenantStore().tenantExists("tenant2"));
        Tenant reloaded = tenants.stream()
                .filter(t -> t.name().equals("tenant1")).findFirst().orElseThrow();
        assertEquals("tenant1.example.com", reloaded.sniHostname());
        assertEquals("pw1", reloaded.keystorePassword());
    }

    @Test
    void tenantsSystemTableCannotBeDropped() {
        open();
        database.executeQuery(
                "CREATE TENANT t1 SNI 'a.example.com' KEYSTORE 'data/a.p12'", null);
        assertThrows(IllegalArgumentException.class,
                () -> database.dropTable("DIESEL_TENANTS"));
        assertThrows(IllegalArgumentException.class,
                () -> database.dropTable("diesel_tenants"));
        assertTrue(database.getTenantStore().tenantExists("t1"),
                "tenant survives the system-table drop attempt");
    }
}
