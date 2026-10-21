package diesel.security;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * CRUD over the {@code DIESEL_TENANTS} system table (prompt4 #36,
 * R3-009 step 3/3). Maps SNI hostnames to per-tenant TLS keystores for
 * multi-tenant deployments; the {@code SniSelector} loads the mappings at
 * server startup.
 *
 * <p>All column values are Strings (UUID/timestamp encoded) so every storage
 * format round-trips them identically.
 */
public final class TenantStore {

    private final TenantStoreBackend backend;

    public TenantStore(TenantStoreBackend backend) {
        this.backend = Objects.requireNonNull(backend, "backend");
    }

    /**
     * Creates a tenant mapping.
     *
     * @throws IllegalArgumentException when a field is blank or a tenant with
     *                                  the same name or SNI hostname exists
     */
    public Tenant createTenant(String name, String sniHostname, String keystorePath, String password) {
        requireNotBlank(name, "Tenant name");
        requireNotBlank(sniHostname, "SNI hostname");
        requireNotBlank(keystorePath, "Keystore path");
        String storedPassword = password == null ? "" : password;
        for (Tenant existing : loadAllTenants()) {
            if (existing.name().equalsIgnoreCase(name)) {
                throw new IllegalArgumentException("Tenant '" + name + "' already exists");
            }
            if (existing.sniHostname().equalsIgnoreCase(sniHostname)) {
                throw new IllegalArgumentException(
                        "SNI hostname '" + sniHostname + "' is already mapped to tenant '"
                                + existing.name() + "'");
            }
        }
        Tenant tenant = new Tenant(UUID.randomUUID(), name, sniHostname, keystorePath,
                storedPassword, LocalDateTime.now());
        backend.appendRow(tenantRow(tenant));
        return tenant;
    }

    /**
     * Drops a tenant by name.
     *
     * @throws IllegalArgumentException when no such tenant exists
     */
    public void dropTenant(String name) {
        requireNotBlank(name, "Tenant name");
        List<Tenant> all = loadAllTenants();
        for (Tenant tenant : all) {
            if (tenant.name().equalsIgnoreCase(name)) {
                backend.deleteRow(tenant.id().toString());
                return;
            }
        }
        throw new IllegalArgumentException("Tenant '" + name + "' does not exist");
    }

    /** Whether a tenant with the given name exists (case-insensitive). */
    public boolean tenantExists(String name) {
        if (name == null || name.isBlank()) {
            return false;
        }
        for (Tenant tenant : loadAllTenants()) {
            if (tenant.name().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    /** Loads every tenant row from the system table. */
    public List<Tenant> loadAllTenants() {
        List<Tenant> tenants = new ArrayList<>();
        for (Map<String, Object> row : backend.readAll()) {
            tenants.add(tenantFromRow(row));
        }
        return tenants;
    }

    /* ------------------------------------------------------------------ */
    /*  Row encoding — all values are Strings for storage-format safety   */
    /* ------------------------------------------------------------------ */

    static Map<String, Object> tenantRow(Tenant tenant) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", tenant.id().toString());
        row.put("NAME", tenant.name());
        row.put("SNI_HOSTNAME", tenant.sniHostname());
        row.put("KEYSTORE_PATH", tenant.keystorePath());
        row.put("KEYSTORE_PASSWORD", tenant.keystorePassword());
        row.put("CREATED_AT", tenant.createdAt().toString());
        return row;
    }

    static Tenant tenantFromRow(Map<String, Object> row) {
        return new Tenant(
                UUID.fromString(String.valueOf(row.get("ID"))),
                String.valueOf(row.get("NAME")),
                String.valueOf(row.get("SNI_HOSTNAME")),
                String.valueOf(row.get("KEYSTORE_PATH")),
                String.valueOf(row.getOrDefault("KEYSTORE_PASSWORD", "")),
                LocalDateTime.parse(String.valueOf(row.get("CREATED_AT"))));
    }

    /** Normalizes an SNI hostname for case-insensitive matching. */
    public static String normalizeHostname(String hostname) {
        return hostname == null ? "" : hostname.trim().toLowerCase(Locale.ROOT);
    }

    private static void requireNotBlank(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
    }
}
