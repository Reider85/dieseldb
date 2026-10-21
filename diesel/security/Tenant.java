package diesel.security;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * A TLS multi-tenant mapping: SNI hostname → tenant keystore
 * (prompt4 #36, R3-009 step 3/3). Stored in the {@code DIESEL_TENANTS}
 * system table; all column values are Strings for storage-format safety.
 *
 * @param id               row identifier
 * @param name             tenant name (unique, case-preserved)
 * @param sniHostname      hostname clients send via SNI (matched case-insensitively)
 * @param keystorePath     filesystem path to the tenant's PKCS12/JKS keystore
 * @param keystorePassword keystore password (may be empty, never logged)
 * @param createdAt        creation timestamp
 */
public record Tenant(
        UUID id,
        String name,
        String sniHostname,
        String keystorePath,
        String keystorePassword,
        LocalDateTime createdAt) {
}
