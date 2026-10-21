package diesel;

/**
 * Executes a CREATE TENANT statement: stores an SNI hostname → keystore
 * mapping for multi-tenant TLS (prompt4 #36, R3-009 step 3/3).
 *
 * @see Query
 * @see diesel.security.TenantStore
 */
class CreateTenantQuery implements Query<String> {
    private final String tenantName;
    private final String sniHostname;
    private final String keystorePath;
    private final String keystorePassword;

    public CreateTenantQuery(String tenantName, String sniHostname, String keystorePath,
            String keystorePassword) {
        this.tenantName = tenantName;
        this.sniHostname = sniHostname;
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
    }

    public String getTenantName() {
        return tenantName;
    }

    public String getSniHostname() {
        return sniHostname;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    /**
     * Returns the keystore password (may be empty). Only for
     * parser/dispatch plumbing — never log it.
     */
    public String getKeystorePassword() {
        return keystorePassword;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeCreateTenant(this);
    }
}
