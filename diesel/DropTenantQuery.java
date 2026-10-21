package diesel;

/**
 * Executes a DROP TENANT statement: removes an SNI tenant mapping
 * (prompt4 #36, R3-009 step 3/3).
 *
 * @see Query
 * @see diesel.security.TenantStore
 */
class DropTenantQuery implements Query<String> {
    private final String tenantName;

    public DropTenantQuery(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getTenantName() {
        return tenantName;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeDropTenant(this);
    }
}
