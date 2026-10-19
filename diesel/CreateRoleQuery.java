package diesel;

/**
 * Executes a CREATE ROLE statement: creates a role through the database's
 * user store (prompt4 #30, R3-008 step 1/4).
 *
 * @see Query
 * @see diesel.security.UserStore
 */
class CreateRoleQuery implements Query<String> {
    private final String roleName;

    /** @param roleName the role name, case-preserved */
    public CreateRoleQuery(String roleName) {
        this.roleName = roleName;
    }

    public String getRoleName() {
        return roleName;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeCreateRole(this);
    }
}
