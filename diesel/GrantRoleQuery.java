package diesel;

/**
 * Executes a GRANT role TO user statement: links an existing role to an
 * existing user through the database's user store (prompt4 #30, R3-008 step
 * 1/4). Table-level GRANT ... ON ... arrives in step 3 (#32).
 *
 * @see Query
 * @see diesel.security.UserStore
 */
class GrantRoleQuery implements Query<String> {
    private final String roleName;
    private final String userName;

    /**
     * @param roleName the role to grant, case-preserved
     * @param userName the receiving user, case-preserved
     */
    public GrantRoleQuery(String roleName, String userName) {
        this.roleName = roleName;
        this.userName = userName;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeGrantRole(this);
    }
}
