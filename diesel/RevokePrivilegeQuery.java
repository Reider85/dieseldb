package diesel;

import diesel.security.Privilege;
import java.util.List;

/**
 * Executes a REVOKE privilege FROM role statement: revokes one or more
 * privileges from an existing role (prompt4 #32, R3-008 step 3/4).
 */
class RevokePrivilegeQuery implements Query<String> {
    private final String roleName;       // case-preserved
    private final List<Privilege.Action> actions;
    private final String tableName;      // empty = database-wide

    public RevokePrivilegeQuery(String roleName, Privilege.Action action, String tableName) {
        this(roleName, List.of(action), tableName);
    }

    public RevokePrivilegeQuery(String roleName, List<Privilege.Action> actions, String tableName) {
        this.roleName = roleName;
        this.actions = List.copyOf(actions);
        this.tableName = tableName;
    }

    public String getRoleName() {
        return roleName;
    }

    /** Returns the first revoked action (single-action convenience). */
    public Privilege.Action getAction() {
        return actions.get(0);
    }

    /** Returns every revoked action (comma-separated REVOKE lists). */
    public List<Privilege.Action> getActions() {
        return actions;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String execute(Table table) {
        return table.getDatabase().executeRevokePrivilege(this);
    }
}
