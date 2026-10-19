package diesel.security;

import java.util.Objects;
import java.util.UUID;

/**
 * A privilege granted to a role: an action on a table, or on the whole
 * database when {@code tableName} is empty. Immutable value object
 * (prompt4 #30, R3-008 step 1/4). GRANT/REVOKE SQL and enforcement arrive
 * in step 3 (#32); this step only defines the model and the
 * {@code DIESEL_PRIVILEGES} storage.
 */
public final class Privilege {

    /** The action a privilege authorizes. ADMIN is the meta-privilege. */
    public enum Action {
        SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ADMIN
    }

    private final UUID id;
    private final String roleName;
    private final Action action;
    private final String tableName;

    /**
     * @param tableName the target table, or an empty string for a
     *                  database-wide privilege
     */
    public Privilege(UUID id, String roleName, Action action, String tableName) {
        this.id = Objects.requireNonNull(id, "id");
        this.roleName = Objects.requireNonNull(roleName, "roleName");
        this.action = Objects.requireNonNull(action, "action");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
    }

    public UUID getId() {
        return id;
    }

    public String getRoleName() {
        return roleName;
    }

    public Action getAction() {
        return action;
    }

    public String getTableName() {
        return tableName;
    }

    /** True when this privilege applies to the whole database, not one table. */
    public boolean isDatabaseWide() {
        return tableName.isEmpty();
    }

    @Override
    public String toString() {
        return "Privilege{id=" + id + ", role='" + roleName + "', action=" + action
                + ", table='" + tableName + "'}";
    }
}
