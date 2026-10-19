package diesel.security;

import java.util.List;
import java.util.Map;

/**
 * Storage bridge for the RBAC system tables (prompt4 #30). Implemented by the
 * engine over the {@code DIESEL_USERS} / {@code DIESEL_ROLES} /
 * {@code DIESEL_USER_ROLES} / {@code DIESEL_PRIVILEGES} tables; unit tests may
 * supply an in-memory fake. Keeping this interface free of engine types
 * (Database/Table are package-private) lets {@link UserStore} live in this
 * package.
 */
public interface UserStoreBackend {

    /** System table names the backend must understand. */
    String USERS = "DIESEL_USERS";
    String ROLES = "DIESEL_ROLES";
    String USER_ROLES = "DIESEL_USER_ROLES";
    String PRIVILEGES = "DIESEL_PRIVILEGES";

    /**
     * Returns all rows of the given system table as column-value maps.
     *
     * @param tableName one of the constants in this interface
     * @return the row list, empty when the table has no rows
     */
    List<Map<String, Object>> readAll(String tableName);

    /**
     * Appends a row to the given system table and makes it durable.
     *
     * @param tableName one of the constants in this interface
     * @param row       the column-value mapping; all values are Strings
     */
    void appendRow(String tableName, Map<String, Object> row);

    /**
     * Replaces the row carrying the same {@code ID} and makes it durable
     * (prompt4 #31 password changes).
     *
     * @param tableName one of the constants in this interface
     * @param row       the full replacement row; all values are Strings
     * @throws IllegalStateException when no row with that ID exists
     */
    void updateRow(String tableName, Map<String, Object> row);
}
