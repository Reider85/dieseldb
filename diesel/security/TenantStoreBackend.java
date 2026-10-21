package diesel.security;

import java.util.List;
import java.util.Map;

/**
 * Storage bridge for the {@code DIESEL_TENANTS} system table (prompt4 #36).
 * Implemented by the engine over the table; unit tests may supply an
 * in-memory fake. Keeping this interface free of engine types lets
 * {@link TenantStore} live in this package.
 */
public interface TenantStoreBackend {

    /** System table name the backend must understand. */
    String TENANTS = "DIESEL_TENANTS";

    /**
     * Returns all rows of the tenants system table as column-value maps.
     *
     * @return the row list, empty when the table has no rows
     */
    List<Map<String, Object>> readAll();

    /**
     * Appends a row to the tenants system table and makes it durable.
     *
     * @param row the column-value mapping; all values are Strings
     */
    void appendRow(Map<String, Object> row);

    /**
     * Deletes the row carrying the given {@code ID} and makes it durable.
     *
     * @param id the ID of the row to delete
     * @throws IllegalStateException when no row with that ID exists
     */
    void deleteRow(String id);
}
