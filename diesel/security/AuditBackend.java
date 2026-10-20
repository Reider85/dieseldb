package diesel.security;

import java.util.List;
import java.util.Map;

/**
 * Storage bridge for the {@code DIESEL_AUDIT_LOG} system table (prompt4
 * #33). Implemented by the engine over the table; unit tests may supply an
 * in-memory fake. Keeping this interface free of engine types (Database/Table
 * are package-private) lets {@link AuditLogger} live in this package.
 */
public interface AuditBackend {

    /** System table name the backend must understand. */
    String AUDIT = "DIESEL_AUDIT_LOG";

    /**
     * Returns all rows of the audit table as column-value maps.
     *
     * @return the row list, empty when the table has no rows
     */
    List<Map<String, Object>> readAll();

    /**
     * Appends a single row and makes it durable.
     *
     * @param row the column-value mapping; all values are Strings
     */
    void appendRow(Map<String, Object> row);

    /**
     * Appends a batch of rows and makes them durable with a single save
     * (the audit consumer drains the queue in batches to amortize the
     * whole-file rewrite).
     *
     * @param rows the column-value mappings; all values are Strings
     */
    void appendBatch(List<Map<String, Object>> rows);

    /**
     * Deletes the rows carrying the given IDs and makes the removal durable
     * with a single save (retention pruning).
     *
     * @param ids the IDs of the rows to delete; unknown IDs are ignored
     */
    void deleteRows(List<String> ids);
}