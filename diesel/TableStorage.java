package diesel;
import java.util.*;

/**
 * Storage contract of a table: row access, schema access and persistence to
 * disk. Implemented by {@link Table}.
 */
interface TableStorage {
    /**
     * Returns the table rows.
     *
     * @return the list of column-to-value maps
     */
    List<Map<String, Object>> getRows();

    /**
     * Returns the ordered column names.
     *
     * @return the column list
     */
    List<String> getColumns();

    /**
     * Returns the column name to type mapping.
     *
     * @return the column type map
     */
    Map<String, Class<?>> getColumnTypes();

    /**
     * Validates and inserts a row.
     *
     * @param row the column-to-value map
     */
    void addRow(Map<String, Object> row);

    /**
     * Writes the table contents to its CSV file.
     *
     * @param tableName the table name
     */
    void saveToFile(String tableName);

    /**
     * Loads the table from its serialized file.
     *
     * @param tableName the table name
     */
    void loadFromFile(String tableName);
}
