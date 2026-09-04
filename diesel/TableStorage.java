package diesel;
import java.util.*;

/**
 * Storage contract of a table: row access, schema access and persistence to
 * disk. Implemented by {@link Table} (row-based) and
 * {@link ColumnarTableStorage} (Parquet-based columnar storage for OLAP).
 */
interface TableStorage {

    /**
     * Enumeration of storage backends supported by DieselDB.
     */
    enum StorageType {
        /** Traditional row-based storage (in-memory ArrayList, persisted as CSV/serialized). */
        ROW_BASED,
        /** Columnar storage backed by Apache Parquet files. */
        COLUMNAR
    }

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

    /**
     * Returns the type of this storage backend.
     *
     * @return the storage type
     */
    default StorageType getStorageType() {
        return StorageType.ROW_BASED;
    }

    /**
     * Returns true when this storage supports predicate pushdown for
     * analytical queries. Columnar storage backed by Parquet supports
     * this via row-group statistics.
     *
     * @return true if predicate pushdown is available
     */
    default boolean supportsPredicatePushdown() {
        return false;
    }
}
