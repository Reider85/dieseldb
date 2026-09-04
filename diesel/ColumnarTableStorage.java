package diesel;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Columnar (Parquet-backed) implementation of {@link TableStorage}. Provides
 * read access to table data stored in Apache Parquet format, enabling
 * efficient analytical (OLAP) queries on large tables. Writes are not
 * supported directly; the Parquet file is created by
 * {@link ColumnarConversionJob} or {@link ParquetWriter}.
 *
 * <p>Projection pushdown is supported: only the requested columns are read
 * from the Parquet file, reducing I/O for wide tables. Predicate pushdown
 * leverages Parquet row-group statistics to skip irrelevant groups.
 *
 * <p>This storage is chosen by {@link QueryOptimizer} when a query is
 * classified as OLAP (analytical), providing the "dual storage" architecture
 * where OLTP operations use the row-based {@link Table} and OLAP operations
 * use this columnar backend.
 *
 * @see TableStorage
 * @see ParquetReader
 * @see ParquetWriter
 * @see ColumnarConversionJob
 */
class ColumnarTableStorage implements TableStorage {

    private static final Logger LOGGER = Logger.getLogger(ColumnarTableStorage.class.getName());

    /** The Parquet file path for this table's columnar storage. */
    private final Path parquetFilePath;

    /** The table name, used for error messages and metadata. */
    private final String tableName;

    /** Lazily loaded schema metadata from the Parquet file. */
    private List<String> cachedColumns;
    private Map<String, Class<?>> cachedColumnTypes;

    /**
     * Creates a new columnar storage backed by the given Parquet file.
     *
     * @param parquetFilePath the path to the Parquet file
     * @param tableName       the table name for logging
     */
    ColumnarTableStorage(Path parquetFilePath, String tableName) {
        this.parquetFilePath = parquetFilePath;
        this.tableName = tableName;
    }

    /**
     * Returns the Parquet file path.
     *
     * @return the file path
     */
    Path getParquetFilePath() {
        return parquetFilePath;
    }

    @Override
    public List<Map<String, Object>> getRows() {
        return getRows(null, null);
    }

    /**
     * Returns rows with optional projection and predicate pushdown.
     *
     * @param columns   the columns to read (null for all)
     * @param conditions the WHERE conditions to push down (null for none)
     * @return the matching rows
     */
    List<Map<String, Object>> getRows(List<String> columns,
                                      List<QueryParser.Condition> conditions) {
        LOGGER.log(Level.FINE, "Reading {0} rows from columnar storage for table {1}",
                new Object[]{columns != null ? columns.size() : "all", tableName});
        if (conditions != null && !conditions.isEmpty()) {
            return ParquetReader.readWhere(parquetFilePath, columns, conditions,
                    getColumnTypes());
        } else if (columns != null) {
            return ParquetReader.readProjected(parquetFilePath, columns);
        } else {
            return ParquetReader.readAll(parquetFilePath);
        }
    }

    @Override
    public List<String> getColumns() {
        if (cachedColumns != null) {
            return new ArrayList<>(cachedColumns);
        }
        try {
            var messageType = ParquetReader.getFileSchema(parquetFilePath);
            cachedColumns = new ArrayList<>();
            for (var field : messageType.getFields()) {
                cachedColumns.add(field.getName());
            }
            return new ArrayList<>(cachedColumns);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to read columns from Parquet file {0}: {1}",
                    new Object[]{parquetFilePath, e.getMessage()});
            return new ArrayList<>();
        }
    }

    @Override
    public Map<String, Class<?>> getColumnTypes() {
        if (cachedColumnTypes != null) {
            return new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER) {{
                putAll(cachedColumnTypes);
            }};
        }
        // If not cached, try to infer from the Parquet schema
        // For now, return empty map; caller should provide column types
        return new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    /**
     * Sets the column types for this columnar storage. Called when the
     * storage is created from a Table to provide type information.
     *
     * @param columnTypes the column type mapping
     */
    void setColumnTypes(Map<String, Class<?>> columnTypes) {
        this.cachedColumnTypes = columnTypes;
    }

    @Override
    public void addRow(Map<String, Object> row) {
        throw new UnsupportedOperationException(
                "Columnar storage is read-only. Use the row-based Table for INSERT operations.");
    }

    @Override
    public void saveToFile(String tableName) {
        throw new UnsupportedOperationException(
                "Columnar storage is read-only. Use ParquetWriter for persistence.");
    }

    @Override
    public void loadFromFile(String tableName) {
        // Parquet file is loaded lazily on first read
        LOGGER.log(Level.FINE, "Columnar storage for {0} will load from {1} on first read",
                new Object[]{tableName, parquetFilePath});
    }

    @Override
    public StorageType getStorageType() {
        return StorageType.COLUMNAR;
    }

    @Override
    public boolean supportsPredicatePushdown() {
        return true;
    }

    /**
     * Returns true if the Parquet file exists and can be read.
     *
     * @return whether the columnar storage is available
     */
    boolean isAvailable() {
        try {
            java.nio.file.Files.exists(parquetFilePath);
            return java.nio.file.Files.exists(parquetFilePath);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return "ColumnarTableStorage{" + tableName + " @ " + parquetFilePath + "}";
    }
}
