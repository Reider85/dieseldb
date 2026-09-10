package diesel.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Base implementation of {@link RowStorage} that stores the schema metadata
 * (columns, column types, table name) and provides a path-resolution utility.
 * Concrete subclasses supply the actual row storage and persistence logic.
 */
public abstract class AbstractRowStorage implements RowStorage {

    protected final List<String> columns;
    protected final Map<String, Class<?>> columnTypes;
    protected final String tableName;
    protected String dataDir;

    /**
     * @param tableName  the table name
     * @param columns    the ordered list of column names
     * @param columnTypes the column name to type mapping
     */
    protected AbstractRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.columnTypes.putAll(columnTypes);
    }

    /** Sets the data directory used for file-based persistence. */
    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    /** Returns the table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the ordered column names. */
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    /** Returns the column name to type mapping. */
    public Map<String, Class<?>> getColumnTypes() {
        Map<String, Class<?>> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(columnTypes);
        return copy;
    }

    /**
     * Resolves the full file path for the given extension.
     *
     * @param extension the file extension including the dot (e.g. ".csv")
     * @return the resolved path
     */
    protected String resolveFilePath(String extension) {
        String dir = (dataDir != null && !dataDir.isBlank()) ? dataDir : ".";
        return dir + File.separator + tableName + extension;
    }
}
