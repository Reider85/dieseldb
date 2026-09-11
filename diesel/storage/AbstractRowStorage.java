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
    private String primaryKeyColumn;

    /**
     * Optional index/cache manager used by delimited file backends (CSV/TSV)
     * to provide prompt-23 functionality: primary-key and secondary indexes,
     * an LRU block cache and parallel file reading. Subclasses provide one via
     * {@link #createIndexManager()}; other backends keep it {@code null}.
     */
    protected DelimitedIndexManager indexManager;

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

    // ─── Index / cache manager hooks (prompt 23) ────────────────────

    /**
     * Creates the {@link DelimitedIndexManager} backing this storage, or
     * returns {@code null} when the storage does not maintain indexes.
     */
    protected DelimitedIndexManager createIndexManager() {
        return null;
    }

    /**
     * Lazily initialises and returns the index manager of this storage.
     * The engine does not necessarily call {@link RowStorage#open()}, so the
     * manager is created on first use.
     *
     * @return the index manager, or {@code null} when indexes are not maintained
     */
    protected DelimitedIndexManager index() {
        if (indexManager == null) {
            indexManager = createIndexManager();
        }
        return indexManager;
    }

    /**
     * Returns the index manager of this storage, or {@code null} when indexes
     * are not maintained.
     */
    public DelimitedIndexManager getIndexManager() {
        return indexManager;
    }

    /** Returns whether this storage maintains an index manager. */
    public boolean isIndexed() {
        return index() != null;
    }

    /**
     * Sets the primary-key column and rebuilds the index structures when this
     * storage maintains an index manager. A no-op otherwise.
     */
    @Override
    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
        DelimitedIndexManager manager = index();
        if (manager != null && primaryKeyColumn != null) {
            manager.buildIndexes(scan(), primaryKeyColumn);
        }
    }

    /** Returns the configured primary-key column, or {@code null}. */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /**
     * Mirrors a position-shifted insert into the index manager. The row at
     * {@code rowIndex} was just physically inserted, shifting all later rows.
     * The index manager updates its position map without rebuilding indexes.
     */
    protected void syncIndexInsert(Map<String, Object> row, int rowIndex) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.insertAt(rowIndex, row);
        }
    }

    /**
     * Mirrors an append-only insert into the index manager. The row was
     * added at the end of the storage — no position shifting needed.
     */
    protected void syncIndexAppend(Map<String, Object> row, int rowIndex) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.appendIndexedRow(row, rowIndex);
        }
    }

    /**
     * Mirrors an index-stable update (same row index) into the index manager.
     */
    protected void syncIndexUpdate(Map<String, Object> oldRow, int rowIndex, Map<String, Object> newRow) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.updateRow(oldRow, rowIndex, newRow);
        }
    }

    /**
     * Mirrors a delete into the index manager. Uses the stable rowId for
     * incremental index removal and position shifting instead of a full rebuild.
     */
    protected void syncIndexDelete(int rowIndex) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.deleteRow(rowIndex);
        }
    }

    /**
     * Rebuilds the index structures from the current rows, used after a bulk
     * load ({@code loadFromFile}) or a wholesale row replacement.
     */
    protected void syncIndexBulk() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.buildIndexes(scan(), primaryKeyColumn);
        }
    }
}
