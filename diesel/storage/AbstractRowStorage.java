package diesel.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base implementation of {@link RowStorage} that stores the schema metadata
 * (columns, column types, table name) and provides a path-resolution utility.
 * Concrete subclasses supply the actual row storage and persistence logic.
 *
 * <p>This class also provides the shared delimited-versus-serialised load
 * resolution (prompt 32): each subclass picks its source via
 * {@link #resolveLoadSource(String, String, String)} based on its configured
 * load mode ({@code file} or {@code auto_mtime}) and validates a serialised
 * fast-path result with {@link #checkSerializedConsistency(SerializedTableData)}.
 */
public abstract class AbstractRowStorage implements RowStorage {

    private static final Logger LOGGER = Logger.getLogger(AbstractRowStorage.class.getName());

    /** Current format version written into {@link SerializedTableData} files. */
    protected static final int CURRENT_STORAGE_FORMAT_VERSION = 1;

    /** Load mode that always reads the delimited file. */
    protected static final String LOAD_MODE_FILE = "file";

    /** Load mode that prefers the fresher of the .table and delimited files. */
    protected static final String LOAD_MODE_AUTO_MTIME = "auto_mtime";

    /** The persistence source chosen for a load operation. */
    protected enum LoadSource {
        /** The delimited file (.csv / .tsv). */
        DELIMITED,
        /** The Java-serialised .table file. */
        SERIALIZED
    }

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
     * The compact Object[] row is shared with the manager, keeping a single
     * representation per row (prompt 36).
     */
    protected void syncIndexInsert(Object[] row, int rowIndex) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.insertAtShared(rowIndex, row);
        }
    }

    /**
     * Mirrors an append-only insert into the index manager. The row was
     * added at the end of the storage — no position shifting needed.
     */
    protected void syncIndexAppend(Object[] row, int rowIndex) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.appendIndexedRowShared(row, rowIndex);
        }
    }

    /**
     * Mirrors an index-stable update (same row index) into the index manager.
     */
    protected void syncIndexUpdate(Object[] oldRow, int rowIndex, Object[] newRow) {
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
     * load ({@code loadFromFile}) or a wholesale row replacement. Outside a
     * bulk-update window this runs the full rebuild immediately; inside one
     * (see {@link #beginBulkUpdate()}) it only marks the index state dirty and
     * defers the single rebuild to {@link #endBulkUpdate()}.
     */
    protected void syncIndexBulk() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.markIndexDirty(scan(), primaryKeyColumn);
        }
    }

    /**
     * Same as {@link #syncIndexBulk()} but takes the storage's compact Object[]
     * rows directly, so the index manager shares the exact row arrays instead of
     * converting through a temporary Map pass (prompt 36).
     */
    protected void syncIndexBulkFromArrays(List<Object[]> rows) {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.markIndexDirtyFromArrays(new ArrayList<>(rows), primaryKeyColumn);
        }
    }

    /**
     * Enters a deferred bulk-update window: per-operation index rebuilds and
     * position shifting are skipped and a single rebuild runs at
     * {@link #endBulkUpdate()}. A no-op for storages without an index manager.
     */
    @Override
    public void beginBulkUpdate() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.beginBulkUpdate();
        }
    }

    /**
     * Leaves a deferred bulk-update window, performing the single index rebuild
     * accumulated while dirty. A no-op for storages without an index manager.
     */
    @Override
    public void endBulkUpdate() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.endBulkUpdate();
        }
    }

    // ─── Load mode resolution (prompt 32) ─────────────────────────────

    /**
     * Whether the storage's secondary .table mirror should be written on save.
     * Default {@code off}: since 3.0.61 {@code Table.saveToSerializedFile()} is
     * the sole default writer of .table files (a storage-written mirror in a
     * different serialization format would break {@code Table.loadFromFile()}'s
     * cast). Enable {@code csv.table.mirror} / {@code tsv.table.mirror} = on to
     * opt into the storage-level fast-load path.
     */
    protected static boolean isTableMirrorEnabled(String configKey) {
        String raw = StorageConfig.getString(configKey, "off");
        return !"off".equalsIgnoreCase(raw.trim());
    }

    /**
     * Resolves and normalises a load mode config key ({@code csv.load.mode} /
     * {@code tsv.load.mode}). The synonyms {@code csv}/{@code tsv} map to
     * {@link #LOAD_MODE_FILE}; unsupported values fall back to it with a
     * WARNING.
     */
    protected static String resolveLoadMode(String configKey) {
        String raw = StorageConfig.getString(configKey, LOAD_MODE_FILE);
        String mode = raw.trim().toLowerCase(Locale.ROOT);
        if (LOAD_MODE_FILE.equals(mode) || "csv".equals(mode) || "tsv".equals(mode)) {
            return LOAD_MODE_FILE;
        }
        if (LOAD_MODE_AUTO_MTIME.equals(mode)) {
            return LOAD_MODE_AUTO_MTIME;
        }
        LOGGER.log(Level.WARNING, "Unsupported {0} value ''{1}'', falling back to {2}",
                new Object[]{configKey, raw, LOAD_MODE_FILE});
        return LOAD_MODE_FILE;
    }

    /**
     * Chooses the persistence source for a load operation. In {@code file}
     * mode the delimited file is always used. In {@code auto_mtime} mode the
     * serialised .table file is preferred only when it exists and is strictly
     * fresher than the delimited file; on equal mtimes the delimited file wins.
     *
     * @param delimitedFileName   the delimited file (.csv / .tsv)
     * @param serializedFileName  the Java-serialised .table file
     * @param loadMode            a mode returned by {@link #resolveLoadMode}
     * @return the chosen source
     */
    protected LoadSource resolveLoadSource(String delimitedFileName, String serializedFileName, String loadMode) {
        if (!LOAD_MODE_AUTO_MTIME.equalsIgnoreCase(loadMode)) {
            return LoadSource.DELIMITED;
        }
        File serialized = new File(serializedFileName);
        File delimited = new File(delimitedFileName);
        if (serialized.exists() && serialized.lastModified() > delimited.lastModified()) {
            return LoadSource.SERIALIZED;
        }
        return LoadSource.DELIMITED;
    }

    /**
     * Reads a {@link SerializedTableData} from disk, or returns {@code null}
     * when the file is missing or cannot be read.
     */
    protected SerializedTableData readSerializedTable(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            return null;
        }
        try (FileInputStream fis = new FileInputStream(fileName);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            return (SerializedTableData) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Failed to read serialised table {0}: {1}",
                    new Object[]{fileName, e.getMessage()});
            return null;
        }
    }

    /**
     * Checks the internal consistency of data loaded from a .table file:
     * format version, recorded row count versus actual rows, and the stored
     * columns/types against the current schema. Returns an empty list when the
     * fast-path result can be trusted.
     */
    protected List<String> checkSerializedConsistency(SerializedTableData data) {
        List<String> problems = new ArrayList<>();
        if (data.formatVersion > CURRENT_STORAGE_FORMAT_VERSION) {
            problems.add("format version " + data.formatVersion + " exceeds supported "
                    + CURRENT_STORAGE_FORMAT_VERSION);
        }
        if (data.rowCount != data.rows.size()) {
            problems.add("recorded row count " + data.rowCount
                    + " does not match loaded rows " + data.rows.size());
        }
        if (!columnsMatch(this.columns, data.columns)) {
            problems.add("stored columns " + data.columns + " do not match schema " + this.columns);
        }
        if (!typesMatch(this.columnTypes, data.columnTypes)) {
            problems.add("stored column types do not match schema");
        }
        return problems;
    }

    private static boolean columnsMatch(List<String> expected, List<String> stored) {
        if (stored == null || stored.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!stored.get(i).equalsIgnoreCase(expected.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean typesMatch(Map<String, Class<?>> expected, Map<String, Class<?>> stored) {
        if (stored == null || stored.size() != expected.size()) {
            return false;
        }
        Map<String, Class<?>> lookup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        lookup.putAll(stored);
        for (Map.Entry<String, Class<?>> entry : expected.entrySet()) {
            Class<?> actual = lookup.get(entry.getKey());
            if (actual == null || !actual.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Lightweight serialisable snapshot of a delimited storage written to the
     * secondary .table file. Used by the {@code auto_mtime} fast load path.
     * Rows are written as compact Object[] arrays (prompt 36); {@code List} is
     * declared as the element type so snapshots produced before the switch to
     * array rows (format version 1 with Map elements) still deserialise and can
     * be converted by the storages on load.
     */
    protected static final class SerializedTableData implements Serializable {
        private static final long serialVersionUID = 1L;
        final int formatVersion;
        final List<String> columns;
        final Map<String, Class<?>> columnTypes;
        final int rowCount;
        final List<?> rows;

        SerializedTableData(int formatVersion, List<String> columns, Map<String, Class<?>> columnTypes,
                            List<Object[]> rows) {
            this.formatVersion = formatVersion;
            this.columns = columns;
            this.columnTypes = columnTypes;
            this.rowCount = rows.size();
            this.rows = rows;
        }
    }
}
