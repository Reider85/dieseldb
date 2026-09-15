package diesel.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.storage.json.JsonPathResolver;
import diesel.storage.json.JsonParserConfig;

/**
 * Index management for the JSONL storage backend (prompt 53). Built directly
 * on the prompt-25 target architecture instead of inheriting the delimited
 * manager's pre-prompt-25 behaviour:
 * <ul>
 * <li>a <em>stable, monotonic</em> {@code rowId} is assigned once per row and
 * never reused while the row lives; primary-key and secondary indexes map
 * {@code key -&gt; rowId} ({@link TreeMap}s, {@code O(log n)} lookups and range
 * scans);</li>
 * <li>a separate {@code rowId -&gt; current position} map decouples the
 * indexes from physical positions, so a clustered {@code insertAt} into the
 * middle only shifts positions ({@code O(n)} in the trailing rows) and never
 * re-keys existing rows - the bug pattern fixed by prompt 25;</li>
 * <li>deletions are tombstones ({@link #deletedRowIds}) with automatic
 * compaction ({@link #compact()} reindexes and reassigns rowIds) when 25% of
 * the live rows are dead.</li>
 * </ul>
 *
 * <p><b>Nested-field indexes (prompt 45):</b> {@link #createIndex(String)}
 * accepts a dot-path and resolves it through the shared path engine
 * {@link JsonPathResolver}. A path that is an exact schema column
 * ({@code flatten} layout stores leaves literally as {@code user.id}) is
 * indexed directly from the row array slot. A dot-path into a whole-JSON
 * {@code json_column} cell resolves to the longest schema-column prefix and
 * the key is extracted per row via {@link JsonPathResolver#extract}; scalars
 * are indexed as their raw token text (mirroring what a JSON Path SELECT
 * returns), so the two modes are each internally consistent with their own
 * read results.
 *
 * <p><b>Sidecar persistence (prompt 53, prompt 30):</b> the index is persisted
 * next to the data as {@code <table>.idx} through the crash-safe
 * {@link AtomicFileWriter} (temp + fsync + atomic rename), so an interrupted
 * sidecar write never corrupts either the data or a previously valid sidecar.
 * The sidecar carries the owning data file's mtime/size stamp plus the ordered
 * schema columns; {@link #syncFromDisk(String, String, List, String)} adopts
 * the persisted maps only when the stamp, the schema and the row count all
 * match, and otherwise rebuilds the index from the freshly loaded rows
 * (re-creating every previously persisted index column).
 *
 * <p><b>Append mode (prompt 49):</b> new rows are assigned fresh rowIds and
 * added to the indexes incrementally via {@link #appendRow(Object[], int)}
 * with no rebuild; deletions are mirrored during the delete so delta-driven
 * reloads stay consistent.
 *
 * <p><b>Deferred bulk updates (prompt 35):</b>
 * {@link #beginBulkUpdate()}/{@link #endBulkUpdate()} open a window in which
 * {@link #insertAt(int, Object[])}/{@link #deleteRow(int)} only mutate the
 * mirror row list and mark the index state dirty; the single rebuild runs at
 * {@link #endBulkUpdate()}, keeping mass delete/insert operations linear
 * instead of quadratic.
 *
 * <p><b>Thread safety:</b> like the delimited manager, this manager is
 * <i>not</i> internally synchronized. All mutable state is guarded by the
 * owning {@link diesel.Table} {@code tableLock}: mutations under the write
 * lock, lookups under the read or write lock.
 */
public final class JsonlIndexManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlIndexManager.class);

    private static final Comparator<Object> KEY_ORDER = (a, b) -> {
        if (a != null && b != null && a instanceof Comparable && b instanceof Comparable) {
            try {
                @SuppressWarnings("unchecked")
                Comparable<Object> ca = (Comparable<Object>) a;
                return ca.compareTo(b);
            } catch (ClassCastException ignored) {
                return String.valueOf(a).compareTo(String.valueOf(b));
            }
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    };

    /** Tombstone compaction threshold: auto-compact when deleted/live exceeds this. */
    private static final double COMPACTION_THRESHOLD = 0.25;

    private final String tableName;
    private final JsonParserConfig config;
    private List<String> columns;
    private RowArrays rowColumns;

    /** Mirror of the storage's rows (the very same array objects, prompt 36). */
    private List<Object[]> rows = new ArrayList<>();
    private String primaryKeyColumn;
    private final NavigableMap<Object, Long> primaryKeyIndex = new TreeMap<>(KEY_ORDER);
    /** Literal-column secondary indexes: column name -&gt; key -&gt; rowIds. */
    private final Map<String, NavigableMap<Object, List<Long>>> secondaryIndexes =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    /** Dot-path secondary indexes over whole-JSON cells (prompt 45). */
    private final Map<String, NestedIndex> nestedIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private long nextRowId = 0;
    private final NavigableMap<Long, Integer> rowIdToPosition = new TreeMap<>();
    private final Set<Long> deletedRowIds = new HashSet<>();

    private boolean bulkMode;
    private boolean bulkDirty;
    private boolean loadedFromSidecar;

    /**
     * @param tableName the table name
     * @param columns   the ordered schema column names
     * @param config    the streaming JSON configuration used for nested
     *                  dot-path key extraction (prompt 45)
     */
    public JsonlIndexManager(String tableName, List<String> columns, JsonParserConfig config) {
        this.tableName = tableName == null ? "" : tableName;
        this.columns = new ArrayList<>(columns == null ? List.of() : columns);
        this.rowColumns = new RowArrays(this.columns);
        this.config = config != null ? config : JsonParserConfig.defaults();
    }

    /** Returns the table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the ordered schema column names this manager is aligned with. */
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    /**
     * Re-aligns the manager with a new schema (column list/layout). Used after
     * a JSONL load that adopted or expanded the schema (prompt 44 hybrid /
     * inferred modes). The caller immediately re-syncs the index afterwards
     * ({@link #syncFromDisk} or {@link #markDirty}), which rebuilds or adopts
     * all structures against the new layout.
     */
    public void adoptSchema(List<String> newColumns) {
        this.columns = new ArrayList<>(newColumns == null ? List.of() : newColumns);
        this.rowColumns = new RowArrays(this.columns);
    }

    /** Returns the configured primary-key column, or {@code null}. */
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    /** Sets (or clears) the primary-key column and rebuilds the indexes. */
    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        reindex();
    }

    /**
     * Wholesale row replacement: adopts the storage's current rows (sharing the
     * array objects) and rebuilds the index. Outside a bulk-update window this
     * is a full immediate rebuild; inside one the new rows are snapped into the
     * mirror list and the rebuild is deferred to {@link #endBulkUpdate()}.
     *
     * @param data             the rows to index, or {@code null} for an empty table
     * @param primaryKeyColumn the primary-key column, or {@code null} for none
     */
    public void markDirty(List<Object[]> data, String primaryKeyColumn) {
        if (!bulkMode) {
            this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
            this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
            secondaryIndexes.clear();
            nestedIndexes.clear();
            loadedFromSidecar = false;
            reindex();
            return;
        }
        this.rows = data != null ? new ArrayList<>(data) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        bulkDirty = true;
    }

    // ─── Stable row-id: append / insertAt / update / delete ─────────────

    /**
     * Appends a row (at the end of the storage). Assigns a fresh stable rowId
     * and adds it to all maintained indexes incrementally - no rebuild, so
     * JSONL append-mode saves never reconstruct the index.
     */
    public void appendRow(Object[] row, int rowIndex) {
        long rid = nextRowId++;
        rowIdToPosition.put(rid, rowIndex);
        rows.add(rowIndex, row);
        insertIndexedRow(row, rid);
    }

    /**
     * Inserts a row at the given physical position, shifting all later
     * positions up by one. The new row gets a fresh stable rowId; existing
     * key -&gt; rowId mappings are untouched (only the position map shifts), so
     * insertion cost is proportional to the rows after the insertion point and
     * never re-keys the whole table. Inside a bulk-update window the row is
     * only added to the mirror list and everything is deferred to the closing
     * {@link #endBulkUpdate()} rebuild.
     */
    public void insertAt(int rowIndex, Object[] row) {
        if (bulkMode) {
            rows.add(rowIndex, row);
            bulkDirty = true;
            return;
        }
        long rid = nextRowId++;
        rowIdToPosition.put(rid, rowIndex);
        rows.add(rowIndex, row);
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() >= rowIndex && e.getKey() != rid) {
                e.setValue(e.getValue() + 1);
            }
        }
        insertIndexedRow(row, rid);
    }

    /**
     * Replaces the row at {@code rowIndex}, refreshing both its key -&gt; rowId
     * index entries and keeping the same stable rowId.
     */
    public void updateRow(Object[] oldRow, int rowIndex, Object[] newRow) {
        Long rid = positionToRowId(rowIndex);
        if (rid == null) {
            return;
        }
        removeIndexedRow(oldRow, rid);
        insertIndexedRow(newRow, rid);
    }

    /**
     * Marks the row at the given physical position as deleted (tombstone),
     * shifts all later positions down by one and auto-compacts when the dead
     * ratio exceeds {@link #COMPACTION_THRESHOLD}. Inside a bulk-update window
     * the row is only removed from the mirror list and the rebuild is deferred.
     */
    public void deleteRow(int rowIndex) {
        if (bulkMode) {
            if (rowIndex < 0 || rowIndex >= rows.size()) {
                return;
            }
            rows.remove(rowIndex);
            bulkDirty = true;
            return;
        }
        Long rid = positionToRowId(rowIndex);
        if (rid == null) {
            return;
        }
        Object[] row = rows.remove(rowIndex);
        removeIndexedRow(row, rid);
        rowIdToPosition.remove(rid);
        deletedRowIds.add(rid);
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() > rowIndex) {
                e.setValue(e.getValue() - 1);
            }
        }
        int liveRows = rowIdToPosition.size();
        if (liveRows > 0 && (double) deletedRowIds.size() / liveRows > COMPACTION_THRESHOLD) {
            compact();
        }
    }

    /**
     * Full rebuild: clears all tombstones, reassigns rowIds sequentially and
     * rebuilds every index from scratch.
     */
    public void compact() {
        reindex();
    }

    /** Rebuilds the primary, secondary and nested indexes from the mirror rows. */
    public void reindex() {
        primaryKeyIndex.clear();
        for (NavigableMap<Object, List<Long>> index : secondaryIndexes.values()) {
            index.clear();
        }
        for (NestedIndex nested : nestedIndexes.values()) {
            nested.tree.clear();
        }
        rowIdToPosition.clear();
        deletedRowIds.clear();
        nextRowId = 0;
        for (int i = 0; i < rows.size(); i++) {
            long rid = nextRowId++;
            rowIdToPosition.put(rid, i);
            insertIndexedRow(rows.get(i), rid);
        }
    }

    // ─── Deferred bulk updates (prompt 35) ─────────────────────────────

    /** Enters a deferred bulk-update window (see class javadoc). */
    public void beginBulkUpdate() {
        bulkMode = true;
    }

    /**
     * Leaves a deferred bulk-update window and performs the single index
     * rebuild accumulated while dirty. Must be paired with
     * {@link #beginBulkUpdate()}, even on exceptional paths.
     */
    public void endBulkUpdate() {
        bulkMode = false;
        if (bulkDirty) {
            bulkDirty = false;
            reindex();
        }
    }

    /** Returns whether a deferred bulk-update window is open. */
    public boolean isBulkUpdating() {
        return bulkMode;
    }

    // ─── Index construction ─────────────────────────────────────────────

    /**
     * Creates (or returns) an index over the given column name or dot-path.
     * A plain schema column (or a literal dot-named flatten leaf such as
     * {@code user.id}) is indexed from the row array slot; a dot-path into a
     * whole-JSON {@code json_column} cell is indexed by the per-row extracted
     * value (prompt 45). Returns {@code false} when the path resolves to no
     * schema column.
     */
    public boolean createIndex(String columnOrPath) {
        if (columnOrPath == null || columnOrPath.isBlank()) {
            return false;
        }
        JsonPathResolver.ResolvedPath resolved = JsonPathResolver.resolve(columns, columnOrPath);
        if (resolved.columnIndex() < 0) {
            return false;
        }
        if (resolved.isPlain()) {
            String column = resolved.column();
            if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(column)) {
                return true;
            }
            if (secondaryIndexes.containsKey(column)) {
                return true;
            }
            NavigableMap<Object, List<Long>> index = new TreeMap<>(KEY_ORDER);
            for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
                Object key = rowColumns.get(rows.get(e.getValue()), column);
                if (key != null) {
                    index.computeIfAbsent(key, k -> new ArrayList<>()).add(e.getKey());
                }
            }
            secondaryIndexes.put(column, index);
            return true;
        }
        if (nestedIndexes.containsKey(columnOrPath)) {
            return true;
        }
        NavigableMap<Object, List<Long>> tree = new TreeMap<>(KEY_ORDER);
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            Object key = nestedKey(resolved, rows.get(e.getValue()));
            if (key != null) {
                tree.computeIfAbsent(key, k -> new ArrayList<>()).add(e.getKey());
            }
        }
        nestedIndexes.put(columnOrPath, new NestedIndex(resolved, tree));
        return true;
    }

    /** Returns the column names / dot-paths that have a maintained index. */
    public List<String> getIndexColumns() {
        List<String> result = new ArrayList<>();
        if (primaryKeyColumn != null) {
            result.add(primaryKeyColumn);
        }
        result.addAll(secondaryIndexes.keySet());
        result.addAll(nestedIndexes.keySet());
        return result;
    }

    // ─── Index queries ───────────────────────────────────────────────────

    /**
     * Exact primary-key search, {@code O(log n)}.
     *
     * @param key the primary-key value
     * @return the matching row indices, or an empty list when absent
     */
    public List<Integer> searchByPrimaryKey(Object key) {
        if (key == null || primaryKeyColumn == null || primaryKeyIndex.isEmpty()) {
            return List.of();
        }
        Long rowId = primaryKeyIndex.get(key);
        if (rowId == null) {
            return List.of();
        }
        Integer pos = rowIdToPosition.get(rowId);
        return pos == null ? List.of() : List.of(pos);
    }

    /**
     * Exact equality search over the primary-key, a literal secondary index or
     * a nested dot-path index.
     *
     * @param column the column or dot-path to search (case-insensitive)
     * @param key    the value to look up
     * @return the matching row indices, possibly empty
     */
    public List<Integer> search(String column, Object key) {
        if (column == null || key == null) {
            return List.of();
        }
        if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(column)) {
            return searchByPrimaryKey(key);
        }
        NavigableMap<Object, List<Long>> index = secondaryIndexes.get(column);
        if (index == null) {
            NestedIndex nested = nestedIndexes.get(column);
            if (nested != null) {
                index = nested.tree;
            }
        }
        if (index == null) {
            return List.of();
        }
        List<Long> ids = index.get(key);
        return ids == null ? List.of() : translateRowIdsToPositions(ids);
    }

    /**
     * Inclusive range search over the primary-key, a literal secondary index or
     * a nested dot-path index.
     *
     * @param column the column or dot-path to search (case-insensitive)
     * @param low    the inclusive lower bound, or {@code null} for open-ended
     * @param high   the inclusive upper bound, or {@code null} for open-ended
     * @return the matching row indices, possibly empty
     */
    public List<Integer> rangeSearch(String column, Object low, Object high) {
        if (column == null) {
            return List.of();
        }
        if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(column)) {
            return collectPositions(subRange(primaryKeyIndex, low, high));
        }
        NavigableMap<Object, List<Long>> index = secondaryIndexes.get(column);
        if (index == null) {
            NestedIndex nested = nestedIndexes.get(column);
            if (nested != null) {
                index = nested.tree;
            }
        }
        if (index == null) {
            return List.of();
        }
        return collectPositions(subRange(index, low, high));
    }

    // ─── Sidecar persistence (prompt 53) ────────────────────────────────

    /**
     * Persists the index to {@code <table>.idx} next to the data file, stamped
     * with the data file's identity, mtime and size plus the current schema
     * columns. The sidecar is written atomically via {@link AtomicFileWriter}
     * (prompt 30), so a crash mid-write leaves the previous valid sidecar (or
     * no sidecar) behind and can never corrupt the data file. Writes are
     * skipped when nothing is indexed (no primary key, no secondary/nested
     * columns). A failed write is logged and never fails the caller's save.
     */
    public void persist(String idxFile, String dataPath) throws IOException {
        if (primaryKeyColumn == null && secondaryIndexes.isEmpty() && nestedIndexes.isEmpty()) {
            return;
        }
        File dataFile = new File(dataPath);
        IndexSidecar sidecar = snapshot(dataFile);
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(idxFile))) {
            ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
            oos.writeObject(sidecar);
            oos.flush();
            afw.commit();
        }
        LOGGER.info("JsonlIndexManager {} persisted {} index entries to {}", tableName, sidecar.rowIdToPosition.size(),
                idxFile);
    }

    /**
     * Re-synchronises the index state against the loaded rows and the persisted
     * sidecar:
     * <ul>
     * <li>when the sidecar exists and its stamp (data mtime/size), schema
     * columns and row count all match the freshly loaded state, the persisted
     * maps are adopted directly -- no rebuild, which is the reload fast path;</li>
     * <li>otherwise the index is rebuilt from the loaded rows and every
     * previously persisted index column is re-created (prompt 53
     * "rebuild upon mismatch").</li>
     * </ul>
     * A corrupt/truncated sidecar is logged and treated as a miss (the load
     * of the data itself never fails because of a bad sidecar).
     *
     * @param idxFile     the {@code <table>.idx} sidecar path
     * @param dataPath    the physical data file ({@code .jsonl} or compressed variant)
     * @param currentRows the freshly loaded storage rows (their arrays are adopted)
     * @param primaryKeyColumn the table's primary-key column, or {@code null}
     * @return {@code true} when the persisted sidecar was adopted without a rebuild
     */
    public boolean syncFromDisk(String idxFile, String dataPath, List<Object[]> currentRows, String primaryKeyColumn) {
        this.rows = currentRows != null ? new ArrayList<>(currentRows) : new ArrayList<>();
        this.primaryKeyColumn = resolveColumn(primaryKeyColumn);
        loadedFromSidecar = false;
        IndexSidecar sidecar = readSidecar(new File(idxFile));
        File dataFile = new File(dataPath);
        if (sidecar != null && dataFile.exists()
                && sidecar.dataMtime == dataFile.lastModified()
                && sidecar.dataSize == dataFile.length()
                && columnsMatch(sidecar.columns, columns)
                && (sidecar.rowIdToPosition == null || sidecar.rowIdToPosition.size() == rows.size())) {
            adoptSidecar(sidecar);
            loadedFromSidecar = true;
            LOGGER.info("JsonlIndexManager {} loaded {} index entries from sidecar {}", tableName,
                    rowIdToPosition.size(), idxFile);
            return true;
        }
        secondaryIndexes.clear();
        nestedIndexes.clear();
        reindex();
        if (sidecar != null) {
            for (String column : sidecar.indexedColumns) {
                createIndex(column);
            }
        }
        LOGGER.info("JsonlIndexManager {} rebuilt index for {} rows ({})", tableName, rows.size(),
                sidecar == null ? "no sidecar" : "stale/invalid sidecar");
        return false;
    }

    /** Returns whether the last {@link #syncFromDisk} adopted a fresh sidecar. */
    public boolean isLoadedFromSidecar() {
        return loadedFromSidecar;
    }

    /** Returns the current live row count mirrored by the index. */
    public int getRowCount() {
        return rowIdToPosition.size();
    }

    /** Returns the number of tombstoned (deleted) rowIds pending compaction. */
    public int getDeletedCount() {
        return deletedRowIds.size();
    }

    /** Returns the next rowId that will be assigned. */
    public long getNextRowId() {
        return nextRowId;
    }

    // ─── Internal helpers ───────────────────────────────────────────────

    /** Injects a new row into all maintained indexes under the given rowId. */
    private void insertIndexedRow(Object[] row, long rid) {
        if (primaryKeyColumn != null) {
            Object key = rowColumns.get(row, primaryKeyColumn);
            if (key != null) {
                primaryKeyIndex.put(key, rid);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = rowColumns.get(row, entry.getKey());
            if (key != null) {
                entry.getValue().computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
            }
        }
        for (NestedIndex nested : nestedIndexes.values()) {
            Object key = nestedKey(nested.path, row);
            if (key != null) {
                nested.tree.computeIfAbsent(key, k -> new ArrayList<>()).add(rid);
            }
        }
    }

    /** Removes a row from all maintained indexes. */
    private void removeIndexedRow(Object[] row, long rid) {
        if (primaryKeyColumn != null) {
            Object key = rowColumns.get(row, primaryKeyColumn);
            if (key != null && rid == primaryKeyIndex.getOrDefault(key, -1L)) {
                primaryKeyIndex.remove(key);
            }
        }
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> entry : secondaryIndexes.entrySet()) {
            Object key = rowColumns.get(row, entry.getKey());
            if (key == null) {
                continue;
            }
            List<Long> ids = entry.getValue().get(key);
            if (ids != null) {
                ids.remove(rid);
                if (ids.isEmpty()) {
                    entry.getValue().remove(key);
                }
            }
        }
        for (NestedIndex nested : nestedIndexes.values()) {
            Object key = nestedKey(nested.path, row);
            if (key == null) {
                continue;
            }
            List<Long> ids = nested.tree.get(key);
            if (ids != null) {
                ids.remove(rid);
                if (ids.isEmpty()) {
                    nested.tree.remove(key);
                }
            }
        }
    }

    /** Extracts a nested dot-path key from a whole-JSON cell (prompt 45). */
    private Object nestedKey(JsonPathResolver.ResolvedPath path, Object[] row) {
        Object holder = rowColumns.get(row, path.column());
        if (holder == null) {
            return null;
        }
        return JsonPathResolver.extract(String.valueOf(holder), path.segments(), config);
    }

    /** Returns the rowId at the given physical position, or {@code null}. */
    private Long positionToRowId(int position) {
        for (Map.Entry<Long, Integer> e : rowIdToPosition.entrySet()) {
            if (e.getValue() == position) {
                return e.getKey();
            }
        }
        return null;
    }

    /** Translates rowIds into current physical positions, sorted ascending. */
    private List<Integer> translateRowIdsToPositions(List<Long> rowIds) {
        List<Integer> result = new ArrayList<>(rowIds.size());
        for (Long rid : rowIds) {
            Integer pos = rowIdToPosition.get(rid);
            if (pos != null) {
                result.add(pos);
            }
        }
        result.sort(Integer::compareTo);
        return result;
    }

    private static Map<Object, ?> subRange(NavigableMap<Object, ?> map, Object low, Object high) {
        if (map.isEmpty()) {
            return map;
        }
        if (low != null && high != null) {
            if (KEY_ORDER.compare(low, high) > 0) {
                return new TreeMap<>(KEY_ORDER);
            }
            return map.subMap(low, true, high, true);
        }
        if (low != null) {
            return map.tailMap(low, true);
        }
        if (high != null) {
            return map.headMap(high, true);
        }
        return map;
    }

    private List<Integer> collectPositions(Map<Object, ?> entries) {
        if (entries.isEmpty()) {
            return List.of();
        }
        List<Long> rowIds = new ArrayList<>();
        for (Object value : entries.values()) {
            if (value instanceof Long rid) {
                rowIds.add(rid);
            } else if (value instanceof List<?> ids) {
                for (Object id : ids) {
                    if (id instanceof Long rid) {
                        rowIds.add(rid);
                    }
                }
            }
        }
        return translateRowIdsToPositions(rowIds);
    }

    private String resolveColumn(String column) {
        if (column == null) {
            return null;
        }
        for (String c : columns) {
            if (c.equalsIgnoreCase(column)) {
                return c;
            }
        }
        return column;
    }

    private static boolean columnsMatch(List<String> expected, List<String> actual) {
        if (expected == null) {
            return true;
        }
        if (actual == null || expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!expected.get(i).equalsIgnoreCase(actual.get(i))) {
                return false;
            }
        }
        return true;
    }

    // ─── Sidecar serialization ──────────────────────────────────────────

    private IndexSidecar snapshot(File dataFile) {
        Map<Object, Long> primaryIterable = null;
        if (primaryKeyColumn != null && !primaryKeyIndex.isEmpty()) {
            primaryIterable = new LinkedHashMap<>(primaryKeyIndex);
        }
        Map<String, Map<Object, List<Long>>> secondaryMaps = new LinkedHashMap<>();
        for (Map.Entry<String, NavigableMap<Object, List<Long>>> e : secondaryIndexes.entrySet()) {
            Map<Object, List<Long>> copy = new LinkedHashMap<>();
            for (Map.Entry<Object, List<Long>> k : e.getValue().entrySet()) {
                copy.put(k.getKey(), new ArrayList<>(k.getValue()));
            }
            secondaryMaps.put(e.getKey(), copy);
        }
        Map<String, Map<Object, List<Long>>> nestedMaps = new LinkedHashMap<>();
        for (Map.Entry<String, NestedIndex> e : nestedIndexes.entrySet()) {
            Map<Object, List<Long>> copy = new LinkedHashMap<>();
            for (Map.Entry<Object, List<Long>> k : e.getValue().tree.entrySet()) {
                copy.put(k.getKey(), new ArrayList<>(k.getValue()));
            }
            nestedMaps.put(e.getKey(), copy);
        }
        List<String> indexedColumns = new ArrayList<>();
        indexedColumns.addAll(secondaryIndexes.keySet());
        indexedColumns.addAll(nestedIndexes.keySet());
        return new IndexSidecar(IndexSidecar.FORMAT_VERSION, dataFile.getAbsolutePath(),
                dataFile.lastModified(), dataFile.length(), new ArrayList<>(columns),
                primaryKeyColumn, indexedColumns, primaryIterable, secondaryMaps, nestedMaps,
                new LinkedHashMap<>(rowIdToPosition), nextRowId);
    }

    private IndexSidecar readSidecar(File idxFile) {
        if (!idxFile.exists()) {
            return null;
        }
        AtomicFileWriter.warnInterruptedWrite(idxFile.toPath());
        try (FileInputStream fis = new FileInputStream(idxFile);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            Object obj = ois.readObject();
            if (obj instanceof IndexSidecar sidecar && sidecar.formatVersion <= IndexSidecar.FORMAT_VERSION) {
                return sidecar;
            }
            return null;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.warn("JsonlIndexManager {} failed to read index sidecar {}: {}", tableName, idxFile.getPath(),
                    e.getMessage());
            return null;
        }
    }

    /** Replaces the manager state with the maps persisted in a fresh sidecar. */
    private void adoptSidecar(IndexSidecar sidecar) {
        primaryKeyIndex.clear();
        secondaryIndexes.clear();
        nestedIndexes.clear();
        deletedRowIds.clear();
        if (sidecar.primaryKeyRowIds != null) {
            primaryKeyIndex.putAll(sidecar.primaryKeyRowIds);
        }
        if (sidecar.secondaryRowIds != null) {
            for (Map.Entry<String, Map<Object, List<Long>>> e : sidecar.secondaryRowIds.entrySet()) {
                NavigableMap<Object, List<Long>> tree = new TreeMap<>(KEY_ORDER);
                for (Map.Entry<Object, List<Long>> k : e.getValue().entrySet()) {
                    tree.put(k.getKey(), new ArrayList<>(k.getValue()));
                }
                secondaryIndexes.put(e.getKey(), tree);
            }
        }
        if (sidecar.nestedRowIds != null) {
            for (Map.Entry<String, Map<Object, List<Long>>> e : sidecar.nestedRowIds.entrySet()) {
                JsonPathResolver.ResolvedPath path = JsonPathResolver.resolve(columns, e.getKey());
                if (path.columnIndex() < 0) {
                    continue;
                }
                NavigableMap<Object, List<Long>> tree = new TreeMap<>(KEY_ORDER);
                for (Map.Entry<Object, List<Long>> k : e.getValue().entrySet()) {
                    tree.put(k.getKey(), new ArrayList<>(k.getValue()));
                }
                nestedIndexes.put(e.getKey(), new NestedIndex(path, tree));
            }
        }
        rowIdToPosition.clear();
        rowIdToPosition.putAll(sidecar.rowIdToPosition);
        nextRowId = Math.max(sidecar.nextRowId,
                (rowIdToPosition.isEmpty() ? 0L : rowIdToPosition.lastKey()) + 1);
    }

    /** A nested dot-path index bound to its resolved schema path. */
    private static final class NestedIndex {
        final JsonPathResolver.ResolvedPath path;
        final NavigableMap<Object, List<Long>> tree;

        NestedIndex(JsonPathResolver.ResolvedPath path, NavigableMap<Object, List<Long>> tree) {
            this.path = path;
            this.tree = tree;
        }
    }

    /**
     * A serialisable snapshot of the index state plus the stamp used to detect
     * stale sidecars. Plain {@link LinkedHashMap}s are used inside instead of
     * comparator-aware {@link TreeMap}s so the snapshot survives Java
     * serialisation without a serializable comparator; the comparator-backed
     * trees are rebuilt on adoption.
     */
    static final class IndexSidecar implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final int FORMAT_VERSION = 1;

        final int formatVersion;
        final String dataPath;
        final long dataMtime;
        final long dataSize;
        final List<String> columns;
        final String primaryKeyColumn;
        final List<String> indexedColumns;
        final Map<Object, Long> primaryKeyRowIds;
        final Map<String, Map<Object, List<Long>>> secondaryRowIds;
        final Map<String, Map<Object, List<Long>>> nestedRowIds;
        final Map<Long, Integer> rowIdToPosition;
        final long nextRowId;

        IndexSidecar(int formatVersion, String dataPath, long dataMtime, long dataSize, List<String> columns,
                     String primaryKeyColumn, List<String> indexedColumns,
                     Map<Object, Long> primaryKeyRowIds,
                     Map<String, Map<Object, List<Long>>> secondaryRowIds,
                     Map<String, Map<Object, List<Long>>> nestedRowIds,
                     Map<Long, Integer> rowIdToPosition, long nextRowId) {
            this.formatVersion = formatVersion;
            this.dataPath = dataPath;
            this.dataMtime = dataMtime;
            this.dataSize = dataSize;
            this.columns = columns;
            this.primaryKeyColumn = primaryKeyColumn;
            this.indexedColumns = indexedColumns;
            this.primaryKeyRowIds = primaryKeyRowIds;
            this.secondaryRowIds = secondaryRowIds;
            this.nestedRowIds = nestedRowIds;
            this.rowIdToPosition = rowIdToPosition;
            this.nextRowId = nextRowId;
        }
    }
}