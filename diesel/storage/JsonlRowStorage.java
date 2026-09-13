package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;

/**
 * JSON Lines (NDJSON) backed implementation of {@link RowStorage}. Rows are
 * kept in an in-memory buffer and persisted as one JSON object per line
 * ({@code <table>.jsonl}, UTF-8, {@code \n} separator).
 *
 * <p>Base implementation (prompt 40), explicitly out of scope here: no
 * secondary Java-serialised .table mirror (prompt 50), no compression
 * (prompt 52), no append-only mode (prompt 49). Type validation is wired
 * through the shared {@link JsonlSchemaManager} on both read and write
 * (prompt 41); strict coercion, schema inference/evolution and the
 * flatten/json_column storage rules land in prompts 43/44/45. Nested
 * object/array values are stored in a column as compact JSON text.
 *
 * <p>Rows are kept internally as compact Object[] arrays (one per row, slot
 * {@code i} = value of schema column {@code i}) instead of per-row Maps
 * (prompt 36). Column-to-value Maps are built only at the Map-based API
 * boundary ({@link #scan()}, {@link #insert(Map)}, {@link #update(int, Map)}).
 *
 * <p>Inherited infrastructure (prompt 46 checklist): saves go through the
 * shared crash-safe {@link AtomicFileWriter} (temp + fsync + atomic rename,
 * prompt 30) so an interrupted write can never truncate the previous valid
 * .jsonl; inserts preserve stable row ids (no repositioning of later rows is
 * ever implied by the engine); {@link #beginBulkUpdate()}/{@link #endBulkUpdate()}
 * (prompt 35) and the storage-level sync hooks are wired like the delimited
 * backends (they no-op here until an index manager exists, prompt 53);
 * logging uses slf4j (prompt 37); UTF-8 and {@code \n} are fixed (prompt 29);
 * the engine calls files under the table write lock (prompt 37).
 */
public class JsonlRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlRowStorage.class);

    protected final List<Object[]> rows = new ArrayList<>();
    private final RowArrays rowColumns;
    private boolean fileInitialized;

    public JsonlRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
        this.rowColumns = new RowArrays(columns);
    }

    /** Returns whether this storage has been persisted to disk at least once. */
    public boolean isFileInitialized() {
        return fileInitialized;
    }

    /** Sets the file-initialized flag. */
    public void setFileInitialized(boolean fileInitialized) {
        this.fileInitialized = fileInitialized;
    }

    // ─── RowStorage lifecycle ───────────────────────────────────────

    @Override
    public void open() {
        // No resources to acquire for the JSONL storage.
    }

    @Override
    public void close() {
        // No resources to release.
    }

    @Override
    public List<Map<String, Object>> scan() {
        List<Map<String, Object>> result = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            result.add(rowColumns.toMap(row));
        }
        return result;
    }

    @Override
    public void insert(Map<String, Object> row) {
        Object[] arr = rowColumns.fromMap(row);
        rows.add(arr);
        syncIndexAppend(arr, rows.size() - 1);
    }

    @Override
    public void insertAt(int rowIndex, Map<String, Object> row) {
        Object[] arr = rowColumns.fromMap(row);
        rows.add(rowIndex, arr);
        syncIndexInsert(arr, rowIndex);
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        Object[] oldRow = rows.get(rowIndex);
        Object[] newRow = rowColumns.fromMap(row);
        rows.set(rowIndex, newRow);
        syncIndexUpdate(oldRow, rowIndex, newRow);
    }

    @Override
    public void delete(int rowIndex) {
        rows.remove(rowIndex);
        syncIndexDelete(rowIndex);
    }

    // ─── Persistence ────────────────────────────────────────────────

    @Override
    public void saveToFile(String tableName) {
        String fileName = resolveFilePath(".jsonl");
        try {
            try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName));
                 JsonlRowWriter jsonlWriter = new JsonlRowWriter(afw.bufferedWriter(), columns, columnTypes)) {
                for (Object[] row : rows) {
                    jsonlWriter.writeRow(row);
                }
                jsonlWriter.flush();
                afw.commit();
            }
            fileInitialized = true;
            LOGGER.info("JsonlRowStorage {} saved JSONL to {} with {} rows",
                    tableName, fileName, rows.size());
        } catch (IOException e) {
            LOGGER.error("Failed to save JSONL for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to JSONL file: " + fileName, e);
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        File file = new File(resolveFilePath(".jsonl"));
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.info("JSONL file {} not found for storage {}", file.getPath(), tableName);
            return;
        }
        List<Object[]> previous = new ArrayList<>(rows);
        try (BufferedReader br = Files.newBufferedReader(file.toPath(), StorageConfig.getCharset());
             JsonlRowReader jsonlReader = new JsonlRowReader(br, columns, columnTypes, file.getPath())) {
            List<Object[]> loaded = new ArrayList<>();
            while (jsonlReader.hasNext()) {
                Object[] row = jsonlReader.nextArray();
                if (row != null) {
                    loaded.add(row);
                }
            }
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.info("JsonlRowStorage {} loaded from {} with {} rows",
                    tableName, file.getPath(), rows.size());
            syncIndexBulkFromArrays(rows);
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            throw new DieselIOException("Failed to load table from JSONL file: " + file.getPath(), e);
        }
    }

    // ─── Internal helpers ───────────────────────────────────────────

    /** Returns the internal row list directly (no copy). */
    public List<Object[]> getInternalRows() {
        return rows;
    }

    /** Replaces the internal row list. */
    public void setRows(List<Map<String, Object>> newRows) {
        rows.clear();
        for (Map<String, Object> row : newRows) {
            rows.add(rowColumns.fromMap(row));
        }
        syncIndexBulkFromArrays(rows);
    }
}