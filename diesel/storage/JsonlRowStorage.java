package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonSchemaInference;

/**
 * JSON Lines (NDJSON) backed implementation of {@link RowStorage}. Rows are
 * kept in an in-memory buffer and persisted as one JSON object per line
 * ({@code <table>.jsonl}, UTF-8, {@code \n} separator).
 *
 * <p>Base implementation (prompt 40), explicitly out of scope here: no
 * secondary Java-serialised .table mirror (prompt 50), no compression
 * (prompt 52), no append-only mode (prompt 49). Type validation is wired
 * through the shared {@link JsonlSchemaManager} on both read and write
 * (prompt 41); strict coercion lands in prompt 43 and the flatten /
 * json_column storage rules in prompt 45 are implemented here (one shared
 * {@link JsonlSchemaManager} is kept across the load&rarr;save lifecycle so the
 * nested-JSON holder columns captured on read are re-embedded as structure on
 * write). Schema inference and evolution
 * (prompt 44) are driven by {@code jsonl.schema.mode}: {@code strict} loads
 * against the fixed schema and rejects unknown fields with a typo hint,
 * {@code inferred} derives the schema from the data on first load and
 * {@code hybrid} (default) keeps the schema columns mandatory and typed
 * while expanding the schema with new fields observed in the data. The
 * adopted schema is persisted to the {@code <table>.schema.json} sidecar
 * together with a data-file mtime/size stamp that detects staleness and
 * triggers re-inference. Nested object/array values follow the configured
 * {@code jsonl.nested.mode}: {@code json_column} stores them in a column as
 * compact JSON text, {@code flatten} stores every nested leaf in its own
 * dot-notation column and scalar arrays may expand to index columns under
 * {@code jsonl.array.columns = expand}.
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
    private RowArrays rowColumns;
    private final JsonParserConfig config;
    private JsonlSchemaManager sharedSchemaManager;
    private boolean fileInitialized;

    public JsonlRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(tableName, columns, columnTypes, JsonParserConfig.defaults());
    }

    /**
     * @param tableName   the table name
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param config      the streaming JSON configuration (backend, limits,
     *                    duplicate-key policy, schema mode) used for every
     *                    line and for the schema sidecar (prompt 44)
     */
    public JsonlRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes,
                           JsonParserConfig config) {
        super(tableName, columns, columnTypes);
        this.config = config != null ? config : JsonParserConfig.defaults();
        this.rowColumns = new RowArrays(columns);
        this.sharedSchemaManager = new JsonlSchemaManager(columns, columnTypes, this.config);
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
                 JsonlRowWriter jsonlWriter = new JsonlRowWriter(afw.bufferedWriter(), sharedSchemaManager)) {
                for (Object[] row : rows) {
                    jsonlWriter.writeRow(row);
                }
                jsonlWriter.flush();
                afw.commit();
            }
            fileInitialized = true;
            if (config.schemaMode() != JsonParserConfig.SchemaMode.STRICT) {
                writeSchemaSidecar();
            }
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
        try {
            SchemaPlan plan = planSchemaForLoad(file);
            sharedSchemaManager = new JsonlSchemaManager(plan.columns(), plan.columnTypes(), config);
            List<Object[]> loaded = new ArrayList<>();
            try (BufferedReader br = Files.newBufferedReader(file.toPath(), StorageConfig.getCharset());
                 JsonlRowReader jsonlReader = new JsonlRowReader(br, sharedSchemaManager,
                         file.getPath(), config)) {
                while (jsonlReader.hasNext()) {
                    Object[] row = jsonlReader.nextArray();
                    if (row != null) {
                        loaded.add(row);
                    }
                }
            }
            adoptSchema(plan);
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.info("JsonlRowStorage {} loaded from {} with {} rows",
                    tableName, file.getPath(), rows.size());
            syncIndexBulkFromArrays(rows);
            if (plan.writeSidecar()) {
                writeSchemaSidecar();
            }
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            sharedSchemaManager = new JsonlSchemaManager(new ArrayList<>(columns), copyTypes(columnTypes), config);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            sharedSchemaManager = new JsonlSchemaManager(new ArrayList<>(columns), copyTypes(columnTypes), config);
            throw new DieselIOException("Failed to load table from JSONL file: " + file.getPath(), e);
        }
    }

    // ─── Schema mode planning (prompt 44) ──────────────────────────

    /** The columns, types and sidecar policy decided before a load pass. */
    private record SchemaPlan(List<String> columns, Map<String, Class<?>> columnTypes, boolean writeSidecar) {
    }

    /**
     * Decides which schema a load pass reads against, per the configured
     * {@code jsonl.schema.mode} (prompt 44):
     * <ul>
     * <li>{@code strict} - always the current schema; unknown fields fail at
     * the reader with a typo hint;</li>
     * <li>{@code hybrid} - current schema stays mandatory and typed, fields
     * observed in the data (or recorded by a fresh sidecar) that are not in
     * the schema are appended with their inferred types; the sidecar is
     * rewritten;</li>
     * <li>{@code inferred} - the data-derived schema replaces the current one;
     * a fresh sidecar is reused, otherwise inference runs and the sidecar is
     * written.</li>
     * </ul>
     */
    private SchemaPlan planSchemaForLoad(File file) throws IOException {
        JsonParserConfig.SchemaMode mode = config.schemaMode();
        if (mode == JsonParserConfig.SchemaMode.STRICT) {
            return new SchemaPlan(new ArrayList<>(columns), copyTypes(columnTypes), false);
        }
        Path sidecarPath = new File(resolveFilePath(JsonlSchemaManager.SCHEMA_FILE_SUFFIX)).toPath();
        JsonlSchemaManager schemaManager = new JsonlSchemaManager(columns, columnTypes, config);
        JsonlSchemaManager.SchemaDescriptor sidecar = schemaManager.readSchemaFile(sidecarPath);
        boolean fresh = sidecar != null && JsonlSchemaManager.SchemaStamp.isFresh(sidecar.data(), file.toPath());
        if (mode == JsonParserConfig.SchemaMode.INFERRED) {
            if (fresh) {
                return planFromDescriptor(sidecar, false);
            }
            return planFromDescriptor(infer(file), true);
        }
        // HYBRID: keep the current columns, append the fields the data adds.
        List<String> planColumns = new ArrayList<>(columns);
        Map<String, Class<?>> planTypes = copyTypes(columnTypes);
        if (fresh) {
            for (JsonlSchemaManager.SchemaColumn column : sidecar.columns()) {
                if (indexOfIgnoreCase(planColumns, column.name()) < 0) {
                    planColumns.add(column.name());
                    planTypes.put(column.name(), JsonlSchemaManager.typeClass(column.type()));
                }
            }
            return new SchemaPlan(planColumns, planTypes, false);
        }
        JsonSchemaInference.InferredSchema inferred = infer(file);
        for (String field : inferred.columns()) {
            if (indexOfIgnoreCase(planColumns, field) < 0) {
                planColumns.add(field);
                planTypes.put(field, inferred.columnTypes().get(field));
            }
        }
        return new SchemaPlan(planColumns, planTypes, true);
    }

    /** Builds a plan from a (fresh) sidecar descriptor. */
    private static SchemaPlan planFromDescriptor(JsonlSchemaManager.SchemaDescriptor sidecar, boolean writeSidecar) {
        List<String> planColumns = new ArrayList<>();
        Map<String, Class<?>> planTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (JsonlSchemaManager.SchemaColumn column : sidecar.columns()) {
            planColumns.add(column.name());
            planTypes.put(column.name(), JsonlSchemaManager.typeClass(column.type()));
        }
        return new SchemaPlan(planColumns, planTypes, writeSidecar);
    }

    /** Builds a plan from a fresh inference result (inferred mode adopts it fully). */
    private static SchemaPlan planFromDescriptor(JsonSchemaInference.InferredSchema inferred, boolean writeSidecar) {
        List<String> planColumns = new ArrayList<>(inferred.columns());
        Map<String, Class<?>> planTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        planTypes.putAll(inferred.columnTypes());
        return new SchemaPlan(planColumns, planTypes, writeSidecar);
    }

    /** Runs the single-pass schema inference over the data file. */
    private JsonSchemaInference.InferredSchema infer(File file) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(file.toPath(), StorageConfig.getCharset())) {
            return JsonSchemaInference.infer(br, file.getPath(), config);
        }
    }

    /** Applies the planned schema to the storage (and re-aligns the row mapper). */
    private void adoptSchema(SchemaPlan plan) {
        setSchema(plan.columns(), plan.columnTypes());
        this.rowColumns = new RowArrays(plan.columns());
    }

    /** Writes the {@code <table>.schema.json} sidecar with the current data stamp. */
    private void writeSchemaSidecar() {
        Path sidecarPath = new File(resolveFilePath(JsonlSchemaManager.SCHEMA_FILE_SUFFIX)).toPath();
        Path dataFile = new File(resolveFilePath(".jsonl")).toPath();
        try {
            sharedSchemaManager.writeSchemaFile(sidecarPath, JsonlSchemaManager.SchemaStamp.of(dataFile));
            LOGGER.info("JsonlRowStorage {} wrote JSONL schema sidecar {}", tableName, sidecarPath);
        } catch (IOException e) {
            LOGGER.warn("Failed to write JSONL schema sidecar {}: {}", sidecarPath, e.getMessage());
        }
    }

    private static Map<String, Class<?>> copyTypes(Map<String, Class<?>> types) {
        Map<String, Class<?>> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copy.putAll(types);
        return copy;
    }

    private static int indexOfIgnoreCase(List<String> names, String name) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
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