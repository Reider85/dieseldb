package diesel.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;
import diesel.ErrorMessages;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonSchemaInference;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;

/**
 * JSON Lines (NDJSON) backed implementation of {@link RowStorage}. Rows are
 * kept in an in-memory buffer and persisted as one JSON object per line
 * ({@code <table>.jsonl}, UTF-8, {@code \n} separator).
 *
 * <p>Base implementation (prompt 40), explicitly out of scope here: no
 * secondary Java-serialised .table mirror (prompt 50). Compression (prompt
 * 52) is wired through the shared {@link CompressionFactory} keyed by the
 * {@code jsonl.compression.codec} config; changing the codec only affects new
 * writes while existing files keep being read by their actual format. Type
 * validation is wired
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
 * <p>Null semantics (prompt 47): a per-row presence mask keeps the three
 * states of a record distinct on save. Rows loaded from a .jsonl file carry
 * the presence flags the reader observed ({@code null} field vs absent key),
 * so a load&rarr;save cycle writes an explicit {@code null} back as JSON
 * {@code null} and an absent key back as an omitted key. Rows inserted in
 * memory mark every column present. Reading is governed by the
 * {@code jsonl.missing.field} policy in {@link JsonParserConfig}.
 *
 * <p>Malformed rows are governed by the {@code jsonl.load.error.mode} policy
 * (prompt 48): {@code fail} (default) aborts the load, rolls back to the
 * previous in-memory state and rethrows the {@link DieselIOException} with
 * {@code file:line}/field diagnostics; {@code skip_row} logs each bad row's
 * coordinates, continues loading the valid rows and commits the result (the
 * reader reports the total skipped count as a final WARNING).
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
 *
 * <p>Write modes (prompt 49): the {@code jsonl.write.mode} config controls
 * how data is persisted. {@code rewrite} (default) performs a full atomic
 * rewrite via {@link AtomicFileWriter} on every save (identical to CSV/TSV).
 * {@code append} tracks new rows and deletions in a {@link JsonlDeltaManager}
 * and writes only deltas to the base file (append + fsync) with an atomic
 * delta sidecar; automatic compaction is triggered when the delta ratio
 * exceeds {@code jsonl.compaction.threshold} (default 0.3).
 */
public class JsonlRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlRowStorage.class);

    /** JSONL compression codec config key (prompt 52): none | zstd | lz4 | snappy. */
    private static final String COMPRESSION_CODEC_KEY = "jsonl.compression.codec";

    /** JSONL compression level config key (prompt 52, ZSTD: 1..22, default 3). */
    private static final String COMPRESSION_LEVEL_KEY = "jsonl.compression.level";

    protected final List<Object[]> rows = new ArrayList<>();
    private RowArrays rowColumns;
    private final JsonParserConfig config;
    private JsonlSchemaManager sharedSchemaManager;
    private boolean fileInitialized;
    /**
     * Per-row present-column flags (prompt 47), parallel to {@link #rows}:
     * index {@code i} marks which schema columns the row actually carried
     * (explicitly, possibly as JSON {@code null}). Used on save to omit the
     * keys that were absent so the null-vs-missing distinction survives a
     * load&rarr;save round trip.
     */
    private final List<boolean[]> rowPresence = new ArrayList<>();

    /** Write mode: REWRITE (full atomic rewrite) or APPEND (delta-tracked, prompt 49). */
    private final JsonParserConfig.WriteMode writeMode;

    /** Delta manager for append mode; null in rewrite mode (prompt 49). */
    private JsonlDeltaManager deltaManager;

    public JsonlRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(tableName, columns, columnTypes, JsonParserConfig.defaults());
    }

    /**
     * @param tableName   the table name
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param config      the streaming JSON configuration (backend, limits,
     *                    duplicate-key policy, schema mode, write mode,
     *                    compaction threshold) used for every line and for
     *                    the schema sidecar (prompt 44, prompt 49)
     */
    public JsonlRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes,
                           JsonParserConfig config) {
        super(tableName, columns, columnTypes);
        this.config = config != null ? config : JsonParserConfig.defaults();
        this.rowColumns = new RowArrays(columns);
        this.sharedSchemaManager = new JsonlSchemaManager(columns, columnTypes, this.config);
        this.writeMode = this.config.writeMode();
        if (this.writeMode == JsonParserConfig.WriteMode.APPEND) {
            this.deltaManager = new JsonlDeltaManager(this.config.compactionThreshold());
        }
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
        boolean[] present = allPresent(rowColumns.size());
        rowPresence.add(present);
        syncIndexAppend(arr, rows.size() - 1);
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.onInsert(arr, present);
        }
    }

    @Override
    public void insertAt(int rowIndex, Map<String, Object> row) {
        Object[] arr = rowColumns.fromMap(row);
        rows.add(rowIndex, arr);
        boolean[] present = allPresent(rowColumns.size());
        rowPresence.add(rowIndex, present);
        syncIndexInsert(arr, rowIndex);
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.onInsert(arr, present);
        }
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        Object[] oldRow = rows.get(rowIndex);
        Object[] newRow = rowColumns.fromMap(row);
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.onDelete(rowIndex, rows);
        }
        rows.set(rowIndex, newRow);
        boolean[] present = allPresent(rowColumns.size());
        rowPresence.set(rowIndex, present);
        syncIndexUpdate(oldRow, rowIndex, newRow);
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.onInsert(newRow, present);
        }
    }

    @Override
    public void delete(int rowIndex) {
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.onDelete(rowIndex, rows);
        }
        rows.remove(rowIndex);
        rowPresence.remove(rowIndex);
        syncIndexDelete(rowIndex);
        checkAutoCompact();
    }

    private static boolean[] allPresent(int size) {
        boolean[] present = new boolean[size];
        Arrays.fill(present, true);
        return present;
    }

    // ─── Persistence ────────────────────────────────────────────────

    @Override
    public void saveToFile(String tableName) {
        if (writeMode == JsonParserConfig.WriteMode.APPEND) {
            saveAppendMode(tableName);
        } else {
            saveRewriteMode(tableName);
        }
        if (isTableMirrorEnabled("jsonl.table.mirror")) {
            saveSerialized(tableName);
        }
    }

    /**
     * Full atomic rewrite via AtomicFileWriter (prompt 30). Used in REWRITE
     * mode and also for compaction in APPEND mode. Compression (prompt 52) is
     * applied at the file boundary: the configured codec decides the physical
     * write target ({@code .jsonl} vs {@code .jsonl.zst} / {@code .lz4} /
     * {@code .snappy}) and wraps the stream, while the row code stays
     * identical for plain and compressed paths.
     */
    private void saveRewriteMode(String tableName) {
        CompressionCodec codec = CompressionFactory.resolveLeveled(COMPRESSION_CODEC_KEY, COMPRESSION_LEVEL_KEY);
        File base = new File(resolveFilePath(".jsonl"));
        String fileName = CompressionFactory.delimitedWriteTarget(base, codec).getPath();
        try {
            if (codec.isNone()) {
                saveRewritePlain(fileName);
            } else {
                saveRewriteCompressed(fileName, codec);
            }
            fileInitialized = true;
            if (config.schemaMode() != JsonParserConfig.SchemaMode.STRICT) {
                writeSchemaSidecar();
            }
            // After a full rewrite, reset the delta manager
            if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
                deltaManager.reset();
                // Re-initialize with current rows as base
                deltaManager.onLoad(rows, rowPresence, rows.size());
            }
            LOGGER.info("JsonlRowStorage {} saved JSONL (rewrite) to {} with {} rows",
                    tableName, fileName, rows.size());
        } catch (IOException e) {
            LOGGER.error("Failed to save JSONL for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to JSONL file: " + fileName, e);
        }
    }

    /** Writes the plain (uncompressed) JSONL file - the pre-prompt-52 format. */
    private void saveRewritePlain(String fileName) throws IOException {
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName));
             JsonlRowWriter jsonlWriter = new JsonlRowWriter(afw.bufferedWriter(), sharedSchemaManager)) {
            for (int i = 0; i < rows.size(); i++) {
                Object[] row = rows.get(i);
                boolean[] present = i < rowPresence.size() ? rowPresence.get(i) : null;
                jsonlRowWriterWrite(jsonlWriter, row, present);
            }
            jsonlWriter.flush();
            afw.commit();
        }
    }

    /**
     * Writes the JSONL file through the configured compressor. The compressor
     * finishes its frame (and is closed) before {@link AtomicFileWriter#commit()}
     * so the fsync'd file is complete and self-contained (same contract as the
     * CSV/TSV compressed writers, prompt 39).
     */
    private void saveRewriteCompressed(String fileName, CompressionCodec codec) throws IOException {
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
            OutputStream compressed = codec.wrapOutputStream(CompressionFactory.nonClosing(afw.outputStream()));
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(compressed, StorageConfig.getCharset()));
                 JsonlRowWriter jsonlWriter = new JsonlRowWriter(writer, sharedSchemaManager)) {
                for (int i = 0; i < rows.size(); i++) {
                    Object[] row = rows.get(i);
                    boolean[] present = i < rowPresence.size() ? rowPresence.get(i) : null;
                    jsonlRowWriterWrite(jsonlWriter, row, present);
                }
                jsonlWriter.flush();
            }
            afw.commit();
        }
    }

    /**
     * Writes a Java-serialised .table mirror for the auto_mtime fast load path.
     * The serialised file carries the same {@link SerializedTableData} format
     * used by CSV/TSV (prompt 32) so the shared consistency checks apply.
     */
    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
            ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
            oos.writeObject(new SerializedTableData(CURRENT_STORAGE_FORMAT_VERSION, columns, columnTypes,
                    new ArrayList<>(rows)));
            oos.flush();
            afw.commit();
            LOGGER.info("JsonlRowStorage {} saved serialised to {} with {} rows",
                    tableName, fileName, rows.size());
        } catch (IOException e) {
            LOGGER.error("Failed to save serialised file for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    /**
     * Append mode: new rows are appended to the base file, deltas are
     * tracked in the .jsonl.delta sidecar (prompt 49).
     */
    private void saveAppendMode(String tableName) {
        if (deltaManager == null) {
            saveRewriteMode(tableName);
            return;
        }
        String fileName = resolveFilePath(".jsonl");
        String deltaFileName = resolveFilePath(JsonlDeltaManager.DELTA_FILE_SUFFIX);
        List<Object[]> pendingNew = deltaManager.getPendingNewRows();
        List<boolean[]> pendingPresence = deltaManager.getPendingNewPresence();
        // If no new rows and no deletions, nothing to save
        if (pendingNew.isEmpty() && deltaManager.getDeletedBaseLines().isEmpty()) {
            LOGGER.debug("JsonlRowStorage {} nothing to save in append mode", tableName);
            return;
        }
        // Append mode is not supported with compression (compressed streams are
        // frame-based and cannot be appended to); fall back to a full rewrite.
        CompressionCodec codec = CompressionFactory.resolveLeveled(COMPRESSION_CODEC_KEY, COMPRESSION_LEVEL_KEY);
        if (!codec.isNone()) {
            LOGGER.info("JsonlRowStorage {} jsonl.compression.codec={} is not none, "
                    + "append mode falls back to a full rewrite", tableName, codec.name());
            saveRewriteMode(tableName);
            return;
        }
        try {
            // Append new rows to the base file
            if (!pendingNew.isEmpty()) {
                File baseFile = new File(fileName);
                boolean append = baseFile.exists();
                try (FileOutputStream fos = new FileOutputStream(baseFile, append);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, StorageConfig.getCharset());
                     Writer bw = new java.io.BufferedWriter(osw, StorageConfig.bufferSize());
                     JsonlRowWriter jsonlWriter = new JsonlRowWriter(bw, sharedSchemaManager, config)) {
                    for (int i = 0; i < pendingNew.size(); i++) {
                        Object[] row = pendingNew.get(i);
                        boolean[] present = i < pendingPresence.size() ? pendingPresence.get(i) : null;
                        jsonlRowWriterWrite(jsonlWriter, row, present);
                    }
                    jsonlWriter.flush();
                    fos.flush();
                    fos.getChannel().force(true); // fsync
                }
                fileInitialized = true;
            }
            // Write the delta file atomically (deletions only; new rows are in the base)
            deltaManager.writeDeltaFile(deltaFileName);
            // After successful save, update delta manager state
            deltaManager.onSaveComplete();
            if (config.schemaMode() != JsonParserConfig.SchemaMode.STRICT) {
                writeSchemaSidecar();
            }
            LOGGER.info("JsonlRowStorage {} saved JSONL (append) to {} with {} base + {} new rows, {} deletions",
                    tableName, fileName, deltaManager.getBaseFileLineCount(),
                    pendingNew.size(), deltaManager.getDeletedBaseLines().size());
        } catch (IOException e) {
            LOGGER.error("Failed to save JSONL (append) for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to JSONL file: " + fileName, e);
        }
    }

    /**
     * Writes a row through the JsonlRowWriter (Map boundary not needed for Object[]).
     */
    private static void jsonlRowWriterWrite(JsonlRowWriter writer, Object[] row, boolean[] present) throws IOException {
        writer.writeRow(row, present);
    }

    @Override
    public void loadFromFile(String tableName) {
        CompressionFactory.ResolvedDelimitedFile ref = resolveJsonlFile();
        String jsonlFile = ref.file().getPath();
        String tableFile = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        String loadMode = resolveLoadMode("jsonl.load.mode");

        if (resolveLoadSource(jsonlFile, tableFile, loadMode) == LoadSource.SERIALIZED) {
            SerializedTableData data = readSerializedTable(tableFile);
            if (data != null) {
                List<String> problems = checkSerializedConsistency(data);
                if (problems.isEmpty()) {
                    List<Object[]> previous = new ArrayList<>(rows);
                    List<boolean[]> previousPresence = new ArrayList<>(rowPresence);
                    try {
                        setSchema(data.columns, data.columnTypes);
                        this.rowColumns = new RowArrays(data.columns);
                        rows.clear();
                        rowPresence.clear();
                        for (Object row : data.rows) {
                            rows.add((Object[]) row);
                            rowPresence.add(allPresent(rowColumns.size()));
                        }
                        fileInitialized = true;
                        LOGGER.info("JsonlRowStorage {} loaded serialised from {} with {} rows",
                                tableName, tableFile, rows.size());
                        syncIndexBulkFromArrays(rows);
                        return;
                    } catch (Exception e) {
                        rows.clear();
                        rows.addAll(previous);
                        rowPresence.clear();
                        rowPresence.addAll(previousPresence);
                        sharedSchemaManager = new JsonlSchemaManager(new ArrayList<>(columns), copyTypes(columnTypes), config);
                        LOGGER.warn("JsonlRowStorage {} serialised fast path rejected ({}), falling back to JSONL {}",
                                tableName, e.getMessage(), jsonlFile);
                    }
                } else {
                    LOGGER.warn("JsonlRowStorage {} serialised fast path rejected ({}), falling back to JSONL {}",
                            tableName, String.join("; ", problems), jsonlFile);
                }
            }
        }

        File file = ref.file();
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.info("JSONL file {} not found for storage {}", file.getPath(), tableName);
            // In append mode, still try to load delta for new rows
            if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
                loadFromDeltaOnly(tableName);
            }
            return;
        }
        List<Object[]> previous = new ArrayList<>(rows);
        List<boolean[]> previousPresence = new ArrayList<>(rowPresence);
        try {
            // Crash recovery for append mode (prompt 49). Compressed files are
            // always complete full rewrites (append+compression falls back to
            // rewrite), and RandomAccessFile cannot address compressed bytes,
            // so the repair only applies to plain files.
            if (writeMode == JsonParserConfig.WriteMode.APPEND && ref.codec().isNone()) {
                repairTruncatedAppend(file);
            }
            SchemaPlan plan = planSchemaForLoad(file, ref.codec());
            sharedSchemaManager = new JsonlSchemaManager(plan.columns(), plan.columnTypes(), config);
            List<Object[]> loaded = new ArrayList<>();
            List<boolean[]> loadedPresence = new ArrayList<>();
            int lineCount = 0;
            try (BufferedReader br = CompressionFactory.openDelimitedReader(file, ref.codec(), StorageConfig.getCharset());
                 JsonlRowReader jsonlReader = new JsonlRowReader(br, sharedSchemaManager,
                         file.getPath(), config)) {
                while (jsonlReader.hasNext()) {
                    Object[] row = jsonlReader.nextArray();
                    if (row != null) {
                        loaded.add(row);
                        boolean[] present = jsonlReader.getLastRowPresent();
                        loadedPresence.add(present != null ? present : allPresent(plan.columns().size()));
                        lineCount++;
                    }
                }
            }
            adoptSchema(plan);
            rows.clear();
            rows.addAll(loaded);
            rowPresence.clear();
            rowPresence.addAll(loadedPresence);
            fileInitialized = true;

            // In append mode, apply delta on top of the base
            if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
                String deltaPath = resolveFilePath(JsonlDeltaManager.DELTA_FILE_SUFFIX);
                deltaManager.onLoad(loaded, loadedPresence, lineCount);
                applyDelta(deltaPath);
            }

            LOGGER.info("JsonlRowStorage {} loaded from {} with {} rows",
                    tableName, file.getPath(), rows.size());
            syncIndexBulkFromArrays(rows);
            if (plan.writeSidecar()) {
                writeSchemaSidecar();
            }
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            rowPresence.clear();
            rowPresence.addAll(previousPresence);
            sharedSchemaManager = new JsonlSchemaManager(new ArrayList<>(columns), copyTypes(columnTypes), config);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            rowPresence.clear();
            rowPresence.addAll(previousPresence);
            sharedSchemaManager = new JsonlSchemaManager(new ArrayList<>(columns), copyTypes(columnTypes), config);
            throw new DieselIOException("Failed to load table from JSONL file: " + file.getPath(), e);
        }
    }

    /**
     * Loads only from the delta file path (base .jsonl missing - nothing to
     * reconstruct, new rows live in the base file, not the delta).
     */
    private void loadFromDeltaOnly(String tableName) {
        String deltaPath = resolveFilePath(JsonlDeltaManager.DELTA_FILE_SUFFIX);
        if (!new File(deltaPath).exists()) {
            return;
        }
        List<Object[]> previous = new ArrayList<>(rows);
        List<boolean[]> previousPresence = new ArrayList<>(rowPresence);
        try {
            deltaManager.readDeltaFile(deltaPath);
            // Without a base file there are no rows to delete from; keep the
            // delta file so a later-recovered base still applies it.
            LOGGER.info("JsonlRowStorage {} loaded with no base; {} deletions deferred",
                    tableName, deltaManager.getDeletedBaseLines().size());
            syncIndexBulkFromArrays(rows);
        } catch (Exception e) {
            rows.clear();
            rows.addAll(previous);
            rowPresence.clear();
            rowPresence.addAll(previousPresence);
            LOGGER.warn("Failed to load delta-only for {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * Applies the delta to the current in-memory rows after loading the base
     * file: removed the recorded base lines. New rows need no re-appending -
     * they are physically part of the base file.
     */
    private void applyDelta(String deltaPath) {
        File deltaFile = new File(deltaPath);
        if (!deltaFile.exists()) {
            return;
        }
        try {
            int result = deltaManager.readDeltaFile(deltaPath);
            if (result < 0) {
                LOGGER.warn("Corrupt delta file for {}: {}", tableName, deltaPath);
                return;
            }
            // Remove deleted lines (iterate in reverse to preserve indices)
            List<Integer> sortedDeletes = new ArrayList<>(deltaManager.getDeletedBaseLines());
            sortedDeletes.sort((a, b) -> b - a); // reverse order
            for (int lineNum : sortedDeletes) {
                if (lineNum < rows.size()) {
                    rows.remove(lineNum);
                    rowPresence.remove(lineNum);
                }
            }
            // The delta file is kept: its deletions are re-applied on each
            // subsequent load until a compaction rewrites the base file.
            LOGGER.info("Applied delta: deleted {} lines -> {} total",
                    sortedDeletes.size(), rows.size());
        } catch (Exception e) {
            LOGGER.warn("Failed to apply delta {}: {}", deltaPath, e.getMessage());
        }
    }

    // ─── Append-mode crash recovery (prompt 49) ───────────────────

    /**
     * Recovers from an append interrupted mid-write. Appends are written with
     * every row terminated by {@code '\n'}, so a base file that does not end in
     * {@code '\n'} carries an unterminated trailing fragment. If that fragment
     * is a complete JSON object we restore the missing terminator (the row is
     * fully written, just missing its newline before the next append); if it
     * is a truncated prefix it is discarded and the file is truncated at the
     * last intact newline.
     */
    private void repairTruncatedAppend(File file) {
        if (!file.exists() || file.length() == 0) {
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long length = raf.length();
            raf.seek(length - 1);
            if (raf.read() == '\n') {
                return; // properly terminated
            }
            long lineStart = lastIndexOfByte(raf, (byte) '\n', length);
            int tailLen = (int) (length - (lineStart + 1));
            if (tailLen <= 0) {
                return;
            }
            byte[] tail = new byte[tailLen];
            raf.seek(lineStart + 1);
            raf.readFully(tail);
            String lastLine = new String(tail, StandardCharsets.UTF_8);
            if (isCompleteJsonObject(lastLine)) {
                raf.seek(length);
                raf.write('\n');
                LOGGER.warn("JsonlRowStorage {} restored missing newline after interrupted append",
                        tableName);
            } else {
                raf.setLength(lineStart + 1);
                LOGGER.warn("JsonlRowStorage {} discarded {} bytes of truncated JSON after interrupted append",
                        tableName, tailLen);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to repair truncated append for {}: {}", tableName, e.getMessage());
        }
    }

    /** Index of the last {@code value} byte in {@code [0, limit)}, or -1. */
    private static long lastIndexOfByte(RandomAccessFile raf, byte value, long limit) throws IOException {
        long pos = limit;
        int chunkSize = 8192;
        while (pos > 0) {
            long readStart = Math.max(0, pos - chunkSize);
            int readLen = (int) (pos - readStart);
            byte[] chunk = new byte[readLen];
            raf.seek(readStart);
            raf.readFully(chunk);
            for (int i = readLen - 1; i >= 0; i--) {
                if (chunk[i] == value) {
                    return readStart + i;
                }
            }
            pos = readStart;
        }
        return -1;
    }

    /** Returns true if the text is one complete, well-formed JSON document. */
    private boolean isCompleteJsonObject(String text) {
        try (JsonStreamParser parser = JsonStreams.createParser(text, config)) {
            while (parser.nextToken() != JsonEvent.END_INPUT) {
                // drain tokens
            }
            return true;
        } catch (IOException e) {
            return false;
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
    private SchemaPlan planSchemaForLoad(File file, CompressionCodec codec) throws IOException {
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
            return planFromDescriptor(infer(file, codec), true);
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
        JsonSchemaInference.InferredSchema inferred = infer(file, codec);
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

    /** Runs the single-pass schema inference over the data file (codec-aware, prompt 52). */
    private JsonSchemaInference.InferredSchema infer(File file, CompressionCodec codec) throws IOException {
        try (BufferedReader br = CompressionFactory.openDelimitedReader(file, codec, StorageConfig.getCharset())) {
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
        Path dataFile = resolveJsonlFile().file().toPath();
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

    /** Resolves the physical JSONL file and its codec, transparent to the configured codec (prompt 52). */
    private CompressionFactory.ResolvedDelimitedFile resolveJsonlFile() {
        return CompressionFactory.resolveActual(new File(resolveFilePath(".jsonl")), COMPRESSION_CODEC_KEY);
    }

    /** Returns the internal row list directly (no copy). */
    public List<Object[]> getInternalRows() {
        return rows;
    }

    /** Replaces the internal row list. Used by compaction (prompt 49). */
    public void setRows(List<Map<String, Object>> newRows) {
        rows.clear();
        rowPresence.clear();
        for (Map<String, Object> row : newRows) {
            rows.add(rowColumns.fromMap(row));
            rowPresence.add(allPresent(rowColumns.size()));
        }
        syncIndexBulkFromArrays(rows);
        // After setRows (compaction), reset delta manager
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null) {
            deltaManager.reset();
            deltaManager.onLoad(rows, rowPresence, rows.size());
        }
    }

    /**
     * Checks whether automatic compaction is needed (prompt 49) and
     * triggers a full rewrite + delta reset if the threshold is exceeded.
     */
    private void checkAutoCompact() {
        if (writeMode == JsonParserConfig.WriteMode.APPEND && deltaManager != null
                && deltaManager.shouldCompact()) {
            LOGGER.info("Auto-compaction triggered for {}", tableName);
            compactJsonl();
        }
    }

    /**
     * Performs a full compaction of the JSONL file (prompt 49): rewrites the
     * base file atomically with only live rows and resets the delta manager.
     * This is equivalent to a full rewrite.
     */
    public void compactJsonl() {
        LOGGER.info("Compacting JSONL for {}: {} rows", tableName, rows.size());
        saveRewriteMode(tableName);
    }

    /** Returns the per-row present-column flags (prompt 47), parallel to the internal rows. */
    public List<boolean[]> getRowPresence() {
        return rowPresence;
    }

    /** Replaces the per-row present-column flags (must stay aligned with the row list). */
    public void setRowPresence(List<boolean[]> presence) {
        rowPresence.clear();
        for (boolean[] p : presence) {
            rowPresence.add(p != null ? p.clone() : null);
        }
    }

    /** Returns the write mode (prompt 49). */
    public JsonParserConfig.WriteMode getWriteMode() {
        return writeMode;
    }

    /** Returns the delta manager (null in REWRITE mode, prompt 49). */
    public JsonlDeltaManager getDeltaManager() {
        return deltaManager;
    }
}