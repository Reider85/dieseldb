package diesel.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;
import diesel.ErrorMessages;

/**
 * CSV-backed implementation of {@link RowStorage}. Rows are kept in an
 * in-memory buffer and persisted as comma-separated files ({@code .csv}).
 * A secondary Java-serialised {@code .table} file is also written for
 * fast round-trip loading.
 *
 * <p>CSV escaping follows RFC 4180: fields containing commas, quotes or
 * newlines are enclosed in double-quotes; literal double-quotes are
 * doubled ({@code ""}).
 *
 * <p>Null values are written as an empty field and read back as {@code null}.
 *
 * <p>Rows are kept internally as compact Object[] arrays (one per row, slot
 * {@code i} = value of schema column {@code i}) instead of per-row Maps
 * (prompt 36). Column-to-value Maps are built only at the Map-based API
 * boundary ({@link #scan()}, {@link #insert(Map)}, {@link #update(int, Map)}).
 */
public class CsvRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvRowStorage.class);

    private static final String COMPRESSION_CODEC_KEY = "csv.compression.codec";
    private static final String COMPRESSION_LEVEL_KEY = "csv.compression.level";

    protected final List<Object[]> rows = new ArrayList<>();
    private final RowArrays rowColumns;
    private boolean fileInitialized;

    public CsvRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
        this.rowColumns = new RowArrays(columns);
    }

    @Override
    protected DelimitedIndexManager createIndexManager() {
        return new CsvIndexManager(tableName, columns, columnTypes);
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
        indexManager = index();
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
        saveCsv(tableName);
        if (isTableMirrorEnabled("csv.table.mirror")) {
            saveSerialized(tableName);
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        CompressionFactory.ResolvedDelimitedFile actual = resolveDelimitedFile();
        String csvFile = actual.file().getPath();
        String tableFile = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        String loadMode = resolveLoadMode("csv.load.mode");
        if (resolveLoadSource(csvFile, tableFile, loadMode) == LoadSource.SERIALIZED) {
            SerializedTableData data = readSerializedTable(tableFile);
            if (data != null) {
                List<String> problems = checkSerializedConsistency(data);
                if (problems.isEmpty() && delimitedHeaderConsistent(actual)) {
                    rows.clear();
                    rows.addAll(convertSerializedRows(data));
                    fileInitialized = true;
                    LOGGER.info("CsvRowStorage {} loaded serialised from {} with {} rows",
                            tableName, tableFile, rows.size());
                    syncIndexBulkFromArrays(rows);
                    return;
                }
                LOGGER.warn("CsvRowStorage {} serialised fast path rejected ({}), falling back to delimited file {}",
                        tableName, String.join("; ", problems), csvFile);
            }
        }
        loadCsv(tableName);
    }

    // ─── CSV persistence ────────────────────────────────────────────

    private void saveCsv(String tableName) {
        CompressionCodec codec = CompressionFactory.resolveLeveled(COMPRESSION_CODEC_KEY, COMPRESSION_LEVEL_KEY);
        File base = new File(resolveFilePath(".csv"));
        String fileName = CompressionFactory.delimitedWriteTarget(base, codec).getPath();
        try {
            if (codec.isNone()) {
                saveCsvPlain(fileName);
            } else {
                saveCsvCompressed(fileName, codec);
            }
            fileInitialized = true;
            LOGGER.info("CsvRowStorage {} saved CSV to {} with {} rows",
                    tableName, fileName, rows.size());
        } catch (IOException e) {
            LOGGER.error("Failed to save CSV for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to CSV file: " + fileName, e);
        }
    }

    /** Writes the plain (uncompressed) CSV file - the pre-prompt-39 format. */
    private void saveCsvPlain(String fileName) throws IOException {
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName));
             CsvRowWriter csvWriter = new CsvRowWriter(afw.bufferedWriter(), columns)) {
            csvWriter.writeHeader();
            for (Object[] row : rows) {
                csvWriter.writeRow(row);
            }
            csvWriter.flush();
            afw.commit();
        }
    }

    /**
     * Writes the CSV file through the configured compressor. The compressor
     * finishes its frame (and is closed) before {@link AtomicFileWriter#commit()}
     * so the fsync'd file is complete and self-contained.
     */
    private void saveCsvCompressed(String fileName, CompressionCodec codec) throws IOException {
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
            OutputStream compressed = codec.wrapOutputStream(CompressionFactory.nonClosing(afw.outputStream()));
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(compressed, StorageConfig.getCharset()));
                 CsvRowWriter csvWriter = new CsvRowWriter(writer, columns)) {
                csvWriter.writeHeader();
                for (Object[] row : rows) {
                    csvWriter.writeRow(row);
                }
                csvWriter.flush();
            }
            afw.commit();
        }
    }

    private void loadCsv(String tableName) {
        CompressionFactory.ResolvedDelimitedFile ref = resolveDelimitedFile();
        File file = ref.file();
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.info("CSV file {} not found for storage {}", file.getPath(), tableName);
            return;
        }
        List<Object[]> previous = new ArrayList<>(rows);
        try (BufferedReader br = CompressionFactory.openDelimitedReader(file, ref.codec(), StorageConfig.getCharset());
             CsvRowReader csvReader = new CsvRowReader(br, columns, columnTypes, file.getPath())) {
            csvReader.readHeader();
            List<Object[]> loaded = new ArrayList<>();
            while (csvReader.hasNext()) {
                Object[] row = csvReader.nextArray();
                if (row != null) {
                    loaded.add(row);
                }
            }
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.info("CsvRowStorage {} loaded CSV from {} with {} rows",
                    tableName, file.getPath(), rows.size());
            syncIndexBulkFromArrays(rows);
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            throw new DieselIOException("Failed to load table from CSV file: " + file.getPath(), e);
        }
    }

    // ─── Serialised .table persistence ──────────────────────────────

    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
            ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
            oos.writeObject(new SerializedTableData(CURRENT_STORAGE_FORMAT_VERSION, columns, columnTypes,
                    new ArrayList<>(rows)));
            oos.flush();
            afw.commit();
            LOGGER.info("CsvRowStorage {} saved serialised to {} with {} rows",
                    tableName, fileName, rows.size());
        } catch (IOException e) {
            LOGGER.error("Failed to save serialised file for {}: {}", tableName, fileName);
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    /**
     * Validates that the delimited file header matches the schema (names must
     * all be present, per prompt-24 semantics). Used as one of the consistency
     * gates for the serialised fast load path. A missing delimited file is
     * tolerated (the .table is then the only source).
     */
    private boolean delimitedHeaderConsistent(CompressionFactory.ResolvedDelimitedFile ref) {
        File file = ref.file();
        if (!file.exists()) {
            return true;
        }
        try (BufferedReader br = CompressionFactory.openDelimitedReader(file, ref.codec(), StorageConfig.getCharset())) {
            new CsvRowReader(br, columns, columnTypes, file.getPath()).readHeader();
            return true;
        } catch (IOException e) {
            LOGGER.warn("CsvRowStorage header consistency check failed for {}: {}",
                    file.getPath(), e.getMessage());
            return false;
        }
    }

    /**
     * Converts the rows of a deserialised {@link SerializedTableData} snapshot
     * into compact Object[] arrays. Snapshots written after the prompt-36
     * switch already hold arrays (kept as-is and shared with the row buffer);
     * older snapshots holding Map rows are converted and detached.
     */
    private List<Object[]> convertSerializedRows(SerializedTableData data) {
        List<Object[]> converted = new ArrayList<>(data.rowCount);
        for (Object element : data.rows) {
            if (element instanceof Object[] arr) {
                converted.add(arr);
            } else if (element == null) {
                converted.add(new Object[columns.size()]);
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) element;
                converted.add(rowColumns.fromMap(map));
            }
        }
        return converted;
    }

    // ─── Internal helpers ───────────────────────────────────────────

    /** Resolves the physical delimited file, transparent to the configured codec. */
    private CompressionFactory.ResolvedDelimitedFile resolveDelimitedFile() {
        return CompressionFactory.resolveActual(new File(resolveFilePath(".csv")), COMPRESSION_CODEC_KEY);
    }

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

    // ─── Index / cache accessors (prompt 23) ──────────────────────

    /** Returns the indexed column names maintained by this storage. */
    public List<String> getIndexColumns() {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.getIndexColumns();
    }

    /**
     * Fast primary-key lookup.
     *
     * @param key the primary-key value
     * @return matching row indexes, or an empty list when absent
     */
    public List<Integer> searchByPrimaryKey(Object key) {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.searchByPrimaryKey(key);
    }

    /**
     * Equality search over the primary-key or a secondary index.
     *
     * @param column the column to search (case-insensitive)
     * @param key    the value to look up
     * @return matching row indexes, or an empty list when no index exists
     */
    public List<Integer> search(String column, Object key) {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.search(column, key);
    }

    /**
     * Inclusive range search over an indexed column.
     *
     * @param column the column to search (case-insensitive)
     * @param low    inclusive lower bound, or {@code null} for open-ended
     * @param high   inclusive upper bound, or {@code null} for open-ended
     * @return matching row indexes, or an empty list when no index exists
     */
    public List<Integer> rangeSearch(String column, Object low, Object high) {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.rangeSearch(column, low, high);
    }

    /** Returns the number of fixed-size blocks the rows are split into. */
    public int getNumBlocks() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getNumBlocks();
    }

    /** Returns the number of rows per cache block. */
    public int getBlockSize() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getBlockSize();
    }

    /**
     * Deprecated (prompt 33): the block cache layer was removed; returns a
     * fresh on-demand slice of the in-memory rows.
     *
     * @param blockIndex the zero-based block index
     * @return the sliced block
     */
    @Deprecated
    public DelimitedIndexManager.Block getBlock(int blockIndex) {
        return index().getBlock(blockIndex);
    }

    /** Deprecated (prompt 33): assembles the blocks sequentially on demand. */
    @Deprecated
    public List<DelimitedIndexManager.Block> loadAllBlocksParallel() {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.loadAllBlocksParallel();
    }

    /** Deprecated (prompt 33): the block cache no longer exists. */
    @Deprecated
    public long getCacheHitCount() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getCacheHitCount();
    }

    /** Deprecated (prompt 33): the block cache no longer exists. */
    @Deprecated
    public long getCacheMissCount() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getCacheMissCount();
    }

    /** Deprecated no-op (prompt 33): there is no cache to invalidate. */
    @Deprecated
    public void invalidateCache() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.invalidateCache();
        }
    }

    /**
     * Loads the CSV file (parallel when beneficial) and builds the primary-key
     * index, replacing the current rows.
     *
     * @param tableName the table name, used as the file base name
     * @param parallel  whether to allow the parallel read path
     * @throws java.io.IOException on I/O errors
     */
    public void loadFromFile(String tableName, boolean parallel) throws java.io.IOException {
        CompressionFactory.ResolvedDelimitedFile ref = resolveDelimitedFile();
        File file = ref.file();
        if (!Files.exists(file.toPath())) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            return;
        }
        boolean useParallel = parallel && !ref.compressed();
        DelimitedIndexManager manager = index();
        List<Map<String, Object>> loaded = useParallel
                ? manager.loadFromFileParallel(file.getPath())
                : manager.loadFromFileSequential(file.getPath());
        rows.clear();
        for (Map<String, Object> row : loaded) {
            rows.add(rowColumns.fromMap(row));
        }
        fileInitialized = true;
        syncIndexBulkFromArrays(rows);
    }
}