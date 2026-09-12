package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
 */
public class CsvRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = Logger.getLogger(CsvRowStorage.class.getName());

    protected final List<Map<String, Object>> rows = new ArrayList<>();
    private boolean fileInitialized;

    public CsvRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
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
        return new ArrayList<>(rows);
    }

    @Override
    public void insert(Map<String, Object> row) {
        Map<String, Object> copy = new HashMap<>(row);
        rows.add(copy);
        syncIndexAppend(copy, rows.size() - 1);
    }

    @Override
    public void insertAt(int rowIndex, Map<String, Object> row) {
        rows.add(rowIndex, new HashMap<>(row));
        syncIndexInsert(rows.get(rowIndex), rowIndex);
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        Map<String, Object> oldRow = rows.get(rowIndex);
        rows.set(rowIndex, new HashMap<>(row));
        syncIndexUpdate(oldRow, rowIndex, rows.get(rowIndex));
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
        String csvFile = resolveFilePath(".csv");
        String tableFile = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        String loadMode = resolveLoadMode("csv.load.mode");
        if (resolveLoadSource(csvFile, tableFile, loadMode) == LoadSource.SERIALIZED) {
            SerializedTableData data = readSerializedTable(tableFile);
            if (data != null) {
                List<String> problems = checkSerializedConsistency(data);
                if (problems.isEmpty() && delimitedHeaderConsistent(csvFile)) {
                    rows.clear();
                    rows.addAll(data.rows);
                    fileInitialized = true;
                    LOGGER.log(Level.INFO, "CsvRowStorage {0} loaded serialised from {1} with {2} rows",
                            new Object[]{tableName, tableFile, rows.size()});
                    syncIndexBulk();
                    return;
                }
                LOGGER.log(Level.WARNING,
                        "CsvRowStorage {0} serialised fast path rejected ({1}), falling back to delimited file {2}",
                        new Object[]{tableName, String.join("; ", problems), csvFile});
            }
        }
        loadCsv(tableName);
    }

    // ─── CSV persistence ────────────────────────────────────────────

    private void saveCsv(String tableName) {
        String fileName = resolveFilePath(".csv");
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName));
             CsvRowWriter csvWriter = new CsvRowWriter(afw.bufferedWriter(), columns)) {
            csvWriter.writeHeader();
            for (Map<String, Object> row : rows) {
                csvWriter.writeRow(row);
            }
            csvWriter.flush();
            afw.commit();
            fileInitialized = true;
            LOGGER.log(Level.INFO, "CsvRowStorage {0} saved CSV to {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save CSV for {0}: {1}", new Object[]{tableName, fileName});
            throw new DieselIOException("Failed to save table to CSV file: " + fileName, e);
        }
    }

    private void loadCsv(String tableName) {
        String fileName = resolveFilePath(".csv");
        File file = new File(fileName);
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.log(Level.INFO, "CSV file {0} not found for storage {1}", new Object[]{fileName, tableName});
            return;
        }
        List<Map<String, Object>> previous = new ArrayList<>(rows);
        try (BufferedReader br = StorageConfig.newReader(new File(fileName));
             CsvRowReader csvReader = new CsvRowReader(br, columns, columnTypes, fileName)) {
            csvReader.readHeader();
            List<Map<String, Object>> loaded = new ArrayList<>();
            while (csvReader.hasNext()) {
                Map<String, Object> row = csvReader.next();
                if (row != null) {
                    loaded.add(row);
                }
            }
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.log(Level.INFO, "CsvRowStorage {0} loaded CSV from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
            syncIndexBulk();
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            throw new DieselIOException("Failed to load table from CSV file: " + fileName, e);
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
            LOGGER.log(Level.INFO, "CsvRowStorage {0} saved serialised to {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save serialised file for {0}: {1}",
                    new Object[]{tableName, fileName});
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    /**
     * Validates that the delimited file header matches the schema (names must
     * all be present, per prompt-24 semantics). Used as one of the consistency
     * gates for the serialised fast load path. A missing delimited file is
     * tolerated (the .table is then the only source).
     */
    private boolean delimitedHeaderConsistent(String csvFile) {
        File file = new File(csvFile);
        if (!file.exists()) {
            return true;
        }
        try (BufferedReader br = StorageConfig.newReader(file)) {
            new CsvRowReader(br, columns, columnTypes, csvFile).readHeader();
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "CsvRowStorage header consistency check failed for {0}: {1}",
                    new Object[]{csvFile, e.getMessage()});
            return false;
        }
    }

    // ─── Internal helpers ───────────────────────────────────────────

    /** Returns the internal row list directly (no copy). */
    public List<Map<String, Object>> getInternalRows() {
        return rows;
    }

    /** Replaces the internal row list. */
    public void setRows(List<Map<String, Object>> newRows) {
        rows.clear();
        rows.addAll(newRows);
        syncIndexBulk();
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
        String fileName = resolveFilePath(".csv");
        Path file = new File(fileName).toPath();
        if (!Files.exists(file)) {
            AtomicFileWriter.warnInterruptedWrite(file);
            return;
        }
        DelimitedIndexManager manager = index();
        List<Map<String, Object>> loaded = parallel
                ? manager.loadFromFileParallel(fileName)
                : manager.loadFromFileSequential(fileName);
        rows.clear();
        rows.addAll(loaded);
        fileInitialized = true;
        manager.buildIndexes(rows, getPrimaryKeyColumn());
    }
}