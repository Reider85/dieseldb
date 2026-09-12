package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
 * TSV-backed implementation of {@link RowStorage}. Rows are kept in an
 * in-memory buffer and persisted as tab-separated files ({@code .tsv}).
 * A secondary Java-serialised {@code .table} file is also written for
 * fast round-trip loading.
 *
 * <p>TSV escaping rules (backslash-based):
 * <ul>
 *   <li>tab character → {@code \t}</li>
 *   <li>newline → {@code \n}</li>
 *   <li>carriage-return → {@code \r}</li>
 *   <li>backslash → {@code \\}</li>
 * </ul>
 *
 * <p>Null values are written as an empty field and read back as {@code null}.
 */
public class TsvRowStorage extends AbstractRowStorage {

    private static final Logger LOGGER = Logger.getLogger(TsvRowStorage.class.getName());

    protected final List<Map<String, Object>> rows = new ArrayList<>();
    private boolean fileInitialized;

    public TsvRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
        super(tableName, columns, columnTypes);
    }

    @Override
    protected DelimitedIndexManager createIndexManager() {
        return new TsvIndexManager(tableName, columns, columnTypes);
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
        saveTsv(tableName);
        saveSerialized(tableName);
    }

    @Override
    public void loadFromFile(String tableName) {
        loadTsv(tableName);
    }

    // ─── TSV persistence ───────────────────────────────────────────

    private void saveTsv(String tableName) {
        String fileName = resolveFilePath(".tsv");
        try (AtomicFileWriter afw = AtomicFileWriter.openText(new File(fileName));
             TsvRowWriter tsvWriter = new TsvRowWriter(afw.bufferedWriter(), columns)) {
            tsvWriter.writeHeader();
            for (Map<String, Object> row : rows) {
                tsvWriter.writeRow(row);
            }
            tsvWriter.flush();
            afw.commit();
            fileInitialized = true;
            LOGGER.log(Level.INFO, "TsvRowStorage {0} saved TSV to {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save TSV for {0}: {1}", new Object[]{tableName, fileName});
            throw new DieselIOException("Failed to save table to TSV file: " + fileName, e);
        }
    }

    private void loadTsv(String tableName) {
        String fileName = resolveFilePath(".tsv");
        File file = new File(fileName);
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            LOGGER.log(Level.INFO, "TSV file {0} not found for storage {1}", new Object[]{fileName, tableName});
            return;
        }
        List<Map<String, Object>> previous = new ArrayList<>(rows);
        try (BufferedReader br = StorageConfig.newReader(new File(fileName));
             TsvRowReader tsvReader = new TsvRowReader(br, columns, columnTypes, fileName)) {
            tsvReader.readHeader();
            List<Map<String, Object>> loaded = new ArrayList<>();
            while (tsvReader.hasNext()) {
                Map<String, Object> row = tsvReader.next();
                if (row != null) {
                    loaded.add(row);
                }
            }
            rows.clear();
            rows.addAll(loaded);
            fileInitialized = true;
            LOGGER.log(Level.INFO, "TsvRowStorage {0} loaded TSV from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
            syncIndexBulk();
        } catch (DieselIOException e) {
            rows.clear();
            rows.addAll(previous);
            throw e;
        } catch (IOException e) {
            rows.clear();
            rows.addAll(previous);
            throw new DieselIOException("Failed to load table from TSV file: " + fileName, e);
        }
    }

    // ─── Serialised .table persistence ──────────────────────────────

    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (AtomicFileWriter afw = AtomicFileWriter.openBinary(new File(fileName))) {
            ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
            oos.writeObject(new SerializableAdapter(columns, new ArrayList<>(rows)));
            oos.flush();
            afw.commit();
            LOGGER.log(Level.INFO, "TsvRowStorage {0} saved serialised to {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save serialised file for {0}: {1}",
                    new Object[]{tableName, fileName});
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        File file = new File(fileName);
        if (!file.exists()) {
            AtomicFileWriter.warnInterruptedWrite(file.toPath());
            return;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            SerializableAdapter adapter = (SerializableAdapter) ois.readObject();
            rows.clear();
            rows.addAll(adapter.rows);
            fileInitialized = true;
            LOGGER.log(Level.INFO, "TsvRowStorage {0} loaded serialised from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Failed to load serialised file for {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
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
     * Returns the block with the given index, loading it into the LRU cache if
     * not already present.
     *
     * @param blockIndex the zero-based block index
     * @return the cached block
     */
    public DelimitedIndexManager.Block getBlock(int blockIndex) {
        return index().getBlock(blockIndex);
    }

    /** Pro-actively loads every block into the cache (parallel when large). */
    public List<DelimitedIndexManager.Block> loadAllBlocksParallel() {
        DelimitedIndexManager manager = index();
        return manager == null ? List.of() : manager.loadAllBlocksParallel();
    }

    /** Returns the number of block-cache hits. */
    public long getCacheHitCount() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getCacheHitCount();
    }

    /** Returns the number of block-cache misses. */
    public long getCacheMissCount() {
        DelimitedIndexManager manager = index();
        return manager == null ? 0 : manager.getCacheMissCount();
    }

    /** Discards all cached blocks. */
    public void invalidateCache() {
        DelimitedIndexManager manager = index();
        if (manager != null) {
            manager.invalidateCache();
        }
    }

    /**
     * Loads the TSV file (parallel when beneficial) and builds the primary-key
     * index, replacing the current rows.
     *
     * @param tableName the table name, used as the file base name
     * @param parallel  whether to allow the parallel read path
     * @throws java.io.IOException on I/O errors
     */
    public void loadFromFile(String tableName, boolean parallel) throws java.io.IOException {
        String fileName = resolveFilePath(".tsv");
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

    /**
     * Lightweight serialisation adapter for TSV storage persistence.
     */
    static class SerializableAdapter implements Serializable {
        private static final long serialVersionUID = 1L;
        final List<String> columns;
        final List<Map<String, Object>> rows;

        SerializableAdapter(List<String> columns, List<Map<String, Object>> rows) {
            this.columns = columns;
            this.rows = rows;
        }
    }
}
