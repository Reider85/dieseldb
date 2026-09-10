package diesel.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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
        // No resources to acquire for TSV in-memory buffer.
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
        rows.add(new HashMap<>(row));
    }

    @Override
    public void update(int rowIndex, Map<String, Object> row) {
        rows.set(rowIndex, new HashMap<>(row));
    }

    @Override
    public void delete(int rowIndex) {
        rows.remove(rowIndex);
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
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, false));
             TsvRowWriter tsvWriter = new TsvRowWriter(bw, columns)) {
            tsvWriter.writeHeader();
            for (Map<String, Object> row : rows) {
                tsvWriter.writeRow(row);
            }
            tsvWriter.flush();
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
            LOGGER.log(Level.INFO, "TSV file {0} not found for storage {1}", new Object[]{fileName, tableName});
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(fileName));
             TsvRowReader tsvReader = new TsvRowReader(br, columns, columnTypes)) {
            tsvReader.readHeader();
            rows.clear();
            while (tsvReader.hasNext()) {
                rows.add(tsvReader.next());
            }
            fileInitialized = true;
            LOGGER.log(Level.INFO, "TsvRowStorage {0} loaded TSV from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to load TSV for {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }

    // ─── Serialised .table persistence ──────────────────────────────

    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
            oos.writeObject(new SerializableAdapter(columns, new ArrayList<>(rows)));
            oos.flush();
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
