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
        // No resources to acquire for CSV in-memory buffer.
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
        saveCsv(tableName);
        saveSerialized(tableName);
    }

    @Override
    public void loadFromFile(String tableName) {
        loadCsv(tableName);
    }

    // ─── CSV persistence ────────────────────────────────────────────

    private void saveCsv(String tableName) {
        String fileName = resolveFilePath(".csv");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, false));
             CsvRowWriter csvWriter = new CsvRowWriter(bw, columns)) {
            csvWriter.writeHeader();
            for (Map<String, Object> row : rows) {
                csvWriter.writeRow(row);
            }
            csvWriter.flush();
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
            LOGGER.log(Level.INFO, "CSV file {0} not found for storage {1}", new Object[]{fileName, tableName});
            return;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(fileName));
             CsvRowReader csvReader = new CsvRowReader(br, columns, columnTypes)) {
            csvReader.readHeader();
            rows.clear();
            while (csvReader.hasNext()) {
                rows.add(csvReader.next());
            }
            fileInitialized = true;
            LOGGER.log(Level.INFO, "CsvRowStorage {0} loaded CSV from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to load CSV for {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }

    // ─── Serialised .table persistence ──────────────────────────────

    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
            oos.writeObject(new SerializableAdapter(columns, new ArrayList<>(rows)));
            oos.flush();
            LOGGER.log(Level.INFO, "CsvRowStorage {0} saved serialised to {1} with {2} rows",
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
            LOGGER.log(Level.INFO, "CsvRowStorage {0} loaded serialised from {1} with {2} rows",
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
     * Lightweight serialisation adapter for CSV storage persistence.
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