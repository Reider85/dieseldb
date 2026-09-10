package diesel.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import diesel.DieselIOException;
import diesel.ErrorMessages;

/**
 * File-backed implementation of {@link RowStorage}. Extends
 * {@link InMemoryRowStorage} with CSV and Java-serialisation persistence,
 * reproducing the original {@code Table} dual-format behaviour.
 *
 * <p>On {@link #saveToFile} both a human-readable CSV and a serialised
 * {@code .table} file are written. On {@link #loadFromFile} the serialised
 * file is preferred; the CSV is only written by {@code saveToFile}.
 */
public class FileBasedRowStorage extends InMemoryRowStorage {

    private static final Logger LOGGER = Logger.getLogger(FileBasedRowStorage.class.getName());
    private boolean fileInitialized;

    /**
     * @param tableName   the table name
     * @param columns     the ordered list of column names
     * @param columnTypes the column name to type mapping
     */
    public FileBasedRowStorage(String tableName, List<String> columns, Map<String, Class<?>> columnTypes) {
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

    @Override
    public void saveToFile(String tableName) {
        saveCsv(tableName);
        saveSerialized(tableName);
    }

    @Override
    public void loadFromFile(String tableName) {
        loadSerialized(tableName);
    }

    // ─── CSV persistence ──────────────────────────────────────────────

    private void saveCsv(String tableName) {
        String fileName = resolveFilePath(".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false))) {
            writer.write(String.join(",", columns));
            writer.newLine();
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                List<String> values = new ArrayList<>();
                for (String column : columns) {
                    values.add(formatValue(row.get(column)));
                }
                writer.write(String.join(",", values));
                writer.newLine();
            }
            fileInitialized = true;
            LOGGER.log(Level.INFO, "Storage {0} saved CSV to {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save CSV for {0}: {1}", new Object[]{tableName, fileName});
            throw new DieselIOException("Failed to save table to file: " + fileName, e);
        }
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return "\"" + value.toString().replace("\"", "\"\"") + "\"";
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime || value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        return value.toString();
    }

    // ─── Serialised .table persistence ────────────────────────────────

    private void saveSerialized(String tableName) {
        String fileName = resolveFilePath(ErrorMessages.TABLE_EXTENSION);
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
            oos.writeObject(new SerializableAdapter(columns, new ArrayList<>(rows)));
            oos.flush();
            fileInitialized = true;
            LOGGER.log(Level.INFO, "Storage {0} saved serialised to {1} with {2} rows",
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
            LOGGER.log(Level.INFO, "Serialised file {0} not found for storage {1}", new Object[]{fileName, tableName});
            return;
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            SerializableAdapter adapter = (SerializableAdapter) ois.readObject();
            rows.clear();
            rows.addAll(adapter.rows);
            fileInitialized = true;
            LOGGER.log(Level.INFO, "Storage {0} loaded serialised from {1} with {2} rows",
                    new Object[]{tableName, fileName, rows.size()});
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.WARNING, "Failed to load serialised file for {0}: {1}",
                    new Object[]{tableName, e.getMessage()});
        }
    }

    /**
     * Lightweight serialisation adapter that carries column names and rows
     * across ObjectOutputStream/ObjectInputStream without requiring the full
     * {@code Table} graph.
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
