package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Streaming reader for CSV files. Reads rows one at a time from a
 * {@link BufferedReader}, handling RFC 4180 double-quote escaping
 * and converting raw strings to the declared column types.
 *
 * <p>Usage:
 * <pre>
 *   CsvRowReader reader = new CsvRowReader(bufferedReader, columns, columnTypes);
 *   reader.readHeader();          // consume &amp; validate header
 *   while (reader.hasNext()) {
 *       Map&lt;String, Object&gt; row = reader.next();
 *       // process row
 *   }
 *   reader.close();
 * </pre>
 */
public class CsvRowReader implements DelimitedRowReader {

    private static final Logger LOGGER = Logger.getLogger(CsvRowReader.class.getName());
    private static final String BOM = "\uFEFF";

    private final BufferedReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private String nextLine;
    private boolean finished;
    private int[] columnMapping;
    private boolean headerRead;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public CsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.finished = false;
        this.nextLine = null;
        this.headerRead = false;
    }

    /** Reads and validates the header line. Returns parsed file header columns. */
    public List<String> readHeader() throws IOException {
        String header = reader.readLine();
        if (header == null) {
            throw new IOException("CSV file is empty – expected header line");
        }
        List<String> headerCols = parseLine(header);
        if (!headerCols.isEmpty()) {
            String first = headerCols.get(0);
            if (first.startsWith(BOM)) {
                headerCols.set(0, first.substring(BOM.length()));
            }
        }
        buildColumnMapping(headerCols);
        headerRead = true;
        return headerCols;
    }

    private void buildColumnMapping(List<String> fileHeader) throws IOException {
        columnMapping = new int[columns.size()];
        Map<String, Integer> fileIndexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < fileHeader.size(); i++) {
            String name = fileHeader.get(i).trim();
            if (!name.isEmpty()) {
                fileIndexByName.put(name, i);
            }
        }
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String schemaCol = columns.get(i);
            Integer fileIdx = fileIndexByName.get(schemaCol);
            columnMapping[i] = (fileIdx != null) ? fileIdx : -1;
            if (fileIdx == null) {
                missing.add(schemaCol);
            }
        }
        if (!missing.isEmpty()) {
            String msg = "CSV header columns missing from file (required by schema): " + missing;
            String mode = readMismatchMode();
            if ("fail".equalsIgnoreCase(mode)) {
                throw new IOException(msg);
            } else {
                LOGGER.log(Level.WARNING, msg);
            }
        }
    }

    private static String readMismatchMode() {
        String mode = System.getProperty("storage.header.mismatch.mode");
        if (mode != null) {
            return mode;
        }
        try {
            java.util.Properties props = new java.util.Properties();
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
            return props.getProperty("storage.header.mismatch.mode", "fail");
        } catch (IOException e) {
            return "fail";
        }
    }

    @Override
    public boolean hasNext() {
        if (nextLine == null && !finished) {
            prefetch();
        }
        return !finished;
    }

    @Override
    public Map<String, Object> next() {
        if (finished) {
            throw new NoSuchElementException("No more rows in CSV file");
        }
        if (nextLine == null) {
            prefetch();
        }
        Map<String, Object> row = parseDataLine(nextLine);
        prefetch();
        return row;
    }

    /** Reads all remaining rows into a list and closes the reader. */
    public List<Map<String, Object>> readAll() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        while (hasNext()) {
            result.add(next());
        }
        close();
        return result;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    // ─── Internal ───────────────────────────────────────────────────

    private void prefetch() {
        try {
            String line = reader.readLine();
            if (line == null) {
                finished = true;
                return;
            }
            StringBuilder sb = new StringBuilder(line);
            while (endsInsideQuotes(sb.toString())) {
                String more = reader.readLine();
                if (more == null) {
                    break;
                }
                sb.append('\n');
                sb.append(more);
            }
            nextLine = sb.toString();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error reading CSV line: {0}", e.getMessage());
            finished = true;
        }
    }

    /** Returns whether the partial text ends inside an unterminated quoted field. */
    static boolean endsInsideQuotes(String text) {
        boolean inQuotes = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
                        i++;
                    } else {
                        inQuotes = false;
                    }
                }
            } else if (c == '"') {
                inQuotes = true;
            }
        }
        return inQuotes;
    }

    private Map<String, Object> parseDataLine(String line) {
        List<String> raw = parseLine(line);
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            int fileIdx = columnMapping[i];
            String rawValue = (fileIdx >= 0 && fileIdx < raw.size()) ? raw.get(fileIdx) : "";
            row.put(colName, convertValue(rawValue, colName));
        }
        return row;
    }

    /**
     * Parses a single CSV line into raw fields, honouring RFC 4180
     * double-quote quoting and {@code ""} escaped quotes.
     */
    public static List<String> parseLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(sb.toString());
        return fields;
    }

    private Object convertValue(String raw, String colName) {
        if (raw.isEmpty()) {
            return null;
        }
        Class<?> type = columnTypes.get(colName);
        if (type == null) {
            return raw;
        }
        return switch (type.getSimpleName()) {
            case "Long" -> Long.parseLong(raw);
            case "Integer" -> Integer.parseInt(raw);
            case "Double" -> Double.parseDouble(raw);
            case "Float" -> Float.parseFloat(raw);
            case "BigDecimal" -> new BigDecimal(raw);
            case "Boolean" -> Boolean.parseBoolean(raw);
            case "LocalDate" -> LocalDate.parse(raw);
            case "LocalDateTime" -> LocalDateTime.parse(raw);
            case "UUID" -> UUID.fromString(raw);
            default -> raw;
        };
    }
}