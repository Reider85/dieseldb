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
 * Streaming reader for TSV files. Reads rows one at a time from a
 * {@link BufferedReader}, unescaping backslash-encoded special characters
 * and converting raw strings to the declared column types.
 *
 * <p>Usage:
 * <pre>
 *   TsvRowReader reader = new TsvRowReader(bufferedReader, columns, columnTypes);
 *   reader.readHeader();          // consume &amp; validate header
 *   while (reader.hasNext()) {
 *       Map&lt;String, Object&gt; row = reader.next();
 *       // process row
 *   }
 *   reader.close();
 * </pre>
 */
public class TsvRowReader implements DelimitedRowReader {

    private static final Logger LOGGER = Logger.getLogger(TsvRowReader.class.getName());
    private static final String BOM = "\uFEFF";

    private final BufferedReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final boolean sentinelMode;
    private String nextLine;
    private boolean finished;
    private int[] columnMapping;
    private boolean headerRead;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public TsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.sentinelMode = TsvRowWriter.isSentinelMode();
        this.finished = false;
        this.nextLine = null;
        this.headerRead = false;
    }

    /** Reads and validates the header line. Returns parsed file header columns. */
    public List<String> readHeader() throws IOException {
        String header = reader.readLine();
        if (header == null) {
            throw new IOException("TSV file is empty – expected header line");
        }
        String[] headerCols = header.split("\t", -1);
        List<String> headerList = new ArrayList<>(List.of(headerCols));
        if (!headerList.isEmpty()) {
            String first = headerList.get(0);
            if (first.startsWith(BOM)) {
                headerList.set(0, first.substring(BOM.length()));
            }
        }
        buildColumnMapping(headerList);
        headerRead = true;
        return headerList;
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
            String msg = "TSV header columns missing from file (required by schema): " + missing;
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
            throw new NoSuchElementException("No more rows in TSV file");
        }
        if (nextLine == null) {
            prefetch();
        }
        Map<String, Object> row = parseLine(nextLine);
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
            nextLine = reader.readLine();
            if (nextLine == null) {
                finished = true;
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error reading TSV line: {0}", e.getMessage());
            finished = true;
        }
    }

    private static String[] splitTab(String line) {
        int len = line.length();
        if (len == 0) {
            return new String[]{""};
        }
        int count = 1;
        for (int i = 0; i < len; i++) {
            if (line.charAt(i) == '\t') {
                count++;
            }
        }
        String[] result = new String[count];
        int start = 0;
        int idx = 0;
        for (int i = 0; i <= len; i++) {
            if (i == len || line.charAt(i) == '\t') {
                result[idx++] = line.substring(start, i);
                start = i + 1;
            }
        }
        return result;
    }

    private Map<String, Object> parseLine(String line) {
        String[] raw = splitTab(line);
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            int fileIdx = columnMapping[i];
            String rawValue = (fileIdx >= 0 && fileIdx < raw.length) ? raw[fileIdx] : "";
            row.put(colName, convertValue(rawValue, colName));
        }
        return row;
    }

    private Object convertValue(String raw, String colName) {
        if (sentinelMode) {
            if ("\\N".equals(raw)) {
                return null;
            }
            if (raw.isEmpty()) {
                return "";
            }
        } else if (raw.isEmpty()) {
            return null;
        }
        String unescaped = unescape(raw);
        Class<?> type = columnTypes.get(colName);
        if (type == null) {
            return unescaped;
        }
        return switch (type.getSimpleName()) {
            case "Long" -> Long.parseLong(unescaped);
            case "Integer" -> Integer.parseInt(unescaped);
            case "Double" -> Double.parseDouble(unescaped);
            case "Float" -> Float.parseFloat(unescaped);
            case "BigDecimal" -> new BigDecimal(unescaped);
            case "Boolean" -> Boolean.parseBoolean(unescaped);
            case "LocalDate" -> LocalDate.parse(unescaped);
            case "LocalDateTime" -> LocalDateTime.parse(unescaped);
            case "UUID" -> UUID.fromString(unescaped);
            default -> unescaped;
        };
    }

    /**
     * Unescapes backslash-encoded characters: {@code \t} → tab,
     * {@code \n} → newline, {@code \r} → carriage-return,
     * {@code \\} → backslash.
     */
    public static String unescape(String raw) {
        if (raw.indexOf('\\') < 0) {
            return raw;
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\\' && i + 1 < raw.length()) {
                char next = raw.charAt(++i);
                switch (next) {
                    case 't' -> sb.append('\t');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case '\\' -> sb.append('\\');
                    default -> {
                        sb.append('\\');
                        sb.append(next);
                    }
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
