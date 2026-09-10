package diesel.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
public class TsvRowReader implements Iterator<Map<String, Object>>, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(TsvRowReader.class.getName());

    private final BufferedReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private String nextLine;
    private boolean finished;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public TsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.finished = false;
        this.nextLine = null;
    }

    /** Reads and validates the header line. */
    public void readHeader() throws IOException {
        String header = reader.readLine();
        if (header == null) {
            throw new IOException("TSV file is empty – expected header line");
        }
        String[] headerCols = header.split("\t", -1);
        if (headerCols.length != columns.size()) {
            LOGGER.log(Level.WARNING,
                    "TSV header column count ({0}) differs from schema ({1}), proceeding anyway",
                    new Object[]{headerCols.length, columns.size()});
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

    private Map<String, Object> parseLine(String line) {
        String[] raw = line.split("\t", -1);
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            String rawValue = (i < raw.length) ? raw[i] : "";
            row.put(colName, convertValue(rawValue, colName));
        }
        return row;
    }

    private Object convertValue(String raw, String colName) {
        if (raw.isEmpty()) {
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
