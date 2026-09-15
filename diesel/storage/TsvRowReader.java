package diesel.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(TsvRowReader.class);
    private static final String BOM = "\uFEFF";

    private final LineSource source;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final boolean sentinelMode;
    private final String fileName;
    private final Function<String, Object>[] converters;
    private String nextLine;
    private boolean finished;
    private int[] columnMapping;
    private boolean headerRead;
    private long lineNumber;
    private long currentRowLine;
    private long lastRowLine;
    private boolean rowSkipped;
    private boolean extraFieldsWarned;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public TsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(reader, columns, columnTypes, null);
    }

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     */
    public TsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes, String fileName) {
        this(reader == null ? null : LineSource.over(reader), columns, columnTypes, fileName);
    }

    /**
     * @param source      the physical-line provider
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public TsvRowReader(LineSource source, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(source, columns, columnTypes, null);
    }

    /**
     * @param source      the physical-line provider
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     */
    public TsvRowReader(LineSource source, List<String> columns, Map<String, Class<?>> columnTypes, String fileName) {
        this.source = source;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.fileName = fileName;
        this.sentinelMode = TsvRowWriter.isSentinelMode();
        this.converters = buildConverters();
        this.finished = false;
        this.nextLine = null;
        this.headerRead = false;
        this.lineNumber = 0;
        this.currentRowLine = 0;
        this.lastRowLine = 0;
        this.rowSkipped = false;
        this.extraFieldsWarned = false;
    }

    @SuppressWarnings("unchecked")
    private Function<String, Object>[] buildConverters() {
        Function<String, Object>[] converters = new Function[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            Class<?> type = columnTypes.get(colName);
            converters[i] = typeParser(type == null ? null : type.getSimpleName(), colName);
        }
        return converters;
    }

    /**
     * Precompiles the value converter for one column: non-String types parse the
     * raw field directly without unescaping (valid numbers, dates, UUIDs and
     * booleans never contain backslashes, so the raw value equals its unescaped
     * form on every successful parse), dropping the per-cell unescape scan and
     * the per-cell type switch. String and unknown columns still run the
     * backslash unescape.
     */
    private Function<String, Object> typeParser(String typeName, String colName) {
        Function<String, Object> parser = (typeName == null) ? null : DelimitedRowReader.baseParser(typeName);
        if (parser == null) {
            return TsvRowReader::unescape;
        }
        return raw -> {
            try {
                return parser.apply(raw);
            } catch (RuntimeException e) {
                return handleConversionError(e, raw, raw, typeName, colName);
            }
        };
    }

    /** Reads and validates the header line. Returns parsed file header columns. */
    public List<String> readHeader() throws IOException {
        String header = source.nextLine();
        if (header == null) {
            throw new IOException("TSV file is empty – expected header line");
        }
        lineNumber++;
        lastRowLine = lineNumber;
        String[] headerCols = header.split("\t", -1);
        List<String> headerList = new ArrayList<>(headerCols.length);
        for (String col : headerCols) {
            headerList.add(unescape(col));
        }
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
LOGGER.warn(msg);
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
        Object[] values = nextArray();
        if (values == null) {
            return null;
        }
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values[i]);
        }
        return row;
    }

    /**
     * Reads the next row into a compact Object[] whose slot {@code i} holds the
     * value of schema column {@code i} (prompt 36). Returns {@code null} when
     * the whole row was skipped by the {@code storage.load.error.mode} policy.
     */
    public Object[] nextArray() {
        if (finished) {
            throw new NoSuchElementException("No more rows in TSV file");
        }
        if (nextLine == null) {
            prefetch();
        }
        Object[] row = parseLineArray(nextLine);
        lastRowLine = currentRowLine;
        prefetch();
        return row;
    }

    /** Reads all remaining rows into a list and closes the reader. */
    public List<Map<String, Object>> readAll() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        while (hasNext()) {
            Map<String, Object> row = next();
            if (row != null) {
                result.add(row);
            }
        }
        close();
        return result;
    }

    @Override
    public long getLineNumber() {
        return lastRowLine;
    }

    @Override
    public int[] columnMapping() {
        return columnMapping;
    }

    @Override
    public void initPartition(int[] mapping, long firstDataLine) {
        this.columnMapping = java.util.Arrays.copyOf(mapping, mapping.length);
        this.headerRead = true;
        this.lineNumber = firstDataLine - 1;
        this.currentRowLine = firstDataLine - 1;
    }

    @Override
    public void close() throws IOException {
        source.close();
    }

    // ─── Internal ───────────────────────────────────────────────────

    private void prefetch() {
        try {
            nextLine = source.nextLine();
            if (nextLine == null) {
                finished = true;
            } else {
                lineNumber++;
                currentRowLine = lineNumber;
            }
        } catch (IOException e) {
            finished = true;
            throw new DieselIOException(contextPrefix() + "I/O error while reading file at line " + lineNumber, e);
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

    private Object[] parseLineArray(String line) {
        String[] raw = splitTab(line);
        if (raw.length > columns.size() && !extraFieldsWarned) {
            extraFieldsWarned = true;
            LOGGER.warn(contextPrefix() + "line " + currentRowLine
                    + ": row has " + raw.length + " fields but schema expects " + columns.size()
                    + " - ignoring extra fields");
        }
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int fileIdx = columnMapping[i];
            String rawValue = (fileIdx >= 0 && fileIdx < raw.length) ? raw[fileIdx] : "";
            values[i] = convertValue(rawValue, i);
        }
        if (rowSkipped) {
            rowSkipped = false;
            return null;
        }
        return values;
    }

    private Object convertValue(String raw, int columnIdx) {
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
        return converters[columnIdx].apply(raw);
    }

    /**
     * Applies the {@code storage.load.error.mode} policy to a value-conversion
     * failure. Returns a placeholder value for the row, skipping the whole row
     * for {@code skip_row}, or re-throws the failure wrapped in a
     * {@link DieselIOException} carrying the file/line/column context.
     */
    private Object handleConversionError(RuntimeException e, String raw, String unescaped, String typeName, String colName) {
        String shown = unescaped.equals(raw) ? raw : unescaped;
        String msg = contextPrefix() + "line " + currentRowLine + ": column '" + colName
                + "': cannot parse \"" + shown + "\" as " + typeName;
        String mode = readLoadErrorMode();
        if ("skip_value".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            return null;
        }
        if ("skip_row".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            rowSkipped = true;
            return null;
        }
        throw new DieselIOException(msg, e);
    }

    private String contextPrefix() {
        return fileName != null ? fileName + ":" : "";
    }

    private static String readLoadErrorMode() {
        String mode = System.getProperty("storage.load.error.mode");
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
            return props.getProperty("storage.load.error.mode", "fail");
        } catch (IOException e) {
            return "fail";
        }
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
