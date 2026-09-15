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

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvRowReader.class);
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
    private boolean unterminatedRow;
    private boolean extraFieldsWarned;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public CsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(reader, columns, columnTypes, null);
    }

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     */
    public CsvRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes, String fileName) {
        this(reader == null ? null : LineSource.over(reader), columns, columnTypes, fileName);
    }

    /**
     * @param source      the physical-line provider
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public CsvRowReader(LineSource source, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(source, columns, columnTypes, null);
    }

    /**
     * @param source      the physical-line provider
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     */
    public CsvRowReader(LineSource source, List<String> columns, Map<String, Class<?>> columnTypes, String fileName) {
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
        this.unterminatedRow = false;
        this.extraFieldsWarned = false;
    }

    @SuppressWarnings("unchecked")
    private Function<String, Object>[] buildConverters() {
        Function<String, Object>[] converters = new Function[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            Class<?> type = columnTypes.get(colName);
            if (type == null) {
                converters[i] = raw -> raw;
                continue;
            }
            String typeName = type.getSimpleName();
            converters[i] = typeParser(typeName, colName);
        }
        return converters;
    }

    /**
     * Precompiles the value converter for one column: the parse function is
     * selected once at construction (see {@link DelimitedRowReader#baseParser})
     * instead of on every cell, so no per-cell type switch runs on the hot path.
     */
    private Function<String, Object> typeParser(String typeName, String colName) {
        Function<String, Object> parser = DelimitedRowReader.baseParser(typeName);
        if (parser == null) {
            return raw -> raw;
        }
        return raw -> {
            try {
                return parser.apply(raw);
            } catch (RuntimeException e) {
                return handleConversionError(e, raw, typeName, colName);
            }
        };
    }

    /**
     * Fast whole-file load: reads the file bytes in one shot, decodes them once
     * with the REPORT charset decoder (same semantics as
     * {@link CompressionFactory#openDelimitedReader}), splits the physical
     * lines exactly like {@link java.io.BufferedReader#readLine()} and streams
     * every data row through this reader's parse pipeline. Returned rows are
     * compact {@code Object[]} arrays indexed by schema column position.
     *
     * <p>This is the path actually used by {@link CsvRowStorage#loadFromFile}
     * and {@link DelimitedIndexManager#loadFromFileSequentialArrays}; exposing
     * it on the reader lets micro-benchmarks exercise the byte fast path
     * without instantiating a full storage.
     *
     * @param file        the plain (uncompressed) CSV file
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @return the decoded rows in file order (skipped rows are omitted)
     */
    public static List<Object[]> loadFast(File file, List<String> columns,
                                         Map<String, Class<?>> columnTypes) throws IOException {
        CompressionFactory.ResolvedDelimitedFile ref =
                CompressionFactory.resolveActual(file, "csv.compression.codec");
        return DelimitedContent.readAllArrays(ref.file(), ref.codec(), columns,
                columnTypes, CsvRowReader::new, StorageConfig.getCharset());
    }

    /** Reads and validates the header line. Returns parsed file header columns. */
    public List<String> readHeader() throws IOException {
        String header = source.nextLine();
        if (header == null) {
            throw new IOException("CSV file is empty – expected header line");
        }
        lineNumber++;
        lastRowLine = lineNumber;
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
            throw new NoSuchElementException("No more rows in CSV file");
        }
        if (nextLine == null) {
            prefetch();
        }
        if (unterminatedRow) {
            unterminatedRow = false;
            lastRowLine = currentRowLine;
            return handleTruncatedRowArray();
        }
        Object[] row = parseDataLineArray(nextLine);
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
            String line = source.nextLine();
            if (line == null) {
                finished = true;
                return;
            }
            lineNumber++;
            currentRowLine = lineNumber;
            StringBuilder sb = new StringBuilder(line);
            boolean inQuotes = scanQuotes(sb, 0, false);
            while (inQuotes) {
                String more = source.nextLine();
                if (more == null) {
                    unterminatedRow = true;
                    break;
                }
                lineNumber++;
                sb.append('\n');
                int start = sb.length();
                sb.append(more);
                inQuotes = scanQuotes(sb, start, true);
            }
            nextLine = sb.toString();
        } catch (IOException e) {
            finished = true;
            throw new DieselIOException(contextPrefix() + "I/O error while reading file at line " + lineNumber, e);
        }
    }

    /** Scans {@code text} from {@code startOffset} given the initial {@code inQuotes} state. */
    private static boolean scanQuotes(CharSequence text, int startOffset, boolean inQuotes) {
        for (int i = startOffset; i < text.length(); i++) {
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

    /** Returns whether the partial text ends inside an unterminated quoted field. */
    static boolean endsInsideQuotes(String text) {
        return scanQuotes(text, 0, false);
    }

    private Object[] parseDataLineArray(String line) {
        DataFields raw = parseDataFields(line);
        if (raw.values.size() > columns.size() && !extraFieldsWarned) {
            extraFieldsWarned = true;
            LOGGER.warn(contextPrefix() + "line " + currentRowLine
                    + ": row has " + raw.values.size() + " fields but schema expects " + columns.size()
                    + " - ignoring extra fields");
        }
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int fileIdx = columnMapping[i];
            boolean present = fileIdx >= 0 && fileIdx < raw.values.size();
            String field = present ? raw.values.get(fileIdx) : "";
            boolean quoted = present && raw.isQuoted(fileIdx);
            values[i] = convertValue(field, quoted, i);
        }
        if (rowSkipped) {
            rowSkipped = false;
            return null;
        }
        return values;
    }

    /**
     * Parses a single CSV line into raw fields, honouring RFC 4180
     * double-quote quoting and {@code ""} escaped quotes.
     */
    public static List<String> parseLine(String line) {
        return parseDataFields(line).values;
    }

    /**
     * Raw fields parsed from a CSV line: the unquoted values plus a parallel
     * quoted-flag array indexing the same positions (a plain boolean[] instead
     * of a per-field boxed record avoids needless allocation on the hot path).
     */
    private static final class DataFields {
        final List<String> values;
        private final boolean[] quoted;

        DataFields(List<String> values, boolean[] quoted) {
            this.values = values;
            this.quoted = quoted;
        }

        boolean isQuoted(int index) {
            return quoted != null && index < quoted.length && quoted[index];
        }
    }

    private static DataFields parseDataFields(String line) {
        // Fast path: no double-quote anywhere means no field is quoted, so we
        // can scan with the intrinsified String.indexOf(',') and slice fields
        // with String.substring (HotSpot intrinsifies both into vector scans
        // / Arrays.copyOfRange). This is 3-5x faster than the char-by-char
        // parser below on typical unquoted CSV lines.
        if (line.indexOf('"') < 0) {
            List<String> fields = new ArrayList<>();
            int start = 0;
            while (true) {
                int next = line.indexOf(',', start);
                if (next < 0) {
                    fields.add(line.substring(start));
                    return new DataFields(fields, null);
                }
                fields.add(line.substring(start, next));
                start = next + 1;
            }
        }
        // Slow path: at least one double-quote, run the full RFC 4180 parser
        // that tracks inQuotes, doubled-quote escaping and fieldQuoted flags.
        return parseDataFieldsQuoted(line);
    }

    private static DataFields parseDataFieldsQuoted(String line) {
        List<String> fields = new ArrayList<>();
        boolean[] quoted = null;
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        boolean fieldQuoted = false;
        int fieldCount = 0;
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
                    fieldQuoted = true;
                } else if (c == ',') {
                    if (fieldQuoted) {
                        quoted = markQuoted(quoted, fieldCount);
                    }
                    fieldCount++;
                    fieldQuoted = false;
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        if (fieldQuoted) {
            quoted = markQuoted(quoted, fieldCount);
        }
        fields.add(sb.toString());
        return new DataFields(fields, quoted);
    }

    private static boolean[] markQuoted(boolean[] quoted, int index) {
        if (quoted == null) {
            quoted = new boolean[8];
        }
        if (index >= quoted.length) {
            quoted = java.util.Arrays.copyOf(quoted, Math.max(quoted.length * 2, index + 1));
        }
        quoted[index] = true;
        return quoted;
    }

    private Object convertValue(String raw, boolean quoted, int columnIdx) {
        if (sentinelMode) {
            if (raw.isEmpty()) {
                return quoted ? "" : null;
            }
        } else if (raw.isEmpty()) {
            return null;
        }
        return converters[columnIdx].apply(raw);
    }

    /**
     * Applies the {@code storage.load.error.mode} policy to a value-conversion
     * failure. Returns the value to store in the row (a placeholder), skipping
     * the whole row for {@code skip_row}, or re-throws the failure wrapped in a
     * {@link DieselIOException} carrying the file/line/column context.
     */
    private Object handleConversionError(RuntimeException e, String raw, String typeName, String colName) {
        String msg = contextPrefix() + "line " + currentRowLine + ": column '" + colName
                + "': cannot parse \"" + raw + "\" as " + typeName;
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

    /** Applies the load-error policy to a row terminated by an unterminated quoted field. */
    private Object[] handleTruncatedRowArray() {
        String msg = contextPrefix() + "line " + currentRowLine
                + ": unterminated quoted field (truncated or malformed row)";
        String mode = readLoadErrorMode();
        if ("skip_row".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            prefetch();
            return null;
        }
        if ("skip_value".equalsIgnoreCase(mode)) {
            LOGGER.warn(msg);
            Object[] row = parseDataLineArray(nextLine);
            prefetch();
            return row;
        }
        throw new DieselIOException(msg, null);
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
}