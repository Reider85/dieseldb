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

    private static final Logger LOGGER = Logger.getLogger(CsvRowReader.class.getName());
    private static final String BOM = "\uFEFF";

    private final BufferedReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final boolean sentinelMode;
    private final String fileName;
    private String nextLine;
    private boolean finished;
    private int[] columnMapping;
    private boolean headerRead;
    private long lineNumber;
    private long currentRowLine;
    private long lastRowLine;
    private boolean rowSkipped;
    private boolean unterminatedRow;

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
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.fileName = fileName;
        this.sentinelMode = TsvRowWriter.isSentinelMode();
        this.finished = false;
        this.nextLine = null;
        this.headerRead = false;
        this.lineNumber = 0;
        this.currentRowLine = 0;
        this.lastRowLine = 0;
        this.rowSkipped = false;
        this.unterminatedRow = false;
    }

    /** Reads and validates the header line. Returns parsed file header columns. */
    public List<String> readHeader() throws IOException {
        String header = reader.readLine();
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
        if (unterminatedRow) {
            unterminatedRow = false;
            lastRowLine = currentRowLine;
            return handleTruncatedRow();
        }
        Map<String, Object> row = parseDataLine(nextLine);
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
            lineNumber++;
            currentRowLine = lineNumber;
            StringBuilder sb = new StringBuilder(line);
            while (endsInsideQuotes(sb.toString())) {
                String more = reader.readLine();
                if (more == null) {
                    unterminatedRow = true;
                    break;
                }
                lineNumber++;
                sb.append('\n');
                sb.append(more);
            }
            nextLine = sb.toString();
        } catch (IOException e) {
            finished = true;
            throw new DieselIOException(contextPrefix() + "I/O error while reading file at line " + lineNumber, e);
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
        List<ParsedCsvField> raw = parseDataFields(line);
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i);
            int fileIdx = columnMapping[i];
            ParsedCsvField field = (fileIdx >= 0 && fileIdx < raw.size())
                    ? raw.get(fileIdx) : new ParsedCsvField("", false);
            row.put(colName, convertValue(field.value(), field.quoted(), colName));
        }
        if (rowSkipped) {
            rowSkipped = false;
            return null;
        }
        return row;
    }

    /**
     * Parses a single CSV line into raw fields, honouring RFC 4180
     * double-quote quoting and {@code ""} escaped quotes.
     */
    public static List<String> parseLine(String line) {
        List<String> fields = new ArrayList<>();
        for (ParsedCsvField f : parseDataFields(line)) {
            fields.add(f.value());
        }
        return fields;
    }

    /** A raw field parsed from a CSV line together with its quoting flag. */
    record ParsedCsvField(String value, boolean quoted) {}

    private static List<ParsedCsvField> parseDataFields(String line) {
        List<ParsedCsvField> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        boolean fieldQuoted = false;
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
                    fields.add(new ParsedCsvField(sb.toString(), fieldQuoted));
                    sb.setLength(0);
                    fieldQuoted = false;
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(new ParsedCsvField(sb.toString(), fieldQuoted));
        return fields;
    }

    private Object convertValue(String raw, boolean quoted, String colName) {
        if (sentinelMode) {
            if (raw.isEmpty()) {
                return quoted ? "" : null;
            }
        } else if (raw.isEmpty()) {
            return null;
        }
        Class<?> type = columnTypes.get(colName);
        if (type == null) {
            return raw;
        }
        try {
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
        } catch (RuntimeException e) {
            return handleConversionError(e, raw, type.getSimpleName(), colName);
        }
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
            LOGGER.log(Level.WARNING, msg);
            return null;
        }
        if ("skip_row".equalsIgnoreCase(mode)) {
            LOGGER.log(Level.WARNING, msg);
            rowSkipped = true;
            return null;
        }
        throw new DieselIOException(msg, e);
    }

    /** Applies the load-error policy to a row terminated by an unterminated quoted field. */
    private Map<String, Object> handleTruncatedRow() {
        String msg = contextPrefix() + "line " + currentRowLine
                + ": unterminated quoted field (truncated or malformed row)";
        String mode = readLoadErrorMode();
        if ("skip_row".equalsIgnoreCase(mode)) {
            LOGGER.log(Level.WARNING, msg);
            prefetch();
            return null;
        }
        if ("skip_value".equalsIgnoreCase(mode)) {
            LOGGER.log(Level.WARNING, msg);
            Map<String, Object> row = parseDataLine(nextLine);
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