package diesel.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes rows to a CSV (Comma-Separated Values) file following RFC 4180.
 * Values containing commas, double-quotes or newlines are enclosed in
 * double-quotes; literal double-quotes inside a field are escaped as
 * {@code ""}. Nulls are written as empty fields.
 */
public class CsvRowWriter implements AutoCloseable {

    private final BufferedWriter writer;
    private final List<String> columns;
    private final boolean sentinelMode;
    private final boolean[] needsScan;
    private final StringBuilder sb = new StringBuilder(256);
    private final StringBuilder batch = new StringBuilder(BATCH_LIMIT);

    private static final int BATCH_LIMIT = 16384;

    /**
     * @param writer  the underlying character-output stream
     * @param columns the ordered column names (written as header)
     */
    public CsvRowWriter(BufferedWriter writer, List<String> columns) {
        this(writer, columns, null);
    }

    /**
     * @param writer      the underlying character-output stream
     * @param columns     the ordered column names (written as header)
     * @param columnTypes column name to expected Java type, used to inline the
     *                    formatting of non-String columns (null or {@code String}
     *                    columns still run the full escape scan)
     */
    public CsvRowWriter(BufferedWriter writer, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.writer = writer;
        this.columns = columns;
        this.sentinelMode = TsvRowWriter.isSentinelMode();
        this.needsScan = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Class<?> type = columnTypes == null ? null : columnTypes.get(columns.get(i));
            needsScan[i] = type == null || type == String.class;
        }
    }

    /** Writes the header line (column names separated by commas). */
    public void writeHeader() throws IOException {
        sb.setLength(0);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escapeValue(columns.get(i), sentinelMode));
        }
        sb.append('\n');
        batch.append(sb);
    }

    /** Writes a single data row.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        sb.setLength(0);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendCsvValue(row == null ? null : row.get(columns.get(i)), i);
        }
        appendRowAndFlush();
    }

    /**
     * Writes a single data row given as a compact Object[] array, where slot
     * {@code i} holds the value of schema column {@code i} (prompt 36). Avoids
     * materialising a per-row Map at save time.
     *
     * @param row the ordered values aligned with the columns
     */
    public void writeRow(Object[] row) throws IOException {
        sb.setLength(0);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendCsvValue(row == null || i >= row.length ? null : row[i], i);
        }
        appendRowAndFlush();
    }

    private void appendRowAndFlush() throws IOException {
        sb.append('\n');
        batch.append(sb);
        if (batch.length() > BATCH_LIMIT) {
            flushBatch();
        }
    }

    private void appendCsvValue(Object value, int columnIdx) {
        if (value == null) {
            return;
        }
        if (needsScan[columnIdx]) {
            sb.append(escapeValue(value, sentinelMode));
        } else if (value instanceof BigDecimal bd) {
            sb.append(bd.toPlainString());
        } else {
            sb.append(value.toString());
        }
    }

    private void flushBatch() throws IOException {
        if (batch.length() > 0) {
            writer.write(batch.toString());
            batch.setLength(0);
        }
    }

    /** Flushes buffered output. */
    public void flush() throws IOException {
        flushBatch();
        writer.flush();
    }

    /** Closes the underlying writer. */
    public void close() throws IOException {
        flushBatch();
        writer.close();
    }

    /**
     * Escapes a single value for CSV output per RFC 4180 using the current
     * {@code storage.null.representation} config setting.
     */
    public static String escapeValue(Object value) {
        return escapeValue(value, TsvRowWriter.isSentinelMode());
    }

    /**
     * Escapes a single value for CSV output per RFC 4180.
     * The value is enclosed in double-quotes if it contains a comma,
     * a double-quote, or a newline. Literal double-quotes inside
     * the value are doubled ({@code ""}).
     * In sentinel mode a null is written as an unquoted empty field and an
     * empty string as a quoted {@code ""}, which keeps the two distinct.
     */
    public static String escapeValue(Object value, boolean sentinelMode) {
        if (value == null) {
            return "";
        }
        String raw;
        if (value instanceof BigDecimal bd) {
            raw = bd.toPlainString();
        } else {
            raw = value.toString();
        }
        if (sentinelMode && raw.isEmpty()) {
            return "\"\"";
        }
        boolean needsQuoting = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needsQuoting = true;
                break;
            }
        }
        if (!needsQuoting) {
            return raw;
        }
        StringBuilder sb = new StringBuilder(raw.length() + 2);
        sb.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '"') {
                sb.append("\"\"");
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
