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

    /**
     * @param writer  the underlying character-output stream
     * @param columns the ordered column names (written as header)
     */
    public CsvRowWriter(BufferedWriter writer, List<String> columns) {
        this.writer = writer;
        this.columns = columns;
    }

    /** Writes the header line (column names separated by commas). */
    public void writeHeader() throws IOException {
        boolean sentinel = TsvRowWriter.isSentinelMode();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escapeValue(columns.get(i), sentinel));
        }
        writer.write(sb.toString());
        writer.write('\n');
    }

    /** Writes a single data row.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        boolean sentinel = TsvRowWriter.isSentinelMode();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escapeValue(row.get(columns.get(i)), sentinel));
        }
        writer.write(sb.toString());
        writer.write('\n');
    }

    /** Flushes buffered output. */
    public void flush() throws IOException {
        writer.flush();
    }

    /** Closes the underlying writer. */
    public void close() throws IOException {
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
