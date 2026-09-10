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
 * Writes rows to a TSV (Tab-Separated Values) file. Values are
 * backslash-escaped: {@code \t} for tab, {@code \n} for newline,
 * {@code \\} for a literal backslash. Nulls are written as empty fields.
 */
public class TsvRowWriter implements AutoCloseable {

    private final BufferedWriter writer;
    private final List<String> columns;

    /**
     * @param writer  the underlying character-output stream
     * @param columns the ordered column names (written as header)
     */
    public TsvRowWriter(BufferedWriter writer, List<String> columns) {
        this.writer = writer;
        this.columns = columns;
    }

    /** Writes the header line (column names separated by tabs). */
    public void writeHeader() throws IOException {
        writer.write(String.join("\t", columns));
        writer.newLine();
    }

    /**
     * Writes a single data row.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(escapeValue(row.get(columns.get(i))));
        }
        writer.write(sb.toString());
        writer.newLine();
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
     * Escapes a single value for TSV output.
     * Tab, newline and backslash are backslash-escaped.
     */
    public static String escapeValue(Object value) {
        if (value == null) {
            return "";
        }
        String raw;
        if (value instanceof BigDecimal bd) {
            raw = bd.toPlainString();
        } else {
            raw = value.toString();
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\\' -> sb.append("\\\\");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
