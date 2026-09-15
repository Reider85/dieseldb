package diesel.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Writes rows to a TSV (Tab-Separated Values) file. Values are
 * backslash-escaped: {@code \t} for tab, {@code \n} for newline,
 * {@code \\} for a literal backslash. Nulls are written as empty fields
 * in legacy mode, or as {@code \N} sentinel in sentinel mode.
 */
public class TsvRowWriter implements AutoCloseable {

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
    public TsvRowWriter(BufferedWriter writer, List<String> columns) {
        this(writer, columns, null);
    }

    /**
     * @param writer      the underlying character-output stream
     * @param columns     the ordered column names (written as header)
     * @param columnTypes column name to expected Java type, used to inline the
     *                    formatting of non-String columns (null or {@code String}
     *                    columns still run the full escape scan)
     */
    public TsvRowWriter(BufferedWriter writer, List<String> columns, Map<String, Class<?>> columnTypes) {
        this.writer = writer;
        this.columns = columns;
        this.sentinelMode = isSentinelMode();
        this.needsScan = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Class<?> type = columnTypes == null ? null : columnTypes.get(columns.get(i));
            needsScan[i] = type == null || type == String.class;
        }
    }

    /** Writes the header line (column names separated by tabs). */
    public void writeHeader() throws IOException {
        sb.setLength(0);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(escapeValue(columns.get(i), sentinelMode));
        }
        sb.append('\n');
        batch.append(sb);
    }

    /**
     * Writes a single data row.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        sb.setLength(0);
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append('\t');
            }
            appendTsvValue(row == null ? null : row.get(columns.get(i)), i);
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
                sb.append('\t');
            }
            appendTsvValue(row == null || i >= row.length ? null : row[i], i);
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

    private void appendTsvValue(Object value, int columnIdx) {
        if (value == null) {
            if (sentinelMode) {
                sb.append("\\N");
            }
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
     * Escapes a single value for TSV output using the current
     * {@code storage.null.representation} config setting.
     */
    public static String escapeValue(Object value) {
        return escapeValue(value, isSentinelMode());
    }

    /**
     * Escapes a single value for TSV output.
     * Tab, newline and backslash are backslash-escaped.
     * In sentinel mode, null is written as {@code \N}.
     */
    public static String escapeValue(Object value, boolean sentinelMode) {
        if (value == null) {
            return sentinelMode ? "\\N" : "";
        }
        String raw;
        if (value instanceof BigDecimal bd) {
            raw = bd.toPlainString();
        } else {
            raw = value.toString();
        }
        boolean needsEscape = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c == '\t' || c == '\n' || c == '\r' || c == '\\') {
                needsEscape = true;
                break;
            }
        }
        if (!needsEscape) {
            return raw;
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

    private static final Properties ROOT_PROPS = loadRootProps();

    private static Properties loadRootProps() {
        Properties props = new Properties();
        try {
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (IOException ignored) {
        }
        return props;
    }

    static boolean isSentinelMode() {
        String mode = System.getProperty("storage.null.representation");
        if (mode != null) return "sentinel".equalsIgnoreCase(mode);
        return "sentinel".equalsIgnoreCase(
                ROOT_PROPS.getProperty("storage.null.representation", "legacy"));
    }
}
