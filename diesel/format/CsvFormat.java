package diesel.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Row-based storage format for the legacy CSV representation. The file layout
 * is a plain text table: one header line with comma-separated column names,
 * followed by one line per row with comma-separated values. Strings are
 * double-quoted with {@code ""} used to escape embedded quotes; {@code null}
 * is an empty field; {@link BigDecimal}, dates, UUIDs and all other values use
 * their {@code toString()} text form.
 *
 * <p>The format is write-append capable but not columnar: it advertises only
 * the row-based capabilities and performs no projection or predicate
 * pushdown.</p>
 */
public final class CsvFormat implements TableFormat {

    /** The canonical format name. */
    public static final String NAME = "CSV";

    /** The canonical file extension. */
    public static final String EXTENSION = ".csv";

    private static final FormatCapabilities CAPABILITIES = FormatCapabilities.rowBased();

    private static final String UUID_PATTERN =
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getDescription() {
        return "Legacy row-based comma-separated values with quoted string escaping";
    }

    @Override
    public String getFileExtension() {
        return EXTENSION;
    }

    @Override
    public FormatCapabilities getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public void write(TableData data, Path filePath, WriteOptions options) throws IOException {
        if (data == null) {
            throw new IllegalArgumentException("TableData must not be null");
        }
        Path parent = filePath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8)) {
            List<String> columns = data.getColumns();
            if (columns.isEmpty() && !data.getColumnTypes().isEmpty()) {
                columns = List.copyOf(data.getColumnTypes().keySet());
            }
            writer.write(String.join(",", columns));
            writer.newLine();
            for (Map<String, Object> row : data.getRows()) {
                List<String> values = new ArrayList<>(columns.size());
                for (String column : columns) {
                    values.add(formatValue(row.get(column)));
                }
                writer.write(String.join(",", values));
                writer.newLine();
            }
        }
    }

    /**
     * Escapes a single cell value to its CSV text form, mirroring the legacy
     * writer so existing files remain readable.
     *
     * @param value the cell value
     * @return the CSV-escaped text
     */
    static String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        if (value instanceof LocalDate || value instanceof LocalDateTime || value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof BigDecimal bd) {
            return bd.toPlainString();
        }
        return value.toString();
    }

    @Override
    public TableData read(Path filePath, ReadOptions options) throws IOException {
        return read(filePath, options == null ? ReadOptions.DEFAULT : options, true);
    }

    @Override
    public TableData inferSchema(Path filePath) throws IOException {
        return read(filePath, ReadOptions.DEFAULT, false);
    }

    private TableData read(Path filePath, ReadOptions options, boolean materializeRows) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                return new TableData(List.of(), Map.of(), List.of(), Map.of());
            }
            List<String> columns = parseLine(header);

            List<Map<String, Object>> rows = new ArrayList<>();
            long limit = options.getLimit();
            String line;
            while ((line = reader.readLine()) != null) {
                if (materializeRows) {
                    List<String> values = parseLine(line);
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 0; i < columns.size(); i++) {
                        String raw = i < values.size() ? values.get(i) : "";
                        row.put(columns.get(i), parseValue(raw));
                    }
                    rows.add(row);
                }
                if (limit >= 0 && rows.size() >= limit) {
                    break;
                }
            }

            Map<String, Class<?>> columnTypes = inferColumnTypes(columns, rows);
            return new TableData(columns, columnTypes, rows, Map.of());
        }
    }

    /**
     * Parses a single CSV line into its fields, honoring double-quoted fields
     * and {@code ""} escapes. Emits at least one field even for blank lines.
     *
     * @param line the raw line
     * @return the parsed fields
     */
    static List<String> parseLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }

    /** Candidate type for a parsed cell, ordered narrowest-first in the enum. */
    private enum CellType {
        INTEGER,
        LONG,
        DOUBLE,
        BOOLEAN,
        UUID,
        DATE,
        DATETIME,
        STRING
    }

    /**
     * Parses a raw CSV cell to its best concrete type.
     *
     * @param raw the raw cell text
     * @return the typed value, or {@code null} for an empty field
     */
    static Object parseValue(String raw) {
        if (raw.isEmpty()) {
            return null;
        }
        String t = raw.trim();
        if (t.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (t.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (raw.matches("-?\\d+")) {
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException ignored) {
                return Long.valueOf(raw);
            }
        }
        if (raw.matches("-?\\d+\\.\\d+")) {
            return Double.valueOf(raw);
        }
        if (t.matches(UUID_PATTERN)) {
            return UUID.fromString(t);
        }
        try {
            return LocalDate.parse(t);
        } catch (DateTimeParseException ignored) {
            // fall through to datetime check
        }
        try {
            return LocalDateTime.parse(t.replace(' ', 'T'));
        } catch (DateTimeParseException ignored) {
            return raw;
        }
    }

    /**
     * Infers the concrete column types by classifying every non-null value:
     * when all values in a column share one classification the column is typed
     * accordingly, otherwise it falls back to {@link String}.
     *
     * @param columns the column names
     * @param rows    the parsed rows (possibly empty)
     * @return column name to Java type map
     */
    static Map<String, Class<?>> inferColumnTypes(List<String> columns, List<Map<String, Object>> rows) {
        Map<String, Class<?>> columnTypes = new LinkedHashMap<>();
        if (rows.isEmpty()) {
            for (String column : columns) {
                columnTypes.put(column, String.class);
            }
            return columnTypes;
        }
        for (String column : columns) {
            CellType common = null;
            boolean mixed = false;
            for (Map<String, Object> row : rows) {
                Object v = row.get(column);
                if (v == null) {
                    continue;
                }
                CellType cell = classifyCell(v);
                if (common == null) {
                    common = cell;
                } else if (!common.equals(cell)) {
                    mixed = true;
                    break;
                }
            }
            columnTypes.put(column, mixed || common == null ? String.class : toJavaType(common));
        }
        return columnTypes;
    }

    private static CellType classifyCell(Object value) {
        if (value instanceof Boolean) {
            return CellType.BOOLEAN;
        }
        if (value instanceof Integer) {
            return CellType.INTEGER;
        }
        if (value instanceof Long) {
            return CellType.LONG;
        }
        if (value instanceof Double) {
            return CellType.DOUBLE;
        }
        if (value instanceof UUID) {
            return CellType.UUID;
        }
        if (value instanceof LocalDate) {
            return CellType.DATE;
        }
        if (value instanceof LocalDateTime) {
            return CellType.DATETIME;
        }
        return CellType.STRING;
    }

    private static Class<?> toJavaType(CellType type) {
        return switch (type) {
            case INTEGER -> Integer.class;
            case LONG -> Long.class;
            case DOUBLE -> Double.class;
            case BOOLEAN -> Boolean.class;
            case UUID -> UUID.class;
            case DATE -> LocalDate.class;
            case DATETIME -> LocalDateTime.class;
            case STRING -> String.class;
        };
    }
}