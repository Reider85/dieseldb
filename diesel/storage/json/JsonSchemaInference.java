package diesel.storage.json;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import diesel.DieselIOException;

/**
 * One-pass schema inference for JSON Lines files (prompt 44) - the
 * {@code inferred} schema mode and the hybrid expansion source. Reads the
 * data file line by line (like {@code JsonlRowReader}): UTF-8 BOM on the
 * first record is stripped, blank lines are skipped, each line must be a
 * single JSON object. For every top-level field name the first-seen order and
 * the set of observed JSON value kinds are recorded; a field whose non-null
 * scalar values change type between rows (number &rarr; string, number &rarr;
 * boolean, string &rarr; boolean) fails fast with
 * {@code file:line:field} coordinates instead of silently coercing.
 *
 * <p>Per-field type resolution:
 * <ul>
 * <li>an object or array value ever observed &rarr; {@code String} column
 * holding the value as compact JSON text (the nested storage contract of
 * prompts 41/45), scalars keep their raw token text;</li>
 * <li>integers only &rarr; {@code Long}; integer + float literals &rarr;
 * {@code Double}; booleans only &rarr; {@code Boolean};</li>
 * <li>strings only &rarr; ISO-8601 date / datetime / UUID when every observed
 * value parses, otherwise {@code String};</li>
 * <li>all values null / no values &rarr; {@code String}.</li>
 * </ul>
 * The resulting schema is deterministic in first-seen file order and is what
 * gets stored into the sidecar (prompt 44).
 */
public final class JsonSchemaInference {

    /** UTF-8 byte-order mark stripped from the first record. */
    private static final String BOM = "\uFEFF";

    /** The inferred schema: ordered columns (first-seen order) and their types. */
    public record InferredSchema(List<String> columns, Map<String, Class<?>> columnTypes) {

        /** Returns an empty schema (no data rows, nothing inferred). */
        public static InferredSchema empty() {
            return new InferredSchema(List.of(), Map.of());
        }

        /** Returns the number of inferred columns. */
        public int size() {
            return columns.size();
        }
    }

    /** A record of the field = value kind observed at first occurrence. */
    private static final class FieldObserved {
        private final EnumSet<JsonEvent> kinds = EnumSet.noneOf(JsonEvent.class);
        private long numberLine = -1;
        private long stringLine = -1;
        private long booleanLine = -1;
        private boolean stringParsesAsDate = true;
        private boolean stringParsesAsDateTime = true;
        private boolean stringParsesAsUuid = true;
        private boolean sawString;
    }

    private JsonSchemaInference() {
        throw new AssertionError("No instances");
    }

    /**
     * Scans the whole data stream and derives the ordered column list with
     * their Java types. Consumes the reader up to EOF; the reader is not
     * closed.
     *
     * @param data     the JSON Lines character stream
     * @param fileName the source file name used in error diagnostics, or
     *                 {@code null} when unknown
     * @param config   the streaming JSON configuration for each line
     * @return the inferred schema (empty when the file has no data rows)
     * @throws DieselIOException on a non-object record, a malformed line or a
     *                           field that changes type between rows
     *                           (coordinates carry the line/field)
     * @throws IOException       on a stream read failure
     */
    public static InferredSchema infer(BufferedReader data, String fileName, JsonParserConfig config)
            throws IOException {
        if (data == null) {
            return InferredSchema.empty();
        }
        JsonParserConfig cfg = config != null ? config : JsonParserConfig.defaults();
        Map<String, FieldObserved> byName = new LinkedHashMap<>();
        long lineNumber = 0;
        boolean firstLine = true;
        String line;
        while ((line = data.readLine()) != null) {
            lineNumber++;
            if (firstLine) {
                firstLine = false;
                if (line.startsWith(BOM)) {
                    line = line.substring(BOM.length());
                }
            }
            if (line.trim().isEmpty()) {
                continue;
            }
            inspectLine(line, lineNumber, fileName, cfg, byName);
        }
        return resolve(byName, fileName);
    }

    private static void inspectLine(String line, long lineNumber, String fileName, JsonParserConfig cfg,
                                    Map<String, FieldObserved> byName) throws IOException {
        try (JsonStreamParser p = JsonStreams.createParser(line, cfg)) {
            if (p.nextToken() != JsonEvent.START_OBJECT) {
                throw new DieselIOException(prefix(fileName, lineNumber)
                        + "JSON record must be a single JSON object, found " + p.currentEvent(), null);
            }
            while (p.nextToken() != JsonEvent.END_OBJECT) {
                if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                    throw new DieselIOException(prefix(fileName, lineNumber)
                            + "malformed JSON record: expected a field name, found " + p.currentEvent(), null);
                }
                String field = p.currentName();
                JsonEvent value = p.nextToken();
                if (value == null) {
                    throw new DieselIOException(prefix(fileName, lineNumber)
                            + ": malformed JSON record: missing value for field '" + field + "'", null);
                }
                observe(byName, field, value, lineNumber, fileName, p);
            }
        }
    }

    private static void observe(Map<String, FieldObserved> byName, String field, JsonEvent value, long line,
                                String fileName, JsonStreamParser p) throws IOException {
        FieldObserved observed = byName.computeIfAbsent(field, k -> new FieldObserved());
        observed.kinds.add(value);
        switch (value) {
            case VALUE_NULL -> { /* null is compatible with any inferred type */ }
            case VALUE_STRING -> {
                if (observed.stringLine < 0) {
                    observed.stringLine = line;
                }
                observed.sawString = true;
                String text = p.getText();
                observed.stringParsesAsDate &= parsesAs(() -> LocalDate.parse(text));
                observed.stringParsesAsDateTime &= parsesAs(() -> LocalDateTime.parse(text));
                observed.stringParsesAsUuid &= parsesAs(() -> UUID.fromString(text));
            }
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                if (observed.numberLine < 0) {
                    observed.numberLine = line;
                }
            }
            case VALUE_TRUE, VALUE_FALSE -> {
                if (observed.booleanLine < 0) {
                    observed.booleanLine = line;
                }
            }
            case START_OBJECT, START_ARRAY -> {
                p.skipChildren();
            }
            default -> { /* END_* tokens are structural and handled by the walk */ }
        }
        throwOnConflict(field, observed, line, fileName);
    }

    private static void throwOnConflict(String field, FieldObserved observed, long line, String fileName) {
        boolean number = observed.kinds.contains(JsonEvent.VALUE_NUMBER_INT)
                || observed.kinds.contains(JsonEvent.VALUE_NUMBER_FLOAT);
        boolean string = observed.kinds.contains(JsonEvent.VALUE_STRING);
        boolean booleanV = observed.kinds.contains(JsonEvent.VALUE_TRUE)
                || observed.kinds.contains(JsonEvent.VALUE_FALSE);
        if (number && string) {
            throw new DieselIOException(prefix(fileName, line) + "field '" + field
                    + "': value changes type between rows (number at line " + observed.numberLine
                    + ", string at line " + observed.stringLine + ") - inference refuses a silent conversion", null);
        }
        if (number && booleanV) {
            throw new DieselIOException(prefix(fileName, line) + "field '" + field
                    + "': value changes type between rows (number at line " + observed.numberLine
                    + ", boolean at line " + observed.booleanLine + ") - inference refuses a silent conversion", null);
        }
        if (string && booleanV) {
            throw new DieselIOException(prefix(fileName, line) + "field '" + field
                    + "': value changes type between rows (string at line " + observed.stringLine
                    + ", boolean at line " + observed.booleanLine + ") - inference refuses a silent conversion", null);
        }
    }

    private static InferredSchema resolve(Map<String, FieldObserved> byName, String fileName) {
        if (byName.isEmpty()) {
            return InferredSchema.empty();
        }
        List<String> columns = new ArrayList<>(byName.size());
        Map<String, Class<?>> columnTypes = new LinkedHashMap<>(byName.size());
        for (Map.Entry<String, FieldObserved> entry : byName.entrySet()) {
            String field = entry.getKey();
            FieldObserved observed = entry.getValue();
            Class<?> type = resolveType(field, observed, fileName);
            columns.add(field);
            columnTypes.put(field, type);
        }
        return new InferredSchema(List.copyOf(columns), Map.copyOf(columnTypes));
    }

    private static Class<?> resolveType(String field, FieldObserved observed, String fileName) {
        if (observed.kinds.contains(JsonEvent.START_OBJECT) || observed.kinds.contains(JsonEvent.START_ARRAY)) {
            return String.class; // captured JSON text (nested storage contract, prompt 45)
        }
        boolean number = observed.kinds.contains(JsonEvent.VALUE_NUMBER_INT)
                || observed.kinds.contains(JsonEvent.VALUE_NUMBER_FLOAT);
        boolean booleanV = observed.kinds.contains(JsonEvent.VALUE_TRUE)
                || observed.kinds.contains(JsonEvent.VALUE_FALSE);
        if (number && !observed.kinds.contains(JsonEvent.VALUE_STRING) && !booleanV) {
            return observed.kinds.contains(JsonEvent.VALUE_NUMBER_FLOAT) ? Double.class : Long.class;
        }
        if (booleanV && !number && !observed.kinds.contains(JsonEvent.VALUE_STRING)) {
            return Boolean.class;
        }
        if (observed.sawString) {
            if (observed.stringParsesAsDate) {
                return LocalDate.class;
            }
            if (observed.stringParsesAsDateTime) {
                return LocalDateTime.class;
            }
            if (observed.stringParsesAsUuid) {
                return UUID.class;
            }
        }
        return String.class;
    }

    private static String prefix(String fileName, long line) {
        return (fileName != null ? fileName + ":" : "") + "line " + line + ": ";
    }

    private static boolean parsesAs(Runnable parser) {
        try {
            parser.run();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}