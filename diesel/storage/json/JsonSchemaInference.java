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
 * prompts 41/45);</li>
 * <li>under {@code jsonl.nested.mode = flatten} (prompt 45) nested objects are
 * instead walked recursively and each leaf gets its own exact dot-notation
 * column ({@code user.address.city}); pure containers emit no column of their
 * own, arrays of scalars follow {@code jsonl.array.columns} (a single JSON
 * column, or {@code field[0]}, {@code field[1]}, ... index columns),
 * arrays of objects always fall back to one JSON column;</li>
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
        // Array tracking for FLATTEN + EXPAND mode (prompt 45)
        private boolean isArray;
        private boolean arrayHasObjectElements;
        private int arrayMaxLength;
        private final Map<Integer, EnumSet<JsonEvent>> arrayIndexKinds = new java.util.LinkedHashMap<>();
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
        return resolve(byName, fileName, cfg);
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
                observe(byName, field, value, lineNumber, fileName, p, cfg);
            }
        }
    }

    private static void observe(Map<String, FieldObserved> byName, String field, JsonEvent value, long line,
                                String fileName, JsonStreamParser p, JsonParserConfig config) throws IOException {
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
            case START_OBJECT -> {
                if (config.nestedMode() == JsonParserConfig.NestedMode.FLATTEN) {
                    observeNestedObject(byName, field, line, fileName, p, config);
                } else {
                    p.skipChildren();
                }
            }
            case START_ARRAY -> {
                if (config.nestedMode() == JsonParserConfig.NestedMode.FLATTEN) {
                    observeNestedArray(observed, field, line, fileName, p, config);
                } else {
                    p.skipChildren();
                }
            }
            default -> { /* END_* tokens are structural and handled by the walk */ }
        }
        throwOnConflict(field, observed, line, fileName);
    }

    /**
     * FLATTEN-mode nested-object walk (prompt 45): recursively emits an exact
     * dot-notation column for every leaf, e.g. {@code user.address.city}.
     * Arrays nested inside objects are delegated to the array walk.
     */
    private static void observeNestedObject(Map<String, FieldObserved> byName, String prefix, long line,
                                            String fileName, JsonStreamParser p, JsonParserConfig config)
            throws IOException {
        while (p.nextToken() != JsonEvent.END_OBJECT) {
            if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                throw new DieselIOException(prefix(fileName, line)
                        + "malformed JSON record: expected a field name, found " + p.currentEvent(), null);
            }
            String child = p.currentName();
            JsonEvent childValue = p.nextToken();
            observe(byName, prefix + "." + child, childValue, line, fileName, p, config);
        }
    }

    /**
     * FLATTEN-mode array walk (prompt 45): an array of objects always falls
     * back to a single JSON (String) column; an array of scalars under
     * {@code jsonl.array.columns = json} also stays a single JSON column,
     * while {@code expand} records the per-index value kinds and the maximum
     * observed length so the schema can emit {@code field[0]},
     * {@code field[1]}, ... columns.
     */
    private static void observeNestedArray(FieldObserved observed, String field, long line,
                                           String fileName, JsonStreamParser p, JsonParserConfig config)
            throws IOException {
        observed.isArray = true;
        int index = 0;
        while (p.nextToken() != JsonEvent.END_ARRAY) {
            JsonEvent element = p.currentEvent();
            if (element == JsonEvent.START_OBJECT || element == JsonEvent.START_ARRAY) {
                observed.arrayHasObjectElements = true;
                p.skipChildren();
                index++;
                continue;
            }
            if (element == JsonEvent.VALUE_STRING) {
                p.getText();
            }
            observed.arrayIndexKinds.computeIfAbsent(index, k -> EnumSet.noneOf(JsonEvent.class)).add(element);
            observed.arrayMaxLength = Math.max(observed.arrayMaxLength, index + 1);
            index++;
        }
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

    private static InferredSchema resolve(Map<String, FieldObserved> byName, String fileName, JsonParserConfig config) {
        if (byName.isEmpty()) {
            return InferredSchema.empty();
        }
        List<String> columns = new ArrayList<>(byName.size());
        Map<String, Class<?>> columnTypes = new LinkedHashMap<>(byName.size());
        boolean flatten = config.nestedMode() == JsonParserConfig.NestedMode.FLATTEN;
        boolean expandArrays = flatten && config.arrayColumns() == JsonParserConfig.ArrayColumnsMode.EXPAND;
        for (Map.Entry<String, FieldObserved> entry : byName.entrySet()) {
            String field = entry.getKey();
            FieldObserved observed = entry.getValue();
            if (flatten && isPureContainer(observed.kinds)) {
                // FLATTEN: nested objects emit their leaf columns only; the
                // container itself gets no column (no silent prefix shadowing).
                continue;
            }
            if (expandArrays && observed.isArray && !observed.arrayHasObjectElements) {
                int length = observed.arrayMaxLength;
                for (int i = 0; i < length; i++) {
                    String indexedColumn = field + "[" + i + "]";
                    EnumSet<JsonEvent> kinds = observed.arrayIndexKinds.get(i);
                    Class<?> type = kinds == null ? String.class : resolveElementType(kinds);
                    columns.add(indexedColumn);
                    columnTypes.put(indexedColumn, type);
                }
                continue;
            }
            Class<?> type = resolveType(field, observed, fileName);
            columns.add(field);
            columnTypes.put(field, type);
        }
        return new InferredSchema(List.copyOf(columns), Map.copyOf(columnTypes));
    }

    /** Returns whether the observed kinds are only nested objects (plus nulls). */
    private static boolean isPureContainer(EnumSet<JsonEvent> kinds) {
        int count = 0;
        for (JsonEvent kind : kinds) {
            if (kind == JsonEvent.VALUE_NULL) {
                continue;
            }
            if (kind != JsonEvent.START_OBJECT) {
                return false;
            }
            count++;
        }
        return count > 0;
    }

    private static Class<?> resolveElementType(EnumSet<JsonEvent> kinds) {
        boolean number = kinds.contains(JsonEvent.VALUE_NUMBER_INT)
                || kinds.contains(JsonEvent.VALUE_NUMBER_FLOAT);
        boolean booleanV = kinds.contains(JsonEvent.VALUE_TRUE) || kinds.contains(JsonEvent.VALUE_FALSE);
        boolean string = kinds.contains(JsonEvent.VALUE_STRING);
        if (!string && !booleanV && number) {
            return kinds.contains(JsonEvent.VALUE_NUMBER_FLOAT) ? Double.class : Long.class;
        }
        if (!number && !string && booleanV) {
            return Boolean.class;
        }
        return String.class;
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