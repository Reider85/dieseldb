package diesel.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonPathResolver;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import diesel.storage.json.JsonTypeMapper;

/**
 * Single owner of the JSONL schema: the ordered column names, the
 * case-insensitive column index, the column-to-Java-type map, value type
 * validation for the reader and the writer, JSON Path (dot-notation)
 * resolution into embedded JSON columns, and the schema sidecar file
 * (prompt 41).
 *
 * <p>The detailed conversion rules (strict vs lenient coercion, 2^53
 * handling) are owned by JsonTypeMapper (prompt 43), the diagnostics policy
 * by prompt 48 and the flatten / json_column storage rules by prompt 45.
 * Schema inference and evolution belong to prompt 44 - this class only
 * provides the base {@code <name>.schema.json} sidecar write/read/verify
 * mechanics those prompts build on.
 *
 * <p>Read validation rejects only the token shapes that would otherwise
 * <em>silently</em> corrupt a value: a nested JSON object/array dropping into
 * a typed (non-String) column, or a JSON number landing in a Boolean / date /
 * UUID column as raw text. Actual value conversion and the number-precision
 * / coercion rules are owned by JsonTypeMapper (prompt 43) and surface their
 * failures through the existing {@code file:line:field} diagnostics.
 */
public class JsonlSchemaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlSchemaManager.class);

    /** Format version written into the {@code <name>.schema.json} sidecar. */
    public static final int SCHEMA_FORMAT_VERSION = 1;

    /** The sidecar file suffix (dots included), e.g. {@code USERS.schema.json}. */
    public static final String SCHEMA_FILE_SUFFIX = ".schema.json";

    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final Map<String, Integer> indexByName;
    private final JsonParserConfig jsonConfig;
    /** Columns that hold compact JSON text for a nested object/array (prompt 45). */
    private final Set<Integer> nestedJsonColumns;

    public JsonlSchemaManager(List<String> columns, Map<String, Class<?>> columnTypes) {
        this(columns, columnTypes, JsonParserConfig.defaults());
    }

    public JsonlSchemaManager(List<String> columns, Map<String, Class<?>> columnTypes, JsonParserConfig jsonConfig) {
        this.columns = new ArrayList<>();
        if (columns != null) {
            this.columns.addAll(columns);
        }
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (columnTypes != null) {
            this.columnTypes.putAll(columnTypes);
        }
        this.indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < this.columns.size(); i++) {
            indexByName.put(this.columns.get(i), i);
        }
        this.jsonConfig = jsonConfig != null ? jsonConfig : JsonParserConfig.defaults();
        if (nestedMode() == JsonParserConfig.NestedMode.FLATTEN) {
            validateFlattenSchema();
        }
        this.nestedJsonColumns = new TreeSet<>();
    }

    /** Returns the streaming JSON configuration used for sidecar reads / JSON Path walks. */
    public JsonParserConfig jsonConfig() {
        return jsonConfig;
    }

    /** Returns the JSONL schema-matching mode (prompt 44), from the shared config. */
    public JsonParserConfig.SchemaMode schemaMode() {
        return jsonConfig.schemaMode();
    }

    /** Returns the JSONL nested-storage mode (prompt 45), from the shared config. */
    public JsonParserConfig.NestedMode nestedMode() {
        return jsonConfig.nestedMode();
    }

    /** Returns the JSONL array-storage mode (prompt 45), from the shared config. */
    public JsonParserConfig.ArrayColumnsMode arrayColumns() {
        return jsonConfig.arrayColumns();
    }

    /**
     * Flatten-mode schema rule (prompt 45): a column that is a dot-prefix of
     * another column (e.g. {@code user} and {@code user.address} together)
     * is ambiguous and rejected with a schema error.
     */
    private void validateFlattenSchema() {
        for (int i = 0; i < this.columns.size(); i++) {
            for (int j = 0; j < this.columns.size(); j++) {
                if (i == j) {
                    continue;
                }
                String a = this.columns.get(i);
                String b = this.columns.get(j);
                if (isDotPrefix(a, b)) {
                    throw new DieselIOException("flatten schema conflict: column '" + a
                            + "' is a dot-prefix of '" + b + "' (ambiguous nested path)", null);
                }
            }
        }
    }

    private static boolean isDotPrefix(String prefix, String candidate) {
        return candidate.length() > prefix.length()
                && candidate.regionMatches(true, 0, prefix, 0, prefix.length())
                && candidate.charAt(prefix.length()) == '.';
    }

    /**
     * Marks the column as a nested-JSON holder: its values are compact JSON
     * text of a nested object/array (prompt 45). In flatten mode such a column
     * is a leaf whose value must be re-embedded as structure on save; in
     * json_column mode every marked column is written back as the nested
     * structure it was captured from.
     */
    public void markNestedJson(int columnIndex) {
        if (columnIndex >= 0 && columnIndex < columns.size()) {
            nestedJsonColumns.add(columnIndex);
        }
    }

    /** Marks a column as a nested-JSON holder by its name (case-insensitive). */
    public void markNestedJson(String column) {
        markNestedJson(indexOf(column));
    }

    /** Returns whether the column index holds nested JSON text. */
    public boolean isNestedJson(int columnIndex) {
        return nestedJsonColumns.contains(columnIndex);
    }

    /** Returns the indexes of all nested-JSON holder columns. */
    public Set<Integer> nestedJsonIndexes() {
        return new TreeSet<>(nestedJsonColumns);
    }

    // ─── Schema accessors ────────────────────────────────────────────

    /** Returns the ordered canonical column names. */
    public List<String> columns() {
        return new ArrayList<>(columns);
    }

    /** Returns the number of schema columns. */
    public int size() {
        return columns.size();
    }

    /** Returns the (case-insensitive) index of the column, or {@code -1}. */
    public int indexOf(String column) {
        if (column == null) {
            return -1;
        }
        Integer idx = indexByName.get(column);
        return idx != null ? idx : -1;
    }

    /** Returns the canonical column name for an index, or {@code null}. */
    public String columnName(int index) {
        return index >= 0 && index < columns.size() ? columns.get(index) : null;
    }

    /** Returns the expected Java type of the given column, or {@code null}. */
    public Class<?> typeOf(String column) {
        return typeOf(indexOf(column));
    }

    /** Returns the expected Java type of the given column index, or {@code null}. */
    public Class<?> typeOf(int index) {
        if (index < 0 || index >= columns.size()) {
            return null;
        }
        return columnTypes.get(columns.get(index));
    }

    /**
     * Returns the effective type treated as a STRING column: an explicit
     * String type or an unmapped/missing type is treated as String (nested
     * JSON text is only capturable into String columns, prompt 41/45).
     */
    private Class<?> effectiveType(int index) {
        Class<?> type = typeOf(index);
        return type == null ? String.class : type;
    }

    // ─── Typo detection (prompt 44) ───────────────────────────────────

    /**
     * Returns the schema column with the smallest case-insensitive Levenshtein
     * distance to {@code name}, when that distance is within the typo
     * threshold ({@code max(2, name.length() / 3)}), or {@code null} when the
     * name is blank, matches no column closely or the schema has no columns.
     * Used to turn an unknown JSON field into an actionable error with a
     * "did you mean ..." hint instead of silently dropping or accepting it.
     */
    public String suggestNearestColumn(String name) {
        if (name == null || name.isBlank() || columns.isEmpty()) {
            return null;
        }
        String target = name.trim().toLowerCase(java.util.Locale.ROOT);
        String best = null;
        int bestDistance = Integer.MAX_VALUE;
        for (String column : columns) {
            int distance = levenshtein(target, column.toLowerCase(java.util.Locale.ROOT));
            if (distance < bestDistance) {
                bestDistance = distance;
                best = column;
            }
        }
        int threshold = Math.max(2, name.length() / 3);
        return bestDistance <= threshold ? best : null;
    }

    private static int levenshtein(String a, String b) {
        int[] previous = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            previous[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            int[] current = new int[b.length() + 1];
            current[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                current[j] = Math.min(
                        Math.min(previous[j] + 1, current[j - 1] + 1),
                        previous[j - 1] + cost);
            }
            previous = current;
        }
        return previous[b.length()];
    }

    // ─── Read-side validation (prompt 41) ────────────────────────────

    /**
     * Validates that a raw JSON token can be placed into the schema column
     * without silent type corruption. Callers convert afterwards; parse
     * failures keep flowing through the existing {@code file:line:field}
     * conversion diagnostics.
     *
     * @param columnIndex the schema column index
     * @param token the JSON value event read from the record
     * @param raw the raw token text (used in the message)
     * @param context a prefix carrying {@code file:line} diagnostics, e.g.
     *                {@code "users.jsonl:line 12: "}
     * @throws DieselIOException when the token shape cannot map to the column
     */
    public void validateReadToken(int columnIndex, JsonEvent token, String raw, String context) {
        if (columnIndex < 0 || columnIndex >= columns.size() || token == null) {
            return;
        }
        String field = columns.get(columnIndex);
        Class<?> type = effectiveType(columnIndex);
        if (isStringType(type)) {
            return;
        }
        if (token == JsonEvent.VALUE_NULL) {
            return;
        }
        if (isNumericType(type)) {
            if (token == JsonEvent.VALUE_STRING
                    || token == JsonEvent.VALUE_NUMBER_INT
                    || token == JsonEvent.VALUE_NUMBER_FLOAT) {
                return;
            }
            throw incompatible(context, field, raw, token, type);
        }
        if (type == Boolean.class || type == LocalDate.class
                || type == LocalDateTime.class || type == UUID.class) {
            if (token == JsonEvent.VALUE_STRING
                    || (type == Boolean.class
                        && (token == JsonEvent.VALUE_TRUE || token == JsonEvent.VALUE_FALSE))) {
                return;
            }
            throw incompatible(context, field, raw, token, type);
        }
        if (token == JsonEvent.VALUE_STRING) {
            return;
        }
        throw incompatible(context, field, raw, token, type);
    }

    private DieselIOException incompatible(String context, String field, String raw, JsonEvent token, Class<?> type) {
        String tag = switch (token) {
            case START_OBJECT -> "object";
            case START_ARRAY -> "array";
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> "number";
            case VALUE_TRUE, VALUE_FALSE -> "boolean";
            default -> String.valueOf(token);
        };
        String detail = raw != null && !raw.isBlank() ? " " + raw : "";
        return new DieselIOException(context + "field '" + field + "': JSON " + tag + detail
                + " is not compatible with column type " + typeName(type), null);
    }

    // ─── Write-side validation (prompt 41) ───────────────────────────

    /**
     * Validates that a Java value can be serialised into the schema column
     * without breaking the read-back contract. Rejects the shapes that would
     * either fail or silently corrupt on load; numeric conversion and the 2^53
     * DOUBLE precision rule are delegated to JsonTypeMapper (prompt 43).
     *
     * @param columnIndex the schema column index
     * @param value the value about to be written
     * @param field the field name used in diagnostics
     * @param recordContext a prefix carrying the record coordinate, e.g.
     *                      {@code "record 1: "} (no file/line exists at write
     *                      time)
     * @throws DieselIOException when the value is not representable in the column
     */
    public void validateWriteValue(int columnIndex, Object value, String field, String recordContext) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            return;
        }
        Class<?> type = effectiveType(columnIndex);
        if (isStringType(type) || value == null) {
            return;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?> || value.getClass().isArray()) {
            throw new DieselIOException(recordContext + "field '" + field + "': JSON object/array cannot be stored "
                    + "in column type " + typeName(type)
                    + " (nested structures are captured as JSON text only in STRING columns)", null);
        }
        if (value instanceof Boolean) {
            requireType(field, value, type, Boolean.class, recordContext);
        } else if (value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte) {
            JsonTypeMapper.validateDoublePrecision(type, value, field, recordContext);
            requireNumeric(field, value, type, recordContext);
        } else if (value instanceof Float || value instanceof Double) {
            if (type == Integer.class || type == Long.class || type == Short.class || type == Byte.class) {
                throw new DieselIOException(recordContext + "field '" + field + "': floating-point value " + value
                        + " cannot be stored in column type " + typeName(type), null);
            }
        } else if (value instanceof BigDecimal) {
            JsonTypeMapper.validateDoublePrecision(type, value, field, recordContext);
            if (type == Integer.class || type == Long.class || type == Short.class || type == Byte.class) {
                throw new DieselIOException(recordContext + "field '" + field + "': BigDecimal value " + value
                        + " cannot be stored in column type " + typeName(type), null);
            }
        } else if (value instanceof LocalDate) {
            requireType(field, value, type, LocalDate.class, recordContext);
        } else if (value instanceof LocalDateTime) {
            requireType(field, value, type, LocalDateTime.class, recordContext);
        } else if (value instanceof UUID) {
            requireType(field, value, type, UUID.class, recordContext);
        }
    }

    private void requireNumeric(String field, Object value, Class<?> type, String recordContext) {
        if (!isNumericType(type) && type != BigDecimal.class) {
            throw new DieselIOException(recordContext + "field '" + field + "': value " + value
                    + " cannot be stored in column type " + typeName(type), null);
        }
    }

    private void requireType(String field, Object value, Class<?> type, Class<?> expected, String recordContext) {
        if (type != expected) {
            throw new DieselIOException(recordContext + "field '" + field + "': value " + value
                    + " cannot be stored in column type " + typeName(type), null);
        }
    }

    // ─── JSON Path (dot-notation) base (prompt 41/45) ────────────────

    /**
     * A projection item resolved against the schema: either an exact column
     * ({@code segments} empty) or a dot-path whose longest schema-column
     * prefix was matched and whose remaining segments address a nested value
     * inside that column's captured JSON text.
     *
     * @param columnIndex the schema column index holding the value
     * @param segments the remaining dot-path segments inside the column value
     * @param key the original projection item string
     */
    public record ProjectionSlot(int columnIndex, List<String> segments, String key) {
        public boolean isPlainColumn() {
            return columnIndex >= 0 && segments.isEmpty();
        }
    }

    /**
     * Resolves a projection item (a plain column name or a dot path) against
     * the schema. Uses the longest schema-column prefix for dotted paths so
     * {@code DATA.user.address.city} maps to the {@code DATA} column with the
     * remaining segments; an item that matches no column at all yields a slot
     * with {@code columnIndex == -1}. Resolution is delegated to the single
     * path engine {@link JsonPathResolver} shared with the SQL layer (prompt 45).
     */
    public ProjectionSlot resolveProjectionItem(String item) {
        if (item == null || item.isBlank()) {
            return new ProjectionSlot(-1, List.of(), item);
        }
        JsonPathResolver.ResolvedPath resolved = JsonPathResolver.resolve(columns, item);
        return new ProjectionSlot(resolved.columnIndex(), resolved.segments(), item);
    }

    /**
     * Extracts the value at the given dot-path segments from a JSON text value
     * (the compact JSON text captured into a STRING column). Token-level walk,
     * no DOM. Returns {@code null} when the path is absent or the container is
     * not an object; scalar leaves are returned as their raw token text and
     * nested leaves as compact JSON text.
     */
    public Object extractPathValue(String jsonText, List<String> segments) {
        return JsonPathResolver.extract(jsonText, segments, jsonConfig);
    }

    private static void skipValue(JsonStreamParser p, JsonEvent t) throws IOException {
        if (t == JsonEvent.START_OBJECT || t == JsonEvent.START_ARRAY) {
            p.skipChildren();
        }
    }

    // ─── Schema sidecar file (base, prompt 41/44) ────────────────────

    /**
     * A deterministic description of the table schema as written to the
     * {@code <name>.schema.json} sidecar.
     *
     * @param formatVersion the sidecar format version
     * @param columns       the ordered columns with names and type names
     * @param data          the data-file stamp (mtime/size) at inference time,
     *                      or {@code null} when the stamp is unknown or was not
     *                      written (pre-prompt-44 sidecars)
     */
    public record SchemaDescriptor(int formatVersion, List<SchemaColumn> columns, SchemaStamp data) {

        /** Creates a descriptor without a data stamp (sidecars written before prompt 44). */
        public SchemaDescriptor(int formatVersion, List<SchemaColumn> columns) {
            this(formatVersion, columns, null);
        }
    }

    /** A single schema column in the sidecar descriptor. */
    public record SchemaColumn(String name, String type) {
    }

    /**
     * The data-file stamp recorded in the sidecar so a loader can detect
     * whether the schema is stale (prompt 44): the data file's last-modified
     * mtime in milliseconds and its size in bytes at schema-inference time.
     *
     * @param mtimeMillis the data file {@code lastModified()} value
     * @param sizeBytes   the data file length in bytes
     */
    public record SchemaStamp(long mtimeMillis, long sizeBytes) {

        /** Returns a fresh stamp from the given data file's current mtime/size. */
        public static SchemaStamp of(Path dataFile) {
            long mtime = Files.exists(dataFile) ? dataFile.toFile().lastModified() : -1L;
            long size = Files.exists(dataFile) ? dataFile.toFile().length() : -1L;
            return new SchemaStamp(mtime, size);
        }

        /** Returns whether the stamp matches the data file's current mtime/size. */
        public boolean matches(Path dataFile) {
            SchemaStamp current = of(dataFile);
            return mtimeMillis == current.mtimeMillis && sizeBytes == current.sizeBytes;
        }

        /**
         * Returns whether the data stamp is usable for a freshness check: both
         * a non-negative mtime/size recorded and a matching current file. A
         * {@code null} stamp (sidecar written before prompt 44) is always stale.
         */
        public static boolean isFresh(SchemaStamp stamp, Path dataFile) {
            return stamp != null && stamp.matches(dataFile);
        }
    }

    /** Returns the current schema as a sidecar descriptor. */
    public SchemaDescriptor describe() {
        List<SchemaColumn> columns = new ArrayList<>();
        for (int i = 0; i < this.columns.size(); i++) {
            columns.add(new SchemaColumn(this.columns.get(i), typeName(effectiveType(i))));
        }
        return new SchemaDescriptor(SCHEMA_FORMAT_VERSION, columns);
    }

    /**
     * Writes the sidecar schema descriptor deterministically (UTF-8, column
     * order, {@code \n} terminator). Schema inference and evolution decisions
     * belong to prompt 44; this only persists the current schema.
     */
    public void writeSchemaFile(Path target) throws IOException {
        writeSchemaFile(target, null);
    }

    /**
     * Writes the sidecar schema descriptor deterministically, recording the
     * data-file stamp {@code stamp} (mtime/size at inference time, prompt 44)
     * so later loads can detect staleness. A {@code null} stamp omits the
     * {@code data} block (pre-prompt-44 layout).
     */
    public void writeSchemaFile(Path target, SchemaStamp stamp) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(target, StandardCharsets.UTF_8);
             JsonStreamGenerator g = JsonStreams.createGenerator(bw, jsonConfig)) {
            g.writeStartObject();
            g.writeFieldName("formatVersion");
            g.writeNumber(SCHEMA_FORMAT_VERSION);
            g.writeFieldName("columns");
            g.writeStartArray();
            for (SchemaColumn column : describe().columns()) {
                g.writeStartObject();
                g.writeFieldName("name");
                g.writeString(column.name());
                g.writeFieldName("type");
                g.writeString(column.type());
                g.writeEndObject();
            }
            g.writeEndArray();
            if (stamp != null) {
                g.writeFieldName("data");
                g.writeStartObject();
                g.writeFieldName("mtime");
                g.writeNumber(stamp.mtimeMillis());
                g.writeFieldName("size");
                g.writeNumber(stamp.sizeBytes());
                g.writeEndObject();
            }
            g.writeEndObject();
            g.writeRaw('\n');
        }
    }

    /**
     * Reads a sidecar descriptor, or returns {@code null} when the file is
     * missing or malformed (a WARNING is logged for malformed content). The
     * descriptor carries the recorded data-file stamp (prompt 44) so callers
     * can detect schema staleness via {@link SchemaStamp#isFresh}.
     */
    public SchemaDescriptor readSchemaFile(Path file) {
        if (!Files.exists(file)) {
            return null;
        }
        try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8);
             JsonStreamParser p = JsonStreams.createParser(br, jsonConfig)) {
            int formatVersion = -1;
            List<SchemaColumn> columns = new ArrayList<>();
            SchemaStamp data = null;
            while (p.nextToken() != JsonEvent.END_INPUT) {
                if (p.currentEvent() == JsonEvent.FIELD_NAME && "formatVersion".equals(p.currentName())) {
                    p.nextToken();
                    formatVersion = (int) p.getLongValue();
                } else if (p.currentEvent() == JsonEvent.FIELD_NAME && "columns".equals(p.currentName())) {
                    p.nextToken();
                    if (p.currentEvent() != JsonEvent.START_ARRAY) {
                        return null;
                    }
                    while (p.nextToken() != JsonEvent.END_ARRAY) {
                        if (p.currentEvent() != JsonEvent.START_OBJECT) {
                            return null;
                        }
                        String name = null;
                        String type = null;
                        while (p.nextToken() != JsonEvent.END_OBJECT) {
                            if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                                return null;
                            }
                            String field = p.currentName();
                            p.nextToken();
                            if ("name".equals(field)) {
                                name = p.getText();
                            } else if ("type".equals(field)) {
                                type = p.getText();
                            } else {
                                skipValue(p, p.currentEvent());
                            }
                        }
                        columns.add(new SchemaColumn(name, type));
                    }
                } else if (p.currentEvent() == JsonEvent.FIELD_NAME && "data".equals(p.currentName())) {
                    p.nextToken();
                    if (p.currentEvent() != JsonEvent.START_OBJECT) {
                        return null;
                    }
                    long mtime = -1;
                    long size = -1;
                    while (p.nextToken() != JsonEvent.END_OBJECT) {
                        if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                            return null;
                        }
                        String field = p.currentName();
                        p.nextToken();
                        if ("mtime".equals(field)) {
                            mtime = p.getLongValue();
                        } else if ("size".equals(field)) {
                            size = p.getLongValue();
                        } else {
                            skipValue(p, p.currentEvent());
                        }
                    }
                    data = new SchemaStamp(mtime, size);
                }
            }
            return new SchemaDescriptor(formatVersion, columns, data);
        } catch (IOException e) {
            LOGGER.warn("Failed to read JSONL schema sidecar {}: {}", file, e.getMessage());
            return null;
        }
    }

    /**
     * Verifies a sidecar descriptor against the current schema and returns a
     * list of problems (empty when the sidecar is consistent).
     */
    public List<String> verifySchemaFile(SchemaDescriptor descriptor) {
        List<String> problems = new ArrayList<>();
        if (descriptor == null || descriptor.columns() == null) {
            problems.add("schema sidecar is missing or malformed");
            return problems;
        }
        if (descriptor.formatVersion() > SCHEMA_FORMAT_VERSION) {
            problems.add("schema sidecar format version " + descriptor.formatVersion()
                    + " exceeds supported " + SCHEMA_FORMAT_VERSION);
        }
        List<SchemaColumn> stored = descriptor.columns();
        if (stored.size() != columns.size()) {
            problems.add("schema sidecar has " + stored.size() + " columns, schema has " + columns.size());
        }
        for (int i = 0; i < Math.min(stored.size(), columns.size()); i++) {
            SchemaColumn column = stored.get(i);
            if (!columns.get(i).equalsIgnoreCase(column.name() == null ? "" : column.name())) {
                problems.add("schema sidecar column " + i + " '" + column.name()
                        + "' does not match schema '" + columns.get(i) + "'");
            } else if (!typeName(effectiveType(i)).equals(column.type())) {
                problems.add("schema sidecar column '" + column.name() + "': type '" + column.type()
                        + "' does not match schema '" + typeName(effectiveType(i)) + "'");
            }
        }
        return problems;
    }

    // ─── Type name helpers ───────────────────────────────────────────

    /** Returns the sidecar type name for a Java class, or {@code null}. */
    public static String typeName(Class<?> type) {
        if (type == null) {
            return null;
        }
        return switch (type.getSimpleName()) {
            case "Long" -> "Long";
            case "Integer" -> "Integer";
            case "Short" -> "Short";
            case "Byte" -> "Byte";
            case "Double" -> "Double";
            case "Float" -> "Float";
            case "BigDecimal" -> "BigDecimal";
            case "Boolean" -> "Boolean";
            case "LocalDate" -> "LocalDate";
            case "LocalDateTime" -> "LocalDateTime";
            case "UUID" -> "UUID";
            case "String" -> "String";
            default -> type.getSimpleName();
        };
    }

    /** Resolves a sidecar type name to a Java class, or {@code null}. */
    public static Class<?> typeClass(String name) {
        if (name == null) {
            return null;
        }
        return switch (name) {
            case "Long" -> Long.class;
            case "Integer" -> Integer.class;
            case "Short" -> Short.class;
            case "Byte" -> Byte.class;
            case "Double" -> Double.class;
            case "Float" -> Float.class;
            case "BigDecimal" -> BigDecimal.class;
            case "Boolean" -> Boolean.class;
            case "LocalDate" -> LocalDate.class;
            case "LocalDateTime" -> LocalDateTime.class;
            case "UUID" -> UUID.class;
            case "String" -> String.class;
            default -> null;
        };
    }

    private static boolean isStringType(Class<?> type) {
        return type == String.class;
    }

    private static boolean isNumericType(Class<?> type) {
        return type == Long.class || type == Integer.class || type == Short.class || type == Byte.class
                || type == Double.class || type == Float.class || type == BigDecimal.class;
    }

    }