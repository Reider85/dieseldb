package diesel.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import diesel.storage.json.JsonTypeMapper;

/**
 * Streaming reader for JSON Lines (NDJSON) files: one JSON object per line.
 * Rows are parsed one line at a time through the streaming JSON abstraction
 * ({@code diesel.storage.json}, prompt 42) - no DOM tree is ever built per
 * line, so memory stays constant regardless of line length or file size
 * (prompt 40 requirement). A nested object/array value is captured as compact
 * JSON text with a token-level walk and stored in the mapped column; full
 * nested storage rules land in prompt 45.
 *
 * <p>Fields map to schema columns by name (case-insensitive); the JSONL
 * format is self-describing, so there is no header line. A missing field
 * yields {@code null}; an unknown field is handled per the schema mode
 * (prompt 44): {@code strict} fails with a "did you mean ..." typo hint,
 * hybrid/inferred warn once per file and are expanded at the storage level.
 * Blank and whitespace-only lines are skipped and a UTF-8 BOM on the first
 * record is stripped (prompt 48 covers BOM handling formally).
 *
 * <p>Error diagnostics carry {@code file:line} / {@code file:line:field}
 * context.
 *
 * <p>Type validation (prompt 41): every mapped field is checked against its
 * schema column type through the shared {@link JsonlSchemaManager} before
 * conversion, so a value that cannot live in a typed column fails with
 * {@code file:line:field} diagnostics instead of silently storing raw text.
 * Conversion itself is owned by {@link JsonTypeMapper} (prompt 43): numbers
 * follow the precision rules (2^53 boundary in DOUBLE columns, exact LONG
 * reads, scientific notation supported) and the {@code jsonl.type.coercion}
 * mode decides whether a JSON string may be coerced into a numeric column
 * (STRICT default rejects, LENIENT allows with a WARNING).
 *
 * <p>Projection (prompt 41): {@link #setProjection} limits a read to the
 * requested schema columns and/or JSON Path (dot-notation) items. Fields
 * outside the projection are skipped at the token level - nested structures
 * via {@code JsonStreamParser.skipChildren()} without any capture/conversion -
 * so a 3-of-40-column projection parses only 3 values per row (the base for
 * the projection-pushdown work in prompt 55). {@link #getParsedFieldCount()} /
 * {@link #getSkippedFieldCount()} expose the exact skipped-vs-parsed split
 * for measurements.
 */
public class JsonlRowReader implements Iterator<Map<String, Object>>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonlRowReader.class);
    private static final String BOM = "\uFEFF";

    private final BufferedReader reader;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String fileName;
    private final Map<String, Integer> indexByName;
    private final JsonlSchemaManager schema;
    private final JsonParserConfig config;
    private final JsonTypeMapper typeMapper;
    private List<JsonlSchemaManager.ProjectionSlot> projectionSlots;
    private boolean[] neededByColumn;
    private long parsedFieldCount;
    private long skippedFieldCount;
    private JsonStreamParser parser;
    private boolean finished;
    private boolean firstLine = true;
    private long lineNumber;
    private long lastRowLine;
    private boolean unknownFieldWarned;
    private boolean duplicateFieldWarned;
    private boolean unresolvedProjectionWarned;

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     */
    public JsonlRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes) {
        this(reader, columns, columnTypes, null, JsonParserConfig.defaults());
    }

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     */
    public JsonlRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes,
                          String fileName) {
        this(reader, columns, columnTypes, fileName, JsonParserConfig.defaults());
    }

    /**
     * @param reader      the underlying character-input stream
     * @param columns     the ordered column names
     * @param columnTypes column name to expected Java type
     * @param fileName    the source file name used in error diagnostics, or
     *                    {@code null} when unknown
     * @param config      the streaming JSON configuration (backend, limits,
     *                    duplicate-key policy) used for every line
     */
    public JsonlRowReader(BufferedReader reader, List<String> columns, Map<String, Class<?>> columnTypes,
                          String fileName, JsonParserConfig config) {
        this.reader = reader;
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.fileName = fileName;
        this.config = config;
        this.typeMapper = new JsonTypeMapper(config);
        this.indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            indexByName.put(columns.get(i), i);
        }
        this.schema = new JsonlSchemaManager(columns, columnTypes, config);
    }

    /**
     * @param reader   the underlying character-input stream
     * @param schema   the shared schema manager owning type validation and
     *                 JSON Path resolution
     * @param fileName the source file name used in error diagnostics, or
     *                 {@code null} when unknown
     */
    public JsonlRowReader(BufferedReader reader, JsonlSchemaManager schema, String fileName) {
        this(reader, schema, fileName, JsonParserConfig.defaults());
    }

    /**
     * @param reader   the underlying character-input stream
     * @param schema   the shared schema manager owning type validation and
     *                 JSON Path resolution
     * @param fileName the source file name used in error diagnostics, or
     *                 {@code null} when unknown
     * @param config   the streaming JSON configuration used for every line
     */
    public JsonlRowReader(BufferedReader reader, JsonlSchemaManager schema, String fileName,
                          JsonParserConfig config) {
        this.reader = reader;
        this.schema = schema;
        this.columns = schema.columns();
        this.columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            Class<?> type = schema.typeOf(i);
            columnTypes.put(columns.get(i), type == null ? String.class : type);
        }
        this.fileName = fileName;
        this.config = config;
        this.typeMapper = new JsonTypeMapper(config);
        this.indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            indexByName.put(columns.get(i), i);
        }
    }

    @Override
    public boolean hasNext() {
        if (parser == null && !finished) {
            prefetch();
        }
        return parser != null;
    }

    /** Returns the next row as a column-to-value map. */
    @Override
    public Map<String, Object> next() {
        Object[] values = nextArray();
        Map<String, Object> row = new HashMap<>(Math.max(columns.size() * 2, 4));
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), values[i]);
        }
        return row;
    }

    /**
     * Returns the next row as a compact Object[] array whose slot {@code i}
     * holds the value of schema column {@code i} (prompt 36). Missing fields
     * leave {@code null} slot values. Values are validated against their
     * column types (prompt 41) - a JSON token that cannot live in a typed
     * column fails with {@code file:line:field} diagnostics.
     */
    public Object[] nextArray() {
        if (finished) {
            throw new NoSuchElementException("No more rows in JSONL file");
        }
        if (parser == null) {
            prefetch();
        }
        if (parser == null) {
            throw new NoSuchElementException("No more rows in JSONL file");
        }
        JsonStreamParser p = parser;
        lastRowLine = lineNumber;
        Object[] row = new Object[columns.size()];
        boolean[] seen = new boolean[columns.size()];
        try {
            while (p.nextToken() != JsonEvent.END_OBJECT) {
                if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                    throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                            + ": malformed JSON record: expected a field name, found " + p.currentEvent(), null);
                }
                String field = p.currentName();
                Integer idx = indexByName.get(field);
                JsonEvent valueToken = p.nextToken();
                if (valueToken == null) {
                    throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                            + ": malformed JSON record: missing value for field '" + field + "'", null);
                }
                if (idx == null) {
                    handleUnknownField(field);
                    skipValue(p, valueToken);
                    skippedFieldCount++;
                } else {
                    if (seen[idx]) {
                        handleDuplicateField(field);
                    }
                    parsedFieldCount++;
                    row[idx] = parseFieldValue(p, idx, field, valueToken);
                    seen[idx] = true;
                }
            }
        } catch (IOException e) {
            throw new DieselIOException(contextPrefix() + "line " + lastRowLine + ": " + e.getMessage(), e);
        } finally {
            closeQuietly(p);
            parser = null;
        }
        return row;
    }

    /**
     * Restricts subsequent {@link #nextProjected()} / {@link #nextProjectedMap()}
     * reads to the requested items - plain schema column names and/or JSON
     * Path (dot-notation) items (prompt 41). Fields not requested are skipped
     * at the token level; unresolved names are dropped with a single WARNING.
     * Passing {@code null} or an empty collection clears the projection.
     */
    public void setProjection(Collection<String> items) {
        if (items == null || items.isEmpty()) {
            projectionSlots = null;
            neededByColumn = null;
            return;
        }
        List<JsonlSchemaManager.ProjectionSlot> slots = new ArrayList<>();
        boolean[] needed = new boolean[columns.size()];
        for (String item : items) {
            JsonlSchemaManager.ProjectionSlot slot = schema.resolveProjectionItem(item);
            if (slot.columnIndex() < 0) {
                warnUnresolvedProjection(item);
                continue;
            }
            needed[slot.columnIndex()] = true;
            slots.add(slot);
        }
        projectionSlots = slots.isEmpty() ? null : slots;
        neededByColumn = slots.isEmpty() ? null : needed;
    }

    /** Returns the current projection items in output order (column names or dot paths). */
    public List<String> getProjectionItems() {
        if (projectionSlots == null) {
            return List.of();
        }
        List<String> keys = new ArrayList<>(projectionSlots.size());
        for (JsonlSchemaManager.ProjectionSlot slot : projectionSlots) {
            keys.add(slot.key());
        }
        return keys;
    }

    /**
     * Returns the next row projected onto the items selected by
     * {@link #setProjection}: the returned array is aligned with
     * {@link #getProjectionItems()}. Non-requested fields are skipped without
     * conversion or capture. Falls back to {@link #nextArray()} when no
     * projection is configured.
     */
    public Object[] nextProjected() {
        if (projectionSlots == null || projectionSlots.isEmpty()) {
            return nextArray();
        }
        if (finished) {
            throw new NoSuchElementException("No more rows in JSONL file");
        }
        if (parser == null) {
            prefetch();
        }
        if (parser == null) {
            throw new NoSuchElementException("No more rows in JSONL file");
        }
        JsonStreamParser p = parser;
        lastRowLine = lineNumber;
        int slots = projectionSlots.size();
        Object[] values = new Object[slots];
        boolean[] seenColumn = new boolean[columns.size()];
        try {
            while (p.nextToken() != JsonEvent.END_OBJECT) {
                if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                    throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                            + ": malformed JSON record: expected a field name, found " + p.currentEvent(), null);
                }
                String field = p.currentName();
                Integer idx = indexByName.get(field);
                JsonEvent valueToken = p.nextToken();
                if (valueToken == null) {
                    throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                            + ": malformed JSON record: missing value for field '" + field + "'", null);
                }
                if (idx == null) {
                    handleUnknownField(field);
                    skipValue(p, valueToken);
                    skippedFieldCount++;
                    continue;
                }
                if (neededByColumn == null || !neededByColumn[idx]) {
                    skipValue(p, valueToken);
                    skippedFieldCount++;
                    continue;
                }
                if (seenColumn[idx]) {
                    handleDuplicateField(field);
                }
                parsedFieldCount++;
                Object value = parseFieldValue(p, idx, field, valueToken);
                seenColumn[idx] = true;
                for (int s = 0; s < slots; s++) {
                    JsonlSchemaManager.ProjectionSlot slot = projectionSlots.get(s);
                    if (slot.columnIndex() != idx) {
                        continue;
                    }
                    if (slot.isPlainColumn()) {
                        values[s] = value;
                    } else if (value instanceof String txt
                            && (valueToken == JsonEvent.START_OBJECT || valueToken == JsonEvent.START_ARRAY)) {
                        values[s] = schema.extractPathValue(txt, slot.segments());
                    }
                }
            }
        } catch (IOException e) {
            throw new DieselIOException(contextPrefix() + "line " + lastRowLine + ": " + e.getMessage(), e);
        } finally {
            closeQuietly(p);
            parser = null;
        }
        return values;
    }

    /**
     * Returns the next row as a column-to-value map restricted to the current
     * projection items (plain columns and/or dot paths). Falls back to the
     * full row map when no projection is configured.
     */
    public Map<String, Object> nextProjectedMap() {
        Object[] values = nextProjected();
        if (projectionSlots == null || projectionSlots.isEmpty()) {
            Map<String, Object> row = new HashMap<>(Math.max(columns.size() * 2, 4));
            for (int i = 0; i < columns.size(); i++) {
                row.put(columns.get(i), values[i]);
            }
            return row;
        }
        Map<String, Object> row = new HashMap<>(Math.max(projectionSlots.size() * 2, 4));
        for (int s = 0; s < projectionSlots.size(); s++) {
            row.put(projectionSlots.get(s).key(), values[s]);
        }
        return row;
    }

    /** Number of map-able field values actually parsed since the reader was created. */
    public long getParsedFieldCount() {
        return parsedFieldCount;
    }

    /** Number of fields skipped at the token level (projection / unknown fields). */
    public long getSkippedFieldCount() {
        return skippedFieldCount;
    }

    /** Reads all remaining rows into a list. */
    public List<Map<String, Object>> readAll() {
        List<Map<String, Object>> result = new ArrayList<>();
        while (hasNext()) {
            result.add(next());
        }
        return result;
    }

    /**
     * Returns the 1-based physical line number on which the last consumed row
     * started, or {@code 0} before any row has been read.
     */
    public long getLineNumber() {
        return lastRowLine;
    }

    /** Releases the active parser; the underlying reader is not closed. */
    @Override
    public void close() throws IOException {
        if (parser != null) {
            closeQuietly(parser);
            parser = null;
        }
    }

    // ─── Internal ───────────────────────────────────────────────────

    private void prefetch() {
        while (!finished) {
            String line = readLineSafely();
            if (line == null) {
                finished = true;
                return;
            }
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
            JsonStreamParser p;
            try {
                p = JsonStreams.createParser(line, config);
            } catch (IOException e) {
                throw new DieselIOException(contextPrefix() + "line " + lineNumber + ": " + e.getMessage(), e);
            }
            try {
                if (p.nextToken() != JsonEvent.START_OBJECT) {
                    closeQuietly(p);
                    throw new DieselIOException(contextPrefix() + "line " + lineNumber
                            + ": JSON record must be a single JSON object, found " + p.currentEvent(), null);
                }
            } catch (IOException e) {
                closeQuietly(p);
                throw new DieselIOException(contextPrefix() + "line " + lineNumber + ": " + e.getMessage(), e);
            }
            parser = p;
            return;
        }
    }

    private String readLineSafely() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            finished = true;
            throw new DieselIOException(contextPrefix() + "I/O error while reading file at line " + lineNumber, e);
        }
    }

    private Object parseFieldValue(JsonStreamParser p, int idx, String field, JsonEvent valueToken) throws IOException {
        schema.validateReadToken(idx, valueToken, p.getText(), contextPrefix() + "line " + lastRowLine + ": ");
        switch (valueToken) {
            case VALUE_NULL -> {
                return null;
            }
            case VALUE_STRING, VALUE_TRUE, VALUE_FALSE, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                return typeMapper.toColumnValue(typeAt(idx), valueToken, p.getText(),
                        contextPrefix() + "line " + lastRowLine + ": field '" + field + "': ");
            }
            case START_OBJECT, START_ARRAY -> {
                return captureNested(p);
            }
            default -> throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                    + ": field '" + field + "': unsupported JSON value " + valueToken, null);
        }
    }

    private Class<?> typeAt(Integer columnIndex) {
        if (columnIndex == null || columnIndex < 0 || columnIndex >= columns.size()) {
            return null;
        }
        Class<?> type = columnTypes.get(columns.get(columnIndex));
        if (type == null || "String".equals(type.getSimpleName())) {
            return null;
        }
        return type;
    }

    /**
     * Captures the nested object/array value at the parser's current position
     * (START_OBJECT or START_ARRAY) as compact JSON text using a pure
     * token-level copy - no DOM tree is built (prompt 40/42). Advances the
     * parser past the whole structure.
     */
    private String captureNested(JsonStreamParser p) throws IOException {
        StringWriter sw = new StringWriter();
        try (JsonStreamGenerator g = JsonStreams.createGenerator(sw, config)) {
            g.copyCurrentStructure(p);
            g.flush();
        }
        return sw.toString();
    }

    private void handleUnknownField(String field) {
        if (config.schemaMode() == JsonParserConfig.SchemaMode.STRICT) {
            String hint = schema.suggestNearestColumn(field);
            throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                    + ": unknown field '" + field + "' is not part of the table schema (strict schema mode)"
                    + (hint != null ? "; did you mean '" + hint + "'?" : ""), null);
        }
        warnUnknownField(field);
    }

    private void warnUnknownField(String field) {
        if (!unknownFieldWarned) {
            unknownFieldWarned = true;
            LOGGER.warn(contextPrefix() + "line " + lastRowLine + ": unknown field '" + field
                    + "' ignored (not part of the table schema); further occurrences are not reported "
                    + "(hybrid schema mode expands the schema, prompt 44)");
        }
    }

    private void warnUnresolvedProjection(String item) {
        if (!unresolvedProjectionWarned) {
            unresolvedProjectionWarned = true;
            LOGGER.warn("projection item '" + item
                    + "' matches no schema column or JSON path; further occurrences are not reported "
                    + "(prompt 41 ignored unresolved projection items)");
        }
    }

    private void handleDuplicateField(String field) {
        if (config.duplicateKeys() == JsonParserConfig.DuplicateKeyMode.FAIL) {
            throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                    + ": duplicate field '" + field
                    + "' (set jsonl.duplicate.keys=LAST_WINS to keep the last value)", null);
        }
        warnDuplicateField(field);
    }

    private void warnDuplicateField(String field) {
        if (!duplicateFieldWarned) {
            duplicateFieldWarned = true;
            LOGGER.warn(contextPrefix() + "line " + lastRowLine + ": duplicate field '" + field
                    + "' - last value wins; further occurrences are not reported "
                    + "(prompt 43 will add the jsonl.duplicate.keys config)");
        }
    }

    /**
     * Advances the parser past a value that is not needed (unknown field or a
     * non-projected field). Scalars are already at their value token; nested
     * objects/arrays are skipped with {@code JsonStreamParser.skipChildren()}.
     */
    private static void skipValue(JsonStreamParser p, JsonEvent t) throws IOException {
        if (t == JsonEvent.START_OBJECT || t == JsonEvent.START_ARRAY) {
            p.skipChildren();
        }
    }

    private String contextPrefix() {
        return fileName != null ? fileName + ":" : "";
    }

    private static void closeQuietly(JsonStreamParser p) {
        try {
            p.close();
        } catch (IOException ignored) {
            // Best effort.
        }
    }
}