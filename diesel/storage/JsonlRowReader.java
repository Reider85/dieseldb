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
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
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
 * (prompt 40 requirement). Nested storage follows the configured mode
 * (prompt 45): {@code json_column} captures a nested object/array as compact
 * JSON text with a token-level walk into the mapped column, while
 * {@code flatten} walks nested objects into exact dot-notation leaf columns
 * ({@code user.address.city}) and stores arrays whole (or expanded index
 * columns for scalar arrays under {@code jsonl.array.columns = expand}).
 *
 * <p>Fields map to schema columns by name (case-insensitive); the JSONL
 * format is self-describing, so there is no header line. The three null-ish
 * states of a record (prompt 47) are kept distinct: an explicit JSON
 * {@code null} yields {@code null} with the column flagged present, the JSON
 * string {@code ""} yields an empty string, and a key absent from the object
 * yields {@code null} with the column flagged absent - the presence flags are
 * exposed by {@link #getLastRowPresent()} and honoured on save so a
 * load&rarr;save round trip preserves the distinction. The
 * {@code jsonl.missing.field} policy decides what happens to the absent
 * columns: {@code null}/{@code default} (backward-compatible) leave them
 * {@code null}, {@code error} fails the row. An unknown field is handled per
 * the schema mode (prompt 44): {@code strict} fails with a "did you mean ..."
 * typo hint, hybrid/inferred warn once per file and are expanded at the
 * storage level.
 * Blank and whitespace-only lines are skipped and a UTF-8 BOM on the first
 * record is stripped (prompt 48 covers the full tolerance contract:
 * blank lines, BOM, non-object lines and truncated last lines).
 *
 * <p>Malformed rows are governed by the {@code jsonl.load.error.mode} policy
 * (prompt 48): {@code fail} (default) aborts the load with {@code file:line}
 * / {@code file:line:field} diagnostics (dot-notation JSON path), while
 * {@code skip_row} logs each bad row's coordinates and reason as a WARNING,
 * advances to the next line and reports a single final WARNING with the total
 * skipped-row count ({@link #getSkippedRowCount()}). A JSON object broken by
 * the end of the file (unclosed bracket on the last line) is treated as a
 * possibly truncated record (interrupted append, prompt 49) and is diagnosed
 * as such.
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
    /** Case-insensitive dot/array prefixes that must be descended in FLATTEN mode
     *  when a projection is active (prompt 55): e.g. {@code user} and
     *  {@code user.address} for a projected {@code user.address.city}, or
     *  {@code tags} for projected {@code tags[0]} expand columns. */
    private java.util.Set<String> neededPrefixes;
    /** True while {@link #parseCurrentRow}/{@link #walkObjectFlat} may prune
     *  non-projected subtrees at the token level. Only ever set inside
     *  {@link #nextProjected()} FLATTEN reads; {@link #nextArray()} always parses
     *  the full row. */
    private boolean projectionPushdown;
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
    /** Suppresses the per-reader final skipped-row WARNING (parallel partitions, prompt 54). */
    private boolean suppressSkipSummary;
    /** Rows skipped so far by the {@code jsonl.load.error.mode=skip_row} policy (prompt 48). */
    private long skippedRowCount;
    private boolean skipRowWarningEmitted;
    /** Present-column flags of the last consumed row (prompt 47). */
    private boolean[] lastRowPresent;
    /** FLATTEN-mode container paths (every dot-prefix of a schema column). */
    private final TreeSet<String> containerPrefixes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

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
        buildContainerPrefixes(columns);
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
        buildContainerPrefixes(columns);
    }

    /**
     * Precomputes every dot-prefix of the schema columns for the FLATTEN-mode
     * object walk (prompt 45): {@code user.address.city} registers both
     * {@code user} and {@code user.address} as container paths, so a nested
     * object at that path is descended instead of being skipped or captured.
     */
    private void buildContainerPrefixes(List<String> columns) {
        containerPrefixes.clear();
        for (String column : columns) {
            int dot = column.indexOf('.');
            while (dot > 0) {
                containerPrefixes.add(column.substring(0, dot).toLowerCase(Locale.ROOT));
                dot = column.indexOf('.', dot + 1);
            }
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
        if (values == null) {
            return null;
        }
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
     * column fails with {@code file:line:field} diagnostics. Returns
     * {@code null} instead of throwing when the end of the file was reached
     * while recovering from a skipped row (prompt 48 {@code skip_row} policy).
     */
    public Object[] nextArray() {
        while (true) {
            if (finished) {
                throw new NoSuchElementException("No more rows in JSONL file");
            }
            if (parser == null) {
                prefetch();
            }
            if (parser == null) {
                return null;
            }
            Object[] row = tryParseRow();
            if (row != null) {
                return row;
            }
        }
    }

    /**
     * Attempts to parse the next row from the current parser position.
     * Returns the parsed row on success, or {@code null} when the row was
     * skipped via the skip-row policy (caller should retry).
     */
    private Object[] tryParseRow() {
        JsonStreamParser p = parser;
        lastRowLine = lineNumber;
        Object[] row = new Object[columns.size()];
        boolean[] seen = new boolean[columns.size()];
        try {
            boolean oldPushdown = projectionPushdown;
            projectionPushdown = false;
            try {
                parseCurrentRow(p, row, seen);
            } finally {
                projectionPushdown = oldPushdown;
            }
        } catch (IOException e) {
            closeQuietly(p);
            parser = null;
            String msg = contextPrefix() + "line " + lastRowLine
                    + (atPhysicalEof() ? " (possibly truncated record: JSON ends unexpectedly at end of file)" : "")
                    + ": " + e.getMessage();
            if (skipRow(msg, e)) {
                return null;
            }
            throw new DieselIOException(msg, e);
        } catch (DieselIOException e) {
            closeQuietly(p);
            parser = null;
            if (skipRow(null, e)) {
                return null;
            }
            throw e;
        }
        closeQuietly(p);
        parser = null;
        try {
            enforceMissingFieldPolicy(seen);
        } catch (DieselIOException e) {
            if (skipRow(null, e)) {
                return null;
            }
            throw e;
        }
        lastRowPresent = seen;
        return row;
    }

    /**
     * Consumes one JSON object from the parser into {@code row}, honoring the
     * configured nested-storage mode (prompt 45): {@code json_column} captures
     * every nested object/array as compact JSON text into the mapped column
     * (existing behaviour); {@code flatten} walks nested objects column by
     * column in dot-notation and expands scalar arrays per {@code jsonl.array.columns}.
     */
    private void parseCurrentRow(JsonStreamParser p, Object[] row, boolean[] seen) throws IOException {
        boolean flatten = config.nestedMode() == JsonParserConfig.NestedMode.FLATTEN;
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
            if (processFieldFlatten(p, row, seen, flatten, field, idx, valueToken)) {
                continue;
            }
            processFieldSimple(p, row, seen, field, idx, valueToken);
        }
    }

    /**
     * Handles a field in FLATTEN mode. Returns {@code true} if the field was
     * fully processed (caller should continue to next token), {@code false} if
     * the caller should fall through to simple processing.
     */
    private boolean processFieldFlatten(JsonStreamParser p, Object[] row, boolean[] seen,
                                        boolean flatten, String field, Integer idx,
                                        JsonEvent valueToken) throws IOException {
        if (!flatten) {
            return false;
        }
        if (projectionPushdown && !subtreeNeeded(field, idx)) {
            if (idx == null && !isContainerPath(field)) {
                handleUnknownField(field);
            }
            skipValue(p, valueToken);
            skippedFieldCount++;
            return true;
        }
        if (valueToken == JsonEvent.START_OBJECT && isContainerPath(field)) {
            walkObjectFlat(p, row, seen, field);
            return true;
        }
        if (valueToken == JsonEvent.START_ARRAY) {
            parseArrayFlat(p, row, seen, field, idx);
            return true;
        }
        if (valueToken == JsonEvent.START_OBJECT) {
            if (idx == null) {
                handleUnknownField(field);
                skipValue(p, valueToken);
                skippedFieldCount++;
                return true;
            }
            if (seen[idx]) {
                handleDuplicateField(field);
            }
            parsedFieldCount++;
            String json = captureNested(p);
            schema.validateReadToken(idx, valueToken, json,
                    contextPrefix() + "line " + lastRowLine + ": field '" + field + "': ");
            row[idx] = json;
            seen[idx] = true;
            schema.markNestedJson(idx);
            return true;
        }
        return false;
    }

    /**
     * Handles a field in simple (non-FLATTEN or non-container) mode.
     */
    private void processFieldSimple(JsonStreamParser p, Object[] row, boolean[] seen,
                                    String field, Integer idx, JsonEvent valueToken) throws IOException {
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

    private boolean isContainerPath(String path) {
        return path != null && containerPrefixes.contains(path.toLowerCase(Locale.ROOT));
    }

    /**
     * FLATTEN-mode walk of a nested object (prompt 45). Every leaf maps to its
     * exact dot-notation column ({@code user.address.city}); scalar leaves are
     * validated + converted, array leaves follow {@link #parseArrayFlat}, and
     * an object leaf with no child columns (no dot-prefix in the schema) is
     * captured as compact JSON text and marked as a nested holder.
     */
    private void walkObjectFlat(JsonStreamParser p, Object[] row, boolean[] seen, String path) throws IOException {
        while (p.nextToken() != JsonEvent.END_OBJECT) {
            if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                        + ": malformed JSON record: expected a field name, found " + p.currentEvent(), null);
            }
            String child = p.currentName();
            JsonEvent childValue = p.nextToken();
            if (childValue == null) {
                throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                        + ": malformed JSON record: missing value for field '" + child + "'", null);
            }
            String full = path + "." + child;
            Integer childIdx = indexByName.get(full);
            if (processFlatField(p, row, seen, full, childIdx, childValue)) {
                continue;
            }
            skipValue(p, childValue);
        }
    }

    /**
     * Processes a single field during FLATTEN-mode object walk.
     *
     * @return {@code true} if the field was fully handled (caller should
     *         continue to next token), {@code false} if the value should be skipped
     */
    private boolean processFlatField(JsonStreamParser p, Object[] row, boolean[] seen,
                                     String full, Integer childIdx,
                                     JsonEvent childValue) throws IOException {
        if (projectionPushdown && !subtreeNeeded(full, childIdx)) {
            skipValue(p, childValue);
            skippedFieldCount++;
            return true;
        }
        if (childValue == JsonEvent.START_OBJECT) {
            if (isContainerPath(full)) {
                walkObjectFlat(p, row, seen, full);
                return true;
            }
            if (childIdx != null && !seen[childIdx]) {
                parsedFieldCount++;
                String json = captureNested(p);
                schema.validateReadToken(childIdx, childValue, json,
                        contextPrefix() + "line " + lastRowLine + ": field '" + full + "': ");
                row[childIdx] = json;
                seen[childIdx] = true;
                schema.markNestedJson(childIdx);
                return true;
            }
            return false;
        }
        if (childValue == JsonEvent.START_ARRAY) {
            parseArrayFlat(p, row, seen, full, childIdx);
            return true;
        }
        if (childIdx != null && !seen[childIdx]) {
            parsedFieldCount++;
            if (childValue == JsonEvent.VALUE_NULL) {
                row[childIdx] = null;
            } else {
                row[childIdx] = parseFieldValue(p, childIdx, full, childValue);
            }
            seen[childIdx] = true;
            return true;
        }
        return false;
    }

    /**
     * FLATTEN-mode handling of an array value (prompt 45). The array is always
     * captured as compact JSON text first (advancing the parser). Under
     * {@code jsonl.array.columns = json} (or for an array that contains nested
     * objects) it is stored whole in the exact column and marked as a nested
     * holder; under {@code expand} a scalar array fills its {@code field[0]},
     * {@code field[1]}, ... columns.
     */
    private void parseArrayFlat(JsonStreamParser p, Object[] row, boolean[] seen, String field, Integer idx)
            throws IOException {
        String json = captureNested(p);
        boolean expand = config.arrayColumns() == JsonParserConfig.ArrayColumnsMode.EXPAND;
        if (expand && expandScalarArray(row, seen, json, field) > 0) {
            return;
        }
        if (idx != null && !seen[idx]) {
            parsedFieldCount++;
            schema.validateReadToken(idx, JsonEvent.START_ARRAY, json,
                    contextPrefix() + "line " + lastRowLine + ": field '" + field + "': ");
            row[idx] = json;
            seen[idx] = true;
            schema.markNestedJson(idx);
        }
    }

    /**
     * Expands a captured scalar array into its {@code field[0..N-1]} schema
     * columns (prompt 45, {@code jsonl.array.columns=expand}). Walks the
     * captured JSON directly with real tokens, so string elements are never
     * re-wrapped. Returns the number of populated columns, or 0 when the array
     * contains objects/arrays (falls back to the whole-array JSON column).
     */
    private int expandScalarArray(Object[] row, boolean[] seen, String json, String field) throws IOException {
        try (JsonStreamParser e = JsonStreams.createParser(json, config)) {
            if (e.nextToken() != JsonEvent.START_ARRAY) {
                return 0;
            }
            int populated = 0;
            int element = 0;
            while (e.nextToken() != JsonEvent.END_ARRAY) {
                String column = field + "[" + element + "]";
                Integer columnIndex = indexByName.get(column);
                JsonEvent token = e.currentEvent();
                if (token == JsonEvent.START_OBJECT || token == JsonEvent.START_ARRAY) {
                    return 0;
                }
                element++;
                if (columnIndex == null || seen[columnIndex]) {
                    continue;
                }
                if (token == JsonEvent.VALUE_NULL) {
                    row[columnIndex] = null;
                    seen[columnIndex] = true;
                    parsedFieldCount++;
                    populated++;
                    continue;
                }
                String text = e.getText();
                schema.validateReadToken(columnIndex, token, text,
                        contextPrefix() + "line " + lastRowLine + ": field '" + column + "': ");
                row[columnIndex] = typeMapper.toColumnValue(typeAt(columnIndex), token, text,
                        contextPrefix() + "line " + lastRowLine + ": field '" + column + "': ");
                seen[columnIndex] = true;
                parsedFieldCount++;
                populated++;
            }
            return populated;
        }
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
            neededPrefixes = null;
            return;
        }
        List<JsonlSchemaManager.ProjectionSlot> slots = new ArrayList<>();
        boolean[] needed = new boolean[columns.size()];
        java.util.Set<String> prefixes = new java.util.HashSet<>();
        for (String item : items) {
            JsonlSchemaManager.ProjectionSlot slot = schema.resolveProjectionItem(item);
            if (slot.columnIndex() < 0) {
                warnUnresolvedProjection(item);
                continue;
            }
            needed[slot.columnIndex()] = true;
            addNeededPrefixes(columns.get(slot.columnIndex()), prefixes);
            slots.add(slot);
        }
        projectionSlots = slots.isEmpty() ? null : slots;
        neededByColumn = slots.isEmpty() ? null : needed;
        neededPrefixes = (slots.isEmpty() || prefixes.isEmpty()) ? null : java.util.Set.copyOf(prefixes);
    }

    /**
     * Registers every FLATTEN container that must be kept to reach projected
     * leaves (prompt 55): the dot-prefixes of a projected dotted column
     * ({@code user.address.city} -> {@code user}, {@code user.address}) and the
     * base name of a projected array-expand column ({@code tags[0]} -> {@code tags}).
     */
    private static void addNeededPrefixes(String column, java.util.Set<String> prefixes) {
        String lower = column.toLowerCase(Locale.ROOT);
        int bracket = lower.indexOf('[');
        if (bracket > 0) {
            prefixes.add(lower.substring(0, bracket));
        }
        int dot = lower.indexOf('.');
        while (dot > 0) {
            prefixes.add(lower.substring(0, dot));
            dot = lower.indexOf('.', dot + 1);
        }
    }

    /** Whether a projected FLATTEN read must keep the subtree at {@code path}. */
    private boolean subtreeNeeded(String path, Integer columnIndex) {
        if (columnIndex != null) {
            if (neededByColumn != null && neededByColumn[columnIndex]) {
                return true;
            }
        }
        return neededPrefixes != null && neededPrefixes.contains(path.toLowerCase(Locale.ROOT));
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
        while (true) {
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
                if (config.nestedMode() == JsonParserConfig.NestedMode.FLATTEN) {
                    nextProjectedFlatten(p, values, slots);
                } else {
                    nextProjectedJsonColumn(p, values, slots, seenColumn);
                }
            } catch (IOException e) {
                closeQuietly(p);
                parser = null;
                String msg = contextPrefix() + "line " + lastRowLine
                        + (atPhysicalEof() ? " (possibly truncated record: JSON ends unexpectedly at end of file)" : "")
                        + ": " + e.getMessage();
                if (skipRow(msg, e)) {
                    continue;
                }
                throw new DieselIOException(msg, e);
            } catch (DieselIOException e) {
                closeQuietly(p);
                parser = null;
                if (skipRow(null, e)) {
                    continue;
                }
                throw e;
            }
            closeQuietly(p);
            parser = null;
            return values;
        }
    }

    /**
     * FLATTEN-mode branch of {@link #nextProjected}: parses nested containers
     * to reach leaf columns, then copies requested slots.
     */
    private void nextProjectedFlatten(JsonStreamParser p, Object[] values, int slots) throws IOException {
        Object[] fullRow = new Object[columns.size()];
        boolean[] seenFull = new boolean[columns.size()];
        projectionPushdown = true;
        try {
            parseCurrentRow(p, fullRow, seenFull);
        } finally {
            projectionPushdown = false;
        }
        lastRowPresent = seenFull;
        for (int s = 0; s < slots; s++) {
            JsonlSchemaManager.ProjectionSlot slot = projectionSlots.get(s);
            int idx = slot.columnIndex();
            if (idx >= 0 && seenFull[idx]) {
                values[s] = fullRow[idx];
            }
        }
    }

    /**
     * json_column-mode branch of {@link #nextProjected}: parses fields directly,
     * projecting only requested columns.
     */
    private void nextProjectedJsonColumn(JsonStreamParser p, Object[] values, int slots,
                                        boolean[] seenColumn) throws IOException {
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
            lastRowPresent = seenColumn;
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
            Map<String, Object> row = next();
            if (row != null) {
                result.add(row);
            }
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

    /**
     * Prepares this reader to decode rows from a byte-offset partition of a
     * JSONL file (prompt 54 parallel read). JSONL has no header and no
     * multi-line rows, so only the line numbering and the BOM policy need to
     * be seeded: the next physical line read is reported as
     * {@code firstDataLine}. The UTF-8 BOM is only stripped when the partition
     * starts at the very first physical line of the file ({@code atFileStart}),
     * so a legitimate {@code \uFEFF} character in a later line is never
     * silently removed.
     *
     * @param firstDataLine the 1-based absolute physical line of the
     *                      partition's first line
     * @param atFileStart   whether the partition begins at the file's first
     *                      physical line (BOM handling applies)
     */
    public void initPartition(long firstDataLine, boolean atFileStart) {
        this.lineNumber = firstDataLine - 1;
        this.lastRowLine = 0;
        this.firstLine = atFileStart;
    }

    /**
     * Suppresses the single final skipped-row WARNING emitted at end of file.
     * Used by the parallel loader (prompt 54), which aggregates the skipped
     * counts of all partitions into one summary instead of one warning per
     * partition.
     *
     * @param suppress whether to suppress this reader's final summary warning
     */
    public void setSuppressSkipSummary(boolean suppress) {
        this.suppressSkipSummary = suppress;
    }

    /**
     * Returns the number of rows skipped under the {@code jsonl.load.error.mode=skip_row}
     * policy (prompt 48) since this reader was created. In {@code fail} mode
     * this stays {@code 0}.
     */
    public long getSkippedRowCount() {
        return skippedRowCount;
    }

    /**
     * Returns the present-column flags of the last consumed row (prompt 47):
     * {@code present[i] == true} means the JSON object carried schema column
     * {@code i} (explicitly, possibly as {@code null}); {@code false} means
     * the key (or flatten leaf) was absent from the record. Distinct from an
     * explicit {@code null}: a slot may be {@code null} while its flag is
     * {@code true} (JSON {@code null}) and a slot's value is only trustworthy
     * when the flag is {@code true}.
     */
    public boolean[] getLastRowPresent() {
        return lastRowPresent == null ? null : lastRowPresent.clone();
    }

    /** Returns whether the configured missing-field policy (prompt 47) is {@code error}. */
    public boolean isMissingFieldError() {
        return config.missingField() == JsonParserConfig.MissingFieldMode.ERROR;
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
                emitSkipRowSummary();
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
            if (buildParserForLine(line)) {
                return;
            }
        }
    }

    /**
     * Builds a JSON parser over one physical line and verifies the record
     * starts as a JSON object. Returns {@code true} when the parser is ready
     * for the caller; {@code false} when the line was malformed and, under the
     * {@code jsonl.load.error.mode=skip_row} policy (prompt 48), was logged and
     * skipped so {@link #prefetch()} keeps scanning (in {@code fail} mode the
     * exception propagates instead).
     */
    private boolean buildParserForLine(String line) {
        JsonStreamParser p = null;
        try {
            p = JsonStreams.createParser(line, config);
            if (p.nextToken() != JsonEvent.START_OBJECT) {
                closeQuietly(p);
                throw new DieselIOException(contextPrefix() + "line " + lineNumber
                        + ": JSON record must be a single JSON object, found " + p.currentEvent(), null);
            }
            parser = p;
            return true;
        } catch (IOException e) {
            closeQuietly(p);
            String msg = contextPrefix() + "line " + lineNumber
                    + (atPhysicalEof() ? " (possibly truncated record: JSON ends unexpectedly at end of file)" : "")
                    + ": " + e.getMessage();
            if (skipRow(msg, e)) {
                return false;
            }
            throw new DieselIOException(msg, e);
        } catch (DieselIOException e) {
            closeQuietly(p);
            if (skipRow(null, e)) {
                return false;
            }
            throw e;
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
        Class<?> type = null;
        if (columnTypes != null) {
            type = columnTypes.get(columns.get(columnIndex));
        }
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

    /**
     * Applies the missing-field policy (prompt 47) after a full row has been
     * parsed. In {@code error} mode a schema column that the record did not
     * carry at all (regardless of an explicit JSON {@code null}) fails the row
     * with {@code file:line} context listing the missing columns. The
     * {@code null}/{@code default} modes leave absent slots as {@code null}.
     */
    private void enforceMissingFieldPolicy(boolean[] seen) {
        if (config.missingField() != JsonParserConfig.MissingFieldMode.ERROR) {
            return;
        }
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (!seen[i]) {
                missing.add("'" + columns.get(i) + "'");
            }
        }
        if (!missing.isEmpty()) {
            throw new DieselIOException(contextPrefix() + "line " + lastRowLine
                    + ": record is missing field(s) " + String.join(", ", missing)
                    + " (jsonl.missing.field=error, prompt 47)", null);
        }
    }

    private String contextPrefix() {
        return fileName != null ? fileName + ":" : "";
    }

    /**
     * Applies the {@code jsonl.load.error.mode} policy (prompt 48) to a
     * malformed row. In {@code fail} mode the error is re-thrown unchanged; in
     * {@code skip_row} mode the coordinates and reason are logged as a WARNING,
     * the row is counted as skipped and the load continues ({@code true}).
     * {@code message} may be {@code null} to reuse the exception's own message.
     */
    private boolean skipRow(String message, Exception e) {
        if (config.loadErrorMode() != JsonParserConfig.LoadErrorMode.SKIP_ROW) {
            throw new DieselIOException(message != null ? message : e.getMessage(), e);
        }
        skippedRowCount++;
        LOGGER.warn("{} skipping malformed JSONL row: {}",
                contextPrefix(),
                message != null ? message : e.getMessage());
        return true;
    }

    /**
     * Cheap probe whether the underlying reader is at the physical end of the
     * file (no more lines). Used to diagnose an unclosed JSON record on the
     * last line as a possibly truncated record (interrupted append, prompt 48
     * gluing onto prompt 49). A failed probe conservatively reports
     * {@code false} so only the true end-of-file case is annotated.
     */
    private boolean atPhysicalEof() {
        try {
            reader.mark(1);
            boolean eof = reader.readLine() == null;
            reader.reset();
            return eof;
        } catch (IOException e) {
            return false;
        }
    }

    /** Emits the single final WARNING with the total skipped-row count (prompt 48). */
    private void emitSkipRowSummary() {
        if (skippedRowCount > 0 && !skipRowWarningEmitted && !suppressSkipSummary) {
            skipRowWarningEmitted = true;
            LOGGER.warn("{} JSONL load skipped {} malformed line(s) (jsonl.load.error.mode=skip_row)",
                    contextPrefix(), skippedRowCount);
        }
    }

    private static void closeQuietly(JsonStreamParser p) {
        if (p == null) {
            return;
        }
        try {
            p.close();
        } catch (IOException ignored) {
            // Best effort.
        }
    }
}