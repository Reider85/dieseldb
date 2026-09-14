package diesel.storage;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import diesel.DieselIOException;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;

/**
 * Writes rows to a JSON Lines (NDJSON) stream: one table row = one JSON object
 * per line, {@code \n}-separated, UTF-8 (the caller supplies the character
 * stream). Fields are written in schema column order so the file layout is
 * deterministic. Null semantics (prompt 47): an explicit {@code null} is
 * written as JSON {@code null}, the empty string as {@code ""}, and an absent
 * field (flagged not-present in the per-row presence mask) has its key omitted
 * from the object entirely - so the three states stay distinct after a
 * load&rarr;save round trip.
 *
 * <p>JSON validity is enforced at write time (prompt 40): values are
 * serialised through the streaming JSON abstraction
 * ({@code diesel.storage.json}, prompt 42) so quoting/escaping is always
 * correct, and non-finite floats ({@code NaN}/{@code Infinity}), which have no
 * JSON representation, are rejected instead of being emitted as invalid
 * tokens.
 *
 * <p>Nested storage rules (prompt 45): in the {@code json_column} mode a
 * column marked as a nested-JSON holder (captured from a nested object/array
 * on load) is written back as the actual JSON structure, not as an escaped
 * string, so a load&rarr;save round trip preserves nesting. In the
 * {@code flatten} mode dot-notation leaf columns ({@code user.address.city})
 * are reconstructed into nested objects, scalar-array index columns
 * ({@code tags[0]}, {@code tags[1]}) into arrays, and nested-JSON holders are
 * embedded as structure - all in deterministic schema-column order.
 *
 * <p>Write-side type validation (prompt 41): when the writer is constructed
 * with schema types, every value is checked against its column type before
 * serialisation. Shapes that would corrupt the read-back contract - a nested
 * object/array into a typed (non-String) column, a floating-point value into
 * an integer column, a date/UUID value into a differently-typed column - fail
 * with {@code record N: field 'X'} diagnostics. String-to-typed coercion
 * stays lenient (strict rules arrive with JsonTypeMapper, prompt 43).
 */
public class JsonlRowWriter implements AutoCloseable {

    private final JsonStreamGenerator generator;
    private final List<String> columns;
    private final JsonlSchemaManager schema;
    private final boolean flatten;
    private long recordNumber;

    /**
     * @param writer  the underlying character-output stream
     * @param columns the ordered column names (field order of each record)
     */
    public JsonlRowWriter(Writer writer, List<String> columns) throws IOException {
        this(writer, new JsonlSchemaManager(columns, null));
    }

    /**
     * @param writer      the underlying character-output stream
     * @param columns     the ordered column names (field order of each record)
     * @param columnTypes the column name to expected Java type, enabling
     *                    write-side type validation against the schema
     */
    public JsonlRowWriter(Writer writer, List<String> columns, Map<String, Class<?>> columnTypes) throws IOException {
        this(writer, new JsonlSchemaManager(columns, columnTypes));
    }

    /**
     * @param writer      the underlying character-output stream
     * @param columns     the ordered column names (field order of each record)
     * @param columnTypes the column name to expected Java type, enabling
     *                    write-side type validation against the schema
     * @param config      the streaming JSON configuration (backend, limits)
     */
    public JsonlRowWriter(Writer writer, List<String> columns, Map<String, Class<?>> columnTypes,
                          JsonParserConfig config) throws IOException {
        this(writer, new JsonlSchemaManager(columns, columnTypes, config), config);
    }

    /**
     * @param writer the underlying character-output stream
     * @param schema the shared schema manager (type validation on write)
     */
    public JsonlRowWriter(Writer writer, JsonlSchemaManager schema) throws IOException {
        this(writer, schema, JsonParserConfig.defaults());
    }

    /**
     * @param writer the underlying character-output stream
     * @param schema the shared schema manager (type validation on write)
     * @param config the streaming JSON configuration (backend, limits)
     */
    public JsonlRowWriter(Writer writer, JsonlSchemaManager schema, JsonParserConfig config) throws IOException {
        this.columns = schema.columns();
        this.schema = schema;
        this.flatten = schema.nestedMode() == JsonParserConfig.NestedMode.FLATTEN;
        this.generator = JsonStreams.createGenerator(writer, config);
    }

    /**
     * Writes a single row given as a column-to-value map. Unknown keys are
     * dropped; missing columns are written as JSON {@code null}.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        recordNumber++;
        if (flatten) {
            Map<String, Object> nested = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                Object value = row == null ? null : row.get(column);
                schema.validateWriteValue(i, value, column, recordContext());
                insertNested(nested, column, unwrapNested(i, value));
            }
            writeObjectMap(nested);
            generator.writeRaw('\n');
            return;
        }
        generator.writeStartObject();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = row == null ? null : row.get(column);
            schema.validateWriteValue(i, value, column, recordContext());
            generator.writeFieldName(column);
            writeValue(unwrapNested(i, value));
        }
        generator.writeEndObject();
        generator.writeRaw('\n');
    }

    /**
     * Writes a single row given as a compact Object[] array, where slot
     * {@code i} holds the value of schema column {@code i} (prompt 36). Avoids
     * materialising a per-row Map at save time.
     *
     * @param row the ordered values aligned with the columns
     */
    public void writeRow(Object[] row) throws IOException {
        writeRow(row, null);
    }

    /**
     * Writes a single Object[] row, honouring the per-column present flags
     * (prompt 47): a column with {@code present[i] == false} has its key
     * omitted from the JSON object, so an absent field stays distinct from an
     * explicit {@code null} after a load&rarr;save round trip. Passing
     * {@code null} (or an array of the wrong length) treats every column as
     * present, which preserves the pre-prompt-47 behaviour.
     *
     * @param row     the ordered values aligned with the columns
     * @param present per-column presence flags, or {@code null} for all-present
     */
    public void writeRow(Object[] row, boolean[] present) throws IOException {
        recordNumber++;
        if (flatten) {
            Map<String, Object> nested = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                if (!isPresent(present, i)) {
                    continue;
                }
                Object value = row == null || i >= row.length ? null : row[i];
                schema.validateWriteValue(i, value, columns.get(i), recordContext());
                insertNested(nested, columns.get(i), unwrapNested(i, value));
            }
            writeObjectMap(nested);
            generator.writeRaw('\n');
            return;
        }
        generator.writeStartObject();
        for (int i = 0; i < columns.size(); i++) {
            if (!isPresent(present, i)) {
                continue;
            }
            Object value = row == null || i >= row.length ? null : row[i];
            schema.validateWriteValue(i, value, columns.get(i), recordContext());
            generator.writeFieldName(columns.get(i));
            writeValue(unwrapNested(i, value));
        }
        generator.writeEndObject();
        generator.writeRaw('\n');
    }

    private static boolean isPresent(boolean[] present, int i) {
        return present == null || i >= present.length || present[i];
    }

    private String recordContext() {
        return "record " + recordNumber + ": ";
    }

    /**
     * FLATTEN-mode value unwrapping (prompt 45): a nested-JSON holder column
     * holds compact JSON text captured on load; before writing, that text is
     * parsed back into the structure it was captured from so the JSON file
     * stays truly nested (no double-encoded strings).
     */
    private Object unwrapNested(int columnIndex, Object value) {
        if (schema.isNestedJson(columnIndex) && value instanceof String text) {
            return parseJsonText(text);
        }
        return value;
    }

    /**
     * Parses captured JSON text into nested Map/List/scalar values, falling
     * back to the original text when it is not valid JSON (defensive - the
     * column is only marked after a successful capture).
     */
    private Object parseJsonText(String text) {
        try (JsonStreamParser p = JsonStreams.createParser(text, schema.jsonConfig())) {
            return parseJsonValue(p.nextToken(), p);
        } catch (IOException e) {
            return text;
        }
    }

    private Object parseJsonValue(JsonEvent token, JsonStreamParser p) throws IOException {
        return switch (token) {
            case VALUE_NULL -> null;
            case VALUE_STRING -> p.getText();
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_NUMBER_INT -> p.getLongValue();
            case VALUE_NUMBER_FLOAT -> p.getDecimalValue();
            case START_OBJECT -> {
                Map<String, Object> map = new LinkedHashMap<>();
                while (p.nextToken() != JsonEvent.END_OBJECT) {
                    String name = p.currentName();
                    map.put(name, parseJsonValue(p.nextToken(), p));
                }
                yield map;
            }
            case START_ARRAY -> {
                List<Object> list = new ArrayList<>();
                while (p.nextToken() != JsonEvent.END_ARRAY) {
                    list.add(parseJsonValue(p.currentEvent(), p));
                }
                yield list;
            }
            default -> throw new DieselIOException("unexpected token " + token + " in nested JSON text", null);
        };
    }

    /**
     * FLATTEN-mode reconstruction: inserts a column value into the nested row
     * structure. Dot-notation columns ({@code user.address.city}) create nested
     * objects; {@code index} tokens ({@code user.tags[0]}) create/extend arrays.
     * Deterministic because columns are processed in schema order.
     */
    private static void insertNested(Map<String, Object> root, String column, Object value) {
        insert(root, tokenizePath(column), 0, value);
    }

    private static void insert(Object container, List<PathToken> tokens, int depth, Object value) {
        PathToken token = tokens.get(depth);
        if (depth == tokens.size() - 1) {
            if (token.index >= 0) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) container;
                ensureSize(list, token.index + 1);
                list.set(token.index, value);
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) container;
                map.put(token.name, value);
            }
            return;
        }
        PathToken next = tokens.get(depth + 1);
        Object child;
        if (token.index >= 0) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) container;
            ensureSize(list, token.index + 1);
            child = list.get(token.index);
            if (child == null) {
                child = next.index >= 0 ? new ArrayList<>() : new LinkedHashMap<>();
                list.set(token.index, child);
            }
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) container;
            child = map.get(token.name);
            if (child == null) {
                child = next.index >= 0 ? new ArrayList<>() : new LinkedHashMap<>();
                map.put(token.name, child);
            }
        }
        insert(child, tokens, depth + 1, value);
    }

    private static void ensureSize(List<Object> list, int size) {
        while (list.size() < size) {
            list.add(null);
        }
    }

    /** A token of a column name path: a map key ({@code index < 0}) or an array element. */
    private record PathToken(String name, int index) {
        PathToken(String name, int index) {
            this.name = name;
            this.index = index;
        }
    }

    private static List<PathToken> tokenizePath(String column) {
        List<PathToken> tokens = new ArrayList<>();
        for (String segment : column.split("\\.")) {
            int open = segment.lastIndexOf('[');
            if (open > 0 && segment.endsWith("]")) {
                String base = segment.substring(0, open);
                String index = segment.substring(open + 1, segment.length() - 1);
                tokens.add(new PathToken(base, -1));
                if (index.matches("\\d+")) {
                    tokens.add(new PathToken(index, Integer.parseInt(index)));
                } else {
                    tokens.add(new PathToken(segment, -1));
                }
            } else {
                tokens.add(new PathToken(segment, -1));
            }
        }
        return tokens;
    }

    /**
     * Serialises a single column value. Scalars map to the natural JSON type
     * (dates, UUIDs and {@link BigDecimal} keep exact textual representations);
     * {@link Map}/{@link List}/array values are written as nested JSON
     * structures; any other type is written as its string form.
     */
    private void writeValue(Object value) throws IOException {
        if (value == null) {
            generator.writeNull();
        } else if (value instanceof String str) {
            generator.writeString(str);
        } else if (value instanceof Boolean b) {
            generator.writeBoolean(b);
        } else if (value instanceof Integer i) {
            generator.writeNumber(i);
        } else if (value instanceof Long l) {
            generator.writeNumber(l);
        } else if (value instanceof Short s) {
            generator.writeNumber(s);
        } else if (value instanceof Byte b) {
            generator.writeNumber(b);
        } else if (value instanceof BigDecimal bd) {
            generator.writeNumber(bd);
        } else if (value instanceof Float f) {
            requireFinite(f.doubleValue());
            generator.writeNumber(f);
        } else if (value instanceof Double d) {
            requireFinite(d);
            generator.writeNumber(d);
        } else if (value instanceof Character c) {
            generator.writeString(c.toString());
        } else if (value instanceof LocalDate ld) {
            generator.writeString(ld.toString());
        } else if (value instanceof LocalDateTime ldt) {
            generator.writeString(ldt.toString());
        } else if (value instanceof UUID uuid) {
            generator.writeString(uuid.toString());
        } else if (value instanceof Map<?, ?> map) {
            writeObjectMap(map);
        } else if (value instanceof List<?> list) {
            writeArray(list.toArray());
        } else if (value.getClass().isArray()) {
            writeArray(toObjectArray(value));
        } else {
            generator.writeString(value.toString());
        }
    }

    private void writeObjectMap(Map<?, ?> map) throws IOException {
        generator.writeStartObject();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            generator.writeFieldName(String.valueOf(entry.getKey()));
            writeValue(entry.getValue());
        }
        generator.writeEndObject();
    }

    private void writeArray(Object[] items) throws IOException {
        generator.writeStartArray();
        for (Object item : items) {
            writeValue(item);
        }
        generator.writeEndArray();
    }

    private static Object[] toObjectArray(Object array) {
        int length = java.lang.reflect.Array.getLength(array);
        Object[] items = new Object[length];
        for (int i = 0; i < length; i++) {
            items[i] = java.lang.reflect.Array.get(array, i);
        }
        return items;
    }

    private static void requireFinite(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new DieselIOException("Cannot write value " + value
                    + ": JSON does not support NaN/Infinity", null);
        }
    }

    /** Flushes buffered output. */
    public void flush() throws IOException {
        generator.flush();
    }

    /** Closes the underlying writer. */
    @Override
    public void close() throws IOException {
        generator.close();
    }
}