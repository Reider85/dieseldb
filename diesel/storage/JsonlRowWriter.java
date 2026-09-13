package diesel.storage;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import diesel.DieselIOException;

/**
 * Writes rows to a JSON Lines (NDJSON) stream: one table row = one JSON object
 * per line, {@code \n}-separated, UTF-8 (the caller supplies the character
 * stream). Fields are written in schema column order so the file layout is
 * deterministic. Nulls are written as JSON {@code null}.
 *
 * <p>JSON validity is enforced at write time (prompt 40): values are
 * serialised through Jackson's streaming API so quoting/escaping is always
 * correct, and non-finite floats ({@code NaN}/{@code Infinity}), which have no
 * JSON representation, are rejected instead of being emitted as the invalid
 * tokens Jackson would otherwise produce.
 *
 * <p>Nested structures (Map/List/array values) are written as nested JSON
 * objects/arrays (base nesting support, prompt 40; the storage stores such
 * values as compact JSON text in a column - full flatten/json_column rules
 * land in prompt 45).
 */
public class JsonlRowWriter implements AutoCloseable {

    private static final JsonFactory JSON = new JsonFactory();

    private final JsonGenerator generator;
    private final List<String> columns;

    /**
     * @param writer  the underlying character-output stream
     * @param columns the ordered column names (field order of each record)
     */
    public JsonlRowWriter(Writer writer, List<String> columns) throws IOException {
        this.columns = columns;
        this.generator = JSON.createGenerator(writer);
    }

    /**
     * Writes a single row given as a column-to-value map. Unknown keys are
     * dropped; missing columns are written as JSON {@code null}.
     *
     * @param row the column-to-value map
     */
    public void writeRow(Map<String, Object> row) throws IOException {
        generator.writeStartObject();
        for (String column : columns) {
            generator.writeFieldName(column);
            writeValue(row == null ? null : row.get(column));
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
        generator.writeStartObject();
        for (int i = 0; i < columns.size(); i++) {
            generator.writeFieldName(columns.get(i));
            writeValue(row == null || i >= row.length ? null : row[i]);
        }
        generator.writeEndObject();
        generator.writeRaw('\n');
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