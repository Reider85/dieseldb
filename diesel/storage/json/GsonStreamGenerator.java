package diesel.storage.json;

import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;

/**
 * Gson streaming writer backend (prompt 42): wraps {@link JsonWriter} behind
 * {@link JsonStreamGenerator}. HTML-safe escaping is disabled so output matches
 * the Jackson default (only quotes/backslashes/control characters escaped).
 * Numbers are written through the {@link Number} overload so preserved scale
 * (e.g. {@code 100.50}) and single-precision shortest forms round-trip, and
 * non-finite floats are rejected by Gson's strict writer itself.
 */
final class GsonStreamGenerator implements JsonStreamGenerator {

    private final JsonWriter writer;
    private final Writer raw;

    private GsonStreamGenerator(JsonWriter writer, Writer raw) {
        this.writer = writer;
        this.raw = raw;
    }

    static JsonStreamGenerator open(Writer writer, JsonParserConfig config) throws IOException {
        JsonWriter gson = new JsonWriter(writer);
        gson.setHtmlSafe(false);
        gson.setSerializeNulls(true);
        gson.setLenient(true);
        return new GsonStreamGenerator(gson, writer);
    }

    @Override
    public void writeStartObject() throws IOException {
        writer.beginObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        writer.endObject();
    }

    @Override
    public void writeStartArray() throws IOException {
        writer.beginArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        writer.endArray();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        writer.name(name);
    }

    @Override
    public void writeString(String value) throws IOException {
        writer.value(value);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        writer.value(value);
    }

    @Override
    public void writeNull() throws IOException {
        writer.nullValue();
    }

    @Override
    public void writeNumber(int value) throws IOException {
        writer.value(value);
    }

    @Override
    public void writeNumber(long value) throws IOException {
        writer.value(value);
    }

    @Override
    public void writeNumber(float value) throws IOException {
        requireFinite(value);
        writer.value((Number) value);
    }

    @Override
    public void writeNumber(double value) throws IOException {
        requireFinite(value);
        writer.value(value);
    }

    private static void requireFinite(double value) throws JsonStreamException {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new JsonStreamException("Non-finite number " + value + " is not valid JSON");
        }
    }

    @Override
    public void writeNumber(BigDecimal value) throws IOException {
        writer.value((Number) value);
    }

    @Override
    public void writeRaw(char c) throws IOException {
        writer.flush();
        raw.write(c);
    }

    @Override
    public void copyCurrentStructure(JsonStreamParser source) throws IOException {
        JsonCopy.copyCurrentStructure(source, this);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}