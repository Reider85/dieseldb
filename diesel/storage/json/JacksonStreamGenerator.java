package diesel.storage.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;

/**
 * Jackson streaming writer backend (prompt 42): wraps {@link JsonGenerator}
 * behind {@link JsonStreamGenerator}. Output is byte-identical to the previous
 * direct Jackson usage (compact, no whitespace): when the source is another
 * {@link JacksonStreamParser}, {@link #copyCurrentStructure} delegates to
 * Jackson's native deep copy so nested values round-trip verbatim.
 */
final class JacksonStreamGenerator implements JsonStreamGenerator {

    private final JsonGenerator generator;

    private JacksonStreamGenerator(JsonGenerator generator) {
        this.generator = generator;
    }

    static JsonStreamGenerator open(Writer writer, JsonParserConfig config) throws IOException {
        JsonGenerator generator = JsonStreamsFactory.jsonFactory(config).createGenerator(writer);
        generator.setRootValueSeparator(new SerializedString(""));
        return new JacksonStreamGenerator(generator);
    }

    @Override
    public void writeStartObject() throws IOException {
        generator.writeStartObject();
    }

    @Override
    public void writeEndObject() throws IOException {
        generator.writeEndObject();
    }

    @Override
    public void writeStartArray() throws IOException {
        generator.writeStartArray();
    }

    @Override
    public void writeEndArray() throws IOException {
        generator.writeEndArray();
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
    }

    @Override
    public void writeString(String value) throws IOException {
        generator.writeString(value);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        generator.writeBoolean(value);
    }

    @Override
    public void writeNull() throws IOException {
        generator.writeNull();
    }

    @Override
    public void writeNumber(int value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(long value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(float value) throws IOException {
        requireFinite(value);
        generator.writeNumber(value);
    }

    @Override
    public void writeNumber(double value) throws IOException {
        requireFinite(value);
        generator.writeNumber(value);
    }

    private static void requireFinite(double value) throws JsonStreamException {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new JsonStreamException("Non-finite number " + value + " is not valid JSON");
        }
    }

    @Override
    public void writeNumber(BigDecimal value) throws IOException {
        generator.writeNumber(value);
    }

    @Override
    public void writeRaw(char c) throws IOException {
        generator.writeRaw(c);
    }

    @Override
    public void copyCurrentStructure(JsonStreamParser source) throws IOException {
        if (source instanceof JacksonStreamParser jackson) {
            generator.copyCurrentStructure(jackson.jackson());
        } else {
            JsonCopy.copyCurrentStructure(source, this);
        }
    }

    @Override
    public void close() throws IOException {
        generator.close();
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }
}