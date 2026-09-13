package diesel.storage.json;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Single point of JSON-library selection (prompt 42): the only class in the
 * codebase that imports a JSON library is this factory (via
 * {@link JsonParserConfig#Backend}). The storage package obtains parsers and
 * generators exclusively through static methods here.
 */
public final class JsonStreams {

    private JsonStreams() {
    }

    /** Opens a streaming parser over {@code text} with the given configuration. */
    public static JsonStreamParser createParser(String text, JsonParserConfig config) throws IOException {
        return switch (config.backend()) {
            case JACKSON -> JacksonStreamParser.open(text, config);
            case GSON -> GsonStreamParser.open(text, config);
        };
    }

    /** Opens a streaming parser over {@code reader} with the given configuration. */
    public static JsonStreamParser createParser(Reader reader, JsonParserConfig config) throws IOException {
        return switch (config.backend()) {
            case JACKSON -> JacksonStreamParser.open(reader, config);
            case GSON -> GsonStreamParser.open(reader, config);
        };
    }

    /** Opens a streaming generator over {@code writer} with the given configuration. */
    public static JsonStreamGenerator createGenerator(Writer writer, JsonParserConfig config) throws IOException {
        return switch (config.backend()) {
            case JACKSON -> JacksonStreamGenerator.open(writer, config);
            case GSON -> GsonStreamGenerator.open(writer, config);
        };
    }
}