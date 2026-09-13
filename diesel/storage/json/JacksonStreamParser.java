package diesel.storage.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;

/**
 * Jackson streaming backend (prompt 42): wraps {@link JsonParser} behind
 * {@link JsonStreamParser}. Lenient Jackson features (comments, non-numeric
 * numbers) stay disabled by default; the {@link JsonParserConfig} limits are
 * enforced through {@link StreamReadConstraints}. Jackson-only behaviour is
 * confined to this class.
 */
final class JacksonStreamParser implements JsonStreamParser {

    private final JsonParser parser;
    private JsonEvent current;

    private JacksonStreamParser(JsonParser parser) {
        this.parser = parser;
    }

    static JsonStreamParser open(String text, JsonParserConfig config) throws IOException {
        return new JacksonStreamParser(JsonStreamsFactory.jsonFactory(config).createParser(text));
    }

    static JsonStreamParser open(Reader reader, JsonParserConfig config) throws IOException {
        return new JacksonStreamParser(JsonStreamsFactory.jsonFactory(config).createParser(reader));
    }

    JsonParser jackson() {
        return parser;
    }

    @Override
    public JsonEvent nextToken() throws IOException {
        try {
            JsonToken t = parser.nextToken();
            if (t == null) {
                current = JsonEvent.END_INPUT;
                return current;
            }
            current = switch (t) {
                case START_OBJECT -> JsonEvent.START_OBJECT;
                case END_OBJECT -> JsonEvent.END_OBJECT;
                case START_ARRAY -> JsonEvent.START_ARRAY;
                case END_ARRAY -> JsonEvent.END_ARRAY;
                case FIELD_NAME -> JsonEvent.FIELD_NAME;
                case VALUE_STRING -> JsonEvent.VALUE_STRING;
                case VALUE_NUMBER_INT -> JsonEvent.VALUE_NUMBER_INT;
                case VALUE_NUMBER_FLOAT -> JsonEvent.VALUE_NUMBER_FLOAT;
                case VALUE_TRUE -> JsonEvent.VALUE_TRUE;
                case VALUE_FALSE -> JsonEvent.VALUE_FALSE;
                case VALUE_NULL -> JsonEvent.VALUE_NULL;
                default -> JsonEvent.VALUE_STRING;
            };
            return current;
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    @Override
    public JsonEvent currentEvent() {
        return current;
    }

    @Override
    public String currentName() {
        if (current != JsonEvent.FIELD_NAME) {
            return null;
        }
        try {
            return parser.currentName();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String getText() throws IOException {
        try {
            String text = parser.getText();
            if ((current == JsonEvent.VALUE_NUMBER_FLOAT || current == JsonEvent.VALUE_NUMBER_INT)
                    && isNonFinite(text)) {
                throw new JsonStreamException("Non-finite number '" + text + "' is not valid JSON");
            }
            return text;
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    @Override
    public long getLongValue() throws IOException {
        try {
            return parser.getLongValue();
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        try {
            String literal = parser.getText();
            return new BigDecimal(literal);
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    @Override
    public void skipChildren() throws IOException {
        try {
            parser.skipChildren();
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            parser.close();
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    private static boolean isNonFinite(String text) {
        return "NaN".equals(text) || "Infinity".equals(text) || "-Infinity".equals(text);
    }
}