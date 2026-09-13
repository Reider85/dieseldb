package diesel.storage.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;

/**
 * Gson streaming backend (prompt 42): wraps {@link JsonReader} behind
 * {@link JsonStreamParser}. Gson runs in strict mode (lenient disabled) so
 * comments, NaN/Infinity and other non-standard JSON shapes fail; nesting
 * depth and string length are enforced by this class because Gson itself has
 * no such limits. Gson-only behaviour is confined to this class.
 */
final class GsonStreamParser implements JsonStreamParser {

    private final JsonReader reader;
    private final JsonParserConfig config;
    private JsonEvent current;
    private String currentName;
    private String textValue;
    private int depth;

    private GsonStreamParser(JsonReader reader, JsonParserConfig config) {
        this.reader = reader;
        this.config = config;
    }

    static JsonStreamParser open(String text, JsonParserConfig config) throws IOException {
        return new GsonStreamParser(new JsonReader(new StringReader(text)), config);
    }

    static JsonStreamParser open(Reader reader, JsonParserConfig config) throws IOException {
        return new GsonStreamParser(new JsonReader(reader), config);
    }

    @Override
    public JsonEvent nextToken() throws IOException {
        if (current == JsonEvent.END_INPUT) {
            return current;
        }
        try {
            JsonToken t = reader.peek();
            if (t == JsonToken.END_DOCUMENT) {
                current = JsonEvent.END_INPUT;
                return current;
            }
            switch (t) {
                case BEGIN_OBJECT -> {
                    reader.beginObject();
                    depth++;
                    checkDepth();
                    current = JsonEvent.START_OBJECT;
                }
                case END_OBJECT -> {
                    reader.endObject();
                    depth--;
                    current = JsonEvent.END_OBJECT;
                }
                case BEGIN_ARRAY -> {
                    reader.beginArray();
                    depth++;
                    checkDepth();
                    current = JsonEvent.START_ARRAY;
                }
                case END_ARRAY -> {
                    reader.endArray();
                    depth--;
                    current = JsonEvent.END_ARRAY;
                }
                case NAME -> {
                    currentName = reader.nextName();
                    checkStringLength(currentName);
                    current = JsonEvent.FIELD_NAME;
                }
                case STRING -> {
                    textValue = reader.nextString();
                    checkStringLength(textValue);
                    current = JsonEvent.VALUE_STRING;
                }
                case NUMBER -> {
                    String literal = reader.nextString();
                    current = isFloatLiteral(literal) ? JsonEvent.VALUE_NUMBER_FLOAT : JsonEvent.VALUE_NUMBER_INT;
                    textValue = literal;
                }
                case BOOLEAN -> {
                    boolean value = reader.nextBoolean();
                    textValue = Boolean.toString(value);
                    current = value ? JsonEvent.VALUE_TRUE : JsonEvent.VALUE_FALSE;
                }
                case NULL -> {
                    reader.nextNull();
                    textValue = "null";
                    current = JsonEvent.VALUE_NULL;
                }
                default -> throw new JsonStreamException("Unsupported Gson token " + t);
            }
            return current;
        } catch (JsonStreamException e) {
            throw e;
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        } catch (IllegalStateException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }

    private void checkDepth() throws JsonStreamException {
        if (depth > config.maxNestingDepth()) {
            throw new JsonStreamException("JSON nesting depth exceeds the limit of "
                    + config.maxNestingDepth() + " (jsonl.max.nesting.depth)");
        }
    }

    private void checkStringLength(String value) throws JsonStreamException {
        if (value != null && value.length() > config.maxStringLength()) {
            throw new JsonStreamException("JSON string exceeds the maximum length of "
                    + config.maxStringLength() + " characters (jsonl.max.string.length)");
        }
    }

    private static boolean isFloatLiteral(String literal) {
        for (int i = 0; i < literal.length(); i++) {
            char c = literal.charAt(i);
            if (c == '.' || c == 'e' || c == 'E') {
                return true;
            }
        }
        return false;
    }

    @Override
    public JsonEvent currentEvent() {
        return current;
    }

    @Override
    public String currentName() {
        return current == JsonEvent.FIELD_NAME ? currentName : null;
    }

    @Override
    public String getText() {
        if (textValue != null) {
            return textValue;
        }
        return current == JsonEvent.FIELD_NAME ? currentName : null;
    }

    @Override
    public long getLongValue() throws IOException {
        if (current != JsonEvent.VALUE_NUMBER_INT && current != JsonEvent.VALUE_NUMBER_FLOAT) {
            throw new JsonStreamException("Current token " + current + " is not a number");
        }
        try {
            return Long.parseLong(textValue);
        } catch (NumberFormatException e) {
            throw new JsonStreamException("Cannot parse number '" + textValue + "': " + e.getMessage(), e);
        }
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        if (current != JsonEvent.VALUE_NUMBER_INT && current != JsonEvent.VALUE_NUMBER_FLOAT) {
            throw new JsonStreamException("Current token " + current + " is not a number");
        }
        return new BigDecimal(textValue);
    }

    @Override
    public void skipChildren() throws IOException {
        if (current != JsonEvent.START_OBJECT && current != JsonEvent.START_ARRAY) {
            return;
        }
        int remaining = 1;
        while (remaining > 0) {
            JsonEvent e = nextToken();
            if (e == JsonEvent.END_INPUT) {
                break;
            }
            switch (e) {
                case START_OBJECT, START_ARRAY -> remaining++;
                case END_OBJECT, END_ARRAY -> remaining--;
                default -> {
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            reader.close();
        } catch (IOException e) {
            throw new JsonStreamException(e.getMessage(), e);
        }
    }
}