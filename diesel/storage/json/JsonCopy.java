package diesel.storage.json;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Library-neutral event-walk copier (prompt 42). Used to copy a current JSON
 * value from any {@link JsonStreamParser} into any {@link JsonStreamGenerator}.
 * Backends may bypass this helper when copying from a parser of their own kind
 * (e.g. Jackson uses its native deep-copy for byte-identical output).
 */
final class JsonCopy {

    private JsonCopy() {
    }

    static void copyCurrentStructure(JsonStreamParser source, JsonStreamGenerator target) throws IOException {
        JsonEvent event = source.currentEvent() != null ? source.currentEvent() : source.nextToken();
        if (event == null) {
            return;
        }
        int depth = 0;
        String pendingFieldName = null;
        while (event != null) {
            switch (event) {
                case START_OBJECT -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeStartObject();
                    depth++;
                }
                case START_ARRAY -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeStartArray();
                    depth++;
                }
                case END_OBJECT -> {
                    target.writeEndObject();
                    depth--;
                }
                case END_ARRAY -> {
                    target.writeEndArray();
                    depth--;
                }
                case FIELD_NAME -> pendingFieldName = source.currentName();
                case VALUE_STRING -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeString(source.getText());
                }
                case VALUE_NUMBER_INT -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeNumber(source.getLongValue());
                }
                case VALUE_NUMBER_FLOAT -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    BigDecimal number = source.getDecimalValue();
                    double asDouble = number.doubleValue();
                    if (Double.isFinite(asDouble)) {
                        target.writeNumber(asDouble);
                    } else {
                        target.writeNumber(number);
                    }
                }
                case VALUE_TRUE -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeBoolean(true);
                }
                case VALUE_FALSE -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeBoolean(false);
                }
                case VALUE_NULL -> {
                    if (pendingFieldName != null) {
                        target.writeFieldName(pendingFieldName);
                        pendingFieldName = null;
                    }
                    target.writeNull();
                }
                case END_INPUT -> {
                    event = null;
                    continue;
                }
            }
            if (depth <= 0 || event == null) {
                break;
            }
            event = source.nextToken();
        }
    }
}