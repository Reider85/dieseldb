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
            pendingFieldName = handleCopyEvent(event, source, target, pendingFieldName);
            depth = updateDepth(event, depth);
            if (depth <= 0 || event == null) {
                break;
            }
            event = source.nextToken();
        }
    }

    /**
     * Copies a single JSON event from source to target, handling field-name
     * buffering and value writing. Returns the updated pending field name.
     */
    private static String handleCopyEvent(JsonEvent event, JsonStreamParser source,
                                          JsonStreamGenerator target,
                                          String pendingFieldName) throws IOException {
        switch (event) {
            case START_OBJECT -> {
                if (pendingFieldName != null) {
                    target.writeFieldName(pendingFieldName);
                    pendingFieldName = null;
                }
                target.writeStartObject();
            }
            case START_ARRAY -> {
                if (pendingFieldName != null) {
                    target.writeFieldName(pendingFieldName);
                    pendingFieldName = null;
                }
                target.writeStartArray();
            }
            case END_OBJECT -> target.writeEndObject();
            case END_ARRAY -> target.writeEndArray();
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
            case END_INPUT -> { /* handled by caller via depth check */ }
        }
        return pendingFieldName;
    }

    /**
     * Tracks nesting depth: +1 for START_OBJECT/START_ARRAY, -1 for END_OBJECT/END_ARRAY.
     * Returns the updated depth.
     */
    private static int updateDepth(JsonEvent event, int depth) {
        return switch (event) {
            case START_OBJECT, START_ARRAY -> depth + 1;
            case END_OBJECT, END_ARRAY -> depth - 1;
            default -> depth;
        };
    }
}