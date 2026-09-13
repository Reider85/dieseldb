package diesel.storage.json;

/**
 * Library-neutral stream event for JSON parsing (prompt 42). The JSONL
 * readers/writers in {@code diesel.storage} only ever see these events - the
 * underlying JSON library (Jackson or Gson) is confined to the
 * {@code diesel.storage.json} package.
 */
public enum JsonEvent {
    START_OBJECT,
    END_OBJECT,
    START_ARRAY,
    END_ARRAY,
    FIELD_NAME,
    VALUE_STRING,
    VALUE_NUMBER_INT,
    VALUE_NUMBER_FLOAT,
    VALUE_TRUE,
    VALUE_FALSE,
    VALUE_NULL,
    END_INPUT
}