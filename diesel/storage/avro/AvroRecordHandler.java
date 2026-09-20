package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Handler for Avro RECORD types in DieselDB (Prompt 75). SQL nested-record
 * values are Java {@link Map}s (column name → value); the handler converts
 * them into {@link GenericData.Record}s for writing, filling each schema field
 * from the map (case-insensitive field-name match, missing fields stay null),
 * and back into Java {@code Map}s on read.
 * <p>
 * Nesting is bounded by {@code avro.record.max.nesting.depth} (sysprop →
 * config.properties → default {@code 10}); self-referencing (recursive) record
 * schemas are rejected with an {@link IllegalArgumentException} because the
 * in-memory Java representation has no recursive structure to mirror.
 *
 * @since Prompt 75
 */
public final class AvroRecordHandler {

    /** config.properties / system-property key: max nesting depth of nested RECORD columns. */
    public static final String MAX_NESTING_DEPTH_KEY = "avro.record.max.nesting.depth";

    private static final int DEFAULT_MAX_NESTING_DEPTH = 10;

    private static final ThreadLocal<Deque<String>> RECORD_STACK = ThreadLocal.withInitial(ArrayDeque::new);

    private AvroRecordHandler() { }

    /**
     * Maximum nesting depth of nested RECORD values, resolved per call from
     * {@code avro.record.max.nesting.depth} sysprop → config.properties →
     * default {@code 10}.
     */
    public static int maxNestingDepth() {
        return Math.max(1, AvroComplexTypeConfig.intValue(MAX_NESTING_DEPTH_KEY, DEFAULT_MAX_NESTING_DEPTH));
    }

    /**
     * Creates an Avro RECORD schema after validating the name and that at
     * least one field is declared.
     */
    public static Schema createRecordSchema(String name, List<Schema.Field> fields, String namespace) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Record name must not be blank");
        }
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Record must declare at least one field");
        }
        Schema record = Schema.createRecord(name, null, namespace, false);
        record.setFields(new ArrayList<>(fields));
        return record;
    }

    /**
     * Creates a nullable {@code ["null", record]} union wrapping a RECORD schema.
     */
    public static Schema createNullableRecordSchema(String name, List<Schema.Field> fields, String namespace) {
        return AvroUnionHandler.createNullableUnion(createRecordSchema(name, fields, namespace));
    }

    /**
     * Converts a Java nested-record value (a {@link Map} of column name →
     * value, matched case-insensitively against the record fields) into a
     * {@link GenericData.Record}. An already-{@link GenericRecord} value is
     * passed through, {@code null} is returned as-is.
     *
     * @throws IllegalArgumentException when the value is not a {@link Map}, the
     *                                  schema is not a RECORD, the record
     *                                  nesting exceeds {@link #maxNestingDepth()},
     *                                  or the schema recursively references itself
     */
    public static Object toAvroRecord(Object value, Schema recordSchema,
                                      AvroUnionHandler.ValueConverter fieldConverter) {
        if (value == null) {
            return null;
        }
        if (recordSchema == null || recordSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be a RECORD, got: " + recordSchema);
        }
        if (fieldConverter == null) {
            throw new IllegalArgumentException("Field converter must not be null");
        }
        if (value instanceof GenericRecord) {
            return value;
        }
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("Record value must be a java.util.Map, got: "
                    + value.getClass().getName());
        }
        Deque<String> stack = RECORD_STACK.get();
        String fullName = recordSchema.getFullName();
        if (stack.contains(fullName)) {
            throw new IllegalArgumentException("Recursive record schema not supported: " + fullName);
        }
        if (stack.size() >= maxNestingDepth()) {
            throw new IllegalArgumentException("Record nesting depth exceeds avro.record.max.nesting.depth="
                    + maxNestingDepth());
        }
        stack.push(fullName);
        try {
            Map<String, Object> lookup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                Object key = entry.getKey();
                if (key != null) {
                    lookup.put(key.toString(), entry.getValue());
                }
            }
            GenericData.Record record = new GenericData.Record(recordSchema);
            for (Schema.Field field : recordSchema.getFields()) {
                Object fieldValue = lookup.get(field.name());
                record.put(field.name(), fieldConverter.convert(fieldValue, field.schema()));
            }
            return record;
        } finally {
            stack.pop();
            if (stack.isEmpty()) {
                RECORD_STACK.remove();
            }
        }
    }

    /**
     * Converts a decoded Avro record value into a Java {@code Map<String,
     * Object>}, converting every field through the given converter. {@code null}
     * is returned as-is.
     */
    public static Map<String, Object> fromAvroRecord(Object avroValue, Schema recordSchema,
                                                     AvroUnionHandler.ValueConverter fieldConverter) {
        if (avroValue == null) {
            return null;
        }
        if (!(avroValue instanceof GenericRecord record)) {
            throw new IllegalArgumentException("Avro RECORD value must be a GenericRecord, got: "
                    + avroValue.getClass().getName());
        }
        Schema schema = record.getSchema() != null ? record.getSchema() : recordSchema;
        Map<String, Object> result = new LinkedHashMap<>();
        if (schema != null) {
            for (Schema.Field field : schema.getFields()) {
                Object fieldValue = record.get(field.name());
                result.put(field.name(), fieldConverter != null
                        ? fieldConverter.convert(fieldValue, field.schema()) : fieldValue);
            }
        }
        return result;
    }
}