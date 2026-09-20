package diesel.storage.avro;

import org.apache.avro.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Handler for Avro MAP types in DieselDB (Prompt 75). SQL map values are Java
 * {@link Map} instances; Avro MAP keys are always strings, and the values are
 * routed through an {@link AvroUnionHandler.ValueConverter} on write and read
 * so nullable/complex value types are supported.
 * <p>
 * A per-call size limit is enforced via {@code avro.map.max.size} (sysprop →
 * config.properties → default {@code 10000}).
 *
 * @since Prompt 75
 */
public final class AvroMapHandler {

    /** config.properties / system-property key: max entries in a MAP column. */
    public static final String MAX_SIZE_KEY = "avro.map.max.size";

    private static final int DEFAULT_MAX_SIZE = 10000;

    private AvroMapHandler() { }

    /**
     * Maximum number of entries allowed in a MAP column, resolved per call
     * from {@code avro.map.max.size} sysprop → config.properties → default
     * {@code 10000}.
     */
    public static int maxSize() {
        return Math.max(1, AvroComplexTypeConfig.intValue(MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
    }

    /**
     * Creates an Avro MAP schema for a given value schema.
     */
    public static Schema createMapSchema(Schema valueSchema) {
        if (valueSchema == null) {
            throw new IllegalArgumentException("Map value schema must not be null");
        }
        return Schema.createMap(valueSchema);
    }

    /**
     * Creates a nullable {@code ["null", map]} union wrapping a MAP schema.
     */
    public static Schema createNullableMapSchema(Schema valueSchema) {
        return AvroUnionHandler.createNullableUnion(createMapSchema(valueSchema));
    }

    /**
     * Converts a Java {@link Map} into an Avro map (string-keyed), converting
     * every value through the given converter against the map's value schema.
     * {@code null} is returned as-is.
     *
     * @throws IllegalArgumentException when the value is not a {@link Map},
     *                                  contains a null key, exceeds
     *                                  {@link #maxSize()}, or the schema is not
     *                                  a MAP
     */
    public static Object toAvroMap(Object value, Schema mapSchema,
                                   AvroUnionHandler.ValueConverter valueConverter) {
        if (value == null) {
            return null;
        }
        if (mapSchema == null || mapSchema.getType() != Schema.Type.MAP) {
            throw new IllegalArgumentException("Schema must be a MAP, got: " + mapSchema);
        }
        if (valueConverter == null) {
            throw new IllegalArgumentException("Value converter must not be null");
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Map value must be a java.util.Map, got: "
                    + value.getClass().getName());
        }
        if (map.size() > maxSize()) {
            throw new IllegalArgumentException("Map with " + map.size()
                    + " entries exceeds avro.map.max.size=" + maxSize());
        }
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (key == null) {
                throw new IllegalArgumentException("Avro MAP keys must not be null");
            }
            result.put(key.toString(), valueConverter.convert(entry.getValue(), mapSchema.getValueType()));
        }
        return result;
    }

    /**
     * Converts a decoded Avro map value into a Java {@code Map<String, Object>},
     * converting every value through the given converter. {@code null} is
     * returned as-is.
     */
    public static Map<String, Object> fromAvroMap(Object avroValue, Schema mapSchema,
                                                  AvroUnionHandler.ValueConverter valueConverter) {
        if (avroValue == null) {
            return null;
        }
        if (!(avroValue instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Avro MAP value must be a Map, got: "
                    + avroValue.getClass().getName());
        }
        Schema valueSchema = mapSchema != null && mapSchema.getType() == Schema.Type.MAP
                ? mapSchema.getValueType() : null;
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey() instanceof CharSequence cs ? cs.toString() : String.valueOf(entry.getKey());
            result.put(key, valueConverter != null ? valueConverter.convert(entry.getValue(), valueSchema) : entry.getValue());
        }
        return result;
    }
}