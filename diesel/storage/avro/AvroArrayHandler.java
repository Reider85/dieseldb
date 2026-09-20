package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Handler for Avro ARRAY types in DieselDB (Prompt 75). SQL array values are
 * Java {@link List}/{@code Object[]} instances; the handler converts them into
 * {@link GenericData.Array} for writing — each element is routed through an
 * {@link AvroUnionHandler.ValueConverter} so elements can themselves be
 * nullable or complex — and back into Java {@code List}s on read.
 * <p>
 * A per-call size limit is enforced via {@code avro.array.max.size} (sysprop →
 * config.properties → default {@code 10000}).
 *
 * @since Prompt 75
 */
public final class AvroArrayHandler {

    /** config.properties / system-property key: max elements in an ARRAY column. */
    public static final String MAX_SIZE_KEY = "avro.array.max.size";

    private static final int DEFAULT_MAX_SIZE = 10000;

    private AvroArrayHandler() { }

    /**
     * Maximum number of elements allowed in an ARRAY column, resolved per call
     * from {@code avro.array.max.size} sysprop → config.properties → default
     * {@code 10000}.
     */
    public static int maxSize() {
        return Math.max(1, AvroComplexTypeConfig.intValue(MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
    }

    /**
     * Creates an Avro ARRAY schema for a given element schema.
     */
    public static Schema createArraySchema(Schema elementSchema) {
        if (elementSchema == null) {
            throw new IllegalArgumentException("Array element schema must not be null");
        }
        return Schema.createArray(elementSchema);
    }

    /**
     * Creates a nullable {@code ["null", array]} union wrapping an ARRAY schema.
     */
    public static Schema createNullableArraySchema(Schema elementSchema) {
        return AvroUnionHandler.createNullableUnion(createArraySchema(elementSchema));
    }

    /**
     * Converts a Java array value ({@link Collection} or {@code Object[]}) into
     * a {@link GenericData.Array}. {@code null} is returned as-is.
     *
     * @throws IllegalArgumentException when the value is not an array-like
     *                                  collection, exceeds
     *                                  {@link #maxSize()}, or the schema is not
     *                                  an ARRAY
     */
    public static Object toAvroArray(Object value, Schema arraySchema,
                                     AvroUnionHandler.ValueConverter elementConverter) {
        if (value == null) {
            return null;
        }
        if (arraySchema == null || arraySchema.getType() != Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Schema must be an ARRAY, got: " + arraySchema);
        }
        if (elementConverter == null) {
            throw new IllegalArgumentException("Element converter must not be null");
        }
        List<Object> elements = asList(value);
        if (elements.size() > maxSize()) {
            throw new IllegalArgumentException("Array with " + elements.size()
                    + " elements exceeds avro.array.max.size=" + maxSize());
        }
        Schema elementSchema = arraySchema.getElementType();
        GenericData.Array<Object> result = new GenericData.Array<>(elements.size(), arraySchema);
        for (Object element : elements) {
            result.add(elementConverter.convert(element, elementSchema));
        }
        return result;
    }

    /**
     * Converts a decoded Avro array value (an {@link Iterable}) into a Java
     * {@link List}. {@code null} is returned as-is.
     */
    public static List<Object> fromAvroArray(Object avroValue, Schema arraySchema,
                                             AvroUnionHandler.ValueConverter elementConverter) {
        if (avroValue == null) {
            return null;
        }
        if (!(avroValue instanceof Iterable<?> iterable)) {
            throw new IllegalArgumentException("Avro ARRAY value must be Iterable, got: "
                    + avroValue.getClass().getName());
        }
        Schema elementSchema = arraySchema != null && arraySchema.getType() == Schema.Type.ARRAY
                ? arraySchema.getElementType() : null;
        List<Object> result = new ArrayList<>();
        for (Object element : iterable) {
            result.add(elementConverter != null ? elementConverter.convert(element, elementSchema) : element);
        }
        return result;
    }

    /**
     * Whether the given Java value is array-shaped (a {@link Collection} or an
     * {@code Object[]}).
     */
    public static boolean isArrayValue(Object value) {
        return value instanceof Collection || value instanceof Object[];
    }

    private static List<Object> asList(Object value) {
        if (value instanceof Object[] arr) {
            return Arrays.asList(arr);
        }
        if (value instanceof Collection<?> coll) {
            return new ArrayList<>(coll);
        }
        throw new IllegalArgumentException("Array value must be a Collection or Object[], got: "
                + value.getClass().getName());
    }
}