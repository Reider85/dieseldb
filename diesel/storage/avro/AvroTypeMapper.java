package diesel.storage.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Bidirectional mapping between DieselDB SQL column types (Java classes)
 * and Apache Avro {@link Schema} types.
 * <p>
 * Scalar mapping:
 * <pre>
 *   STRING      → STRING
 *   INTEGER     → INT
 *   LONG        → LONG
 *   SHORT       → INT   (Avro has no SHORT)
 *   BYTE        → INT   (Avro has no BYTE)
 *   FLOAT       → FLOAT
 *   DOUBLE      → DOUBLE
 *   BIGDECIMAL  → BYTES (decimal logical type)
 *   BOOLEAN     → BOOLEAN
 *   DATE        → INT   (date logical type)
 *   DATETIME    → LONG  (timestamp-millis logical type)
 *   DATETIME_MS → LONG  (timestamp-millis logical type)
 *   CHAR        → STRING
 *   UUID        → STRING (uuid logical type)
 * </pre>
 * <p>
 * Complex types (for nested structures):
 * <pre>
 *   RECORD  → Schema.createRecord(...)
 *   ARRAY   → Schema.createArray(elementSchema)
 *   MAP     → Schema.createMap(valueSchema)
 *   ENUM    → Schema.createEnum(...)
 * </pre>
 *
 * @since Prompt 58
 */
public final class AvroTypeMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroTypeMapper.class);

    private AvroTypeMapper() { }

    // ─── SQL → Avro ────────────────────────────────────────────────

    /**
     * Converts a DieselDB Java type to the corresponding Avro {@link Schema}.
     *
     * @param javaType    the Java class used by DieselDB for the column
     * @param columnName  column name (used for named Avro types like records/enums)
     * @return the Avro schema for the type
     * @throws IllegalArgumentException if the Java type has no Avro mapping
     */
    public static Schema toAvroSchema(Class<?> javaType, String columnName) {
        if (javaType == null) {
            throw new IllegalArgumentException("Column type must not be null for column: " + columnName);
        }
        return switch (javaType.getSimpleName()) {
            case "String" -> Schema.create(Schema.Type.STRING);
            case "Integer" -> Schema.create(Schema.Type.INT);
            case "Long" -> Schema.create(Schema.Type.LONG);
            case "Short" -> Schema.create(Schema.Type.INT);   // Avro has no SHORT
            case "Byte" -> Schema.create(Schema.Type.INT);    // Avro has no BYTE
            case "Float" -> Schema.create(Schema.Type.FLOAT);
            case "Double" -> Schema.create(Schema.Type.DOUBLE);
            case "Boolean" -> Schema.create(Schema.Type.BOOLEAN);
            case "BigDecimal" -> LogicalTypes.decimal(38, 18)
                    .addToSchema(Schema.create(Schema.Type.BYTES));
            case "LocalDate" -> LogicalTypes.date()
                    .addToSchema(Schema.create(Schema.Type.INT));
            case "LocalDateTime" -> LogicalTypes.timestampMillis()
                    .addToSchema(Schema.create(Schema.Type.LONG));
            case "Character" -> Schema.create(Schema.Type.STRING);
            case "UUID" -> LogicalTypes.uuid()
                    .addToSchema(Schema.create(Schema.Type.STRING));
            // Prompt 75 complex types: List/Map columns default to arrays/maps of
            // nullable strings; callers needing precise element/value types build
            // the schema with AvroArrayHandler/AvroMapHandler directly.
            case "List", "ArrayList", "LinkedList", "Collection" ->
                    AvroArrayHandler.createArraySchema(AvroTypeMapper.nullableOf(Schema.create(Schema.Type.STRING)));
            case "Map", "HashMap", "LinkedHashMap", "TreeMap" ->
                    AvroMapHandler.createMapSchema(AvroTypeMapper.nullableOf(Schema.create(Schema.Type.STRING)));
            default -> throw new IllegalArgumentException(
                    "Unsupported DieselDB type for Avro mapping: " + javaType.getName()
                            + " (column: " + columnName + ")");
        };
    }

    /**
     * Creates a nullable Avro schema: {@code ["null", baseSchema]} union.
     */
    public static Schema nullableOf(Class<?> javaType, String columnName) {
        Schema base = toAvroSchema(javaType, columnName);
        return nullableOf(base);
    }

    /**
     * Wraps an existing schema into a nullable {@code ["null", schema]} union with
     * the NULL branch always first (delegated to {@link AvroUnionHandler}).
     */
    public static Schema nullableOf(Schema base) {
        return AvroUnionHandler.createNullableUnion(base);
    }

    // ─── Avro → SQL ────────────────────────────────────────────────

    /**
     * Converts an Avro {@link Schema} to the corresponding DieselDB Java class.
     * <p>
     * Handles logical types (date, timestamp-millis, decimal, uuid) and falls
     * back to the underlying primitive type when no logical type is present.
     *
     * @param avroSchema the Avro schema
     * @return the DieselDB Java class, or {@code null} if the schema type is unknown
     */
    public static Class<?> toJavaType(Schema avroSchema) {
        if (avroSchema == null) {
            return null;
        }
        // Unwrap UNION: take the first non-null branch (delegated to AvroUnionHandler)
        if (avroSchema.getType() == Schema.Type.UNION) {
            return AvroUnionHandler.unwrapNonNullType(avroSchema);
        }
        // Unwrap logical types on primitives
        Schema base = avroSchema.getType() == Schema.Type.RECORD
                || avroSchema.getType() == Schema.Type.ARRAY
                || avroSchema.getType() == Schema.Type.MAP
                || avroSchema.getType() == Schema.Type.ENUM
                ? avroSchema
                : avroSchema; // logical type is attached to the same Schema object

        if (avroSchema.getLogicalType() != null) {
            return switch (avroSchema.getLogicalType().getName()) {
                case "date" -> LocalDate.class;
                case "timestamp-millis", "timestamp-micros" -> LocalDateTime.class;
                case "decimal" -> BigDecimal.class;
                case "uuid" -> UUID.class;
                default -> toPrimitiveJavaType(avroSchema.getType());
            };
        }
        return toPrimitiveJavaType(avroSchema.getType());
    }

    /**
     * Maps a bare Avro primitive type (without logical type) to a Java class.
     */
    private static Class<?> toPrimitiveJavaType(Schema.Type type) {
        return switch (type) {
            case STRING -> String.class;
            case INT -> Integer.class;
            case LONG -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case BOOLEAN -> Boolean.class;
            case BYTES -> byte[].class;
            case FIXED -> byte[].class;
            case RECORD -> null;            // complex type, caller must handle
            case ARRAY -> List.class;       // complex type (Prompt 75)
            case MAP -> Map.class;          // complex type (Prompt 75)
            case ENUM -> String.class;      // enums stored as strings in SQL
            case NULL -> null;
            default -> null;
        };
    }

    // ─── Helpers ────────────────────────────────────────────────────

    /**
     * Checks whether the given Avro schema is a UNION that contains NULL
     * (delegated to {@link AvroUnionHandler#isNullableUnion}).
     */
    public static boolean isNullable(Schema schema) {
        return AvroUnionHandler.isNullableUnion(schema);
    }

    /**
     * Returns the DieselDB type name string for a Java class.
     * Follows the convention of {@code SqlKeywords.TYPE_*} constants.
     */
    public static String typeName(Class<?> type) {
        if (type == null) {
            return null;
        }
        return switch (type.getSimpleName()) {
            case "Long" -> "LONG";
            case "Integer" -> "INTEGER";
            case "Short" -> "SHORT";
            case "Byte" -> "BYTE";
            case "Double" -> "DOUBLE";
            case "Float" -> "FLOAT";
            case "BigDecimal" -> "BIGDECIMAL";
            case "Boolean" -> "BOOLEAN";
            case "LocalDate" -> "DATE";
            case "LocalDateTime" -> "DATETIME";
            case "UUID" -> "UUID";
            case "String" -> "STRING";
            case "Character" -> "CHAR";
            case "byte[]" -> "BYTES";
            // Prompt 75 complex types
            case "List", "ArrayList", "LinkedList" -> "ARRAY";
            case "Map", "HashMap", "LinkedHashMap" -> "MAP";
            default -> type.getSimpleName();
        };
    }

    /**
     * Resolves a DieselDB type name (e.g. "STRING", "INTEGER") to a Java class.
     */
    public static Class<?> typeClass(String name) {
        if (name == null) {
            return null;
        }
        return switch (name.toUpperCase()) {
            case "STRING" -> String.class;
            case "INTEGER" -> Integer.class;
            case "LONG" -> Long.class;
            case "SHORT" -> Short.class;
            case "BYTE" -> Byte.class;
            case "DOUBLE" -> Double.class;
            case "FLOAT" -> Float.class;
            case "BIGDECIMAL" -> BigDecimal.class;
            case "BOOLEAN" -> Boolean.class;
            case "DATE" -> LocalDate.class;
            case "DATETIME", "DATETIME_MS" -> LocalDateTime.class;
            case "UUID" -> UUID.class;
            case "CHAR" -> Character.class;
            // Prompt 75 complex types
            case "ARRAY" -> List.class;
            case "MAP" -> Map.class;
            default -> null;
        };
    }

    // ─── Complex type builders ──────────────────────────────────────

    /**
     * Creates an Avro RECORD schema.
     *
     * @param name      record name (must be a valid Avro name)
     * @param fields    field definitions (name → schema)
     * @param namespace Avro namespace (may be {@code null})
     */
    public static Schema createRecord(String name, Schema.Field[] fields, String namespace) {
        Schema record = Schema.createRecord(name, null, namespace, false);
        record.setFields(java.util.Arrays.asList(fields));
        return record;
    }

    /**
     * Creates an Avro RECORD schema from a list of named fields.
     *
     * @param name      record name
     * @param fields    array of Schema.Field
     * @param namespace Avro namespace (may be {@code null})
     * @param isError   whether this is an error record
     * @return the RECORD schema
     */
    public static Schema buildRecord(String name, java.util.List<Schema.Field> fields,
                                     String namespace, boolean isError) {
        Schema record = Schema.createRecord(name, null, namespace, isError);
        record.setFields(fields);
        return record;
    }

    /**
     * Creates an Avro ARRAY schema for a given element type
     * (delegated to {@link AvroArrayHandler}).
     */
    public static Schema createArray(Schema elementSchema) {
        return AvroArrayHandler.createArraySchema(elementSchema);
    }

    /**
     * Creates an Avro MAP schema for a given value type
     * (delegated to {@link AvroMapHandler}).
     */
    public static Schema createMap(Schema valueSchema) {
        return AvroMapHandler.createMapSchema(valueSchema);
    }

    /**
     * Creates an Avro ENUM schema.
     *
     * @param name      enum name
     * @param values    allowed enum values
     * @param namespace Avro namespace (may be {@code null})
     */
    public static Schema createEnum(String name, java.util.List<String> values, String namespace) {
        return AvroEnumHandler.createEnumSchema(name, values, namespace);
    }
}
