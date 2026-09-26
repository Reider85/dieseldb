package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import diesel.ConfigKeys;

/**
 * Central handler for Apache Avro {@code UNION} types in DieselDB.
 * <p>
 * Provides schema construction with a guaranteed {@code null}-first branch
 * order (the Avro spec recommends {@code null} as the first union branch for
 * both readability and decode efficiency), schema inspection helpers, branch
 * resolution, and the fast-path (de)serialization helpers used by
 * {@link AvroRowStorage} so a nullable column never pays union overhead when
 * the value is present.
 *
 * <p>Union ordering is a per-call decision resolved from sysprop
 * {@code avro.union.null.first} → config.properties → default {@code true}
 * (mirroring the other AVRO config classes, no static caching). Validation
 * of the null-first invariant is governed by {@code avro.union.validate.ordering}
 * with the same resolution chain.
 *
 * @since Prompt 74
 */
public final class AvroUnionHandler {

    /** config.properties / system-property key: move the NULL branch to index 0 at union creation. */
    public static final String UNION_NULL_FIRST_KEY = "avro.union.null.first";
    /** config.properties / system-property key: check the null-first invariant in {@link #validateUnionOrdering}. */
    public static final String UNION_VALIDATE_ORDERING_KEY = "avro.union.validate.ordering";
    /** config.properties / system-property key: config file override used by tests. */
    public static final String UNION_CONFIG_FILE_KEY = "avro.union.config.file";

    private static final String DEFAULT_CONFIG_FILE = "config.properties";

    private AvroUnionHandler() { }

    // ─── Schema construction ────────────────────────────────────────

    /**
     * Wraps a base schema into a nullable {@code ["null", base]} union with the
     * NULL branch always first.
     *
     * @param base the non-null, non-union branch schema
     * @return a two-branch union {@code ["null", base]}
     * @throws IllegalArgumentException if {@code base} is {@code null}, already a
     *                                  UNION, or the bare NULL type
     */
    public static Schema createNullableUnion(Schema base) {
        if (base == null) {
            throw new IllegalArgumentException("Base schema must not be null for a nullable union");
        }
        if (base.getType() == Schema.Type.UNION) {
            throw new IllegalArgumentException("Base schema must not already be a union: " + base);
        }
        if (base.getType() == Schema.Type.NULL) {
            throw new IllegalArgumentException("Base schema must not be the NULL type itself: " + base);
        }
        List<Schema> branches = new ArrayList<>(2);
        branches.add(Schema.create(Schema.Type.NULL));
        branches.add(base);
        return Schema.createUnion(branches);
    }

    /**
     * Creates a UNION schema from the given branches. When
     * {@code avro.union.null.first} is enabled (default) any NULL branch is moved
     * to index 0, preserving the relative order of the remaining branches.
     * Duplicate branch types are rejected (Avro unions must be unique; named
     * RECORD/ENUM/FIXED branches are compared by fullname).
     *
     * @param branches the union branches
     * @return the UNION schema
     * @throws IllegalArgumentException if no branch is given, a branch is
     *                                  {@code null}, or two branches duplicate
     */
    public static Schema createUnion(Schema... branches) {
        if (branches == null || branches.length == 0) {
            throw new IllegalArgumentException("Union must have at least one branch");
        }
        return createUnion(Arrays.asList(branches));
    }

    /**
     * {@link #createUnion(Schema...)} overload accepting a {@link List}.
     */
    public static Schema createUnion(List<Schema> branches) {
        if (branches == null || branches.isEmpty()) {
            throw new IllegalArgumentException("Union must have at least one branch");
        }
        List<Schema> ordered = new ArrayList<>(branches);
        for (Schema branch : ordered) {
            if (branch == null) {
                throw new IllegalArgumentException(
                        "Union branch must not be null; use Schema.create(Schema.Type.NULL) for a null branch");
            }
        }
        if (nullFirst()) {
            ordered.sort((s1, s2) -> {
                boolean n1 = s1.getType() == Schema.Type.NULL;
                boolean n2 = s2.getType() == Schema.Type.NULL;
                if (n1 == n2) {
                    return 0; // stable sort preserves relative order of non-null branches
                }
                return n1 ? -1 : 1;
            });
        }
        Map<String, String> seen = new LinkedHashMap<>();
        for (Schema branch : ordered) {
            String key = branchKey(branch);
            String previous = seen.put(key, branch.toString());
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate union branch " + branch + " (already present as " + previous + ")");
            }
        }
        return Schema.createUnion(ordered);
    }

    private static String branchKey(Schema branch) {
        Schema.Type type = branch.getType();
        if (type == Schema.Type.RECORD || type == Schema.Type.ENUM || type == Schema.Type.FIXED) {
            return type + ":" + branch.getFullName();
        }
        return type.name();
    }

    // ─── Schema inspection ──────────────────────────────────────────

    /**
     * Returns {@code true} when the schema is a UNION that contains a NULL branch.
     */
    public static boolean isNullableUnion(Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return false;
        }
        int nullIndex = getNullBranchIndex(schema);
        return nullIndex >= 0;
    }

    /**
     * Returns the index of the NULL branch in a UNION, or {@code -1} when the
     * schema is not a UNION or contains no NULL branch.
     */
    public static int getNullBranchIndex(Schema union) {
        if (union == null || union.getType() != Schema.Type.UNION) {
            return -1;
        }
        List<Schema> types = union.getTypes();
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).getType() == Schema.Type.NULL) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the non-null branches of a UNION as an unmodifiable list. For a
     * non-union schema an empty list is returned.
     */
    public static List<Schema> getNonNullableBranches(Schema union) {
        if (union == null || union.getType() != Schema.Type.UNION) {
            return List.of();
        }
        List<Schema> branches = new ArrayList<>(union.getTypes().size());
        for (Schema branch : union.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                branches.add(branch);
            }
        }
        return Collections.unmodifiableList(branches);
    }

    /**
     * Returns {@code true} when the schema is a UNION with exactly one non-null
     * branch, i.e. the common nullable column shape {@code ["null", T]}.
     */
    public static boolean isSingleTypeUnion(Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return false;
        }
        return getNonNullableBranches(schema).size() == 1;
    }

    /**
     * Strips the union wrapper: returns the first non-null branch of a UNION, the
     * schema itself when it is not a UNION, or {@code null} when the UNION has no
     * non-null branch.
     */
    public static Schema unwrapUnion(Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        return null;
    }

    /**
     * Maps a (possibly union) Avro schema to its DieselDB Java class by unwrapping
     * the first non-null branch and delegating to {@link AvroTypeMapper#toJavaType}.
     *
     * @param schema the Avro schema, possibly a UNION
     * @return the DieselDB Java class, or {@code null} for an all-null union or an
     *         unknown schema
     */
    public static Class<?> unwrapNonNullType(Schema schema) {
        Schema base = unwrapUnion(schema);
        return AvroTypeMapper.toJavaType(base);
    }

    // ─── Value resolution ───────────────────────────────────────────

    /**
     * Resolves the union branch index that best matches a Java value. A {@code null}
     * value resolves to the NULL branch. Non-null values are matched against the
     * non-null branches by their runtime primitive type (logical-type decorations
     * are ignored: a {@code LocalDate} matches an INT branch, a {@code BigDecimal}
     * matches a BYTES branch, a {@code UUID} a STRING branch).
     *
     * @param union the UNION schema
     * @param value the Java value
     * @return the matching branch index, or {@code -1} when no branch matches
     * @throws IllegalArgumentException if {@code union} is not a UNION
     */
    public static int resolveBranchIndex(Schema union, Object value) {
        if (union == null || union.getType() != Schema.Type.UNION) {
            throw new IllegalArgumentException("Schema must be a UNION, got: " + union);
        }
        if (value == null) {
            return getNullBranchIndex(union);
        }
        List<Schema> types = union.getTypes();
        for (int i = 0; i < types.size(); i++) {
            Schema branch = types.get(i);
            if (branch.getType() != Schema.Type.NULL && valueMatchesBranch(value, branch)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean valueMatchesBranch(Object value, Schema branch) {
        Schema.Type valueType = primitiveTypeOf(value);
        if (valueType != null && valueType == branch.getType()) {
            return true;
        }
        // Prompt 75 complex types: a CharSequence value can fill an ENUM branch
        // (validated against the enum symbols) and a Map can fill a RECORD branch,
        // so nullable complex columns resolve to the correct union branch before
        // the value is converted.
        if (branch.getType() == Schema.Type.ENUM && value instanceof CharSequence) {
            return AvroEnumHandler.isValidSymbol(branch, value.toString());
        }
        return branch.getType() == Schema.Type.RECORD && value instanceof Map;
    }

    private static Schema.Type primitiveTypeOf(Object value) {
        if (value == null) {
            return Schema.Type.NULL;
        }
        if (value instanceof String || value instanceof Character) {
            return Schema.Type.STRING;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return Schema.Type.INT;
        }
        if (value instanceof Long) {
            return Schema.Type.LONG;
        }
        if (value instanceof Float) {
            return Schema.Type.FLOAT;
        }
        if (value instanceof Double) {
            return Schema.Type.DOUBLE;
        }
        if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        }
        if (value instanceof BigDecimal || value instanceof ByteBuffer || value instanceof byte[]) {
            return Schema.Type.BYTES;
        }
        if (value instanceof LocalDate) {
            return Schema.Type.INT; // date logical type lives on an INT primitive
        }
        if (value instanceof LocalDateTime || value instanceof Instant) {
            return Schema.Type.LONG; // timestamp logic type lives on a LONG primitive
        }
        if (value instanceof UUID) {
            return Schema.Type.STRING; // uuid logical type lives on a STRING primitive
        }
        if (value instanceof GenericRecord) {
            return Schema.Type.RECORD;
        }
        if (value instanceof org.apache.avro.generic.GenericEnumSymbol) {
            return Schema.Type.ENUM;
        }
        if (value instanceof Map) {
            return Schema.Type.MAP;
        }
        if (value instanceof java.util.Collection || value instanceof GenericData.Array
                || value instanceof Object[]) {
            return Schema.Type.ARRAY;
        }
        return null;
    }

    // ─── Write / read helpers ───────────────────────────────────────

    /**
     * Converts a Java value to its Avro representation for the given field schema.
     * <p>
     * Fast paths (optimization for the common case):
     * <ul>
     *   <li>non-union field schema → the value is delegated straight to the
     *       {@code converter} without any union overhead;</li>
     *   <li>{@code null} value on a nullable union → returned as-is (the NULL
     *       branch), the converter is not invoked.</li>
     * </ul>
     * For a non-null value on a multi-branch union the matching branch is resolved
     * and the converter is invoked against that single branch schema.
     *
     * @param value       the Java value
     * @param fieldSchema the Avro field schema (union or not)
     * @param converter   callback converting the value against a concrete
     *                    (already unwrapped) branch schema
     * @return the Avro-ready value for the record
     * @throws IllegalArgumentException when no union branch matches the value
     */
    public static Object wrapForWrite(Object value, Schema fieldSchema, ValueConverter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("ValueConverter must not be null");
        }
        if (fieldSchema == null || fieldSchema.getType() != Schema.Type.UNION) {
            return converter.convert(value, fieldSchema); // common case: not a union, no overhead
        }
        if (value == null) {
            return null; // fast path: null value → NULL union branch
        }
        int index = resolveBranchIndex(fieldSchema, value);
        if (index < 0) {
            throw new IllegalArgumentException(
                    "No union branch matches value " + value + " of type " + value.getClass().getName()
                            + " in schema " + fieldSchema);
        }
        Schema branch = fieldSchema.getTypes().get(index);
        return converter.convert(value, branch);
    }

    /**
     * Normalizes an already-unwrapped Avro union value (the decoder returns the
     * branch value directly, e.g. {@code null}, {@code Integer} or {@code String})
     * into the branch schema it was read from.
     *
     * @param avroValue   the raw Avro value read back from a union field
     * @param fieldSchema the Avro field schema (union or not)
     * @return a {@link UnionReadValue} carrying the resolved branch schema and the
     *         (unchanged) value
     */
    public static UnionReadValue unwrapForRead(Object avroValue, Schema fieldSchema) {
        if (fieldSchema == null || fieldSchema.getType() != Schema.Type.UNION) {
            return new UnionReadValue(fieldSchema, avroValue);
        }
        if (avroValue == null) {
            int nullIndex = getNullBranchIndex(fieldSchema);
            Schema branch = nullIndex >= 0 ? fieldSchema.getTypes().get(nullIndex) : null;
            return new UnionReadValue(branch, null);
        }
        for (Schema branch : fieldSchema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL && valueMatchesBranch(avroValue, branch)) {
                return new UnionReadValue(branch, avroValue);
            }
        }
        return new UnionReadValue(unwrapUnion(fieldSchema), avroValue);
    }

    // ─── Ordering validation ────────────────────────────────────────

    /**
     * Validates the null-first invariant of a union: when a NULL branch is present
     * it must be at index 0 (the Avro spec recommendation). Verification can be
     * disabled via {@code avro.union.validate.ordering=false}.
     *
     * @param schema the Avro schema to check
     * @return the validation result
     */
    public static UnionOrdering validateUnionOrdering(Schema schema) {
        if (schema == null || schema.getType() != Schema.Type.UNION) {
            return new UnionOrdering(true, "Not a union: nothing to validate");
        }
        if (!validateOrdering()) {
            return new UnionOrdering(true, "Ordering validation disabled by avro.union.validate.ordering=false");
        }
        int nullIndex = getNullBranchIndex(schema);
        if (nullIndex < 0) {
            return new UnionOrdering(true, "Union has no NULL branch: " + schema);
        }
        if (nullIndex == 0) {
            return new UnionOrdering(true, "NULL is the first union branch: " + schema);
        }
        return new UnionOrdering(false,
                "NULL must be the first union branch — found at index " + nullIndex + " in " + schema);
    }

    // ─── Config resolution ──────────────────────────────────────────

    /**
     * Whether union creation moves a NULL branch to index 0 — resolved per call
     * from {@code avro.union.null.first} sysprop → config.properties → default
     * {@code true}.
     */
    public static boolean nullFirst() {
        return resolveBooleanConfig(UNION_NULL_FIRST_KEY, true);
    }

    /**
     * Whether {@link #validateUnionOrdering} enforces the null-first invariant —
     * resolved per call from {@code avro.union.validate.ordering} sysprop →
     * config.properties → default {@code true}.
     */
    public static boolean validateOrdering() {
        return resolveBooleanConfig(UNION_VALIDATE_ORDERING_KEY, true);
    }

    private static boolean resolveBooleanConfig(String key, boolean defaultValue) {
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isBlank()) {
            return Boolean.parseBoolean(sysValue);
        }
        String userDir = System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, ".");
        String configPath = System.getProperty(UNION_CONFIG_FILE_KEY, DEFAULT_CONFIG_FILE);
        File configFile = new File(userDir, configPath);
        if (configFile.exists()) {
            try {
                var props = new java.util.Properties();
                try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                    props.load(in);
                }
                String val = props.getProperty(key);
                if (val != null && !val.isBlank()) {
                    return Boolean.parseBoolean(val);
                }
            } catch (IOException ignored) {
                // fall through to the default
            }
        }
        return defaultValue;
    }

    // ─── Records / functional interface ─────────────────────────────

    /**
     * Result of {@link #validateUnionOrdering}.
     *
     * @param ok      whether the ordering invariant holds
     * @param message human-readable diagnostic
     */
    public record UnionOrdering(boolean ok, String message) { }

    /**
     * Result of {@link #unwrapForRead}: the branch schema a union value was read
     * from, alongside the (unchanged) decoded value.
     *
     * @param branchSchema the resolved branch schema (may be {@code null})
     * @param value        the decoded value
     */
    public record UnionReadValue(Schema branchSchema, Object value) { }

    /**
     * Callback converting a Java value against a concrete (already unwrapped)
     * Avro branch schema. {@link AvroRowStorage} supplies its scalar conversion
     * logic here, so the handler never has a hard dependency back on the storage
     * class.
     */
    @FunctionalInterface
    public interface ValueConverter {
        Object convert(Object value, Schema fieldSchema);
    }
}