package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.List;
import diesel.storage.StorageMessageConstants;

/**
 * Handler for Avro ENUM types in DieselDB (Prompt 75). Enum columns expose a
 * fixed set of allowed symbols (constrained columns); SQL values are ordinary
 * strings that are canonicalized against the schema's symbol list.
 * <p>
 * Symbol matching follows {@code avro.enum.case.sensitive} (sysprop →
 * config.properties → default {@code false}): when case-insensitive, a value
 * such as {@code "active"} is canonicalized to the schema's {@code "ACTIVE"}.
 * Writers receive a {@link GenericData.EnumSymbol} so both plain enum fields
 * and enums nested inside unions encode correctly.
 *
 * @since Prompt 75
 */
public final class AvroEnumHandler {

    /** config.properties / system-property key: case-sensitive enum symbol matching. */
    public static final String CASE_SENSITIVE_KEY = "avro.enum.case.sensitive";

    private static final boolean DEFAULT_CASE_SENSITIVE = false;

    private AvroEnumHandler() { }

    /**
     * Whether enum symbol matching is case-sensitive — resolved per call from
     * {@code avro.enum.case.sensitive} sysprop → config.properties → default
     * {@code false}.
     */
    public static boolean caseSensitive() {
        return AvroComplexTypeConfig.booleanValue(CASE_SENSITIVE_KEY, DEFAULT_CASE_SENSITIVE);
    }

    /**
     * Creates an Avro ENUM schema after validating name and symbols
     * (non-blank, no duplicates). Symbol syntax itself is enforced by
     * {@link Schema#createEnum(String, String, String, List)}.
     */
    public static Schema createEnumSchema(String name, List<String> symbols, String namespace) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Enum name must not be blank");
        }
        if (symbols == null || symbols.isEmpty()) {
            throw new IllegalArgumentException("Enum must declare at least one symbol");
        }
        List<String> clean = new ArrayList<>(symbols.size());
        for (String symbol : symbols) {
            if (symbol == null || symbol.isBlank()) {
                throw new IllegalArgumentException("Enum symbols must not be null or blank");
            }
            if (clean.contains(symbol)) {
                throw new IllegalArgumentException("Duplicate enum symbol: " + symbol);
            }
            clean.add(symbol);
        }
        return Schema.createEnum(name, null, namespace, clean);
    }

    /**
     * Whether {@code symbol} is an allowed value of the given enum schema,
     * honouring {@link #caseSensitive()}.
     */
    public static boolean isValidSymbol(Schema enumSchema, String symbol) {
        if (enumSchema == null || enumSchema.getType() != Schema.Type.ENUM || symbol == null) {
            return false;
        }
        if (caseSensitive()) {
            return enumSchema.getEnumSymbols().contains(symbol);
        }
        for (String s : enumSchema.getEnumSymbols()) {
            if (s.equalsIgnoreCase(symbol)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the exact schema symbol matching {@code symbol} (canonicalizing
     * the case when {@code avro.enum.case.sensitive=false}), or throws when no
     * symbol matches.
     */
    public static String canonicalSymbol(Schema enumSchema, String symbol) {
        if (enumSchema == null || enumSchema.getType() != Schema.Type.ENUM) {
            throw new IllegalArgumentException(StorageMessageConstants.SCHEMA_NOT_ENUM + enumSchema);
        }
        if (caseSensitive()) {
            if (enumSchema.getEnumSymbols().contains(symbol)) {
                return symbol;
            }
        } else {
            for (String s : enumSchema.getEnumSymbols()) {
                if (s.equalsIgnoreCase(symbol)) {
                    return s;
                }
            }
        }
        throw new IllegalArgumentException("Value '" + symbol + "' is not a valid "
                + enumSchema.getFullName() + " enum symbol; allowed: " + enumSchema.getEnumSymbols());
    }

    /**
     * Converts a Java value (expected {@code String} or an existing
     * {@link GenericData.EnumSymbol}) into an Avro {@link GenericData.EnumSymbol}
     * for the given enum schema.
     */
    public static Object toAvroEnum(Object value, Schema enumSchema) {
        if (value == null) {
            return null;
        }
        if (enumSchema == null || enumSchema.getType() != Schema.Type.ENUM) {
            throw new IllegalArgumentException(StorageMessageConstants.SCHEMA_NOT_ENUM + enumSchema);
        }
        String symbol = value instanceof GenericData.EnumSymbol es ? es.toString() : value.toString();
        return new GenericData.EnumSymbol(enumSchema, canonicalSymbol(enumSchema, symbol));
    }

    /**
     * Converts a decoded Avro enum value back to its symbol string.
     */
    public static String fromAvroEnum(Object avroValue) {
        return avroValue == null ? null : avroValue.toString();
    }

    /**
     * Returns the allowed symbols of an enum schema.
     */
    public static List<String> symbols(Schema enumSchema) {
        if (enumSchema == null || enumSchema.getType() != Schema.Type.ENUM) {
            throw new IllegalArgumentException(StorageMessageConstants.SCHEMA_NOT_ENUM + enumSchema);
        }
        return List.copyOf(enumSchema.getEnumSymbols());
    }
}