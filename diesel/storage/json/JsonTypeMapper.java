package diesel.storage.json;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.DieselIOException;

/**
 * Single point of JSON value {@code <->} SQL column type conversion for the
 * JSONL storage (prompt 43). Owns the {@code jsonl.type.coercion} rules
 * (strict vs lenient), the number-precision rules (2^53 boundary in DOUBLE
 * columns, exact LONG reads, scientific notation) and the string-to-typed
 * rules (ISO-8601 dates, UUIDs, strict booleans); NaN/Infinity overflow is
 * rejected on read so no non-finite value can ever reach a column.
 *
 * <p>Both the reader (diesel.storage.JsonlRowReader), the writer-side schema
 * validation (diesel.storage.JsonlSchemaManager) and future schema inference
 * (prompt 44) delegate here, so conversion rules live in exactly one place.
 * Error messages are appended to a caller-supplied {@code context} prefix
 * carrying {@code file:line[:field]} diagnostics.
 *
 * <p>Coercion mode ({@link JsonParserConfig#typeCoercion()}): {@code STRICT}
 * (default) rejects a JSON <em>string</em> value destined for a numeric
 * column ("1" -&gt; 1 is not allowed); {@code LENIENT} performs the same
 * conversions with a WARNING. ISO-8601 string {@code ->} date/time, string
 * {@code ->} boolean and string {@code ->} UUID conversions are deliberate
 * type-specific conversions and are allowed in both modes.
 */
public final class JsonTypeMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTypeMapper.class);

    /** 2^53, the largest integer exactly representable by {@code double}. */
    static final BigDecimal DOUBLE_SAFE_INTEGER = new BigDecimal("9007199254740992");

    private final JsonParserConfig config;

    public JsonTypeMapper() {
        this(JsonParserConfig.defaults());
    }

    public JsonTypeMapper(JsonParserConfig config) {
        this.config = config != null ? config : JsonParserConfig.defaults();
    }

    /** Returns the configuration this mapper converts according to. */
    public JsonParserConfig config() {
        return config;
    }

    /** Returns the effective coercion mode (strict by default). */
    public JsonParserConfig.CoercionMode coercionMode() {
        return config.typeCoercion();
    }

    /**
     * Converts a scalar JSON token into the target column's Java type.
     * JSON {@code null} maps to {@code null}; an untyped/String column keeps
     * the raw token text (dates, UUIDs and numeric literals stay textual in
     * STRING columns). Nested objects/arrays are not handled here - capturing
     * them as compact JSON text stays the reader's job.
     *
     * @param columnType the target schema column type, or {@code null} for an
     *                   untyped column
     * @param token      the JSON value event
     * @param raw        the raw value literal (string text or number literal)
     * @param context    a diagnostics prefix carrying {@code file:line:field},
     *                   e.g. {@code "users.jsonl:line 3: field 'AGE': "}
     * @return the converted value
     * @throws DieselIOException when the value cannot be represented in the
     *                           column type without loss or illegal coercion
     */
    public Object toColumnValue(Class<?> columnType, JsonEvent token, String raw, String context) {
        if (token == JsonEvent.VALUE_NULL) {
            return null;
        }
        if (token == null || raw == null) {
            return raw;
        }
        if (columnType == null || String.class.equals(columnType)) {
            return raw;
        }
        return switch (token) {
            case VALUE_STRING -> stringToColumn(columnType, raw, context);
            case VALUE_TRUE, VALUE_FALSE -> booleanTokenToColumn(columnType, raw, context);
            case VALUE_NUMBER_INT -> integerTokenToColumn(columnType, raw, context);
            case VALUE_NUMBER_FLOAT -> floatTokenToColumn(columnType, raw, context);
            default -> notCompatible(context, raw, columnType, "value");
        };
    }

    /**
     * Write-side precision guard (prompt 43): rejects an integer-valued value
     * whose magnitude exceeds 2^53 being written into a {@code Double} column
     * - such a value would come back from a JSON read as a rounded double
     * (see {@link #toColumnValue}). Keeps the write/read contract symmetric so
     * a file that passes validation can always be read back exactly.
     *
     * @param columnType    the target column type
     * @param value         the value about to be serialised
     * @param field         the field name used in diagnostics
     * @param recordContext a prefix carrying the record coordinate, e.g.
     *                      {@code "record 1: "}
     * @throws DieselIOException when the integer value is not exactly
     *                           representable in a DOUBLE column
     */
    public static void validateDoublePrecision(Class<?> columnType, Object value, String field, String recordContext) {
        if (columnType != Double.class || !(value instanceof Number)) {
            return;
        }
        BigDecimal exact;
        if (value instanceof Integer i) {
            exact = BigDecimal.valueOf(i.longValue());
        } else if (value instanceof Long l) {
            exact = BigDecimal.valueOf(l);
        } else if (value instanceof Short s) {
            exact = BigDecimal.valueOf(s.longValue());
        } else if (value instanceof Byte b) {
            exact = BigDecimal.valueOf(b.longValue());
        } else if (value instanceof BigDecimal bd) {
            exact = bd;
        } else {
            return; // Float/Double are already doubles by definition
        }
        if (isIntegral(exact) && exact.abs().compareTo(DOUBLE_SAFE_INTEGER) > 0) {
            throw new DieselIOException(recordContext + "field '" + field + "': integer value '"
                    + exact.toPlainString() + "' exceeds the 2^53 (" + DOUBLE_SAFE_INTEGER
                    + ") precision boundary of a DOUBLE column - it would be silently rounded "
                    + "on read; store it in a LONG or BigDecimal column instead", null);
        }
    }

    // ─── String tokens ────────────────────────────────────────────────

    private Object stringToColumn(Class<?> type, String raw, String context) {
        if (isNumericType(type) || type == BigDecimal.class) {
            if (coercionMode() == JsonParserConfig.CoercionMode.LENIENT) {
                LOGGER.warn("{}string value '{}' coerced to {} (jsonl.type.coercion=lenient)",
                        context, raw, typeName(type));
            } else {
                throw new DieselIOException(context + "string value '" + raw + "' cannot be coerced to "
                        + typeName(type) + " in strict mode (set jsonl.type.coercion=lenient to allow it)", null);
            }
            try {
                return numericToColumn(type, new BigDecimal(raw.trim()), raw, context, false);
            } catch (NumberFormatException e) {
                throw parseError(context, e, raw, type);
            }
        }
        try {
            if (type == LocalDate.class) {
                return LocalDate.parse(raw.trim());
            }
            if (type == LocalDateTime.class) {
                return LocalDateTime.parse(raw.trim());
            }
            if (type == UUID.class) {
                return UUID.fromString(raw.trim());
            }
            if (type == Boolean.class) {
                return parseBooleanStrict(raw);
            }
            return raw;
        } catch (RuntimeException e) {
            throw parseError(context, e, raw, type);
        }
    }

    private Object booleanTokenToColumn(Class<?> type, String raw, String context) {
        try {
            if (type == Boolean.class) {
                return parseBooleanStrict(raw);
            }
            throw notCompatible(context, raw, type, "boolean");
        } catch (IllegalArgumentException e) {
            throw parseError(context, e, raw, type);
        }
    }

    // ─── Number tokens ────────────────────────────────────────────────

    private Object integerTokenToColumn(Class<?> type, String raw, String context) {
        try {
            return numericToColumn(type, new BigDecimal(raw), raw, context, true);
        } catch (ArithmeticException e) {
            throw new DieselIOException(context + "integer value '" + raw + "' is out of range for column type "
                    + typeName(type), e);
        }
    }

    private Object floatTokenToColumn(Class<?> type, String raw, String context) {
        try {
            return numericToColumn(type, new BigDecimal(raw), raw, context, false);
        } catch (NumberFormatException | ArithmeticException e) {
            throw parseError(context, e, raw, type);
        }
    }

    private Object numericToColumn(Class<?> type, BigDecimal exact, String raw, String context, boolean integerLiteral) {
        if (type == Long.class) {
            return exact.longValueExact();
        }
        if (type == Integer.class) {
            return exact.intValueExact();
        }
        if (type == Short.class) {
            return exact.shortValueExact();
        }
        if (type == Byte.class) {
            return exact.byteValueExact();
        }
        if (type == BigDecimal.class) {
            return exact;
        }
        if (type == Double.class) {
            if (integerLiteral && isIntegral(exact) && exact.abs().compareTo(DOUBLE_SAFE_INTEGER) > 0) {
                throw new DieselIOException(context + "integer value '" + raw + "' exceeds the 2^53 ("
                        + DOUBLE_SAFE_INTEGER + ") precision boundary of a DOUBLE column - such values are "
                        + "silently rounded as double; use a LONG or BigDecimal column to keep them exact", null);
            }
            double converted = exact.doubleValue();
            if (Double.isInfinite(converted)) {
                throw new DieselIOException(context + "value '" + raw + "' overflows the DOUBLE range", null);
            }
            return converted;
        }
        if (type == Float.class) {
            float converted = exact.floatValue();
            if (Float.isInfinite(converted)) {
                throw new DieselIOException(context + "value '" + raw + "' overflows the FLOAT range", null);
            }
            return converted;
        }
        throw notCompatible(context, raw, type, "number");
    }

    // ─── Shared helpers ───────────────────────────────────────────────

    private static boolean isIntegral(BigDecimal value) {
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() <= 0;
    }

    private static boolean isNumericType(Class<?> type) {
        return type == Long.class || type == Integer.class || type == Short.class || type == Byte.class
                || type == Double.class || type == Float.class;
    }

    private DieselIOException notCompatible(String context, String raw, Class<?> type, String kind) {
        String detail = raw != null && !raw.isBlank() ? " '" + raw + "'" : "";
        return new DieselIOException(context + "JSON " + kind + detail + " is not compatible with column type "
                + typeName(type), null);
    }

    private DieselIOException parseError(String context, RuntimeException cause, String raw, Class<?> type) {
        return new DieselIOException(context + "cannot parse \"" + raw + "\" as " + typeName(type)
                + (cause.getMessage() != null ? ": " + cause.getMessage() : ""), cause);
    }

    /** Strict boolean parsing like the delimited readers: true/false/1/0/yes/no/t/f. */
    private static boolean parseBooleanStrict(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Invalid boolean value: null");
        }
        return switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "1", "yes", "t" -> true;
            case "false", "0", "no", "f" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean value: '" + raw + "'");
        };
    }

    private static String typeName(Class<?> type) {
        return type == null ? "String" : type.getSimpleName();
    }
}