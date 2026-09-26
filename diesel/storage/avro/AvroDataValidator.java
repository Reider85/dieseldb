package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diesel.SqlKeywords;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Validates data rows against an Avro {@link Schema} before they are written
 * (Prompt 73).
 *
 * <p>A row is validated field-by-field against the schema's RECORD definition —
 * field presence, nullability and the Java value's type against the Avro field
 * type, including Avro logical types ({@code decimal}/{@code date}/{@code
 * timestamp}/{@code uuid}) and ENUM symbol membership. Long/Short/Byte values
 * are accepted in INT columns only when they fit the int range; a null is only
 * valid for a nullable column ("null" type or union containing NULL).
 *
 * <p>Two validation modes are supported:
 * <ul>
 *   <li>{@link ValidationMode#PERMISSIVE} (default) — every row is scanned,
 *       each invalid row is logged at WARN (respecting
 *       {@code avro.validation.log.invalid}), the run still completes and the
 *       returned {@link DatasetValidationResult} carries the full validation
 *       statistics (valid/invalid counts, error counts per type and per field,
 *       failed row indexes).</li>
 *   <li>{@link ValidationMode#STRICT} — fail-fast: the first invalid row throws
 *       {@link AvroValidationException} (with the row index and its field
 *       errors) so the caller can abort the write.</li>
 * </ul>
 *
 * <p>Rows can be supplied as positional {@code Object[]} arrays (the compact
 * form used by the row storages), name-keyed {@code Map}s (looked up
 * case-insensitively, matching the DieselDB column convention) or
 * {@link GenericRecord}s. The {@code avro.validation.mode} config key is
 * resolved from a system-property override, then {@code config.properties}
 * (honouring the {@code avro.schema.config.file} override shared with the
 * Prompt 71/72 schema classes), then the code default {@code permissive} —
 * mirroring {@link SchemaCompatibilityChecker#resolveCompatibilityMode()}.
 *
 * @since Prompt 73
 */
public final class AvroDataValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataValidator.class);

    /** Config key controlling the default validation mode (strict | permissive). */
    public static final String MODE_KEY = "avro.validation.mode";
    /** Config key controlling whether invalid rows are logged (on | off). */
    public static final String LOG_KEY = "avro.validation.log.invalid";

    /** Code-level default for {@link #MODE_KEY}. */
    public static final String DEFAULT_MODE = "permissive";
    /** Code-level default for {@link #LOG_KEY}. */
    public static final boolean DEFAULT_LOG_INVALID = true;

    private AvroDataValidator() { }

    // ─── Types and results ──────────────────────────────────────────

    /**
     * Validation policy. {@link #STRICT} aborts the write on the first invalid
     * row; {@link #PERMISSIVE} warns, continues and aggregates statistics.
     */
    public enum ValidationMode {
        /** Fail-fast: the first invalid row throws {@link AvroValidationException}. */
        STRICT,
        /** Log-and-continue: every row is scanned and statistics are collected. */
        PERMISSIVE
    }

    /**
     * Kind of validation failure for a single field.
     */
    public enum ErrorType {
        /** A non-nullable field received a {@code null} value. */
        NULL_NOT_ALLOWED,
        /** The value's Java type does not match the Avro field type. */
        TYPE_MISMATCH,
        /** A numeric value does not fit the target Avro type range (e.g. int). */
        VALUE_OUT_OF_RANGE,
        /** A field/value pair exists in the row but not in the schema. */
        EXTRA_FIELD,
        /** A String value is not a member of the Avro ENUM symbol set. */
        NOT_IN_ENUM
    }

    /**
     * A single field-level validation failure.
     *
     * @param fieldName   the schema field name (or {@code row[i]} for extras)
     * @param type        the failure kind
     * @param message     human-readable description
     * @param actualValue the offending value
     */
    public record FieldError(String fieldName, ErrorType type, String message, Object actualValue) {
    }

    /**
     * Immutable outcome of a single-row validation.
     *
     * @param valid  whether the row passed
     * @param errors the field errors when {@code valid} is false
     */
    public record ValidationResult(boolean valid, List<FieldError> errors) {

        /** Shortcut for {@code valid}. */
        public boolean isValid() {
            return valid;
        }
    }

    /**
     * Immutable outcome of a whole-dataset validation with statistics.
     *
     * @param allValid           {@code true} when every row passed
     * @param totalRows          number of rows validated
     * @param validRows          number of rows that passed
     * @param invalidRows        number of rows that failed
     * @param errorCounts        error counts by {@link ErrorType}
     * @param perFieldErrorCounts error counts by field name
     * @param failedRowIndexes   indexes of the invalid rows
     * @param summary            one-line human-readable verdict
     */
    public record DatasetValidationResult(
            boolean allValid,
            int totalRows,
            int validRows,
            int invalidRows,
            Map<ErrorType, Long> errorCounts,
            Map<String, Long> perFieldErrorCounts,
            List<Integer> failedRowIndexes,
            String summary) {

        /** Shortcut for {@code allValid}. */
        public boolean isAllValid() {
            return allValid;
        }
    }

    /**
     * Thrown by {@link ValidationMode#STRICT} validation when an invalid row is
     * encountered. Carries the failing row index and its field errors.
     */
    public static class AvroValidationException extends IllegalArgumentException {

        private final int rowIndex;
        private final List<FieldError> errors;
        private final ValidationMode mode;

        AvroValidationException(String message, int rowIndex, List<FieldError> errors, ValidationMode mode) {
            super(message);
            this.rowIndex = rowIndex;
            this.errors = List.copyOf(errors);
            this.mode = mode;
        }

        /** Index of the first invalid row, or {@code -1} when not applicable. */
        public int rowIndex() {
            return rowIndex;
        }

        /** The field errors of the offending row. */
        public List<FieldError> fieldErrors() {
            return errors;
        }

        /** The mode that produced the failure. */
        public ValidationMode mode() {
            return mode;
        }
    }

    // ─── Single-row validation ──────────────────────────────────────

    /**
     * Validates a positional {@code Object[]} row against a RECORD schema.
     * The value at index {@code i} is checked against field {@code i}; extra
     * trailing values are reported as {@link ErrorType#EXTRA_FIELD} and missing
     * trailing values are treated as {@code null}.
     *
     * @param row    the row to validate (may be {@code null} = all-null row)
     * @param schema the RECORD schema to validate against
     * @return the validation result (never {@code null})
     * @throws IllegalArgumentException when {@code schema} is {@code null} or not a RECORD
     */
    public static ValidationResult validateRow(Object[] row, Schema schema) {
        List<FieldError> errors = new ArrayList<>();
        checkRecord(schema);
        List<Schema.Field> fields = schema.getFields();
        int n = fields.size();
        for (int i = 0; i < n; i++) {
            Schema.Field field = fields.get(i);
            Object value = (row != null && i < row.length) ? row[i] : null;
            errors.addAll(checkField(field.name(), value, field.schema()));
        }
        if (row != null) {
            for (int i = n; i < row.length; i++) {
                errors.add(new FieldError("row[" + i + "]", ErrorType.EXTRA_FIELD,
                        "extra value at index " + i + " is not defined by the schema", row[i]));
            }
        }
        return new ValidationResult(errors.isEmpty(), List.copyOf(errors));
    }

    /**
     * Validates a name-keyed {@code Map} row against a RECORD schema. Keys are
     * matched to fields case-insensitively; keys absent from the schema are
     * reported as {@link ErrorType#EXTRA_FIELD} and missing fields are treated
     * as {@code null}.
     *
     * @param row    the row to validate (may be {@code null} = all-null row)
     * @param schema the RECORD schema to validate against
     * @return the validation result (never {@code null})
     * @throws IllegalArgumentException when {@code schema} is {@code null} or not a RECORD
     */
    public static ValidationResult validateRow(Map<String, Object> row, Schema schema) {
        List<FieldError> errors = new ArrayList<>();
        checkRecord(schema);
        Map<String, Object> lookup = new LinkedHashMap<>();
        if (row != null) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (entry.getKey() != null) {
                    lookup.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
                }
            }
        }
        List<Schema.Field> fields = schema.getFields();
        Set<String> knownFields = new HashSet<>(fields.size());
        for (Schema.Field field : fields) {
            knownFields.add(field.name().toLowerCase(Locale.ROOT));
        }
        for (Schema.Field field : fields) {
            Object value = lookup.get(field.name().toLowerCase(Locale.ROOT));
            errors.addAll(checkField(field.name(), value, field.schema()));
        }
        if (row != null) {
            for (String key : row.keySet()) {
                if (key == null) {
                    continue;
                }
                if (!knownFields.contains(key.toLowerCase(Locale.ROOT))) {
                    errors.add(new FieldError(key, ErrorType.EXTRA_FIELD,
                            "value for '" + key + "' is not defined by the schema", row.get(key)));
                }
            }
        }
        return new ValidationResult(errors.isEmpty(), List.copyOf(errors));
    }

    /**
     * Validates a {@link GenericRecord} against a RECORD schema (usually the
     * record's own schema, but any RECORD may be passed).
     *
     * @param record the record to validate (may be {@code null})
     * @param schema the RECORD schema to validate against
     * @return the validation result (never {@code null})
     * @throws IllegalArgumentException when {@code schema} is {@code null} or not a RECORD
     */
    public static ValidationResult validateRow(GenericRecord record, Schema schema) {
        List<FieldError> errors = new ArrayList<>();
        checkRecord(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = record != null ? record.get(field.name()) : null;
            errors.addAll(checkField(field.name(), value, field.schema()));
        }
        return new ValidationResult(errors.isEmpty(), List.copyOf(errors));
    }

    // ─── Dataset validation ─────────────────────────────────────────

    /**
     * Validates a whole dataset using the configured default mode
     * ({@link #resolveMode()}); the {@code avro.validation.log.invalid}
     * setting controls invalid-row logging.
     *
     * @param rows   the rows to validate ({@code Object[]}, {@code Map} or
     *               {@link GenericRecord} elements; may be {@code null})
     * @param schema the RECORD schema to validate against
     * @return the aggregated statistics report, or {@code null} for null input
     * @throws AvroValidationException in {@link ValidationMode#STRICT} on the first invalid row
     */
    public static DatasetValidationResult validateDataset(List<?> rows, Schema schema) {
        return validateDataset(rows, schema, resolveMode(), resolveLogInvalid());
    }

    /**
     * Validates a whole dataset under an explicit mode.
     *
     * @param rows        the rows to validate ({@code Object[]}, {@code Map} or
     *                    {@link GenericRecord} elements; may be {@code null})
     * @param schema      the RECORD schema to validate against
     * @param mode        validation policy; when {@code null} the configured default is used
     * @param logInvalid  whether invalid rows are logged
     * @return the aggregated statistics report, or {@code null} for null input
     * @throws AvroValidationException in {@link ValidationMode#STRICT} on the first invalid row
     * @throws IllegalArgumentException when {@code schema} is {@code null} or not a RECORD
     */
    public static DatasetValidationResult validateDataset(
            List<?> rows, Schema schema, ValidationMode mode, boolean logInvalid) {
        checkRecord(schema);
        if (rows == null) {
            return null;
        }
        ValidationMode effectiveMode = mode == null ? resolveMode() : mode;
        int total = rows.size();
        int validCount = 0;
        Map<ErrorType, Long> errorCounts = new LinkedHashMap<>();
        Map<String, Long> perFieldErrorCounts = new LinkedHashMap<>();
        List<Integer> failedIndexes = new ArrayList<>();

        for (int i = 0; i < total; i++) {
            ValidationResult result = validateOne(rows.get(i), schema);
            if (result.valid()) {
                validCount++;
                continue;
            }
            if (effectiveMode == ValidationMode.STRICT) {
                LOGGER.error("AVRO validation: strict mode failed at row {}: {}", i, renderErrors(result.errors()));
                throw new AvroValidationException(
                        "AVRO validation failed in strict mode at row " + i + ": " + renderErrors(result.errors()),
                        i, result.errors(), effectiveMode);
            }
            failedIndexes.add(i);
            for (FieldError error : result.errors()) {
                errorCounts.merge(error.type(), 1L, Long::sum);
                perFieldErrorCounts.merge(error.fieldName(), 1L, Long::sum);
            }
            if (logInvalid) {
                LOGGER.warn("AVRO validation: invalid row {}: {}", i, renderErrors(result.errors()));
            }
        }

        int invalid = failedIndexes.size();
        String summary = summary(total, validCount, invalid, errorCounts);
        if (invalid > 0 && logInvalid) {
            LOGGER.warn("AVRO validation: {}", summary);
        }
        return new DatasetValidationResult(
                invalid == 0,
                total,
                validCount,
                invalid,
                Collections.unmodifiableMap(errorCounts),
                Collections.unmodifiableMap(perFieldErrorCounts),
                List.copyOf(failedIndexes),
                summary);
    }

    /**
     * Validates a whole dataset under an explicit mode; invalid-row logging
     * follows {@link #resolveLogInvalid()}.
     */
    public static DatasetValidationResult validateDataset(List<?> rows, Schema schema, ValidationMode mode) {
        return validateDataset(rows, schema, mode, resolveLogInvalid());
    }

    /**
     * Throws an {@link AvroValidationException} when the report is not fully
     * valid. Lets a caller turn a permissive scan into a rejection decision.
     *
     * @param report the permissive-mode report (may be {@code null})
     * @return the report when fully valid
     * @throws AvroValidationException when {@code report} is not all-valid
     */
    public static DatasetValidationResult requireValid(DatasetValidationResult report) {
        if (report != null && !report.allValid()) {
            throw new AvroValidationException(report.summary(), -1,
                    fieldErrorsOf(report), ValidationMode.PERMISSIVE);
        }
        return report;
    }

    private static List<FieldError> fieldErrorsOf(DatasetValidationResult report) {
        List<FieldError> errors = new ArrayList<>();
        for (Map.Entry<ErrorType, Long> entry : report.errorCounts().entrySet()) {
            errors.add(new FieldError("*", entry.getKey(),
                    "count=" + entry.getValue() + " (type " + entry.getKey() + ")", null));
        }
        return errors;
    }

    // ─── Per-field checks ───────────────────────────────────────────

    /** Dispatches a single dataset element to the matching row validator. */
    private static ValidationResult validateOne(Object row, Schema schema) {
        if (row == null) {
            return validateRow((Object[]) null, schema);
        }
        if (row instanceof Object[] arrayRow) {
            return validateRow(arrayRow, schema);
        }
        if (row instanceof GenericRecord record) {
            return validateRow(record, schema);
        }
        if (row instanceof Map<?, ?> mapRow) {
            Map<String, Object> typed = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : mapRow.entrySet()) {
                if (entry.getKey() instanceof String key) {
                    typed.put(key, entry.getValue());
                }
            }
            return validateRow(typed, schema);
        }
        throw new IllegalArgumentException(
                "Unsupported row type for validation: " + row.getClass().getName());
    }

    private static List<FieldError> checkField(String fieldName, Object value, Schema fieldSchema) {
        if (value == null) {
            if (isNullable(fieldSchema)) {
                return List.of();
            }
            return List.of(new FieldError(fieldName, ErrorType.NULL_NOT_ALLOWED,
                    "field '" + fieldName + "' is not nullable but received null", null));
        }
        List<Schema> branches = nonNullBranches(fieldSchema);
        if (branches.isEmpty() || (branches.size() == 1 && branches.get(0).getType() == Schema.Type.NULL)) {
            return List.of(new FieldError(fieldName, ErrorType.TYPE_MISMATCH,
                    "field '" + fieldName + "' is NULL-typed but received a value", value));
        }
        for (Schema branch : branches) {
            FieldError failure = checkAgainstBranch(fieldName, value, branch);
            if (failure == null) {
                return List.of();
            }
        }
        if (branches.size() == 1) {
            return List.of(checkAgainstBranch(fieldName, value, branches.get(0)));
        }
        String expected = describe(fieldSchema);
        return List.of(new FieldError(fieldName, ErrorType.TYPE_MISMATCH,
                "field '" + fieldName + "' expects " + expected + " but received "
                        + describeValue(value), value));
    }

    private static FieldError checkAgainstBranch(String fieldName, Object value, Schema branch) {
        Schema.Type type = branch.getType();
        if (branch.getLogicalType() != null) {
            // a logical type on a primitive governs the accepted Java form:
            // e.g. only BigDecimal/String-for-decimal, LocalDate|Integer for date,
            // LocalDateTime|Long for timestamp, UUID|parseable String for uuid
            return checkLogicalType(fieldName, value, branch);
        }
        return switch (type) {
            case STRING -> value instanceof CharSequence ? null : mismatch(fieldName, value, SqlKeywords.TYPE_STRING);
            case INT -> checkInt(fieldName, value);
            case LONG -> value instanceof Number ? null : mismatch(fieldName, value, SqlKeywords.TYPE_LONG);
            case FLOAT, DOUBLE -> value instanceof Number ? null : mismatch(fieldName, value, type.name());
            case BOOLEAN -> value instanceof Boolean ? null : mismatch(fieldName, value, SqlKeywords.TYPE_BOOLEAN);
            case BYTES, FIXED ->
                    (value instanceof byte[] || value instanceof ByteBuffer)
                            ? null : mismatch(fieldName, value, type.name());
            case ENUM -> checkEnum(fieldName, value, branch);
            case RECORD -> (value instanceof GenericRecord || value instanceof Map)
                    ? null : mismatch(fieldName, value, "RECORD");
            case ARRAY -> (value instanceof Collection || value instanceof Object[])
                    ? null : mismatch(fieldName, value, "ARRAY");
            case MAP -> value instanceof Map ? null : mismatch(fieldName, value, "MAP");
            case NULL -> mismatch(fieldName, value, "NULL");
            default -> mismatch(fieldName, value, type.name());
        };
    }

    private static FieldError checkLogicalType(String fieldName, Object value, Schema branch) {
        return switch (branch.getLogicalType().getName()) {
            case "decimal" -> value instanceof BigDecimal ? null : mismatch(fieldName, value, "decimal");
            case "date" ->
                    (value instanceof LocalDate || value instanceof Integer)
                            ? null : mismatch(fieldName, value, "date");
            case "timestamp-millis", "timestamp-micros" ->
                    (value instanceof LocalDateTime || value instanceof Long)
                            ? null : mismatch(fieldName, value, "timestamp");
            case "uuid" -> checkUuid(fieldName, value);
            default -> null;
        };
    }

    private static FieldError checkUuid(String fieldName, Object value) {
        if (value instanceof UUID) {
            return null;
        }
        if (value instanceof CharSequence s) {
            try {
                UUID.fromString(s.toString());
                return null;
            } catch (IllegalArgumentException e) {
                return mismatch(fieldName, value, "uuid");
            }
        }
        return mismatch(fieldName, value, "uuid");
    }

    private static FieldError checkInt(String fieldName, Object value) {
        if (value instanceof Integer) {
            return null;
        }
        if (value instanceof Number n) {
            long l = n.longValue();
            return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE
                    ? null
                    : new FieldError(fieldName, ErrorType.VALUE_OUT_OF_RANGE,
                    "field '" + fieldName + "' is INT but the value " + l
                            + " is outside the int range", value);
        }
        return mismatch(fieldName, value, "INT");
    }

    private static FieldError checkEnum(String fieldName, Object value, Schema branch) {
        if (!(value instanceof CharSequence cs)) {
            return mismatch(fieldName, value, "ENUM");
        }
        if (branch.getEnumSymbols().stream().anyMatch(sym -> sym.equalsIgnoreCase(cs.toString()))) {
            return null;
        }
        return new FieldError(fieldName, ErrorType.NOT_IN_ENUM,
                "field '" + fieldName + "' value '" + cs + "' is not in the ENUM symbols "
                        + branch.getEnumSymbols(), value);
    }

    private static FieldError mismatch(String fieldName, Object value, String expected) {
        return new FieldError(fieldName, ErrorType.TYPE_MISMATCH,
                "field '" + fieldName + "' expects " + expected + " but received "
                        + describeValue(value), value);
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static String renderErrors(List<FieldError> errors) {
        StringBuilder sb = new StringBuilder();
        for (FieldError error : errors) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(error.fieldName()).append(": ").append(error.message());
        }
        return sb.toString();
    }

    private static String summary(int total, int valid, int invalid, Map<ErrorType, Long> errorCounts) {
        StringBuilder sb = new StringBuilder("validated ")
                .append(total).append(" row").append(total == 1 ? "" : "s")
                .append(": ").append(valid).append(" valid, ").append(invalid).append(" invalid");
        if (!errorCounts.isEmpty()) {
            sb.append(" (");
            int count = 0;
            for (Map.Entry<ErrorType, Long> entry : errorCounts.entrySet()) {
                if (count++ > 0) {
                    sb.append(", ");
                }
                sb.append(entry.getValue()).append(' ').append(entry.getKey());
            }
            sb.append(')');
        }
        return sb.toString();
    }

    private static String describe(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return "union " + schema.getTypes();
        }
        if (schema.getLogicalType() != null) {
            return "logical type " + schema.getLogicalType().getName();
        }
        return schema.getType().name();
    }

    private static String describeValue(Object value) {
        if (value == null) {
            return "null";
        }
        return value.getClass().getSimpleName() + " '" + value + "'";
    }

    private static boolean isNullable(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(b -> b.getType() == Schema.Type.NULL);
    }

    /** Returns the non-null union branches, or the schema itself when it is not a UNION. */
    private static List<Schema> nonNullBranches(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return List.of(schema);
        }
        List<Schema> result = new ArrayList<>();
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                result.add(branch);
            }
        }
        return result;
    }

    private static void checkRecord(Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema must not be null");
        }
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    "Validation schema must be a RECORD (got: " + schema.getType() + ")");
        }
    }

    // ─── Config resolution ──────────────────────────────────────────

    /**
     * Resolves the default validation mode: system property override, then
     * {@code config.properties} (override file honouring
     * {@code avro.schema.config.file}), then the code default
     * {@code permissive}. Unknown values fall back to permissive with a warning.
     *
     * @return the resolved mode (never {@code null})
     */
    public static ValidationMode resolveMode() {
        String raw = getString(MODE_KEY, DEFAULT_MODE);
        String key = raw == null || raw.isBlank() ? DEFAULT_MODE : raw.trim().toLowerCase(Locale.ROOT);
        switch (key) {
            case "strict":
                return ValidationMode.STRICT;
            case "permissive":
                return ValidationMode.PERMISSIVE;
            default:
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, MODE_KEY, raw, DEFAULT_MODE);
                return ValidationMode.PERMISSIVE;
        }
    }

    /**
     * Resolves whether invalid rows are logged: system property override, then
     * {@code config.properties}, then the code default {@code true}.
     *
     * @return whether invalid rows are logged
     */
    public static boolean resolveLogInvalid() {
        return getBoolean(LOG_KEY, DEFAULT_LOG_INVALID);
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        if (raw == null) {
            return defaultValue;
        }
        switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "on":
            case "true":
            case "yes":
                return true;
            case "off":
            case "false":
            case "no":
                return false;
            default:
                LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
                return defaultValue;
        }
    }

    private static String getString(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null) {
            return systemValue;
        }
        String prop = rootProps().getProperty(key);
        return prop == null ? defaultValue : prop;
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty("avro.schema.config.file");
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }
}