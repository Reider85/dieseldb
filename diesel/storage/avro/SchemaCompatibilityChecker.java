package diesel.storage.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Avro schema evolution compatibility checker (Prompt 71).
 *
 * <p>Implements the three canonical Avro evolution rules over two versions of a
 * record schema — a <em>writer</em> schema (the schema the data was stored
 * with) and a <em>reader</em> schema (the schema the data is being read with):
 *
 * <ul>
 *   <li><b>Backward</b> — a reader using the <em>new</em> schema can read data
 *       written with the <em>old</em> schema. Fields added to the reader must
 *       have a default (or be nullable), so old records still decode.</li>
 *   <li><b>Forward</b> — a reader using the <em>old</em> schema can read data
 *       written with the <em>new</em> schema. Every reader field must still be
 *       present in the writer; fields the writer adds are simply ignored.</li>
 *   <li><b>Full</b> — backward <em>and</em> forward must both hold.</li>
 * </ul>
 *
 * <p>Field types are compared using Avro's widening rules
 * ({@code INT→LONG→FLOAT→DOUBLE} and {@code STRING↔BYTES}); nullable unions are
 * unwrapped before comparison so a {@code ["null", T]} writer field satisfies a
 * {@code ["null", U]} reader field whenever {@code T} promotes to {@code U}.
 * Field lookup is case-insensitive (matching the DieselDB column convention in
 * {@link AvroSchemaManager}) and honours field aliases.
 *
 * <p>The default check mode is read from the {@code avro.schema.compatibility.mode}
 * config key (system property override, then {@code config.properties}, then the
 * code default {@code BACKWARD}) — mirroring {@link AvroCompressionConfig}.
 *
 * @since Prompt 71
 */
public final class SchemaCompatibilityChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCompatibilityChecker.class);

    /** Config key for the default compatibility mode. */
    public static final String MODE_CONFIG_KEY = "avro.schema.compatibility.mode";

    /** Code-level default mode. */
    public static final String DEFAULT_MODE = "BACKWARD";

    private SchemaCompatibilityChecker() {
    }

    // ─── Modes and results ──────────────────────────────────────────

    /**
     * Compatibility check level: {@link #BACKWARD}, {@link #FORWARD},
     * {@link #FULL} (both) or {@link #NONE} (no constraints).
     */
    public enum CompatibilityMode {
        /** A new reader schema can read data written with the old writer schema. */
        BACKWARD,
        /** An old reader schema can read data written with the new writer schema. */
        FORWARD,
        /** Both backward and forward compatibility must hold. */
        FULL,
        /** No compatibility constraint is enforced. */
        NONE
    }

    /**
     * Severity of a single schema diff entry.
     */
    public enum CompatibilityResult {
        /** The field is fully compatible. */
        COMPATIBLE,
        /** The field breaks compatibility for the checked mode. */
        INCOMPATIBLE,
        /** The field is readable but deserves attention (e.g. dropped data). */
        WARNING
    }

    /**
     * A single field-level diff between a writer and a reader schema.
     *
     * @param fieldName the field name the diff concerns ({@code "*"} for record-level)
     * @param issue     human-readable description of the difference
     * @param severity  impact on compatibility
     */
    public record SchemaDiff(String fieldName, String issue, CompatibilityResult severity) {
    }

    /**
     * Immutable result of a compatibility check.
     *
     * @param mode       the checked mode
     * @param compatible whether the pair is compatible under that mode
     * @param diffs      per-field findings (empty means a clean pair)
     * @param summary    one-line human-readable verdict
     */
    public record CompatibilityReport(
            CompatibilityMode mode,
            boolean compatible,
            List<SchemaDiff> diffs,
            String summary) {

        /** Shortcut: {@code true} when no diff is {@link CompatibilityResult#INCOMPATIBLE}. */
        public boolean isCompatible() {
            return compatible;
        }
    }

    // ─── Public entry points ────────────────────────────────────────

    /**
     * Checks whether the {@code writer} schema can be read with the
     * {@code reader} schema under the given mode.
     *
     * @param writer the schema the data was written with (may be {@code null})
     * @param reader the schema the data is read with (may be {@code null})
     * @param mode   the compatibility level to enforce
     * @return an immutable report with per-field findings
     */
    public static CompatibilityReport checkCompatibility(Schema writer, Schema reader, CompatibilityMode mode) {
        if (mode == null || mode == CompatibilityMode.NONE) {
            return new CompatibilityReport(mode, true, List.of(), "no compatibility constraints enforced");
        }
        List<SchemaDiff> diffs = new ArrayList<>();
        List<String> guards = guardDiffs(writer, reader, mode, diffs);
        if (!guards.isEmpty()) {
            String summary = String.join("; ", guards);
            return new CompatibilityReport(mode, false, List.copyOf(diffs), summary);
        }
        boolean compatible;
        String summary;
        switch (mode) {
            case BACKWARD -> {
                compatible = isBackwardCompatible(writer, reader);
                summary = compatible
                        ? "reader schema can read data written with the writer schema (backward)"
                        : "reader schema CANNOT read data written with the writer schema (backward)";
            }
            case FORWARD -> {
                compatible = isForwardCompatible(writer, reader);
                summary = compatible
                        ? "reader schema can read data written with the writer schema (forward)"
                        : "reader schema CANNOT read data written with the writer schema (forward)";
            }
            case FULL -> {
                compatible = isFullyCompatible(writer, reader);
                summary = compatible
                        ? "reader and writer schemas are fully compatible (both directions)"
                        : "reader and writer schemas are NOT fully compatible";
            }
            default -> throw new IllegalStateException("Unexpected mode: " + mode);
        }
        diffs.addAll(collectDiffs(writer, reader));
        return new CompatibilityReport(mode, compatible, List.copyOf(diffs), summary);
    }

    /**
     * Backward compatibility: a new reader schema can read data written with
     * the old writer schema.
     *
     * @param writer the old schema the data was written with
     * @param reader the new schema the data is read with
     * @return {@code true} when every added reader field has a default and all
     * shared field types widen
     */
    public static boolean isBackwardCompatible(Schema writer, Schema reader) {
        if (writer == null || reader == null) {
            return true;
        }
        if (!isRecord(writer) || !isRecord(reader)) {
            return false;
        }
        Map<String, Schema.Field> writerFields = indexFields(writer);
        for (Schema.Field readerField : reader.getFields()) {
            Schema.Field writerField = lookup(writerFields, readerField);
            if (writerField == null) {
                if (!hasUsableDefault(readerField)) {
                    LOGGER.debug("Backward incompatible: reader field '{}' added without a default",
                            readerField.name());
                    return false;
                }
                continue;
            }
            if (!isTypePromotable(readerField.schema(), writerField.schema())) {
                LOGGER.debug("Backward incompatible: type of field '{}' does not widen "
                        + "({} -> {})", readerField.name(), writerField.schema(), readerField.schema());
                return false;
            }
        }
        return true;
    }

    /**
     * Forward compatibility: an old reader schema can read data written with
     * the new writer schema.
     *
     * @param writer the new schema the data was written with
     * @param reader the old schema the data is read with
     * @return {@code true} when every reader field is still present in the
     * writer and all shared field types widen
     */
    public static boolean isForwardCompatible(Schema writer, Schema reader) {
        if (writer == null || reader == null) {
            return true;
        }
        if (!isRecord(writer) || !isRecord(reader)) {
            return false;
        }
        Map<String, Schema.Field> writerFields = indexFields(writer);
        for (Schema.Field readerField : reader.getFields()) {
            Schema.Field writerField = lookup(writerFields, readerField);
            if (writerField == null) {
                LOGGER.debug("Forward incompatible: reader field '{}' is no longer present in the writer",
                        readerField.name());
                return false;
            }
            if (!isTypePromotable(readerField.schema(), writerField.schema())) {
                LOGGER.debug("Forward incompatible: type of field '{}' does not widen "
                        + "({} -> {})", readerField.name(), writerField.schema(), readerField.schema());
                return false;
            }
        }
        return true;
    }

    /**
     * Full compatibility: backward <em>and</em> forward compatibility hold at
     * the same time.
     *
     * @param writer the writer schema
     * @param reader the reader schema
     * @return {@code true} when the pair is compatible in both directions
     */
    public static boolean isFullyCompatible(Schema writer, Schema reader) {
        // backward: new reader reads old writer data;
        // forward: old reader reads new writer data — the arguments switch roles
        return isBackwardCompatible(writer, reader)
                && isForwardCompatible(reader, writer);
    }

    /**
     * Collects field-by-field differences between a writer and a reader schema.
     * Missing reader fields that carry a default, promotable type changes and
     * extra writer fields are reported as informative/warning entries; anything
     * that would fail a {@link CompatibilityMode#FULL} check is reported as
     * {@link CompatibilityResult#INCOMPATIBLE}.
     *
     * @param writer the writer schema
     * @param reader the reader schema
     * @return a mutable list of per-field diffs
     */
    public static List<SchemaDiff> collectDiffs(Schema writer, Schema reader) {
        List<SchemaDiff> diffs = new ArrayList<>();
        guardDiffs(writer, reader, CompatibilityMode.FULL, diffs);
        if (writer == null || reader == null || !isRecord(writer) || !isRecord(reader)) {
            return diffs;
        }
        Map<String, Schema.Field> writerFields = indexFields(writer);
        Map<String, Schema.Field> readerFields = indexFields(reader);

        for (Schema.Field readerField : reader.getFields()) {
            Schema.Field writerField = lookup(writerFields, readerField);
            if (writerField == null) {
                if (hasUsableDefault(readerField)) {
                    diffs.add(new SchemaDiff(readerField.name(),
                            "field present in reader only with a default (compatible)", CompatibilityResult.COMPATIBLE));
                } else {
                    diffs.add(new SchemaDiff(readerField.name(),
                            "field present in reader only and missing a default", CompatibilityResult.INCOMPATIBLE));
                }
                continue;
            }
            if (!isTypePromotable(readerField.schema(), writerField.schema())) {
                diffs.add(new SchemaDiff(readerField.name(),
                        "type mismatch — writer=" + writerField.schema() + ", reader=" + readerField.schema(),
                        CompatibilityResult.INCOMPATIBLE));
            }
        }
        for (Schema.Field writerField : writer.getFields()) {
            if (lookup(readerFields, writerField) == null) {
                diffs.add(new SchemaDiff(writerField.name(),
                        "field present in writer only, ignored by the reader", CompatibilityResult.WARNING));
            }
        }
        return diffs;
    }

    // ─── Type promotion ─────────────────────────────────────────────

    /**
     * Checks whether a reader field type can decode the values of a writer
     * field type (Avro widening rules, unions unwrapped).
     *
     * @param readerFieldSchema the field type the reader expects
     * @param writerFieldSchema the field type the data was written with
     * @return {@code true} when the writer type can be read as the reader type
     */
    public static boolean isTypePromotable(Schema readerFieldSchema, Schema writerFieldSchema) {
        if (readerFieldSchema == null || writerFieldSchema == null) {
            return false;
        }
        Schema r = unwrapUnion(readerFieldSchema);
        Schema w = unwrapUnion(writerFieldSchema);
        if (r == null && w == null) {
            return true; // both sides are plain NULL
        }
        if (r == null) {
            return false; // a NULL-only reader cannot hold a value
        }
        if (w == null) {
            return AvroTypeMapper.isNullable(readerFieldSchema); // writer wrote null — reader must accept it
        }
        Schema.Type rt = r.getType();
        Schema.Type wt = w.getType();
        if (rt == wt) {
            return true;
        }
        switch (wt) {
            case INT:    return rt == Schema.Type.LONG || rt == Schema.Type.FLOAT || rt == Schema.Type.DOUBLE;
            case LONG:   return rt == Schema.Type.FLOAT || rt == Schema.Type.DOUBLE;
            case FLOAT:  return rt == Schema.Type.DOUBLE;
            case STRING: return rt == Schema.Type.BYTES;
            case BYTES:  return rt == Schema.Type.STRING;
            default:     return false;
        }
    }

    // ─── Config resolution ──────────────────────────────────────────

    /**
     * Resolves the default compatibility mode: system property override, then
     * {@code config.properties}, then the code default {@code BACKWARD}.
     * Unknown values fall back to the default with a warning. The configuration
     * file is re-read on every call (matching {@link AvroBlockConfig}).
     *
     * @return the resolved mode (never {@code null})
     */
    public static CompatibilityMode resolveCompatibilityMode() {
        String raw = getString(MODE_CONFIG_KEY, DEFAULT_MODE);
        String key = raw == null || raw.isBlank() ? DEFAULT_MODE : raw.trim().toUpperCase(Locale.ROOT);
        switch (key) {
            case "BACKWARD":
                return CompatibilityMode.BACKWARD;
            case "FORWARD":
                return CompatibilityMode.FORWARD;
            case "FULL":
                return CompatibilityMode.FULL;
            case "NONE":
                return CompatibilityMode.NONE;
            default:
                LOGGER.warn("Invalid {} value '{}', using default {}", MODE_CONFIG_KEY, raw, DEFAULT_MODE);
                return CompatibilityMode.BACKWARD;
        }
    }

    // ─── Internals ──────────────────────────────────────────────────

    private static List<String> guardDiffs(
            Schema writer, Schema reader, CompatibilityMode mode, List<SchemaDiff> diffs) {
        List<String> guards = new ArrayList<>();
        if (writer == null && reader == null) {
            return guards;
        }
        if (writer == null) {
            guards.add("writer schema is null");
            diffs.add(new SchemaDiff("*", "writer schema is null", CompatibilityResult.INCOMPATIBLE));
            return guards;
        }
        if (reader == null) {
            guards.add("reader schema is null");
            diffs.add(new SchemaDiff("*", "reader schema is null", CompatibilityResult.INCOMPATIBLE));
            return guards;
        }
        if (!isRecord(writer)) {
            guards.add("writer schema is not a RECORD (got: " + writer.getType() + ")");
            diffs.add(new SchemaDiff("*", "writer schema is not a RECORD", CompatibilityResult.INCOMPATIBLE));
        }
        if (!isRecord(reader)) {
            guards.add("reader schema is not a RECORD (got: " + reader.getType() + ")");
            diffs.add(new SchemaDiff("*", "reader schema is not a RECORD", CompatibilityResult.INCOMPATIBLE));
        }
        if (mode == CompatibilityMode.FULL) {
            // record names must match (even though reading still works via aliases,
            // full evolution equivalence requires a single logical record)
            String wName = writer.getFullName();
            String rName = reader.getFullName();
            if (!wName.isEmpty() && !rName.isEmpty() && !wName.equals(rName)) {
                guards.add("record names differ: writer='" + wName + "', reader='" + rName + "'");
                diffs.add(new SchemaDiff("*",
                        "record names differ: writer='" + wName + "', reader='" + rName + "'",
                        CompatibilityResult.INCOMPATIBLE));
            }
        }
        return guards;
    }

    private static boolean isRecord(Schema schema) {
        return schema != null && schema.getType() == Schema.Type.RECORD;
    }

    private static boolean hasUsableDefault(Schema.Field field) {
        if (field.hasDefaultValue()) {
            return true;
        }
        // Avro treats ["null", T] reader fields as having an implicit null default
        Schema fieldSchema = field.schema();
        return fieldSchema != null
                && fieldSchema.getType() == Schema.Type.UNION
                && fieldSchema.getTypes().stream()
                .anyMatch(b -> b.getType() == Schema.Type.NULL);
    }

    /**
     * Unwraps a nullable union to its single non-null branch; returns {@code null}
     * when the schema is the bare NULL type, and the schema unchanged for
     * multi-branch unions.
     */
    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return null;
        }
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        Schema nonNull = null;
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                if (nonNull != null) {
                    return schema; // multi-branch union — compare as-is
                }
                nonNull = branch;
            }
        }
        return nonNull;
    }

    /** Indexes record fields by lower-case name for case-insensitive lookup. */
    private static Map<String, Schema.Field> indexFields(Schema record) {
        Map<String, Schema.Field> byName = new LinkedHashMap<>();
        for (Schema.Field field : record.getFields()) {
            byName.put(field.name().toLowerCase(Locale.ROOT), field);
        }
        return byName;
    }

    /**
     * Finds the writer field matching a reader field: by name first (case
     * insensitive), then by any alias of the reader field.
     */
    private static Schema.Field lookup(Map<String, Schema.Field> writerFields, Schema.Field readerField) {
        Schema.Field exact = writerFields.get(readerField.name().toLowerCase(Locale.ROOT));
        if (exact != null) {
            return exact;
        }
        for (String alias : readerField.aliases()) {
            Schema.Field byAlias = writerFields.get(alias.toLowerCase(Locale.ROOT));
            if (byAlias != null) {
                return byAlias;
            }
        }
        return null;
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
                LOGGER.debug("Could not read config.properties, using defaults: {}", ignored.getMessage());
            }
        }
        return props;
    }
}