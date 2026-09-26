package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Avro schema conflict resolution during schema evolution (Prompt 72).
 *
 * <p>Extends the detection-only {@link SchemaCompatibilityChecker} with an
 * <em>active</em> resolution layer. Given a <em>writer</em> (old) and a
 * <em>reader</em> (new) RECORD schema,
 * {@link #resolveConflicts(Schema, Schema, ResolutionConfig)} classifies every
 * difference into a {@link ConflictType}, applies a per-type
 * {@link ResolutionStrategy}, and returns an immutable {@link ResolutionResult}
 * holding the resolved conflicts, the concrete default values to apply for
 * added fields, and any warnings.
 *
 * <p>Four conflict-resolution rules are implemented:
 *
 * <ul>
 *   <li><b>Resolution rules</b> — each difference is classified
 *       (FIELD_ADDED / FIELD_REMOVED / TYPE_MISMATCH / TYPE_PROMOTED /
 *       FIELD_RENAMED) and mapped to a strategy (USE_DEFAULT / SKIP / FAIL /
 *       PROMOTE / RENAME). Any conflict whose strategy is {@link
 *       ResolutionStrategy#FAIL} makes the evolution unresolvable as configured
 *       ({@link ResolutionResult#resolved()} is {@code false}).</li>
 *   <li><b>Defaults for new fields</b> — a reader field absent from the writer
 *       is resolved with an explicit {@link ResolutionConfig#fieldDefaults}
 *       value, its own Avro default, or an implicit {@code null} for a nullable
 *       union. The resolved values are collected in
 *       {@link ResolutionResult#defaultValues()} and can be materialised into a
 *       record via {@link #applyDefaults(GenericRecord, Map)}.</li>
 *   <li><b>Ignoring removed fields</b> — writer fields absent from the reader
 *       are ignored by default ({@link #IGNORE_REMOVED_KEY}); when the flag is
 *       off they are still reported as SKIP conflicts so the caller can
 *       consciously drop the data.</li>
 *   <li><b>Renaming with aliases</b> — a reader field that matches a writer
 *       field through an Avro {@code aliases} entry or an explicit
 *       {@link ResolutionConfig#aliasMapping} ({@code oldName→newName}) is
 *       resolved as FIELD_RENAMED; {@link #buildAliasedSchema(Schema, Map)}
 *       produces a reader schema whose renamed fields carry the old names as
 *       aliases so Avro's own resolution matches the historical data.</li>
 * </ul>
 *
 * <p>The resolution policy is resolved from system properties then
 * {@code config.properties} (override file honouring
 * {@code avro.schema.config.file}) then code defaults, mirroring
 * {@link SchemaCompatibilityChecker#resolveCompatibilityMode()}.
 *
 * @since Prompt 72
 */
public final class SchemaConflictResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConflictResolver.class);

    /** Config key controlling whether writer-only (removed) fields listed as conflicts. */
    public static final String IGNORE_REMOVED_KEY = "avro.schema.conflict.ignore.removed";
    /** Config key controlling whether default values for added fields are applied. */
    public static final String USE_DEFAULTS_KEY = "avro.schema.conflict.use.defaults";
    /** Config key controlling whether alias-based rename detection is enabled. */
    public static final String ALLOW_ALIASES_KEY = "avro.schema.conflict.allow.aliases";
    /** Config key controlling whether unresolvable conflicts force a FAIL result. */
    public static final String STRICT_KEY = "avro.schema.conflict.strict";

    /** Code-level default for {@link #IGNORE_REMOVED_KEY}. */
    public static final boolean DEFAULT_IGNORE_REMOVED = true;
    /** Code-level default for {@link #USE_DEFAULTS_KEY}. */
    public static final boolean DEFAULT_USE_DEFAULTS = true;
    /** Code-level default for {@link #ALLOW_ALIASES_KEY}. */
    public static final boolean DEFAULT_ALLOW_ALIASES = true;
    /** Code-level default for {@link #STRICT_KEY}. */
    public static final boolean DEFAULT_STRICT = false;

    /** Sentinel meaning "no default value available" (an implicit null IS a default). */
    private static final Object NO_DEFAULT = new Object();

    private SchemaConflictResolver() {
    }

    // ─── Types and results ──────────────────────────────────────────

    /**
     * Kind of difference between a writer and a reader schema.
     */
    public enum ConflictType {
        /** A reader field is absent from the writer (added in the new schema). */
        FIELD_ADDED,
        /** A writer field is absent from the reader (removed in the new schema). */
        FIELD_REMOVED,
        /** A shared field type cannot decode the stored type (no promotion). */
        TYPE_MISMATCH,
        /** A shared field type changed but widens ({@code INT→LONG→FLOAT→DOUBLE}). */
        TYPE_PROMOTED,
        /** A reader field matched the writer through an alias (field renamed). */
        FIELD_RENAMED,
        /** No difference for this field. */
        COMPATIBLE
    }

    /**
     * Strategy applied to resolve a single conflict.
     */
    public enum ResolutionStrategy {
        /** The conflict is resolved by using a concrete default value. */
        USE_DEFAULT,
        /** The conflicting field is acknowledged and skipped (data dropped/ignored). */
        SKIP,
        /** The conflict cannot be resolved under the configured policy. */
        FAIL,
        /** The conflict is resolved by applying Avro type promotion. */
        PROMOTE,
        /** The conflict is resolved by mapping the field through an alias. */
        RENAME
    }

    /**
     * A single classified conflict between two schemas.
     *
     * @param fieldName      the field the conflict concerns
     * @param type           the classified conflict kind
     * @param writerField    the field in the writer schema ({@code null} for FIELD_ADDED)
     * @param readerField    the field in the reader schema ({@code null} for FIELD_REMOVED)
     * @param defaultValue   the resolved default value, or {@code null} when not applicable
     * @param strategy       the resolution strategy applied
     */
    public record FieldConflict(
            String fieldName,
            ConflictType type,
            Schema.Field writerField,
            Schema.Field readerField,
            Object defaultValue,
            ResolutionStrategy strategy) {
    }

    /**
     * Immutable outcome of {@link #resolveConflicts(Schema, Schema, ResolutionConfig)}.
     *
     * @param resolved      whether every conflict was resolved under the policy
     * @param conflicts     classification of every field difference (in reader
     *                      then writer order)
     * @param defaultValues reader field name → concrete default for added fields
     * @param warnings      human-readable notes for skipped/ignored fields
     */
    public record ResolutionResult(
            boolean resolved,
            List<FieldConflict> conflicts,
            Map<String, Object> defaultValues,
            List<String> warnings) {

        /** Shortcut for {@code resolved}. */
        public boolean isResolved() {
            return resolved;
        }
    }

    // ─── Resolution configuration ───────────────────────────────────

    /**
     * Mutable, builder-style policy controlling how conflicts are resolved.
     * Defaults mirror the code-level defaults of the {@code avro.schema.conflict.*}
     * keys. All fluent setters return {@code this} for chaining.
     */
    public static final class ResolutionConfig {

        private boolean ignoreRemovedFields = DEFAULT_IGNORE_REMOVED;
        private boolean useDefaultValues = DEFAULT_USE_DEFAULTS;
        private boolean allowAliases = DEFAULT_ALLOW_ALIASES;
        private boolean strict = DEFAULT_STRICT;
        private final Map<String, Object> fieldDefaults = new LinkedHashMap<>();
        private final Map<String, String> aliasMapping = new LinkedHashMap<>();

        /** Creates a config with the code-level defaults. */
        public ResolutionConfig() {
        }

        /** Creates a config copying another config's settings. */
        public ResolutionConfig(ResolutionConfig other) {
            Objects.requireNonNull(other, "other");
            this.ignoreRemovedFields = other.ignoreRemovedFields;
            this.useDefaultValues = other.useDefaultValues;
            this.allowAliases = other.allowAliases;
            this.strict = other.strict;
            this.fieldDefaults.putAll(other.fieldDefaults);
            this.aliasMapping.putAll(other.aliasMapping);
        }

        public boolean ignoreRemovedFields() {
            return ignoreRemovedFields;
        }

        public ResolutionConfig ignoreRemovedFields(boolean ignoreRemovedFields) {
            this.ignoreRemovedFields = ignoreRemovedFields;
            return this;
        }

        public boolean useDefaultValues() {
            return useDefaultValues;
        }

        public ResolutionConfig useDefaultValues(boolean useDefaultValues) {
            this.useDefaultValues = useDefaultValues;
            return this;
        }

        public boolean allowAliases() {
            return allowAliases;
        }

        public ResolutionConfig allowAliases(boolean allowAliases) {
            this.allowAliases = allowAliases;
            return this;
        }

        public boolean strict() {
            return strict;
        }

        public ResolutionConfig strict(boolean strict) {
            this.strict = strict;
            return this;
        }

        /** Explicit default values for reader fields absent from the writer (unmodifiable view). */
        public Map<String, Object> fieldDefaults() {
            return fieldDefaults;
        }

        /** Adds an explicit default value for a reader field that may be absent from the writer. */
        public ResolutionConfig withFieldDefault(String field, Object defaultValue) {
            this.fieldDefaults.put(field, defaultValue);
            return this;
        }

        /**
         * Maps an old writer field name to its new reader field name so rename
         * conflicts are detected even without Avro aliases on the reader field.
         */
        public ResolutionConfig withAlias(String oldName, String newName) {
            this.aliasMapping.put(oldName, newName);
            return this;
        }

        /** Explicit oldName→newName rename mapping (unmodifiable view). */
        public Map<String, String> aliasMapping() {
            return aliasMapping;
        }

        @Override
        public String toString() {
            return "ResolutionConfig{ignoreRemovedFields=" + ignoreRemovedFields
                    + ", useDefaultValues=" + useDefaultValues
                    + ", allowAliases=" + allowAliases
                    + ", strict=" + strict
                    + ", fieldDefaults=" + fieldDefaults.keySet()
                    + ", aliasMapping=" + aliasMapping + '}';
        }
    }

    // ─── Resolution entry points ────────────────────────────────────

    /**
     * Resolves conflicts using the resolved default policy
     * ({@link #resolveConfig()}).
     *
     * @param writer the schema the data was written with (must be a RECORD)
     * @param reader the schema the data is read with (must be a RECORD)
     * @return the resolution result
     */
    public static ResolutionResult resolveConflicts(Schema writer, Schema reader) {
        return resolveConflicts(writer, reader, resolveConfig());
    }

    /**
     * Resolves all field differences between a writer and a reader RECORD
     * schema under the given policy and returns the classified conflicts, the
     * default values to apply for added fields, and the resolved flag.
     *
     * @param writer the schema the data was written with
     * @param reader the schema the data is read with
     * @param config the resolution policy ({@code null} → resolved default)
     * @return the immutable resolution result
     * @throws IllegalArgumentException when either schema is {@code null} or not
     *                                  a RECORD
     */
    public static ResolutionResult resolveConflicts(
            Schema writer, Schema reader, ResolutionConfig config) {
        if (writer == null || reader == null) {
            throw new IllegalArgumentException("writer and reader schemas must not be null");
        }
        if (writer.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("writer schema must be a RECORD (got: " + writer.getType() + ")");
        }
        if (reader.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("reader schema must be a RECORD (got: " + reader.getType() + ")");
        }
        ResolutionConfig cfg = config == null ? resolveConfig() : config;

        List<FieldConflict> conflicts = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        Map<String, Object> defaults = new LinkedHashMap<>();
        Map<String, Schema.Field> writerByName = indexFields(writer);
        Map<String, Schema.Field> readerByName = indexFields(reader);

        for (Schema.Field readerField : reader.getFields()) {
            Schema.Field writerField = lookupWriter(writerByName, readerField, cfg);
            if (writerField == null) {
                handleAddedField(readerField, cfg, conflicts, warnings, defaults);
                continue;
            }
            boolean directlyNamed = writerField.name().equalsIgnoreCase(readerField.name());
            if (!directlyNamed && cfg.allowAliases()) {
                conflicts.add(new FieldConflict(readerField.name(), ConflictType.FIELD_RENAMED,
                        writerField, readerField, null, ResolutionStrategy.RENAME));
                LOGGER.debug("Renamed field '{}' resolved via alias from '{}'",
                        readerField.name(), writerField.name());
                continue;
            }
            if (SchemaCompatibilityChecker.isTypePromotable(readerField.schema(), writerField.schema())) {
                ConflictType type = readerField.schema().equals(writerField.schema())
                        ? ConflictType.COMPATIBLE
                        : ConflictType.TYPE_PROMOTED;
                conflicts.add(new FieldConflict(readerField.name(), type,
                        writerField, readerField, null, ResolutionStrategy.PROMOTE));
            } else {
                conflicts.add(new FieldConflict(readerField.name(), ConflictType.TYPE_MISMATCH,
                        writerField, readerField, null, ResolutionStrategy.FAIL));
                warnings.add("Field '" + readerField.name()
                        + "' type mismatch — writer=" + writerField.schema()
                        + ", reader=" + readerField.schema());
            }
        }

        for (Schema.Field writerField : writer.getFields()) {
            if (readerByName.get(writerField.name().toLowerCase(Locale.ROOT)) == null) {
                if (!cfg.ignoreRemovedFields()) {
                    conflicts.add(new FieldConflict(writerField.name(), ConflictType.FIELD_REMOVED,
                            writerField, null, null, ResolutionStrategy.SKIP));
                }
                warnings.add("Writer field '" + writerField.name()
                        + "' removed from the reader (data ignored)");
            }
        }

        boolean resolved = conflicts.stream().noneMatch(c -> c.strategy() == ResolutionStrategy.FAIL);
        return new ResolutionResult(resolved, List.copyOf(conflicts),
                Collections.unmodifiableMap(new LinkedHashMap<>(defaults)), List.copyOf(warnings));
    }

    // ─── Default values ─────────────────────────────────────────────

    /**
     * Computes the default value for every reader field absent from the writer:
     * the explicit map takes precedence, then the field's own Avro default, then
     * an implicit {@code null} for a nullable union. Fields without any default
     * are omitted.
     *
     * @param writer           the writer (old) schema
     * @param reader           the reader (new) schema
     * @param explicitDefaults overrides keyed by reader field name (may be {@code null})
     * @return field name → default value (unmodifiable)
     * @throws IllegalArgumentException when either schema is {@code null} or not a RECORD
     */
    public static Map<String, Object> resolveDefaults(
            Schema writer, Schema reader, Map<String, Object> explicitDefaults) {
        if (writer == null || reader == null) {
            throw new IllegalArgumentException("writer and reader schemas must not be null");
        }
        if (writer.getType() != Schema.Type.RECORD || reader.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("both schemas must be RECORD for default resolution");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Schema.Field> writerByName = indexFields(writer);
        Map<String, Schema.Field> readerByName = indexFields(reader);
        for (Schema.Field readerField : reader.getFields()) {
            if (writerByName.get(readerField.name().toLowerCase(Locale.ROOT)) != null) {
                continue;
            }
            if (readerField.aliases().iterator().hasNext()
                    && matchesAnyAlias(writerByName, readerByName, readerField)) {
                continue; // existing data will be found through the alias — no default needed
            }
            Object value = NO_DEFAULT;
            if (explicitDefaults != null && explicitDefaults.containsKey(readerField.name())) {
                value = explicitDefaults.get(readerField.name());
            } else if (readerField.hasDefaultValue()) {
                value = readerField.defaultVal();
            } else if (isNullableUnion(readerField.schema())) {
                value = null;
            }
            if (value != NO_DEFAULT) {
                result.put(readerField.name(), value);
            }
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(result));
    }

    /** Overload of {@link #resolveDefaults(Schema, Schema, Map)} with no overrides. */
    public static Map<String, Object> resolveDefaults(Schema writer, Schema reader) {
        return resolveDefaults(writer, reader, null);
    }

    /**
     * Fills concrete default values into a {@link GenericRecord} for fields not
     * already set, so a record read with the old writer schema can be decoded
     * with the new reader schema.
     *
     * @param record   the record to fill (may be {@code null})
     * @param defaults field name → default value (may be {@code null})
     * @return the same record, mutated in place
     */
    public static GenericRecord applyDefaults(GenericRecord record, Map<String, Object> defaults) {
        if (record != null && defaults != null) {
            for (Map.Entry<String, Object> e : defaults.entrySet()) {
                // GenericData.Record.hasField() only checks schema membership, so a
                // null value is the signal that the slot was never populated.
                if (record.get(e.getKey()) == null) {
                    record.put(e.getKey(), e.getValue());
                }
            }
        }
        return record;
    }

    // ─── Aliasing ───────────────────────────────────────────────────

    /**
     * Builds a copy of a reader RECORD schema in which every field listed in
     * {@code aliasMapping} (newFieldName → originalName) carries the original
     * name as an Avro {@code aliases} entry (existing aliases are preserved), so
     * Avro field resolution matches data still written under the old name.
     * Field defaults, order and properties are preserved through the Avro
     * {@code Schema.Field(Field, Schema)} copy constructor.
     *
     * @param reader       the reader RECORD schema whose renamed fields get aliases
     * @param aliasMapping newFieldName → oldFieldName (may be {@code null})
     * @return a new RECORD schema with the aliases applied
     * @throws IllegalArgumentException when {@code reader} is {@code null} or not a RECORD
     */
    public static Schema buildAliasedSchema(Schema reader, Map<String, String> aliasMapping) {
        if (reader == null || reader.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("reader schema must be a RECORD for aliasing");
        }
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field field : reader.getFields()) {
            // The Field(Field, Schema) copy constructor preserves name, doc, default,
            // order, properties and existing aliases.
            Schema.Field copy = new Schema.Field(field, field.schema());
            if (aliasMapping != null) {
                String original = aliasMapping.get(field.name());
                if (original != null && !original.isBlank() && !original.equalsIgnoreCase(field.name())) {
                    copy.addAlias(original);
                }
            }
            for (String existing : field.aliases()) {
                copy.addAlias(existing);
            }
            fields.add(copy);
        }
        Schema result = Schema.createRecord(
                reader.getName(), reader.getDoc(), reader.getNamespace(), reader.isError());
        result.setFields(fields);
        return result;
    }

    // ─── Config resolution ──────────────────────────────────────────

    /** Resolves {@link #IGNORE_REMOVED_KEY} (system property → config → default). */
    public static boolean resolveIgnoreRemoved() {
        return getBoolean(IGNORE_REMOVED_KEY, DEFAULT_IGNORE_REMOVED);
    }

    /** Resolves {@link #USE_DEFAULTS_KEY} (system property → config → default). */
    public static boolean resolveUseDefaults() {
        return getBoolean(USE_DEFAULTS_KEY, DEFAULT_USE_DEFAULTS);
    }

    /** Resolves {@link #ALLOW_ALIASES_KEY} (system property → config → default). */
    public static boolean resolveAllowAliases() {
        return getBoolean(ALLOW_ALIASES_KEY, DEFAULT_ALLOW_ALIASES);
    }

    /** Resolves {@link #STRICT_KEY} (system property → config → default). */
    public static boolean resolveStrict() {
        return getBoolean(STRICT_KEY, DEFAULT_STRICT);
    }

    /**
     * Builds a {@link ResolutionConfig} from the current resolved
     * {@code avro.schema.conflict.*} keys.
     *
     * @return a fresh config (never {@code null})
     */
    public static ResolutionConfig resolveConfig() {
        return new ResolutionConfig()
                .ignoreRemovedFields(resolveIgnoreRemoved())
                .useDefaultValues(resolveUseDefaults())
                .allowAliases(resolveAllowAliases())
                .strict(resolveStrict());
    }

    // ─── Internals ──────────────────────────────────────────────────

    private static void handleAddedField(
            Schema.Field readerField, ResolutionConfig cfg,
            List<FieldConflict> conflicts, List<String> warnings, Map<String, Object> defaults) {
        if (!cfg.useDefaultValues()) {
            conflicts.add(new FieldConflict(readerField.name(), ConflictType.FIELD_ADDED,
                    null, readerField, null, ResolutionStrategy.SKIP));
            warnings.add("Reader field '" + readerField.name()
                    + "' added without applying defaults (useDefaultValues=off)");
            return;
        }
        Object value = resolveAddedFieldDefault(readerField, cfg, defaults);
        if (value != NO_DEFAULT) {
            conflicts.add(new FieldConflict(readerField.name(), ConflictType.FIELD_ADDED,
                    null, readerField, value, ResolutionStrategy.USE_DEFAULT));
            LOGGER.debug("Added reader field '{}' resolved with default {}", readerField.name(), value);
        } else if (cfg.strict()) {
            conflicts.add(new FieldConflict(readerField.name(), ConflictType.FIELD_ADDED,
                    null, readerField, null, ResolutionStrategy.FAIL));
            warnings.add("Reader field '" + readerField.name()
                    + "' added without a usable default (strict mode)");
        } else {
            conflicts.add(new FieldConflict(readerField.name(), ConflictType.FIELD_ADDED,
                    null, readerField, null, ResolutionStrategy.SKIP));
            warnings.add("Reader field '" + readerField.name()
                    + "' added without a default (skipped)");
        }
    }

    private static Object resolveAddedFieldDefault(
            Schema.Field readerField, ResolutionConfig cfg, Map<String, Object> defaults) {
        if (cfg.fieldDefaults().containsKey(readerField.name())) {
            Object explicit = cfg.fieldDefaults().get(readerField.name());
            defaults.put(readerField.name(), explicit);
            return explicit;
        }
        if (readerField.hasDefaultValue()) {
            Object fieldDefault = readerField.defaultVal();
            defaults.put(readerField.name(), fieldDefault);
            return fieldDefault;
        }
        if (isNullableUnion(readerField.schema())) {
            defaults.put(readerField.name(), null);
            return null;
        }
        return NO_DEFAULT;
    }

    private static boolean matchesAnyAlias(
            Map<String, Schema.Field> writerByName,
            Map<String, Schema.Field> readerByName,
            Schema.Field readerField) {
        for (String alias : readerField.aliases()) {
            if (writerByName.containsKey(alias.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds the writer field matching a reader field: by name (case-insensitive),
     * then — when aliases are allowed — by the reader field's own Avro aliases,
     * then through the config's explicit oldName→newName mapping.
     */
    private static Schema.Field lookupWriter(
            Map<String, Schema.Field> writerByName, Schema.Field readerField, ResolutionConfig cfg) {
        Schema.Field direct = writerByName.get(readerField.name().toLowerCase(Locale.ROOT));
        if (direct != null) {
            return direct;
        }
        if (cfg.allowAliases()) {
            for (String alias : readerField.aliases()) {
                Schema.Field byAlias = writerByName.get(alias.toLowerCase(Locale.ROOT));
                if (byAlias != null) {
                    return byAlias;
                }
            }
            for (Map.Entry<String, String> entry : cfg.aliasMapping().entrySet()) {
                if (entry.getValue().equalsIgnoreCase(readerField.name())) {
                    Schema.Field byOldName = writerByName.get(entry.getKey().toLowerCase(Locale.ROOT));
                    if (byOldName != null) {
                        return byOldName;
                    }
                }
            }
        }
        return null;
    }

    /** Indexes record fields by lower-case name. */
    private static Map<String, Schema.Field> indexFields(Schema record) {
        Map<String, Schema.Field> byName = new LinkedHashMap<>();
        for (Schema.Field field : record.getFields()) {
            byName.put(field.name().toLowerCase(Locale.ROOT), field);
        }
        return byName;
    }

    /** {@code true} for a bare NULL type or a union containing NULL. */
    private static boolean isNullableUnion(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(b -> b.getType() == Schema.Type.NULL);
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, null);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "on":
            case "true":
            case "yes":
            case "1":
                return true;
            case "off":
            case "false":
            case "no":
            case "0":
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