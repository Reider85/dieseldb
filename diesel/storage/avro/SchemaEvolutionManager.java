package diesel.storage.avro;

import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Versioned Avro schema registry and evolution tracker (Prompt 71).
 *
 * <p>Holds the successive versions of a table's Avro schema as immutable
 * {@link SchemaVersion} entries, assigns monotonically increasing version
 * numbers on every evolution step, and gates each registration through the
 * configured {@link SchemaCompatibilityChecker.CompatibilityMode} so the
 * history never contains a schema that breaks evolution.
 *
 * <p>History can be persisted as a JSON array to a file via
 * {@link #writeVersionHistory(Path)} and restored via {@link #readVersionHistory(Path)}.
 * The default history file name derives from the table name
 * ({@code <table>.schema-history.json}); the location can be overridden with the
 * {@code avro.schema.evolution.history.file} config key.
 *
 * <p>The default compatibility mode is resolved per call from the
 * {@code avro.schema.compatibility.mode} key (see
 * {@link SchemaCompatibilityChecker#resolveCompatibilityMode()}).
 *
 * @since Prompt 71
 */
public final class SchemaEvolutionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaEvolutionManager.class);

    /** Config key for the history file template ({@code {table}} is expanded). */
    public static final String HISTORY_FILE_KEY = "avro.schema.evolution.history.file";

    /** Default history file name for a table (placeholder for the sanitized name). */
    public static final String DEFAULT_HISTORY_FILE = "{table}.schema-history.json";

    /** Top-level JSON property of a registered entry. */
    private static final String PROP_VERSION = "version";
    private static final String PROP_TIMESTAMP_MS = "timestampMs";
    private static final String PROP_DESCRIPTION = "description";
    private static final String PROP_SCHEMA = "schema";

    private final String tableName;
    private final List<SchemaVersion> versions = new ArrayList<>();
    private SchemaCompatibilityChecker.CompatibilityMode mode;

    /**
     * A single versioned schema entry.
     *
     * @param schema      the Avro RECORD schema at this version
     * @param version     the 1-based monotonically increasing version number
     * @param timestampMs epoch milliseconds of registration
     * @param description human-readable evolution description
     */
    public record SchemaVersion(Schema schema, int version, long timestampMs, String description) {
    }

    /**
     * Creates a manager with no table context, using the resolved default mode.
     */
    public SchemaEvolutionManager() {
        this(null);
    }

    /**
     * Creates a manager for a table, using the resolved default mode.
     *
     * @param tableName the table this registry tracks (used for the default
     *                  history file name; may be {@code null})
     */
    public SchemaEvolutionManager(String tableName) {
        this.tableName = tableName;
        this.mode = SchemaCompatibilityChecker.resolveCompatibilityMode();
    }

    /**
     * Creates a manager with an explicit compatibility mode.
     *
     * @param tableName the table this registry tracks (may be {@code null})
     * @param mode      the compatibility level enforced on {@link #registerSchema}
     */
    public SchemaEvolutionManager(String tableName, SchemaCompatibilityChecker.CompatibilityMode mode) {
        this.tableName = tableName;
        this.mode = mode == null ? SchemaCompatibilityChecker.resolveCompatibilityMode() : mode;
    }

    // ─── Registration / evolution ───────────────────────────────────

    /**
     * Registers a new schema version, validating it against the latest
     * registered version under the current compatibility mode first.
     *
     * @param schema      the new Avro RECORD schema
     * @param description evolution description
     * @return the registered version entry
     * @throws IllegalArgumentException when the schema is {@code null}, not a
     *                                  RECORD, or incompatible with the latest
     *                                  registered version under the current mode
     */
    public SchemaVersion registerSchema(Schema schema, String description) {
        return registerSchema(schema, description, false);
    }

    /**
     * Registers a new schema version, optionally bypassing the configured
     * compatibility gate.
     *
     * @param schema      the new Avro RECORD schema
     * @param description evolution description
     * @param force       when {@code true}, register even if the schema breaks
     *                    the configured compatibility mode
     * @return the registered version entry
     * @throws IllegalArgumentException when the schema is {@code null} or not a
     *                                  RECORD, or (unless {@code force}) incompatible
     *                                  with the latest registered version
     */
    public SchemaVersion registerSchema(Schema schema, String description, boolean force) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema must not be null");
        }
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    "Schema must be a RECORD for schema evolution (got: " + schema.getType() + ")");
        }
        SchemaVersion latest = getLatestVersion();
        if (!force && latest != null) {
            SchemaCompatibilityChecker.CompatibilityReport report =
                    SchemaCompatibilityChecker.checkCompatibility(latest.schema(), schema, mode);
            if (!report.compatible()) {
                throw new IllegalArgumentException(
                        "Schema version " + (latest.version() + 1)
                                + " breaks " + mode + " compatibility against version "
                                + latest.version() + ": " + report.summary());
            }
        }
        int version = latest == null ? 1 : latest.version() + 1;
        SchemaVersion entry = new SchemaVersion(schema, version, System.currentTimeMillis(), description);
        versions.add(entry);
        LOGGER.debug("Registered schema version {} for table '{}', mode {}",
                version, tableName, mode);
        return entry;
    }

    /**
     * Evolves the schema: alias of {@link #registerSchema(Schema, String)} that
     * communicates intent in evolution pipelines.
     */
    public SchemaVersion evolveSchema(Schema newSchema, String description) {
        return registerSchema(newSchema, description);
    }

    // ─── Query ──────────────────────────────────────────────────────

    /** Returns the most recently registered version, or {@code null} if none. */
    public SchemaVersion getLatestVersion() {
        return versions.isEmpty() ? null : versions.get(versions.size() - 1);
    }

    /** Returns the version with the given number, or {@code null} if unknown. */
    public SchemaVersion getVersion(int version) {
        if (version < 1 || version > versions.size()) {
            return null;
        }
        return versions.get(version - 1);
    }

    /** Returns the number of registered versions. */
    public int getVersionCount() {
        return versions.size();
    }

    /** Returns all registered versions in registration order (unmodifiable). */
    public List<SchemaVersion> getAllVersions() {
        return Collections.unmodifiableList(new ArrayList<>(versions));
    }

    /**
     * Returns the intermediate evolution steps strictly after {@code fromVersion}
     * up to {@code toVersion} inclusive, in ascending version order. An empty
     * list is returned when no step falls in that range.
     *
     * @param fromVersion lower bound (exclusive)
     * @param toVersion   upper bound (inclusive)
     * @return ordered list of intermediate versions
     * @throws IllegalArgumentException when {@code toVersion} is below
     *                                  {@code fromVersion} or out of range
     */
    public List<SchemaVersion> getEvolutionPath(int fromVersion, int toVersion) {
        if (fromVersion < 0) {
            throw new IllegalArgumentException("fromVersion must be >= 0, got " + fromVersion);
        }
        if (toVersion < fromVersion) {
            throw new IllegalArgumentException(
                    "toVersion must be >= fromVersion (fromVersion=" + fromVersion
                            + ", toVersion=" + toVersion + ")");
        }
        if (toVersion > versions.size()) {
            throw new IllegalArgumentException(
                    "toVersion " + toVersion + " exceeds registered version count " + versions.size());
        }
        List<SchemaVersion> path = new ArrayList<>();
        for (int v = fromVersion + 1; v <= toVersion; v++) {
            path.add(versions.get(v - 1));
        }
        return path;
    }

    // ─── Validation ─────────────────────────────────────────────────

    /**
     * Delegates to {@link SchemaCompatibilityChecker#checkCompatibility} using
     * the internal default mode of the checker (independent of this manager's
     * configured mode).
     *
     * @param writer the writer schema
     * @param reader the reader schema
     * @param mode   the compatibility level to enforce
     * @return the compatibility report
     */
    public SchemaCompatibilityChecker.CompatibilityReport validateEvolution(
            Schema writer, Schema reader, SchemaCompatibilityChecker.CompatibilityMode mode) {
        return SchemaCompatibilityChecker.checkCompatibility(writer, reader, mode);
    }

    // ─── Mode ───────────────────────────────────────────────────────

    /** Returns the compatibility mode enforced on new registrations. */
    public SchemaCompatibilityChecker.CompatibilityMode getCompatibilityMode() {
        return mode;
    }

    /** Sets the compatibility mode enforced on new registrations. */
    public void setCompatibilityMode(SchemaCompatibilityChecker.CompatibilityMode mode) {
        this.mode = mode == null ? SchemaCompatibilityChecker.resolveCompatibilityMode() : mode;
    }

    // ─── Persistence ────────────────────────────────────────────────

    /**
     * Resolves the default history file path for this manager's table:
     * the {@code avro.schema.evolution.history.file} config override
     * (a {@code {table}} placeholder is expanded with the sanitized table name),
     * else {@code <table>.schema-history.json} in the working directory
     * ({@code schema-history.json} when no table was given).
     *
     * @return the resolved default path
     */
    public Path defaultHistoryFile() {
        String configured = getString(HISTORY_FILE_KEY, null);
        String sanitized = tableName == null ? "" : AvroSchemaManager.sanitizeName(tableName);
        if (configured != null && !configured.isBlank()) {
            String expanded = configured.replace("{table}", sanitized);
            return Path.of(expanded);
        }
        String name = sanitized.isEmpty() ? "schema-history.json" : sanitized + ".schema-history.json";
        return Path.of(name);
    }

    /**
     * Writes the full version history as a JSON array to the given file.
     *
     * @param file target path (parent directories are created if missing)
     * @throws IOException on I/O errors
     */
    public void writeVersionHistory(Path file) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("History file must not be null");
        }
        Path parent = file.getParent();
        if (parent != null) {
            java.nio.file.Files.createDirectories(parent);
        }
        JsonParserConfig config = JsonParserConfig.defaults();
        try (JsonStreamGenerator gen = JsonStreams.createGenerator(
                java.nio.file.Files.newBufferedWriter(file, StandardCharsets.UTF_8), config)) {
            gen.writeStartArray();
            for (SchemaVersion entry : versions) {
                gen.writeStartObject();
                gen.writeFieldName(PROP_VERSION);
                gen.writeNumber(entry.version());
                gen.writeFieldName(PROP_TIMESTAMP_MS);
                gen.writeNumber(entry.timestampMs());
                gen.writeFieldName(PROP_DESCRIPTION);
                gen.writeString(entry.description() == null ? "" : entry.description());
                gen.writeFieldName(PROP_SCHEMA);
                String schemaJson = entry.schema().toString();
                try (JsonStreamParser schemaParser = JsonStreams.createParser(schemaJson, config)) {
                    if (schemaParser.nextToken() == null) {
                        throw new IOException("Failed to serialize schema for history entry");
                    }
                    gen.copyCurrentStructure(schemaParser);
                }
                gen.writeEndObject();
            }
            gen.writeEndArray();
        }
        LOGGER.debug("Wrote schema version history ({} versions) to {}", versions.size(), file);
    }

    /**
     * Reads a version history JSON file back into a new manager. Previously
     * registered versions are untouched; a read must be applied onto a fresh
     * manager for a complete history.
     *
     * @param file the history JSON file
     * @return a new manager pre-populated with the persisted versions
     * @throws IOException on I/O errors or a malformed history file
     */
    public static SchemaEvolutionManager readVersionHistory(Path file) throws IOException {
        if (file == null || !java.nio.file.Files.exists(file)) {
            throw new IOException("Schema version history file does not exist: " + file);
        }
        SchemaEvolutionManager manager = new SchemaEvolutionManager();
        JsonParserConfig config = JsonParserConfig.defaults();
        try (JsonStreamParser parser = JsonStreams.createParser(
                java.nio.file.Files.newBufferedReader(file, StandardCharsets.UTF_8), config)) {
            if (parser.nextToken() != JsonEvent.START_ARRAY) {
                throw new IOException("Malformed schema version history: expected a JSON array in " + file);
            }
            JsonEvent event;
            while ((event = parser.nextToken()) == JsonEvent.START_OBJECT) {
                manager.versions.add(parseVersion(parser, file));
            }
            if (event != JsonEvent.END_ARRAY) {
                throw new IOException("Malformed schema version history in " + file);
            }
        }
        LOGGER.debug("Read schema version history ({} versions) from {}", manager.versions.size(), file);
        return manager;
    }

    private static SchemaVersion parseVersion(JsonStreamParser parser, Path file) throws IOException {
        int version = -1;
        long timestampMs = -1L;
        String description = "";
        Schema schema = null;
        JsonEvent event;
        while ((event = parser.nextToken()) != JsonEvent.END_OBJECT) {
            if (event != JsonEvent.FIELD_NAME) {
                throw new IOException("Malformed schema version history in " + file);
            }
            String field = parser.currentName();
            parser.nextToken();
            switch (field) {
                case PROP_VERSION -> version = (int) parser.getLongValue();
                case PROP_TIMESTAMP_MS -> timestampMs = parser.getLongValue();
                case PROP_DESCRIPTION -> description = parser.getText();
                case PROP_SCHEMA -> schema = new Schema.Parser().parse(copyJsonValue(parser));
                default -> {
                    // tolerate forward-dated fields
                }
            }
        }
        if (version < 1 || schema == null) {
            throw new IOException("Malformed schema version history entry (missing version/schema) in " + file);
        }
        return new SchemaVersion(schema, version, timestampMs, description);
    }

    // ─── Config helpers ─────────────────────────────────────────────

    /**
     * Copies the JSON value the parser is currently positioned at (object,
     * array, string, number, boolean or null) back into verbatim JSON text,
     * consuming tokens through the matching END event. Used to extract the Avro
     * schema field without a databind codec.
     */
    private static String copyJsonValue(JsonStreamParser parser) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        copyStructure(parser, sb);
        return sb.toString();
    }

    private static void copyStructure(JsonStreamParser parser, StringBuilder sb) throws IOException {
        JsonEvent event = parser.currentEvent();
        switch (event) {
            case START_OBJECT -> {
                sb.append('{');
                boolean first = true;
                JsonEvent token;
                while ((token = parser.nextToken()) != JsonEvent.END_OBJECT) {
                    if (token != JsonEvent.FIELD_NAME) {
                        throw new IOException("Malformed object structure while reading schema value");
                    }
                    String name = parser.currentName();
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    sb.append('"').append(escapeJson(name)).append("\":");
                    parser.nextToken();
                    copyStructure(parser, sb);
                }
                sb.append('}');
            }
            case START_ARRAY -> {
                sb.append('[');
                boolean first = true;
                JsonEvent token;
                while ((token = parser.nextToken()) != JsonEvent.END_ARRAY) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    copyStructure(parser, sb);
                }
                sb.append(']');
            }
            default -> appendValue(parser, event, sb);
        }
    }

    private static void appendValue(JsonStreamParser parser, JsonEvent event, StringBuilder sb) throws IOException {
        switch (event) {
            case START_OBJECT -> sb.append('{');
            case END_OBJECT -> sb.append('}');
            case START_ARRAY -> sb.append('[');
            case END_ARRAY -> sb.append(']');
            case VALUE_STRING -> sb.append('"').append(escapeJson(parser.getText())).append('"');
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> sb.append(parser.getText());
            case VALUE_TRUE -> sb.append("true");
            case VALUE_FALSE -> sb.append("false");
            case VALUE_NULL -> sb.append("null");
            default -> throw new IOException("Unexpected JSON event '" + event + "' while reading schema value");
        }
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
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

    @Override
    public String toString() {
        return "SchemaEvolutionManager{table='" + tableName + "', versions=" + versions.size()
                + ", mode=" + mode + '}';
    }
}