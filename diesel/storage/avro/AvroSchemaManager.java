package diesel.storage.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Manages Avro schemas for DieselDB tables: builds Avro RECORD schemas from
 * DieselDB column definitions, reads/writes {@code .avsc} sidecar files,
 * and validates schema compatibility.
 * <p>
 * The Avro schema namespace defaults to {@code "diesel.avro"}.
 * Schema files are stored alongside the data or in the configured schema directory.
 *
 * @since Prompt 58
 */
public final class AvroSchemaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaManager.class);

    /** Default Avro namespace for generated schemas. */
    private static final String DEFAULT_NAMESPACE = "diesel.avro";

    /** Config key for the schema source directory (from Prompt 57). */
    public static final String SCHEMA_PATH_KEY = "avro.schema.path";

    /** Default schema path. */
    private static final String DEFAULT_SCHEMA_PATH = "src/main/avro";

    /** File extension for Avro schema files. */
    public static final String AVSC_EXTENSION = ".avsc";

    private AvroSchemaManager() { }

    // ─── Build schema from DieselDB columns ─────────────────────────

    /**
     * Builds an Avro RECORD schema from DieselDB column definitions.
     * <p>
     * Each column becomes a field. Nullable columns use a
     * {@code ["null", type]} union.
     *
     * @param tableName   table name (used as the record name)
     * @param columns     ordered list of column names
     * @param columnTypes map of column name → Java class (case-insensitive)
     * @return the Avro RECORD schema
     * @throws IllegalArgumentException if a column type is unsupported
     */
    public static Schema buildTableSchema(String tableName,
                                          List<String> columns,
                                          Map<String, Class<?>> columnTypes) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name must not be blank");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Column list must not be empty for table: " + tableName);
        }

        String avroName = sanitizeName(tableName);
        List<Schema.Field> fields = new ArrayList<>(columns.size());
        Map<String, Class<?>> types = caseInsensitiveTypes(columnTypes);

        for (String col : columns) {
            Class<?> javaType = types.get(col);
            if (javaType == null) {
                throw new IllegalArgumentException(
                        "No type defined for column: " + col + " in table: " + tableName);
            }
            Schema fieldSchema = AvroTypeMapper.toAvroSchema(javaType, col);
            Schema.Field field = new Schema.Field(col, fieldSchema, null, null);
            fields.add(field);
        }

        Schema record = Schema.createRecord(avroName,
                "DieselDB table: " + tableName,
                DEFAULT_NAMESPACE,
                false);
        record.setFields(fields);
        LOGGER.debug("Built Avro schema for table '{}' with {} columns", tableName, columns.size());
        return record;
    }

    /**
     * Builds an Avro RECORD schema from DieselDB column definitions,
     * using explicit SQL type name strings instead of Java classes.
     *
     * @param tableName   table name
     * @param columns     ordered column names
     * @param typeNames   map of column name → SQL type name (e.g. "STRING", "INTEGER")
     * @return the Avro RECORD schema
     */
    public static Schema buildTableSchemaFromTypeNames(String tableName,
                                                       List<String> columns,
                                                       Map<String, String> typeNames) {
        Map<String, Class<?>> columnTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : typeNames.entrySet()) {
            Class<?> cls = AvroTypeMapper.typeClass(entry.getValue());
            if (cls == null) {
                throw new IllegalArgumentException(
                        "Unknown SQL type name: " + entry.getValue() + " for column: " + entry.getKey());
            }
            columnTypes.put(entry.getKey(), cls);
        }
        return buildTableSchema(tableName, columns, columnTypes);
    }

    // ─── Write / Read .avsc sidecar ─────────────────────────────────

    /**
     * Writes an Avro schema to a {@code .avsc} file.
     * Parent directories are created if missing.
     *
     * @param schema   the Avro schema to write
     * @param avscPath target file path
     * @throws IOException on I/O errors
     */
    public static void writeSchemaFile(Schema schema, Path avscPath) throws IOException {
        if (schema == null) {
            throw new IllegalArgumentException("Schema must not be null");
        }
        if (avscPath == null) {
            throw new IllegalArgumentException("Path must not be null");
        }
        Path parent = avscPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String json = schema.toString(true); // pretty-printed
        Files.writeString(avscPath, json, StandardCharsets.UTF_8);
        LOGGER.debug("Wrote Avro schema to {}", avscPath);
    }

    /**
     * Reads an Avro schema from a {@code .avsc} file.
     *
     * @param avscPath path to the schema file
     * @return the parsed Avro schema
     * @throws IOException on I/O errors or invalid schema
     */
    public static Schema readSchemaFile(Path avscPath) throws IOException {
        if (avscPath == null || !Files.exists(avscPath)) {
            throw new IOException("Schema file does not exist: " + avscPath);
        }
        String content = Files.readString(avscPath, StandardCharsets.UTF_8);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(content);
    }

    // ─── Resolve schema path ────────────────────────────────────────

    /**
     * Resolves the .avsc file path for a given table name.
     * Uses the configured {@code avro.schema.path} or the default {@code src/main/avro}.
     *
     * @param tableName table name
     * @return resolved Path to the .avsc file
     */
    public static Path resolveSchemaPath(String tableName) {
        String schemaDir = System.getProperty(SCHEMA_PATH_KEY);
        if (schemaDir == null || schemaDir.isBlank()) {
            schemaDir = System.getProperty("user.dir", ".");
            Path configPath = Paths.get(schemaDir, "config.properties");
            if (Files.exists(configPath)) {
                try {
                    var props = new java.util.Properties();
                    try (var in = Files.newInputStream(configPath)) {
                        props.load(in);
                    }
                    String configured = props.getProperty(SCHEMA_PATH_KEY);
                    if (configured != null && !configured.isBlank()) {
                        schemaDir = configured;
                    }
                } catch (IOException e) {
                    LOGGER.debug("Could not read config.properties for schema path", e);
                }
            }
            if (schemaDir.equals(".")) {
                schemaDir = DEFAULT_SCHEMA_PATH;
            }
        }
        return Paths.get(schemaDir, sanitizeName(tableName) + AVSC_EXTENSION);
    }

    // ─── Compatibility validation ───────────────────────────────────

    /**
     * Validates that an existing Avro schema is compatible with the current
     * DieselDB column definitions.
     *
     * @param existing   the existing Avro schema (e.g. from a .avsc file)
     * @param columns    current DieselDB column names
     * @param columnTypes current DieselDB column types
     * @return list of incompatibility descriptions (empty = fully compatible)
     */
    public static List<String> validateCompatibility(Schema existing,
                                                     List<String> columns,
                                                     Map<String, Class<?>> columnTypes) {
        List<String> issues = new ArrayList<>();
        if (existing == null) {
            issues.add("Existing schema is null");
            return issues;
        }
        if (existing.getType() != Schema.Type.RECORD) {
            issues.add("Existing schema is not a RECORD (got: " + existing.getType() + ")");
            return issues;
        }

        Map<String, Schema.Field> existingFields = new LinkedHashMap<>();
        for (Schema.Field f : existing.getFields()) {
            existingFields.put(f.name().toLowerCase(), f);
        }
        Map<String, Class<?>> types = caseInsensitiveTypes(columnTypes);
        Set<String> columnNames = new HashSet<>(columns.size());
        for (String col : columns) {
            columnNames.add(col.toLowerCase());
        }

        // Check columns in DieselDB schema against existing Avro fields
        for (String col : columns) {
            Schema.Field avroField = existingFields.get(col.toLowerCase());
            if (avroField == null) {
                issues.add("Column '" + col + "' exists in DieselDB but not in Avro schema");
                continue;
            }
            Class<?> expectedType = types.get(col);
            if (expectedType != null) {
                Class<?> actualJavaType = AvroTypeMapper.toJavaType(avroField.schema());
                if (actualJavaType != null && !expectedType.equals(actualJavaType)) {
                    issues.add("Column '" + col + "': type mismatch — DieselDB="
                            + expectedType.getSimpleName()
                            + ", Avro=" + actualJavaType.getSimpleName());
                }
            }
        }

        // Check fields in Avro that are missing from DieselDB columns
        for (Schema.Field f : existing.getFields()) {
            if (!columnNames.contains(f.name().toLowerCase())) {
                issues.add("Avro field '" + f.name() + "' exists but not in DieselDB columns");
            }
        }

        return issues;
    }

    // ─── Internals ──────────────────────────────────────────────────

    /**
     * Resolves the Java type for a column name from the type map (case-insensitive lookup).
     */
    private static Map<String, Class<?>> caseInsensitiveTypes(Map<String, Class<?>> columnTypes) {
        Map<String, Class<?>> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        types.putAll(columnTypes);
        return types;
    }

    /**
     * Sanitizes a table name into a valid Avro name (alphanumeric + underscore only).
     */
    public static String sanitizeName(String name) {
        if (name == null) {
            return "unknown";
        }
        StringBuilder sb = new StringBuilder(name.length());
        for (char c : name.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(c);
            } else {
                sb.append('_');
            }
        }
        // Avro names must start with a letter or underscore
        if (!sb.isEmpty() && Character.isDigit(sb.charAt(0))) {
            sb.insert(0, '_');
        }
        String result = sb.toString();
        return result.isEmpty() ? "unknown" : result;
    }
}
