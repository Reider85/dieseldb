package diesel.storage.avro;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable DieselDB metadata stored in the Avro object-container file header
 * (Prompt 76).
 *
 * <p>An Avro data file header carries a key/value metadata map next to the
 * mandatory {@code avro.schema} and {@code avro.codec} entries. This class
 * wraps the DieselDB-owned subset of that map — engine, database, table name,
 * file-format version, schema version, creation timestamp and the compression
 * codec/level that produced the file — and provides the two directions of the
 * boundary:
 *
 * <ul>
 *   <li>{@link #toMetaMap()} turns the header into the binary
 *       {@code Map<String, byte[]>} that
 *       {@link org.apache.avro.file.DataFileWriter#setMeta(String, byte[])}
 *       expects before {@code create(...)}; {@link AvroDataFileWriter} writes
 *       it into the file header.</li>
 *   <li>{@link #fromMetaMap(Map)} reconstructs a header from the raw metadata
 *       map parsed out of a file's header; absent DieselDB keys fall back to
 *       defaults so files written before Prompt 76 (which only carry
 *       {@code avro.schema} / {@code avro.codec}) remain readable.</li>
 * </ul>
 *
 * <p>{@link #validate()} / {@link #requireValid()} enforce the read-side rules:
 * the file-format version must not be newer than the one this engine supports,
 * and any present DieselDB metadata has to be well-formed. These fail fast on a
 * header from a newer/foreign engine or on corrupted metadata instead of
 * silently mis-parsing the data.
 *
 * @since Prompt 76
 */
public final class AvroFileHeader {

    // ─── Metadata keys ──────────────────────────────────────────────

    /** Engine name (always written by this engine). */
    public static final String KEY_ENGINE = "diesel.engine";
    /** Database name the file belongs to. */
    public static final String KEY_DATABASE = "diesel.database";
    /** Sanitized table name the file belongs to. */
    public static final String KEY_TABLE = "diesel.table";
    /** File-format version of this metadata layout (semver "major.minor"). */
    public static final String KEY_FORMAT_VERSION = "diesel.format.version";
    /** Schema version of the table schema used to write the file. */
    public static final String KEY_SCHEMA_VERSION = "diesel.schema.version";
    /** ISO-8601 UTC creation timestamp. */
    public static final String KEY_CREATION_TIMESTAMP = "diesel.creation.timestamp";
    /** Compression codec name (mirrors {@code avro.codec} under a DieselDB key). */
    public static final String KEY_COMPRESSION_CODEC = "diesel.compression.codec";
    /** Compression level used when the file was written. */
    public static final String KEY_COMPRESSION_LEVEL = "diesel.compression.level";

    /** All DieselDB-owned metadata keys, for completeness checks. */
    public static final List<String> META_KEYS = List.of(
            KEY_ENGINE, KEY_DATABASE, KEY_TABLE, KEY_FORMAT_VERSION,
            KEY_SCHEMA_VERSION, KEY_CREATION_TIMESTAMP,
            KEY_COMPRESSION_CODEC, KEY_COMPRESSION_LEVEL);

    // ─── Defaults ───────────────────────────────────────────────────

    /** Name of the engine that writes DieselDB Avro files. */
    public static final String ENGINE_NAME = "DieselDB";
    /** Supported file-format version of this metadata layout. */
    public static final String FORMAT_VERSION = "1.0";
    /** Default schema version when the key is absent. */
    public static final int DEFAULT_SCHEMA_VERSION = 1;
    /** Default compression codec when the key is absent. */
    public static final String DEFAULT_COMPRESSION_CODEC = "null";
    /** Default compression level when the key is absent. */
    public static final int DEFAULT_COMPRESSION_LEVEL = -1;

    private final String engine;
    private final String database;
    private final String tableName;
    private final String formatVersion;
    private final int schemaVersion;
    private final Instant creationTimestamp;
    private final String compressionCodec;
    private final int compressionLevel;
    private final boolean metadataKeysPresent;

    private AvroFileHeader(String engine, String database, String tableName,
                           String formatVersion, int schemaVersion,
                           Instant creationTimestamp, String compressionCodec,
                           int compressionLevel, boolean metadataKeysPresent) {
        this.engine = engine;
        this.database = database;
        this.tableName = tableName;
        this.formatVersion = formatVersion;
        this.schemaVersion = schemaVersion;
        this.creationTimestamp = creationTimestamp;
        this.compressionCodec = compressionCodec;
        this.compressionLevel = compressionLevel;
        this.metadataKeysPresent = metadataKeysPresent;
    }

    // ─── Accessors ──────────────────────────────────────────────────

    /** Returns the engine name, or {@code null} when not recorded. */
    public String engine() {
        return engine;
    }

    /** Returns the database name, or {@code null} when not recorded. */
    public String database() {
        return database;
    }

    /** Returns the sanitized table name, or {@code null} when not recorded. */
    public String tableName() {
        return tableName;
    }

    /** Returns the file-format version, or {@code null} when not recorded. */
    public String formatVersion() {
        return formatVersion;
    }

    /** Returns the schema version, defaulting to 1 when not recorded. */
    public int schemaVersion() {
        return schemaVersion;
    }

    /** Returns the creation timestamp, or {@code null} when not recorded. */
    public Instant creationTimestamp() {
        return creationTimestamp;
    }

    /** Returns the compression codec name, defaulting to {@code "null"}. */
    public String compressionCodec() {
        return compressionCodec;
    }

    /** Returns the compression level, defaulting to {@code -1} (codec default). */
    public int compressionLevel() {
        return compressionLevel;
    }

    /**
     * Returns {@code true} when this header was reconstructed from a metadata
     * map that carried at least one DieselDB-owned key (i.e. it was written by a
     * Prompt-76-aware engine). Pre-76 files have no DieselDB keys and report
     * {@code false}.
     */
    public boolean hasDieselMetadata() {
        return metadataKeysPresent;
    }

    // ─── Serialization ──────────────────────────────────────────────

    /**
     * Serializes this header to the binary metadata map written by
     * {@link org.apache.avro.file.DataFileWriter#setMeta} into the file header.
     * {@code null} values except {@link #compressionCodec()} and
     * {@link #compressionLevel()} are omitted; those two always serialize so a
     * round-trip through {@link #fromMetaMap(Map)} is stable.
     *
     * @return a mutable map of UTF-8 encoded key/value pairs (never {@code null})
     */
    public Map<String, byte[]> toMetaMap() {
        Map<String, byte[]> map = new LinkedHashMap<>();
        put(map, KEY_ENGINE, engine);
        put(map, KEY_DATABASE, database);
        put(map, KEY_TABLE, tableName);
        put(map, KEY_FORMAT_VERSION, formatVersion);
        if (schemaVersion > 0) {
            put(map, KEY_SCHEMA_VERSION, Integer.toString(schemaVersion));
        }
        if (creationTimestamp != null) {
            put(map, KEY_CREATION_TIMESTAMP, creationTimestamp.toString());
        }
        map.put(KEY_COMPRESSION_CODEC, bytes(compressionCodec == null ? DEFAULT_COMPRESSION_CODEC : compressionCodec));
        map.put(KEY_COMPRESSION_LEVEL, bytes(Integer.toString(compressionLevel)));
        return map;
    }

    private static void put(Map<String, byte[]> map, String key, String value) {
        if (value != null) {
            map.put(key, bytes(value));
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Reconstructs a header from the raw metadata map of an Avro file header.
     * Missing DieselDB keys are replaced with defaults so headers written before
     * Prompt 76 parse cleanly; present-but-malformed values fail fast with
     * {@link IllegalArgumentException} because they indicate corruption or a
     * foreign writer.
     *
     * @param meta the parsed header metadata map (may be empty)
     * @return the reconstructed header (never {@code null})
     */
    public static AvroFileHeader fromMetaMap(Map<String, byte[]> meta) {
        boolean present = false;
        for (String key : META_KEYS) {
            if (meta.containsKey(key)) {
                present = true;
                break;
            }
        }
        String engine = decode(meta, KEY_ENGINE);
        if (engine == null) {
            engine = ENGINE_NAME;
        }
        String database = decode(meta, KEY_DATABASE);
        String tableName = decode(meta, KEY_TABLE);
        String formatVersion = decode(meta, KEY_FORMAT_VERSION);
        if (formatVersion == null) {
            formatVersion = FORMAT_VERSION;
        }
        String schemaVersionRaw = decode(meta, KEY_SCHEMA_VERSION);
        int schemaVersion = schemaVersionRaw == null
                ? DEFAULT_SCHEMA_VERSION
                : parseInt(KEY_SCHEMA_VERSION, schemaVersionRaw);
        String timestampRaw = decode(meta, KEY_CREATION_TIMESTAMP);
        Instant timestamp = timestampRaw == null ? null : parseInstant(timestampRaw);
        String codec = decode(meta, KEY_COMPRESSION_CODEC);
        if (codec == null) {
            codec = DEFAULT_COMPRESSION_CODEC;
        }
        String levelRaw = decode(meta, KEY_COMPRESSION_LEVEL);
        int level = levelRaw == null
                ? DEFAULT_COMPRESSION_LEVEL
                : parseInt(KEY_COMPRESSION_LEVEL, levelRaw);
        return new AvroFileHeader(engine, database, tableName, formatVersion,
                schemaVersion, timestamp, codec, level, present);
    }

    private static String decode(Map<String, byte[]> meta, String key) {
        byte[] raw = meta.get(key);
        return raw == null ? null : new String(raw, StandardCharsets.UTF_8);
    }

    private static int parseInt(String key, String raw) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Corrupt AVRO header metadata: '" + key + "' is not a valid integer: '" + raw + "'", e);
        }
    }

    private static Instant parseInstant(String raw) {
        try {
            return Instant.parse(raw);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Corrupt AVRO header metadata: '" + KEY_CREATION_TIMESTAMP
                            + "' is not a valid ISO-8601 timestamp: '" + raw + "'", e);
        }
    }

    // ─── Validation ─────────────────────────────────────────────────

    /**
     * Validates this header against the rules the read path enforces.
     *
     * @return the list of problems (empty when the header is acceptable)
     */
    public List<String> validate() {
        List<String> problems = new ArrayList<>();
        if (formatVersion != null) {
            String[] parts = formatVersion.split("\\.", 2);
            if (parts.length != 2 || !parts[0].chars().allMatch(Character::isDigit)
                    || !parts[1].chars().allMatch(Character::isDigit)) {
                problems.add("Malformed file-format version '" + formatVersion
                        + "' (expected 'major.minor')");
            } else {
                int major = Integer.parseInt(parts[0]);
                int supportedMajor = Integer.parseInt(FORMAT_VERSION.split("\\.", 2)[0]);
                if (major > supportedMajor) {
                    problems.add("Unsupported file-format version '" + formatVersion
                            + "' (this engine supports up to '" + FORMAT_VERSION + "')");
                }
            }
        }
        if (schemaVersion < 1) {
            problems.add("Invalid schema version " + schemaVersion + " (must be >= 1)");
        }
        if (compressionCodec == null || compressionCodec.isBlank()) {
            problems.add("Missing compression codec");
        }
        if (database != null && database.isBlank()) {
            problems.add("Blank database name");
        }
        if (tableName != null && tableName.isBlank()) {
            problems.add("Blank table name");
        }
        return problems;
    }

    /**
     * Throws {@link IllegalArgumentException} when {@link #validate()} reports any
     * problem, joining the reasons with "{@code ; }".
     */
    public void requireValid() {
        List<String> problems = validate();
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid AVRO file header: " + String.join("; ", problems));
        }
    }

    // ─── Builder ────────────────────────────────────────────────────

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String engine = ENGINE_NAME;
        private String database;
        private String tableName;
        private String formatVersion = FORMAT_VERSION;
        private int schemaVersion = DEFAULT_SCHEMA_VERSION;
        private Instant creationTimestamp;
        private String compressionCodec = DEFAULT_COMPRESSION_CODEC;
        private int compressionLevel = DEFAULT_COMPRESSION_LEVEL;

        public Builder engine(String engine) {
            this.engine = engine;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder formatVersion(String formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder schemaVersion(int schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public Builder creationTimestamp(Instant creationTimestamp) {
            this.creationTimestamp = creationTimestamp;
            return this;
        }

        public Builder compressionCodec(String compressionCodec) {
            this.compressionCodec = compressionCodec;
            return this;
        }

        public Builder compressionLevel(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            return this;
        }

        public AvroFileHeader build() {
            return new AvroFileHeader(engine, database, tableName, formatVersion,
                    schemaVersion, creationTimestamp, compressionCodec, compressionLevel, false);
        }
    }

    // ─── Object contract ────────────────────────────────────────────

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AvroFileHeader that)) {
            return false;
        }
        return schemaVersion == that.schemaVersion
                && compressionLevel == that.compressionLevel
                && Objects.equals(engine, that.engine)
                && Objects.equals(database, that.database)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(formatVersion, that.formatVersion)
                && Objects.equals(creationTimestamp, that.creationTimestamp)
                && Objects.equals(compressionCodec, that.compressionCodec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(engine, database, tableName, formatVersion,
                schemaVersion, creationTimestamp, compressionCodec, compressionLevel);
    }

    @Override
    public String toString() {
        return "AvroFileHeader{engine='" + engine + "', database='" + database
                + "', table='" + tableName + "', formatVersion='" + formatVersion
                + "', schemaVersion=" + schemaVersion
                + ", creationTimestamp=" + creationTimestamp
                + ", compressionCodec='" + compressionCodec
                + "', compressionLevel=" + compressionLevel + '}';
    }
}