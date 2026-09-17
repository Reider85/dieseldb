package diesel.storage.json;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * Common configuration for the streaming JSON abstraction (prompt 42).
 *
 * <p>Defaults (all deliberate, prompt 42): lenient mode is always <b>off</b> -
 * NaN/Infinity, comments and other non-standard JSON shapes are rejected by
 * every backend; nesting depth is capped at {@value #DEFAULT_MAX_NESTING_DEPTH}
 * and string values at {@value #DEFAULT_MAX_STRING_LENGTH}. The backend
 * (Jackson or Gson) is chosen once here; the storage package never selects the
 * library itself.
 *
 * <p>Duplicate-key behaviour is carried here as the single hook for the
 * {@code jsonl.duplicate.keys} config (prompt 43): {@link #FAIL} (default)
 * rejects a duplicated field name, {@link #LAST_WINS} keeps the current
 * warn-and-overwrite behaviour. The type-coercion mode for JSONL readers is
 * carried here as the hook for {@code jsonl.type.coercion} (prompt 43):
 * {@link #STRICT} (default) rejects a JSON string being coerced into a
 * numeric column, {@link #LENIENT} allows it with a WARNING.
 *
 * <p>Values may be overridden through the root {@code config.properties}
 * file, and a system property of the same name wins over it:
 * {@code jsonl.max.nesting.depth}, {@code jsonl.max.string.length},
 * {@code jsonl.parser.backend}, {@code jsonl.duplicate.keys},
 * {@code jsonl.type.coercion}, {@code jsonl.schema.mode},
 * {@code jsonl.nested.mode}, {@code jsonl.array.columns},
 * {@code jsonl.missing.field} and {@code jsonl.load.error.mode}; invalid
 * overrides fall back to the defaults.
 *
 * <p>The write-mode policy ({@code jsonl.write.mode}, prompt 49) decides how
 * {@link diesel.storage.JsonlRowStorage} persists data: {@link WriteMode#REWRITE}
 * (default) performs a full atomic rewrite via {@link diesel.storage.AtomicFileWriter}
 * on every save, {@link WriteMode#APPEND} appends new rows to the base file
 * and tracks deltas in a sidecar. The compaction threshold
 * ({@code jsonl.compaction.threshold}, prompt 49) controls automatic
 * compaction when the delta ratio exceeds the configured fraction (default 0.3).
 *
 * <p>The schema mode ({@code jsonl.schema.mode}, prompt 44) is carried here
 * as the single hook for the JSONL schema-matching policies: {@link #STRICT}
 * requires every row's field set to match the table schema (unknown fields
 * are errors, a typo is reported with the nearest column-name hint),
 * {@link #INFERRED} derives the schema from the data on first load and
 * {@link #HYBRID} (default) keeps the schema columns mandatory and typed
 * while expanding the schema with new fields observed in the data.
 *
 * <p>The nested-storage mode ({@code jsonl.nested.mode}, prompt 45) decides
 * how nested JSON objects are persisted: {@link NestedMode#FLATTEN} (default)
 * writes every nested leaf into its own dot-notated column
 * ({@code user.address.city}) and reconstructs the object on save, while
 * {@link NestedMode#JSON_COLUMN} stores the whole object/array as compact JSON
 * text in a single column (type TEXT) addressed through JSON Path. The
 * array-storage mode ({@code jsonl.array.columns}, prompt 45) decides how an
 * array of scalars is persisted in flatten mode: {@link ArrayColumnsMode#JSON}
 * (default) keeps the whole array in one JSON column, {@link ArrayColumnsMode#EXPAND}
 * expands it into {@code arr[0]}, {@code arr[1]}, ... columns. Arrays of
 * objects always fall back to a single JSON column.
 *
 * <p>The load-error policy ({@code jsonl.load.error.mode}, prompt 48) decides
 * what a {@link diesel.storage.JsonlRowReader} does with a malformed row (bad
 * JSON, non-object line, type failure): {@link LoadErrorMode#FAIL} (default)
 * aborts the load with {@code file:line}/field diagnostics,
 * {@link LoadErrorMode#SKIP_ROW} logs the coordinates, skips the row and lets
 * the load continue (a final WARNING reports the skipped-row count).
 *
 * <p>The lazy-block policy (prompt 55) decides how
 * {@link diesel.storage.JsonlRowStorage} loads a plain (uncompressed) JSONL
 * file: {@code jsonl.lazy.blocks = true} defers the full parse and makes rows
 * available block-by-block through {@link diesel.storage.JsonlBlockManager}
 * (real byte-offset ranges from the prompt-54 pre-scan, LRU-evicted); any
 * classic access (scan/DML/save) materialises the full row list on first use,
 * so results are identical to the eager mode. The block size
 * ({@code jsonl.block.rows}, default 1000) and the LRU capacity
 * ({@code jsonl.block.cache.blocks}, default 16) bound the block manager.
 */
public final class JsonParserConfig {

    /** Maximum nesting depth of objects/arrays (default, prompt 42). */
    public static final int DEFAULT_MAX_NESTING_DEPTH = 64;

    /** Maximum length of a single JSON string value (default, prompt 42). */
    public static final int DEFAULT_MAX_STRING_LENGTH = 1_000_000;

    /** Default number of data lines per lazy block (prompt 55). */
    public static final int DEFAULT_BLOCK_ROWS = 1000;

    /** Default LRU capacity in blocks (prompt 55). */
    public static final int DEFAULT_BLOCK_CACHE_BLOCKS = 16;

    /** Supported streaming JSON backends. */
    public enum Backend { JACKSON, GSON }

    /** Duplicate-key policy wired to the jsonl.duplicate.keys config (prompt 43). */
    public enum DuplicateKeyMode { FAIL, LAST_WINS }

    /** JSONL type-coercion mode wired to the jsonl.type.coercion config (prompt 43). */
    public enum CoercionMode { STRICT, LENIENT }

    /** JSONL schema-matching mode wired to the jsonl.schema.mode config (prompt 44). */
    public enum SchemaMode { STRICT, INFERRED, HYBRID }

    /** JSONL nested-storage mode wired to the jsonl.nested.mode config (prompt 45). */
    public enum NestedMode { FLATTEN, JSON_COLUMN }

    /** JSONL array-storage mode wired to the jsonl.array.columns config (prompt 45). */
    public enum ArrayColumnsMode { JSON, EXPAND }

    /** JSONL missing-field policy wired to the jsonl.missing.field config (prompt 47). */
    public enum MissingFieldMode {
        /** Missing field is treated as {@code null}. */
        NULL,
        /** Missing field raises an error with file:line:field diagnostics. */
        ERROR,
        /** Backward-compatible default: missing field is treated as {@code null}. */
        DEFAULT
    }

    /** JSONL load-error policy wired to the jsonl.load.error.mode config (prompt 48). */
    public enum LoadErrorMode {
        /** A malformed row aborts the load with file:line diagnostics. */
        FAIL,
        /** A malformed row is logged with file:line and skipped; loads continue. */
        SKIP_ROW
    }

    /** JSONL write-mode policy wired to the jsonl.write.mode config (prompt 49). */
    public enum WriteMode {
        /** Full atomic rewrite via AtomicFileWriter on every save (current default). */
        REWRITE,
        /** Append new rows to the base file; deltas tracked in a sidecar. */
        APPEND
    }

    private final int maxNestingDepth;
    private final int maxStringLength;
    private final Backend backend;
    private final DuplicateKeyMode duplicateKeys;
    private final CoercionMode coercion;
    private final SchemaMode schemaMode;
    private final NestedMode nestedMode;
    private final ArrayColumnsMode arrayColumns;
    private final MissingFieldMode missingField;
    private final LoadErrorMode loadErrorMode;
    private final WriteMode writeMode;
    private final double compactionThreshold;
    private final boolean lazyBlocks;
    private final int blockRows;
    private final int blockCacheBlocks;

    private JsonParserConfig(Builder builder) {
        this.maxNestingDepth = builder.maxNestingDepth;
        this.maxStringLength = builder.maxStringLength;
        this.backend = builder.backend;
        this.duplicateKeys = builder.duplicateKeys;
        this.coercion = builder.coercion;
        this.schemaMode = builder.schemaMode;
        this.nestedMode = builder.nestedMode;
        this.arrayColumns = builder.arrayColumns;
        this.missingField = builder.missingField;
        this.loadErrorMode = builder.loadErrorMode;
        this.writeMode = builder.writeMode;
        this.compactionThreshold = builder.compactionThreshold;
        this.lazyBlocks = builder.lazyBlocks;
        this.blockRows = builder.blockRows;
        this.blockCacheBlocks = builder.blockCacheBlocks;
    }

    /** Returns the default configuration (strict, depth 64, 1MB strings, Jackson backend). */
    public static JsonParserConfig defaults() {
        return new Builder().build();
    }

    /** Returns the defaults with a chosen backend (used by the backend-swap tests). */
    public static JsonParserConfig defaultsFor(Backend backend) {
        return new Builder().backend(backend).build();
    }

    public int maxNestingDepth() {
        return maxNestingDepth;
    }

    public int maxStringLength() {
        return maxStringLength;
    }

    public Backend backend() {
        return backend;
    }

    public DuplicateKeyMode duplicateKeys() {
        return duplicateKeys;
    }

    /** Returns the JSONL type-coercion mode (STRICT by default, prompt 43). */
    public CoercionMode typeCoercion() {
        return coercion;
    }

    /** Returns the JSONL schema-matching mode (HYBRID by default, prompt 44). */
    public SchemaMode schemaMode() {
        return schemaMode;
    }

    /** Returns the JSONL nested-storage mode (FLATTEN by default, prompt 45). */
    public NestedMode nestedMode() {
        return nestedMode;
    }

    /** Returns the JSONL array-storage mode (JSON by default, prompt 45). */
    public ArrayColumnsMode arrayColumns() {
        return arrayColumns;
    }

    /** Returns the JSONL missing-field policy (DEFAULT by default, prompt 47). */
    public MissingFieldMode missingField() {
        return missingField;
    }

    /** Returns the JSONL load-error policy (FAIL by default, prompt 48). */
    public LoadErrorMode loadErrorMode() {
        return loadErrorMode;
    }

    /** Returns the JSONL write-mode policy (REWRITE by default, prompt 49). */
    public WriteMode writeMode() {
        return writeMode;
    }

    /** Returns the JSONL compaction threshold (0.3 = 30% default, prompt 49). */
    public double compactionThreshold() {
        return compactionThreshold;
    }

    /** Returns whether lazy block loading is enabled (false by default, prompt 55). */
    public boolean lazyBlocks() {
        return lazyBlocks;
    }

    /** Returns the number of data lines per lazy block (1000 by default, prompt 55). */
    public int blockRows() {
        return blockRows;
    }

    /** Returns the LRU capacity of the lazy block cache (16 blocks by default, prompt 55). */
    public int blockCacheBlocks() {
        return blockCacheBlocks;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder; invalid values are clamped to the defaults. */
    public static final class Builder {

        private static final Properties ROOT_PROPS = loadRootProps();

        private int maxNestingDepth = readIntProperty("jsonl.max.nesting.depth", DEFAULT_MAX_NESTING_DEPTH);
        private int maxStringLength = readIntProperty("jsonl.max.string.length", DEFAULT_MAX_STRING_LENGTH);
        private Backend backend = readBackendProperty();
        private DuplicateKeyMode duplicateKeys = readDuplicateKeyProperty();
        private CoercionMode coercion = readCoercionProperty();
        private SchemaMode schemaMode = readSchemaModeProperty();
        private NestedMode nestedMode = readNestedModeProperty();
        private ArrayColumnsMode arrayColumns = readArrayColumnsProperty();
        private MissingFieldMode missingField = readMissingFieldProperty();
        private LoadErrorMode loadErrorMode = readLoadErrorModeProperty();
        private WriteMode writeMode = readWriteModeProperty();
        private double compactionThreshold = readDoubleProperty("jsonl.compaction.threshold", 0.3);
        private boolean lazyBlocks = readBooleanProperty("jsonl.lazy.blocks", false);
        private int blockRows = readIntProperty("jsonl.block.rows", DEFAULT_BLOCK_ROWS);
        private int blockCacheBlocks = readIntProperty("jsonl.block.cache.blocks", DEFAULT_BLOCK_CACHE_BLOCKS);

        private static int readIntProperty(String key, int fallback) {
            String value = readString(key, null);
            if (value == null) {
                return fallback;
            }
            try {
                int parsed = Integer.parseInt(value.trim());
                return parsed > 0 ? parsed : fallback;
            } catch (NumberFormatException e) {
                return fallback;
            }
        }

        private static boolean readBooleanProperty(String key, boolean fallback) {
            String value = readString(key, null);
            if (value == null) {
                return fallback;
            }
            String trimmed = value.trim();
            if ("true".equalsIgnoreCase(trimmed)) {
                return true;
            }
            if ("false".equalsIgnoreCase(trimmed)) {
                return false;
            }
            return fallback;
        }

        private static String readString(String key, String fallback) {
            String systemValue = System.getProperty(key);
            if (systemValue != null) {
                return systemValue;
            }
            String configured = ROOT_PROPS.getProperty(key);
            return configured != null && !configured.isBlank() ? configured.trim() : fallback;
        }

        private static Backend readBackendProperty() {
            String value = readString("jsonl.parser.backend", "JACKSON");
            try {
                return Backend.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return Backend.JACKSON;
            }
        }

        private static DuplicateKeyMode readDuplicateKeyProperty() {
            String value = readString("jsonl.duplicate.keys", "FAIL");
            try {
                return DuplicateKeyMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return DuplicateKeyMode.FAIL;
            }
        }

        private static CoercionMode readCoercionProperty() {
            String value = readString("jsonl.type.coercion", "STRICT");
            try {
                return CoercionMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return CoercionMode.STRICT;
            }
        }

        private static SchemaMode readSchemaModeProperty() {
            String value = readString("jsonl.schema.mode", "HYBRID");
            try {
                return SchemaMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return SchemaMode.HYBRID;
            }
        }

        private static NestedMode readNestedModeProperty() {
            String value = readString("jsonl.nested.mode", "FLATTEN");
            try {
                return NestedMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return NestedMode.FLATTEN;
            }
        }

        private static ArrayColumnsMode readArrayColumnsProperty() {
            String value = readString("jsonl.array.columns", "JSON");
            try {
                return ArrayColumnsMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return ArrayColumnsMode.JSON;
            }
        }

        private static MissingFieldMode readMissingFieldProperty() {
            String value = readString("jsonl.missing.field", "DEFAULT");
            try {
                return MissingFieldMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return MissingFieldMode.DEFAULT;
            }
        }

        private static LoadErrorMode readLoadErrorModeProperty() {
            String value = readString("jsonl.load.error.mode", "FAIL");
            try {
                return LoadErrorMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return LoadErrorMode.FAIL;
            }
        }

        private static WriteMode readWriteModeProperty() {
            String value = readString("jsonl.write.mode", "REWRITE");
            try {
                return WriteMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return WriteMode.REWRITE;
            }
        }

        private static double readDoubleProperty(String key, double fallback) {
            String value = readString(key, null);
            if (value == null) {
                return fallback;
            }
            try {
                double parsed = Double.parseDouble(value.trim());
                return parsed >= 0.0 && parsed <= 1.0 ? parsed : fallback;
            } catch (NumberFormatException e) {
                return fallback;
            }
        }

        private static Properties loadRootProps() {
            Properties props = new Properties();
            try {
                File configFile = new File("config.properties");
                if (configFile.exists()) {
                    try (FileInputStream fis = new FileInputStream(configFile)) {
                        props.load(fis);
                    }
                }
            } catch (IOException ignored) {
                // Fail-safe: an empty set lets callers keep defaults.
            }
            return props;
        }

        public Builder maxNestingDepth(int maxNestingDepth) {
            this.maxNestingDepth = maxNestingDepth > 0 ? maxNestingDepth : DEFAULT_MAX_NESTING_DEPTH;
            return this;
        }

        public Builder maxStringLength(int maxStringLength) {
            this.maxStringLength = maxStringLength > 0 ? maxStringLength : DEFAULT_MAX_STRING_LENGTH;
            return this;
        }

        public Builder backend(Backend backend) {
            this.backend = backend != null ? backend : Backend.JACKSON;
            return this;
        }

        public Builder duplicateKeys(DuplicateKeyMode duplicateKeys) {
            this.duplicateKeys = duplicateKeys != null ? duplicateKeys : DuplicateKeyMode.FAIL;
            return this;
        }

        /** Sets the JSONL type-coercion mode ({@code null} resets to STRICT). */
        public Builder typeCoercion(CoercionMode coercion) {
            this.coercion = coercion != null ? coercion : CoercionMode.STRICT;
            return this;
        }

        /** Sets the JSONL schema-matching mode ({@code null} resets to HYBRID). */
        public Builder schemaMode(SchemaMode schemaMode) {
            this.schemaMode = schemaMode != null ? schemaMode : SchemaMode.HYBRID;
            return this;
        }

        /** Sets the JSONL nested-storage mode ({@code null} resets to FLATTEN, prompt 45). */
        public Builder nestedMode(NestedMode nestedMode) {
            this.nestedMode = nestedMode != null ? nestedMode : NestedMode.FLATTEN;
            return this;
        }

        /** Sets the JSONL array-storage mode ({@code null} resets to JSON, prompt 45). */
        public Builder arrayColumns(ArrayColumnsMode arrayColumns) {
            this.arrayColumns = arrayColumns != null ? arrayColumns : ArrayColumnsMode.JSON;
            return this;
        }

        /** Sets the JSONL missing-field policy ({@code null} resets to DEFAULT, prompt 47). */
        public Builder missingField(MissingFieldMode missingField) {
            this.missingField = missingField != null ? missingField : MissingFieldMode.DEFAULT;
            return this;
        }

        /** Sets the JSONL load-error policy ({@code null} resets to FAIL, prompt 48). */
        public Builder loadErrorMode(LoadErrorMode loadErrorMode) {
            this.loadErrorMode = loadErrorMode != null ? loadErrorMode : LoadErrorMode.FAIL;
            return this;
        }

        /** Sets the JSONL write-mode policy ({@code null} resets to REWRITE, prompt 49). */
        public Builder writeMode(WriteMode writeMode) {
            this.writeMode = writeMode != null ? writeMode : WriteMode.REWRITE;
            return this;
        }

        /** Sets the JSONL compaction threshold (prompt 49). */
        public Builder compactionThreshold(double compactionThreshold) {
            this.compactionThreshold = compactionThreshold >= 0.0 && compactionThreshold <= 1.0
                    ? compactionThreshold : 0.3;
            return this;
        }

        /** Sets lazy block loading (prompt 55); invalid values reset to the default. */
        public Builder lazyBlocks(boolean lazyBlocks) {
            this.lazyBlocks = lazyBlocks;
            return this;
        }

        /** Sets the number of data lines per lazy block (prompt 55); non-positive resets to 1000. */
        public Builder blockRows(int blockRows) {
            this.blockRows = blockRows > 0 ? blockRows : DEFAULT_BLOCK_ROWS;
            return this;
        }

        /** Sets the LRU capacity of the lazy block cache (prompt 55); non-positive resets to 16. */
        public Builder blockCacheBlocks(int blockCacheBlocks) {
            this.blockCacheBlocks = blockCacheBlocks > 0 ? blockCacheBlocks : DEFAULT_BLOCK_CACHE_BLOCKS;
            return this;
        }

        public JsonParserConfig build() {
            return new JsonParserConfig(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JsonParserConfig other)) {
            return false;
        }
        return maxNestingDepth == other.maxNestingDepth
                && maxStringLength == other.maxStringLength
                && backend == other.backend
                && duplicateKeys == other.duplicateKeys
                && coercion == other.coercion
                && schemaMode == other.schemaMode
                && nestedMode == other.nestedMode
                && arrayColumns == other.arrayColumns
                && missingField == other.missingField
                && loadErrorMode == other.loadErrorMode
                && writeMode == other.writeMode
                && lazyBlocks == other.lazyBlocks
                && blockRows == other.blockRows
                && blockCacheBlocks == other.blockCacheBlocks
                && Double.compare(compactionThreshold, other.compactionThreshold) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNestingDepth, maxStringLength, backend, duplicateKeys, coercion, schemaMode,
                nestedMode, arrayColumns, missingField, loadErrorMode, writeMode,
                lazyBlocks, blockRows, blockCacheBlocks,
                Double.hashCode(compactionThreshold));
    }

    @Override
    public String toString() {
        return "JsonParserConfig{maxNestingDepth=" + maxNestingDepth
                + ", maxStringLength=" + maxStringLength
                + ", backend=" + backend
                + ", duplicateKeys=" + duplicateKeys
                + ", coercion=" + coercion
                + ", schemaMode=" + schemaMode
                + ", nestedMode=" + nestedMode
                + ", arrayColumns=" + arrayColumns
                + ", missingField=" + missingField
                + ", loadErrorMode=" + loadErrorMode
                + ", writeMode=" + writeMode
                + ", compactionThreshold=" + compactionThreshold
                + ", lazyBlocks=" + lazyBlocks
                + ", blockRows=" + blockRows
                + ", blockCacheBlocks=" + blockCacheBlocks + '}';
    }
}