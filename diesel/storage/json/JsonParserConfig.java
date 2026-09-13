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
 * {@code jsonl.parser.backend}, {@code jsonl.duplicate.keys} and
 * {@code jsonl.type.coercion}; invalid overrides fall back to the defaults.
 */
public final class JsonParserConfig {

    /** Maximum nesting depth of objects/arrays (default, prompt 42). */
    public static final int DEFAULT_MAX_NESTING_DEPTH = 64;

    /** Maximum length of a single JSON string value (default, prompt 42). */
    public static final int DEFAULT_MAX_STRING_LENGTH = 1_000_000;

    /** Supported streaming JSON backends. */
    public enum Backend { JACKSON, GSON }

    /** Duplicate-key policy wired to the jsonl.duplicate.keys config (prompt 43). */
    public enum DuplicateKeyMode { FAIL, LAST_WINS }

    /** JSONL type-coercion mode wired to the jsonl.type.coercion config (prompt 43). */
    public enum CoercionMode { STRICT, LENIENT }

    private final int maxNestingDepth;
    private final int maxStringLength;
    private final Backend backend;
    private final DuplicateKeyMode duplicateKeys;
    private final CoercionMode coercion;

    private JsonParserConfig(Builder builder) {
        this.maxNestingDepth = builder.maxNestingDepth;
        this.maxStringLength = builder.maxStringLength;
        this.backend = builder.backend;
        this.duplicateKeys = builder.duplicateKeys;
        this.coercion = builder.coercion;
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
                && coercion == other.coercion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNestingDepth, maxStringLength, backend, duplicateKeys, coercion);
    }

    @Override
    public String toString() {
        return "JsonParserConfig{maxNestingDepth=" + maxNestingDepth
                + ", maxStringLength=" + maxStringLength
                + ", backend=" + backend
                + ", duplicateKeys=" + duplicateKeys
                + ", coercion=" + coercion + '}';
    }
}