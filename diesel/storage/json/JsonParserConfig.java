package diesel.storage.json;

import java.util.Objects;

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
 * warn-and-overwrite behaviour.
 *
 * <p>Values may be overridden through system properties
 * {@code jsonl.max.nesting.depth}, {@code jsonl.max.string.length},
 * {@code jsonl.parser.backend} and {@code jsonl.duplicate.keys}; invalid
 * overrides fall back to the defaults.
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

    private final int maxNestingDepth;
    private final int maxStringLength;
    private final Backend backend;
    private final DuplicateKeyMode duplicateKeys;

    private JsonParserConfig(Builder builder) {
        this.maxNestingDepth = builder.maxNestingDepth;
        this.maxStringLength = builder.maxStringLength;
        this.backend = builder.backend;
        this.duplicateKeys = builder.duplicateKeys;
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

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder; invalid values are clamped to the defaults. */
    public static final class Builder {

        private int maxNestingDepth = readIntProperty("jsonl.max.nesting.depth", DEFAULT_MAX_NESTING_DEPTH);
        private int maxStringLength = readIntProperty("jsonl.max.string.length", DEFAULT_MAX_STRING_LENGTH);
        private Backend backend = readBackendProperty();
        private DuplicateKeyMode duplicateKeys = readDuplicateKeyProperty();

        private static int readIntProperty(String key, int fallback) {
            String value = System.getProperty(key);
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

        private static Backend readBackendProperty() {
            String value = System.getProperty("jsonl.parser.backend");
            if (value == null) {
                return Backend.JACKSON;
            }
            try {
                return Backend.valueOf(value.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                return Backend.JACKSON;
            }
        }

        private static DuplicateKeyMode readDuplicateKeyProperty() {
            String value = System.getProperty("jsonl.duplicate.keys");
            if (value == null) {
                return DuplicateKeyMode.FAIL;
            }
            try {
                return DuplicateKeyMode.valueOf(value.trim().toUpperCase().replace('-', '_'));
            } catch (IllegalArgumentException e) {
                return DuplicateKeyMode.FAIL;
            }
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
                && duplicateKeys == other.duplicateKeys;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNestingDepth, maxStringLength, backend, duplicateKeys);
    }

    @Override
    public String toString() {
        return "JsonParserConfig{maxNestingDepth=" + maxNestingDepth
                + ", maxStringLength=" + maxStringLength
                + ", backend=" + backend
                + ", duplicateKeys=" + duplicateKeys + '}';
    }
}