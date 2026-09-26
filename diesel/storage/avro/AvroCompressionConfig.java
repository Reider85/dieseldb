package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import diesel.ConfigKeys;

/**
 * Resolved AVRO compression configuration (Prompt 62).
 *
 * <p>The codec ({@code avro.compression.codec}), its level
 * ({@code avro.compression.level}) and the auto-select switch
 * ({@code avro.compression.auto} / {@code avro.compression.auto.min.bytes}) are
 * resolved from a system-property override first, then the root
 * {@code config.properties}, then the code-level defaults — mirroring
 * {@code CompressionFactory.resolveLeveled} in the delimited backend.
 *
 * <p>Supported codecs: {@code null} (no compression), {@code deflate},
 * {@code snappy}, {@code zstandard}, {@code bzip2}. The level is passed through
 * to {@link AvroCodecFactory}; {@code -1} means the Avro codec's built-in
 * default (deflate {@code -1}, zstandard {@code 3}), snappy/bzip2 ignore it.
 *
 * <p>Auto-select: when {@code avro.compression.auto=on}, the configured codec
 * is only applied once the estimated uncompressed payload size reaches
 * {@code avro.compression.auto.min.bytes}; smaller datasets fall back to the
 * {@code null} codec so the compression overhead is not paid on tiny files.
 *
 * @since Prompt 62
 */
public final class AvroCompressionConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroCompressionConfig.class);

    /** Codec config key. */
    public static final String CODEC_KEY = "avro.compression.codec";
    /** Level config key. */
    public static final String LEVEL_KEY = "avro.compression.level";
    /** Auto-select config key (on | off). */
    public static final String AUTO_KEY = "avro.compression.auto";
    /** Auto-select size threshold config key (bytes). */
    public static final String AUTO_MIN_BYTES_KEY = "avro.compression.auto.min.bytes";

    /** Code-level defaults. */
    public static final String DEFAULT_CODEC = "null";
    public static final int DEFAULT_LEVEL = -1;
    public static final boolean DEFAULT_AUTO = false;
    public static final long DEFAULT_AUTO_MIN_BYTES = 1_048_576L;

    /** Supported codec names (lower-case canonical). */
    public static final List<String> SUPPORTED_CODECS =
            List.of("null", "deflate", "snappy", "zstandard", "bzip2");

    private final String codec;
    private final int level;
    private final boolean auto;
    private final long minBytes;

    private AvroCompressionConfig(String codec, int level, boolean auto, long minBytes) {
        this.codec = codec;
        this.level = level;
        this.auto = auto;
        this.minBytes = minBytes;
    }

    /** The configured codec name (canonical lower-case), e.g. {@code "zstandard"}. */
    public String codec() {
        return codec;
    }

    /** The configured compression level, or {@code -1} for the codec default. */
    public int level() {
        return level;
    }

    /** Whether auto-select by data size is enabled. */
    public boolean auto() {
        return auto;
    }

    /** The auto-select payload-size threshold in bytes. */
    public long minBytes() {
        return minBytes;
    }

    /**
     * The codec to use for a dataset of the given rows: when auto-select is on
     * and the estimated payload is below {@link #minBytes()}, the {@code null}
     * codec is returned; otherwise the configured codec.
     *
     * @param rows the dataset being written (may be empty)
     * @return the effective codec name
     */
    public String effectiveCodec(List<?> rows) {
        if (auto && estimatedBytes(rows) < minBytes) {
            LOGGER.debug("AVRO auto-select: {} rows estimated below {} bytes, using null codec",
                    rows.size(), minBytes);
            return "null";
        }
        return codec;
    }

    /**
     * Resolves the AVRO compression configuration:
     * system property override, then {@code config.properties}, then defaults.
     *
     * @return the resolved configuration (never {@code null})
     * @throws IllegalArgumentException when {@code avro.compression.codec}
     *                                  names an unsupported codec
     */
    public static AvroCompressionConfig resolve() {
        String codecRaw = getString(CODEC_KEY, DEFAULT_CODEC);
        String codec = normalize(codecRaw);
        if (!SUPPORTED_CODECS.contains(codec)) {
            throw new IllegalArgumentException(
                    "Unknown AVRO compression codec '" + codecRaw + "' (expected: null, deflate, snappy, zstandard, bzip2)");
        }
        return new AvroCompressionConfig(
                codec,
                getInt(LEVEL_KEY, DEFAULT_LEVEL),
                parseAuto(getString(AUTO_KEY, String.valueOf(DEFAULT_AUTO))),
                getLong(AUTO_MIN_BYTES_KEY, DEFAULT_AUTO_MIN_BYTES));
    }

    /**
     * Rough estimate of the uncompressed payload size of the given rows. Used
     * by auto-select (and tests); string length is approximated with one byte
     * per char (UTF-8 expansion is negligible at the threshold scale).
     */
    public static long estimatedBytes(List<?> rows) {
        long total = 0;
        for (Object row : rows) {
            total += 16L; // per-row overhead
            if (!(row instanceof Object[] a)) {
                continue;
            }
            for (Object value : a) {
                if (value == null) {
                    continue;
                }
                if (value instanceof String s) {
                    total += s.length() + 4L;
                } else if (value instanceof byte[] b) {
                    total += b.length + 4L;
                } else if (value instanceof java.nio.ByteBuffer bb) {
                    total += bb.remaining() + 4L;
                } else {
                    total += 16L; // numbers, dates, booleans, UUIDs
                }
            }
        }
        return total;
    }

    // ─── Config helpers ─────────────────────────────────────────────

    private static String normalize(String raw) {
        return raw == null || raw.isBlank() ? DEFAULT_CODEC : raw.trim().toLowerCase(Locale.ROOT);
    }

    private static boolean parseAuto(String raw) {
        if (raw == null) {
            return DEFAULT_AUTO;
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
                LOGGER.warn("Invalid {} value '{}', using default auto-select {}", AUTO_KEY, raw, DEFAULT_AUTO);
                return DEFAULT_AUTO;
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

    private static int getInt(String key, int defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long getLong(String key, long defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            long value = Long.parseLong(raw.trim());
            return Math.max(0L, value);
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static final Properties rootProps() {
        return Holder.ROOT_PROPS;
    }

    private static final class Holder {
        private static final Properties ROOT_PROPS = load();

        private static Properties load() {
            Properties props = new Properties();
            String userDir = System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, ".");
            File configFile = new File(userDir, ConfigKeys.CONFIG_FILE);
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

    @Override
    public String toString() {
        return "AvroCompressionConfig{codec='" + codec + "', level=" + level
                + ", auto=" + auto + ", minBytes=" + minBytes + '}';
    }
}