package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import diesel.ConfigKeys;

/**
 * Resolved AVRO bloom filter configuration (Prompt 87).
 *
 * <p>The bloom filter settings are resolved from a system-property
 * override first, then the root {@code config.properties}, then the
 * code-level defaults:
 *
 * <ul>
 *   <li>{@code avro.bloom.enabled} — master switch (default {@code true})</li>
 *   <li>{@code avro.bloom.bits.per.key} — bits allocated per distinct key
 *       (default {@code 10})</li>
 *   <li>{@code avro.bloom.num.hashes} — number of hash functions
 *       (default {@code 7})</li>
 *   <li>{@code avro.bloom.fpp} — target false-positive probability
 *       (default {@code 0.01})</li>
 * </ul>
 *
 * @since Prompt 87
 */
public final class AvroBloomFilterConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBloomFilterConfig.class);

    /** Config key: master switch. */
    public static final String ENABLED_KEY = "avro.bloom.enabled";
    /** Config key: bits per distinct key. */
    public static final String BITS_PER_KEY_KEY = "avro.bloom.bits.per.key";
    /** Config key: number of hash functions. */
    public static final String NUM_HASHES_KEY = "avro.bloom.num.hashes";
    /** Config key: target false-positive probability. */
    public static final String FPP_KEY = "avro.bloom.fpp";

    /** Code-level defaults. */
    public static final boolean DEFAULT_ENABLED = true;
    public static final int DEFAULT_BITS_PER_KEY = 10;
    public static final int DEFAULT_NUM_HASHES = 7;
    public static final double DEFAULT_FPP = 0.01;

    /** Config key: overrides the config.properties file location (test-support hook). */
    static final String CONFIG_FILE_KEY = "avro.bloom.config.file";

    private final boolean enabled;
    private final int bitsPerKey;
    private final int numHashes;
    private final double fpp;

    private AvroBloomFilterConfig(boolean enabled, int bitsPerKey, int numHashes, double fpp) {
        this.enabled = enabled;
        this.bitsPerKey = bitsPerKey;
        this.numHashes = numHashes;
        this.fpp = fpp;
    }

    /** Whether the bloom filter is enabled. */
    public boolean enabled() {
        return enabled;
    }

    /** Bits allocated per distinct key. */
    public int bitsPerKey() {
        return bitsPerKey;
    }

    /** Number of hash functions. */
    public int numHashes() {
        return numHashes;
    }

    /** Target false-positive probability. */
    public double fpp() {
        return fpp;
    }

    /**
     * Computes the optimal bit-array size for {@code expectedInsertions}
     * distinct keys at the configured bits-per-key ratio.
     *
     * @param expectedInsertions expected number of distinct keys in a block
     * @return bit array size in bits (at least 8)
     */
    public int bitSizeFor(int expectedInsertions) {
        long bits = (long) Math.max(1, expectedInsertions) * bitsPerKey;
        return (int) Math.max(8, Math.min(bits, Integer.MAX_VALUE - 64));
    }

    /**
     * Builds a configuration from explicit values (used when restoring a
     * filter from a sidecar file whose header records the settings, and by
     * tests constructing controlled configurations).
     *
     * @param bitsPerKey bits per distinct key
     * @param numHashes  number of hash functions
     * @param fpp        target false-positive probability
     * @param enabled    master switch
     * @return the resolved configuration (never {@code null})
     */
    public static AvroBloomFilterConfig resolveFor(int bitsPerKey, int numHashes,
                                                   double fpp, boolean enabled) {
        int bk = Math.max(1, Math.min(64, bitsPerKey));
        int nh = Math.max(1, Math.min(32, numHashes));
        double f = (fpp > 0.0 && fpp < 1.0) ? fpp : DEFAULT_FPP;
        return new AvroBloomFilterConfig(enabled, bk, nh, f);
    }

    /**
     * Resolves the AVRO bloom filter configuration:
     * system property override, then {@code config.properties}, then defaults.
     *
     * @return the resolved configuration (never {@code null})
     */
    public static AvroBloomFilterConfig resolve() {
        boolean enabled = getBoolean(ENABLED_KEY, DEFAULT_ENABLED);
        int bitsPerKey = getInt(BITS_PER_KEY_KEY, DEFAULT_BITS_PER_KEY);
        if (bitsPerKey < 1 || bitsPerKey > 64) {
            LOGGER.warn("Invalid {} value {}, clamping to [1, 64]", BITS_PER_KEY_KEY, bitsPerKey);
            bitsPerKey = Math.max(1, Math.min(64, bitsPerKey));
        }
        int numHashes = getInt(NUM_HASHES_KEY, DEFAULT_NUM_HASHES);
        if (numHashes < 1 || numHashes > 32) {
            LOGGER.warn("Invalid {} value {}, clamping to [1, 32]", NUM_HASHES_KEY, numHashes);
            numHashes = Math.max(1, Math.min(32, numHashes));
        }
        double fpp = getDouble(FPP_KEY, DEFAULT_FPP);
        if (fpp <= 0.0 || fpp >= 1.0) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED, FPP_KEY, fpp, DEFAULT_FPP);
            fpp = DEFAULT_FPP;
        }
        return new AvroBloomFilterConfig(enabled, bitsPerKey, numHashes, fpp);
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

    private static double getDouble(String key, double defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        try {
            return Double.parseDouble(raw.trim());
        } catch (RuntimeException e) {
            LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, String.valueOf(defaultValue));
        String v = raw.trim().toLowerCase(java.util.Locale.ROOT);
        if (v.equals("true") || v.equals("on") || v.equals("yes")) {
            return true;
        }
        if (v.equals("false") || v.equals("off") || v.equals("no")) {
            return false;
        }
        LOGGER.warn(AvroMetricConstants.MSG_INVALID_VALUE_QUOTED, key, raw, defaultValue);
        return defaultValue;
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty(ConfigKeys.SYS_PROP_USER_DIR, "."), ConfigKeys.CONFIG_FILE);
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
                LOGGER.debug(AvroMetricConstants.MSG_CONFIG_READ_FAILED, ignored.getMessage());
            }
        }
        return props;
    }

    @Override
    public String toString() {
        return "AvroBloomFilterConfig{enabled=" + enabled
                + ", bitsPerKey=" + bitsPerKey
                + ", numHashes=" + numHashes
                + ", fpp=" + fpp + '}';
    }
}
