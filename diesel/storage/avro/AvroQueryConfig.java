package diesel.storage.avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Resolved AVRO query executor configuration (Prompt 91).
 *
 * <p>Settings are resolved from a system-property override first, then the
 * root {@code config.properties}, then the code-level defaults:
 *
 * <ul>
 *   <li>{@code avro.query.pushdown.enabled} — master switch for predicate
 *       pushdown (default {@code true})</li>
 *   <li>{@code avro.query.projection.enabled} — column projection
 *       pushdown (default {@code true})</li>
 *   <li>{@code avro.query.parallel.threshold} — minimum row count before
 *       the parallel reader is used (default {@code 10000})</li>
 * </ul>
 *
 * @since Prompt 91
 */
public final class AvroQueryConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroQueryConfig.class);

    /** Config key: master switch for predicate pushdown. */
    public static final String PUSHDOWN_ENABLED_KEY = "avro.query.pushdown.enabled";
    /** Config key: column projection pushdown. */
    public static final String PROJECTION_ENABLED_KEY = "avro.query.projection.enabled";
    /** Config key: parallel read threshold for queries. */
    public static final String PARALLEL_THRESHOLD_KEY = "avro.query.parallel.threshold";

    public static final boolean DEFAULT_PUSHDOWN_ENABLED = true;
    public static final boolean DEFAULT_PROJECTION_ENABLED = true;
    public static final int DEFAULT_PARALLEL_THRESHOLD = 10000;

    static final String CONFIG_FILE_KEY = "avro.query.config.file";

    private final boolean pushdownEnabled;
    private final boolean projectionEnabled;
    private final int parallelThreshold;

    private AvroQueryConfig(boolean pushdownEnabled, boolean projectionEnabled,
                            int parallelThreshold) {
        this.pushdownEnabled = pushdownEnabled;
        this.projectionEnabled = projectionEnabled;
        this.parallelThreshold = parallelThreshold;
    }

    public boolean pushdownEnabled() { return pushdownEnabled; }
    public boolean projectionEnabled() { return projectionEnabled; }
    public int parallelThreshold() { return parallelThreshold; }

    /**
     * Resolves the AVRO query executor configuration:
     * system property override, then {@code config.properties}, then defaults.
     *
     * @return the resolved configuration (never {@code null})
     */
    public static AvroQueryConfig resolve() {
        boolean pushdown = getBoolean(PUSHDOWN_ENABLED_KEY, DEFAULT_PUSHDOWN_ENABLED);
        boolean projection = getBoolean(PROJECTION_ENABLED_KEY, DEFAULT_PROJECTION_ENABLED);
        int threshold = getInt(PARALLEL_THRESHOLD_KEY, DEFAULT_PARALLEL_THRESHOLD);
        if (threshold < 0) {
            LOGGER.warn("Invalid {} value {}, using default {}", PARALLEL_THRESHOLD_KEY, threshold, DEFAULT_PARALLEL_THRESHOLD);
            threshold = DEFAULT_PARALLEL_THRESHOLD;
        }
        return new AvroQueryConfig(pushdown, projection, threshold);
    }

    @Override
    public String toString() {
        return "AvroQueryConfig{pushdownEnabled=" + pushdownEnabled
                + ", projectionEnabled=" + projectionEnabled
                + ", parallelThreshold=" + parallelThreshold + '}';
    }

    // ─── Config helpers ─────────────────────────────────────────────

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
            LOGGER.warn("Invalid {} value '{}', using default {}", key, raw, defaultValue);
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
        LOGGER.warn("Invalid {} value '{}', using default {}", key, raw, defaultValue);
        return defaultValue;
    }

    private static Properties rootProps() {
        Properties props = new Properties();
        String override = System.getProperty(CONFIG_FILE_KEY);
        File configFile = (override != null && !override.isBlank())
                ? new File(override)
                : new File(System.getProperty("user.dir", "."), "config.properties");
        if (configFile.exists()) {
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            } catch (IOException ignored) {
            }
        }
        return props;
    }
}
