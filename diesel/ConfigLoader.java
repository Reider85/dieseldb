package diesel;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Central, fail-safe loader for {@code config.properties}. The configuration
 * is read from the process working directory (CWD), so the root
 * {@code config.properties} is the single source of truth for every class.
 * A missing or unreadable file yields an empty {@link Properties} instance and
 * callers fall back to their defaults.
 */
final class ConfigLoader {

    private ConfigLoader() {
    }

    /** Loads {@code config.properties} from the working directory. */
    static Properties load() {
        Properties props = new Properties();
        File configFile = new File(ErrorMessages.CONFIG_FILE);
        if (configFile.exists()) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                props.load(fis);
            } catch (Exception ignored) {
                // Fail-safe: an empty Properties lets callers keep defaults.
            }
        }
        return props;
    }

    static String getString(String key, String defaultValue) {
        return load().getProperty(key, defaultValue);
    }

    static int getInt(String key, int defaultValue) {
        String raw = load().getProperty(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    static long getLong(String key, long defaultValue) {
        String raw = load().getProperty(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    static double getDouble(String key, double defaultValue) {
        String raw = load().getProperty(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    static boolean getBoolean(String key, boolean defaultValue) {
        String raw = load().getProperty(key);
        return raw == null ? defaultValue : Boolean.parseBoolean(raw.trim());
    }
    
    // Tablespace configuration keys
    static final String TABLESPACE_DEFAULT_PATH = "tablespace.default.path";
    static final String TABLESPACE_FILE_MAX_SIZE_MB = "tablespace.file.max.size.mb";
    
    // Tablespace configuration methods
    static String getTablespaceDefaultPath(String defaultValue) {
        return getString(TABLESPACE_DEFAULT_PATH, defaultValue);
    }
    
    static long getTablespaceFileMaxSizeMB(long defaultValue) {
        return getLong(TABLESPACE_FILE_MAX_SIZE_MB, defaultValue);
    }

    // ─── Metrics HTTP endpoint (prompt4 #45) ────────────────────────
    static final String METRICS_ENABLED = "metrics.enabled";
    static final String METRICS_PORT = "metrics.port";
    static final String METRICS_HOST = "metrics.host";
    /** Default endpoint switch: on, so {@code curl :9090/metrics} works out of the box. */
    static final boolean DEFAULT_METRICS_ENABLED = true;
    /** Default Prometheus scrape port. */
    static final int DEFAULT_METRICS_PORT = 9090;
    /** Default bind address: all interfaces, so remote Prometheus/Grafana can scrape. */
    static final String DEFAULT_METRICS_HOST = "0.0.0.0";

    /** Whether the /metrics HTTP endpoint starts with the database server. */
    static boolean getMetricsEnabled() {
        return getBoolean(METRICS_ENABLED, DEFAULT_METRICS_ENABLED);
    }

    /** The TCP port for the /metrics endpoint. */
    static int getMetricsPort() {
        return getInt(METRICS_PORT, DEFAULT_METRICS_PORT);
    }

    /** The bind address for the /metrics endpoint. */
    static String getMetricsHost() {
        return getString(METRICS_HOST, DEFAULT_METRICS_HOST);
    }

    // ─── String literal case compatibility (prompt4 #49) ────────────
    static final String COMPAT_UPPERCASE_STRING_LITERALS = "compat.uppercase.string.literals";
    /** Default false: string literals keep their original case (SQL standard). */
    static final boolean DEFAULT_COMPAT_UPPERCASE_STRING_LITERALS = false;
    /**
     * File-based value resolved once at class load. The flag is consulted on
     * every string-literal extraction (hot path: a 100k-row INSERT calls it
     * 100k times), so re-reading config.properties per call would cost one
     * file open per literal. Tests flip the SYSTEM PROPERTY instead, which is
     * still checked on every call below.
     */
    private static final boolean COMPAT_UPPERCASE_FILE_VALUE =
            getBoolean(COMPAT_UPPERCASE_STRING_LITERALS, DEFAULT_COMPAT_UPPERCASE_STRING_LITERALS);

    /**
     * Whether string literals are uppercased at parse time (pre-#49 legacy
     * behavior). Resolution order: system property (checked per call so tests
     * can flip it) then the once-cached config.properties value then the
     * default - the sysprop-first pattern of {@code Table.resolvePersistConfig},
     * with the file read hoisted out of the per-literal hot path.
     */
    static boolean getCompatUppercaseStringLiterals() {
        String sysprop = System.getProperty(COMPAT_UPPERCASE_STRING_LITERALS);
        if (sysprop != null && !sysprop.isBlank()) {
            return Boolean.parseBoolean(sysprop.trim());
        }
        return COMPAT_UPPERCASE_FILE_VALUE;
    }
}