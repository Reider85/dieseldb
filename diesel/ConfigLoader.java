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
}