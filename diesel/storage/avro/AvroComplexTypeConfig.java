package diesel.storage.avro;

import java.io.File;
import java.io.IOException;

/**
 * Shared per-call config resolution for the Prompt 75 complex-type handlers
 * (array/map size limits, record nesting depth, enum case sensitivity).
 * <p>
 * Every value is resolved from sysprop → config.properties → default on each
 * call (no static caching), mirroring the other AVRO config classes so tests
 * can override behaviour with {@code System.setProperty}.
 */
final class AvroComplexTypeConfig {

    /** config.properties / system-property key: config file override used by tests. */
    static final String CONFIG_FILE_KEY = "diesel.avro.config.file";

    private static final String DEFAULT_CONFIG_FILE = "config.properties";

    private AvroComplexTypeConfig() { }

    static int intValue(String key, int defaultValue) {
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isBlank()) {
            try {
                return Integer.parseInt(sysValue.trim());
            } catch (NumberFormatException ignored) {
                // fall through to the config file / default
            }
        }
        String val = readConfigProperty(key);
        if (val != null && !val.isBlank()) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException ignored) {
                // fall through to the default
            }
        }
        return defaultValue;
    }

    static boolean booleanValue(String key, boolean defaultValue) {
        String sysValue = System.getProperty(key);
        if (sysValue != null && !sysValue.isBlank()) {
            return Boolean.parseBoolean(sysValue);
        }
        String val = readConfigProperty(key);
        if (val != null && !val.isBlank()) {
            return Boolean.parseBoolean(val);
        }
        return defaultValue;
    }

    private static String readConfigProperty(String key) {
        String userDir = System.getProperty("user.dir", ".");
        String configPath = System.getProperty(CONFIG_FILE_KEY, DEFAULT_CONFIG_FILE);
        File configFile = new File(userDir, configPath);
        if (!configFile.exists()) {
            return null;
        }
        try {
            var props = new java.util.Properties();
            try (var in = java.nio.file.Files.newInputStream(configFile.toPath())) {
                props.load(in);
            }
            return props.getProperty(key);
        } catch (IOException ignored) {
            return null;
        }
    }
}