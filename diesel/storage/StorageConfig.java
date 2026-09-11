package diesel.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import diesel.ErrorMessages;

/**
 * Central configuration and deterministic text I/O helpers for the
 * {@code diesel.storage} package.
 *
 * <p>All delimited-file readers and writers resolve their charset from the
 * {@code storage.charset} config key (default {@code UTF-8}) via this class,
 * instead of falling back to the platform default charset. This keeps file
 * bytes deterministic across operating systems and JVM settings.
 *
 * <p>The config key is resolved from a system property override first, then
 * from the root {@code config.properties} file, then the default. The root
 * properties are loaded once at class initialization.
 */
final class StorageConfig {

    private static final Logger LOGGER = Logger.getLogger(StorageConfig.class.getName());

    private static final String CHARSET_KEY = "storage.charset";
    private static final String DEFAULT_CHARSET = StandardCharsets.UTF_8.name();

    private static final Properties ROOT_PROPS = loadRootProps();

    private StorageConfig() {
        throw new AssertionError("No instances");
    }

    /** Returns the configured charset (default UTF-8). */
    static Charset getCharset() {
        String name = System.getProperty(CHARSET_KEY);
        if (name == null) {
            name = ROOT_PROPS.getProperty(CHARSET_KEY, DEFAULT_CHARSET);
        }
        try {
            return Charset.forName(name.trim());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unsupported storage.charset ''{0}'', falling back to UTF-8: {1}",
                    new Object[]{name, e.getMessage()});
            return StandardCharsets.UTF_8;
        }
    }

    /** Opens a buffered reader over the given path using the configured charset. */
    static BufferedReader newReader(Path path) throws IOException {
        return java.nio.file.Files.newBufferedReader(path, getCharset());
    }

    /** Opens a buffered reader over the given file using the configured charset. */
    static BufferedReader newReader(File file) throws IOException {
        return newReader(file.toPath());
    }

    /** Opens a buffered writer to the given path using the configured charset. */
    static BufferedWriter newWriter(Path path) throws IOException {
        return java.nio.file.Files.newBufferedWriter(path, getCharset());
    }

    /** Opens a buffered writer to the given file using the configured charset. */
    static BufferedWriter newWriter(File file) throws IOException {
        return newWriter(file.toPath());
    }

    private static Properties loadRootProps() {
        Properties props = new Properties();
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (IOException ignored) {
            LOGGER.log(Level.FINE, "Config error, using defaults: {0}", ignored.getMessage());
        }
        return props;
    }
}