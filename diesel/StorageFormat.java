package diesel;

import diesel.format.FormatRegistry;
import diesel.format.TableFormat;

import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the persistent storage format for tables from {@code config.properties}.
 * This selects the file read/write mechanism used by each table:
 *
 * <ul>
 *   <li>{@code PARQUET} — a single {@code .parquet} file per table serves both
 *       primary persistence and columnar (OLAP) reads, replacing the legacy
 *       {@code .table} (Java serialization) and {@code .csv} files.</li>
 *   <li>{@code CSV} — legacy behavior ({@code .table} serialization plus a
 *       {@code .csv} export on auto-commit DML).</li>
 *   <li>{@code SERIALIZED} — Java serialization only ({@code .table}).</li>
 *   <li>{@code AUTO} — {@code PARQUET} for tables at or above
 *       {@link Table#COLUMNAR_THRESHOLD_ROWS} live rows, {@code CSV} otherwise.</li>
 * </ul>
 *
 * <p>Resolution order is: a per-table override {@code storage.format.<TABLE>},
 * then {@code storage.format.default}, then the legacy global
 * {@code storage.format}. The default is {@code CSV}. Unknown values fall back
 * to {@code CSV}. Format names are validated against {@link FormatRegistry}.</p>
 */
final class StorageFormat {

    private static final Logger LOGGER = Logger.getLogger(StorageFormat.class.getName());

    private static final String CONFIG_KEY = "storage.format";
    private static final String CONFIG_KEY_DEFAULT = "storage.format.default";
    private static final String CONFIG_KEY_TABLE_PREFIX = "storage.format.";

    /** Test override / cached configured format; null means read from config file. */
    private static volatile String configuredFormat;

    private StorageFormat() {
    }

    /**
     * Returns the configured base format string (uppercased), or
     * {@link ErrorMessages#STORAGE_FORMAT_CSV} if not present/invalid.
     *
     * @return the raw configured format
     */
    static String configuredFormat() {
        String raw = configuredFormat;
        if (raw == null) {
            raw = readRawFormat(null);
            if (raw == null) {
                raw = ErrorMessages.STORAGE_FORMAT_CSV;
            }
        }
        return raw;
    }

    /**
     * Loads properties from {@code config.properties}.
     */
    private static Properties loadProperties() {
        Properties props = new Properties();
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to read config.properties: {0}", e.getMessage());
        }
        return props;
    }

    /**
     * Reads the raw selected format name for a table from the config file,
     * consulting per-table, then default, then global keys.
     *
     * @param tableName the table name, or {@code null} to skip the per-table key
     * @return the raw format name (uppercased) or {@code null} when unset
     */
    private static String readRawFormat(String tableName) {
        Properties props = loadProperties();
        if (tableName != null) {
            String perTable = props.getProperty(CONFIG_KEY_TABLE_PREFIX + tableName);
            if (perTable != null && !perTable.isBlank()) {
                return perTable.trim().toUpperCase();
            }
        }
        String defaultValue = props.getProperty(CONFIG_KEY_DEFAULT);
        if (defaultValue != null && !defaultValue.isBlank()) {
            return defaultValue.trim().toUpperCase();
        }
        String global = props.getProperty(CONFIG_KEY);
        if (global != null && !global.isBlank()) {
            return global.trim().toUpperCase();
        }
        return null;
    }

    private static boolean isValid(String v) {
        return v.equals(ErrorMessages.STORAGE_FORMAT_PARQUET)
                || v.equals(ErrorMessages.STORAGE_FORMAT_CSV)
                || v.equals(ErrorMessages.STORAGE_FORMAT_SERIALIZED)
                || v.equals(ErrorMessages.STORAGE_FORMAT_AUTO);
    }

    /**
     * Returns the concrete storage format name for a table, resolving the
     * {@code AUTO} pseudo-format against the table's live row count. Honors a
     * test override installed via {@link #setFormatForTest(String)}. The
     * returned name is validated against {@link FormatRegistry}; unregistered
     * names fall back to {@link ErrorMessages#STORAGE_FORMAT_CSV}.
     *
     * @param tableName    the table name (used for the per-table config key)
     * @param liveRowCount the table's live row count (for {@code AUTO})
     * @return the concrete format name (PARQUET, CSV or SERIALIZED)
     */
    static String formatForTable(String tableName, long liveRowCount) {
        String raw;
        if (configuredFormat != null) {
            raw = configuredFormat;
        } else {
            raw = readRawFormat(tableName);
            if (raw == null) {
                raw = ErrorMessages.STORAGE_FORMAT_CSV;
            }
        }
        if (!isValid(raw)) {
            LOGGER.log(Level.WARNING, "Unknown storage format '{0}', falling back to CSV", raw);
            raw = ErrorMessages.STORAGE_FORMAT_CSV;
        }
        if (raw.equals(ErrorMessages.STORAGE_FORMAT_AUTO)) {
            raw = liveRowCount >= Table.COLUMNAR_THRESHOLD_ROWS
                    ? ErrorMessages.STORAGE_FORMAT_PARQUET
                    : ErrorMessages.STORAGE_FORMAT_CSV;
        }
        if (FormatRegistry.get(raw) == null) {
            LOGGER.log(Level.WARNING, "Format '{0}' not registered in FormatRegistry, falling back to CSV", raw);
            raw = ErrorMessages.STORAGE_FORMAT_CSV;
        }
        return raw;
    }

    /**
     * Resolves the {@link TableFormat} handler for a table. This combines
     * config-based format name resolution with {@link FormatRegistry} lookup,
     * providing a single entry point for the engine to obtain the correct
     * format handler.
     *
     * @param tableName    the table name (used for per-table config keys)
     * @param liveRowCount the table's live row count (for {@code AUTO})
     * @return the resolved format handler, never null
     */
    static TableFormat resolveFormatHandler(String tableName, long liveRowCount) {
        String formatName = formatForTable(tableName, liveRowCount);
        TableFormat format = FormatRegistry.get(formatName);
        if (format == null) {
            format = FormatRegistry.get(ErrorMessages.STORAGE_FORMAT_CSV);
        }
        return format;
    }

    /**
     * Returns true when the given table should be persisted to Parquet given
     * the configured format and the table's current live row count.
     *
     * @param tableName    the table name (for per-table config keys)
     * @param liveRowCount the table's live row count
     * @return whether Parquet persistence applies
     */
    static boolean usesParquet(String tableName, long liveRowCount) {
        return ErrorMessages.STORAGE_FORMAT_PARQUET.equals(formatForTable(tableName, liveRowCount));
    }

    /**
     * Returns true when a table with the given live row count should be
     * persisted to Parquet, ignoring per-table config keys.
     *
     * @param liveRowCount the table's live row count
     * @return whether Parquet persistence applies
     */
    static boolean usesParquet(long liveRowCount) {
        return usesParquet(null, liveRowCount);
    }

    /**
     * Converts the config.properties to a Map suitable for
     * {@link FormatRegistry#resolve(String, Map)}.
     */
    static Map<String, String> toConfigMap() {
        Properties props = loadProperties();
        Map<String, String> map = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
        }
        return map;
    }

    /**
     * Clears the cached/override configured format (used by tests to force
     * re-read).
     */
    static void resetCacheForTest() {
        configuredFormat = null;
    }

    /**
     * Forces a specific configured format for tests without touching the
     * {@code config.properties} file. The next {@link #resetCacheForTest()}
     * clears the override so the real configuration is read again.
     *
     * @param format one of the {@code STORAGE_FORMAT_*} constants
     */
    static void setFormatForTest(String format) {
        configuredFormat = format;
    }
}
