package diesel;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resolves the whole-database persistent storage format from the
 * {@code storage.format} key in {@code config.properties}. This selects the
 * file read/write mechanism used by every table:
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
 * <p>The default is {@code CSV}. Unknown values fall back to {@code CSV}.
 * Users who want columnar storage can opt in explicitly with
 * {@code storage.format=PARQUET} in {@code config.properties} or via
 * {@code SET storage.format = PARQUET;}.</p>
 */
final class StorageFormat {

    private static final Logger LOGGER = Logger.getLogger(StorageFormat.class.getName());

    private static final String CONFIG_KEY = "storage.format";
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
            raw = loadFromConfig();
            configuredFormat = raw;
        }
        return raw;
    }

    private static String loadFromConfig() {
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                Properties props = new Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String val = props.getProperty(CONFIG_KEY);
                if (val != null && !val.isBlank()) {
                    String v = val.trim().toUpperCase();
                    if (isValid(v)) {
                        return v;
                    }
                    LOGGER.log(Level.WARNING, "Unknown storage.format '{0}', falling back to CSV", val);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to read storage.format: {0}", e.getMessage());
        }
        return ErrorMessages.STORAGE_FORMAT_CSV;
    }

    private static boolean isValid(String v) {
        return v.equals(ErrorMessages.STORAGE_FORMAT_PARQUET)
                || v.equals(ErrorMessages.STORAGE_FORMAT_CSV)
                || v.equals(ErrorMessages.STORAGE_FORMAT_SERIALIZED)
                || v.equals(ErrorMessages.STORAGE_FORMAT_AUTO);
    }

    /**
     * Returns true when the given table should be persisted to Parquet given
     * the configured format and the table's current live row count.
     *
     * @param liveRowCount the table's live row count
     * @return whether Parquet persistence applies
     */
    static boolean usesParquet(long liveRowCount) {
        String fmt = configuredFormat();
        if (fmt.equals(ErrorMessages.STORAGE_FORMAT_PARQUET)) {
            return true;
        }
        if (fmt.equals(ErrorMessages.STORAGE_FORMAT_SERIALIZED)) {
            return false;
        }
        if (fmt.equals(ErrorMessages.STORAGE_FORMAT_CSV)) {
            return false;
        }
        // AUTO
        return liveRowCount >= Table.COLUMNAR_THRESHOLD_ROWS;
    }

    /**
     * Clears the cached configured format (used by tests to force re-read).
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
