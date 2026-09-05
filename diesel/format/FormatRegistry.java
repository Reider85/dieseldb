package diesel.format;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plugin registry for {@link TableFormat} implementations. The engine, tools
 * and tests register formats once; thereafter files and tables can be
 * addressed purely by format name. The registry also resolves which format a
 * table should use, consulting (in order):
 *
 * <ol>
 *   <li>a per-table override {@code storage.format.<TABLE_NAME>},</li>
 *   <li>the per-format default key {@code storage.format.default},</li>
 *   <li>a global fallback key {@code storage.format} (legacy config),</li>
 *   <li>the {@link #DEFAULT_FORMAT_NAME}.</li>
 * </ol>
 */
public final class FormatRegistry {

    /** Config key for the global legacy format selector ({@code storage.format}). */
    public static final String CONFIG_KEY_GLOBAL = "storage.format";

    /** Config key for the framework default ({@code storage.format.default}). */
    public static final String CONFIG_KEY_DEFAULT = "storage.format.default";

    /** Config key prefix for per-table overrides ({@code storage.format.<TABLE>}). */
    public static final String CONFIG_KEY_TABLE_PREFIX = "storage.format.";

    /** Format used when no configuration selects a registered format. */
    public static final String DEFAULT_FORMAT_NAME = "CSV";

    private static final Map<String, TableFormat> FORMATS = new ConcurrentHashMap<>();

    static {
        register(new CsvFormat());
        register(new ParquetFormat());
    }

    private FormatRegistry() {
    }

    /**
     * Registers a format. Registering the same name twice replaces the
     * previous implementation.
     *
     * @param format the format to register; name must not be blank
     * @return the previously registered format for that name, or {@code null}
     */
    public static TableFormat register(TableFormat format) {
        if (format == null || format.getName() == null || format.getName().isBlank()) {
            throw new IllegalArgumentException("Format name must not be blank");
        }
        return FORMATS.put(format.getName().toUpperCase(), format);
    }

    /**
     * Returns the registered format for the given name (case-insensitive).
     *
     * @param name the format name (e.g. {@code "CSV"})
     * @return the format, or {@code null} when not registered
     */
    public static TableFormat get(String name) {
        if (name == null) {
            return null;
        }
        TableFormat format = FORMATS.get(name.toUpperCase());
        if (format != null) {
            return format;
        }
        for (Map.Entry<String, TableFormat> e : FORMATS.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
                return e.getValue();
            }
        }
        return null;
    }

    /**
     * Returns all registered format names.
     *
     * @return the registered names
     */
    public static Set<String> getRegisteredFormats() {
        return FORMATS.keySet();
    }

    /**
     * Removes a registered format (used by tests).
     *
     * @param name the format name
     * @return the removed format, or {@code null}
     */
    public static TableFormat unregister(String name) {
        return FORMATS.remove(name == null ? null : name.toUpperCase());
    }

    /**
     * Resolves the format for a table from the provided configuration.
     *
     * @param tableName the table name (may be {@code null})
     * @param config    configuration properties (key → value)
     * @return the resolved format, falling back to {@link #DEFAULT_FORMAT_NAME}
     */
    public static TableFormat resolve(String tableName, java.util.Map<String, String> config) {
        String name = null;
        if (tableName != null && config != null) {
            String perTable = config.get(CONFIG_KEY_TABLE_PREFIX + tableName);
            if (perTable != null) {
                name = perTable;
            }
        }
        if (name == null && config != null) {
            name = config.get(CONFIG_KEY_DEFAULT);
        }
        if (name == null && config != null) {
            name = config.get(CONFIG_KEY_GLOBAL);
        }
        TableFormat format = get(name);
        return format != null ? format : get(DEFAULT_FORMAT_NAME);
    }

    /**
     * Resolves the format for a table using only the global config key
     * (legacy {@code storage.format} semantics).
     *
     * @param value  the configured global format name
     * @return the resolved format
     */
    public static TableFormat resolve(String value) {
        TableFormat format = get(value);
        return format != null ? format : get(DEFAULT_FORMAT_NAME);
    }
}