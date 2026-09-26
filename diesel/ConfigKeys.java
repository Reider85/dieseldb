package diesel;

/**
 * Prompt 14 (java:S1192): Configuration-file and system-property literals that
 * were previously hard-coded in 30+ classes across the engine and the AVRO
 * storage module.
 *
 * <p>Centralising them guarantees a config file cannot be renamed in one module
 * while another module still looks for the old name, and removes the duplicated
 * {@code "config.properties"} / {@code "user.dir"} spellings that SonarQube
 * flagged as S1192. Behaviour is identical to using the inline literal.
 */
public final class ConfigKeys {

    private ConfigKeys() {
    }

    /**
     * Name of the engine configuration file, resolved relative to the working
     * directory (or to the table's schema directory, where applicable).
     */
    public static final String CONFIG_FILE = "config.properties";

    /**
     * System property holding the process working directory. Used as the base
     * directory when locating {@link #CONFIG_FILE}.
     */
    public static final String SYS_PROP_USER_DIR = "user.dir";

    /** Fallback working directory used when {@link #SYS_PROP_USER_DIR} is unset. */
    public static final String CURRENT_DIR_FALLBACK = ".";

    /**
     * System property that selects the storage format ({@code csv}, {@code tsv},
     * {@code jsonl}, {@code avro}).
     */
    public static final String STORAGE_TYPE_PROP = "diesel.storage.type";
}
