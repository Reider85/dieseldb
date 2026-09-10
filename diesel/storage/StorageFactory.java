package diesel.storage;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory for creating {@link RowStorage} implementations by type name.
 * The storage type is configured via {@code storage.type} in
 * {@code config.properties}. Supported values:
 *
 * <ul>
 *   <li>{@code in_memory} (default) &mdash; {@link InMemoryRowStorage}</li>
 *   <li>{@code file_based} &mdash; {@link FileBasedRowStorage}</li>
 * </ul>
 */
public final class StorageFactory {

    private static final Logger LOGGER = Logger.getLogger(StorageFactory.class.getName());

    private StorageFactory() {
        // Utility class.
    }

    /**
     * Creates a {@link RowStorage} of the requested type.
     *
     * @param type        the storage type name ({@code "in_memory"} or
     *                    {@code "file_based"})
     * @param tableName   the table name
     * @param columns     the ordered column names
     * @param columnTypes the column name to type mapping
     * @return a new {@code RowStorage} instance
     * @throws IllegalArgumentException if {@code type} is not recognised
     */
    public static RowStorage create(String type, String tableName,
                                    List<String> columns, Map<String, Class<?>> columnTypes) {
        String resolved = (type != null && !type.isBlank()) ? type.trim().toLowerCase() : "in_memory";
        return switch (resolved) {
            case "file_based" -> {
                LOGGER.log(Level.FINE, "Creating FileBasedRowStorage for table {0}", tableName);
                yield new FileBasedRowStorage(tableName, columns, columnTypes);
            }
            default -> {
                LOGGER.log(Level.FINE, "Creating InMemoryRowStorage for table {0}", tableName);
                yield new InMemoryRowStorage(tableName, columns, columnTypes);
            }
        };
    }
}
