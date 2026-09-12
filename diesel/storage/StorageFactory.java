package diesel.storage;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link RowStorage} implementations by type name.
 * The storage type is configured via {@code storage.type} in
 * {@code config.properties}. Supported values:
 *
 * <ul>
 *   <li>{@code in_memory} (default) &mdash; {@link InMemoryRowStorage}</li>
 *   <li>{@code csv} &mdash; {@link CsvRowStorage}</li>
 *   <li>{@code tsv} &mdash; {@link TsvRowStorage}</li>
 * </ul>
 */
public final class StorageFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageFactory.class);

    private StorageFactory() {
        // Utility class.
    }

    /**
     * Creates a {@link RowStorage} of the requested type.
     *
     * @param type        the storage type name ({@code "in_memory"},
     *                    {@code "csv"} or {@code "tsv"})
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
            case "csv" -> {
                LOGGER.debug("Creating CsvRowStorage for table {}", tableName);
                yield new CsvRowStorage(tableName, columns, columnTypes);
            }
            case "tsv" -> {
                LOGGER.debug("Creating TsvRowStorage for table {}", tableName);
                yield new TsvRowStorage(tableName, columns, columnTypes);
            }
            default -> {
                LOGGER.debug("Creating InMemoryRowStorage for table {}", tableName);
                yield new InMemoryRowStorage(tableName, columns, columnTypes);
            }
        };
    }
}
