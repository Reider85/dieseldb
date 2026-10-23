package diesel.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable schema version for online schema changes (R3-010 step 1/3).
 * Represents a point-in-time snapshot of table columns and types.
 * 
 * @see SchemaVersionRegistry
 * @see diesel.storage.page.CatalogSchema
 */
public final class SchemaVersion {
    private final long version;
    private final long createdAtMs;
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final String description;

    /**
     * Creates a new schema version.
     * 
     * @param version        Monotonic version number (1-based, starting from 1)
     * @param createdAtMs    System.currentTimeMillis() when version was created
     * @param columns        Unmodifiable list of column names (defensive copy)
     * @param columnTypes    Unmodifiable map of column name to type (defensive copy)
     * @param description    Human-readable change description (e.g. "ADD COLUMN AGE Integer")
     */
    public SchemaVersion(long version, long createdAtMs, List<String> columns, 
                         Map<String, Class<?>> columnTypes, String description) {
        this.version = version;
        this.createdAtMs = createdAtMs;
        this.columns = Collections.unmodifiableList(Objects.requireNonNull(columns));
        this.columnTypes = Collections.unmodifiableMap(Objects.requireNonNull(columnTypes));
        this.description = description;
    }

    public long getVersion() {
        return version;
    }

    public long getCreatedAtMs() {
        return createdAtMs;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Map<String, Class<?>> getColumnTypes() {
        return columnTypes;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaVersion that = (SchemaVersion) o;
        return version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version);
    }

    @Override
    public String toString() {
        return "SchemaVersion{" +
               "version=" + version +
               ", description='" + description + '\'' +
               ", columns=" + columns +
               '}';
    }
}