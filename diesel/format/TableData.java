package diesel.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unifying data carrier exchanged between the engine and {@link TableFormat}
 * handlers. It carries the ordered column list, the column-type map, the rows
 * (column-name → value maps) and an arbitrary metadata map. Metadata uses the
 * {@code META_*} constants for the keys the engine understands (primary key,
 * sequences, index definitions …). Format handlers never see the engine's
 * {@code Table} object; they serialize/deserialize this carrier instead.
 *
 * <p>The carrier is immutable in shape: its lists/maps are defensive copies,
 * but the row maps themselves are shared by reference for performance (the
 * engine writes them under its own row locks).</p>
 */
public final class TableData {

    /** Metadata key: table persistent format version ({@link Integer}). */
    public static final String META_FORMAT_VERSION = "formatVersion";
    /** Metadata key: primary key column name ({@link String}, may be absent). */
    public static final String META_PRIMARY_KEY = "primaryKey";
    /** Metadata key: whether a clustered index exists ({@link Boolean}). */
    public static final String META_HAS_CLUSTERED_INDEX = "hasClusteredIndex";
    /** Metadata key: clustered index column name ({@link String}). */
    public static final String META_CLUSTERED_INDEX_COLUMN = "clusteredIndexColumn";
    /** Metadata key: column name → {@code Sequence} map. */
    public static final String META_SEQUENCES = "sequences";
    /** Metadata key: indexed column/name → index definition string map. */
    public static final String META_INDEX_DEFINITIONS = "indexDefinitions";
    /** Metadata key: indexed column/name → covering column list map. */
    public static final String META_COVER_COLUMN_DEFINITIONS = "coverColumnDefinitions";

    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes;
    private final List<Map<String, Object>> rows;
    private final Map<String, Object> metadata;

    /**
     * Creates the carrier.
     *
     * @param columns     ordered column names (empty for unknown schema)
     * @param columnTypes column name to Java type mapping
     * @param rows        row data, possibly empty
     * @param metadata    schema/dictionary metadata
     */
    public TableData(List<String> columns,
                     Map<String, Class<?>> columnTypes,
                     List<Map<String, Object>> rows,
                     Map<String, Object> metadata) {
        this.columns = columns == null ? List.of() : List.copyOf(columns);
        this.columnTypes = Collections.unmodifiableMap(new LinkedHashMap<>(
                columnTypes == null ? Map.of() : columnTypes));
        this.rows = rows == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(rows));
        this.metadata = metadata == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
    }

    /**
     * Returns the ordered column names.
     *
     * @return the columns
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * Returns the column name to Java type mapping.
     *
     * @return the column types
     */
    public Map<String, Class<?>> getColumnTypes() {
        return columnTypes;
    }

    /**
     * Returns the row data. May be empty for schema-only reads.
     *
     * @return the rows
     */
    public List<Map<String, Object>> getRows() {
        return rows;
    }

    /**
     * Returns the metadata map.
     *
     * @return the metadata
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Case-insensitively looks up a metadata value.
     *
     * @param key the metadata key
     * @return the value or {@code null}
     */
    public Object getMetadataValue(String key) {
        if (metadata.containsKey(key)) {
            return metadata.get(key);
        }
        for (Map.Entry<String, Object> e : metadata.entrySet()) {
            if (e.getKey().equalsIgnoreCase(key)) {
                return e.getValue();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "TableData{columns=" + columns + ", rows=" + rows.size()
                + ", metadataKeys=" + metadata.keySet() + '}';
    }

    /**
     * Returns a builder for fluent construction.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link TableData}. */
    public static final class Builder {
        private List<String> columns = List.of();
        private Map<String, Class<?>> columnTypes = Map.of();
        private List<Map<String, Object>> rows = List.of();
        private Map<String, Object> metadata = Map.of();

        private Builder() {
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder columnTypes(Map<String, Class<?>> columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public Builder rows(List<Map<String, Object>> rows) {
            this.rows = rows;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder metadataValue(String key, Object value) {
            Map<String, Object> merged = new LinkedHashMap<>(this.metadata);
            merged.put(key, value);
            this.metadata = merged;
            return this;
        }

        public TableData build() {
            return new TableData(columns, columnTypes, rows, metadata);
        }
    }
}