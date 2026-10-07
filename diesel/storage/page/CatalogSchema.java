package diesel.storage.page;

import diesel.storage.StorageType;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema of a single user table as stored by {@link CatalogTable} in the
 * catalog page (prompt4 #9, R3-002).
 *
 * <p>Serialisation is hand-written against the library-neutral
 * {@link JsonStreamGenerator}/{@link JsonStreamParser} streaming API (prompt 42),
 * so this class never imports a JSON library directly.
 */
public final class CatalogSchema {

    private String tableName;
    private final List<ColumnSchema> columns = new ArrayList<>();
    private String primaryKey;
    private StorageType storageType;
    private final List<String> sequences = new ArrayList<>();
    private final List<IndexSchema> indices = new ArrayList<>();

    public CatalogSchema() {
    }

    public CatalogSchema(String tableName, List<ColumnSchema> columns, String primaryKey,
                         StorageType storageType, List<String> sequences, List<IndexSchema> indices) {
        this.tableName = tableName;
        if (columns != null) {
            this.columns.addAll(columns);
        }
        this.primaryKey = primaryKey;
        this.storageType = storageType;
        if (sequences != null) {
            this.sequences.addAll(sequences);
        }
        if (indices != null) {
            this.indices.addAll(indices);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnSchema> getColumns() {
        return columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public List<String> getSequences() {
        return sequences;
    }

    public List<IndexSchema> getIndices() {
        return indices;
    }

    /** Appends an index definition if one with the same name is not present yet. */
    public void addIndex(IndexSchema index) {
        if (index == null) {
            return;
        }
        indices.removeIf(existing -> existing.getName() != null && existing.getName().equals(index.getName()));
        indices.add(index);
    }

    /** Writes the whole catalog ({@code [ schema, ... ]}) as a JSON array. */
    static void writeArray(JsonStreamGenerator gen, List<CatalogSchema> schemas) throws IOException {
        gen.writeStartArray();
        for (CatalogSchema schema : schemas) {
            schema.write(gen);
        }
        gen.writeEndArray();
    }

    /** Reads a whole catalog JSON array produced by {@link #writeArray}. */
    static List<CatalogSchema> readArray(JsonStreamParser parser) throws IOException {
        List<CatalogSchema> schemas = new ArrayList<>();
        if (parser.nextToken() != JsonEvent.START_ARRAY) {
            throw new CatalogCorruptedException("catalog page must contain a JSON array of table schemas");
        }
        while (parser.nextToken() == JsonEvent.START_OBJECT) {
            schemas.add(readSchema(parser));
        }
        if (parser.currentEvent() != JsonEvent.END_ARRAY) {
            throw new CatalogCorruptedException("unterminated table schema array in catalog page");
        }
        return schemas;
    }

    private void write(JsonStreamGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeFieldName("tableName");
        gen.writeString(tableName);
        gen.writeFieldName("primaryKey");
        if (primaryKey == null) {
            gen.writeNull();
        } else {
            gen.writeString(primaryKey);
        }
        gen.writeFieldName("storageType");
        if (storageType == null) {
            gen.writeNull();
        } else {
            gen.writeString(storageType.getTypeName());
        }

        gen.writeFieldName("columns");
        gen.writeStartArray();
        for (ColumnSchema column : columns) {
            column.write(gen);
        }
        gen.writeEndArray();

        gen.writeFieldName("sequences");
        gen.writeStartArray();
        for (String sequence : sequences) {
            gen.writeString(sequence);
        }
        gen.writeEndArray();

        gen.writeFieldName("indices");
        gen.writeStartArray();
        for (IndexSchema index : indices) {
            index.write(gen);
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    /** Reads one {@code {...}} schema object; the parser sits on its START_OBJECT. */
    private static CatalogSchema readSchema(JsonStreamParser parser) throws IOException {
        CatalogSchema schema = new CatalogSchema();
        while (parser.nextToken() == JsonEvent.FIELD_NAME) {
            String field = parser.currentName();
            JsonEvent value = parser.nextToken();
            switch (field) {
                case "tableName" -> schema.tableName = text(parser, value, field);
                case "primaryKey" -> schema.primaryKey = value == JsonEvent.VALUE_NULL
                        ? null : text(parser, value, field);
                case "storageType" -> schema.storageType = value == JsonEvent.VALUE_NULL
                        ? null : StorageType.fromString(text(parser, value, field));
                case "columns" -> readColumns(parser, value, schema.columns);
                case "sequences" -> readStrings(parser, value, schema.sequences);
                case "indices" -> readIndices(parser, value, schema.indices);
                default -> parser.skipChildren();
            }
        }
        if (schema.tableName == null) {
            throw new CatalogCorruptedException("table schema without a tableName in catalog page");
        }
        return schema;
    }

    private static void readColumns(JsonStreamParser parser, JsonEvent event, List<ColumnSchema> out)
            throws IOException {
        if (event != JsonEvent.START_ARRAY) {
            throw new CatalogCorruptedException("columns must be a JSON array");
        }
        while (parser.nextToken() == JsonEvent.START_OBJECT) {
            out.add(ColumnSchema.read(parser));
        }
        if (parser.currentEvent() != JsonEvent.END_ARRAY) {
            throw new CatalogCorruptedException("unterminated columns array");
        }
    }

    private static void readIndices(JsonStreamParser parser, JsonEvent event, List<IndexSchema> out)
            throws IOException {
        if (event != JsonEvent.START_ARRAY) {
            throw new CatalogCorruptedException("indices must be a JSON array");
        }
        while (parser.nextToken() == JsonEvent.START_OBJECT) {
            out.add(IndexSchema.read(parser));
        }
        if (parser.currentEvent() != JsonEvent.END_ARRAY) {
            throw new CatalogCorruptedException("unterminated indices array");
        }
    }

    private static void readStrings(JsonStreamParser parser, JsonEvent event, List<String> out)
            throws IOException {
        if (event != JsonEvent.START_ARRAY) {
            throw new CatalogCorruptedException("sequences must be a JSON array");
        }
        JsonEvent item;
        while ((item = parser.nextToken()) == JsonEvent.VALUE_STRING) {
            out.add(parser.getText());
        }
        if (item != JsonEvent.END_ARRAY) {
            throw new CatalogCorruptedException("unterminated sequences array");
        }
    }

    private static String text(JsonStreamParser parser, JsonEvent event, String field) throws IOException {
        if (event != JsonEvent.VALUE_STRING) {
            throw new CatalogCorruptedException("field '" + field + "' must be a JSON string");
        }
        return parser.getText();
    }

    /** Serialised column definition. */
    public static final class ColumnSchema {
        private String name;
        private String type;
        private boolean nullable;
        private boolean unique;

        public ColumnSchema() {
        }

        public ColumnSchema(String name, String type, boolean nullable, boolean unique) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.unique = unique;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isNullable() {
            return nullable;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        public boolean isUnique() {
            return unique;
        }

        public void setUnique(boolean unique) {
            this.unique = unique;
        }

        private void write(JsonStreamGenerator gen) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("name");
            gen.writeString(name);
            gen.writeFieldName("type");
            gen.writeString(type);
            gen.writeFieldName("nullable");
            gen.writeBoolean(nullable);
            gen.writeFieldName("unique");
            gen.writeBoolean(unique);
            gen.writeEndObject();
        }

        private static ColumnSchema read(JsonStreamParser parser) throws IOException {
            ColumnSchema column = new ColumnSchema();
            while (parser.nextToken() == JsonEvent.FIELD_NAME) {
                String field = parser.currentName();
                JsonEvent value = parser.nextToken();
                switch (field) {
                    case "name" -> column.name = text(parser, value, field);
                    case "type" -> column.type = text(parser, value, field);
                    case "nullable" -> column.nullable = bool(parser, value, field);
                    case "unique" -> column.unique = bool(parser, value, field);
                    default -> parser.skipChildren();
                }
            }
            return column;
        }
    }

    /** Serialised index definition. */
    public static final class IndexSchema {
        private String name;
        private final List<String> columns = new ArrayList<>();
        private boolean unique;

        public IndexSchema() {
        }

        public IndexSchema(String name, List<String> columns, boolean unique) {
            this.name = name;
            if (columns != null) {
                this.columns.addAll(columns);
            }
            this.unique = unique;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getColumns() {
            return columns;
        }

        public boolean isUnique() {
            return unique;
        }

        public void setUnique(boolean unique) {
            this.unique = unique;
        }

        private void write(JsonStreamGenerator gen) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("name");
            gen.writeString(name);
            gen.writeFieldName("unique");
            gen.writeBoolean(unique);
            gen.writeFieldName("columns");
            gen.writeStartArray();
            for (String column : columns) {
                gen.writeString(column);
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }

        private static IndexSchema read(JsonStreamParser parser) throws IOException {
            IndexSchema index = new IndexSchema();
            while (parser.nextToken() == JsonEvent.FIELD_NAME) {
                String field = parser.currentName();
                JsonEvent value = parser.nextToken();
                switch (field) {
                    case "name" -> index.name = text(parser, value, field);
                    case "unique" -> index.unique = bool(parser, value, field);
                    case "columns" -> readStrings(parser, value, index.columns);
                    default -> parser.skipChildren();
                }
            }
            return index;
        }
    }

    private static boolean bool(JsonStreamParser parser, JsonEvent event, String field) throws IOException {
        if (event == JsonEvent.VALUE_TRUE) {
            return true;
        }
        if (event == JsonEvent.VALUE_FALSE) {
            return false;
        }
        throw new CatalogCorruptedException("field '" + field + "' must be a JSON boolean");
    }

    /** Fluent builder retained for readable test/DDL construction. */
    public static final class Builder {
        private String tableName;
        private final List<ColumnSchema> columns = new ArrayList<>();
        private String primaryKey;
        private StorageType storageType;
        private final List<String> sequences = new ArrayList<>();
        private final List<IndexSchema> indices = new ArrayList<>();

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder addColumn(String name, String type, boolean nullable, boolean unique) {
            this.columns.add(new ColumnSchema(name, type, nullable, unique));
            return this;
        }

        public Builder primaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder storageType(StorageType storageType) {
            this.storageType = storageType;
            return this;
        }

        public Builder addSequence(String sequence) {
            this.sequences.add(sequence);
            return this;
        }

        public Builder addIndex(String name, List<String> columns, boolean unique) {
            this.indices.add(new IndexSchema(name, columns, unique));
            return this;
        }

        public CatalogSchema build() {
            return new CatalogSchema(tableName, columns, primaryKey, storageType, sequences, indices);
        }
    }
}
