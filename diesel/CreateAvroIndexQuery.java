package diesel;

import diesel.storage.avro.AvroRowStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DDL query for creating secondary indexes on AVRO tables.
 * Supports both single-column and composite indexes.
 */
public class CreateAvroIndexQuery {

    private static final Pattern INDEX_PATTERN = Pattern.compile(
        "CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(IF\\s+NOT\\s+EXISTS\\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\\s+ON\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(([^)]+)\\)",
        Pattern.CASE_INSENSITIVE
    );

    private final String indexName;
    private final String tableName;
    private final List<String> columns;
    private final boolean unique;
    private final boolean ifNotExists;

    public CreateAvroIndexQuery(String indexName, String tableName, List<String> columns, 
                               boolean unique, boolean ifNotExists) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.unique = unique;
        this.ifNotExists = ifNotExists;
    }

    /**
     * Parse a CREATE INDEX statement and create a query.
     */
    public static CreateAvroIndexQuery parse(String sql) {
        sql = sql.trim();
        Matcher matcher = INDEX_PATTERN.matcher(sql);
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid CREATE INDEX syntax: " + sql);
        }

        boolean unique = matcher.group(1) != null;
        boolean ifNotExists = matcher.group(2) != null;
        String indexName = matcher.group(3);
        String tableName = matcher.group(4);
        
        // Parse column list
        String columnList = matcher.group(5);
        List<String> columns = new ArrayList<>();
        
        // Split columns by comma, handling spaces
        String[] columnArray = columnList.split("\\s*,\\s*");
        for (String column : columnArray) {
            if (!column.isEmpty()) {
                columns.add(column);
            }
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be specified for index");
        }

        return new CreateAvroIndexQuery(indexName, tableName, columns, unique, ifNotExists);
    }

    public String toString() {
        return String.format("CREATE%sINDEX %s%s ON %s (%s)",
                unique ? " UNIQUE" : "",
                ifNotExists ? " IF NOT EXISTS" : "",
                indexName,
                tableName,
                String.join(", ", columns));
    }

    // Builder for programmatic index creation
    public static class Builder {
        private String indexName;
        private String tableName;
        private List<String> columns = new ArrayList<>();
        private boolean unique = false;
        private boolean ifNotExists = false;

        public Builder indexName(String name) {
            this.indexName = name;
            return this;
        }

        public Builder tableName(String name) {
            this.tableName = name;
            return this;
        }

        public Builder column(String column) {
            this.columns.add(column);
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder unique(boolean unique) {
            this.unique = unique;
            return this;
        }

        public Builder ifNotExists(boolean ifNotExists) {
            this.ifNotExists = ifNotExists;
            return this;
        }

        public CreateAvroIndexQuery build() {
            if (indexName == null || indexName.isEmpty()) {
                throw new IllegalArgumentException("Index name must be specified");
            }
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name must be specified");
            }
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("At least one column must be specified");
            }
            return new CreateAvroIndexQuery(indexName, tableName, columns, unique, ifNotExists);
        }
    }
}