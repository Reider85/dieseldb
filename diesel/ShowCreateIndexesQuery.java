package diesel;

import java.util.List;
import java.util.Map;

/**
 * Query that returns the CREATE INDEX DDL statements for a specific table.
 */
class ShowCreateIndexesQuery implements Query<List<Map<String, Object>>> {

    private final String tableName;

    /**
     * Creates a new SHOW CREATE INDEXES query.
     *
     * @param tableName the table name to get index DDL for
     */
    public ShowCreateIndexesQuery(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return execute(table.getDatabase(), -1);
    }
    
    @Override
    public List<Map<String, Object>> execute(Database database, long txid) {
        Table t = database.getTable(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Generate CREATE INDEX DDL statements
        List<String> ddlStatements = generateCreateIndexDdl(t);
        
        // Return as list with "table" and "create_index" columns
        List<Map<String, Object>> results = new java.util.ArrayList<>();
        for (String ddl : ddlStatements) {
            Map<String, Object> row = new java.util.HashMap<>();
            row.put("table", tableName);
            row.put("create_index", ddl);
            results.add(row);
        }
        
        return results;
    }

    /**
     * Generates the CREATE INDEX DDL statements for the given table.
     *
     * @param table the table to generate index DDL for
     * @return list of CREATE INDEX statements
     */
    private List<String> generateCreateIndexDdl(Table table) {
        List<String> statements = new java.util.ArrayList<>();
        Map<String, String> indexDefinitions = table.getIndexDefinitions();
        
        for (Map.Entry<String, String> entry : indexDefinitions.entrySet()) {
            String indexName = entry.getKey();
            String indexType = entry.getValue();
            
            // Find the column name for this index
            String columnName = findColumnForIndex(indexName, table);
            if (columnName == null) {
                continue; // Skip if we can't find the column
            }
            
            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE ");
            
            if (indexType.contains("UNIQUE")) {
                ddl.append("UNIQUE ");
            }
            
            if (indexType.contains("HASH")) {
                ddl.append("HASH ");
            }
            
            ddl.append("INDEX ").append(indexName).append(" ON ").append(table.getName());
            ddl.append(" (").append(columnName).append(")");
            
            if (indexType.contains("COVERING")) {
                // For covering indexes, add the covering columns
                // This is a simplified version - actual covering index implementation
                // would need to extract the covering columns from the index definition
                ddl.append(" COVERING (...)");
            }
            
            statements.add(ddl.toString());
        }
        
        return statements;
    }

    /**
     * Finds the column name for a given index name.
     * This is a simplified approach - in a real implementation, we would
     * need to parse the index definition more carefully.
     *
     * @param indexName the index name
     * @param table the table to search in
     * @return the column name, or null if not found
     */
    private String findColumnForIndex(String indexName, Table table) {
        // For now, assume the index name matches the column name
        // This works for simple indexes but may need enhancement for composite indexes
        if (table.getIndexes().containsKey(indexName)) {
            return indexName;
        }
        
        // If not found directly, try to find it in the index definitions
        // This is a heuristic and may not work for all cases
        for (String column : table.getColumns()) {
            if (table.getIndexes().containsKey(column)) {
                return column;
            }
        }
        
        return null;
    }

    @Override
    public String toString() {
        return "SHOW CREATE INDEXES " + tableName;
    }
}