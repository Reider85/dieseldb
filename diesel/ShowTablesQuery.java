package diesel;

import java.util.List;
import java.util.Map;

/**
 * Query that returns a list of all user table names.
 * Internal system tables (prefixed with "DIESEL_") are excluded.
 */
class ShowTablesQuery implements Query<List<Map<String, Object>>> {

    /**
     * Creates a new SHOW TABLES query.
     */
    public ShowTablesQuery() {
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        return execute(table.getDatabase(), -1);
    }
    
    @Override
    public List<Map<String, Object>> execute(Database database, long txid) {
        java.util.List<String> tables = new java.util.ArrayList<>();
        for (Table t : database.getTables()) {
            if (!t.getName().startsWith("DIESEL_")) {
                tables.add(t.getName());
            }
        }
        
        // Return as list of maps with "table" column
        java.util.List<Map<String, Object>> results = new java.util.ArrayList<>();
        for (String tableName : tables) {
            java.util.Map<String, Object> row = new java.util.HashMap<>();
            row.put("table", tableName);
            results.add(row);
        }
        
        return results;
    }

    @Override
    public String toString() {
        return "SHOW TABLES";
    }
}