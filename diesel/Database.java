package diesel;
import java.util.*;
// Database facade (High Cohesion, Controller)
class Database {
    private final Map<String, Table> tables = new HashMap<>();
    private final QueryParser parser = new QueryParser();

    public void createTable(String name, List<String> columns) {
        tables.put(name, new Table(columns));
    }

    public void insert(String tableName, Map<String, String> row) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.addRow(row);
        }
    }

    public List<Map<String, String>> executeQuery(String query) {
        Query parsedQuery = parser.parse(query);
        String tableName = extractTableName(query);
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return parsedQuery.execute(table);
    }

    private String extractTableName(String query) {
        String normalized = query.toUpperCase();
        String[] parts = normalized.split("FROM");
        if (parts.length < 2) {
            return "";
        }
        String tablePart = parts[1].trim();
        return tablePart.split(" ")[0].trim();
    }
}
