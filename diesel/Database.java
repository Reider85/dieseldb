package diesel;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
// Database facade (High Cohesion, Controller)
class Database {
    private static final Logger LOGGER = Logger.getLogger(Database.class.getName());
    private final Map<String, Table> tables = new HashMap<>();
    private final QueryParser parser = new QueryParser();

    public void createTable(String name, List<String> columns) {
        tables.put(name, new Table(columns));
        LOGGER.log(Level.INFO, "Created table: {0} with columns: {1}", new Object[]{name, columns});
    }

    public void insert(String tableName, Map<String, String> row) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.addRow(row);
            LOGGER.log(Level.INFO, "Inserted row into table {0}: {1}", new Object[]{tableName, row});
        } else {
            LOGGER.log(Level.WARNING, "Table not found for insert: {0}", tableName);
        }
    }

    public List<Map<String, String>> executeQuery(String query) {
        try {
            LOGGER.log(Level.INFO, "Executing query: {0}", query);
            Query parsedQuery = parser.parse(query);
            String tableName = extractTableName(query);
            Table table = tables.get(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
            List<Map<String, String>> result = parsedQuery.execute(table);
            LOGGER.log(Level.INFO, "Query returned {0} rows", new Object[]{result.size()});
            return result;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query execution failed: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
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